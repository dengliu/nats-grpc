package rpc

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwebrtc/nats-grpc/examples/benchmark/protos/benchmark"
	"github.com/cloudwebrtc/nats-grpc/pkg/protos/nrpc"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// runEmbeddedNATS starts a NATS server on an OS-chosen port and returns its
// nats:// URL. The server is shut down at test cleanup.
func runEmbeddedNATS(t *testing.T) string {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = natsserver.RANDOM_PORT
	s := natstest.RunServer(&opts)
	t.Cleanup(s.Shutdown)
	if !s.ReadyForConnections(2 * time.Second) {
		t.Fatal("nats-server not ready")
	}
	return fmt.Sprintf("nats://%s", s.Addr().String())
}

// benchSrv is a tiny benchmark.BenchmarkServer used by the integration tests.
// It echoes the request payload and reports which replica handled the call
// via the response's server_id field. A shared counter records call counts
// for load-balancing assertions.
type benchSrv struct {
	benchmark.UnimplementedBenchmarkServer
	id      string
	handled *int64 // optional, atomic-incremented per call
}

func (b *benchSrv) Execute(ctx context.Context, req *benchmark.BenchmarkRequest) (*benchmark.BenchmarkResponse, error) {
	if b.handled != nil {
		atomic.AddInt64(b.handled, 1)
	}
	return &benchmark.BenchmarkResponse{ServerId: b.id, Payload: req.Payload}, nil
}

// TestIntegration_UnaryRoundTrip: client → embedded NATS → server → reply.
// Exercises the full Option-2 unary path (Client.invoker → nc.RequestWithContext
// on the client side; QueueSubscribe → serverUnaryRequestHandler → msg.Respond
// on the server side) and catches any wire-format drift between the two halves.
func TestIntegration_UnaryRoundTrip(t *testing.T) {
	url := runEmbeddedNATS(t)

	// Server.
	scn, err := nats.Connect(url)
	require.NoError(t, err)
	defer scn.Close()
	srv := NewServer(scn, "svc-1")
	benchmark.RegisterBenchmarkServer(srv, &benchSrv{id: "svc-1"})
	t.Cleanup(srv.Stop)

	// Client.
	ccn, err := nats.Connect(url)
	require.NoError(t, err)
	defer ccn.Close()
	cli := benchmark.NewBenchmarkClient(NewClient(ccn, "svc-1", "client-1"))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := cli.Execute(ctx, &benchmark.BenchmarkRequest{
		ServerId: "svc-1",
		Payload:  []byte("ping"),
	})
	require.NoError(t, err)
	assert.Equal(t, "svc-1", resp.ServerId)
	assert.Equal(t, []byte("ping"), resp.Payload)
}

// TestIntegration_UnaryLoadBalances: three server replicas register the same
// (nid, service) so they share a queue group. With Option 2 every RPC is a
// single request/reply message, so every call must succeed and load is spread
// across replicas. Before Option 2 this exact setup is what caused the
// benchmark's DeadlineExceeded storm.
func TestIntegration_UnaryLoadBalances(t *testing.T) {
	url := runEmbeddedNATS(t)

	const replicas = 3
	counters := make([]int64, replicas)
	var stops []func()
	for i := 0; i < replicas; i++ {
		nc, err := nats.Connect(url)
		require.NoError(t, err)
		srv := NewServer(nc, "svc-lb")
		benchmark.RegisterBenchmarkServer(srv, &benchSrv{
			id:      fmt.Sprintf("replica-%d", i),
			handled: &counters[i],
		})
		stops = append(stops, func() { srv.Stop(); nc.Close() })
	}
	defer func() {
		for _, stop := range stops {
			stop()
		}
	}()

	ccn, err := nats.Connect(url)
	require.NoError(t, err)
	defer ccn.Close()
	cli := benchmark.NewBenchmarkClient(NewClient(ccn, "svc-lb", "client-lb"))

	const calls = 60
	for i := 0; i < calls; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := cli.Execute(ctx, &benchmark.BenchmarkRequest{
			ServerId: "svc-lb",
			Payload:  []byte("x"),
		})
		cancel()
		require.NoError(t, err, "call %d failed", i)
	}

	var total int64
	for _, c := range counters {
		total += atomic.LoadInt64(&c)
	}
	assert.Equal(t, int64(calls), total, "every call should be handled exactly once across replicas")

	hit := 0
	for _, c := range counters {
		if atomic.LoadInt64(&c) > 0 {
			hit++
		}
	}
	assert.Greater(t, hit, 1, "expected NATS queue group to load-balance across multiple replicas; got %v", counters)
}

// TestIntegration_UnaryConcurrentClients fires concurrent unary RPCs to
// expose any state-sharing bug between calls (which the per-RPC subscription
// in the old code path was prone to).
func TestIntegration_UnaryConcurrentClients(t *testing.T) {
	url := runEmbeddedNATS(t)

	scn, err := nats.Connect(url)
	require.NoError(t, err)
	defer scn.Close()
	srv := NewServer(scn, "svc-conc")
	benchmark.RegisterBenchmarkServer(srv, &benchSrv{id: "svc-conc"})
	t.Cleanup(srv.Stop)

	ccn, err := nats.Connect(url)
	require.NoError(t, err)
	defer ccn.Close()
	cli := benchmark.NewBenchmarkClient(NewClient(ccn, "svc-conc", "client-conc"))

	const workers, perWorker = 8, 25
	var wg sync.WaitGroup
	errs := make(chan error, workers*perWorker)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				payload := []byte(fmt.Sprintf("w%d-i%d", w, i))
				resp, err := cli.Execute(ctx, &benchmark.BenchmarkRequest{Payload: payload})
				cancel()
				if err != nil {
					errs <- err
					return
				}
				if string(resp.Payload) != string(payload) {
					errs <- fmt.Errorf("payload mismatch: got %q want %q", resp.Payload, payload)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent call failed: %v", err)
	}
}

// TestIntegration_ServerUnaryHandler_Wire focuses on the server side only.
// It publishes a hand-built Request_Unary against a queue-subscribed server
// and asserts the reply on its inbox is a well-formed Response_Unary —
// catching any framing bugs in serverUnaryRequestHandler / msg.Respond
// independently of the client implementation.
func TestIntegration_ServerUnaryHandler_Wire(t *testing.T) {
	url := runEmbeddedNATS(t)

	scn, err := nats.Connect(url)
	require.NoError(t, err)
	defer scn.Close()
	srv := NewServer(scn, "svc-wire")
	benchmark.RegisterBenchmarkServer(srv, &benchSrv{id: "svc-wire"})
	t.Cleanup(srv.Stop)

	// Independent NATS connection acting as a raw client.
	raw, err := nats.Connect(url)
	require.NoError(t, err)
	defer raw.Close()

	// Build a Request_Unary by hand and send to the server's subject.
	body, err := proto.Marshal(&benchmark.BenchmarkRequest{Payload: []byte("raw")})
	require.NoError(t, err)
	wire, err := proto.Marshal(&nrpc.Request{Type: &nrpc.Request_Unary{Unary: &nrpc.UnaryRequest{
		Method: "/benchmark.Benchmark/Execute",
		Data:   body,
	}}})
	require.NoError(t, err)

	msg, err := raw.Request("nrpc.svc-wire.benchmark.Benchmark.Execute", wire, 2*time.Second)
	require.NoError(t, err, "no reply from server")

	resp := &nrpc.Response{}
	require.NoError(t, proto.Unmarshal(msg.Data, resp))
	u := resp.GetUnary()
	require.NotNil(t, u, "expected Response_Unary on the wire, got %T", resp.Type)

	// Status should be OK (code 0).
	if u.Status != nil {
		assert.Equal(t, int32(codes.OK), u.Status.Code, "got status %v", status.FromProto(u.Status).Err())
	}

	out := &benchmark.BenchmarkResponse{}
	require.NoError(t, proto.Unmarshal(u.Data, out))
	assert.Equal(t, "svc-wire", out.ServerId)
	assert.Equal(t, []byte("raw"), out.Payload)
}

// TestIntegration_ServerUnaryHandler_Error: when the gRPC handler returns a
// non-OK status, it must surface in the Response_Unary status field rather
// than as a Data payload.
func TestIntegration_ServerUnaryHandler_Error(t *testing.T) {
	url := runEmbeddedNATS(t)

	scn, err := nats.Connect(url)
	require.NoError(t, err)
	defer scn.Close()
	srv := NewServer(scn, "svc-err")
	benchmark.RegisterBenchmarkServer(srv, &errSrv{})
	t.Cleanup(srv.Stop)

	ccn, err := nats.Connect(url)
	require.NoError(t, err)
	defer ccn.Close()
	cli := benchmark.NewBenchmarkClient(NewClient(ccn, "svc-err", "client-err"))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = cli.Execute(ctx, &benchmark.BenchmarkRequest{})
	st, ok := status.FromError(err)
	if assert.True(t, ok) {
		assert.Equal(t, codes.FailedPrecondition, st.Code())
		assert.Equal(t, "intentional", st.Message())
	}
}

type errSrv struct {
	benchmark.UnimplementedBenchmarkServer
}

func (errSrv) Execute(ctx context.Context, req *benchmark.BenchmarkRequest) (*benchmark.BenchmarkResponse, error) {
	return nil, status.Error(codes.FailedPrecondition, "intentional")
}

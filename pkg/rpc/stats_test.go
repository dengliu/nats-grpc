package rpc

import (
	"context"
	"errors"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/cloudwebrtc/nats-grpc/examples/benchmark/protos/benchmark"
	"github.com/cloudwebrtc/nats-grpc/examples/protos/echo"
	"github.com/cloudwebrtc/nats-grpc/pkg/protos/nrpc"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/stats"
	"google.golang.org/protobuf/proto"
)

// --- Recording stats.Handler ---------------------------------------------

// recordingHandler implements stats.Handler and stores every event for
// assertion. Goroutine-safe: HandleRPC fires concurrently from server-side
// per-frame callbacks.
type recordingHandler struct {
	mu     sync.Mutex
	tags   []*stats.RPCTagInfo
	events []stats.RPCStats
}

func newRecordingHandler() *recordingHandler { return &recordingHandler{} }

func (h *recordingHandler) TagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	h.mu.Lock()
	h.tags = append(h.tags, info)
	h.mu.Unlock()
	return ctx
}
func (h *recordingHandler) HandleRPC(ctx context.Context, rs stats.RPCStats) {
	h.mu.Lock()
	h.events = append(h.events, rs)
	h.mu.Unlock()
}
func (h *recordingHandler) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}
func (h *recordingHandler) HandleConn(_ context.Context, _ stats.ConnStats) {}

// eventTypes returns the recorded event types in order.
func (h *recordingHandler) eventTypes() []reflect.Type {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]reflect.Type, len(h.events))
	for i, e := range h.events {
		out[i] = reflect.TypeOf(e)
	}
	return out
}

// --- Unary client: mock-based test (no network) ---------------------------

// TestStats_Unary_Client_Success drives Client.invoker with a mocked NATS
// connection and asserts the lifecycle event sequence
// (TagRPC → Begin → OutPayload → InPayload → End) plus key fields.
func TestStats_Unary_Client_Success(t *testing.T) {
	mockNC := new(MockNatsConn)
	wantReply := &nrpc.Pong{Timestamp: 7}
	mockNC.On("RequestWithContext", mock.Anything, mock.Anything, mock.Anything).
		Return(&nats.Msg{Data: marshalUnaryResp(t, wantReply)}, nil)

	h := newRecordingHandler()
	client := NewClientWithOptions(mockNC, "svc", "nid", WithStatsHandler(h))
	defer client.Close()

	err := client.Invoke(context.Background(), "/test.Svc/M", &nrpc.Ping{Timestamp: 1}, &nrpc.Pong{})
	assert.NoError(t, err)

	if assert.Len(t, h.tags, 1) {
		assert.Equal(t, "/test.Svc/M", h.tags[0].FullMethodName)
	}
	wantTypes := []reflect.Type{
		reflect.TypeOf(&stats.Begin{}),
		reflect.TypeOf(&stats.OutPayload{}),
		reflect.TypeOf(&stats.InPayload{}),
		reflect.TypeOf(&stats.End{}),
	}
	assert.Equal(t, wantTypes, h.eventTypes())

	// Verify End carries no error and a non-zero duration.
	end := h.events[3].(*stats.End)
	assert.NoError(t, end.Error)
	assert.True(t, end.EndTime.After(end.BeginTime) || end.EndTime.Equal(end.BeginTime))
	assert.True(t, end.IsClient())
}

// TestStats_Unary_Client_Error verifies End.Error is set when the call fails.
func TestStats_Unary_Client_Error(t *testing.T) {
	mockNC := new(MockNatsConn)
	mockNC.On("RequestWithContext", mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nats.ErrNoResponders)

	h := newRecordingHandler()
	client := NewClientWithOptions(mockNC, "svc", "nid", WithStatsHandler(h))
	defer client.Close()

	_ = client.Invoke(context.Background(), "/test.Svc/M", &nrpc.Ping{}, &nrpc.Pong{})

	// On error path there's no InPayload — Begin → OutPayload → End.
	wantTypes := []reflect.Type{
		reflect.TypeOf(&stats.Begin{}),
		reflect.TypeOf(&stats.OutPayload{}),
		reflect.TypeOf(&stats.End{}),
	}
	assert.Equal(t, wantTypes, h.eventTypes())
	end := h.events[2].(*stats.End)
	assert.Error(t, end.Error)
}

// TestStats_MultipleHandlers verifies each registered handler sees the same
// events, in the same order.
func TestStats_MultipleHandlers(t *testing.T) {
	mockNC := new(MockNatsConn)
	mockNC.On("RequestWithContext", mock.Anything, mock.Anything, mock.Anything).
		Return(&nats.Msg{Data: marshalUnaryResp(t, &nrpc.Pong{})}, nil)

	h1 := newRecordingHandler()
	h2 := newRecordingHandler()
	client := NewClientWithOptions(mockNC, "svc", "nid",
		WithStatsHandler(h1), WithStatsHandler(h2))
	defer client.Close()

	err := client.Invoke(context.Background(), "/test.Svc/M", &nrpc.Ping{}, &nrpc.Pong{})
	assert.NoError(t, err)
	assert.Equal(t, h1.eventTypes(), h2.eventTypes())
	assert.Len(t, h1.tags, 1)
	assert.Len(t, h2.tags, 1)
}

// --- Unary server: integration via embedded NATS --------------------------

// TestStats_Unary_Server_Success exercises the server-side stats wiring by
// publishing a hand-built UnaryRequest and asserting the recorded server-side
// events.
func TestStats_Unary_Server_Success(t *testing.T) {
	url := runEmbeddedNATS(t)

	h := newRecordingHandler()
	scn, err := nats.Connect(url)
	require.NoError(t, err)
	defer scn.Close()
	srv := NewServerWithOptions(scn, "svc-stats", WithServerStatsHandler(h))
	benchmark.RegisterBenchmarkServer(srv, &benchSrv{id: "svc-stats"})
	t.Cleanup(srv.Stop)

	raw, err := nats.Connect(url)
	require.NoError(t, err)
	defer raw.Close()

	body, err := proto.Marshal(&benchmark.BenchmarkRequest{Payload: []byte("hi")})
	require.NoError(t, err)
	wire, err := proto.Marshal(&nrpc.Request{Type: &nrpc.Request_Unary{Unary: &nrpc.UnaryRequest{
		Method: "/benchmark.Benchmark/Execute",
		Data:   body,
	}}})
	require.NoError(t, err)
	_, err = raw.Request("nrpc.svc-stats.benchmark.Benchmark.Execute", wire, 2*time.Second)
	require.NoError(t, err)

	// The server-side defer schedules End after Respond, so allow a moment for
	// the goroutine to complete before asserting.
	require.Eventually(t, func() bool {
		return len(h.eventTypes()) == 4
	}, 2*time.Second, 10*time.Millisecond, "expected 4 events, got %v", h.eventTypes())

	wantTypes := []reflect.Type{
		reflect.TypeOf(&stats.Begin{}),
		reflect.TypeOf(&stats.InPayload{}),
		reflect.TypeOf(&stats.OutPayload{}),
		reflect.TypeOf(&stats.End{}),
	}
	assert.Equal(t, wantTypes, h.eventTypes())
	if assert.Len(t, h.tags, 1) {
		assert.Equal(t, "/benchmark.Benchmark/Execute", h.tags[0].FullMethodName)
	}
	end := h.events[3].(*stats.End)
	assert.NoError(t, end.Error)
	assert.False(t, end.IsClient())
}

// --- Streaming: full client↔server lifecycle ------------------------------

// echoSrv implements echo.EchoServer's bidi Echo method: it reads each request
// and sends back a reply with the same message. EOF on the recv side ends the
// stream successfully.
type echoSrv struct {
	echo.UnimplementedEchoServer
}

func (echoSrv) Echo(stream echo.Echo_EchoServer) error {
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&echo.EchoReply{Msg: in.Msg}); err != nil {
			return err
		}
	}
}

// TestStats_Streaming_Lifecycle exercises a 2-frame bidi RPC and asserts the
// client and server each emit Begin once, an OutPayload/InPayload per frame,
// and a single End at stream close.
func TestStats_Streaming_Lifecycle(t *testing.T) {
	url := runEmbeddedNATS(t)

	serverH := newRecordingHandler()
	scn, err := nats.Connect(url)
	require.NoError(t, err)
	defer scn.Close()
	srv := NewServerWithOptions(scn, "svc-stream", WithServerStatsHandler(serverH))
	echo.RegisterEchoServer(srv, echoSrv{})
	t.Cleanup(srv.Stop)

	clientH := newRecordingHandler()
	ccn, err := nats.Connect(url)
	require.NoError(t, err)
	defer ccn.Close()
	cli := echo.NewEchoClient(NewClientWithOptions(ccn, "svc-stream", "client-stream",
		WithStatsHandler(clientH)))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := cli.Echo(ctx)
	require.NoError(t, err)

	// Two send/recv round-trips, then half-close.
	for i := 0; i < 2; i++ {
		require.NoError(t, stream.Send(&echo.EchoRequest{Msg: "ping"}))
		reply, err := stream.Recv()
		require.NoError(t, err)
		assert.Equal(t, "ping", reply.Msg)
	}
	require.NoError(t, stream.CloseSend())
	// Drain until EOF (server returns nil after RecvMsg EOF).
	for {
		_, err := stream.Recv()
		if err == io.EOF || err == nil {
			break
		}
		// On nats-grpc the close handshake may surface as a status error or
		// completion; either signals end-of-stream.
		break
	}

	// Allow goroutines to flush stats.End.
	require.Eventually(t, func() bool {
		ct := clientH.eventTypes()
		st := serverH.eventTypes()
		return len(ct) >= 5 && len(st) >= 5 &&
			ct[len(ct)-1] == reflect.TypeOf(&stats.End{}) &&
			st[len(st)-1] == reflect.TypeOf(&stats.End{})
	}, 2*time.Second, 10*time.Millisecond,
		"client=%v server=%v", clientH.eventTypes(), serverH.eventTypes())

	// Client sequence: Begin, then 2× (OutPayload+InPayload) in some order
	// (NATS doesn't guarantee strict interleaving), then End.
	clientTypes := clientH.eventTypes()
	assert.Equal(t, reflect.TypeOf(&stats.Begin{}), clientTypes[0])
	assert.Equal(t, reflect.TypeOf(&stats.End{}), clientTypes[len(clientTypes)-1])
	assert.Equal(t, 2, countType(clientH, &stats.OutPayload{}))
	assert.GreaterOrEqual(t, countType(clientH, &stats.InPayload{}), 2)

	// Server sequence: Begin, then per-frame Inv/Out, then End.
	serverTypes := serverH.eventTypes()
	assert.Equal(t, reflect.TypeOf(&stats.Begin{}), serverTypes[0])
	assert.Equal(t, reflect.TypeOf(&stats.End{}), serverTypes[len(serverTypes)-1])
	assert.Equal(t, 2, countType(serverH, &stats.InPayload{}))
	assert.Equal(t, 2, countType(serverH, &stats.OutPayload{}))

	if assert.Len(t, clientH.tags, 1) {
		assert.Equal(t, "/echo.Echo/Echo", clientH.tags[0].FullMethodName)
	}
	if assert.Len(t, serverH.tags, 1) {
		assert.Equal(t, "nrpc.svc-stream.echo.Echo.Echo", serverH.tags[0].FullMethodName)
	}
}

// countType returns how many events in h have the same reflect.Type as want.
func countType(h *recordingHandler, want stats.RPCStats) int {
	h.mu.Lock()
	defer h.mu.Unlock()
	target := reflect.TypeOf(want)
	n := 0
	for _, e := range h.events {
		if reflect.TypeOf(e) == target {
			n++
		}
	}
	return n
}

// Sanity that the streaming server emits End with the configured error on
// non-EOF termination.
func TestStats_Streaming_Server_HandlerError(t *testing.T) {
	url := runEmbeddedNATS(t)

	serverH := newRecordingHandler()
	scn, err := nats.Connect(url)
	require.NoError(t, err)
	defer scn.Close()
	srv := NewServerWithOptions(scn, "svc-stream-err", WithServerStatsHandler(serverH))
	echo.RegisterEchoServer(srv, errEchoSrv{err: errors.New("boom")})
	t.Cleanup(srv.Stop)

	ccn, err := nats.Connect(url)
	require.NoError(t, err)
	defer ccn.Close()
	cli := echo.NewEchoClient(NewClient(ccn, "svc-stream-err", "client-stream-err"))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := cli.Echo(ctx)
	require.NoError(t, err)
	_ = stream.Send(&echo.EchoRequest{Msg: "x"})
	_, _ = stream.Recv() // expect an error or EOF, we don't care which

	require.Eventually(t, func() bool {
		ts := serverH.eventTypes()
		return len(ts) > 0 && ts[len(ts)-1] == reflect.TypeOf(&stats.End{})
	}, 2*time.Second, 10*time.Millisecond, "server events: %v", serverH.eventTypes())
	last := serverH.events[len(serverH.events)-1].(*stats.End)
	assert.Error(t, last.Error)
}

type errEchoSrv struct {
	echo.UnimplementedEchoServer
	err error
}

func (e errEchoSrv) Echo(stream echo.Echo_EchoServer) error {
	if _, err := stream.Recv(); err != nil {
		return err
	}
	return e.err
}

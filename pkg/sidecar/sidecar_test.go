package sidecar

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwebrtc/nats-grpc/examples/protos/echo"
	pb "github.com/cloudwebrtc/nats-grpc/pkg/protos/sidecar"
	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// --- test scaffolding -------------------------------------------------------

func runEmbeddedNATS(t *testing.T) string {
	t.Helper()
	opts := natstest.DefaultTestOptions
	opts.Port = natsserver.RANDOM_PORT
	srv := natstest.RunServer(&opts)
	t.Cleanup(srv.Shutdown)
	if !srv.ReadyForConnections(2 * time.Second) {
		t.Fatal("nats-server not ready")
	}
	return fmt.Sprintf("nats://%s", srv.Addr().String())
}

// startSidecar runs a sidecar on OS-picked ports and returns it. Cleanup is
// registered with t.Cleanup so tests can chain without manual teardown.
func startSidecar(t *testing.T, natsURL, nid string) *Sidecar {
	t.Helper()
	sc := New(Config{
		NATSURL:       natsURL,
		EgressAddr:    "127.0.0.1:0",
		AdminAddr:     "127.0.0.1:0",
		HTTPAdminAddr: "127.0.0.1:0",
		Nid:           nid,
	})
	require.NoError(t, sc.Start(context.Background()))
	t.Cleanup(func() { _ = sc.Close() })
	return sc
}

// echoSrv is a tiny Echo implementation used as the upstream gRPC backend
// for ingress tests. It records the metadata it observed (post-strip) and
// what svcid the call appears to have targeted.
type echoSrv struct {
	echo.UnimplementedEchoServer
	id           string
	seenMetaMu   sync.Mutex
	lastMD       metadata.MD
	callCount    atomic.Int64
}

func (e *echoSrv) SayHello(ctx context.Context, req *echo.HelloRequest) (*echo.HelloReply, error) {
	e.callCount.Add(1)
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		e.seenMetaMu.Lock()
		e.lastMD = md.Copy()
		e.seenMetaMu.Unlock()
	}
	return &echo.HelloReply{Msg: e.id + ":" + req.Msg}, nil
}

func (e *echoSrv) Echo(stream echo.Echo_EchoServer) error {
	e.callCount.Add(1)
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := stream.Send(&echo.EchoReply{Msg: e.id + ":" + req.Msg}); err != nil {
			return err
		}
	}
}

// startUpstreamEcho runs a real gRPC server (the local app) and returns
// its loopback address.
func startUpstreamEcho(t *testing.T, impl *echoSrv) string {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	srv := grpc.NewServer()
	echo.RegisterEchoServer(srv, impl)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.GracefulStop)
	return lis.Addr().String()
}

// registerApp drives the admin Register stream end-to-end and returns the
// sidecar-assigned nid plus a release func that closes the stream.
func registerApp(t *testing.T, adminAddr, svcid, upstream string, services []string) (string, func()) {
	t.Helper()
	conn, err := grpc.NewClient(adminAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	cli := pb.NewSidecarAdminClient(conn)
	ctx, cancel := context.WithCancel(context.Background())
	stream, err := cli.Register(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&pb.RegisterRequest{Type: &pb.RegisterRequest_Init{
		Init: &pb.Init{Svcid: svcid, Upstream: upstream, Services: services},
	}}))
	resp, err := stream.Recv()
	require.NoError(t, err)
	reg := resp.GetRegistered()
	require.NotNil(t, reg, "expected Registered, got %T (err=%v)", resp.GetType(), resp.GetError())

	release := func() {
		cancel()
		_ = stream.CloseSend()
	}
	return reg.Nid, release
}

// dialEgress dials the sidecar's egress port with insecure credentials and
// the proto-default codec — the local app uses normal gRPC.
func dialEgress(t *testing.T, addr string) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// --- tests ------------------------------------------------------------------

// TestEgress_RequiresSvcIDHeader pins the contract that x-nats-svcid is
// mandatory. Without it the sidecar must return InvalidArgument before
// any NATS publish.
func TestEgress_RequiresSvcIDHeader(t *testing.T) {
	url := runEmbeddedNATS(t)
	sc := startSidecar(t, url, "egress-1")

	conn := dialEgress(t, sc.EgressAddr())
	cli := echo.NewEchoClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := cli.SayHello(ctx, &echo.HelloRequest{Msg: "hi"})
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
	assert.Contains(t, st.Message(), "x-nats-svcid")
}

// TestEgress_StreamingRequiresTargetNid pins the contract for streaming.
func TestEgress_StreamingRequiresTargetNid(t *testing.T) {
	url := runEmbeddedNATS(t)
	sc := startSidecar(t, url, "egress-2")

	conn := dialEgress(t, sc.EgressAddr())
	cli := echo.NewEchoClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx,
		"x-nats-svcid", "anything",
		"x-nats-mode", "streaming",
	)
	stream, err := cli.Echo(ctx)
	require.NoError(t, err)
	// Recv (or any send + recv) materializes the error returned by the
	// handler — the InvalidArgument from the sidecar.
	_ = stream.Send(&echo.EchoRequest{Msg: "x"})
	_, err = stream.Recv()
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.InvalidArgument, st.Code())
	assert.Contains(t, st.Message(), "x-nats-target-nid")
}

// TestEgress_BadModeIsInvalidArgument covers x-nats-mode validation.
func TestEgress_BadModeIsInvalidArgument(t *testing.T) {
	url := runEmbeddedNATS(t)
	sc := startSidecar(t, url, "egress-3")

	conn := dialEgress(t, sc.EgressAddr())
	cli := echo.NewEchoClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx,
		"x-nats-svcid", "anything",
		"x-nats-mode", "telepathy",
	)
	_, err := cli.SayHello(ctx, &echo.HelloRequest{Msg: "x"})
	st, _ := status.FromError(err)
	assert.Equal(t, codes.InvalidArgument, st.Code())
}

// TestEndToEnd_UnaryWithSidecarBothSides is the full path:
//
//	local client → egress sidecar → NATS → ingress sidecar → upstream gRPC
//
// It also pins the routing-strip contract: x-nats-* must NOT reach the
// backend handler.
func TestEndToEnd_UnaryWithSidecarBothSides(t *testing.T) {
	url := runEmbeddedNATS(t)
	egress := startSidecar(t, url, "egress-side")
	ingress := startSidecar(t, url, "ingress-side")

	upstream := &echoSrv{id: "backend-A"}
	upstreamAddr := startUpstreamEcho(t, upstream)
	_, release := registerApp(t, ingress.AdminAddr(), "svcA", upstreamAddr, []string{"echo.Echo"})
	defer release()

	conn := dialEgress(t, egress.EgressAddr())
	cli := echo.NewEchoClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx,
		"x-nats-svcid", "svcA",
		"x-app-trace", "preserved",
	)

	resp, err := cli.SayHello(ctx, &echo.HelloRequest{Msg: "hello"})
	require.NoError(t, err)
	assert.Equal(t, "backend-A:hello", resp.Msg)

	// Reserved x-nats-* headers are stripped at the egress; non-reserved
	// metadata is forwarded.
	upstream.seenMetaMu.Lock()
	got := upstream.lastMD.Copy()
	upstream.seenMetaMu.Unlock()
	assert.Empty(t, got.Get("x-nats-svcid"), "x-nats-svcid leaked to upstream")
	assert.Empty(t, got.Get("x-nats-mode"), "x-nats-mode leaked to upstream")
	assert.Equal(t, []string{"preserved"}, got.Get("x-app-trace"))
}

// TestEndToEnd_DynamicSvcIDPerCall is the headline use case: the same
// gRPC stub makes two consecutive calls that route to two different
// backends purely via the x-nats-svcid header value.
func TestEndToEnd_DynamicSvcIDPerCall(t *testing.T) {
	url := runEmbeddedNATS(t)
	egress := startSidecar(t, url, "egress-dyn")
	ingressA := startSidecar(t, url, "ingress-A")
	ingressB := startSidecar(t, url, "ingress-B")

	upstreamA := &echoSrv{id: "A"}
	addrA := startUpstreamEcho(t, upstreamA)
	_, relA := registerApp(t, ingressA.AdminAddr(), "serviceid_1", addrA, []string{"echo.Echo"})
	defer relA()

	upstreamB := &echoSrv{id: "B"}
	addrB := startUpstreamEcho(t, upstreamB)
	_, relB := registerApp(t, ingressB.AdminAddr(), "serviceid_2", addrB, []string{"echo.Echo"})
	defer relB()

	conn := dialEgress(t, egress.EgressAddr())
	cli := echo.NewEchoClient(conn)

	// Call 1 → serviceid_1
	{
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		ctx = metadata.AppendToOutgoingContext(ctx, "x-nats-svcid", "serviceid_1")
		resp, err := cli.SayHello(ctx, &echo.HelloRequest{Msg: "ping"})
		cancel()
		require.NoError(t, err)
		assert.Equal(t, "A:ping", resp.Msg)
	}
	// Call 2 → serviceid_2 (same stub, different metadata)
	{
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		ctx = metadata.AppendToOutgoingContext(ctx, "x-nats-svcid", "serviceid_2")
		resp, err := cli.SayHello(ctx, &echo.HelloRequest{Msg: "ping"})
		cancel()
		require.NoError(t, err)
		assert.Equal(t, "B:ping", resp.Msg)
	}

	assert.Equal(t, int64(1), upstreamA.callCount.Load(), "backend A should have seen exactly one call")
	assert.Equal(t, int64(1), upstreamB.callCount.Load(), "backend B should have seen exactly one call")
}

// TestEndToEnd_ConcurrentCallsRouteByInbox is the explicit answer to "if
// many local clients use the same sidecar, how does the sidecar dispatch
// responses?" Two simultaneous calls land on different backends; both
// goroutines must wake up correctly via per-call inbox demux, regardless
// of the shared sidecar nid.
func TestEndToEnd_ConcurrentCallsRouteByInbox(t *testing.T) {
	url := runEmbeddedNATS(t)
	egress := startSidecar(t, url, "egress-conc")
	ingA := startSidecar(t, url, "in-A")
	ingB := startSidecar(t, url, "in-B")

	upA := &echoSrv{id: "A"}
	upB := &echoSrv{id: "B"}
	addrA := startUpstreamEcho(t, upA)
	addrB := startUpstreamEcho(t, upB)
	_, relA := registerApp(t, ingA.AdminAddr(), "s1", addrA, []string{"echo.Echo"})
	defer relA()
	_, relB := registerApp(t, ingB.AdminAddr(), "s2", addrB, []string{"echo.Echo"})
	defer relB()

	conn := dialEgress(t, egress.EgressAddr())
	cli := echo.NewEchoClient(conn)

	const perSvcid = 20
	var wg sync.WaitGroup
	errs := make(chan error, perSvcid*2)
	expect := func(svcid, prefix string) {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		ctx = metadata.AppendToOutgoingContext(ctx, "x-nats-svcid", svcid)
		resp, err := cli.SayHello(ctx, &echo.HelloRequest{Msg: "hi"})
		if err != nil {
			errs <- err
			return
		}
		if resp.Msg != prefix+":hi" {
			errs <- fmt.Errorf("svcid %s: want %q got %q", svcid, prefix+":hi", resp.Msg)
		}
	}
	for i := 0; i < perSvcid; i++ {
		wg.Add(2)
		go expect("s1", "A")
		go expect("s2", "B")
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("concurrent call mismatch: %v", err)
	}
	assert.Equal(t, int64(perSvcid), upA.callCount.Load())
	assert.Equal(t, int64(perSvcid), upB.callCount.Load())
}

// TestEndToEnd_Streaming exercises the streaming dispatch path:
// x-nats-mode: streaming + x-nats-target-nid (the ingress sidecar's nid).
func TestEndToEnd_Streaming(t *testing.T) {
	url := runEmbeddedNATS(t)
	egress := startSidecar(t, url, "egress-stream")
	ingress := startSidecar(t, url, "ingress-stream")

	upstream := &echoSrv{id: "S"}
	addr := startUpstreamEcho(t, upstream)
	nid, release := registerApp(t, ingress.AdminAddr(), "svcS", addr, []string{"echo.Echo"})
	defer release()
	// Nid returned by Register should equal the ingress sidecar's own nid.
	assert.Equal(t, ingress.Nid(), nid)

	conn := dialEgress(t, egress.EgressAddr())
	cli := echo.NewEchoClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx,
		"x-nats-svcid", "svcS",
		"x-nats-mode", "streaming",
		"x-nats-target-nid", nid,
	)
	stream, err := cli.Echo(ctx)
	require.NoError(t, err)

	const n = 10
	go func() {
		for i := 0; i < n; i++ {
			_ = stream.Send(&echo.EchoRequest{Msg: fmt.Sprintf("m%d", i)})
		}
		_ = stream.CloseSend()
	}()
	for i := 0; i < n; i++ {
		reply, err := stream.Recv()
		require.NoError(t, err, "recv %d", i)
		assert.Equal(t, fmt.Sprintf("S:m%d", i), reply.Msg)
	}
	// EOF on the last recv.
	_, err = stream.Recv()
	if err != nil && err != io.EOF {
		// The streaming pump synthesizes End{OK}; the gRPC layer might
		// surface this as nil-then-EOF or just EOF — either is fine.
		t.Logf("trailing recv: %v", err)
	}
}

// TestAdmin_AppDeathDeregisters verifies that when the admin Register
// stream drops, the sidecar tears down the corresponding NATS
// subscriptions. After teardown, a unary call to that svcid must fail
// with Unavailable (no subscribers → nats.ErrNoResponders).
func TestAdmin_AppDeathDeregisters(t *testing.T) {
	url := runEmbeddedNATS(t)
	egress := startSidecar(t, url, "egress-deathd")
	ingress := startSidecar(t, url, "ingress-deathd")

	upstream := &echoSrv{id: "X"}
	addr := startUpstreamEcho(t, upstream)
	_, release := registerApp(t, ingress.AdminAddr(), "svcDie", addr, []string{"echo.Echo"})

	// Sanity: while registered, calls succeed.
	conn := dialEgress(t, egress.EgressAddr())
	cli := echo.NewEchoClient(conn)
	{
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		ctx = metadata.AppendToOutgoingContext(ctx, "x-nats-svcid", "svcDie")
		_, err := cli.SayHello(ctx, &echo.HelloRequest{Msg: "alive"})
		cancel()
		require.NoError(t, err)
	}

	// Now kill the registration. Two observable effects must follow:
	//   1. ingress.registrations drops the entry (white-box state).
	//   2. Future calls to that svcid fail (black-box behaviour).
	//
	// On modern NATS servers (>=2.2) #2 surfaces as codes.Unavailable
	// via nats.ErrNoResponders. On the older nats-server 2.1.x bundled
	// with this repo's tests the no-responders 503 isn't emitted, so
	// the call simply hangs to ctx deadline (codes.DeadlineExceeded).
	// Either failure mode proves the sidecar is no longer answering.
	release()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		ingress.mu.Lock()
		regCount := len(ingress.registrations)
		ingress.mu.Unlock()
		if regCount == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	ingress.mu.Lock()
	finalCount := len(ingress.registrations)
	ingress.mu.Unlock()
	require.Zero(t, finalCount, "ingress.registrations did not drain after Register stream closed")

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	ctx = metadata.AppendToOutgoingContext(ctx, "x-nats-svcid", "svcDie")
	_, err := cli.SayHello(ctx, &echo.HelloRequest{Msg: "dead?"})
	require.Error(t, err, "expected call to fail after deregister")
	st, _ := status.FromError(err)
	if st.Code() != codes.Unavailable && st.Code() != codes.DeadlineExceeded {
		t.Fatalf("expected Unavailable or DeadlineExceeded after deregister, got %v", st.Code())
	}
}

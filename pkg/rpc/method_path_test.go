package rpc

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cloudwebrtc/nats-grpc/examples/protos/echo"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/stats"
)

// fullMethodCapturingSrv records grpc.MethodFromServerStream() inside the
// Echo handler so the test can verify what gRPC interceptor code observes.
type fullMethodCapturingSrv struct {
	echo.UnimplementedEchoServer
	got     chan string
	gotOnce sync.Once
}

func (s *fullMethodCapturingSrv) Echo(stream echo.Echo_EchoServer) error {
	m, _ := grpc.MethodFromServerStream(stream)
	s.gotOnce.Do(func() { s.got <- m })
	for {
		_, err := stream.Recv()
		if err != nil {
			return nil
		}
	}
}

// TestIntegration_StreamingFullMethodIsGRPCPath asserts two consequences of
// fixing the nrpc.Call.Method inconsistency:
//
//  1. The handler observes a gRPC-style FullMethod (`/echo.Echo/Echo`) via
//     grpc.MethodFromServerStream — proxy/handler.go and interceptors both
//     depend on this format.
//  2. stats.RPCTagInfo.FullMethodName is the same gRPC path (matching the
//     unary fast path, which already tagged correctly via UnaryRequest.Method).
//
// Before the fix, both observed the NATS subject string
// (`nrpc.<nid>.echo.Echo.Echo`) — breaking otelgrpc's span/metric naming
// for streaming RPCs.
func TestIntegration_StreamingFullMethodIsGRPCPath(t *testing.T) {
	url := runEmbeddedNATS(t)

	h := newRecordingHandler()
	scn, err := nats.Connect(url)
	require.NoError(t, err)
	defer scn.Close()
	srv := NewServerWithOptions(scn, "method-path", WithServerStatsHandler(h))
	impl := &fullMethodCapturingSrv{got: make(chan string, 1)}
	echo.RegisterEchoServer(srv, impl)
	t.Cleanup(srv.Stop)

	ccn, err := nats.Connect(url)
	require.NoError(t, err)
	defer ccn.Close()
	cli := echo.NewEchoClient(NewClient(ccn, "method-path", "client-mp"))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := cli.Echo(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&echo.EchoRequest{Msg: "x"}))

	// Handler should observe the gRPC method path.
	select {
	case got := <-impl.got:
		assert.Equal(t, "/echo.Echo/Echo", got,
			"grpc.MethodFromServerStream should expose the gRPC method path, not the NATS subject")
	case <-time.After(2 * time.Second):
		t.Fatal("server handler never reached Echo")
	}

	_ = stream.CloseSend()

	// Locate TagRPC's FullMethodName and assert it matches the gRPC path.
	// (statsBegin runs from processCall; we just need to let it settle.)
	deadline := time.Now().Add(2 * time.Second)
	var tagged string
	for time.Now().Before(deadline) {
		h.mu.Lock()
		if len(h.tags) > 0 {
			tagged = h.tags[0].FullMethodName
		}
		h.mu.Unlock()
		if tagged != "" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.Equal(t, "/echo.Echo/Echo", tagged,
		"stats.RPCTagInfo.FullMethodName should be the gRPC path on streaming RPCs too")

	// Also verify a Begin event fired with the path-tagged context.
	h.mu.Lock()
	var sawBegin bool
	for _, ev := range h.events {
		if _, ok := ev.(*stats.Begin); ok {
			sawBegin = true
		}
	}
	h.mu.Unlock()
	assert.True(t, sawBegin, "expected a stats.Begin event for the streaming RPC")
}

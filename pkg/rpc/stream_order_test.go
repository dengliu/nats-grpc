package rpc

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cloudwebrtc/nats-grpc/examples/protos/echo"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

var _ = io.EOF // keep io in imports across edits

// orderedEchoSrv records the order in which Echo received frames. It also
// echoes each frame back so the test can drive a bidi stream.
type orderedEchoSrv struct {
	echo.UnimplementedEchoServer
	mu       sync.Mutex
	received []int
}

func (s *orderedEchoSrv) SayHello(ctx context.Context, req *echo.HelloRequest) (*echo.HelloReply, error) {
	return &echo.HelloReply{Msg: req.Msg}, nil
}

func (s *orderedEchoSrv) Echo(stream echo.Echo_EchoServer) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		n, perr := strconv.Atoi(req.Msg)
		if perr != nil {
			return perr
		}
		s.mu.Lock()
		s.received = append(s.received, n)
		s.mu.Unlock()
		if err := stream.Send(&echo.EchoReply{Msg: req.Msg}); err != nil {
			return err
		}
	}
}

// TestIntegration_StreamingPreservesFrameOrder regression-tests the streaming
// server dispatch. Before the per-stream worker fix, server.go spawned a new
// goroutine for each inbound frame (`go stream.onMessage` + `go s.onRequest`),
// so Data frames could overtake the Call that opened the stream and each
// other. With high enough message counts the old code reliably observed
// out-of-order frames; the new dispatch must preserve NATS's per-subscription
// order.
func TestIntegration_StreamingPreservesFrameOrder(t *testing.T) {
	url := runEmbeddedNATS(t)

	scn, err := nats.Connect(url)
	require.NoError(t, err)
	defer scn.Close()
	srv := NewServer(scn, "stream-order")
	impl := &orderedEchoSrv{}
	echo.RegisterEchoServer(srv, impl)
	t.Cleanup(srv.Stop)

	ccn, err := nats.Connect(url)
	require.NoError(t, err)
	defer ccn.Close()
	cli := echo.NewEchoClient(NewClient(ccn, "stream-order", "client-stream-order"))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := cli.Echo(ctx, grpc.WaitForReady(true))
	require.NoError(t, err)

	const n = 200
	// Send and receive concurrently so the test exercises in-flight ordering
	// (the bug surfaced when Data frames overtook Call / each other on the
	// server). We deliberately don't CloseSend until every reply has been
	// observed — calling CloseSend cancels the stream's ctx, which would
	// abort pending Recv calls.
	var sendErr error
	sendDone := make(chan struct{})
	go func() {
		defer close(sendDone)
		for i := 0; i < n; i++ {
			if err := stream.Send(&echo.EchoRequest{Msg: fmt.Sprintf("%d", i)}); err != nil {
				sendErr = err
				return
			}
		}
	}()

	for i := 0; i < n; i++ {
		reply, err := stream.Recv()
		require.NoError(t, err, "recv %d", i)
		assert.Equal(t, strconv.Itoa(i), reply.Msg, "client observed out-of-order reply at %d", i)
	}
	<-sendDone
	require.NoError(t, sendErr)
	require.NoError(t, stream.CloseSend())

	// Drain any trailing frame; ignore non-EOF (server may close with status).
	_, _ = stream.Recv()

	impl.mu.Lock()
	got := append([]int(nil), impl.received...)
	impl.mu.Unlock()

	require.Len(t, got, n, "server saw %d frames, want %d", len(got), n)
	for i, v := range got {
		assert.Equal(t, i, v, "server observed out-of-order frame at index %d: got %d", i, v)
	}
}

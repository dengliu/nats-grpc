package rpc

import (
	"context"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/cloudwebrtc/nats-grpc/examples/protos/echo"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
)

// blockingEchoSrv blocks Echo's Recv until the test signals release. This is
// how we hold a stream open across a Server.Stop() call.
type blockingEchoSrv struct {
	echo.UnimplementedEchoServer
	release chan struct{}
	entered chan struct{}
	once    sync.Once
}

func (b *blockingEchoSrv) Echo(stream echo.Echo_EchoServer) error {
	b.once.Do(func() { close(b.entered) })
	// Block until the test releases us, or the stream's context is cancelled.
	select {
	case <-b.release:
	case <-stream.Context().Done():
	}
	// Drain — don't care about results.
	for {
		if _, err := stream.Recv(); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

// TestIntegration_ServerStop_FiresEndForInFlightStreams asserts that
// Server.Stop fires stats.End for every stream still in flight. Before the
// fix, Stop just cancelled server.ctx and unsubscribed — the per-stream
// worker exited silently, leaving observability backends with a Begin and
// no End (orphaned spans / never-closed histogram observations).
func TestIntegration_ServerStop_FiresEndForInFlightStreams(t *testing.T) {
	url := runEmbeddedNATS(t)

	h := newRecordingHandler()
	scn, err := nats.Connect(url)
	require.NoError(t, err)
	defer scn.Close()
	srv := NewServerWithOptions(scn, "stop-fire", WithServerStatsHandler(h))
	impl := &blockingEchoSrv{
		release: make(chan struct{}),
		entered: make(chan struct{}),
	}
	echo.RegisterEchoServer(srv, impl)

	ccn, err := nats.Connect(url)
	require.NoError(t, err)
	defer ccn.Close()
	cli := echo.NewEchoClient(NewClient(ccn, "stop-fire", "client-stop"))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := cli.Echo(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&echo.EchoRequest{Msg: "hi"}))

	// Wait until the server handler is parked inside Echo.
	select {
	case <-impl.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("server handler never entered Echo")
	}

	// Stop while the stream is in flight.
	srv.Stop()
	close(impl.release)

	// Allow the per-stream worker / End fan-out to settle.
	deadline := time.Now().Add(2 * time.Second)
	var sawEnd bool
	for time.Now().Before(deadline) {
		for _, ev := range h.events {
			if e, ok := ev.(*stats.End); ok {
				sawEnd = true
				assert.Error(t, e.Error, "expected an Unavailable err to be attached to End")
				if e.Error != nil {
					st, _ := status.FromError(e.Error)
					assert.Equal(t, codes.Unavailable, st.Code())
				}
				break
			}
		}
		if sawEnd {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.True(t, sawEnd, "Server.Stop did not fire stats.End for the in-flight stream; events=%v", h.eventTypes())

	// Begin must precede End for a well-formed stream lifecycle.
	first := reflect.TypeOf(&stats.Begin{})
	require.NotEmpty(t, h.eventTypes())
	assert.Equal(t, first, h.eventTypes()[0], "first event should be Begin")
}

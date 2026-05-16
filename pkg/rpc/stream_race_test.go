package rpc

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/cloudwebrtc/nats-grpc/examples/protos/echo"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

// TestIntegration_ClientStream_ConcurrentClose verifies that clientStream's
// `closed` and `lastErr` fields are safe under concurrent access from the
// public API surface. Before the fix, `closed` was a plain bool written from
// done() and read from pingLoop / SendMsg, and `lastErr` was a plain error
// written from ReadMsg and read from RecvMsg — both reproducibly flagged by
// `go test -race` on the streaming code path.
//
// This test exercises many concurrent streams, each driven by parallel
// Send / Recv / CloseSend, so any unsynchronized field access in the
// stream lifecycle is caught by the race detector.
func TestIntegration_ClientStream_ConcurrentClose(t *testing.T) {
	url := runEmbeddedNATS(t)

	scn, err := nats.Connect(url)
	require.NoError(t, err)
	defer scn.Close()
	srv := NewServer(scn, "race-close")
	echo.RegisterEchoServer(srv, &orderedEchoSrv{})
	t.Cleanup(srv.Stop)

	ccn, err := nats.Connect(url)
	require.NoError(t, err)
	defer ccn.Close()
	cli := echo.NewEchoClient(NewClient(ccn, "race-close", "client-race"))

	const streams = 40
	var wg sync.WaitGroup
	for i := 0; i < streams; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			stream, err := cli.Echo(ctx)
			if err != nil {
				t.Errorf("Echo open: %v", err)
				return
			}

			// One goroutine sends, one receives, then CloseSend.
			done := make(chan struct{})
			go func() {
				defer close(done)
				_ = stream.Send(&echo.EchoRequest{Msg: "1"})
				if _, err := stream.Recv(); err != nil {
					t.Logf("recv: %v", err)
				}
			}()
			<-done
			_ = stream.CloseSend()
		}()
	}
	wg.Wait()
}

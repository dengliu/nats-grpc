package rpc

import (
	"context"
	"testing"
	"time"

	"github.com/cloudwebrtc/nats-grpc/examples/protos/echo"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_ClientStreamMapDoesNotLeak regression-tests that
// clientStream.done removes its entry from Client.streams. The map is keyed
// by reply (see Client.streamer), but done() used to call
// remove(c.subject) — a silent no-op that leaked the entry forever.
func TestIntegration_ClientStreamMapDoesNotLeak(t *testing.T) {
	url := runEmbeddedNATS(t)

	scn, err := nats.Connect(url)
	require.NoError(t, err)
	defer scn.Close()
	srv := NewServer(scn, "leak-test")
	echo.RegisterEchoServer(srv, &orderedEchoSrv{})
	t.Cleanup(srv.Stop)

	ccn, err := nats.Connect(url)
	require.NoError(t, err)
	defer ccn.Close()
	rawClient := NewClient(ccn, "leak-test", "client-leak")
	cli := echo.NewEchoClient(rawClient)

	const n = 20
	for i := 0; i < n; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		stream, err := cli.Echo(ctx)
		require.NoError(t, err)

		require.NoError(t, stream.Send(&echo.EchoRequest{Msg: "1"}))
		reply, err := stream.Recv()
		require.NoError(t, err)
		assert.Equal(t, "1", reply.Msg)

		require.NoError(t, stream.CloseSend())
		cancel()
	}

	// Give clientStream.done a tick to run.
	deadline := time.Now().Add(2 * time.Second)
	var size int
	for time.Now().Before(deadline) {
		rawClient.mu.Lock()
		size = len(rawClient.streams)
		rawClient.mu.Unlock()
		if size == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	assert.Zero(t, size, "Client.streams leaked %d entries after %d completed streams", size, n)
}

package rpc

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudwebrtc/nats-grpc/pkg/protos/nrpc"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

// pingCountingStreamingServer wires a raw NATS subscriber for the streaming
// subject so the test can count Ping frames on the wire without standing up
// a full Server. The reply is a Begin so the client transitions out of its
// pre-Begin state.
type pingCountingStreamingServer struct {
	nc      *nats.Conn
	subject string

	pings atomic.Int64
}

func (s *pingCountingStreamingServer) start(t *testing.T) {
	t.Helper()
	sub, err := s.nc.Subscribe(s.subject+".>", func(msg *nats.Msg) {
		req := &nrpc.Request{}
		if err := proto.Unmarshal(msg.Data, req); err != nil {
			return
		}
		switch req.Type.(type) {
		case *nrpc.Request_Call:
			// Reply with Begin so the client's stream is fully alive.
			begin := &nrpc.Response{Type: &nrpc.Response_Begin{Begin: &nrpc.Begin{Nid: "stub-server"}}}
			b, _ := proto.Marshal(begin)
			_ = s.nc.Publish(msg.Reply, b)
		case *nrpc.Request_Ping:
			s.pings.Add(1)
			// Don't reply with Pong — the heartbeat monitor isn't under
			// test here, only the Ping cadence on the wire.
		}
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })
}

// TestHeartbeat_Disabled_NoPingsOnTheWire verifies WithoutHeartbeat (and the
// equivalent WithHeartbeat(0,0)) skips the per-stream ping goroutines —
// observable as zero Ping frames published by the client.
func TestHeartbeat_Disabled_NoPingsOnTheWire(t *testing.T) {
	url := runEmbeddedNATS(t)

	nc, err := nats.Connect(url)
	require.NoError(t, err)
	defer nc.Close()
	stub := &pingCountingStreamingServer{nc: nc, subject: "nrpc.hb-off.echo.Echo"}
	stub.start(t)

	ccn, err := nats.Connect(url)
	require.NoError(t, err)
	defer ccn.Close()

	cli := NewClientWithOptions(ccn, "hb-off", "client-off", WithoutHeartbeat())
	defer cli.Close()

	// Open a stream manually — easier than wiring through echo.NewEchoClient
	// because we want a long-lived, otherwise-idle stream.
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	stream, err := cli.streamer(ctx, nil, nil, "/echo.Echo/Echo")
	require.NoError(t, err)
	require.NoError(t, stream.SendMsg(&nrpc.Ping{Timestamp: 0})) // forces Call onto the wire

	// Wait long enough that 5x the default ping interval would have fired
	// (default is 5s; this is much shorter, but the goroutines are gated
	// behind pingInterval > 0 so any non-zero count here is a regression).
	time.Sleep(1 * time.Second)

	got := stub.pings.Load()
	assert.Equal(t, int64(0), got, "WithoutHeartbeat must not emit any Ping frames; saw %d", got)
}

// TestHeartbeat_CustomInterval_EmitsPingsAtConfiguredRate verifies that
// WithHeartbeat(short interval) drives the ticker at the requested cadence.
// We use 100ms / 1s and assert at least 3 pings within 600ms.
func TestHeartbeat_CustomInterval_EmitsPingsAtConfiguredRate(t *testing.T) {
	url := runEmbeddedNATS(t)

	nc, err := nats.Connect(url)
	require.NoError(t, err)
	defer nc.Close()
	stub := &pingCountingStreamingServer{nc: nc, subject: "nrpc.hb-fast.echo.Echo"}
	stub.start(t)

	ccn, err := nats.Connect(url)
	require.NoError(t, err)
	defer ccn.Close()

	cli := NewClientWithOptions(ccn, "hb-fast", "client-fast",
		WithHeartbeat(100*time.Millisecond, 1*time.Second))
	defer cli.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	stream, err := cli.streamer(ctx, nil, nil, "/echo.Echo/Echo")
	require.NoError(t, err)
	require.NoError(t, stream.SendMsg(&nrpc.Ping{Timestamp: 0}))

	// 100ms interval should yield ~6 pings in 600ms; allow some scheduling
	// jitter and accept >= 3.
	time.Sleep(600 * time.Millisecond)
	got := stub.pings.Load()
	assert.GreaterOrEqual(t, got, int64(3),
		"expected at least 3 Pings at 100ms cadence over 600ms; saw %d", got)
}

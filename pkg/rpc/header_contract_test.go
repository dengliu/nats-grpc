package rpc

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/cloudwebrtc/nats-grpc/examples/protos/echo"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setHeaderAfterSendSrv tries to call SetHeader after SendMsg has already
// fired Begin on the wire. It records the returned error so the test can
// assert that ErrIllegalHeaderWrite is surfaced — the same contract grpc-go
// implements.
type setHeaderAfterSendSrv struct {
	echo.UnimplementedEchoServer
	got     chan error
	gotOnce sync.Once
}

func (s *setHeaderAfterSendSrv) Echo(stream echo.Echo_EchoServer) error {
	// First read drives Begin via SendMsg below.
	_, err := stream.Recv()
	if err != nil && err != io.EOF {
		return err
	}
	if err := stream.Send(&echo.EchoReply{Msg: "first"}); err != nil {
		return err
	}
	// Begin has now been sent. SetHeader must error.
	setErr := stream.SetHeader(map[string][]string{"x": {"too-late"}})
	s.gotOnce.Do(func() { s.got <- setErr })
	return nil
}

// TestIntegration_SetHeaderAfterSend_ReturnsErrIllegalHeaderWrite is a
// behavioural pin on the SetHeader contract. It already held on the old
// code (hasBegun gate in SetHeader) but had no test; pin it so a future
// refactor of beginMaybe can't break it silently.
func TestIntegration_SetHeaderAfterSend_ReturnsErrIllegalHeaderWrite(t *testing.T) {
	url := runEmbeddedNATS(t)

	scn, err := nats.Connect(url)
	require.NoError(t, err)
	defer scn.Close()
	srv := NewServer(scn, "header-contract")
	impl := &setHeaderAfterSendSrv{got: make(chan error, 1)}
	echo.RegisterEchoServer(srv, impl)
	t.Cleanup(srv.Stop)

	ccn, err := nats.Connect(url)
	require.NoError(t, err)
	defer ccn.Close()
	cli := echo.NewEchoClient(NewClient(ccn, "header-contract", "client-hc"))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := cli.Echo(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&echo.EchoRequest{Msg: "hi"}))
	_, err = stream.Recv()
	require.NoError(t, err)

	select {
	case got := <-impl.got:
		assert.True(t, errors.Is(got, ErrIllegalHeaderWrite),
			"SetHeader after SendMsg should return ErrIllegalHeaderWrite, got: %v", got)
	case <-time.After(2 * time.Second):
		t.Fatal("server handler never observed SetHeader result")
	}
}

// nilHeaderEchoSrv intentionally never calls SetHeader. Before the fix,
// beginMaybe skipped writing the Begin frame in this case — so the client
// never learned the server's nid, leaving point-to-point heartbeat routing
// without its target.
type nilHeaderEchoSrv struct {
	echo.UnimplementedEchoServer
}

func (nilHeaderEchoSrv) Echo(stream echo.Echo_EchoServer) error {
	_, err := stream.Recv()
	if err != nil && err != io.EOF {
		return err
	}
	return stream.Send(&echo.EchoReply{Msg: "ok"})
}

// TestIntegration_BeginAlwaysSentSoClientLearnsServerNid asserts the client
// learns the server's nid (via the Begin frame's Nid field) even when the
// handler never sets any headers. Before the fix, beginMaybe gated Begin on
// header != nil and the client's pnid stayed empty.
func TestIntegration_BeginAlwaysSentSoClientLearnsServerNid(t *testing.T) {
	url := runEmbeddedNATS(t)

	scn, err := nats.Connect(url)
	require.NoError(t, err)
	defer scn.Close()
	const serverNid = "server-nid-7"
	srv := NewServer(scn, serverNid)
	echo.RegisterEchoServer(srv, nilHeaderEchoSrv{})
	t.Cleanup(srv.Stop)

	ccn, err := nats.Connect(url)
	require.NoError(t, err)
	defer ccn.Close()
	rawClient := NewClient(ccn, serverNid, "client-7")
	cli := echo.NewEchoClient(rawClient)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := cli.Echo(ctx)
	require.NoError(t, err)
	require.NoError(t, stream.Send(&echo.EchoRequest{Msg: "x"}))
	_, err = stream.Recv()
	require.NoError(t, err)

	// Reach into Client.streams to find this stream and inspect pnid.
	rawClient.mu.Lock()
	var found bool
	for _, st := range rawClient.streams {
		if st.pnid == serverNid {
			found = true
			break
		}
	}
	rawClient.mu.Unlock()
	assert.True(t, found, "client never received Begin.Nid; pnid was never populated")

	// Cleanup.
	_ = stream.CloseSend()
}

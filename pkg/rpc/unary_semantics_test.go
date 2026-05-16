package rpc

import (
	"context"
	"reflect"
	"testing"

	"github.com/cloudwebrtc/nats-grpc/pkg/protos/nrpc"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	googlerpc "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/protobuf/proto"
)

// marshalUnaryErrResp returns wire bytes for an nrpc.Response carrying a
// non-OK UnaryResponse with a non-empty data field — the rare-but-legal case
// where a server packs payload bytes alongside an error status.
func marshalUnaryErrResp(t *testing.T, code codes.Code, msg string, data []byte) []byte {
	t.Helper()
	resp := &nrpc.Response{Type: &nrpc.Response_Unary{Unary: &nrpc.UnaryResponse{
		Status: &googlerpc.Status{Code: int32(code), Message: msg},
		Data:   data,
	}}}
	out, err := proto.Marshal(resp)
	require.NoError(t, err)
	return out
}

// marshalUnaryRespWithMeta lets a test inject header + trailer metadata so the
// header/trailer-call-option semantics can be verified.
func marshalUnaryRespWithMeta(t *testing.T, payload proto.Message, header, trailer map[string][]string) []byte {
	t.Helper()
	body, err := proto.Marshal(payload)
	require.NoError(t, err)
	u := &nrpc.UnaryResponse{
		Status: &googlerpc.Status{Code: int32(codes.OK)},
		Data:   body,
	}
	if header != nil {
		u.Header = &nrpc.Metadata{Md: make(map[string]*nrpc.Strings, len(header))}
		for k, v := range header {
			u.Header.Md[k] = &nrpc.Strings{Values: v}
		}
	}
	if trailer != nil {
		u.Trailer = &nrpc.Metadata{Md: make(map[string]*nrpc.Strings, len(trailer))}
		for k, v := range trailer {
			u.Trailer.Md[k] = &nrpc.Strings{Values: v}
		}
	}
	resp := &nrpc.Response{Type: &nrpc.Response_Unary{Unary: u}}
	out, err := proto.Marshal(resp)
	require.NoError(t, err)
	return out
}

// TestUnary_InPayload_FiresWithBytesOnError verifies that when a server
// responds with a non-OK status that nonetheless carries payload bytes,
// stats.InPayload is still emitted with the raw byte count. Before the fix,
// the client returned early on non-OK and never fired InPayload, so size
// metrics under-reported errored RPCs that carried error-detail data.
func TestUnary_InPayload_FiresWithBytesOnError(t *testing.T) {
	mockNC := new(MockNatsConn)
	errBody := []byte("payload-on-error")
	mockNC.On("RequestWithContext", mock.Anything, mock.Anything, mock.Anything).
		Return(&nats.Msg{Data: marshalUnaryErrResp(t, codes.FailedPrecondition, "boom", errBody)}, nil)

	h := newRecordingHandler()
	client := NewClientWithOptions(mockNC, "svc", "nid", WithStatsHandler(h))
	defer client.Close()

	err := client.Invoke(context.Background(), "/test.Svc/M", &nrpc.Ping{}, &nrpc.Pong{})
	assert.Error(t, err)

	// Find the InPayload event; it must be present with Length matching the
	// payload byte count.
	var got *stats.InPayload
	for _, ev := range h.events {
		if in, ok := ev.(*stats.InPayload); ok {
			got = in
		}
	}
	require.NotNil(t, got, "expected stats.InPayload to fire even on non-OK status; events=%v", h.eventTypes())
	assert.Equal(t, len(errBody), got.Length)
	// Payload should be unset for non-OK responses (we don't unmarshal into
	// reply on the error path), but the byte count is still recorded.
	assert.Nil(t, got.Payload)
}

// TestUnary_InPayload_NotFiredOnZeroBytes covers the inverse: an OK response
// with no payload (Data is empty) shouldn't synthesize a phantom InPayload.
func TestUnary_InPayload_NotFiredOnZeroBytes(t *testing.T) {
	mockNC := new(MockNatsConn)
	resp := &nrpc.Response{Type: &nrpc.Response_Unary{Unary: &nrpc.UnaryResponse{
		Status: &googlerpc.Status{Code: int32(codes.OK)},
	}}}
	out, err := proto.Marshal(resp)
	require.NoError(t, err)
	mockNC.On("RequestWithContext", mock.Anything, mock.Anything, mock.Anything).
		Return(&nats.Msg{Data: out}, nil)

	h := newRecordingHandler()
	client := NewClientWithOptions(mockNC, "svc", "nid", WithStatsHandler(h))
	defer client.Close()

	err = client.Invoke(context.Background(), "/test.Svc/M", &nrpc.Ping{}, &nrpc.Pong{})
	assert.NoError(t, err)
	for _, ev := range h.events {
		_, isIn := ev.(*stats.InPayload)
		assert.False(t, isIn, "InPayload should not fire when no bytes were on the wire")
	}
	// Match the canonical sequence without InPayload.
	assert.Equal(t, []reflect.Type{
		reflect.TypeOf(&stats.Begin{}),
		reflect.TypeOf(&stats.OutPayload{}),
		reflect.TypeOf(&stats.End{}),
	}, h.eventTypes())
}

// TestUnary_HeaderCallOption_OverwritesNotAppends verifies grpc-go-aligned
// semantics: a reused *metadata.MD passed via grpc.Header(&md) is replaced
// each call, not appended into. Before the fix, two sequential calls would
// produce duplicate values in the caller's MD.
func TestUnary_HeaderCallOption_OverwritesNotAppends(t *testing.T) {
	mockNC := new(MockNatsConn)
	// First call returns header={"x":["one"]}; second returns {"x":["two"]}.
	mockNC.On("RequestWithContext", mock.Anything, mock.Anything, mock.Anything).
		Return(&nats.Msg{Data: marshalUnaryRespWithMeta(t, &nrpc.Pong{}, map[string][]string{"x": {"one"}}, nil)}, nil).Once()
	mockNC.On("RequestWithContext", mock.Anything, mock.Anything, mock.Anything).
		Return(&nats.Msg{Data: marshalUnaryRespWithMeta(t, &nrpc.Pong{}, map[string][]string{"x": {"two"}}, nil)}, nil).Once()

	client := NewClient(mockNC, "svc", "nid")
	defer client.Close()

	var md metadata.MD
	require.NoError(t, client.Invoke(context.Background(), "/test.Svc/M", &nrpc.Ping{}, &nrpc.Pong{}, grpc.Header(&md)))
	assert.Equal(t, []string{"one"}, md.Get("x"))

	// Reuse the same &md for the second call — grpc-go overwrites, not
	// appends. Before the fix the test would observe ["one","two"].
	require.NoError(t, client.Invoke(context.Background(), "/test.Svc/M", &nrpc.Ping{}, &nrpc.Pong{}, grpc.Header(&md)))
	assert.Equal(t, []string{"two"}, md.Get("x"),
		"HeaderCallOption must overwrite *HeaderAddr like grpc-go, not append into it")
}

// TestUnary_TrailerCallOption_OverwritesNotAppends is the trailer counterpart.
func TestUnary_TrailerCallOption_OverwritesNotAppends(t *testing.T) {
	mockNC := new(MockNatsConn)
	mockNC.On("RequestWithContext", mock.Anything, mock.Anything, mock.Anything).
		Return(&nats.Msg{Data: marshalUnaryRespWithMeta(t, &nrpc.Pong{}, nil, map[string][]string{"t": {"a"}})}, nil).Once()
	mockNC.On("RequestWithContext", mock.Anything, mock.Anything, mock.Anything).
		Return(&nats.Msg{Data: marshalUnaryRespWithMeta(t, &nrpc.Pong{}, nil, map[string][]string{"t": {"b"}})}, nil).Once()

	client := NewClient(mockNC, "svc", "nid")
	defer client.Close()

	var trailer metadata.MD
	require.NoError(t, client.Invoke(context.Background(), "/test.Svc/M", &nrpc.Ping{}, &nrpc.Pong{}, grpc.Trailer(&trailer)))
	assert.Equal(t, []string{"a"}, trailer.Get("t"))

	require.NoError(t, client.Invoke(context.Background(), "/test.Svc/M", &nrpc.Ping{}, &nrpc.Pong{}, grpc.Trailer(&trailer)))
	assert.Equal(t, []string{"b"}, trailer.Get("t"),
		"TrailerCallOption must overwrite *TrailerAddr, not append into it")
}

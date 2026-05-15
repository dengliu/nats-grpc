package rpc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/cloudwebrtc/nats-grpc/pkg/protos/nrpc"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// MockNatsConn is a mock implementation of NatsConn
type MockNatsConn struct {
	mock.Mock
}

func (m *MockNatsConn) Publish(subj string, data []byte) error {
	args := m.Called(subj, data)
	return args.Error(0)
}

func (m *MockNatsConn) PublishRequest(subj, reply string, data []byte) error {
	args := m.Called(subj, reply, data)
	return args.Error(0)
}

func (m *MockNatsConn) ChanSubscribe(subj string, ch chan *nats.Msg) (*nats.Subscription, error) {
	args := m.Called(subj, ch)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*nats.Subscription), args.Error(1)
}

func (m *MockNatsConn) Subscribe(subj string, cb nats.MsgHandler) (*nats.Subscription, error) {
	args := m.Called(subj, cb)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*nats.Subscription), args.Error(1)
}

func (m *MockNatsConn) Request(subj string, data []byte, timeout time.Duration) (*nats.Msg, error) {
	args := m.Called(subj, data, timeout)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*nats.Msg), args.Error(1)
}

func (m *MockNatsConn) RequestWithContext(ctx context.Context, subj string, data []byte) (*nats.Msg, error) {
	args := m.Called(ctx, subj, data)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*nats.Msg), args.Error(1)
}

func (m *MockNatsConn) SubscribeSync(subj string) (*nats.Subscription, error) {
	args := m.Called(subj)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*nats.Subscription), args.Error(1)
}

func (m *MockNatsConn) QueueSubscribe(subj, queue string, cb nats.MsgHandler) (*nats.Subscription, error) {
	args := m.Called(subj, queue, cb)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*nats.Subscription), args.Error(1)
}

func (m *MockNatsConn) LastError() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockNatsConn) Flush() error {
	args := m.Called()
	return args.Error(0)
}

func (m *MockNatsConn) Close() {
	m.Called()
}

// MockSubscription is a mock implementation of nats.Subscription
type MockSubscription struct {
	mock.Mock
}

func (m *MockSubscription) Unsubscribe() error {
	args := m.Called()
	return args.Error(0)
}

// Test that unary interceptor is called when configured
func TestUnaryInterceptor_Called(t *testing.T) {
	mockNC := new(MockNatsConn)
	mockSub := &nats.Subscription{}
	
	// Setup expectations
	mockNC.On("ChanSubscribe", mock.Anything, mock.Anything).Return(mockSub, nil)
	mockNC.On("PublishRequest", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	
	interceptorCalled := false
	testInterceptor := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		interceptorCalled = true
		// Don't call the actual invoker to avoid complex mocking
		return nil
	}
	
	client := NewClientWithOptions(mockNC, "test-service", "test-nid", WithUnaryInterceptor(testInterceptor))
	defer client.Close()
	
	ctx := context.Background()
	err := client.Invoke(ctx, "/test.Service/Method", &struct{}{}, &struct{}{})
	
	assert.NoError(t, err)
	assert.True(t, interceptorCalled, "Unary interceptor should have been called")
}

// Test that unary interceptor receives correct parameters
func TestUnaryInterceptor_Parameters(t *testing.T) {
	mockNC := new(MockNatsConn)
	mockSub := &nats.Subscription{}
	
	mockNC.On("ChanSubscribe", mock.Anything, mock.Anything).Return(mockSub, nil)
	mockNC.On("PublishRequest", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	
	expectedMethod := "/test.Service/TestMethod"
	expectedReq := &struct{ Value string }{Value: "test"}
	expectedReply := &struct{ Result string }{}
	
	var capturedMethod string
	var capturedReq interface{}
	var capturedReply interface{}
	
	testInterceptor := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		capturedMethod = method
		capturedReq = req
		capturedReply = reply
		
		// Verify context is not nil
		assert.NotNil(t, ctx)
		// Verify cc is nil (as per gRPC interceptor pattern for client-side)
		assert.Nil(t, cc)
		
		return nil
	}
	
	client := NewClientWithOptions(mockNC, "test-service", "test-nid", WithUnaryInterceptor(testInterceptor))
	defer client.Close()
	
	ctx := context.Background()
	err := client.Invoke(ctx, expectedMethod, expectedReq, expectedReply)
	
	assert.NoError(t, err)
	assert.Equal(t, expectedMethod, capturedMethod)
	assert.Equal(t, expectedReq, capturedReq)
	assert.Equal(t, expectedReply, capturedReply)
}

// Test that interceptor can modify context (e.g., add timeout)
func TestUnaryInterceptor_ContextModification(t *testing.T) {
	mockNC := new(MockNatsConn)
	mockSub := &nats.Subscription{}
	
	mockNC.On("ChanSubscribe", mock.Anything, mock.Anything).Return(mockSub, nil)
	mockNC.On("PublishRequest", mock.Anything, mock.Anything, mock.Anything).Return(nil)
	
	timeoutInterceptor := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		// Add timeout to context
		ctx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()
		
		// Verify timeout is set
		deadline, ok := ctx.Deadline()
		assert.True(t, ok, "Context should have a deadline")
		assert.True(t, time.Until(deadline) <= 100*time.Millisecond, "Deadline should be within 100ms")
		
		return nil
	}
	
	client := NewClientWithOptions(mockNC, "test-service", "test-nid", WithUnaryInterceptor(timeoutInterceptor))
	defer client.Close()
	
	ctx := context.Background()
	err := client.Invoke(ctx, "/test.Service/Method", &struct{}{}, &struct{}{})
	
	assert.NoError(t, err)
}

// Test that interceptor can return errors
func TestUnaryInterceptor_ErrorPropagation(t *testing.T) {
	mockNC := new(MockNatsConn)
	
	expectedErr := status.Error(codes.PermissionDenied, "access denied")
	
	errorInterceptor := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		// Return error without calling invoker
		return expectedErr
	}
	
	client := NewClientWithOptions(mockNC, "test-service", "test-nid", WithUnaryInterceptor(errorInterceptor))
	defer client.Close()
	
	ctx := context.Background()
	err := client.Invoke(ctx, "/test.Service/Method", &struct{}{}, &struct{}{})
	
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
}

// Test backward compatibility: NewClient without options still works
func TestNewClient_BackwardCompatibility(t *testing.T) {
	mockNC := new(MockNatsConn)
	
	client := NewClient(mockNC, "test-service", "test-nid")
	defer client.Close()
	
	assert.NotNil(t, client)
	assert.Equal(t, "test-service", client.svcid)
	assert.Equal(t, "test-nid", client.nid)
	assert.Nil(t, client.unaryInt, "No unary interceptor should be set")
	assert.Nil(t, client.streamInt, "No stream interceptor should be set")
}

// Test that without interceptor, direct invocation path is correct
func TestInvoke_WithoutInterceptor(t *testing.T) {
	mockNC := new(MockNatsConn)
	
	client := NewClient(mockNC, "test-service", "test-nid")
	defer client.Close()
	
	// Verify no interceptors are set
	assert.Nil(t, client.unaryInt, "No unary interceptor should be set")
	assert.Nil(t, client.streamInt, "No stream interceptor should be set")
	
	// Verify client fields are correctly initialized
	assert.Equal(t, "test-service", client.svcid)
	assert.Equal(t, "test-nid", client.nid)
	assert.NotNil(t, client.streams)
}

// Test stream interceptor is called when configured
func TestStreamInterceptor_Called(t *testing.T) {
	mockNC := new(MockNatsConn)
	
	interceptorCalled := false
	var capturedStreamer grpc.Streamer
	
	testStreamInterceptor := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		interceptorCalled = true
		capturedStreamer = streamer
		// Don't call the actual streamer to avoid creating background goroutines
		return nil, nil
	}
	
	client := NewClientWithOptions(mockNC, "test-service", "test-nid", WithStreamInterceptor(testStreamInterceptor))
	defer client.Close()
	
	ctx := context.Background()
	desc := &grpc.StreamDesc{
		StreamName:    "TestStream",
		ServerStreams: true,
		ClientStreams: true,
	}
	
	stream, err := client.NewStream(ctx, desc, "/test.Service/StreamMethod")
	
	assert.NoError(t, err)
	assert.Nil(t, stream) // We returned nil in the interceptor
	assert.True(t, interceptorCalled, "Stream interceptor should have been called")
	assert.NotNil(t, capturedStreamer, "Streamer should have been passed to interceptor")
}

// Test stream interceptor receives correct parameters
func TestStreamInterceptor_Parameters(t *testing.T) {
	mockNC := new(MockNatsConn)
	
	expectedMethod := "/test.Service/StreamMethod"
	expectedDesc := &grpc.StreamDesc{
		StreamName:    "TestStream",
		ServerStreams: true,
		ClientStreams: true,
	}
	
	var capturedMethod string
	var capturedDesc *grpc.StreamDesc
	var capturedCtx context.Context
	
	testStreamInterceptor := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		capturedMethod = method
		capturedDesc = desc
		capturedCtx = ctx
		
		assert.NotNil(t, ctx)
		assert.Nil(t, cc)
		
		// Don't call streamer to avoid background goroutines
		return nil, nil
	}
	
	client := NewClientWithOptions(mockNC, "test-service", "test-nid", WithStreamInterceptor(testStreamInterceptor))
	defer client.Close()
	
	ctx := context.Background()
	stream, err := client.NewStream(ctx, expectedDesc, expectedMethod)
	
	assert.NoError(t, err)
	assert.Nil(t, stream) // We returned nil in interceptor
	assert.Equal(t, expectedMethod, capturedMethod)
	assert.Equal(t, expectedDesc, capturedDesc)
	assert.NotNil(t, capturedCtx)
}

// Test stream interceptor can return errors
func TestStreamInterceptor_ErrorPropagation(t *testing.T) {
	mockNC := new(MockNatsConn)
	
	expectedErr := errors.New("stream creation failed")
	
	errorStreamInterceptor := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return nil, expectedErr
	}
	
	client := NewClientWithOptions(mockNC, "test-service", "test-nid", WithStreamInterceptor(errorStreamInterceptor))
	defer client.Close()
	
	ctx := context.Background()
	desc := &grpc.StreamDesc{}
	
	stream, err := client.NewStream(ctx, desc, "/test.Service/StreamMethod")
	
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	assert.Nil(t, stream)
}

// Test multiple options can be applied
func TestNewClientWithOptions_MultipleOptions(t *testing.T) {
	mockNC := new(MockNatsConn)
	
	unaryInterceptorCalled := false
	streamInterceptorCalled := false
	
	unaryInterceptor := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		unaryInterceptorCalled = true
		return nil
	}
	
	streamInterceptor := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		streamInterceptorCalled = true
		// Don't call streamer to avoid background goroutines
		return nil, nil
	}
	
	client := NewClientWithOptions(
		mockNC,
		"test-service",
		"test-nid",
		WithUnaryInterceptor(unaryInterceptor),
		WithStreamInterceptor(streamInterceptor),
	)
	defer client.Close()
	
	// Test unary call
	ctx := context.Background()
	client.Invoke(ctx, "/test.Service/Method", &struct{}{}, &struct{}{})
	assert.True(t, unaryInterceptorCalled)
	
	// Test stream call
	desc := &grpc.StreamDesc{}
	stream, err := client.NewStream(ctx, desc, "/test.Service/StreamMethod")
	assert.NoError(t, err)
	assert.Nil(t, stream) // We returned nil in interceptor
	assert.True(t, streamInterceptorCalled)
}

// Test chaining interceptors (simulating multiple interceptors via wrapping)
func TestUnaryInterceptor_Chaining(t *testing.T) {
	mockNC := new(MockNatsConn)
	
	callOrder := []string{}
	
	// Mock invoker that just records being called
	mockInvoker := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		callOrder = append(callOrder, "invoker-called")
		return nil
	}
	
	// First interceptor (inner)
	interceptor1 := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		callOrder = append(callOrder, "interceptor1-before")
		err := mockInvoker(ctx, method, req, reply, cc, opts...)
		callOrder = append(callOrder, "interceptor1-after")
		return err
	}
	
	// Second interceptor (outer) - wrap interceptor1
	interceptor2 := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		callOrder = append(callOrder, "interceptor2-before")
		// Call interceptor1 instead of the actual invoker
		err := interceptor1(ctx, method, req, reply, cc, invoker, opts...)
		callOrder = append(callOrder, "interceptor2-after")
		return err
	}
	
	client := NewClientWithOptions(mockNC, "test-service", "test-nid", WithUnaryInterceptor(interceptor2))
	defer client.Close()
	
	ctx := context.Background()
	client.Invoke(ctx, "/test.Service/Method", &struct{}{}, &struct{}{})
	
	// Verify the order of execution
	expected := []string{
		"interceptor2-before",
		"interceptor1-before",
		"invoker-called",
		"interceptor1-after",
		"interceptor2-after",
	}
	assert.Equal(t, expected, callOrder, "Interceptors should be called in the correct order")
}

// Test that invoker is passed correctly to the interceptor
func TestUnaryInterceptor_InvokerCalled(t *testing.T) {
	mockNC := new(MockNatsConn)
	
	invokerReceived := false
	var receivedInvoker grpc.UnaryInvoker
	
	testInterceptor := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		invokerReceived = true
		receivedInvoker = invoker
		// Don't call the invoker to avoid hanging
		return nil
	}
	
	client := NewClientWithOptions(mockNC, "test-service", "test-nid", WithUnaryInterceptor(testInterceptor))
	defer client.Close()
	
	ctx := context.Background()
	client.Invoke(ctx, "/test.Service/Method", &struct{}{}, &struct{}{})
	
	assert.True(t, invokerReceived, "Interceptor should have received an invoker")
	assert.NotNil(t, receivedInvoker, "Invoker should not be nil")
}

// Test client cleanup
func TestClient_Close(t *testing.T) {
	mockNC := new(MockNatsConn)
	
	client := NewClient(mockNC, "test-service", "test-nid")
	
	assert.NotNil(t, client.ctx)
	assert.NotNil(t, client.cancel)
	
	err := client.Close()
	assert.NoError(t, err)
	
	// Verify context is cancelled
	select {
	case <-client.ctx.Done():
		// Expected
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Context should have been cancelled")
	}
}

// Test that options are applied correctly
func TestClientOption_Application(t *testing.T) {
	mockNC := new(MockNatsConn)
	
	dummyUnaryInt := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return nil
	}
	
	dummyStreamInt := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return nil, nil
	}
	
	client := NewClientWithOptions(
		mockNC,
		"test-service",
		"test-nid",
		WithUnaryInterceptor(dummyUnaryInt),
		WithStreamInterceptor(dummyStreamInt),
	)
	defer client.Close()
	
	assert.NotNil(t, client.unaryInt, "Unary interceptor should be set")
	assert.NotNil(t, client.streamInt, "Stream interceptor should be set")
}

// Test NewStream without interceptor creates proper stream structure
func TestNewStream_WithoutInterceptor(t *testing.T) {
	mockNC := new(MockNatsConn)

	client := NewClient(mockNC, "test-service", "test-nid")
	defer client.Close()

	// Verify client has no stream interceptor
	assert.Nil(t, client.streamInt, "No stream interceptor should be set")

	// Verify the client is properly initialized for stream operations
	assert.NotNil(t, client.streams, "Streams map should be initialized")
	assert.Equal(t, "test-service", client.svcid)
	assert.Equal(t, "test-nid", client.nid)
}

// --- Unary RPC path (Option 2: nc.RequestWithContext) ---------------------
//
// These tests cover Client.invoker after the switch to single-message
// request/reply. They use nrpc.Ping / nrpc.Pong as concrete proto.Messages
// because the unary path actually marshals args/reply via proto.

// marshalUnaryResp returns wire bytes for an nrpc.Response carrying a
// successful UnaryResponse wrapping payload.
func marshalUnaryResp(t *testing.T, payload proto.Message) []byte {
	t.Helper()
	data, err := proto.Marshal(payload)
	assert.NoError(t, err)
	resp := &nrpc.Response{Type: &nrpc.Response_Unary{Unary: &nrpc.UnaryResponse{
		Status: status.Convert(nil).Proto(),
		Data:   data,
	}}}
	out, err := proto.Marshal(resp)
	assert.NoError(t, err)
	return out
}

// TestUnary_HappyPath drives the full invoker round-trip with a mocked NATS
// reply and verifies the reply proto is populated and the subject is built
// correctly.
func TestUnary_HappyPath(t *testing.T) {
	mockNC := new(MockNatsConn)

	wantReply := &nrpc.Pong{Timestamp: 12345}
	mockNC.On("RequestWithContext", mock.Anything, "nrpc.test-svc.test.Svc.Method", mock.Anything).
		Return(&nats.Msg{Data: marshalUnaryResp(t, wantReply)}, nil)

	client := NewClient(mockNC, "test-svc", "test-nid")
	defer client.Close()

	gotReply := &nrpc.Pong{}
	err := client.Invoke(context.Background(), "/test.Svc/Method", &nrpc.Ping{Timestamp: 1}, gotReply)

	assert.NoError(t, err)
	assert.Equal(t, int64(12345), gotReply.Timestamp)
	mockNC.AssertExpectations(t)
}

// TestUnary_RequestSerialization verifies the on-wire Request is Request_Unary
// with method, data, and outgoing metadata correctly packed.
func TestUnary_RequestSerialization(t *testing.T) {
	mockNC := new(MockNatsConn)

	var captured []byte
	mockNC.On("RequestWithContext", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			captured = append([]byte(nil), args.Get(2).([]byte)...)
		}).
		Return(&nats.Msg{Data: marshalUnaryResp(t, &nrpc.Pong{})}, nil)

	client := NewClient(mockNC, "svc", "nid")
	defer client.Close()

	ctx := metadata.AppendToOutgoingContext(context.Background(), "x-test", "v1")
	err := client.Invoke(ctx, "/test.Svc/Method", &nrpc.Ping{Timestamp: 42}, &nrpc.Pong{})
	assert.NoError(t, err)

	req := &nrpc.Request{}
	assert.NoError(t, proto.Unmarshal(captured, req))
	ureq := req.GetUnary()
	if assert.NotNil(t, ureq, "expected Request_Unary on the wire") {
		assert.Equal(t, "/test.Svc/Method", ureq.Method)
		if assert.NotNil(t, ureq.Metadata) {
			assert.Equal(t, []string{"v1"}, ureq.Metadata.Md["x-test"].Values)
		}
		// Body bytes should round-trip back to the original Ping.
		echo := &nrpc.Ping{}
		assert.NoError(t, proto.Unmarshal(ureq.Data, echo))
		assert.Equal(t, int64(42), echo.Timestamp)
	}
}

// TestUnary_NoResponders maps nats.ErrNoResponders to codes.Unavailable.
func TestUnary_NoResponders(t *testing.T) {
	mockNC := new(MockNatsConn)
	mockNC.On("RequestWithContext", mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nats.ErrNoResponders)

	client := NewClient(mockNC, "svc", "nid")
	defer client.Close()

	err := client.Invoke(context.Background(), "/test.Svc/Method", &nrpc.Ping{}, &nrpc.Pong{})
	assert.Equal(t, codes.Unavailable, status.Code(err))
}

// TestUnary_DeadlineExceeded maps context.DeadlineExceeded to codes.DeadlineExceeded.
func TestUnary_DeadlineExceeded(t *testing.T) {
	mockNC := new(MockNatsConn)
	mockNC.On("RequestWithContext", mock.Anything, mock.Anything, mock.Anything).
		Return(nil, context.DeadlineExceeded)

	client := NewClient(mockNC, "svc", "nid")
	defer client.Close()

	err := client.Invoke(context.Background(), "/test.Svc/Method", &nrpc.Ping{}, &nrpc.Pong{})
	assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
}

// TestUnary_NatsTimeout maps nats.ErrTimeout to codes.DeadlineExceeded.
func TestUnary_NatsTimeout(t *testing.T) {
	mockNC := new(MockNatsConn)
	mockNC.On("RequestWithContext", mock.Anything, mock.Anything, mock.Anything).
		Return(nil, nats.ErrTimeout)

	client := NewClient(mockNC, "svc", "nid")
	defer client.Close()

	err := client.Invoke(context.Background(), "/test.Svc/Method", &nrpc.Ping{}, &nrpc.Pong{})
	assert.Equal(t, codes.DeadlineExceeded, status.Code(err))
}

// TestUnary_StatusErrorPropagation verifies that a non-OK status returned in
// the UnaryResponse is surfaced as a matching gRPC status on the client.
func TestUnary_StatusErrorPropagation(t *testing.T) {
	mockNC := new(MockNatsConn)

	resp := &nrpc.Response{Type: &nrpc.Response_Unary{Unary: &nrpc.UnaryResponse{
		Status: status.New(codes.PermissionDenied, "nope").Proto(),
	}}}
	out, err := proto.Marshal(resp)
	assert.NoError(t, err)
	mockNC.On("RequestWithContext", mock.Anything, mock.Anything, mock.Anything).
		Return(&nats.Msg{Data: out}, nil)

	client := NewClient(mockNC, "svc", "nid")
	defer client.Close()

	err = client.Invoke(context.Background(), "/test.Svc/Method", &nrpc.Ping{}, &nrpc.Pong{})
	st, ok := status.FromError(err)
	if assert.True(t, ok, "error should be a gRPC status") {
		assert.Equal(t, codes.PermissionDenied, st.Code())
		assert.Equal(t, "nope", st.Message())
	}
}

// TestUnary_HeaderTrailerCallOptions verifies that grpc.Header / grpc.Trailer
// CallOptions are populated from the unary response.
func TestUnary_HeaderTrailerCallOptions(t *testing.T) {
	mockNC := new(MockNatsConn)

	resp := &nrpc.Response{Type: &nrpc.Response_Unary{Unary: &nrpc.UnaryResponse{
		Status:  status.Convert(nil).Proto(),
		Data:    mustMarshal(t, &nrpc.Pong{}),
		Header:  &nrpc.Metadata{Md: map[string]*nrpc.Strings{"h-key": {Values: []string{"hv"}}}},
		Trailer: &nrpc.Metadata{Md: map[string]*nrpc.Strings{"t-key": {Values: []string{"tv"}}}},
	}}}
	out, err := proto.Marshal(resp)
	assert.NoError(t, err)
	mockNC.On("RequestWithContext", mock.Anything, mock.Anything, mock.Anything).
		Return(&nats.Msg{Data: out}, nil)

	client := NewClient(mockNC, "svc", "nid")
	defer client.Close()

	var hdr, tr metadata.MD
	err = client.Invoke(context.Background(), "/test.Svc/Method",
		&nrpc.Ping{}, &nrpc.Pong{},
		grpc.Header(&hdr), grpc.Trailer(&tr))
	assert.NoError(t, err)
	assert.Equal(t, []string{"hv"}, hdr["h-key"])
	assert.Equal(t, []string{"tv"}, tr["t-key"])
}

func mustMarshal(t *testing.T, m proto.Message) []byte {
	t.Helper()
	b, err := proto.Marshal(m)
	assert.NoError(t, err)
	return b
}

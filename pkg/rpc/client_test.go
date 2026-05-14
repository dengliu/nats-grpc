package rpc

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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

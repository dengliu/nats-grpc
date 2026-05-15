package rpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/cloudwebrtc/nats-grpc/pkg/protos/nrpc"
	"github.com/cloudwebrtc/nats-grpc/pkg/utils"
	"github.com/nats-io/nats.go"
	log "github.com/pion/ion-log"
	"github.com/sirupsen/logrus"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type Client struct {
	nc        NatsConn
	ctx       context.Context
	cancel    context.CancelFunc
	log       *logrus.Logger
	streams   map[string]*clientStream
	svcid     string
	nid       string
	mu        sync.Mutex
	unaryInt  grpc.UnaryClientInterceptor
	streamInt grpc.StreamClientInterceptor
}

func NewClient(nc NatsConn, svcid string, nid string) *Client {
	return NewClientWithOptions(nc, svcid, nid)
}

// ClientOption is a functional option for configuring a Client
type ClientOption func(*Client)

// WithUnaryInterceptor returns a ClientOption that specifies the unary interceptor for the client
func WithUnaryInterceptor(interceptor grpc.UnaryClientInterceptor) ClientOption {
	return func(c *Client) {
		c.unaryInt = interceptor
	}
}

// WithStreamInterceptor returns a ClientOption that specifies the stream interceptor for the client
func WithStreamInterceptor(interceptor grpc.StreamClientInterceptor) ClientOption {
	return func(c *Client) {
		c.streamInt = interceptor
	}
}

func NewClientWithOptions(nc NatsConn, svcid string, nid string, opts ...ClientOption) *Client {
	c := &Client{
		nc:      nc,
		svcid:   svcid,
		nid:     nid,
		streams: make(map[string]*clientStream),
		log:     log.NewLoggerWithFields(log.DebugLevel, "nats-grpc.Client", log.Fields{"svc-id": svcid, "self-nid": nid}),
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	
	// Apply options
	for _, opt := range opts {
		opt(c)
	}
	
	return c
}

// Close gracefully stops a Client
func (p *Client) Close() error {
	p.cancel()
	p.mu.Lock()
	defer p.mu.Unlock()
	for name, st := range p.streams {
		err := st.done()
		if err != nil {
			p.log.Errorf("Unsubscribe [%v] failed %v", name, err)
			return err
		}
	}
	return nil
}

func (p *Client) CloseStream(nid string) bool {
	if p.svcid == nid {
		p.Close()
		return true
	}
	return false
}

func (c *Client) remove(subj string) {
	c.mu.Lock()
	delete(c.streams, subj)
	c.mu.Unlock()
}

// Invoke performs a unary RPC and returns after the request is received
// into reply.
func (c *Client) Invoke(ctx context.Context, method string, args interface{}, reply interface{}, opts ...grpc.CallOption) error {
	// If interceptor is set, use it
	if c.unaryInt != nil {
		return c.unaryInt(ctx, method, args, reply, nil, c.invoker, opts...)
	}
	// Otherwise, call directly
	return c.invoker(ctx, method, args, reply, nil, opts...)
}

// invoker is the actual RPC invocation logic for unary RPCs.
//
// Unary calls use NATS request/reply (a single message in, single message out)
// so that queue-group load balancing across multiple server replicas is safe:
// the entire RPC lands on exactly one replica. Streaming RPCs still go through
// newClientStream / NewStream and remain pinned to one server (see HEARTBEAT.md).
func (c *Client) invoker(ctx context.Context, method string, args interface{}, reply interface{}, _ *grpc.ClientConn, opts ...grpc.CallOption) error {
	prefix := "nrpc"
	if len(c.svcid) > 0 {
		prefix = fmt.Sprintf("nrpc.%v", c.svcid)
	}
	subj := prefix + strings.ReplaceAll(method, "/", ".")

	payload, err := proto.Marshal(args.(proto.Message))
	if err != nil {
		return err
	}

	req := &nrpc.Request{
		Type: &nrpc.Request_Unary{
			Unary: &nrpc.UnaryRequest{
				Method: method,
				Data:   payload,
			},
		},
	}
	if md, ok := metadata.FromOutgoingContext(ctx); ok {
		req.GetUnary().Metadata = utils.MakeMetadata(md)
	}

	reqBytes, err := proto.Marshal(req)
	if err != nil {
		return err
	}

	// Honor ctx deadline; nc.RequestWithContext returns nats.ErrTimeout when
	// the deadline fires, which we map to gRPC DeadlineExceeded.
	msg, err := c.nc.RequestWithContext(ctx, subj, reqBytes)
	if err != nil {
		switch err {
		case context.DeadlineExceeded, nats.ErrTimeout:
			return status.Error(codes.DeadlineExceeded, "deadline exceeded")
		case context.Canceled:
			return status.Error(codes.Canceled, "context canceled")
		case nats.ErrNoResponders:
			return status.Error(codes.Unavailable, "no responders")
		}
		return status.Error(codes.Unavailable, err.Error())
	}

	resp := &nrpc.Response{}
	if err := proto.Unmarshal(msg.Data, resp); err != nil {
		return err
	}
	u := resp.GetUnary()
	if u == nil {
		return status.Error(codes.Internal, "expected unary response")
	}

	// Populate optional header/trailer call options.
	for _, o := range opts {
		switch o := o.(type) {
		case grpc.HeaderCallOption:
			if u.Header != nil && o.HeaderAddr != nil {
				if *o.HeaderAddr == nil {
					*o.HeaderAddr = metadata.MD{}
				}
				for hdr, data := range u.Header.Md {
					o.HeaderAddr.Append(hdr, data.Values...)
				}
			}
		case grpc.TrailerCallOption:
			if u.Trailer != nil && o.TrailerAddr != nil {
				if *o.TrailerAddr == nil {
					*o.TrailerAddr = metadata.MD{}
				}
				for hdr, data := range u.Trailer.Md {
					o.TrailerAddr.Append(hdr, data.Values...)
				}
			}
		}
	}

	if u.Status != nil && u.Status.Code != int32(codes.OK) {
		return status.ErrorProto(u.Status)
	}
	return proto.Unmarshal(u.Data, reply.(proto.Message))
}

//NewStream begins a streaming RPC.
func (c *Client) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	// If stream interceptor is set, use it
	if c.streamInt != nil {
		return c.streamInt(ctx, desc, nil, method, c.streamer, opts...)
	}
	// Otherwise, call directly
	return c.streamer(ctx, desc, nil, method, opts...)
}

// streamer is the actual stream creation logic
func (c *Client) streamer(ctx context.Context, desc *grpc.StreamDesc, _ *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	prefix := "nrpc"
	if len(c.svcid) > 0 {
		prefix = fmt.Sprintf("nrpc.%v", c.svcid)
	}
	subj := prefix + strings.ReplaceAll(method, "/", ".")
	stream := newClientStream(ctx, c, subj, c.log, opts...)
	c.mu.Lock()
	c.streams[stream.reply] = stream
	c.mu.Unlock()
	return stream, nil
}

type clientStream struct {
	md            *metadata.MD
	header        *metadata.MD
	trailer       *metadata.MD
	lastErr       error
	ctx           context.Context
	cancel        context.CancelFunc
	log           *logrus.Logger
	client        *Client
	subject       string
	reply         string
	msgCh         chan *nats.Msg
	sub           *nats.Subscription
	closed        bool
	recvRead      <-chan []byte
	recvWrite     chan []byte
	hasBegun      bool
	pnid          string
	lastPongTime  time.Time
	pongMu        sync.Mutex
	pingInterval  time.Duration
	pongTimeout   time.Duration
	heartbeatStop chan struct{}
}

func newClientStream(ctx context.Context, client *Client, subj string, log *logrus.Logger, opts ...grpc.CallOption) *clientStream {
	stream := &clientStream{
		client:        client,
		log:           log,
		subject:       subj,
		reply:         utils.NewInBox(),
		closed:        false,
		pingInterval:  5 * time.Second,
		pongTimeout:   15 * time.Second,
		lastPongTime:  time.Now(),
		heartbeatStop: make(chan struct{}),
	}
	stream.ctx, stream.cancel = context.WithCancel(ctx)

	recv := make(chan []byte, 1)
	stream.recvRead = recv
	stream.recvWrite = recv

	stream.msgCh = make(chan *nats.Msg, 8192)
	stream.sub, _ = client.nc.ChanSubscribe(stream.reply, stream.msgCh)

	md, ok := metadata.FromOutgoingContext(ctx)
	if ok {
		//log.Printf("stream outgoing md => %v", md)
		stream.md = &md
	}

	for _, o := range opts {
		switch o := o.(type) {
		case grpc.HeaderCallOption:
			//log.Printf("o.HeaderAddr => %v", o.HeaderAddr)
			stream.header = o.HeaderAddr
		case grpc.TrailerCallOption:
			//log.Printf("o.TrailerAddr => %v", o.TrailerAddr)
			stream.trailer = o.TrailerAddr
		case grpc.PeerCallOption:
		case grpc.PerRPCCredsCallOption:
		case grpc.FailFastCallOption:
		case grpc.MaxRecvMsgSizeCallOption:
		case grpc.MaxSendMsgSizeCallOption:
		case grpc.CompressorCallOption:
		case grpc.ContentSubtypeCallOption:
		}
	}

	go stream.ReadMsg()
	go stream.pingLoop()
	go stream.pongMonitor()
	return stream
}

func (c *clientStream) Header() (metadata.MD, error) {
	if c.header == nil {
		c.header = &metadata.MD{}
	}
	return *c.header, nil
}

func (c *clientStream) Trailer() metadata.MD {
	if c.trailer == nil {
		c.trailer = &metadata.MD{}
	}
	return *c.trailer
}

func (c *clientStream) CloseSend() error {
	c.log.Info("Client CloseSend")
	c.writeEnd(&nrpc.End{
		Status: status.Convert(nil).Proto(),
	})
	return c.done()
}

func (c *clientStream) close(err error) {
	c.writeEnd(&nrpc.End{
		Status: status.Convert(err).Proto(),
	})
	c.done()
}

func (c *clientStream) Context() context.Context {
	return c.ctx
}

func (c *clientStream) onMessage(msg *nats.Msg) error {
	response := &nrpc.Response{}
	err := proto.Unmarshal(msg.Data, response)
	if err != nil {
		c.log.WithField("data", string(msg.Data)).Error("unknown message")
		return err
	}

	switch r := response.Type.(type) {
	case *nrpc.Response_Begin:
		//c.log.WithField("call", r.Begin).Info("recv call")
		c.processBegin(r.Begin)
	case *nrpc.Response_Data:
		//c.log.WithField("data", r.Data).Info("recv data")
		c.processData(r.Data)
	case *nrpc.Response_End:
		//c.log.WithField("end", r.End).Info("recv end")
		return c.processEnd(r.End)
	case *nrpc.Response_Pong:
		//c.log.WithField("pong", r.Pong).Debug("recv pong")
		c.processPong(r.Pong)
	}
	return nil
}

func (c *clientStream) ReadMsg() error {
	for {
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		case msg, ok := <-c.msgCh:
			if ok {
				err := c.onMessage(msg)
				if err != nil {
					c.lastErr = err
					return err
				}
				break
			}
			return io.EOF
		}
	}
}

func (c *clientStream) done() error {
	if !c.closed {
		c.closed = true
		c.cancel()
		close(c.heartbeatStop) // Stop heartbeat goroutines
		err := c.sub.Unsubscribe()
		close(c.msgCh)
		c.client.remove(c.subject)
		return err
	}
	return errors.New("Client Streaming already closed")
}

func (c *clientStream) SendMsg(m interface{}) error {
	if c.closed {
		return fmt.Errorf("client streaming closed=true")
	}

	if !c.hasBegun {
		c.hasBegun = true
		call := &nrpc.Call{
			Method: c.subject,
			Nid:    c.client.nid,
		}
		if c.md != nil {
			call.Metadata = utils.MakeMetadata(*c.md)
		}
		//write call with metatdata
		c.writeCall(call)
	}

	var data *nrpc.Data
	if frame, ok := m.(*Frame); ok {
		data = &nrpc.Data{
			Data: frame.Payload,
		}
	} else {
		payload, err := proto.Marshal(m.(proto.Message))
		if err != nil {
			c.log.Errorf("clientStream.SendMsg failed: %v", err)
			return err
		}
		data = &nrpc.Data{
			Data: payload,
		}
	}
	//write grpc args
	return c.writeData(data)
}

func (c *clientStream) RecvMsg(m interface{}) error {
	select {
	case <-c.ctx.Done():
		if c.lastErr != nil {
			return c.lastErr
		}
		// Convert context errors to gRPC status codes for proper metrics reporting
		if c.ctx.Err() == context.DeadlineExceeded {
			c.log.Errorf("context deadline exceeded for c.RecvMsg")
			return status.Error(codes.DeadlineExceeded, "deadline exceeded")
		}
		return status.Error(codes.Canceled, "context canceled")
	case bytes, ok := <-c.recvRead:
		if !ok {
			return io.EOF
		}

		if frame, ok := m.(*Frame); ok {
			frame.Payload = bytes
			return nil
		}
		return proto.Unmarshal(bytes, m.(proto.Message))
	}
}

func (c *clientStream) Invoke(ctx context.Context, method string, args interface{}, reply interface{}, opts ...grpc.CallOption) error {
	payload, err := proto.Marshal(args.(proto.Message))
	if err != nil {
		c.log.Fatalf("%v for request", err)
		return err
	}

	//write call with metatdata
	call := &nrpc.Call{
		Method: method,
	}
	if c.md != nil {
		call.Metadata = utils.MakeMetadata(*c.md)
	}

	c.writeCall(call)

	//write grpc args
	c.writeData(&nrpc.Data{
		Data: payload,
	})

	err = c.RecvMsg(reply)

	if err != nil {
		c.log.Errorf("%v for c.RecvMsg", err)
	}

	c.CloseSend()

	return err
}

func (c *clientStream) writeRequest(request *nrpc.Request) error {
	//c.log.WithField("request", request).Info("send")
	data, err := proto.Marshal(request)
	if err != nil {
		return err
	}
	return c.client.nc.PublishRequest(c.subject, c.reply, data)
}

func (c *clientStream) writeCall(call *nrpc.Call) error {
	return c.writeRequest(&nrpc.Request{
		Type: &nrpc.Request_Call{
			Call: call,
		},
	})
}

func (c *clientStream) writeData(data *nrpc.Data) error {
	return c.writeRequest(&nrpc.Request{
		Type: &nrpc.Request_Data{
			Data: data,
		},
	})
}

func (c *clientStream) writeEnd(end *nrpc.End) error {
	return c.writeRequest(&nrpc.Request{
		Type: &nrpc.Request_End{
			End: end,
		},
	})
}

func (c *clientStream) processBegin(begin *nrpc.Begin) error {
	c.log.Debugf("nrpc.Begin: %v", begin.Header)
	if begin.Header != nil {
		if c.header == nil {
			c.header = &metadata.MD{}
		}
		for hdr, data := range begin.Header.Md {
			c.header.Append(hdr, data.Values...)
		}
	}
	c.pnid = begin.Nid
	return nil
}

func (c *clientStream) processData(data *nrpc.Data) {
	if c.recvWrite == nil {
		c.log.Error("data received after client closeSend")
		return
	}
	c.recvWrite <- data.Data
}

func (c *clientStream) processEnd(end *nrpc.End) error {

	if end.Trailer != nil && c.trailer != nil {
		if c.trailer == nil {
			c.trailer = &metadata.MD{}
		}
		for hdr, data := range end.Trailer.Md {
			c.trailer.Append(hdr, data.Values...)
		}
	}

	if end.Status != nil {
		c.log.WithField("status", end.Status).Info("cancel")
		c.done()
		return status.Error(codes.Code(end.Status.Code), end.Status.GetMessage())
	}
	c.log.Info("Server CloseSend")
	c.recvWrite <- nil
	close(c.recvWrite)
	c.recvWrite = nil
	return nil
}

// pingLoop periodically sends Ping messages to the server
func (c *clientStream) pingLoop() {
	ticker := time.NewTicker(c.pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.heartbeatStop:
			return
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			if c.closed {
				return
			}
			err := c.writePing(&nrpc.Ping{
				Timestamp: time.Now().UnixNano(),
			})
			if err != nil {
				c.log.WithError(err).Debug("failed to send ping")
			}
		}
	}
}

// pongMonitor monitors for Pong timeout and closes stream if no Pong received
func (c *clientStream) pongMonitor() {
	ticker := time.NewTicker(c.pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.heartbeatStop:
			return
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.pongMu.Lock()
			elapsed := time.Since(c.lastPongTime)
			c.pongMu.Unlock()

			if elapsed > c.pongTimeout {
				c.log.WithField("elapsed", elapsed).Error("heartbeat timeout: no pong received")
				c.close(status.Error(codes.Unavailable, "server heartbeat timeout"))
				return
			}
		}
	}
}

// processPong handles incoming Pong messages by updating lastPongTime
func (c *clientStream) processPong(pong *nrpc.Pong) {
	c.pongMu.Lock()
	c.lastPongTime = time.Now()
	c.pongMu.Unlock()
}

// writePing sends a Ping request to the server
func (c *clientStream) writePing(ping *nrpc.Ping) error {
	return c.writeRequest(&nrpc.Request{
		Type: &nrpc.Request_Ping{
			Ping: ping,
		},
	})
}

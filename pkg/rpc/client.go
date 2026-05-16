package rpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudwebrtc/nats-grpc/pkg/protos/nrpc"
	"github.com/cloudwebrtc/nats-grpc/pkg/utils"
	"github.com/nats-io/nats.go"
	log "github.com/pion/ion-log"
	"github.com/sirupsen/logrus"
	grpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Heartbeat defaults preserved from the original implementation.
const (
	defaultPingInterval = 5 * time.Second
	defaultPongTimeout  = 15 * time.Second
)

type Client struct {
	nc            NatsConn
	ctx           context.Context
	cancel        context.CancelFunc
	log           *logrus.Logger
	streams       map[string]*clientStream
	svcid         string
	nid           string
	mu            sync.Mutex
	unaryInt      grpc.UnaryClientInterceptor
	streamInt     grpc.StreamClientInterceptor
	statsHandlers []stats.Handler

	// pingInterval and pongTimeout apply to every new streaming RPC. A
	// pingInterval of 0 disables the heartbeat entirely (no ping/pong
	// goroutines spawned per stream); useful for very high stream
	// fan-out where the goroutine overhead dwarfs the liveness benefit.
	pingInterval time.Duration
	pongTimeout  time.Duration
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

// WithHeartbeat configures the per-stream Ping/Pong cadence for streaming
// RPCs. interval is how often the client sends a Ping; timeout is how long
// the client waits for any Pong before declaring the server dead and
// closing the stream with codes.Unavailable.
//
// Constraints:
//   - timeout must be > interval — otherwise a single dropped pong races
//     the very next ping and the stream tears itself down.
//   - interval == 0 disables the heartbeat entirely. The pingLoop /
//     pongMonitor goroutines are not started, so streams rely solely on
//     the underlying ctx deadline / NATS connection loss to detect death.
//
// Defaults: 5s interval, 15s timeout (preserving prior behavior).
func WithHeartbeat(interval, timeout time.Duration) ClientOption {
	return func(c *Client) {
		c.pingInterval = interval
		c.pongTimeout = timeout
	}
}

// WithoutHeartbeat disables the streaming heartbeat. Equivalent to
// WithHeartbeat(0, 0).
func WithoutHeartbeat() ClientOption {
	return WithHeartbeat(0, 0)
}

// WithStatsHandler registers a stats.Handler with the client. Multiple
// handlers may be registered; they are invoked in registration order at each
// lifecycle event (TagRPC, Begin, OutPayload, InPayload, End), matching
// grpc-go's behavior for repeated stats.Handler options.
//
// This is the integration point for libraries like otelgrpc — pass
// otelgrpc.NewClientHandler() to get RPC-level traces and metrics without
// pulling otelgrpc into pkg/rpc.
func WithStatsHandler(h stats.Handler) ClientOption {
	return func(c *Client) {
		c.statsHandlers = append(c.statsHandlers, h)
	}
}

// tagRPC fans TagRPC out across registered stats handlers. Each handler may
// attach values to ctx; the cumulative ctx is returned for downstream use.
func (c *Client) tagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	for _, h := range c.statsHandlers {
		ctx = h.TagRPC(ctx, info)
	}
	return ctx
}

// handleRPC fans HandleRPC out across registered stats handlers.
func (c *Client) handleRPC(ctx context.Context, s stats.RPCStats) {
	for _, h := range c.statsHandlers {
		h.HandleRPC(ctx, s)
	}
}

func NewClientWithOptions(nc NatsConn, svcid string, nid string, opts ...ClientOption) *Client {
	c := &Client{
		nc:           nc,
		svcid:        svcid,
		nid:          nid,
		streams:      make(map[string]*clientStream),
		log:          log.NewLoggerWithFields(log.DebugLevel, "nats-grpc.Client", log.Fields{"svc-id": svcid, "self-nid": nid}),
		pingInterval: defaultPingInterval,
		pongTimeout:  defaultPongTimeout,
	}
	c.ctx, c.cancel = context.WithCancel(context.Background())
	
	// Apply options
	for _, opt := range opts {
		opt(c)
	}
	
	return c
}

// Close gracefully stops a Client. Snapshot the streams under the mutex,
// then release it before calling st.done() — done() walks the same map
// (via Client.remove keyed by stream.reply) and would otherwise deadlock
// against this Close-held mutex.
func (p *Client) Close() error {
	p.cancel()
	p.mu.Lock()
	snapshot := make(map[string]*clientStream, len(p.streams))
	for k, v := range p.streams {
		snapshot[k] = v
	}
	p.mu.Unlock()
	var firstErr error
	for name, st := range snapshot {
		if err := st.done(); err != nil {
			p.log.Errorf("Unsubscribe [%v] failed %v", name, err)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
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
//
// Lifecycle events fired to any registered stats.Handler:
//
//	TagRPC → Begin → OutPayload(request) → InPayload(response) → End
//
// End fires exactly once with the final error (or nil on success).
func (c *Client) invoker(ctx context.Context, method string, args interface{}, reply interface{}, _ *grpc.ClientConn, opts ...grpc.CallOption) error {
	ctx = c.tagRPC(ctx, &stats.RPCTagInfo{FullMethodName: method})
	beginTime := time.Now()
	c.handleRPC(ctx, &stats.Begin{Client: true, BeginTime: beginTime, FailFast: true})

	var rpcErr error
	defer func() {
		c.handleRPC(ctx, &stats.End{
			Client:    true,
			BeginTime: beginTime,
			EndTime:   time.Now(),
			Error:     rpcErr,
		})
	}()

	prefix := "nrpc"
	if len(c.svcid) > 0 {
		prefix = fmt.Sprintf("nrpc.%v", c.svcid)
	}
	subj := prefix + strings.ReplaceAll(method, "/", ".")

	payload, err := proto.Marshal(args.(proto.Message))
	if err != nil {
		rpcErr = err
		return err
	}
	c.handleRPC(ctx, &stats.OutPayload{
		Client:           true,
		Payload:          args,
		Length:           len(payload),
		CompressedLength: len(payload),
		SentTime:         time.Now(),
	})

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
		rpcErr = err
		return err
	}

	// Honor ctx deadline; nc.RequestWithContext returns nats.ErrTimeout when
	// the deadline fires, which we map to gRPC DeadlineExceeded.
	msg, err := c.nc.RequestWithContext(ctx, subj, reqBytes)
	if err != nil {
		switch err {
		case context.DeadlineExceeded, nats.ErrTimeout:
			rpcErr = status.Error(codes.DeadlineExceeded, "deadline exceeded")
		case context.Canceled:
			rpcErr = status.Error(codes.Canceled, "context canceled")
		case nats.ErrNoResponders:
			rpcErr = status.Error(codes.Unavailable, "no responders")
		default:
			rpcErr = status.Error(codes.Unavailable, err.Error())
		}
		return rpcErr
	}

	resp := &nrpc.Response{}
	if err := proto.Unmarshal(msg.Data, resp); err != nil {
		rpcErr = err
		return err
	}
	u := resp.GetUnary()
	if u == nil {
		rpcErr = status.Error(codes.Internal, "expected unary response")
		return rpcErr
	}

	// Populate optional header/trailer call options. grpc-go's contract is
	// to overwrite *HeaderAddr / *TrailerAddr with a fresh metadata.MD —
	// not to append into whatever the caller previously stored — so a
	// reused &md doesn't accumulate duplicate keys across calls.
	for _, o := range opts {
		switch o := o.(type) {
		case grpc.HeaderCallOption:
			if o.HeaderAddr != nil {
				md := metadata.MD{}
				if u.Header != nil {
					for hdr, data := range u.Header.Md {
						md.Append(hdr, data.Values...)
					}
				}
				*o.HeaderAddr = md
			}
		case grpc.TrailerCallOption:
			if o.TrailerAddr != nil {
				md := metadata.MD{}
				if u.Trailer != nil {
					for hdr, data := range u.Trailer.Md {
						md.Append(hdr, data.Values...)
					}
				}
				*o.TrailerAddr = md
			}
		}
	}

	// Unmarshal the body before checking status, so we can emit InPayload
	// with the deserialized message — matching grpc-go's stats ordering
	// (bytes → InPayload(Payload=deserialized) → return). Skip unmarshal
	// when the server reported an error: by protocol the data field is
	// empty in that case, but if a future server packs error details into
	// Data we'd otherwise overwrite the reply with garbage.
	statusOK := u.Status == nil || u.Status.Code == int32(codes.OK)
	if statusOK && len(u.Data) > 0 {
		if err := proto.Unmarshal(u.Data, reply.(proto.Message)); err != nil {
			rpcErr = err
			return err
		}
	}

	// Fire InPayload whenever bytes were on the wire — including non-OK
	// responses that happen to carry payload. Length reports the raw
	// wire size; Payload is the deserialized message when available.
	if len(u.Data) > 0 {
		ev := &stats.InPayload{
			Client:           true,
			Length:           len(u.Data),
			CompressedLength: len(u.Data),
			RecvTime:         time.Now(),
		}
		if statusOK {
			ev.Payload = reply
		}
		c.handleRPC(ctx, ev)
	}

	if !statusOK {
		rpcErr = status.ErrorProto(u.Status)
		return rpcErr
	}
	return nil
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
	stream := newClientStream(ctx, c, method, subj, c.log, opts...)
	c.mu.Lock()
	c.streams[stream.reply] = stream
	c.mu.Unlock()
	return stream, nil
}

type clientStream struct {
	md  *metadata.MD
	header  *metadata.MD
	trailer *metadata.MD
	// lastErr captures the first transport-level error observed by ReadMsg.
	// RecvMsg reads it after ctx.Done fires; ReadMsg and RecvMsg run on
	// different goroutines, so the field must be guarded.
	lastErrMu sync.Mutex
	lastErr   error
	ctx           context.Context
	cancel        context.CancelFunc
	log           *logrus.Logger
	client        *Client
	method        string
	subject       string
	reply         string
	msgCh         chan *nats.Msg
	sub           *nats.Subscription
	closed        atomic.Bool
	recvRead      <-chan []byte
	recvWrite     chan []byte
	hasBegun      bool
	pnid          string
	lastPongTime  time.Time
	pongMu        sync.Mutex
	pingInterval  time.Duration
	pongTimeout   time.Duration
	heartbeatStop chan struct{}

	// Stats handler lifecycle. statsCtx is the ctx returned by TagRPC; it
	// becomes the parent for HandleRPC events fired during the stream.
	// beginTime stamps the stats.Begin event; endErr captures the final
	// error reported on stats.End. endOnce guards stats.End so it fires at
	// most once even when done() is called from several close paths
	// (CloseSend, close(err), processEnd).
	beginTime time.Time
	endErr    error
	statsCtx  context.Context
	endOnce   sync.Once
}

func newClientStream(ctx context.Context, client *Client, method, subj string, log *logrus.Logger, opts ...grpc.CallOption) *clientStream {
	// TagRPC may attach values to ctx (e.g. a tracing span). Use the returned
	// ctx for downstream HandleRPC calls and as the parent of stream.ctx so
	// any tagged values propagate to the user's handler.
	statsCtx := client.tagRPC(ctx, &stats.RPCTagInfo{FullMethodName: method})
	begin := time.Now()
	client.handleRPC(statsCtx, &stats.Begin{Client: true, BeginTime: begin, FailFast: true})

	stream := &clientStream{
		client:        client,
		log:           log,
		method:        method,
		subject:       subj,
		reply:         utils.NewInBox(),
		pingInterval:  client.pingInterval,
		pongTimeout:   client.pongTimeout,
		lastPongTime:  time.Now(),
		heartbeatStop: make(chan struct{}),
		beginTime:     begin,
		statsCtx:      statsCtx,
	}
	stream.ctx, stream.cancel = context.WithCancel(statsCtx)

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
	// Skip the heartbeat goroutines entirely when configured off
	// (pingInterval <= 0). This is the documented WithoutHeartbeat /
	// WithHeartbeat(0, 0) path — no goroutines, no per-stream ticker, no
	// per-tick allocation.
	if stream.pingInterval > 0 {
		go stream.pingLoop()
		go stream.pongMonitor()
	}
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
	c.endErr = err
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
					c.setLastErr(err)
					return err
				}
				break
			}
			return io.EOF
		}
	}
}

func (c *clientStream) done() error {
	// CompareAndSwap guarantees done()'s body runs at most once even when
	// called concurrently from CloseSend, close(err), and processEnd.
	if c.closed.CompareAndSwap(false, true) {
		c.endOnce.Do(func() {
			c.client.handleRPC(c.statsCtx, &stats.End{
				Client:    true,
				BeginTime: c.beginTime,
				EndTime:   time.Now(),
				Error:     c.endErr,
			})
		})
		c.cancel()
		close(c.heartbeatStop) // Stop heartbeat goroutines
		err := c.sub.Unsubscribe()
		close(c.msgCh)
		// Client.streams is keyed by reply (see Client.streamer); removing by
		// subject silently no-ops and leaks the entry for the life of the
		// process.
		c.client.remove(c.reply)
		return err
	}
	return errors.New("Client Streaming already closed")
}

func (c *clientStream) SendMsg(m interface{}) error {
	if c.closed.Load() {
		return fmt.Errorf("client streaming closed=true")
	}

	if !c.hasBegun {
		c.hasBegun = true
		call := &nrpc.Call{
			// Carry the canonical gRPC method path (e.g.
			// `/pkg.Service/Method`) so the server can tag stats
			// handlers consistently with the unary fast path. The NATS
			// subject is already on msg.Subject if anything downstream
			// needs it.
			Method: c.method,
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
	c.client.handleRPC(c.statsCtx, &stats.OutPayload{
		Client:           true,
		Payload:          m,
		Length:           len(data.Data),
		CompressedLength: len(data.Data),
		SentTime:         time.Now(),
	})
	//write grpc args
	return c.writeData(data)
}

func (c *clientStream) setLastErr(err error) {
	c.lastErrMu.Lock()
	if c.lastErr == nil {
		c.lastErr = err
	}
	c.lastErrMu.Unlock()
}

func (c *clientStream) getLastErr() error {
	c.lastErrMu.Lock()
	defer c.lastErrMu.Unlock()
	return c.lastErr
}

func (c *clientStream) RecvMsg(m interface{}) error {
	select {
	case <-c.ctx.Done():
		if err := c.getLastErr(); err != nil {
			return err
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
	c.client.handleRPC(c.statsCtx, &stats.InPayload{
		Client:           true,
		Length:           len(data.Data),
		CompressedLength: len(data.Data),
		RecvTime:         time.Now(),
	})
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

	// Non-OK status from the server is a real cancel/error close. OK status
	// (or nil) is the normal server half-close — closing the recv channel
	// signals EOF to RecvMsg without a phantom empty-message frame.
	if end.Status != nil && end.Status.Code != int32(codes.OK) {
		c.log.WithField("status", end.Status).Info("cancel")
		c.endErr = status.ErrorProto(end.Status)
		c.done()
		return status.Error(codes.Code(end.Status.Code), end.Status.GetMessage())
	}
	c.log.Info("Server CloseSend")
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
			if c.closed.Load() {
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

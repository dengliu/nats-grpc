package rpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/cloudwebrtc/nats-grpc/pkg/protos/nrpc"
	"github.com/cloudwebrtc/nats-grpc/pkg/utils"
	"github.com/nats-io/nats.go"
	log "github.com/pion/ion-log"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// redefine grpc.serverMethodHandler as it is not exposed
type serverMethodHandler func(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error)

// unaryHandler dispatches a single UnaryRequest message and writes the reply
// to msg.Reply. Used for queue-group load-balanced unary RPCs.
type unaryHandler func(srv *Server, msg *nats.Msg, req *nrpc.UnaryRequest)

type serverTransportStream struct {
	stream *serverStream
}

func (s *serverTransportStream) Method() string {
	return s.stream.method
}
func (s *serverTransportStream) SetHeader(md metadata.MD) error {
	return s.stream.SetHeader(md)
}

func (s *serverTransportStream) SendHeader(md metadata.MD) error {
	return s.stream.SendHeader(md)
}

func (s *serverTransportStream) SetTrailer(md metadata.MD) error {
	s.stream.SetTrailer(md)
	return nil
}

func serverUnaryHandler(srv interface{}, handler serverMethodHandler) handlerFunc {
	return func(s *serverStream) {
		// Use the server's unary interceptor if set
		interceptor := s.server.unaryInt
		ctx := grpc.NewContextWithServerTransportStream(s.Context(), &serverTransportStream{stream: s})
		if s.md != nil {
			ctx = metadata.NewIncomingContext(ctx, s.md)
		}
		response, err := handler(srv, ctx, s.RecvMsg, interceptor)
		if s.ctx.Err() == nil {
			if err != nil {
				s.close(err)
				return
			}
			if s.SendMsg(response) == nil {
				s.close(err)
			}
		}
	}
}

// serverUnaryRequestHandler builds a single-shot handler used by the unary
// queue subscription. The request is delivered as a UnaryRequest and the reply
// is written back to msg.Reply via msg.Respond — no per-call subscription or
// stream state is created on either side.
//
// Lifecycle events fired to any registered stats.Handler:
//
//	TagRPC → Begin → InPayload(request) → OutPayload(response) → End
//
// End fires exactly once with the handler's error (or nil on success);
// framework-level errors (marshal/respond) are logged but don't override
// the handler's error in the End event.
func serverUnaryRequestHandler(srv interface{}, handler serverMethodHandler) unaryHandler {
	return func(s *Server, msg *nats.Msg, ureq *nrpc.UnaryRequest) {
		interceptor := s.unaryInt

		ctx := context.Background()
		if ureq.Metadata != nil {
			md := make(metadata.MD)
			for hdr, data := range ureq.Metadata.Md {
				md[hdr] = data.Values
			}
			ctx = metadata.NewIncomingContext(ctx, md)
		}

		ctx = s.tagRPC(ctx, &stats.RPCTagInfo{FullMethodName: ureq.Method})
		beginTime := time.Now()
		s.handleRPC(ctx, &stats.Begin{BeginTime: beginTime})
		s.handleRPC(ctx, &stats.InPayload{
			Length:           len(ureq.Data),
			CompressedLength: len(ureq.Data),
			RecvTime:         time.Now(),
		})

		// dec is called by the generated gRPC handler once to unmarshal the
		// request body. We have the bytes already; this just decodes them.
		dec := func(in interface{}) error {
			return proto.Unmarshal(ureq.Data, in.(proto.Message))
		}

		response, err := handler(srv, ctx, dec, interceptor)

		defer func() {
			s.handleRPC(ctx, &stats.End{
				BeginTime: beginTime,
				EndTime:   time.Now(),
				Error:     err,
			})
		}()

		resp := &nrpc.Response{Type: &nrpc.Response_Unary{Unary: &nrpc.UnaryResponse{}}}
		u := resp.GetUnary()
		if err != nil {
			u.Status = status.Convert(err).Proto()
		} else {
			payload, marshalErr := proto.Marshal(response.(proto.Message))
			if marshalErr != nil {
				u.Status = status.Convert(marshalErr).Proto()
			} else {
				u.Data = payload
				u.Status = status.Convert(nil).Proto()
				s.handleRPC(ctx, &stats.OutPayload{
					Payload:          response,
					Length:           len(payload),
					CompressedLength: len(payload),
					SentTime:         time.Now(),
				})
			}
		}

		out, marshalErr := proto.Marshal(resp)
		if marshalErr != nil {
			s.log.Errorf("marshal unary response: %v", marshalErr)
			return
		}
		if respondErr := msg.Respond(out); respondErr != nil {
			s.log.Errorf("respond unary: %v", respondErr)
		}
	}
}

func serverStreamHandler(srv interface{}, handler grpc.StreamHandler) handlerFunc {
	return func(s *serverStream) {
		// If stream interceptor is set, use it
		if s.server.streamInt != nil {
			info := &grpc.StreamServerInfo{
				FullMethod:     s.method,
				IsClientStream: true, // conservative assumption
				IsServerStream: true,
			}
			err := s.server.streamInt(srv, s, info, handler)
			if s.ctx.Err() == nil {
				s.close(err)
			}
			return
		}
		// Otherwise call handler directly
		err := handler(srv, s)
		if s.ctx.Err() == nil {
			s.close(err)
		}
	}
}

type handlerFunc func(s *serverStream)

// serviceInfo wraps information about a service. It is very similar to
// ServiceDesc and is constructed from it for internal purposes.
type serviceInfo struct {
	// Contains the implementation for the methods in this service.
	serviceImpl interface{}
	methods     map[string]*grpc.MethodDesc
	streams     map[string]*grpc.StreamDesc
	mdata       interface{}
}

// Server is the interface to gRPC over NATS
type Server struct {
	nc            NatsConn
	ctx           context.Context
	cancel        context.CancelFunc
	log           *logrus.Logger
	handlers      map[string]handlerFunc
	unaryHandlers map[string]unaryHandler // method -> unary fast-path handler
	streams       map[string]*serverStream
	mu            sync.Mutex
	subs          map[string]*nats.Subscription
	nid           string
	services      map[string]*serviceInfo // service name -> service info
	unaryInt      grpc.UnaryServerInterceptor
	streamInt     grpc.StreamServerInterceptor
	statsHandlers []stats.Handler
}

// NewServer creates a new Proxy
func NewServer(nc NatsConn, nid string) *Server {
	return NewServerWithOptions(nc, nid)
}

// ServerOption is a functional option for configuring a Server
type ServerOption func(*Server)

// WithUnaryServerInterceptor returns a ServerOption that specifies the unary interceptor for the server
func WithUnaryServerInterceptor(interceptor grpc.UnaryServerInterceptor) ServerOption {
	return func(s *Server) {
		s.unaryInt = interceptor
	}
}

// WithStreamServerInterceptor returns a ServerOption that specifies the stream interceptor for the server
func WithStreamServerInterceptor(interceptor grpc.StreamServerInterceptor) ServerOption {
	return func(s *Server) {
		s.streamInt = interceptor
	}
}

// WithServerStatsHandler registers a stats.Handler with the server. Multiple
// handlers may be registered; they are invoked in registration order at each
// lifecycle event (TagRPC, Begin, InPayload, OutPayload, End), matching
// grpc-go's behavior for repeated stats.Handler options.
//
// This is the integration point for libraries like otelgrpc — pass
// otelgrpc.NewServerHandler() to get RPC-level traces and metrics without
// pulling otelgrpc into pkg/rpc.
func WithServerStatsHandler(h stats.Handler) ServerOption {
	return func(s *Server) {
		s.statsHandlers = append(s.statsHandlers, h)
	}
}

// tagRPC fans TagRPC out across registered stats handlers. Each handler may
// attach values to ctx; the cumulative ctx is returned for downstream use.
func (s *Server) tagRPC(ctx context.Context, info *stats.RPCTagInfo) context.Context {
	for _, h := range s.statsHandlers {
		ctx = h.TagRPC(ctx, info)
	}
	return ctx
}

// handleRPC fans HandleRPC out across registered stats handlers.
func (s *Server) handleRPC(ctx context.Context, rs stats.RPCStats) {
	for _, h := range s.statsHandlers {
		h.HandleRPC(ctx, rs)
	}
}

// NewServerWithOptions creates a new Server with options
func NewServerWithOptions(nc NatsConn, nid string, opts ...ServerOption) *Server {
	s := &Server{
		nc:            nc,
		handlers:      make(map[string]handlerFunc),
		unaryHandlers: make(map[string]unaryHandler),
		streams:       make(map[string]*serverStream),
		subs:          make(map[string]*nats.Subscription),
		services:      make(map[string]*serviceInfo),
		log:           log.NewLoggerWithFields(log.DebugLevel, "nats-grpc.Server", log.Fields{"self-nid": nid}),
		nid:           nid,
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())
	
	// Apply options
	for _, opt := range opts {
		opt(s)
	}
	
	return s
}

// Stop gracefully stops the server. All in-flight streams are torn down
// with a stats.End event so observability backends don't see orphaned
// long-running spans/metrics, and NATS subscriptions are unsubscribed.
func (s *Server) Stop() {
	// Snapshot the streams under the mutex; done() mutates the same map.
	s.mu.Lock()
	streams := make([]*serverStream, 0, len(s.streams))
	for _, st := range s.streams {
		streams = append(streams, st)
	}
	s.mu.Unlock()
	stopErr := status.Error(codes.Unavailable, "server stopping")
	for _, st := range streams {
		// finalize is endOnce-guarded so it's safe to race with the stream's
		// own close paths (handler completion, processEnd error). The first
		// finalize call wins; later ones no-op. No wire End is written here —
		// the connection may already be torn down.
		st.finalize(stopErr)
	}

	s.cancel()
	for name, sub := range s.subs {
		err := sub.Unsubscribe()
		if err != nil {
			s.log.Errorf("Unsubscribe [%v] failed %v", name, err)
		}
	}
}

func (s *Server) CloseStream(nid string) error {
	for name, st := range s.streams {
		if st.pnid == nid {
			st.done()
			s.log.Infof("CloseStream nid = %v, name = %v", nid, name)
		}
	}
	return nil
}

// RegisterService is used to register gRPC services
func (s *Server) RegisterService(sd *grpc.ServiceDesc, ss interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	prefix := fmt.Sprintf("nrpc.%v", sd.ServiceName)
	if len(s.nid) > 0 {
		prefix = fmt.Sprintf("nrpc.%v.%v", s.nid, sd.ServiceName)
	}
	subject := prefix + ".>"
	s.log.Infof("QueueSubscribe: subject => %v, queue => %v", subject, sd.ServiceName)
	sub, _ := s.nc.QueueSubscribe(subject, sd.ServiceName, s.onMessage)

	s.subs[sd.ServiceName] = sub
	for _, it := range sd.Methods {
		desc := it
		path := fmt.Sprintf("%v.%v", prefix, desc.MethodName)
		s.handlers[path] = serverUnaryHandler(ss, serverMethodHandler(desc.Handler))
		s.unaryHandlers[path] = serverUnaryRequestHandler(ss, serverMethodHandler(desc.Handler))
		s.log.Infof("RegisterService: method path => %v", path)
	}
	for _, it := range sd.Streams {
		desc := it
		path := fmt.Sprintf("%v.%v", prefix, desc.StreamName)
		s.handlers[path] = serverStreamHandler(ss, desc.Handler)
		s.log.Infof("RegisterService: stream path => %v", path)
	}
	s.nc.Flush()

	s.register(sd, ss)
}

func (s *Server) register(sd *grpc.ServiceDesc, ss interface{}) {
	s.log.Infof("RegisterService(%q)", sd.ServiceName)

	if _, ok := s.services[sd.ServiceName]; ok {
		s.log.Fatalf("grpc: Server.RegisterService found duplicate service registration for %q", sd.ServiceName)
	}
	info := &serviceInfo{
		serviceImpl: ss,
		methods:     make(map[string]*grpc.MethodDesc),
		streams:     make(map[string]*grpc.StreamDesc),
		mdata:       sd.Metadata,
	}
	for i := range sd.Methods {
		d := &sd.Methods[i]
		info.methods[d.MethodName] = d
	}
	for i := range sd.Streams {
		d := &sd.Streams[i]
		info.streams[d.StreamName] = d
	}
	s.services[sd.ServiceName] = info
}

func (s *Server) GetServiceInfo() map[string]grpc.ServiceInfo {
	ret := make(map[string]grpc.ServiceInfo)
	for n, srv := range s.services {
		methods := make([]grpc.MethodInfo, 0, len(srv.methods)+len(srv.streams))
		for m := range srv.methods {
			methods = append(methods, grpc.MethodInfo{
				Name:           m,
				IsClientStream: false,
				IsServerStream: false,
			})
		}
		for m, d := range srv.streams {
			methods = append(methods, grpc.MethodInfo{
				Name:           m,
				IsClientStream: d.ClientStreams,
				IsServerStream: d.ServerStreams,
			})
		}

		ret[n] = grpc.ServiceInfo{
			Methods:  methods,
			Metadata: srv.mdata,
		}
	}
	return ret
}

func (s *Server) onMessage(msg *nats.Msg) {
	method := msg.Subject

	// Peek at the request to see if this is a single-shot unary RPC.
	// Unary requests bypass the streaming machinery entirely so queue-group
	// load balancing across replicas is safe (the whole RPC is one message).
	// Unary calls are independent of each other, so spawning a goroutine per
	// request is fine — and required to keep the NATS dispatcher unblocked.
	request := &nrpc.Request{}
	if err := proto.Unmarshal(msg.Data, request); err == nil {
		if ureq, ok := request.Type.(*nrpc.Request_Unary); ok {
			if h, ok := s.unaryHandlers[method]; ok {
				go h(s, msg, ureq.Unary)
				return
			}
		}
	}

	log := s.log.WithField("method", method)
	s.mu.Lock()
	stream, ok := s.streams[msg.Reply]
	if !ok {
		stream = newServerStream(s, method, msg.Reply, log)
		s.streams[msg.Reply] = stream
	}
	s.mu.Unlock()

	// Hand off to the stream's serialized worker. Per-stream serialization is
	// required to preserve frame order: NATS delivers in order on a single
	// subscription, but if we dispatched each frame in its own goroutine
	// (the old behavior) Data frames could overtake the Call that initiated
	// the stream, and End could race past in-flight Data and trigger
	// `send on closed channel` in processData.
	stream.enqueue(msg)
}

func (s *Server) remove(reply string) {
	s.mu.Lock()
	delete(s.streams, reply)
	s.mu.Unlock()
}

var (
	// https://github.com/grpc/grpc-go/blob/master/internal/transport/http2_server.go#L54

	// ErrIllegalHeaderWrite indicates that setting header is illegal because of
	// the stream's state.
	ErrIllegalHeaderWrite = errors.New("transport: the stream is done or WriteHeader was already called")
)

type serverStream struct {
	ctx       context.Context
	cancel    context.CancelFunc
	server    *Server
	log       *logrus.Entry
	recvRead  <-chan []byte
	recvWrite chan<- []byte
	muWrite   sync.Mutex
	hasBegun  bool
	md        metadata.MD // recevied metadata from client
	header    metadata.MD // send header to client
	trailer   metadata.MD // send trialer to client
	method    string
	reply     string
	pnid      string

	// inbox is the per-stream queue of inbound NATS frames. A single worker
	// goroutine (started in newServerStream) drains it and dispatches frames
	// synchronously to processCall/Data/End/Ping, preserving the order NATS
	// delivered them in.
	inbox chan *nats.Msg

	// Stats handler lifecycle. statsCtx is the ctx returned by TagRPC and
	// becomes the parent for all subsequent HandleRPC calls. beginTime stamps
	// the stats.Begin event; endErr captures the final error reported on
	// stats.End. Begin/End fan-out is guarded by sync.Once so each stream
	// produces exactly one Begin and one End — necessary because
	// processCall/processData/processEnd run as separate goroutines and can
	// race (see Server.onMessage's go-spawn dispatch).
	beginTime  time.Time
	endErr     error
	statsCtx   context.Context
	beginOnce  sync.Once
	endOnce    sync.Once
	statsBegun bool // set true inside beginOnce; read by statsEnd
}

func newServerStream(server *Server, method, reply string, log *logrus.Entry) *serverStream {
	s := &serverStream{
		server:   server,
		log:      log,
		method:   method,
		reply:    reply,
		statsCtx: server.ctx,
		inbox:    make(chan *nats.Msg, 8192),
	}
	s.ctx, s.cancel = context.WithCancel(server.ctx)
	recv := make(chan []byte, 1)
	s.recvRead = recv
	s.recvWrite = recv
	go s.runInbox()
	return s
}

// enqueue hands a frame to the per-stream worker. Frames are processed in
// the order NATS delivered them.
func (s *serverStream) enqueue(msg *nats.Msg) {
	select {
	case <-s.ctx.Done():
		// Stream is shutting down; drop the frame.
	case s.inbox <- msg:
	}
}

// runInbox is the per-stream serialized worker. It unmarshals each frame and
// dispatches synchronously, guaranteeing that Call precedes Data precedes End
// for a given stream.
func (s *serverStream) runInbox() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case msg, ok := <-s.inbox:
			if !ok {
				return
			}
			request := &nrpc.Request{}
			if err := proto.Unmarshal(msg.Data, request); err != nil {
				s.log.WithField("data", string(msg.Data)).Error("unknown message")
				continue
			}
			s.onRequest(msg, request)
		}
	}
}

// statsBegin fires TagRPC + Begin exactly once for this stream. It is called
// from any of processCall / processData / processEnd — whichever wins the
// goroutine race — so that subsequent per-frame events (InPayload, OutPayload)
// are always preceded by Begin.
func (s *serverStream) statsBegin() {
	s.beginOnce.Do(func() {
		ctx := s.ctx
		if s.md != nil {
			ctx = metadata.NewIncomingContext(ctx, s.md)
		}
		ctx = s.server.tagRPC(ctx, &stats.RPCTagInfo{FullMethodName: s.method})
		s.statsCtx = ctx
		s.beginTime = time.Now()
		s.server.handleRPC(ctx, &stats.Begin{BeginTime: s.beginTime})
		s.statsBegun = true
	})
}

// finalize tears the stream down exactly once. It is the single writer of
// endErr and the single emitter of stats.End — both are guarded by endOnce so
// concurrent close paths (handler completion, processEnd's error path, and
// Server.Stop) cannot race the field or double-fire the event.
func (s *serverStream) finalize(err error) {
	s.endOnce.Do(func() {
		s.endErr = err
		if s.statsBegun {
			s.server.handleRPC(s.statsCtx, &stats.End{
				BeginTime: s.beginTime,
				EndTime:   time.Now(),
				Error:     err,
			})
		}
	})
	s.cancel()
	s.server.remove(s.reply)
}

// done is retained for callers that already populated endErr (legacy path).
// Prefer finalize(err) — it eliminates the racy write of endErr.
func (s *serverStream) done() {
	s.finalize(s.endErr)
}

func (s *serverStream) onRequest(msg *nats.Msg, request *nrpc.Request) {
	switch r := request.Type.(type) {
	case *nrpc.Request_Call:
		//s.log.WithField("call", r.Call).Info("recv call")
		s.processCall(r.Call)
	case *nrpc.Request_Data:
		//s.log.WithField("data", r.Data).Info("recv data")
		s.processData(r.Data)
	case *nrpc.Request_End:
		//s.log.WithField("end", r.End).Info("recv end")
		s.processEnd(r.End)
	case *nrpc.Request_Ping:
		//s.log.WithField("ping", r.Ping).Debug("recv ping")
		s.processPing(r.Ping)
	}
}

func (s *serverStream) processCall(call *nrpc.Call) {
	s.log = s.log.WithField("method", s.method)
	handlerFunc, ok := s.server.handlers[s.method]
	if !ok {
		s.close(status.Error(codes.Unimplemented, codes.Unimplemented.String()))
		return
	}
	// save metadata to context
	if call.Metadata != nil {
		md := make(metadata.MD)
		for hdr, data := range call.Metadata.Md {
			md[hdr] = data.Values
		}
		if s.md == nil {
			s.md = md
		} else if md != nil {
			s.md = metadata.Join(s.md, md)
		}
	}
	s.pnid = call.Nid
	s.statsBegin()
	go handlerFunc(s)
}

func (s *serverStream) processData(data *nrpc.Data) {
	if s.recvWrite == nil {
		s.log.Error("data received after client closeSend")
		return
	}
	// Defensive: Data can arrive before Call wins the goroutine race in
	// Server.onMessage. statsBegin is idempotent (sync.Once) and ensures
	// Begin always precedes InPayload in the emitted event stream.
	s.statsBegin()
	s.server.handleRPC(s.statsCtx, &stats.InPayload{
		Length:           len(data.Data),
		CompressedLength: len(data.Data),
		RecvTime:         time.Now(),
	})
	s.recvWrite <- data.Data
}

func (s *serverStream) processEnd(end *nrpc.End) {
	// Treat any non-OK status as a cancel; OK and nil status are normal
	// half-close, in which case we just close the recv channel — RecvMsg
	// will return io.EOF on the next read.
	if end.Status != nil && end.Status.Code != int32(codes.OK) {
		s.log.WithField("status", end.Status).Info("cancel")
		s.finalize(status.ErrorProto(end.Status))
		return
	}
	s.muWrite.Lock()
	defer s.muWrite.Unlock()
	s.log.Info("closeSend")
	if s.recvWrite != nil {
		close(s.recvWrite)
		s.recvWrite = nil
	}
}

func (s *serverStream) processPing(ping *nrpc.Ping) {
	// Immediately respond with pong
	s.writePong(&nrpc.Pong{
		Timestamp: ping.Timestamp,
	})
}

func (s *serverStream) beginMaybe() error {
	if !s.hasBegun {
		s.hasBegun = true
		if s.header != nil {
			return s.writeBegin(&nrpc.Begin{
				Header: utils.MakeMetadata(s.header),
				Nid:    s.server.nid,
			})
		}
	}
	return nil
}

func (s *serverStream) close(err error) {
	s.beginMaybe()
	s.writeEnd(&nrpc.End{
		Status:  status.Convert(err).Proto(),
		Trailer: utils.MakeMetadata(s.trailer),
	})
	s.finalize(err)
}

//
// Server Stream interface
//
func (s *serverStream) Method() string {
	return s.method
}

func (s *serverStream) SetHeader(header metadata.MD) error {
	if s.hasBegun {
		return ErrIllegalHeaderWrite
	}
	if s.header == nil {
		s.header = header
	} else if header != nil {
		s.header = metadata.Join(s.header, header)
	}
	return nil
}

func (s *serverStream) SendHeader(header metadata.MD) error {
	err := s.SetHeader(header)
	if err != nil {
		return err
	}
	return s.beginMaybe()
}

func (s *serverStream) SetTrailer(trailer metadata.MD) {
	if s.trailer == nil {
		s.trailer = trailer
	} else if trailer != nil {
		s.trailer = metadata.Join(s.trailer, trailer)
	}
}

func (s *serverStream) Context() context.Context {
	return s.ctx
}

func (s *serverStream) SendMsg(m interface{}) (err error) {
	defer func() {
		if err != nil {
			s.close(err)
		}
	}()

	err = s.beginMaybe()
	if err == nil {
		data, err := proto.Marshal(m.(proto.Message))
		if err == nil {
			s.server.handleRPC(s.statsCtx, &stats.OutPayload{
				Payload:          m,
				Length:           len(data),
				CompressedLength: len(data),
				SentTime:         time.Now(),
			})
			s.writeData(&nrpc.Data{
				Data: data,
			})
		}
	}
	return
}

func (s *serverStream) RecvMsg(m interface{}) error {
	select {
	case <-s.ctx.Done():
		return s.ctx.Err()
	case bytes, ok := <-s.recvRead:
		if !ok {
			return io.EOF
		}
		return proto.Unmarshal(bytes, m.(proto.Message))
	}
}

func (s *serverStream) writeResponse(response *nrpc.Response) error {
	//s.log.WithField("response", response).Info("send")
	data, err := proto.Marshal(response)
	if err != nil {
		return err
	}
	return s.server.nc.Publish(s.reply, data)
}

func (s *serverStream) writeBegin(begin *nrpc.Begin) error {
	return s.writeResponse(&nrpc.Response{
		Type: &nrpc.Response_Begin{
			Begin: begin,
		},
	})
}

func (s *serverStream) writeData(data *nrpc.Data) error {
	return s.writeResponse(&nrpc.Response{
		Type: &nrpc.Response_Data{
			Data: data,
		},
	})
}

func (s *serverStream) writeEnd(end *nrpc.End) error {
	return s.writeResponse(&nrpc.Response{
		Type: &nrpc.Response_End{
			End: end,
		},
	})
}

func (s *serverStream) writePong(pong *nrpc.Pong) error {
	return s.writeResponse(&nrpc.Response{
		Type: &nrpc.Response_Pong{
			Pong: pong,
		},
	})
}

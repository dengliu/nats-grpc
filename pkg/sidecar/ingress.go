package sidecar

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	rpc "github.com/cloudwebrtc/nats-grpc/pkg/rpc"
	"github.com/cloudwebrtc/nats-grpc/pkg/protos/nrpc"
	"github.com/nats-io/nats.go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	googlerpc "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// registration captures one live admin session: the NATS subscriptions
// opened on the app's behalf and the upstream gRPC connection used to
// dispatch inbound calls.
type registration struct {
	id       string
	svcid    string
	upstream string
	services []string

	conn *grpc.ClientConn
	subs []*nats.Subscription

	// per-stream ingress workers — one goroutine per inbound streaming
	// RPC. Tracked here so teardown can cancel them.
	mu          sync.Mutex
	streamCtxs  map[string]context.CancelFunc
	torndown    bool
}

func (r *registration) addStream(reply string, cancel context.CancelFunc) {
	r.mu.Lock()
	if r.torndown {
		r.mu.Unlock()
		cancel()
		return
	}
	r.streamCtxs[reply] = cancel
	r.mu.Unlock()
}

func (r *registration) removeStream(reply string) {
	r.mu.Lock()
	delete(r.streamCtxs, reply)
	r.mu.Unlock()
}

// teardown unsubscribes everything and cancels every in-flight ingress
// RPC. Safe to call multiple times.
func (r *registration) teardown() {
	r.mu.Lock()
	if r.torndown {
		r.mu.Unlock()
		return
	}
	r.torndown = true
	cancels := make([]context.CancelFunc, 0, len(r.streamCtxs))
	for _, c := range r.streamCtxs {
		cancels = append(cancels, c)
	}
	r.streamCtxs = nil
	r.mu.Unlock()

	for _, sub := range r.subs {
		_ = sub.Unsubscribe()
	}
	for _, cancel := range cancels {
		cancel()
	}
	if r.conn != nil {
		_ = r.conn.Close()
	}
}

// openIngress is called by the admin server when a Register Init message
// arrives. It dials the local upstream, opens the NATS subscriptions for
// each (svcid, service), and returns the registration handle.
func (s *Sidecar) openIngress(svcid, upstream string, services []string) (*registration, error) {
	if svcid == "" {
		return nil, status.Error(codes.InvalidArgument, "svcid is required")
	}
	if upstream == "" {
		return nil, status.Error(codes.InvalidArgument, "upstream is required")
	}
	if len(services) == 0 {
		return nil, status.Error(codes.InvalidArgument, "at least one service is required")
	}

	conn, err := grpc.NewClient(upstream,
		grpc.WithTransportCredentials(insecureTransport()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(newRawCodec())),
	)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "dial upstream %q: %v", upstream, err)
	}

	reg := &registration{
		id:         randomID(),
		svcid:      svcid,
		upstream:   upstream,
		services:   append([]string(nil), services...),
		conn:       conn,
		streamCtxs: map[string]context.CancelFunc{},
	}

	for _, svc := range services {
		if err := s.subscribeService(reg, svc); err != nil {
			reg.teardown()
			return nil, status.Errorf(codes.Internal, "subscribe %q: %v", svc, err)
		}
	}

	s.mu.Lock()
	s.registrations[reg.id] = reg
	s.mu.Unlock()
	return reg, nil
}

func (s *Sidecar) closeIngress(reg *registration) {
	s.mu.Lock()
	delete(s.registrations, reg.id)
	s.mu.Unlock()
	reg.teardown()
}

// subscribeService opens two NATS subscriptions per service:
//
//   - Modern unary subject (queue-grouped so backend replicas sharing the
//     same svcid load-balance).
//   - Modern streaming subject including the sidecar's nid (no queue
//     group — each replica subscribes only to its own nid-scoped subject,
//     so streaming frames stay coherent on one replica).
//
// Inbound messages are dispatched on the NATS subscription goroutine via
// onUnary / onStream — both spawn goroutines for actual handler work so
// the dispatcher stays unblocked.
func (s *Sidecar) subscribeService(reg *registration, service string) error {
	unarySubj := fmt.Sprintf("nrpc.unary.%s.%s.>", reg.svcid, service)
	unaryQ := fmt.Sprintf("u:%s:%s", reg.svcid, service)
	unarySub, err := s.nc.QueueSubscribe(unarySubj, unaryQ, func(msg *nats.Msg) {
		go s.onUnary(reg, msg)
	})
	if err != nil {
		return err
	}
	reg.subs = append(reg.subs, unarySub)

	streamSubj := fmt.Sprintf("nrpc.stream.%s.%s.%s.>", reg.svcid, s.cfg.Nid, service)
	streamSub, err := s.nc.Subscribe(streamSubj, func(msg *nats.Msg) {
		s.onStreamFrame(reg, msg)
	})
	if err != nil {
		return err
	}
	reg.subs = append(reg.subs, streamSub)
	return nil
}

// onUnary handles one NATS unary request. Reads UnaryRequest, forwards
// to the upstream gRPC server, packs the response into UnaryResponse,
// and replies via msg.Respond.
func (s *Sidecar) onUnary(reg *registration, msg *nats.Msg) {
	req := &nrpc.Request{}
	if err := proto.Unmarshal(msg.Data, req); err != nil {
		s.respondUnaryError(msg, codes.Internal, "unmarshal request: "+err.Error())
		return
	}
	u := req.GetUnary()
	if u == nil {
		s.respondUnaryError(msg, codes.InvalidArgument, "expected UnaryRequest")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = metadata.NewOutgoingContext(ctx, nrpcMetadataToGRPC(u.Metadata))

	in := &rpc.Frame{Payload: u.Data}
	out := &rpc.Frame{}
	var headerMD, trailerMD metadata.MD
	err := reg.conn.Invoke(ctx, u.Method, in, out,
		grpc.ForceCodec(newRawCodec()),
		grpc.Header(&headerMD),
		grpc.Trailer(&trailerMD),
	)

	resp := &nrpc.Response{Type: &nrpc.Response_Unary{Unary: &nrpc.UnaryResponse{}}}
	r := resp.GetUnary()
	r.Header = makeNrpcMetadata(headerMD)
	r.Trailer = makeNrpcMetadata(trailerMD)
	if err != nil {
		st, _ := status.FromError(err)
		r.Status = st.Proto()
	} else {
		r.Status = &googlerpc.Status{Code: int32(codes.OK)}
		r.Data = out.Payload
	}
	wire, marshalErr := proto.Marshal(resp)
	if marshalErr != nil {
		s.respondUnaryError(msg, codes.Internal, "marshal response: "+marshalErr.Error())
		return
	}
	_ = msg.Respond(wire)
}

func (s *Sidecar) respondUnaryError(msg *nats.Msg, code codes.Code, message string) {
	resp := &nrpc.Response{Type: &nrpc.Response_Unary{Unary: &nrpc.UnaryResponse{
		Status: &googlerpc.Status{Code: int32(code), Message: message},
	}}}
	if data, err := proto.Marshal(resp); err == nil {
		_ = msg.Respond(data)
	}
}

// onStreamFrame handles one streaming NATS frame. Stream identity is
// keyed by msg.Reply (each client stream uses a unique reply inbox);
// frames are dispatched to a per-stream worker goroutine.
func (s *Sidecar) onStreamFrame(reg *registration, msg *nats.Msg) {
	if msg.Reply == "" {
		return
	}
	w := s.ensureStreamWorker(reg, msg)
	if w == nil {
		return
	}
	select {
	case w.frames <- msg:
	case <-w.ctx.Done():
	}
}

// streamWorker is one per active ingress stream. It owns the upstream
// grpc.ClientStream and pumps frames in both directions.
type streamWorker struct {
	reg      *registration
	sidecar  *Sidecar
	reply    string
	method   string
	frames   chan *nats.Msg
	upstream grpc.ClientStream
	ctx      context.Context
	cancel   context.CancelFunc

	startOnce sync.Once
}

func (s *Sidecar) ensureStreamWorker(reg *registration, first *nats.Msg) *streamWorker {
	reg.mu.Lock()
	if reg.torndown {
		reg.mu.Unlock()
		return nil
	}
	if _, exists := reg.streamCtxs[first.Reply]; exists {
		// Worker already running.
		reg.mu.Unlock()
		// Find and return it via a separate map; the streamWorker map
		// lives on reg. (Kept minimal: reg.streamCtxs holds cancels;
		// we look up workers in a parallel map below.)
		s.streamsMu.Lock()
		w := s.streams[first.Reply]
		s.streamsMu.Unlock()
		return w
	}
	reg.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	// Peek at the first frame to determine method (Call frame).
	req := &nrpc.Request{}
	if err := proto.Unmarshal(first.Data, req); err != nil {
		cancel()
		return nil
	}
	call := req.GetCall()
	if call == nil {
		// First frame must be Call for streaming; drop otherwise.
		cancel()
		return nil
	}
	desc := &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}
	method := call.Method
	if method == "" {
		// Defensive — older clients may pass the subject; we can't
		// route without a gRPC path, so error out.
		cancel()
		return nil
	}
	upCtx := metadata.NewOutgoingContext(ctx, nrpcMetadataToGRPC(call.Metadata))
	upstream, err := reg.conn.NewStream(upCtx, desc, method, grpc.ForceCodec(newRawCodec()))
	if err != nil {
		cancel()
		// Best-effort wire-level error back to caller.
		s.publishStreamEnd(first.Reply, err)
		return nil
	}

	w := &streamWorker{
		reg:      reg,
		sidecar:  s,
		reply:    first.Reply,
		method:   method,
		frames:   make(chan *nats.Msg, 64),
		upstream: upstream,
		ctx:      ctx,
		cancel:   cancel,
	}

	reg.addStream(first.Reply, cancel)
	s.streamsMu.Lock()
	s.streams[first.Reply] = w
	s.streamsMu.Unlock()

	// Send Begin so the client learns this sidecar's nid (used for
	// CloseStream cleanup, parity with pkg/rpc.Server.beginMaybe).
	s.publishBegin(w.reply)
	go w.run()
	return w
}

// run is the streamWorker main loop. Pulls NATS frames in order and
// pumps them to the upstream gRPC stream; runs a sibling goroutine for
// the upstream→NATS direction.
func (w *streamWorker) run() {
	defer func() {
		w.reg.removeStream(w.reply)
		w.sidecar.streamsMu.Lock()
		delete(w.sidecar.streams, w.reply)
		w.sidecar.streamsMu.Unlock()
		w.cancel()
	}()

	// upstream → NATS pump.
	upDone := make(chan error, 1)
	go func() {
		for {
			f := &rpc.Frame{}
			if err := w.upstream.RecvMsg(f); err != nil {
				upDone <- err
				return
			}
			w.sidecar.publishData(w.reply, f.Payload)
		}
	}()

	for {
		select {
		case <-w.ctx.Done():
			return
		case msg, ok := <-w.frames:
			if !ok {
				return
			}
			req := &nrpc.Request{}
			if err := proto.Unmarshal(msg.Data, req); err != nil {
				continue
			}
			switch r := req.Type.(type) {
			case *nrpc.Request_Call:
				// Already consumed by ensureStreamWorker; ignore duplicates.
			case *nrpc.Request_Data:
				_ = w.upstream.SendMsg(&rpc.Frame{Payload: r.Data.Data})
			case *nrpc.Request_End:
				_ = w.upstream.CloseSend()
				// Wait for upstream to drain.
				err := <-upDone
				w.sidecar.publishStreamEnd(w.reply, normalizeStreamErr(err))
				return
			case *nrpc.Request_Ping:
				w.sidecar.publishPong(w.reply, r.Ping.Timestamp)
			}
		case err := <-upDone:
			// Upstream completed before we saw End from the caller; relay.
			w.sidecar.publishStreamEnd(w.reply, normalizeStreamErr(err))
			return
		}
	}
}

func normalizeStreamErr(err error) error {
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

// --- response publishers ----------------------------------------------------

func (s *Sidecar) publishBegin(reply string) {
	resp := &nrpc.Response{Type: &nrpc.Response_Begin{Begin: &nrpc.Begin{Nid: s.cfg.Nid}}}
	if data, err := proto.Marshal(resp); err == nil {
		_ = s.nc.Publish(reply, data)
	}
}

func (s *Sidecar) publishData(reply string, payload []byte) {
	resp := &nrpc.Response{Type: &nrpc.Response_Data{Data: &nrpc.Data{Data: payload}}}
	if data, err := proto.Marshal(resp); err == nil {
		_ = s.nc.Publish(reply, data)
	}
}

func (s *Sidecar) publishPong(reply string, ts int64) {
	resp := &nrpc.Response{Type: &nrpc.Response_Pong{Pong: &nrpc.Pong{Timestamp: ts}}}
	if data, err := proto.Marshal(resp); err == nil {
		_ = s.nc.Publish(reply, data)
	}
}

func (s *Sidecar) publishStreamEnd(reply string, err error) {
	end := &nrpc.End{}
	if err == nil {
		end.Status = &googlerpc.Status{Code: int32(codes.OK)}
	} else {
		st, _ := status.FromError(err)
		end.Status = st.Proto()
	}
	resp := &nrpc.Response{Type: &nrpc.Response_End{End: end}}
	if data, err := proto.Marshal(resp); err == nil {
		_ = s.nc.Publish(reply, data)
	}
}

// --- metadata translation ---------------------------------------------------

func nrpcMetadataToGRPC(in *nrpc.Metadata) metadata.MD {
	if in == nil {
		return metadata.MD{}
	}
	out := make(metadata.MD, len(in.Md))
	for k, v := range in.Md {
		if v == nil {
			continue
		}
		out[k] = append([]string(nil), v.Values...)
	}
	return out
}

func makeNrpcMetadata(md metadata.MD) *nrpc.Metadata {
	if md == nil || md.Len() == 0 {
		return nil
	}
	out := &nrpc.Metadata{Md: make(map[string]*nrpc.Strings, md.Len())}
	for k, vs := range md {
		out.Md[k] = &nrpc.Strings{Values: append([]string(nil), vs...)}
	}
	return out
}

// --- misc helpers -----------------------------------------------------------

func randomID() string {
	return randomNid() + fmt.Sprintf("-%d", time.Now().UnixNano())
}

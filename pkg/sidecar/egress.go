package sidecar

import (
	"context"
	"io"
	"strings"

	rpc "github.com/cloudwebrtc/nats-grpc/pkg/rpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Reserved metadata header names the sidecar consumes and strips before
// forwarding. See SIDECAR.md §1.
const (
	HeaderSvcID     = "x-nats-svcid"
	HeaderMode      = "x-nats-mode"
	HeaderTargetNid = "x-nats-target-nid"
)

const (
	modeUnary     = "unary"
	modeStreaming = "streaming"
)

// newEgressServer builds a gRPC server with an UnknownServiceHandler that
// routes every inbound call through the sidecar's NATS client. The
// raw-codec means the server doesn't need to know about any specific
// proto schema — every message body is forwarded as opaque bytes.
func newEgressServer(s *Sidecar) *grpc.Server {
	return grpc.NewServer(
		grpc.ForceServerCodec(newRawCodec()),
		grpc.UnknownServiceHandler(s.handleEgress),
	)
}

// handleEgress is the routing brain. Reads x-nats-svcid (mandatory) and
// x-nats-mode (default unary), strips the x-nats-* family, and
// dispatches via the appropriate nrpc path.
func (s *Sidecar) handleEgress(_ interface{}, stream grpc.ServerStream) error {
	fullMethod, ok := grpc.MethodFromServerStream(stream)
	if !ok {
		return status.Error(codes.Internal, "could not resolve method from server stream")
	}
	md, _ := metadata.FromIncomingContext(stream.Context())
	svcid := firstHeader(md, HeaderSvcID)
	if svcid == "" {
		return status.Errorf(codes.InvalidArgument, "%s header is required", HeaderSvcID)
	}
	mode := firstHeader(md, HeaderMode)
	if mode == "" {
		mode = modeUnary
	}

	forwardMD := stripReservedHeaders(md)
	outCtx := metadata.NewOutgoingContext(stream.Context(), forwardMD)

	switch mode {
	case modeUnary:
		return s.dispatchUnaryEgress(outCtx, svcid, fullMethod, stream)
	case modeStreaming:
		nid := firstHeader(md, HeaderTargetNid)
		if nid == "" {
			return status.Errorf(codes.InvalidArgument,
				"%s header is required when %s is %q", HeaderTargetNid, HeaderMode, modeStreaming)
		}
		return s.dispatchStreamingEgress(outCtx, svcid, nid, fullMethod, stream)
	default:
		return status.Errorf(codes.InvalidArgument, "%s value %q invalid (want %q or %q)",
			HeaderMode, mode, modeUnary, modeStreaming)
	}
}

func (s *Sidecar) dispatchUnaryEgress(ctx context.Context, svcid, method string, stream grpc.ServerStream) error {
	// Read one request frame (raw bytes via the proxy codec).
	in := &rpc.Frame{}
	if err := stream.RecvMsg(in); err != nil {
		return err
	}
	// nrpc.Client.InvokeWithSvcID wraps the bytes in UnaryRequest and
	// publishes via nc.RequestWithContext on the modern subject.
	out := &rpc.Frame{}
	if err := s.cli.InvokeWithSvcID(ctx, svcid, method, in, out); err != nil {
		return err
	}
	return stream.SendMsg(out)
}

func (s *Sidecar) dispatchStreamingEgress(ctx context.Context, svcid, nid, method string, server grpc.ServerStream) error {
	desc := &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}
	client, err := s.cli.NewStreamWithSvcID(ctx, svcid, nid, desc, method)
	if err != nil {
		return err
	}
	return pumpBidi(server, client)
}

// pumpBidi shuttles raw frames between the local gRPC stream and the
// outbound nrpc stream. Either direction ending in io.EOF means that
// side has half-closed (CloseSend). Any other error tears down both.
func pumpBidi(server grpc.ServerStream, client grpc.ClientStream) error {
	s2cErr := make(chan error, 1)
	go func() {
		for {
			f := &rpc.Frame{}
			if err := server.RecvMsg(f); err != nil {
				s2cErr <- err
				return
			}
			if err := client.SendMsg(f); err != nil {
				s2cErr <- err
				return
			}
		}
	}()

	c2sErr := make(chan error, 1)
	go func() {
		for {
			f := &rpc.Frame{}
			if err := client.RecvMsg(f); err != nil {
				c2sErr <- err
				return
			}
			if err := server.SendMsg(f); err != nil {
				c2sErr <- err
				return
			}
		}
	}()

	// Wait for one side to terminate. If it's a happy half-close on
	// the server side (client app called CloseSend), tell the outbound
	// nrpc stream to half-close too and wait for the response side.
	// Any non-EOF error propagates.
	for i := 0; i < 2; i++ {
		select {
		case err := <-s2cErr:
			if err == io.EOF {
				_ = client.CloseSend()
				continue
			}
			return err
		case err := <-c2sErr:
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
	return nil
}

func firstHeader(md metadata.MD, name string) string {
	if md == nil {
		return ""
	}
	vs := md.Get(name)
	if len(vs) == 0 {
		return ""
	}
	return vs[0]
}

// stripReservedHeaders returns a copy of md with every x-nats-* key
// removed. The sidecar's own routing fuel must never reach the backend
// handler — leaking it would let backends accidentally honor sidecar
// routing semantics meant for the next hop.
func stripReservedHeaders(md metadata.MD) metadata.MD {
	out := metadata.MD{}
	for k, v := range md {
		if strings.HasPrefix(strings.ToLower(k), "x-nats-") {
			continue
		}
		out[k] = append([]string(nil), v...)
	}
	return out
}

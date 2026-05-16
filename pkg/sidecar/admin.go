package sidecar

import (
	"errors"
	"io"

	pb "github.com/cloudwebrtc/nats-grpc/pkg/protos/sidecar"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// newAdminServer builds the gRPC server for SidecarAdmin. Loopback only,
// no auth — see SIDECAR.md §6.
func newAdminServer(s *Sidecar) *grpc.Server {
	srv := grpc.NewServer()
	pb.RegisterSidecarAdminServer(srv, &adminServer{sc: s})
	return srv
}

type adminServer struct {
	pb.UnimplementedSidecarAdminServer
	sc *Sidecar
}

// Register is the registration lease. First message must be Init; reply
// is Registered{Nid}. The app then holds the stream open. When the
// stream closes (clean or app crash), the sidecar tears down all NATS
// subscriptions and aborts in-flight ingress RPCs associated with this
// registration.
func (a *adminServer) Register(stream pb.SidecarAdmin_RegisterServer) error {
	first, err := stream.Recv()
	if err != nil {
		return status.Error(codes.InvalidArgument, "expected Init as first message")
	}
	init := first.GetInit()
	if init == nil {
		return status.Error(codes.InvalidArgument, "first message must be Init")
	}

	reg, err := a.sc.openIngress(init.Svcid, init.Upstream, init.Services)
	if err != nil {
		// openIngress returns a status.Error so it's safe to relay.
		_ = stream.Send(&pb.RegisterResponse{Type: &pb.RegisterResponse_Error{
			Error: &pb.ErrorResponse{Message: err.Error()},
		}})
		return err
	}
	defer a.sc.closeIngress(reg)

	if err := stream.Send(&pb.RegisterResponse{Type: &pb.RegisterResponse_Registered{
		Registered: &pb.Registered{Nid: a.sc.cfg.Nid},
	}}); err != nil {
		return err
	}

	// Hold the stream open. Heartbeats arrive as inbound messages; any
	// non-Heartbeat message after Init is a protocol violation. The
	// stream closes when the app disconnects or the sidecar's done
	// channel fires.
	recvDone := make(chan error, 1)
	go func() {
		for {
			req, err := stream.Recv()
			if err != nil {
				recvDone <- err
				return
			}
			if hb := req.GetHeartbeat(); hb != nil {
				_ = stream.Send(&pb.RegisterResponse{Type: &pb.RegisterResponse_HeartbeatAck{
					HeartbeatAck: &pb.HeartbeatAck{ServerTimestampUnixNano: hb.ClientTimestampUnixNano},
				}})
				continue
			}
			if req.GetInit() != nil {
				recvDone <- status.Error(codes.InvalidArgument, "Init may only appear as the first message")
				return
			}
		}
	}()

	select {
	case err := <-recvDone:
		if errors.Is(err, io.EOF) {
			return nil
		}
		return err
	case <-a.sc.done:
		// Sidecar shutting down — propagate to the client so it can react.
		return status.Error(codes.Unavailable, "sidecar shutting down")
	}
}

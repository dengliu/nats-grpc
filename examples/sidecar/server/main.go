// Backend echo server that registers itself with a nats-grpc sidecar's
// admin API at startup. The svcid is a CLI flag — the same binary runs
// as "serviceid_1" or "serviceid_2" depending on how it was launched,
// which is the dynamic-routing story the example illustrates.
package main

import (
	"context"
	"flag"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/cloudwebrtc/nats-grpc/examples/protos/echo"
	pb "github.com/cloudwebrtc/nats-grpc/pkg/protos/sidecar"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type echoServer struct {
	echo.UnimplementedEchoServer
	id string
}

func (s *echoServer) SayHello(_ context.Context, req *echo.HelloRequest) (*echo.HelloReply, error) {
	log.Printf("[%s] SayHello msg=%q", s.id, req.Msg)
	return &echo.HelloReply{Msg: s.id + ": hello " + req.Msg}, nil
}

func main() {
	svcid := flag.String("svcid", "serviceid_1", "svcid to register under")
	listen := flag.String("listen", "127.0.0.1:0", "local gRPC listen addr (0 = OS picks)")
	adminAddr := flag.String("admin", "127.0.0.1:50100", "sidecar admin addr")
	flag.Parse()

	// Stand up the local gRPC server. The sidecar dials this address
	// when ingress traffic arrives for our svcid.
	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	echo.RegisterEchoServer(srv, &echoServer{id: *svcid})
	go func() { _ = srv.Serve(lis) }()
	log.Printf("backend %q listening on %s", *svcid, lis.Addr())

	// Register with the sidecar. The Register stream is the lease:
	// while we hold it open the sidecar keeps our NATS subscriptions
	// alive; if we drop it (e.g. process exit) the sidecar tears
	// them down automatically.
	conn, err := grpc.NewClient(*adminAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial sidecar admin: %v", err)
	}
	defer conn.Close()
	cli := pb.NewSidecarAdminClient(conn)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := cli.Register(ctx)
	if err != nil {
		log.Fatalf("Register: %v", err)
	}
	if err := stream.Send(&pb.RegisterRequest{Type: &pb.RegisterRequest_Init{
		Init: &pb.Init{
			Svcid:    *svcid,
			Upstream: lis.Addr().String(),
			Services: []string{"echo.Echo"},
		},
	}}); err != nil {
		log.Fatalf("send Init: %v", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		log.Fatalf("recv Registered: %v", err)
	}
	if r := resp.GetRegistered(); r != nil {
		log.Printf("registered with sidecar — sidecar nid=%s", r.Nid)
	} else if e := resp.GetError(); e != nil {
		log.Fatalf("sidecar refused registration: %s", e.Message)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	log.Printf("shutting down")
	cancel()
	srv.GracefulStop()
}

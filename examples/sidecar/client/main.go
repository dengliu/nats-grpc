// Client that calls SayHello twice through the same gRPC stub but with
// two different x-nats-svcid metadata values, demonstrating runtime
// per-call routing through the sidecar.
package main

import (
	"context"
	"flag"
	"log"
	"time"

	"github.com/cloudwebrtc/nats-grpc/examples/protos/echo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func main() {
	egress := flag.String("egress", "127.0.0.1:50051", "sidecar egress addr")
	flag.Parse()

	conn, err := grpc.NewClient(*egress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial sidecar egress: %v", err)
	}
	defer conn.Close()
	cli := echo.NewEchoClient(conn)

	// The headline use case: same stub, same method, two backends,
	// distinguished only by the per-call x-nats-svcid header value.
	for _, target := range []string{"serviceid_1", "serviceid_2"} {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		ctx = metadata.AppendToOutgoingContext(ctx, "x-nats-svcid", target)
		resp, err := cli.SayHello(ctx, &echo.HelloRequest{Msg: "world"})
		cancel()
		if err != nil {
			log.Printf("→ %s: error: %v", target, err)
			continue
		}
		log.Printf("→ %s: reply=%q", target, resp.Msg)
	}
}

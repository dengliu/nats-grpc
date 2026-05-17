// Go client for the docker-compose demo. Dials the sidecar's egress
// port on loopback (shared network namespace via
// network_mode: service:sidecar-go-client) and sends one SayHello
// every 3 seconds, alternating between python-server (odd #) and
// go-server (even #) purely via the x-nats-svcid metadata header.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/cloudwebrtc/nats-grpc/examples/protos/echo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func main() {
	const egress = "127.0.0.1:50051"
	conn, err := grpc.NewClient(egress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial sidecar egress %s: %v", egress, err)
	}
	defer conn.Close()
	cli := echo.NewEchoClient(conn)

	type target struct {
		svcid string
		label string
	}
	targets := []target{
		{svcid: "python-server", label: "Python Server"},
		{svcid: "go-server", label: "Go Server"},
	}

	for n := 1; ; n++ {
		t := targets[(n-1)%len(targets)]
		msg := fmt.Sprintf("Hi %s, I am Go Client request #%d", t.label, n)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		ctx = metadata.AppendToOutgoingContext(ctx, "x-nats-svcid", t.svcid)
		resp, err := cli.SayHello(ctx, &echo.HelloRequest{Msg: msg})
		cancel()
		if err != nil {
			log.Printf("→ %-13s  error: %v", t.svcid, err)
		} else {
			log.Printf("→ %-13s  reply=%q", t.svcid, resp.Msg)
		}
		time.Sleep(3 * time.Second)
	}
}

// Go client for the docker-compose demo. Dials the sidecar's egress
// port on loopback (shared network namespace via
// network_mode: service:sidecar-go-client) and sends one SayHello
// every 3 seconds, alternating between py-server (odd #) and
// go-server (even #) purely via the x-nats-svcid metadata header.
//
// Wire format: requests are "<self> -> <target> #<N>", replies come
// back as "<target> -> <self> #<N>".
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
	const selfLabel = "go-client"

	conn, err := grpc.NewClient(egress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("dial sidecar egress %s: %v", egress, err)
	}
	defer conn.Close()
	cli := echo.NewEchoClient(conn)

	type target struct {
		svcid string // routing key on the wire (x-nats-svcid header)
		label string // short human-readable name used in the message
	}
	targets := []target{
		{svcid: "python-server", label: "py-server"},
		{svcid: "go-server", label: "go-server"},
	}

	for n := 1; ; n++ {
		t := targets[(n-1)%len(targets)]
		msg := fmt.Sprintf("%s -> %s #%d", selfLabel, t.label, n)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		ctx = metadata.AppendToOutgoingContext(ctx, "x-nats-svcid", t.svcid)
		resp, err := cli.SayHello(ctx, &echo.HelloRequest{Msg: msg})
		cancel()
		if err != nil {
			log.Printf("→ %-13s  error: %v", t.svcid, err)
		} else {
			log.Printf("%s  ⇒  %s", msg, resp.Msg)
		}
		time.Sleep(3 * time.Second)
	}
}

// Minimal sidecar binary. Exposes a loopback gRPC port for the local
// app's egress calls (50051) and an HTTP/JSON admin port for
// registration (50101); bridges everything to NATS.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/cloudwebrtc/nats-grpc/pkg/sidecar"
)

func main() {
	natsURL := flag.String("nats", "nats://localhost:4222", "NATS server URL")
	egress := flag.String("egress", "127.0.0.1:50051", "egress gRPC listen addr")
	httpAdmin := flag.String("http-admin", "127.0.0.1:50101",
		`HTTP/JSON admin listen addr; use "-" to disable`)
	nid := flag.String("nid", "", "sidecar nid (default: auto-generated)")
	flag.Parse()

	sc := sidecar.New(sidecar.Config{
		NATSURL:       *natsURL,
		EgressAddr:    *egress,
		HTTPAdminAddr: *httpAdmin,
		Nid:           *nid,
	})
	if err := sc.Start(context.Background()); err != nil {
		log.Fatalf("sidecar.Start: %v", err)
	}
	log.Printf("sidecar ready — nid=%s egress=%s http-admin=%s",
		sc.Nid(), sc.EgressAddr(), sc.HTTPAdminAddr())

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	log.Printf("shutting down")
	_ = sc.Close()
}

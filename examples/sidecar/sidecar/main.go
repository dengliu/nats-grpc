// Minimal sidecar binary for the examples. Exposes loopback gRPC ports
// for the local app's egress calls (50051) and admin registration
// (50100), bridges everything to NATS.
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
	admin := flag.String("admin", "127.0.0.1:50100", "admin gRPC listen addr")
	nid := flag.String("nid", "", "sidecar nid (default: auto-generated)")
	flag.Parse()

	sc := sidecar.New(sidecar.Config{
		NATSURL:    *natsURL,
		EgressAddr: *egress,
		AdminAddr:  *admin,
		Nid:        *nid,
	})
	if err := sc.Start(context.Background()); err != nil {
		log.Fatalf("sidecar.Start: %v", err)
	}
	log.Printf("sidecar ready — nid=%s egress=%s admin=%s",
		sc.Nid(), sc.EgressAddr(), sc.AdminAddr())

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	log.Printf("shutting down")
	_ = sc.Close()
}

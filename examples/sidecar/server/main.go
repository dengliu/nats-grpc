// Backend echo server that registers itself with a nats-grpc sidecar's
// HTTP/JSON admin endpoint at startup. The svcid is a CLI flag — the
// same binary runs as "serviceid_1" or "serviceid_2" depending on how
// it was launched, which is the dynamic-routing story the example
// illustrates.
//
// The registration uses plain net/http — no nats-grpc imports, no
// generated stubs. Exactly what a Python or Node service would do
// (see ./examples/sidecar/python for the Python equivalent).
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/cloudwebrtc/nats-grpc/examples/protos/echo"
	"google.golang.org/grpc"
)

type echoServer struct {
	echo.UnimplementedEchoServer
	id string
}

func (s *echoServer) SayHello(_ context.Context, req *echo.HelloRequest) (*echo.HelloReply, error) {
	log.Printf("[%s] SayHello msg=%q", s.id, req.Msg)
	return &echo.HelloReply{Msg: s.id + ": hello " + req.Msg}, nil
}

// registerWithSidecar opens a long-lived POST /v1/register against the
// sidecar's HTTP admin port. The first response line carries the
// sidecar's nid; subsequent lines are keepalive acks. The open HTTP
// connection IS the registration lease: when this function returns
// (because we exit the read loop or the connection drops), the
// sidecar's per-connection handler runs its deferred closeIngress
// and the NATS subscriptions tear down.
func registerWithSidecar(ctx context.Context, adminURL, svcid, upstream string, services []string) error {
	body, err := json.Marshal(map[string]any{
		"svcid":    svcid,
		"upstream": upstream,
		"services": services,
	})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, adminURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	// DisableKeepAlives so closing the response body actually closes
	// the underlying TCP connection (the sidecar uses the close to
	// detect deregistration).
	tr := &http.Transport{DisableKeepAlives: true}
	client := &http.Client{Transport: tr, Timeout: 0}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		errBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("register: HTTP %d: %s", resp.StatusCode, errBody)
	}

	reader := bufio.NewReader(resp.Body)
	first, err := reader.ReadBytes('\n')
	if err != nil {
		return fmt.Errorf("read initial response: %w", err)
	}
	var initial struct {
		Nid string `json:"nid"`
	}
	if err := json.Unmarshal(first, &initial); err != nil {
		return fmt.Errorf("parse initial response: %w", err)
	}
	log.Printf("registered with sidecar — sidecar nid=%s", initial.Nid)

	// Hold the connection. Each NDJSON line we read is a keepalive
	// ack from the sidecar (production cadence is every 30s). The
	// loop exits when:
	//   - the connection drops (sidecar gone, network blip) → reader returns err
	//   - ctx is cancelled (SIGINT/SIGTERM)        → reader.ReadBytes unblocks via ctx
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			return err
		}
		var msg map[string]any
		if json.Unmarshal(line, &msg) == nil {
			if ts, ok := msg["ack"]; ok {
				log.Printf("heartbeat ack ts=%v", ts)
			}
		}
	}
}

func main() {
	svcid := flag.String("svcid", "serviceid_1", "svcid to register under")
	listen := flag.String("listen", "127.0.0.1:0", "local gRPC listen addr (0 = OS picks)")
	adminURL := flag.String("admin", "http://127.0.0.1:50101/v1/register", "sidecar HTTP admin URL")
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

	// Register with the sidecar in a background goroutine so the
	// signal handler can cancel it on shutdown.
	ctx, cancel := context.WithCancel(context.Background())
	regDone := make(chan error, 1)
	go func() {
		regDone <- registerWithSidecar(ctx, *adminURL, *svcid, lis.Addr().String(), []string{"echo.Echo"})
	}()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-sigs:
		log.Printf("shutting down")
	case err := <-regDone:
		log.Printf("registration ended: %v", err)
	}
	cancel()
	srv.GracefulStop()
}

// Go echo backend for the docker-compose demo. Listens on
// 127.0.0.1:8080, registers itself with the sidecar via HTTP/JSON
// admin, and answers every request with the original message
// suffixed by " I am go server".
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/cloudwebrtc/nats-grpc/examples/protos/echo"
	"google.golang.org/grpc"
)

const selfLabel = "go-server"

type echoServer struct {
	echo.UnimplementedEchoServer
}

func (echoServer) SayHello(_ context.Context, req *echo.HelloRequest) (*echo.HelloReply, error) {
	reply := swapSenderTarget(req.Msg, selfLabel)
	log.Printf("SayHello in=%q  out=%q", req.Msg, reply)
	return &echo.HelloReply{Msg: reply}, nil
}

// swapSenderTarget rewrites a request of the form
// "<sender> -> <target> #<N>" as "<self> -> <sender> #<N>".
// The target component is discarded — by construction it equals
// self anyway (the sidecar only delivers messages addressed to our
// svcid). Inputs that don't match the format are returned as-is,
// keeping the demo running on unexpected payloads.
func swapSenderTarget(req, self string) string {
	senderEnd := strings.Index(req, " -> ")
	if senderEnd < 0 {
		return req
	}
	rest := req[senderEnd+len(" -> "):]
	hashIdx := strings.Index(rest, " #")
	if hashIdx < 0 {
		return req
	}
	// rest[hashIdx:] is " #<N>" with the leading space — splice it
	// straight back in so the output reads "self -> sender #N".
	return self + " -> " + req[:senderEnd] + rest[hashIdx:]
}

func registerWithSidecar(ctx context.Context, adminURL, svcid, upstream string) error {
	body, _ := json.Marshal(map[string]any{
		"svcid":    svcid,
		"upstream": upstream,
		"services": []string{"echo.Echo"},
	})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, adminURL, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	tr := &http.Transport{DisableKeepAlives: true}
	resp, err := (&http.Client{Transport: tr, Timeout: 0}).Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		buf, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("register: HTTP %d: %s", resp.StatusCode, buf)
	}
	reader := bufio.NewReader(resp.Body)
	first, err := reader.ReadBytes('\n')
	if err != nil {
		return err
	}
	var initial struct {
		Nid string `json:"nid"`
	}
	_ = json.Unmarshal(first, &initial)
	log.Printf("registered as svcid=%q  sidecar.nid=%s", svcid, initial.Nid)

	// Signal "I've registered" to docker-compose's healthcheck. The
	// client containers gate on this so they don't fire their first
	// RPC before the sidecar's NATS subscriptions are live.
	if err := os.WriteFile("/tmp/ready", nil, 0o644); err != nil {
		log.Printf("write /tmp/ready: %v", err)
	}

	// io.Copy(io.Discard, reader) is the canonical "drain to EOF"
	// stdlib idiom — the open HTTP connection IS the lease.
	_, _ = io.Copy(io.Discard, reader)
	return io.EOF
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	svcid := env("SVCID", "go-server")
	listen := env("LISTEN", "127.0.0.1:8080")
	adminURL := env("ADMIN_URL", "http://127.0.0.1:50101/v1/register")

	lis, err := net.Listen("tcp", listen)
	if err != nil {
		log.Fatalf("listen %s: %v", listen, err)
	}
	srv := grpc.NewServer()
	echo.RegisterEchoServer(srv, echoServer{})
	go func() { _ = srv.Serve(lis) }()
	log.Printf("go echo server listening on %s", lis.Addr())

	ctx, cancel := context.WithCancel(context.Background())
	regDone := make(chan error, 1)
	go func() { regDone <- registerWithSidecar(ctx, adminURL, svcid, lis.Addr().String()) }()

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

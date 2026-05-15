package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/cloudwebrtc/nats-grpc/examples/benchmark/protos/benchmark"
	"github.com/cloudwebrtc/nats-grpc/pkg/rpc"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/timeout"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/nats-io/nats.go"
	log "github.com/pion/ion-log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
)

var (
	// Keep activeClients as it tracks concurrent client count
	activeClients = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "grpc_client_active_total",
			Help: "Number of active clients",
		},
	)
)

func init() {
	// Register our custom active clients gauge
	// Note: grpc_prometheus.DefaultClientMetrics is already registered globally
	prometheus.MustRegister(activeClients)
}

func runClient(clientID string, serverID string, requestPayload []byte, natsURL string, requestTimeout time.Duration) {
	defer activeClients.Dec()

	// Create individual NATS connection for this client
	opts := []nats.Option{nats.Name(fmt.Sprintf("nats-grpc benchmark client %s", clientID))}
	opts = rpc.SetupConnOptions(opts)

	nc, err := nats.Connect(natsURL, opts...)
	if err != nil {
		log.Errorf("Client %s: failed to connect to NATS: %v", clientID, err)
		return
	}
	defer nc.Close()

	// Chain timeout and Prometheus interceptors
	chainedInterceptor := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		// Apply timeout interceptor first with configurable timeout
		timeoutInt := timeout.UnaryClientInterceptor(requestTimeout)
		return timeoutInt(ctx, method, req, reply, cc, func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
			// Then apply Prometheus interceptor
			return grpc_prometheus.UnaryClientInterceptor(ctx, method, req, reply, cc, invoker, opts...)
		}, opts...)
	}

	// Create nats-grpc client with chained interceptors
	ncli := rpc.NewClientWithOptions(nc, serverID, clientID,
		rpc.WithUnaryInterceptor(chainedInterceptor),
	)
	defer ncli.Close()

	cli := benchmark.NewBenchmarkClient(ncli)

	// Send requests at 1Hz (1 request per second)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	log.Infof("Client %s started, targeting %s", clientID, serverID)

	for range ticker.C {
		// No manual timeout needed - handled by timeout interceptor
		_, err := cli.Execute(context.Background(), &benchmark.BenchmarkRequest{
			ServerId: serverID,
			Payload:  requestPayload,
		})

		if err != nil {
			log.Errorf("Client %s: request failed: %v", clientID, err)
		}
	}
}

func main() {
	var (
		natsURL        = flag.String("nats-url", nats.DefaultURL, "NATS server URL")
		clientCount    = flag.Int("client-count", 1000, "Number of clients to spawn")
		payloadBytes   = flag.Int("payload-size", 4096, "Payload size in bytes")
		metricsPort    = flag.Int("metrics-port", 9091, "Prometheus metrics port")
		requestTimeout = flag.Int("request-timeout", 5, "gRPC request timeout in seconds")
	)
	flag.Parse()

	// Start metrics server
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		addr := fmt.Sprintf(":%d", *metricsPort)
		log.Infof("Starting Prometheus metrics server on %s", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Errorf("Failed to start metrics server: %v", err)
		}
	}()

	// Prepare request payload
	requestPayload := make([]byte, *payloadBytes)
	for i := range requestPayload {
		requestPayload[i] = byte(i % 256)
	}

	log.Infof("Starting %d benchmark clients...", *clientCount)

	var wg sync.WaitGroup

	for i := 0; i < *clientCount; i++ {
		clientID := fmt.Sprintf("benchmark-client-%d", i)
		serverID := fmt.Sprintf("benchmark-server-%d", i)

		activeClients.Inc()
		wg.Add(1)

		go func(cid, sid string) {
			defer wg.Done()
			runClient(cid, sid, requestPayload, *natsURL, time.Duration(*requestTimeout)*time.Second)
		}(clientID, serverID)

		// Small delay to avoid overwhelming connection setup
		if i > 0 && i%100 == 0 {
			time.Sleep(100 * time.Millisecond)
			log.Infof("Started %d clients...", i)
		}
	}

	log.Infof("All %d clients started successfully", *clientCount)

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Infof("Shutting down clients...")
	// Clients will stop when the main function exits
	log.Infof("Client shutdown complete")
}

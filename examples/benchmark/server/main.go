package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"github.com/cloudwebrtc/nats-grpc/examples/benchmark/protos/benchmark"
	"github.com/cloudwebrtc/nats-grpc/pkg/rpc"
	grpc_prometheus "github.com/grpc-ecosystem/go-grpc-prometheus"
	"github.com/nats-io/nats.go"
	log "github.com/pion/ion-log"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// podOrdinalOffset returns the global ID offset for this pod, derived from the
// trailing ordinal of POD_NAME (set by the StatefulSet via the downward API).
// Pod "<sts>-0" hosts IDs [0, n); "<sts>-1" hosts [n, 2n); etc.
func podOrdinalOffset(n int) int {
	name := os.Getenv("POD_NAME")
	if name == "" {
		return 0
	}
	i := strings.LastIndex(name, "-")
	if i == -1 {
		return 0
	}
	ord, err := strconv.Atoi(name[i+1:])
	if err != nil {
		return 0
	}
	return ord * n
}

type benchmarkServer struct {
	benchmark.UnimplementedBenchmarkServer
	serverID      string
	responseBytes []byte
}

func (s *benchmarkServer) Execute(ctx context.Context, req *benchmark.BenchmarkRequest) (*benchmark.BenchmarkResponse, error) {
	return &benchmark.BenchmarkResponse{
		ServerId: s.serverID,
		Payload:  s.responseBytes,
	}, nil
}

func main() {
	var (
		natsURL      = flag.String("nats-url", nats.DefaultURL, "NATS server URL")
		serverCount  = flag.Int("server-count", 1000, "Number of servers to spawn")
		payloadBytes = flag.Int("payload-size", 4096, "Payload size in bytes")
		metricsPort  = flag.Int("metrics-port", 9090, "Prometheus metrics port")
	)
	flag.Parse()

	// Start metrics server with standard gRPC server metrics
	http.Handle("/metrics", promhttp.Handler())
	go func() {
		addr := fmt.Sprintf(":%d", *metricsPort)
		log.Infof("Starting Prometheus metrics server on %s", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Errorf("Failed to start metrics server: %v", err)
		}
	}()

	// Prepare response payload
	responsePayload := make([]byte, *payloadBytes)
	for i := range responsePayload {
		responsePayload[i] = byte(i % 256)
	}

	var wg sync.WaitGroup
	servers := make([]*rpc.Server, *serverCount)

	offset := podOrdinalOffset(*serverCount)
	log.Infof("Starting %d benchmark servers (IDs %d..%d)...", *serverCount, offset, offset+*serverCount-1)

	for i := 0; i < *serverCount; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			serverID := fmt.Sprintf("benchmark-server-%d", offset+index)

			// Create individual NATS connection for each server
			opts := []nats.Option{nats.Name(fmt.Sprintf("nats-grpc benchmark server %d", index))}
			opts = rpc.SetupConnOptions(opts)

			nc, err := nats.Connect(*natsURL, opts...)
			if err != nil {
				log.Errorf("Server %d: failed to connect to NATS: %v", index, err)
				return
			}
			defer nc.Close()

			// Create nats-grpc server with Prometheus interceptor
			ncs := rpc.NewServerWithOptions(nc, serverID,
				rpc.WithUnaryServerInterceptor(grpc_prometheus.UnaryServerInterceptor),
			)
			servers[index] = ncs

			// Register benchmark service
			srv := &benchmarkServer{
				serverID:      serverID,
				responseBytes: responsePayload,
			}
			benchmark.RegisterBenchmarkServer(ncs, srv)

			if index == 0 || (index+1)%100 == 0 {
				log.Infof("Server %d (%s) is running", offset+index, serverID)
			}

			// Keep server running
			select {}
		}(i)
	}

	log.Infof("All %d servers started successfully", *serverCount)

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Infof("Shutting down servers...")
	for _, server := range servers {
		if server != nil {
			server.Stop()
		}
	}

	log.Infof("Server shutdown complete")
}

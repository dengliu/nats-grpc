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
	"github.com/nats-io/nats.go"
	log "github.com/pion/ion-log"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
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

// setupOTelMetrics wires an OpenTelemetry MeterProvider whose metrics are
// scraped through the existing Prometheus `/metrics` endpoint.
func setupOTelMetrics(serviceName string) (*sdkmetric.MeterProvider, error) {
	exporter, err := otelprom.New()
	if err != nil {
		return nil, fmt.Errorf("create prometheus exporter: %w", err)
	}
	// NewSchemaless avoids a Merge conflict when the SDK's default resource
	// uses a different semconv schema URL than this package's import.
	res, err := resource.Merge(resource.Default(), resource.NewSchemaless(
		semconv.ServiceName(serviceName),
	))
	if err != nil {
		return nil, fmt.Errorf("build resource: %w", err)
	}
	return sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(exporter),
		sdkmetric.WithResource(res),
	), nil
}

func main() {
	var (
		natsURL      = flag.String("nats-url", nats.DefaultURL, "NATS server URL")
		serverCount  = flag.Int("server-count", 1000, "Number of servers to spawn")
		payloadBytes = flag.Int("payload-size", 4096, "Payload size in bytes")
		metricsPort  = flag.Int("metrics-port", 9090, "Prometheus metrics port")
	)
	flag.Parse()

	mp, err := setupOTelMetrics("nats-grpc-benchmark-server")
	if err != nil {
		log.Errorf("OTel setup failed: %v", err)
		os.Exit(1)
	}
	defer mp.Shutdown(context.Background())

	otelHandler := otelgrpc.NewServerHandler(otelgrpc.WithMeterProvider(mp))

	http.Handle("/metrics", promhttp.Handler())
	go func() {
		addr := fmt.Sprintf(":%d", *metricsPort)
		log.Infof("Starting Prometheus metrics server on %s", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Errorf("Failed to start metrics server: %v", err)
		}
	}()

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

			opts := []nats.Option{nats.Name(fmt.Sprintf("nats-grpc benchmark server %d", index))}
			opts = rpc.SetupConnOptions(opts)

			nc, err := nats.Connect(*natsURL, opts...)
			if err != nil {
				log.Errorf("Server %d: failed to connect to NATS: %v", index, err)
				return
			}
			defer nc.Close()

			ncs := rpc.NewServerWithOptions(nc, serverID,
				rpc.WithServerStatsHandler(otelHandler),
			)
			servers[index] = ncs

			srv := &benchmarkServer{
				serverID:      serverID,
				responseBytes: responsePayload,
			}
			benchmark.RegisterBenchmarkServer(ncs, srv)

			if index == 0 || (index+1)%100 == 0 {
				log.Infof("Server %d (%s) is running", offset+index, serverID)
			}

			select {}
		}(i)
	}

	log.Infof("All %d servers started successfully", *serverCount)

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

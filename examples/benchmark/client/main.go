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
	"time"

	"github.com/cloudwebrtc/nats-grpc/examples/benchmark/protos/benchmark"
	"github.com/cloudwebrtc/nats-grpc/pkg/rpc"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/timeout"
	"github.com/nats-io/nats.go"
	log "github.com/pion/ion-log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"google.golang.org/grpc/stats"
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

var (
	// activeClients tracks the count of concurrent client goroutines. This
	// is a vanilla Prometheus gauge — orthogonal to the gRPC instrumentation
	// produced by the otelgrpc stats handler.
	activeClients = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "grpc_client_active_total",
			Help: "Number of active clients",
		},
	)
)

func init() {
	prometheus.MustRegister(activeClients)
}

func runClient(clientID, serverID string, requestPayload []byte, natsURL string, requestTimeout time.Duration, otelHandler stats.Handler) {
	defer activeClients.Dec()

	opts := []nats.Option{nats.Name(fmt.Sprintf("nats-grpc benchmark client %s", clientID))}
	opts = rpc.SetupConnOptions(opts)

	nc, err := nats.Connect(natsURL, opts...)
	if err != nil {
		log.Errorf("Client %s: failed to connect to NATS: %v", clientID, err)
		return
	}
	defer nc.Close()

	// Timeout enforcement stays as an interceptor. Metrics + tracing are
	// driven by the otelgrpc stats handler, which receives Begin/End from
	// pkg/rpc and emits rpc.client.* histograms via the global MeterProvider.
	ncli := rpc.NewClientWithOptions(nc, serverID, clientID,
		rpc.WithUnaryInterceptor(timeout.UnaryClientInterceptor(requestTimeout)),
		rpc.WithStatsHandler(otelHandler),
	)
	defer ncli.Close()

	cli := benchmark.NewBenchmarkClient(ncli)

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	log.Infof("Client %s started, targeting %s", clientID, serverID)

	for range ticker.C {
		_, err := cli.Execute(context.Background(), &benchmark.BenchmarkRequest{
			ServerId: serverID,
			Payload:  requestPayload,
		})
		if err != nil {
			log.Errorf("Client %s: request failed: %v", clientID, err)
		}
	}
}

// setupOTelMetrics wires an OpenTelemetry MeterProvider whose metrics are
// scraped through the existing Prometheus `/metrics` endpoint. The returned
// MeterProvider is passed to otelgrpc; Shutdown should be called at exit.
func setupOTelMetrics(serviceName string) (*sdkmetric.MeterProvider, error) {
	// Prometheus exporter registers with prometheus.DefaultRegisterer, so the
	// existing promhttp.Handler() automatically exposes the OTel metrics.
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
		natsURL        = flag.String("nats-url", nats.DefaultURL, "NATS server URL")
		clientCount    = flag.Int("client-count", 1000, "Number of clients to spawn")
		payloadBytes   = flag.Int("payload-size", 4096, "Payload size in bytes")
		metricsPort    = flag.Int("metrics-port", 9091, "Prometheus metrics port")
		requestTimeout = flag.Int("request-timeout", 5, "gRPC request timeout in seconds")
	)
	flag.Parse()

	mp, err := setupOTelMetrics("nats-grpc-benchmark-client")
	if err != nil {
		log.Errorf("OTel setup failed: %v", err)
		os.Exit(1)
	}
	defer mp.Shutdown(context.Background())

	otelHandler := otelgrpc.NewClientHandler(otelgrpc.WithMeterProvider(mp))

	http.Handle("/metrics", promhttp.Handler())
	go func() {
		addr := fmt.Sprintf(":%d", *metricsPort)
		log.Infof("Starting Prometheus metrics server on %s", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Errorf("Failed to start metrics server: %v", err)
		}
	}()

	requestPayload := make([]byte, *payloadBytes)
	for i := range requestPayload {
		requestPayload[i] = byte(i % 256)
	}

	offset := podOrdinalOffset(*clientCount)
	log.Infof("Starting %d benchmark clients (IDs %d..%d)...", *clientCount, offset, offset+*clientCount-1)

	var wg sync.WaitGroup
	for i := 0; i < *clientCount; i++ {
		clientID := fmt.Sprintf("benchmark-client-%d", offset+i)
		serverID := fmt.Sprintf("benchmark-server-%d", offset+i)

		activeClients.Inc()
		wg.Add(1)
		go func(cid, sid string) {
			defer wg.Done()
			runClient(cid, sid, requestPayload, *natsURL, time.Duration(*requestTimeout)*time.Second, otelHandler)
		}(clientID, serverID)

		if i > 0 && i%100 == 0 {
			time.Sleep(100 * time.Millisecond)
			log.Infof("Started %d clients...", i)
		}
	}

	log.Infof("All %d clients started successfully", *clientCount)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	log.Infof("Shutting down clients...")
	log.Infof("Client shutdown complete")
}

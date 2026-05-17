# Benchmark Example

This example demonstrates a high-scale benchmark of nats-grpc with configurable client/server pairs, payload sizes, and Prometheus metrics.

## Overview

The benchmark spawns N server instances and N client instances, where each client is paired with a specific server. Each client sends unary gRPC requests at 1Hz (1 request per second) with configurable payload sizes.

## Architecture

```
┌─────────────┐        ┌─────────────┐        ┌─────────────┐
│  Client 0   │────────│  Server 0   │        │  Client N   │
│             │   1Hz  │             │        │             │
│  4KB req    │────────│  4KB resp   │   ...  │  4KB req    │
└─────────────┘        └─────────────┘        └─────────────┘
      │                      │                        │
      └──────────────────────┼────────────────────────┘
                             │
                      ┌─────────────┐
                      │ NATS Server │
                      └─────────────┘
```

## Features

- **Configurable Scale**: Spawn 1-1000+ client-server pairs
- **Configurable Payload**: Customize request/response payload size
- **1:1 Pairing**: Each client targets a specific server via unique serverID
- **1Hz Rate**: Each client sends 1 request per second
- **Prometheus Metrics**: Comprehensive metrics for monitoring
- **Independent Connections**: Each client and server has its own NATS connection
- **Graceful Shutdown**: Clean shutdown on SIGINT/SIGTERM
- **Kubernetes Support**: Deploy at scale with Helm chart

## Prerequisites

1. **NATS Server**: Start a NATS server
   ```bash
   nats-server
   ```

2. **Generate Proto Files** (if not already done):
   ```bash
   cd examples/benchmark
   mkdir -p protos/benchmark
   protoc -I ./protos \
     --go_out=./protos/benchmark \
     --go-grpc_out=./protos/benchmark \
     ./protos/benchmark.proto
   ```

## Local Usage

### Start the Server

```bash
# Start with default settings (1000 servers, 4KB payload)
go run server/main.go

# Start with custom settings
go run server/main.go \
  --server-count=100 \
  --payload-size=8192 \
  --nats-url=nats://localhost:4222 \
  --metrics-port=9090
```

### Start the Client

```bash
# Start with default settings (1000 clients, 4KB payload)
go run client/main.go

# Start with custom settings
go run client/main.go \
  --client-count=100 \
  --payload-size=8192 \
  --nats-url=nats://localhost:4222 \
  --metrics-port=9091
```

## Configuration Options

### Server Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--nats-url` | `nats://localhost:4222` | NATS server URL |
| `--server-count` | `1000` | Number of servers to spawn |
| `--payload-size` | `4096` | Response payload size in bytes |
| `--metrics-port` | `9090` | Prometheus metrics HTTP port |

### Client Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--nats-url` | `nats://localhost:4222` | NATS server URL |
| `--client-count` | `1000` | Number of clients to spawn |
| `--payload-size` | `4096` | Request payload size in bytes |
| `--metrics-port` | `9091` | Prometheus metrics HTTP port |
| `--request-timeout` | `5` | gRPC request timeout in seconds |

## Metrics

### Server Metrics (http://localhost:9090/metrics)

- `grpc_server_requests_total{server_id, method}` - Total requests received
- `grpc_server_request_duration_seconds{server_id, method}` - Request duration histogram
- `grpc_server_payload_bytes{server_id, direction}` - Payload size histogram (request/response)

### Client Metrics (http://localhost:9091/metrics)

- `grpc_client_requests_total{client_id, method, status}` - Total requests sent (success/error)
- `grpc_client_request_duration_seconds{client_id, method}` - Request duration histogram
- `grpc_client_payload_bytes{client_id, direction}` - Payload size histogram (request/response)
- `grpc_client_active_total` - Number of active clients

## Example: Small Scale Test

For testing, start with a smaller number of clients/servers:

```bash
# Terminal 1: Start NATS server
nats-server

# Terminal 2: Start 10 servers
go run server/main.go --server-count=10

# Terminal 3: Start 10 clients
go run client/main.go --client-count=10

# Terminal 4: View server metrics
curl http://localhost:9090/metrics | grep grpc_server_requests_total

# Terminal 5: View client metrics
curl http://localhost:9091/metrics | grep grpc_client_requests_total
```

## Example: Large Scale Test




Expected behavior:
- 1000 client-server pairs
- Each client sends 1 request/second
- Total system throughput: 1000 requests/second
- Total payload throughput: ~8 MB/s (4KB request + 4KB response) × 1000

## Kubernetes Deployment with Helm

For production-scale deployments, use the included Helm chart to deploy the benchmark on Kubernetes.


### Building the Docker Image

From the `examples/benchmark` directory:

```bash
# Build for linux/amd64 platform (required for most Kubernetes clusters)
docker build --platform -t nats-grpc-benchmark:latest -f Dockerfile ../..
```

If using a registry:

```bash
# Build and push for linux/amd64 platform in one command
docker build --platform linux/amd64 --push -t us-central1-docker.pkg.dev/encoded-stage-394013/containers/nats-grpc-benchmark:latest -f Dockerfile ../..
```

**Note**: The `--platform linux/amd64` flag is important to ensure the image is compatible with amd64-based Kubernetes nodes, especially if building on ARM-based machines (e.g., Apple Silicon M1/M2).

### Installing the Helm Chart

```bash
# Install/upgrade with default values
helm upgrade --install nats-grpc-benchmark -n sub-agent ./helm
```



### Scaling on Kubernetes

Total servers/clients = replicaCount × count per pod (both default to
`common.replicaCount` × `common.count`).

Examples (defaults):
- 100 pods × 1000 servers = 100 000 total servers
- 100 pods × 1000 clients = 100 000 total clients

### Kubernetes Monitoring

#### Datadog Metrics Export

The Helm chart is configured to export metrics to Datadog by default using Autodiscovery annotations. The Datadog Agent DaemonSet will automatically discover and scrape metrics from the pods.

**Configuration in `values.yaml`:**
```yaml
datadog:
  enabled: true  # Enable/disable Datadog autodiscovery
  namespace: "nats_grpc_benchmark"  # Datadog metrics namespace
  metricsPattern: "grpc_*"  # Metrics pattern to collect
```

**Metrics Available in Datadog:**
- `nats_grpc_benchmark.grpc_server_requests_total`
- `nats_grpc_benchmark.grpc_server_request_duration_seconds`
- `nats_grpc_benchmark.grpc_server_payload_bytes`
- `nats_grpc_benchmark.grpc_client_requests_total`
- `nats_grpc_benchmark.grpc_client_request_duration_seconds`
- `nats_grpc_benchmark.grpc_client_payload_bytes`
- `nats_grpc_benchmark.grpc_client_active_total`

**To disable Datadog and use Prometheus instead:**
```bash
helm upgrade --install nats-grpc-benchmark ./helm \
  --set datadog.enabled=false \
  --set serviceMonitor.enabled=true
```

#### Prometheus Metrics on Kubernetes

Both client and server expose Prometheus metrics:
- Server: `http://<pod-ip>:9090/metrics`
- Client: `http://<pod-ip>:9091/metrics`




### Kubernetes Architecture

```
┌─────────────────┐
│ Client Pod 1    │──┐
│ (100 clients)   │  │
└─────────────────┘  │
                     │    ┌──────────┐
┌─────────────────┐  │    │          │     ┌─────────────────┐
│ Client Pod 2    │──┼───▶│   NATS   │◀────┤ Server Pod 1    │
│ (100 clients)   │  │    │          │     │ (100 servers)   │
└─────────────────┘  │    └──────────┘     └─────────────────┘
                     │                             ▲
        ...          │                             │
                     │                             │
┌─────────────────┐  │                      ┌─────────────────┐
│ Client Pod N    │──┘                      │ Server Pod N    │
│ (100 clients)   │                         │ (100 servers)   │
└─────────────────┘                         └─────────────────┘
        │                                           │
        │                                           │
        ▼                                           ▼
   Metrics:9091                               Metrics:9090
```

### Uninstalling the Helm Chart

```bash
helm uninstall nats-grpc-benchmark
```

## Monitoring with Prometheus

Create a `prometheus.yml` configuration:

```yaml
global:
  scrape_interval: 5s

scrape_configs:
  - job_name: 'benchmark-server'
    static_configs:
      - targets: ['localhost:9090']
  
  - job_name: 'benchmark-client'
    static_configs:
      - targets: ['localhost:9091']
```

## Performance Tuning

For large-scale benchmarks (1000+ clients), consider:

1. **Increase OS limits**:
   ```bash
   ulimit -n 10000  # Increase max open files
   ```

2. **NATS Server tuning**:
   ```bash
   nats-server --max_connections 3000 --max_payload 16777216
   ```

3. **Monitor system resources**:
   - CPU usage
   - Memory usage
   - Network bandwidth
   - Open file descriptors

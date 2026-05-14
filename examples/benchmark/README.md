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

## Prerequisites

1. **NATS Server**: Start a NATS server
   ```bash
   nats-server
   ```

2. **Generate Proto Files** (if not already done):
   ```bash
   cd examples
   mkdir -p protos/benchmark
   protoc -I ./protos \
     --go_out=./protos/benchmark \
     --go-grpc_out=./protos/benchmark \
     ./protos/benchmark.proto
   ```

## Usage

### Start the Server

```bash
# Start with default settings (1000 servers, 4KB payload)
go run examples/benchmark/server/main.go

# Start with custom settings
go run examples/benchmark/server/main.go \
  --server-count=100 \
  --payload-size=8192 \
  --nats-url=nats://localhost:4222 \
  --metrics-port=9090
```

### Start the Client

```bash
# Start with default settings (1000 clients, 4KB payload)
go run examples/benchmark/client/main.go

# Start with custom settings
go run examples/benchmark/client/main.go \
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
go run examples/benchmark/server/main.go --server-count=10

# Terminal 3: Start 10 clients
go run examples/benchmark/client/main.go --client-count=10

# Terminal 4: View server metrics
curl http://localhost:9090/metrics | grep grpc_server_requests_total

# Terminal 5: View client metrics
curl http://localhost:9091/metrics | grep grpc_client_requests_total
```

## Example: Large Scale Test

For a full-scale benchmark:

```bash
# Terminal 1: Start NATS server
nats-server

# Terminal 2: Start 1000 servers with 4KB payloads
go run examples/benchmark/server/main.go \
  --server-count=1000 \
  --payload-size=4096

# Terminal 3: Start 1000 clients with 4KB payloads
go run examples/benchmark/client/main.go \
  --client-count=1000 \
  --payload-size=4096
```

Expected behavior:
- 1000 client-server pairs
- Each client sends 1 request/second
- Total system throughput: 1000 requests/second
- Total payload throughput: ~8 MB/s (4KB request + 4KB response) × 1000

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

Start Prometheus:
```bash
prometheus --config.file=prometheus.yml
```

Access the Prometheus UI at http://localhost:9090

### Example Queries

```promql
# Total requests per second (server-side)
rate(grpc_server_requests_total[1m])

# Average request duration
histogram_quantile(0.95, rate(grpc_client_request_duration_seconds_bucket[1m]))

# Error rate
rate(grpc_client_requests_total{status="error"}[1m])

# Success rate
rate(grpc_client_requests_total{status="success"}[1m])
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

## Troubleshooting

### Too many open files

```bash
# Increase file descriptor limit
ulimit -n 10000
```

### NATS connection errors

- Ensure NATS server is running: `nats-server`
- Check NATS server logs for errors
- Verify `--nats-url` flag is correct

### Metrics not showing

- Verify metrics ports are accessible: `curl http://localhost:9090/metrics`
- Check for port conflicts
- Ensure clients/servers are running

## Cleanup

Stop the benchmark with `Ctrl+C` in each terminal. The applications will gracefully shut down.

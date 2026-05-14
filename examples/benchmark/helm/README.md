# nats-grpc Benchmark Helm Chart

This Helm chart deploys both the nats-grpc benchmark client and server on Kubernetes.

## Prerequisites

- Kubernetes cluster (1.19+)
- Helm 3.0+
- NATS server running in the cluster
- (Optional) Prometheus Operator for ServiceMonitor support

## Building the Docker Image

From the root of the repository:

```bash
docker build -t nats-grpc-benchmark:latest -f examples/benchmark/Dockerfile .
```

If using a registry:

```bash
docker build -t us-central1-docker.pkg.dev/encoded-stage-394013/containers/nats-grpc-benchmark:latest -f examples/benchmark/Dockerfile .
docker push us-central1-docker.pkg.dev/encoded-stage-394013/containers/nats-grpc-benchmark:latest
```

## Installing the Chart

```bash
# Install with default values
helm install nats-grpc-benchmark ./examples/benchmark/helm

# Install with custom values
helm install nats-grpc-benchmark ./examples/benchmark/helm \
  --set image.repository=your-registry/nats-grpc-benchmark \
  --set nats.url=nats://your-nats-server:4222 \
  --set server.replicaCount=10 \
  --set client.replicaCount=10
```

## Configuration

### Key Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `image.repository` | Image repository | `nats-grpc-benchmark` |
| `image.tag` | Image tag | `latest` |
| `nats.url` | NATS server URL | `nats://nats:4222` |
| `server.enabled` | Enable server deployment | `true` |
| `server.replicaCount` | Number of server pods | `10` |
| `server.serverCount` | Servers per pod | `100` |
| `client.enabled` | Enable client deployment | `true` |
| `client.replicaCount` | Number of client pods | `10` |
| `client.clientCount` | Clients per pod | `100` |
| `serviceMonitor.enabled` | Enable Prometheus ServiceMonitor | `false` |

### Full Configuration

See `values.yaml` for all available configuration options.

## Scaling

Total servers/clients = replicaCount × count per pod

Examples:
- 10 pods × 100 servers = 1000 total servers
- 10 pods × 100 clients = 1000 total clients

Adjust based on your cluster resources:

```bash
# Scale to 5000 clients total (50 pods × 100 clients)
helm upgrade nats-grpc-benchmark ./examples/benchmark/helm \
  --set client.replicaCount=50 \
  --set client.clientCount=100
```

## Monitoring

### Prometheus Metrics

Both client and server expose Prometheus metrics:
- Server: `http://<pod-ip>:9090/metrics`
- Client: `http://<pod-ip>:9091/metrics`

### Enable ServiceMonitor

If using Prometheus Operator:

```bash
helm upgrade nats-grpc-benchmark ./examples/benchmark/helm \
  --set serviceMonitor.enabled=true
```

### Port Forward for Local Access

```bash
# Server metrics
kubectl port-forward svc/nats-grpc-benchmark-server 9090:9090

# Client metrics  
kubectl port-forward svc/nats-grpc-benchmark-client 9091:9091
```

## Uninstalling

```bash
helm uninstall nats-grpc-benchmark
```

## Architecture

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

## Troubleshooting

### Check Pod Status

```bash
kubectl get pods -l release=nats-grpc-benchmark
```

### View Logs

```bash
# Server logs
kubectl logs -l app=nats-grpc-benchmark-server

# Client logs
kubectl logs -l app=nats-grpc-benchmark-client
```

### Check Metrics

```bash
# Check if metrics are being exported
kubectl exec -it <pod-name> -- wget -q -O- localhost:9090/metrics | head
```

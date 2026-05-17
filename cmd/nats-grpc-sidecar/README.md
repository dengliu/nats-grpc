# nats-grpc-sidecar

A language-agnostic sidecar that bridges gRPC and NATS. Drop it next to any
gRPC application — in any language — and the application can issue and
serve RPCs over a NATS bus without importing nats-grpc.

For the *why* and the wire protocol see [`SIDECAR.md`](../../SIDECAR.md).
For a runnable end-to-end demo see [`examples/sidecar/`](../../examples/sidecar/).
This README is the operator's reference for the binary itself.

## What it does

Two loopback gRPC ports, plus one outbound NATS connection:

| Port (default) | Purpose |
|---|---|
| `127.0.0.1:50051` | **Egress.** The local app dials this to make outbound RPCs. Each call must carry routing metadata (see [Reserved headers](#reserved-headers)). |
| `127.0.0.1:50100` | **Admin.** The local app calls `SidecarAdmin.Register` on this port at boot to declare which gRPC services it serves and under which svcid. |

Routing is **per-call, metadata-driven**. The caller stamps `x-nats-svcid`
onto each call; the sidecar uses that as the NATS routing key. There is
no static `service→svcid` config file.

## Building

### Local binary

```sh
go build -o nats-grpc-sidecar ./cmd/nats-grpc-sidecar
./nats-grpc-sidecar -nats nats://localhost:4222
```

### Container

From the repo root:

```sh
docker build -f cmd/nats-grpc-sidecar/Dockerfile -t nats-grpc-sidecar:latest .
```

Multi-stage build (`golang:1.26-alpine` → `alpine:latest`), statically
linked, stripped. Exposes ports `50051` and `50100`.

For multi-arch (Apple Silicon → amd64 cluster):

```sh
docker build --platform linux/amd64 \
  -f cmd/nats-grpc-sidecar/Dockerfile \
  -t nats-grpc-sidecar:latest .
```

## Flags

```
-nats     string  NATS server URL (default "nats://localhost:4222")
-egress   string  egress gRPC listen addr (default "127.0.0.1:50051")
-admin    string  admin gRPC listen addr (default "127.0.0.1:50100")
-nid      string  sidecar nid (default: auto-generated, "sc-<random hex>")
```

Defaults bind to loopback because the intended deployment is a Kubernetes
**pod-level sidecar**: app and sidecar containers share the pod's network
namespace, so the app reaches the sidecar at `127.0.0.1`. For
process-pair or bare-metal deployments where the app is in a different
network namespace, override `-egress` / `-admin` with a non-loopback
address — and front it with TLS or a firewall, because the admin port
has no authentication.

## Reserved headers

Callers control routing per call via gRPC metadata. The sidecar
**consumes and strips** these before forwarding:

| Header | When required | Meaning |
|---|---|---|
| `x-nats-svcid` | every egress call | Target backend svcid (NATS routing key). Missing → `InvalidArgument` before any NATS publish. |
| `x-nats-mode` | optional (default `unary`) | `unary` or `streaming`. Streaming pins the call to one server replica. |
| `x-nats-target-nid` | required when `mode=streaming` | Specific server replica to talk to. Required because streaming spans multiple frames and they must all land on the same backend. |

Non-`x-nats-*` metadata is forwarded transparently — propagate trace
headers, tenant IDs, auth tokens through unchanged.

## Lifecycle

- **Startup**: opens the NATS connection and both gRPC listeners, then
  blocks. Egress and admin start serving immediately; no warm-up.
- **Ingress registration**: lazy. The sidecar opens no NATS
  subscriptions until a local app calls `SidecarAdmin.Register`. Each
  registration is leased by an open gRPC stream — drop the stream,
  registrations evaporate.
- **Shutdown** (`SIGINT` / `SIGTERM`): tears down all live registrations
  (so the upstream apps' streams drain cleanly), then closes the gRPC
  servers and the NATS connection. No graceful drain window for in-flight
  RPCs — they fail fast with `Unavailable`. Don't terminate the sidecar
  before the app it serves.

## Kubernetes deployment sketch

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-app
spec:
  template:
    spec:
      containers:
        # The application: any gRPC server / client, any language.
        # Dials 127.0.0.1:50051 for outbound RPCs.
        # Calls SidecarAdmin.Register on 127.0.0.1:50100 at boot.
        - name: app
          image: my-app:latest

        # The sidecar: shares the pod's network namespace with `app`.
        - name: nats-grpc-sidecar
          image: nats-grpc-sidecar:latest
          args:
            - "-nats=nats://nats.default.svc.cluster.local:4222"
            - "-egress=127.0.0.1:50051"
            - "-admin=127.0.0.1:50100"
          ports:
            - { containerPort: 50051, name: egress }
            - { containerPort: 50100, name: admin }
```

The app declares ingress (the svcids it serves) at startup via the admin
RPC — no annotations, no operator, no config map. svcid is decided
in app code, often from a tenant claim or pod label.

## Production checklist

- **NATS auth**: pass credentials via NATS URL (`nats://user:pass@…`) or
  bake `nats.Option`s into a custom `main.go` if you need certs. The
  current binary does not load mTLS material from flags.
- **Resource sizing**: each egress call is one goroutine, one outbound
  NATS publish, one inbound message. Each ingress streaming RPC is one
  worker goroutine. Ballpark: tens of thousands of concurrent in-flight
  calls on a single sidecar is uncontroversial.
- **Observability**: not exposed by this binary directly. To add traces /
  metrics, wrap `pkg/sidecar` in your own `main.go` and pass
  `rpc.WithStatsHandler` / `rpc.WithServerStatsHandler` (otelgrpc plugs
  in directly — see `pkg/rpc/integration_test.go` for the pattern).
- **Loopback only**: the admin port is unauthenticated; never bind it to
  a routable interface.

## See also

- [`SIDECAR.md`](../../SIDECAR.md) — full design, wire protocol, open
  questions.
- [`examples/sidecar/`](../../examples/sidecar/) — runnable two-backend
  scenario plus a `grpcurl` walkthrough demonstrating that no nats-grpc
  knowledge is required on the client side.
- [`pkg/sidecar/`](../../pkg/sidecar/) — Go API if you want to embed the
  sidecar in another binary instead of running it standalone.

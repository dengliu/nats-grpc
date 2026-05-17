# Sidecar example — dynamic per-call routing

A runnable scenario showing how the `nats-grpc` sidecar bridges plain gRPC
and NATS, with the **same gRPC stub** routing each call to a different
backend purely via metadata.

```
                  ┌──────────────────────────────────────┐
                  │           NATS server                │
                  └────────────┬──────────────┬──────────┘
                               │              │
              ┌────────────────┘              └────────────────┐
              ▼                                                 ▼
       ┌──────────────┐                                  ┌──────────────┐
       │  sidecar     │                                  │  sidecar     │
       │ (egress)     │                                  │ (ingress)    │
       │ :50051       │                                  │              │
       │ :50100 admin │                                  │ :50100 admin │
       └──────▲───────┘                                  └──────▲───────┘
              │                                                 │
              │ gRPC over loopback                              │ gRPC over loopback
              │   metadata: x-nats-svcid=serviceid_1|2          │   ↑
              │                                                 │   │ register
       ┌──────┴───────┐                                  ┌──────┴───┴───┐
       │    client    │                                  │   backends   │
       │ (calls 2x,   │                                  │  serviceid_1 │
       │  different   │                                  │  serviceid_2 │
       │  svcid each) │                                  │   (one each) │
       └──────────────┘                                  └──────────────┘
```

The point: the client uses an **ordinary gRPC stub** (no nats-grpc imports,
no awareness of NATS). It picks a backend per call by stamping
`x-nats-svcid` into the call's metadata. Same stub, different routing.

## What's in here

- `server/` — echo backend. Stands up a real gRPC server, then calls
  `SidecarAdmin.Register` to tell the sidecar "I'm serving `echo.Echo`
  under svcid X; forward calls to me at this address."
- `client/` — calls `SayHello` twice via the same stub, with two
  different `x-nats-svcid` headers. Each call lands on a different
  backend; the response prefix proves it.

The sidecar binary itself lives in [`cmd/nats-grpc-sidecar/`](../../cmd/nats-grpc-sidecar)
— it's a real shippable artifact, not example code. A Dockerfile sits
next to it for container builds.

## Prerequisites

- A running NATS server. The simplest way:
  ```sh
  go install github.com/nats-io/nats-server/v2@latest
  nats-server
  ```
  Or `docker run -p 4222:4222 nats:latest`.

- For the polyglot demo at the bottom, [`grpcurl`](https://github.com/fullstorydev/grpcurl):
  ```sh
  go install github.com/fullstorydev/grpcurl/cmd/grpcurl@latest
  ```

## Running it

You'll need five terminals. The roles are: NATS, two sidecars, two
backends, and the client. One sidecar would technically suffice for
the egress side, but two separate sidecars more closely mirror a
Kubernetes deployment where each pod runs its own.

```sh
# Terminal 1 — NATS
nats-server

# Terminal 2 — sidecar 1 (egress for the client; ingress for backend A)
go run ./cmd/nats-grpc-sidecar \
  -egress 127.0.0.1:50051 -http-admin 127.0.0.1:50101 \
  -nats nats://localhost:4222

# Terminal 3 — sidecar 2 (ingress for backend B)
go run ./cmd/nats-grpc-sidecar \
  -egress 127.0.0.1:50052 -http-admin 127.0.0.1:50201 \
  -nats nats://localhost:4222

# Terminal 4 — backend A (registers as serviceid_1 against sidecar 1)
go run ./examples/sidecar/server -svcid serviceid_1 -admin http://127.0.0.1:50101/v1/register

# Terminal 5 — backend B (registers as serviceid_2 against sidecar 2)
go run ./examples/sidecar/server -svcid serviceid_2 -admin http://127.0.0.1:50201/v1/register

# Terminal 6 — client (dials sidecar 1's egress port)
go run ./examples/sidecar/client -egress 127.0.0.1:50051
```

Expected client output:

```
→ serviceid_1: reply="serviceid_1: hello world"
→ serviceid_2: reply="serviceid_2: hello world"
```

The client made two calls through the same stub; the routing was
decided entirely by the `x-nats-svcid` value attached to each call's
context. Neither backend knows about NATS — they're plain `echo.Echo`
servers, and registration is plain HTTP/JSON (no nats-grpc
dependency).

In a backend's terminal you'll also see periodic heartbeat acks like
`heartbeat ack ts=1716000000000000000` every 30 seconds. The open
HTTP connection to the sidecar IS the registration lease; closing
the terminal drops the connection and the sidecar deregisters
automatically.

### Single-sidecar mode

A single sidecar instance can serve both ingress registrations
because the HTTP admin allows multiple concurrent POSTs. Drop
terminal 3 and point both backends at `http://127.0.0.1:50101/v1/register`.

## The polyglot story — grpcurl

The whole point of the sidecar is that the local app's *language* is
irrelevant. To prove it, hit the same egress port with `grpcurl` — a
generic CLI tool that has zero awareness of nats-grpc:

```sh
grpcurl \
  -plaintext \
  -import-path ./examples/protos \
  -proto echo.proto \
  -H "x-nats-svcid: serviceid_1" \
  -d '{"msg":"world"}' \
  127.0.0.1:50051 \
  echo.Echo/SayHello
```

Response:
```json
{
  "msg": "serviceid_1: hello world"
}
```

Swap `serviceid_1` → `serviceid_2` in the `-H` header and you'll see
the call routed to the other backend. Same gRPC port, same proto,
different backend — selected by one header value.

Any gRPC client in any language can do exactly this. The local app
does not need to import nats-grpc.

## Failure modes worth observing

A few things to try once it's running:

- **Missing routing header.** Calls without `x-nats-svcid` return
  `InvalidArgument` from the sidecar without ever publishing to NATS:
  ```sh
  grpcurl -plaintext -import-path ./examples/protos -proto echo.proto \
    -d '{"msg":"x"}' 127.0.0.1:50051 echo.Echo/SayHello
  # Error: rpc error: code = InvalidArgument desc = x-nats-svcid header is required
  ```

- **Backend dies.** Kill terminal 4 (the `serviceid_1` backend). The
  next call to `serviceid_1` fails (`Unavailable` or `DeadlineExceeded`
  depending on NATS server version — see `pkg/sidecar/sidecar_test.go`
  for why). Calls to `serviceid_2` continue to work.

- **Reserved headers are stripped.** Add `-H "x-nats-svcid: serviceid_1"
  -H "x-tenant: acme"` to the grpcurl call. The backend logs will show
  `x-tenant: [acme]` in its incoming metadata, but **not** `x-nats-svcid`
  — the sidecar strips its own routing fuel before forwarding.

## Building the container

For a Kubernetes (or any container-runtime) deployment, build the
sidecar image from the repo root:

```sh
docker build -f cmd/nats-grpc-sidecar/Dockerfile -t nats-grpc-sidecar:latest .
```

The Dockerfile is a multi-stage build (alpine-based) that produces a
small statically-linked image exposing ports `50051` (egress) and
`50100` (admin). Override the bind addresses with `-egress` /
`-admin` flags when running outside a Kubernetes pod (where loopback
is shared across containers).

## What this example does NOT demonstrate

- **Streaming.** Would need `x-nats-mode: streaming` and
  `x-nats-target-nid: <sidecar nid>`. See `pkg/sidecar/sidecar_test.go`
  → `TestEndToEnd_Streaming` for the wire shape.
- **TLS / auth.** The sidecar trusts loopback. Adding TLS to the
  sidecar↔app hop is straightforward (`grpc.NewServer` with creds)
  but out of scope here.
- **Discovery.** The client must know which svcids exist. A future
  discovery layer (NATS service framework or a custom heartbeat
  subject) would make this dynamic — see `SIDECAR.md` §12.

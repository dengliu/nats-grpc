# Sidecar demo — docker-compose, Go ⇄ Python

A fully runnable scenario that brings up:

- **1 NATS server**
- **4 nats-grpc sidecars** (one per app)
- **2 backends** — a Go echo server and a Python echo server
- **2 clients** — a Go client and a Python client, each calling
  *both* backends every 3 seconds, alternating between them via the
  `x-nats-svcid` header

The point: nothing in the language matrix matters. The Python client
hits the Go backend exactly the same way the Go client does, and the
Go client hits the Python backend exactly the same way. The only
nats-grpc dependency in any of the four apps is the **sidecar
container itself**.

## Topology

```
                   ┌───────────────────────┐
                   │   nats (port 4222)    │
                   └──────────┬────────────┘
                              │
        ┌─────────────────────┼─────────────────────┐─────────────────────┐
        │                     │                     │                     │
   ┌────▼─────────────┐  ┌────▼─────────────┐  ┌────▼─────────────┐  ┌────▼─────────────┐
   │ sidecar-go-server│  │ sidecar-py-server│  │ sidecar-go-client│  │ sidecar-py-client│
   └────┬─────────────┘  └────┬─────────────┘  └────┬─────────────┘  └─┬────────────────┘
        │ (same netns)        │ (same netns)        │ (same netns)     │ (same netns)
   ┌────▼────────────┐  ┌─────▼───────────┐  ┌──────▼─────────┐  ┌─────▼───────────┐
   │ go-server       │  │ py-server       │  │ go-client      │  │ py-client       │
   │ svcid=go-server │  │ svcid=python-…  │  │  (egress only) │  │  (egress only)  │
   │ ":8080" gRPC    │  │ ":8080" gRPC    │  │  dials :50051  │  │  dials :50051   │
   └─────────────────┘  └─────────────────┘  └────────────────┘  └─────────────────┘
```

Each backend pairs with its sidecar via
`network_mode: "service:<sidecar>"` — the docker-compose analog of a
Kubernetes pod-mate. App + sidecar share a single network namespace,
talking over loopback (`127.0.0.1`).

## Running

From the repo root:

```sh
docker compose up --build
```

The `--build` flag forces a build the first time; later runs can drop
it. Once everything is up you'll see logs interleaved from all eight
containers (4 sidecars + 2 servers + 2 clients).

To shut down cleanly:

```sh
docker compose down
```

## Expected output

The first few lines from each app (interleaved in `docker compose`'s
output):

```
nats           | [INF] Starting nats-server
sidecar-go-…   | sidecar ready — nid=sc-go-server  egress=127.0.0.1:50051  http-admin=127.0.0.1:50101
sidecar-py-…   | sidecar ready — nid=sc-py-server  …
sidecar-…-cli  | sidecar ready — nid=sc-go-client  …
sidecar-…-cli  | sidecar ready — nid=sc-py-client  …
go-server      | go echo server listening on 127.0.0.1:8080
go-server      | registered as svcid="go-server"  sidecar.nid=sc-go-server
py-server      | python echo server listening on 127.0.0.1:8080
py-server      | registered as svcid='python-server'  sidecar.nid=sc-py-server
```

Then once per 3 seconds, each client emits a request and you see
the matched backend log it. Requests are `<sender> -> <target> #<N>`;
replies swap sender/target with the same counter:

```
go-client | go-client -> py-server #1   ⇒  py-server -> go-client #1
py-server | SayHello in='go-client -> py-server #1'   out='py-server -> go-client #1'

py-client | py-client -> py-server #1   ⇒  py-server -> py-client #1
py-server | SayHello in='py-client -> py-server #1'   out='py-server -> py-client #1'

go-client | go-client -> go-server #2   ⇒  go-server -> go-client #2
go-server | SayHello in="go-client -> go-server #2"  out="go-server -> go-client #2"

py-client | py-client -> go-server #2   ⇒  go-server -> py-client #2
go-server | SayHello in="py-client -> go-server #2"  out="go-server -> py-client #2"
```

Each client alternates: odd-numbered requests go to `python-server`,
even-numbered to `go-server`. Both clients drive the same pattern in
parallel.

## What's worth poking at

- **`docker compose stop py-server`** — the Python backend
  disappears, its sidecar deregisters within milliseconds, and you'll
  see `→ python-server  error: UNAVAILABLE …` in both clients' logs
  for odd-numbered requests. Even-numbered ones still succeed against
  the Go backend.

- **`docker compose stop go-client`** — the Go client's sidecar
  keeps running but has nothing to serve egress for. Nothing else is
  affected.

- **Swap which language fronts what svcid** — restart `go-server`
  with `SVCID=python-server` (and vice versa) and the demo keeps
  working with the language assignments reversed. Routing is data,
  not config.

## Files

```
docker-compose.yaml      one NATS, four sidecars, four apps
go-server/
  Dockerfile             multi-stage Go build
  main.go                echo backend, registers via HTTP
go-client/
  Dockerfile             multi-stage Go build
  main.go                alternating-target client loop
py-server/
  Dockerfile             python:3.11 + grpcio + proto codegen
  requirements.txt
  server.py
py-client/
  Dockerfile
  requirements.txt
  client.py
```

The Python services generate `echo_pb2.py` / `echo_pb2_grpc.py` at
image-build time from `../../protos/echo.proto`, so there are no
checked-in generated stubs. The Go services use the existing
generated stubs under `examples/protos/echo/`.

## Why four sidecars (not one)?

Each app is paired with its *own* sidecar via shared network
namespace. This mirrors the production deployment model — every pod
in Kubernetes runs its own sidecar container next to the app
container. One sidecar per app keeps each app's `127.0.0.1:50051`
private to that pod-mate pair.

A single shared sidecar would technically work for this demo (the
admin endpoint accepts multiple concurrent registrations) but
wouldn't reflect how this is deployed in practice.

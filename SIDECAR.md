# nats-grpc Sidecar — Design

A language-agnostic sidecar that bridges standard gRPC and the nats-grpc
protocol. Local applications speak vanilla gRPC to the sidecar over loopback;
the sidecar terminates the gRPC call, looks up routing from per-call
metadata, and forwards it over NATS to the appropriate backend. The reverse
path (local app as gRPC server) works the same way.

The goal is "polyglot nats-grpc": any gRPC implementation in any language
gets to use NATS as the transport without needing a per-language port of
this repo.

---

## 1. Routing model

Routing is **per-call, metadata-driven**. The caller stamps each gRPC call
with a metadata header naming the target svcid; the sidecar reads the header
and uses it as the NATS routing key. There is no static `service → svcid`
config — by design, because the user requirement is that the same RPC
method needs to reach different svcids over the lifetime of a single
process.

### Reserved headers

| Header | Direction | Required | Purpose |
|---|---|---|---|
| `x-nats-svcid` | egress | **yes** | NATS svcid the call routes to |
| `x-nats-mode` | egress | no (default `unary`) | `unary` or `streaming` — selects the NATS dispatch path |
| `x-nats-target-nid` | egress | only for `streaming` mode | Picks a specific server replica for streaming RPCs |
| `x-nats-timeout` | egress | no | Per-call timeout override (overrides the context deadline if shorter) |

All `x-nats-*` headers are **stripped** before forwarding — neither the
backend handler nor the local server app ever sees them. The sidecar treats
them like the HTTP `Host` header: routing fuel, not application payload.

### Worked example

```go
// Call 1 → backend svcid serviceid_1
ctx1 := metadata.AppendToOutgoingContext(ctx, "x-nats-svcid", "serviceid_1")
client.Greet(ctx1, req)

// Call 2 → backend svcid serviceid_2 (same stub, same method)
ctx2 := metadata.AppendToOutgoingContext(ctx, "x-nats-svcid", "serviceid_2")
client.Greet(ctx2, req)
```

Behind the sidecar:

```
ctx1 → /pkg.Greeter/Greet, md{x-nats-svcid: serviceid_1}
     → publish to nrpc.unary.serviceid_1.pkg.Greeter.Greet
     → backend svcid serviceid_1

ctx2 → /pkg.Greeter/Greet, md{x-nats-svcid: serviceid_2}
     → publish to nrpc.unary.serviceid_2.pkg.Greeter.Greet
     → backend svcid serviceid_2
```

### Missing/invalid header

If `x-nats-svcid` is absent, the sidecar returns
`status.Error(codes.InvalidArgument, "x-nats-svcid header is required")`
**before** any NATS publish. This is non-negotiable: silently picking a
default is the kind of thing that's invisible in test and catastrophic in
prod. There is no fallback svcid.

If `x-nats-mode` has an unrecognized value (anything other than `unary` /
`streaming`), same treatment.

If `x-nats-mode: streaming` is set without `x-nats-target-nid`, same
treatment — streaming RPCs need point-to-point addressing.

---

## 2. Subject convention (wire protocol change)

The sidecar uses a new namespacing convention that structurally separates
unary and streaming dispatch. The legacy `nrpc.<svcid>.<service>.<method>`
layout — which forces a peek-at-request-type to decide dispatch and which
makes "safe load-balance" and "safe streaming" mutually exclusive — is **not
used by the sidecar**. The existing direct-Go API in `pkg/rpc` continues to
use the legacy format for now; cross-traffic between sidecar peers and
direct-API peers requires both endpoints to agree on the namespace (see §9).

### Unary

```
Subject:     nrpc.unary.<svcid>.<package>.<Service>.<Method>
Queue group: u:<svcid>:<package>.<Service>
```

All backend replicas serving the same svcid share the queue group. NATS
delivers each `UnaryRequest` to exactly one replica. Single message in,
single message out — safe by construction.

### Streaming

```
Subject:     nrpc.stream.<svcid>.<nid>.<package>.<Service>.<Method>
Queue group: (none)
```

`<nid>` is unique per replica (sidecar auto-generates — see §4.2). Each
replica subscribes only to its own nid-scoped subject; there is no queue
group, so no fan-out is possible. The caller's `x-nats-target-nid` header
maps directly to `<nid>` in the subject.

### Why this is the right time

The legacy layout's "one subject for both kinds, peek the request type to
dispatch" pattern is exactly the design I flagged as Fix #10 in the review.
I deferred fixing it on the direct API because it's a wire-format change
that breaks existing peers. The sidecar is new code with no existing peers,
so it ships with the cleaner layout from day one.

---

## 3. Component architecture

```
┌──────────────────────────────────────────────────────────────┐
│ Pod / Host                                                    │
│                                                               │
│  ┌─────────────────┐         ┌───────────────────────────┐    │
│  │ Local App       │  gRPC   │ nats-grpc Sidecar         │    │
│  │                 │ ──────► │                           │    │
│  │ gRPC client     │ :50051  │ ┌──────────────────────┐  │    │
│  │                 │         │ │ Egress (gRPC server) │  │    │
│  │ gRPC server     │ ◄────── │ └──────────┬───────────┘  │    │
│  │                 │ :8080   │            │              │    │
│  └────────┬────────┘         │            ▼              │    │
│           │ (admin)          │ ┌──────────────────────┐  │    │
│           │ :50100           │ │ NATS client          │  │    │
│           ▼                  │ │ (this repo's nrpc)   │  │    │
│  ┌────────────────┐          │ └──────────┬───────────┘  │    │
│  │ Sidecar admin  │          │            │              │    │
│  │ gRPC service   │          │ ┌──────────┴───────────┐  │    │
│  └────────────────┘          │ │ Ingress (NATS subs)  │  │    │
│                              │ └──────────────────────┘  │    │
│                              └───────────────┬───────────┘    │
│                                              │                │
└──────────────────────────────────────────────┼────────────────┘
                                               │
                                          ┌────▼────┐
                                          │  NATS   │
                                          └─────────┘
```

Three logical components inside the sidecar:

1. **Egress server** — vanilla gRPC server on loopback. The local app dials
   it for outbound RPCs.
2. **Ingress dispatcher** — NATS subscriber set; forwards inbound calls to
   the local app's gRPC server.
3. **Admin API** — gRPC service on a separate loopback port. The local app
   registers ingress svcid/services here at startup.

The local app sees the sidecar as two ordinary gRPC endpoints (egress +
admin) and is itself an ordinary gRPC server reachable on a loopback port.
It has zero source-level dependency on NATS or this repo.

---

## 4. Egress (local app → NATS)

### 4.1 Wire handling

The egress server uses gRPC's `UnknownServiceHandler` plus the raw-codec
proxy pattern (`pkg/rpc/proxy/handler.go` in this repo). Concretely:

```go
egress := grpc.NewServer(
    grpc.UnknownServiceHandler(sidecarUnknownHandler),
    grpc.ForceServerCodec(rpc.Codec()),  // raw bytes
)
```

`sidecarUnknownHandler` is the routing brain:

```go
func sidecarUnknownHandler(srv interface{}, stream grpc.ServerStream) error {
    fullMethod, _ := grpc.MethodFromServerStream(stream)
    md, _ := metadata.FromIncomingContext(stream.Context())

    svcid := first(md.Get("x-nats-svcid"))
    if svcid == "" {
        return status.Error(codes.InvalidArgument, "x-nats-svcid header is required")
    }
    mode := first(md.Get("x-nats-mode"))
    if mode == "" { mode = "unary" }

    // Strip routing headers before forwarding.
    forward := metadata.MD{}
    for k, v := range md {
        if !strings.HasPrefix(strings.ToLower(k), "x-nats-") {
            forward[k] = v
        }
    }
    outCtx := metadata.NewOutgoingContext(stream.Context(), forward)

    switch mode {
    case "unary":    return dispatchUnary(outCtx, svcid, fullMethod, stream)
    case "streaming":return dispatchStreaming(outCtx, svcid, md, fullMethod, stream)
    default:
        return status.Errorf(codes.InvalidArgument, "x-nats-mode %q invalid", mode)
    }
}
```

### 4.2 Unary dispatch

```go
func dispatchUnary(ctx context.Context, svcid, method string, stream grpc.ServerStream) error {
    // Read exactly one request frame.
    in := &rpc.Frame{}
    if err := stream.RecvMsg(in); err != nil { return err }

    // Use nrpc.Client.Invoke, bypassing its subject-builder via a per-call
    // svcid. (Today nrpc.Client carries svcid as constructor state; the
    // sidecar will need a small extension that lets Invoke take a
    // per-call svcid — see §10.)
    out := &rpc.Frame{}
    if err := nrpcClient.InvokeWithSvcID(ctx, svcid, method, in, out); err != nil {
        return err
    }
    return stream.SendMsg(out)
}
```

This path is queue-balanced across all backend replicas registered under
that svcid — multiple replicas competing on the queue group, NATS picks
one, returns one response.

### 4.3 Streaming dispatch

```go
func dispatchStreaming(ctx context.Context, svcid string, md metadata.MD, method string, server grpc.ServerStream) error {
    nid := first(md.Get("x-nats-target-nid"))
    if nid == "" {
        return status.Error(codes.InvalidArgument, "x-nats-target-nid is required for streaming mode")
    }
    client, err := nrpcClient.NewStreamWithSvcID(ctx, svcid, nid, method)
    if err != nil { return err }
    return proxy.PumpBidi(server, client)  // existing bidi pump from proxy/handler.go
}
```

Streaming pins the entire call to one server replica via the `<nid>`
component of the subject. The pump in `proxy/handler.go` already handles
the bidirectional copy correctly.

### 4.4 Response routing — how one sidecar handles N concurrent callers

A natural worry: "if multiple local gRPC clients in the same pod use one
sidecar (one NATS connection, one sidecar nid), how does the sidecar know
which response belongs to which caller?" The short answer is **responses
route by reply inbox subject, not by nid**. The sidecar's nid is metadata,
not a demultiplexing key.

The mechanism, end to end:

1. **nats.go owns the inbox plumbing.** Each `*nats.Conn` lazily creates
   one shared subscription to `_INBOX.<conn-uid>.>` the first time
   `Request()` is called. `<conn-uid>` is generated once when the
   connection opens. All replies for that connection — across all
   concurrent callers — come back on this single subscription.

2. **Each call gets a unique inbox suffix.** Per `RequestWithContext`
   invocation, nats.go generates a fresh ~22-byte random suffix and
   registers a per-call response channel in an internal map keyed by
   that suffix. The request is published with
   `reply-to: _INBOX.<conn-uid>.<suffix>`.

3. **The calling goroutine blocks on its private channel.** Two
   simultaneous calls from the same sidecar with identical subject and
   identical Call.Nid still get different suffixes, different map
   entries, and different blocked goroutines.

4. **The server replies via `msg.Respond`**, which publishes to the
   reply-to subject the request carried — guaranteed unique per call.

5. **The shared inbox dispatcher demuxes by suffix**, looks up the
   per-call channel, delivers the message, and unblocks the right
   goroutine.

Worked example: two local gRPC clients A and B in the same pod each
call `Greet` on `svcid=serviceid_1` through the sidecar at the same
moment:

```text
Goroutine A: nc.RequestWithContext("nrpc.unary.serviceid_1.pkg.Greeter.Greet", req_A)
  nats.go: respMap["AAA111"] = chA
           publish req_A with reply-to=_INBOX.conn1.AAA111
           block A on <-chA

Goroutine B: nc.RequestWithContext("nrpc.unary.serviceid_1.pkg.Greeter.Greet", req_B)
  nats.go: respMap["BBB222"] = chB           # different suffix
           publish req_B with reply-to=_INBOX.conn1.BBB222
           block B on <-chB

Backend replies via msg.Respond(...) for each:
  → publish to _INBOX.conn1.AAA111  (response for A)
  → publish to _INBOX.conn1.BBB222  (response for B)

Sidecar's shared inbox subscription delivers them to the dispatcher:
  AAA111 → respMap["AAA111"] → chA → unblocks goroutine A
  BBB222 → respMap["BBB222"] → chB → unblocks goroutine B
```

Streaming uses the same idea but explicitly: each `clientStream` calls
`utils.NewInBox()` and `ChanSubscribe`s to its own inbox up front, then
sets that inbox as the reply-to on the `Call` frame. The per-stream
goroutine reads from the per-stream inbox channel. No suffix demux
needed — one subscription per stream.

**Takeaway**: one sidecar with one nid scales to as many concurrent
local callers as the goroutine scheduler allows. nid plays no role in
response dispatch.

### 4.5 What nid is used for, and the cleanup-granularity tradeoff

Since nid doesn't route responses, what does it do? On streaming RPCs
the client's nid is carried in `Call.Nid` and stored server-side as
`pnid` ([server.go:610](pkg/rpc/server.go#L610)). Its only real use is
`Server.CloseStream(nid)` — a server-initiated bulk cleanup of all
streams whose `pnid` matches. (For unary, `Call.Nid` isn't even
transmitted: `UnaryRequest` has no Nid field; the whole point is moot.)

With option A (one sidecar nid for all egress), this means:

- ✅ Response dispatch — every concurrent call returns to the right
  goroutine via per-call inbox suffixes. Unaffected.
- ⚠️ Bulk cleanup granularity — a backend operator invoking
  `CloseStream("sidecar-instance-1")` would tear down **every streaming
  RPC from every local client routed through that sidecar**, not just
  one logical client's streams.

For the vast majority of deployments this is fine: nid-based cleanup is
rarely used in practice — streams normally end via context cancellation
or peer disconnect, not by the server proactively killing them. The
heartbeat does the heavy lifting for liveness.

If a real use case shows up — e.g. a multi-tenant sidecar shared by
many logically distinct clients where the backend needs to selectively
evict one tenant's streams — we can introduce an **optional**
`x-nats-nid` header that the local app stamps on each call, which the
sidecar passes through into `Call.Nid`. Defaults to the sidecar's nid
when absent. This is a non-breaking add: today's callers continue to
work without the header.

For v1, the sidecar uses one auto-generated nid and does not honor
`x-nats-nid`. See §13 for the open question.

### 4.6 Headers, trailers, status

The raw-codec proxy forwards gRPC headers and trailers as-is. The
nrpc-side `UnaryResponse` / `End` already carries status + metadata
(see `pkg/protos/nrpc/nrpc.proto`); the sidecar maps these back onto the
local app's stream via `SetHeader` / `SetTrailer` / returning a status
error. Existing logic in this repo handles all of it — the sidecar
inherits it for free.

---

## 5. Ingress (NATS → local app)

### 5.1 Registration via admin API

Because svcid is determined at app startup, the sidecar cannot pre-subscribe
to NATS subjects. The app tells the sidecar what to subscribe to via an
admin gRPC service:

```protobuf
service SidecarAdmin {
    // Register adds NATS subscriptions for the given (svcid, services).
    // The connection is the lease — when it closes, all subscriptions
    // registered through it are removed.
    rpc Register(stream RegisterRequest) returns (stream RegisterResponse);
}

message RegisterRequest {
    oneof type {
        Init init = 1;
        Heartbeat heartbeat = 2;
    }
}

message Init {
    string svcid = 1;               // dynamic, decided at app startup
    string upstream = 2;            // local gRPC server addr, e.g. "127.0.0.1:8080"
    repeated string services = 3;   // proto service names, e.g. ["pkg.Greeter"]
}

message Heartbeat {}

message RegisterResponse {
    oneof type {
        Registered registered = 1;
        Error error = 2;
    }
}

message Registered {
    string nid = 1;  // sidecar-assigned, unique per replica; surfaced
                     // so the app can advertise it elsewhere if useful
}

message Error { string message = 1; }
```

App-side flow:

```
1. App boots, decides svcid (from env, tenant claim, whatever).
2. App starts its gRPC server on :8080.
3. App opens a streaming RPC to the sidecar admin: Register({svcid, services, upstream}).
4. Sidecar opens NATS subscriptions, replies with Registered{nid}.
5. App keeps the stream alive (periodic Heartbeat or just hold-open).
6. When the app exits, the stream closes; sidecar tears down the
   subscriptions automatically.
```

`Register` is intentionally a bidi stream and not a unary call so the
sidecar can detect app death via the closed stream rather than relying on
liveness probes or unregister-on-shutdown — which would be missed on hard
crashes.

### 5.2 NATS subscription set

Per registration, the sidecar opens **two subscriptions per service**:

```
Unary:     nrpc.unary.<svcid>.<pkg.Service>.>      queue=u:<svcid>:<pkg.Service>
Streaming: nrpc.stream.<svcid>.<sidecar-nid>.<pkg.Service>.>   (no queue)
```

Where `<sidecar-nid>` is auto-generated per sidecar process (UUID or
`hostname-randomBytes`). The app doesn't see this and doesn't care.

### 5.3 Dispatch to local app

Both subscriptions feed a single dispatcher that wraps the framed
nrpc request in a normal gRPC call against the local app's port:

```go
upstreamConn, _ := grpc.NewClient(reg.upstream, grpc.WithInsecure())
```

For unary: `UnaryRequest{method, data, metadata}` → outbound
`upstreamConn.Invoke(ctx, method, in, out, callOpts...)`.

For streaming: open a `proxy.PumpBidi` between the inbound NATS-side
stream and an outbound `upstreamConn.NewStream(...)`.

Metadata on the wire (the `nrpc.Metadata` in `Call` / `UnaryRequest`) is
forwarded into the upstream gRPC call's outgoing context, minus the
`x-nats-*` family which is sidecar-only.

---

## 6. Sidecar admin: implementation shape

The admin service runs on a separate loopback port (default 50100) with
**no authentication** — same trust model as the egress loopback. It exposes
only `Register`; deregistration is implicit on stream close.

```go
type SidecarAdminServer struct {
    sc *Sidecar
}

func (s *SidecarAdminServer) Register(stream pb.SidecarAdmin_RegisterServer) error {
    req, err := stream.Recv()
    if err != nil { return err }
    init := req.GetInit()
    if init == nil { return status.Error(codes.InvalidArgument, "first message must be Init") }

    nid, teardown, err := s.sc.openIngress(init.Svcid, init.Upstream, init.Services)
    if err != nil { return err }
    defer teardown()

    if err := stream.Send(&pb.RegisterResponse{
        Type: &pb.RegisterResponse_Registered{Registered: &pb.Registered{Nid: nid}},
    }); err != nil { return err }

    // Hold the stream open; exit on app disconnect.
    for {
        if _, err := stream.Recv(); err != nil { return nil }
    }
}
```

### Re-registration / mid-flight changes

For v1 the registration is fixed for the lifetime of the admin stream —
to change svcid or service list, the app drops the stream and registers
again. This matches the stated requirement ("determined at runtime when
the application starts"). If we later need hot reconfiguration without
dropping the admin stream, add a `Reconfigure` variant to the stream's
`oneof`.

---

## 7. Config schema (bootstrap only)

Because routing is data, the static config is tiny — just connection
bootstrap. Suggested YAML mounted at `/etc/nats-grpc-sidecar/config.yaml`:

```yaml
nats:
  url: nats://nats.default.svc.cluster.local:4222
  # Optional auth / TLS — passes through to nats.Connect options.
  credentials_file: /etc/nats/creds
  tls:
    ca_file: /etc/nats/ca.crt

# Egress: gRPC server local apps dial.
egress:
  addr: 127.0.0.1:50051

# Admin: gRPC server local apps register ingress with.
admin:
  addr: 127.0.0.1:50100

# Sidecar identity (used as the streaming-subscription <nid>).
# Defaults to UUIDv4 generated at startup. Override for sticky routing
# (e.g. when the operator wants pod_name in subjects).
nid: ${POD_NAME}

# Heartbeat / observability tuning (optional).
heartbeat:
  ping_interval: 5s
  pong_timeout: 15s

observability:
  otlp_endpoint: http://otel-collector:4317
  metrics_port: 9100
```

That's the whole file. There is no `routes:` section by design.

---

## 8. Failure handling

| Failure | Behavior |
|---|---|
| Missing `x-nats-svcid` on egress call | `InvalidArgument` returned to local app; no NATS publish |
| `nats.ErrNoResponders` from publish | `Unavailable` returned to local app (existing nrpc mapping) |
| Context deadline expires | `DeadlineExceeded` returned (existing nrpc mapping) |
| Streaming heartbeat timeout | Stream closed with `Unavailable` (existing nrpc behavior; sidecar passes through) |
| Upstream gRPC dial fails (ingress) | Sidecar emits `Unavailable` over NATS back to caller; logs locally |
| Admin stream drops (app died) | Sidecar tears down NATS subscriptions for that registration; in-flight RPCs see context cancellation |
| Sidecar restarts | All ingress registrations lost (apps must re-Register); egress calls in flight fail with `Unavailable` |
| NATS connection lost | nats.go reconnects automatically; in-flight calls fail with `Unavailable`, new calls block until reconnect or fail with `Unavailable` if `MaxReconnects` exhausted |

The sidecar does **not** retry RPCs. Retry policy belongs in the caller's
gRPC service config (`google.rpc.RetryPolicy`) — adding retries at the
sidecar layer makes failure modes opaque and double-counts retries when
the caller also retries.

---

## 9. Interop with the direct nrpc API

Two configurations:

- **Sidecar-to-sidecar**: both endpoints use the new
  `nrpc.unary.*` / `nrpc.stream.*` namespacing. Clean separation,
  structural safety for streaming. This is the intended deployment.
- **Sidecar-to-legacy-direct-API**: the existing `pkg/rpc` Server uses
  `nrpc.<svcid>.<service>.*`. A sidecar talking to such a server would
  publish on the wrong subject.

For interop in mixed deployments, the sidecar accepts a per-route
**legacy mode** opt-in via a metadata header (`x-nats-legacy: true`)
that switches publish to the legacy `nrpc.<svcid>.<service>.<method>`
subject. Off by default — we don't want quiet wire-format ambiguity.

This is an escape hatch; the eventual end state is for the direct API
to migrate to the split namespace too, at which point `x-nats-legacy`
gets removed.

---

## 10. Required changes to `pkg/rpc`

The sidecar can't be built purely on the existing public API. Three
extensions are needed:

1. **Per-call svcid on `Client.Invoke` and `NewStream`.** Today svcid
   is constructor state (`NewClient(nc, svcid, nid)`). For sidecar
   egress we need:

   ```go
   func (c *Client) InvokeWithSvcID(ctx context.Context, svcid, method string, args, reply interface{}, opts ...grpc.CallOption) error
   func (c *Client) NewStreamWithSvcID(ctx context.Context, svcid, targetNid string, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error)
   ```

   Internally these build the subject from the per-call svcid using the
   new namespace.

2. **Subject builders are factored out.** The current code inlines
   subject construction in three places (`Client.invoker`,
   `Client.streamer`, `Server.RegisterService`). Pull them into a
   single helper that takes `(namespace, svcid, nid, fullMethod)` and
   returns subject + queue group. The sidecar uses the new namespace,
   the existing direct API continues to use the legacy namespace, both
   share one function.

3. **`Server` can run with explicit unary + streaming subscriptions.**
   The current `Server.RegisterService` opens one subscription per
   service. The sidecar's ingress dispatcher needs two (unary with
   queue group, streaming without). Either expose
   `Server.RegisterServiceWithLayout(layout SubjectLayout)` or build
   the ingress dispatcher as a peer to `Server` that uses the lower
   level NATS API directly. The latter is probably cleaner — the
   sidecar's ingress is a forwarder, not a service registry, so it
   doesn't really want the full `Server` lifecycle.

---

## 11. Observability

Both halves of the sidecar plug into `stats.Handler`, so dropping in
otelgrpc gives end-to-end spans:

```
local app gRPC client
  └─ span: gRPC.client (otelgrpc)
       └─ enters sidecar egress
            └─ span: nrpc.client (this repo via WithStatsHandler)
                 └─ NATS hop
                      └─ enters sidecar ingress
                           └─ span: nrpc.server
                                └─ exits ingress as upstream gRPC client
                                     └─ span: gRPC.client → local app
```

`traceparent` rides in metadata and is preserved end-to-end because the
sidecar forwards non-routing metadata transparently. There is no extra
work needed here — both gRPC and nrpc are already integrated.

Sidecar-specific metrics worth exposing on the metrics port:

- `sidecar_egress_calls_total{svcid, method, mode, code}`
- `sidecar_ingress_calls_total{svcid, service, method, code}`
- `sidecar_admin_registrations_active`
- `sidecar_admin_registrations_total{result}`
- `sidecar_nats_publish_errors_total{reason}`

---

## 12. Out of scope (v1)

Explicitly **not** in v1 to keep scope honest:

1. **Discovery of available svcids / nids.** The caller is responsible
   for knowing which svcid to set. A future v2 could subscribe to
   `_SYS.>` or a custom `_NRPC.SERVICES.>` heartbeat for runtime
   discovery, but the current ask doesn't need it.
2. **Authentication / authorization.** The sidecar trusts loopback and
   trusts NATS-level auth. Anything else (token validation, mTLS to
   the local app) is a future add.
3. **Multi-tenant isolation.** All registrations share one NATS
   connection. Could shard later.
4. **Streaming nid selection without explicit caller hint.** v1
   requires the caller to pass `x-nats-target-nid`. A future v2 could
   pick a replica via discovery or round-robin from a known pool.
5. **Hot reload of bootstrap config.** Sidecar restart is the supported
   path for changing NATS URL / TLS / etc.

---

## 13. Open questions

Things I'd want to settle with the user before writing code:

1. **Admin port discovery.** How does the local app find the admin port?
   Hardcoded default (`127.0.0.1:50100`) + environment override
   (`NATS_GRPC_SIDECAR_ADMIN_ADDR`) is simplest. Acceptable?
2. **What happens to in-flight egress RPCs when the admin stream from
   the same process drops?** Egress and admin are independent today —
   should they be? Argument for coupling: if the app died, why are we
   still serving its outbound calls? Argument against: a network blip
   on admin shouldn't cancel egress.
3. **Streaming replica selection.** Requiring `x-nats-target-nid` for
   every streaming call pushes the discovery problem onto the caller.
   Is that acceptable for v1, or do we need a "stream to any replica
   of svcid X" mode that uses a queue group on a separate
   "stream-claim" subject? (Possible but adds protocol complexity.)
4. **Metadata header naming convention.** I went with `x-nats-*` for
   sidecar-reserved headers. Some shops prefer `nats-grpc-*`. Bike-shed
   pick.
5. **Optional `x-nats-nid` for caller identity.** §4.5 covers why one
   sidecar nid is fine for response dispatch but coarse for
   server-initiated bulk-cleanup (`Server.CloseStream(nid)`). Worth
   adding the header in v1 even though no concrete use case has
   surfaced yet, so the contract is stable from day one — or defer
   until a real need shows up?

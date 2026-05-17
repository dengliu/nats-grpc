# Python sidecar example — registration

A Python script that registers itself with the nats-grpc sidecar via the
HTTP/JSON admin endpoint and holds the connection open as the lease.
Demonstrates that participating in the sidecar lifecycle from a non-Go
language needs **no nats-grpc dependency** and **no proto codegen** —
just `requests` and the standard library.

## What it shows

1. **Registration is plain HTTP.** No grpc-python, no generated stubs.
2. **The connection is the lease.** As long as the script holds the
   open response stream, the sidecar keeps its NATS subscriptions
   alive on this svcid's behalf. There is no application-level
   heartbeat — TCP-level connection close is the signal.
3. **Shutdown is automatic.** Ctrl-C closes the connection; the sidecar
   tears down the subscriptions immediately. No deregister API call
   needed.

## Prerequisites

- A running NATS server (`nats-server` on `:4222`).
- The nats-grpc sidecar running (`go run ./cmd/nats-grpc-sidecar` from
  the repo root). The default HTTP admin port is `127.0.0.1:50101`.
- Python ≥ 3.8 and the one dependency:
  ```sh
  cd examples/sidecar/python
  pip install -r requirements.txt
  ```

## Running

```sh
python register.py
```

Expected output (timestamps will differ):

```
[2026-05-16T17:30:00.000Z] dummy upstream listening on 127.0.0.1:54321
[2026-05-16T17:30:00.005Z] POST http://127.0.0.1:50101/v1/register body={'svcid': 'python-demo', 'upstream': '127.0.0.1:54321', 'services': ['echo.Echo']}
[2026-05-16T17:30:00.012Z] registered with sidecar nid = sc-abc123def456
[2026-05-16T17:30:00.013Z] holding connection (the registration lease) — Ctrl-C to deregister
```

The script then blocks. Press Ctrl-C — you'll see "shutting down",
the TCP connection drops, and on the sidecar's side the registration
disappears immediately.

### Flags

```
--admin URL    sidecar HTTP admin URL (default http://127.0.0.1:50101/v1/register)
--svcid NAME   svcid to register under (default python-demo)
--listen ADDR  placeholder TCP listen addr (default 127.0.0.1:0 — OS picks)
--services CSV comma-separated proto service names (default echo.Echo)
```

## What's NOT in this demo

This script does NOT serve real gRPC traffic. It listens on a TCP port
just so the sidecar has somewhere to dial, but the listener immediately
closes accepted connections — any client RPC that the sidecar tries to
forward will fail at the HTTP/2 layer. The point of the demo is the
**registration + lease lifecycle**, not RPC forwarding.

## Adding a real gRPC server

To turn this into a production-style integration, install grpc-python:

```sh
pip install grpcio grpcio-tools
```

Then generate Python stubs from the proto:

```sh
python -m grpc_tools.protoc \
  -I ../../protos \
  --python_out=. \
  --grpc_python_out=. \
  ../../protos/echo.proto
```

Replace the `start_dummy_listener` call with a real gRPC server using
the generated stubs:

```python
import grpc
from concurrent import futures
import echo_pb2_grpc, echo_pb2

class EchoServicer(echo_pb2_grpc.EchoServicer):
    def SayHello(self, request, context):
        return echo_pb2.HelloReply(msg=f"python: hello {request.msg}")

server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
echo_pb2_grpc.add_EchoServicer_to_server(EchoServicer(), server)
port = server.add_insecure_port("127.0.0.1:0")
server.start()
upstream = f"127.0.0.1:{port}"

# Then register exactly as register.py does, passing this `upstream`
# to the sidecar. Calls arriving on the egress side with
# `x-nats-svcid: python-demo` will now reach the SayHello handler.
```

The registration code stays identical — that's the whole point of the
HTTP admin contract.

## Why HTTP, not gRPC, for registration

When the sidecar exposed a gRPC `SidecarAdmin` service, registering
from Python required:

1. `pip install grpcio-tools`
2. `python -m grpc_tools.protoc … sidecar.proto`
3. Commit / vendor the generated stubs
4. Import `nats_grpc_sidecar_pb2_grpc` from somewhere

Friction the rest of the integration doesn't have. The HTTP/JSON
endpoint drops all of that — every language that can do an HTTP POST
and parse JSON can participate.

"""Python client for the docker-compose demo.

Dials the sidecar's egress port on 127.0.0.1:50051 (shared network
namespace via network_mode: service:sidecar-py-client) and sends one
SayHello every 3 seconds, alternating between py-server (odd #) and
go-server (even #) purely via the x-nats-svcid metadata header.

Wire format: requests are '<self> -> <target> #<N>', replies come
back as '<target> -> <self> #<N>'.
"""
from __future__ import annotations

import signal
import sys
import time

import grpc

import echo_pb2
import echo_pb2_grpc


SELF = "py-client"
# (svcid on the wire, short label used in the message body)
TARGETS = [
    ("python-server", "py-server"),
    ("go-server",     "go-server"),
]


def main() -> int:
    sys.stdout.reconfigure(line_buffering=True)

    # Graceful shutdown on SIGTERM (docker-compose down) so the
    # output isn't cut off mid-RPC.
    def shutdown(_signum, _frame):
        print("shutting down", flush=True)
        sys.exit(0)
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    channel = grpc.insecure_channel("127.0.0.1:50051")
    stub = echo_pb2_grpc.EchoStub(channel)

    n = 0
    while True:
        n += 1
        svcid, label = TARGETS[(n - 1) % len(TARGETS)]
        msg = f"{SELF} -> {label} #{n}"
        try:
            resp = stub.SayHello(
                echo_pb2.HelloRequest(msg=msg),
                metadata=[("x-nats-svcid", svcid)],
                timeout=3.0,
            )
            print(f"{msg}  ⇒  {resp.msg}", flush=True)
        except grpc.RpcError as e:
            print(f"{msg}  error: {e.code().name}: {e.details()}",
                  flush=True)
        time.sleep(3)


if __name__ == "__main__":
    sys.exit(main())

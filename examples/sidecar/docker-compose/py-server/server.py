"""Python echo backend for the docker-compose demo.

Listens on 127.0.0.1:8080 (shared loopback with the sidecar), registers
itself via the sidecar's HTTP/JSON admin, and replies to every
SayHello with the original message suffixed by " I am python server".
"""
from __future__ import annotations

import json
import os
import signal
import sys
import threading
from concurrent import futures

import grpc
import requests

import echo_pb2
import echo_pb2_grpc


class EchoServicer(echo_pb2_grpc.EchoServicer):
    def SayHello(self, request, context):
        reply = f"{request.msg} I am python server"
        print(f"SayHello in={request.msg!r}  out={reply!r}", flush=True)
        return echo_pb2.HelloReply(msg=reply)


def register_with_sidecar(admin_url: str, svcid: str, upstream: str) -> None:
    body = {"svcid": svcid, "upstream": upstream, "services": ["echo.Echo"]}
    # stream=True keeps the response body open as the registration
    # lease. resp.raw.read() (idiomatic `requests` "drain to EOF") at
    # the end blocks until the connection drops.
    resp = requests.post(admin_url, json=body, stream=True, timeout=None)
    resp.raise_for_status()
    first = next(resp.iter_lines(decode_unicode=True))
    msg = json.loads(first)
    print(f"registered as svcid={svcid!r}  sidecar.nid={msg['nid']}", flush=True)
    resp.raw.read()
    print("registration connection closed", flush=True)


def main() -> int:
    sys.stdout.reconfigure(line_buffering=True)
    svcid = os.getenv("SVCID", "python-server")
    listen = os.getenv("LISTEN", "127.0.0.1:8080")
    admin_url = os.getenv("ADMIN_URL", "http://127.0.0.1:50101/v1/register")

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8))
    echo_pb2_grpc.add_EchoServicer_to_server(EchoServicer(), server)
    server.add_insecure_port(listen)
    server.start()
    print(f"python echo server listening on {listen}", flush=True)

    # Shut the gRPC server down on signal so the sidecar's upstream
    # dial fails fast on subsequent calls.
    def shutdown(_signum, _frame):
        print("shutting down", flush=True)
        server.stop(grace=2)
        sys.exit(0)
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    # Block on the registration call — it returns only when the
    # sidecar drops the connection.
    threading.Thread(
        target=register_with_sidecar,
        args=(admin_url, svcid, listen),
        daemon=False,
    ).start()
    server.wait_for_termination()
    return 0


if __name__ == "__main__":
    sys.exit(main())

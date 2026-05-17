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
import time
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
    """Open a registration; on transient failures retry forever.

    The sidecar's HTTP admin may not be bound yet when this server
    starts (docker-compose's `depends_on: service_started` only waits
    for the *container* to start, not for the inner process to bind).
    Retry with a short backoff so the demo is robust against the
    startup race.
    """
    body = {"svcid": svcid, "upstream": upstream, "services": ["echo.Echo"]}
    backoff = 0.25
    while True:
        try:
            # stream=True keeps the response body open as the lease.
            resp = requests.post(admin_url, json=body, stream=True, timeout=None)
            resp.raise_for_status()
            # Use iter_lines as the SOLE reader on this stream — it
            # maintains an internal buffer over resp.raw, and mixing
            # it with resp.raw.read() leaves the buffer/raw split
            # state inconsistent (raw can return b'' immediately
            # while iter_lines's pending data hasn't been drained).
            lines = resp.iter_lines(decode_unicode=True)
            first = next(lines)
            msg = json.loads(first)
            print(f"registered as svcid={svcid!r}  sidecar.nid={msg['nid']}", flush=True)
            # Signal "I've registered" to docker-compose's healthcheck.
            # The client containers gate on this so they don't fire
            # their first RPC before the sidecar's NATS subscriptions
            # are live.
            open("/tmp/ready", "w").close()
            # No further lines are expected — the sidecar holds the
            # stream open without writing anything more. The for loop
            # blocks on the next chunk read, which only returns when
            # the connection drops (sidecar shutdown, network blip).
            for _ in lines:
                pass
            print("registration connection closed, re-registering...", flush=True)
        except (requests.exceptions.ConnectionError, requests.exceptions.ChunkedEncodingError) as e:
            print(f"register: connect error ({e!r}); retrying in {backoff:.1f}s", flush=True)
        except requests.exceptions.HTTPError as e:
            print(f"register: HTTP error {e!r}; retrying in {backoff:.1f}s", flush=True)
        except Exception as e:
            print(f"register: unexpected error {e!r}; retrying in {backoff:.1f}s", flush=True)
        time.sleep(backoff)
        backoff = min(backoff * 2, 5.0)


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

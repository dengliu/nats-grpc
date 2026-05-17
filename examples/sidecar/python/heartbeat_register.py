"""Heartbeat-focused registration demo for the nats-grpc sidecar.

Demonstrates the HTTP/JSON admin protocol from Python without any
nats-grpc imports or generated code — just `requests` and `json`.
Run this against a sidecar bound to 127.0.0.1:50101 (the default)
and you'll see:

    registered with sidecar nid = sc-...
    heartbeat ack at 2026-05-16T17:00:30.123Z
    heartbeat ack at 2026-05-16T17:01:00.456Z
    ...

Acks arrive every 30 seconds (the sidecar's default keepalive
interval). The open HTTP connection IS the registration lease: drop
it (Ctrl-C, kill the process, pull the network cable) and the
sidecar tears down the NATS subscriptions automatically.

This script does NOT include a real gRPC server — the "upstream"
address it gives to the sidecar points at a placeholder TCP
listener that just keeps the port reachable. To turn this into a
production-style integration, swap the dummy listener for an
actual `grpc.server(...)` and the calls coming through the sidecar
will land on your handlers. See the "Adding a real gRPC server"
section in the README.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import signal
import socket
import sys
import threading
from typing import Iterable

import requests


# ---------------------------------------------------------------------------
# A bare TCP listener that acts as a placeholder for the local gRPC server.
#
# The sidecar will dial this address when traffic arrives for our svcid.
# Without a real gRPC server on the other end the call will fail at the
# HTTP/2 layer — but registration + heartbeats still work, which is what
# this demo focuses on.
# ---------------------------------------------------------------------------

def start_dummy_listener(addr: str) -> tuple[socket.socket, str]:
    """Bind a TCP socket on `addr` and return (sock, "host:port")."""
    host, port_str = addr.rsplit(":", 1)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, int(port_str)))
    sock.listen(8)
    actual_host, actual_port = sock.getsockname()

    def accept_loop():
        # Accept and immediately drop — we never speak HTTP/2. The sidecar
        # will see EOF and report Unavailable to its egress caller. We
        # don't care; we're demonstrating heartbeats, not RPCs.
        while True:
            try:
                conn, _ = sock.accept()
            except OSError:
                return
            conn.close()

    threading.Thread(target=accept_loop, daemon=True).start()
    return sock, f"{actual_host}:{actual_port}"


# ---------------------------------------------------------------------------
# Registration + heartbeat loop.
# ---------------------------------------------------------------------------

def now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def register_and_hold(
    admin_url: str,
    svcid: str,
    upstream: str,
    services: Iterable[str],
) -> None:
    """POST a registration, then block reading the NDJSON ack stream.

    Returns only when the connection breaks (sidecar shutdown, network
    error, or local SIGINT).
    """
    body = {"svcid": svcid, "upstream": upstream, "services": list(services)}
    print(f"[{now_iso()}] POST {admin_url} body={body}")

    # stream=True so requests doesn't buffer the body — we want to read
    # each NDJSON line as the sidecar writes it. timeout=None so the
    # request doesn't get aborted while we hold the lease.
    resp = requests.post(admin_url, json=body, stream=True, timeout=None)
    resp.raise_for_status()

    lines = resp.iter_lines(decode_unicode=True)
    first = json.loads(next(lines))
    print(f"[{now_iso()}] registered with sidecar nid = {first['nid']}")

    # Hold the connection. Each line is a {"ack": <unix_nano>} message;
    # the sidecar sends one every 30 seconds by default. The loop ends
    # when iter_lines raises (connection closed) or the user hits ^C.
    for raw in lines:
        if not raw:
            continue
        msg = json.loads(raw)
        if "ack" in msg:
            print(f"[{now_iso()}] heartbeat ack ts={msg['ack']}")
        else:
            print(f"[{now_iso()}] unexpected message: {msg}")


# ---------------------------------------------------------------------------
# Entry point.
# ---------------------------------------------------------------------------

def main() -> int:
    # Force line-buffered stdout so heartbeat acks show up promptly
    # when this script is run under a parent that pipes its output
    # (Docker, tee, kubectl logs, etc.). Otherwise Python's default
    # block-buffering hides the live progress for minutes at a time.
    sys.stdout.reconfigure(line_buffering=True)

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--admin", default="http://127.0.0.1:50101/v1/register",
                    help="sidecar HTTP admin URL")
    ap.add_argument("--svcid", default="python-demo",
                    help="svcid to register under")
    ap.add_argument("--listen", default="127.0.0.1:0",
                    help="placeholder TCP listen addr (default: OS-picks)")
    ap.add_argument("--services", default="echo.Echo",
                    help="comma-separated proto service names")
    args = ap.parse_args()

    sock, upstream = start_dummy_listener(args.listen)
    print(f"[{now_iso()}] dummy upstream listening on {upstream}")

    # Cleanly close the listener on signal so the port is released.
    def shutdown(_signum, _frame):
        print(f"\n[{now_iso()}] shutting down")
        try:
            sock.close()
        except OSError:
            pass
        sys.exit(0)
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        register_and_hold(args.admin, args.svcid, upstream,
                          args.services.split(","))
    except requests.exceptions.RequestException as e:
        print(f"[{now_iso()}] registration error: {e}", file=sys.stderr)
        return 1
    except StopIteration:
        # Sidecar closed the stream before sending Registered{nid}.
        print(f"[{now_iso()}] sidecar closed the stream during registration",
              file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

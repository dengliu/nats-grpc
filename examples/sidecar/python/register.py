"""Registration demo for the nats-grpc sidecar, from Python.

Demonstrates the HTTP/JSON admin protocol with no nats-grpc imports
and no proto codegen — just `requests` and the standard library.

The script:

  1. Binds a placeholder TCP listener so the sidecar has a real
     `upstream` address to forward to.
  2. POSTs JSON to /v1/register and reads exactly one line of NDJSON
     containing the assigned sidecar nid.
  3. Holds the response stream open. **The open HTTP connection IS
     the registration lease.** When this process exits (Ctrl-C, kill,
     crash), the TCP connection drops and the sidecar deregisters
     the NATS subscriptions immediately.

The sidecar sends no further bytes after the Registered line, so the
script just blocks on the response body until the connection tears
down. There is no application-level heartbeat — TCP-level connection
close is the signal.
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
# The sidecar will dial this address when traffic arrives for our svcid.
# Without a real gRPC server on the other end calls would fail at the
# HTTP/2 layer — but registration + lease still work, which is what this
# demo focuses on.
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
        while True:
            try:
                conn, _ = sock.accept()
            except OSError:
                return
            conn.close()

    threading.Thread(target=accept_loop, daemon=True).start()
    return sock, f"{actual_host}:{actual_port}"


# ---------------------------------------------------------------------------
# Registration.
# ---------------------------------------------------------------------------

def now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def register_and_hold(
    admin_url: str,
    svcid: str,
    upstream: str,
    services: Iterable[str],
) -> None:
    """POST a registration, read the nid, then hold the connection open.

    Returns only when the connection drops (sidecar shutdown, network
    error, or local SIGINT closing the socket).
    """
    body = {"svcid": svcid, "upstream": upstream, "services": list(services)}
    print(f"[{now_iso()}] POST {admin_url} body={body}")

    # stream=True so requests doesn't buffer the body — we want to read
    # the initial NDJSON line as soon as the sidecar writes it.
    # timeout=None so the connection isn't aborted while we hold the
    # lease.
    resp = requests.post(admin_url, json=body, stream=True, timeout=None)
    resp.raise_for_status()

    # The sidecar writes exactly one line ({"nid":"..."}) and then
    # nothing — the open connection IS the lease.
    first = next(resp.iter_lines(decode_unicode=True))
    msg = json.loads(first)
    print(f"[{now_iso()}] registered with sidecar nid = {msg['nid']}")
    print(f"[{now_iso()}] holding connection (the registration lease) — Ctrl-C to deregister")

    # Block reading the body until the connection drops. resp.raw.read
    # returns b'' on EOF; we discard whatever bytes might arrive
    # (currently none) and exit when the stream ends.
    while True:
        chunk = resp.raw.read(4096)
        if not chunk:
            break
    print(f"[{now_iso()}] registration connection closed")


# ---------------------------------------------------------------------------
# Entry point.
# ---------------------------------------------------------------------------

def main() -> int:
    # Force line-buffered stdout so log lines show up promptly under a
    # parent that pipes their output (Docker, tee, kubectl logs, etc.).
    sys.stdout.reconfigure(line_buffering=True)

    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--admin", default="http://127.0.0.1:50101/v1/register",
                    help="sidecar HTTP admin URL")
    ap.add_argument("--svcid", default="python-demo",
                    help="svcid to register under")
    ap.add_argument("--listen", default="127.0.0.1:0",
                    help="placeholder TCP listen addr (default: OS picks)")
    ap.add_argument("--services", default="echo.Echo",
                    help="comma-separated proto service names")
    args = ap.parse_args()

    sock, upstream = start_dummy_listener(args.listen)
    print(f"[{now_iso()}] dummy upstream listening on {upstream}")

    # Close the listener on signal so the port is released.
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
        print(f"[{now_iso()}] sidecar closed the stream during registration",
              file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

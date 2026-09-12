#!/usr/bin/env python3
"""End-to-end smoke test against a fake NUT upstream.

Deliberately does NOT touch a real UPS: the whole point of this proxy is that the real
device has a scarce connection budget, so the test must not spend any of it.

Covers:
  1. LIST VAR is served from cache and matches upstream
  2. GET VAR for a known and an unknown variable
  3. the upstream is polled ONCE per interval, no matter how many clients connect
  4. a stuck CAL flag is stripped after CAL_MAX_SEC, and the status stays meaningful
  5. when the upstream dies, connections are RESET rather than served stale data

Run:  python test_proxy.py
"""
import os
import socket
import subprocess
import sys
import threading
import time

UPSTREAM_PORT = 33493
PROXY_PORT = 33494

status_to_serve = "OL"
upstream_connections = 0
upstream_running = True
_lock = threading.Lock()


def fake_upstream():
    """A minimal NUT server: answers LIST VAR and counts connections."""
    global upstream_connections
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", UPSTREAM_PORT))
    srv.listen(5)
    srv.settimeout(0.5)
    while upstream_running:
        try:
            conn, _ = srv.accept()
        except socket.timeout:
            continue
        with _lock:
            upstream_connections += 1
        try:
            conn.settimeout(2)
            data = conn.recv(4096).decode(errors="replace")
            if data.startswith("LIST VAR"):
                body = (
                    "BEGIN LIST VAR ups\n"
                    f'VAR ups ups.status "{status_to_serve}"\n'
                    'VAR ups battery.charge "100"\n'
                    'VAR ups input.voltage "230.1"\n'
                    "END LIST VAR ups\n"
                )
                conn.sendall(body.encode())
        except Exception:
            pass
        finally:
            conn.close()
    srv.close()


def ask(port, *commands, timeout=3):
    """Send commands to a NUT server and return the raw reply."""
    s = socket.create_connection(("127.0.0.1", port), timeout=timeout)
    s.settimeout(timeout)
    out = b""
    try:
        for c in commands:
            s.sendall((c + "\n").encode())
            time.sleep(0.15)
            try:
                out += s.recv(8192)
            except socket.timeout:
                pass
    finally:
        s.close()
    return out.decode(errors="replace")


def check(name, condition, detail=""):
    print(f"  [{'PASS' if condition else 'FAIL'}] {name}"
          + (f"  -- {detail}" if detail else ""))
    return bool(condition)


def main():
    global status_to_serve, upstream_running

    threading.Thread(target=fake_upstream, daemon=True).start()
    time.sleep(0.3)

    env = dict(os.environ)
    env.update({
        "UPSTREAM_HOST": "127.0.0.1",
        "UPSTREAM_PORT": str(UPSTREAM_PORT),
        "LISTEN_HOST": "127.0.0.1",
        "LISTEN_PORT": str(PROXY_PORT),
        "POLL_INTERVAL": "1",
        "STALE_SEC": "3",
        "CAL_MAX_SEC": "4",
        "EVENT_ALERTS": "false",
    })
    proc = subprocess.Popen([sys.executable, "-u", "nut_proxy.py"], env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True)
    time.sleep(2.5)

    ok = True
    print("\n1) LIST VAR served from cache")
    r = ask(PROXY_PORT, "LIST VAR ups")
    ok &= check("contains ups.status OL", 'ups.status "OL"' in r)
    ok &= check("contains battery.charge", 'battery.charge "100"' in r)
    ok &= check("list properly terminated", "END LIST VAR ups" in r)

    print("\n2) GET VAR")
    r = ask(PROXY_PORT, "GET VAR ups input.voltage")
    ok &= check("known variable", "230.1" in r, r.strip())
    r = ask(PROXY_PORT, "GET VAR ups does.not.exist")
    ok &= check("unknown variable -> ERR", "VAR-NOT-SUPPORTED" in r, r.strip())

    print("\n3) Connection budget: many clients, one upstream")
    with _lock:
        before = upstream_connections
    for _ in range(12):
        ask(PROXY_PORT, "LIST VAR ups")
    with _lock:
        after = upstream_connections
    grew = after - before
    ok &= check("12 client queries add no per-query upstream load",
                grew <= 3, f"{grew} upstream connections (the polls only)")

    print("\n4) Stuck CAL flag is defused")
    status_to_serve = "OL CAL"
    time.sleep(2)
    r = ask(PROXY_PORT, "LIST VAR ups")
    ok &= check("fresh CAL is passed through", "OL CAL" in r, r.split("\n")[1])
    time.sleep(4)
    r = ask(PROXY_PORT, "LIST VAR ups")
    ok &= check("standing CAL is stripped", "CAL" not in r)
    ok &= check("status stays meaningful", 'ups.status "OL"' in r)
    status_to_serve = "OL"
    time.sleep(2)

    print("\n5) Upstream dead -> connection reset, no stale data")
    upstream_running = False
    time.sleep(5)
    try:
        r = ask(PROXY_PORT, "LIST VAR ups")
        ok &= check("no stale data served", "battery.charge" not in r, repr(r[:60]))
    except (ConnectionResetError, ConnectionAbortedError, OSError) as e:
        ok &= check("connection hard-reset", True, type(e).__name__)

    proc.terminate()
    try:
        output = proc.communicate(timeout=5)[0]
    except subprocess.TimeoutExpired:
        proc.kill()
        output = ""

    print("\n--- proxy log ---")
    for line in output.strip().split("\n"):
        print("   ", line)

    print("\n" + ("ALL TESTS PASSED" if ok else "THERE WERE FAILURES"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

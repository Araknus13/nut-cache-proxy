#!/usr/bin/env python3
"""End-to-end smoke test against a fake NUT upstream.

Deliberately does NOT touch a real UPS: the whole point of this proxy is that the real
device has a scarce connection budget, so the test must not spend any of it.

Covers:
  1. LIST VAR is served from cache and matches upstream
  2. GET VAR for a known and an unknown variable
  3. the upstream is polled ONCE per interval, no matter how many clients connect
  4. a stuck CAL flag is stripped after CAL_MAX_SEC, and the status stays meaningful
  5. the proxy logs in upstream (USERNAME/PASSWORD) before LIST VAR, without LOGIN
  6. an upstream ERR is named verbatim (not "timed out"), and the fault gets a start
     and an end in the log file -- while per-client reset lines stay out of the file
  7. when the upstream dies, connections are RESET rather than served stale data
  8. fault events reach a (fake) InfluxDB with "Authorization: Token ...", and the token
     shows up in no log

Run:  python test_proxy.py
"""
import atexit
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

UPSTREAM_PORT = 33493
PROXY_PORT = 33494
INFLUX_PORT = 33495
USER, PASSWORD = "monuser", "test-only-password"
INFLUX_TOKEN = "test-only-token"

status_to_serve = "OL"
deny_all = False              # answer every LIST VAR with ERR ACCESS-DENIED
upstream_connections = 0
upstream_commands = []        # (connection number, command verb) as seen by the fake UPS
upstream_running = True
influx_writes = []            # (path, Authorization header, body) as seen by the fake InfluxDB
_lock = threading.Lock()


class FakeInflux(BaseHTTPRequestHandler):
    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0))).decode()
        with _lock:
            influx_writes.append((self.path, self.headers.get("Authorization"), body))
        self.send_response(204)
        self.end_headers()

    def log_message(self, *args):
        pass


def handle_upstream(conn, number):
    """One NUT session, line by line, like the real server."""
    logged_in = user_ok = False
    f = conn.makefile("rb")
    while True:
        raw = f.readline()
        if not raw:
            return
        line = raw.decode(errors="replace").strip()
        verb = line.split(" ", 1)[0].upper()
        with _lock:
            upstream_commands.append((number, verb))
        if verb == "USERNAME":
            user_ok = line == f"USERNAME {USER}"
            conn.sendall(b"OK\n")
        elif verb == "PASSWORD":
            logged_in = user_ok and line == f"PASSWORD {PASSWORD}"
            conn.sendall(b"OK\n" if logged_in else b"ERR ACCESS-DENIED\n")
        elif line.startswith("LIST VAR"):
            if deny_all or not logged_in:
                conn.sendall(b"ERR ACCESS-DENIED\n")
                continue
            conn.sendall((
                "BEGIN LIST VAR ups\n"
                f'VAR ups ups.status "{status_to_serve}"\n'
                'VAR ups battery.charge "100"\n'
                'VAR ups input.voltage "230.1"\n'
                "END LIST VAR ups\n").encode())
        elif verb == "LOGOUT":
            conn.sendall(b"OK Goodbye\n")
            return
        else:
            conn.sendall(b"ERR UNKNOWN-COMMAND\n")


def fake_upstream():
    """A minimal NUT server that requires a login and counts connections."""
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
            number = upstream_connections
        try:
            conn.settimeout(2)
            handle_upstream(conn, number)
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


def read_file(path):
    try:
        with open(path, encoding="utf-8") as fh:
            return fh.read()
    except OSError:
        return ""


def main():
    global status_to_serve, upstream_running, deny_all

    threading.Thread(target=fake_upstream, daemon=True).start()
    influx = ThreadingHTTPServer(("127.0.0.1", INFLUX_PORT), FakeInflux)
    threading.Thread(target=influx.serve_forever, daemon=True).start()
    time.sleep(0.3)

    log_file = os.path.join(tempfile.mkdtemp(prefix="nut-proxy-test-"), "proxy.log")
    env = dict(os.environ)
    env.update({
        "UPSTREAM_HOST": "127.0.0.1",
        "UPSTREAM_PORT": str(UPSTREAM_PORT),
        "UPSTREAM_USER": USER,
        "UPSTREAM_PASS": PASSWORD,
        "LISTEN_HOST": "127.0.0.1",
        "LISTEN_PORT": str(PROXY_PORT),
        "POLL_INTERVAL": "1",
        "STALE_SEC": "3",
        "CAL_MAX_SEC": "4",
        "EVENT_ALERTS": "false",
        "LOG_FILE": log_file,
        # Short, so the 5 s fault in test 6 is long enough to notify -- that is what
        # produces the InfluxDB writes test 8 looks at.
        "FAULT_ALERT_SEC": "2",
        "INFLUX_URL": f"http://127.0.0.1:{INFLUX_PORT}",
        "INFLUX_TOKEN": INFLUX_TOKEN,
    })
    proc = subprocess.Popen([sys.executable, "-u", "nut_proxy.py"], env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True)
    # Never leave the proxy behind if a check crashes -- a leftover process keeps the
    # port and makes the next run fail for a reason that has nothing to do with the code.
    atexit.register(lambda: proc.poll() is None and proc.kill())
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
    ok &= check("fresh CAL is passed through", "OL CAL" in r, (r.split("\n") + ["", ""])[1])
    time.sleep(4)
    r = ask(PROXY_PORT, "LIST VAR ups")
    ok &= check("standing CAL is stripped", "CAL" not in r)
    ok &= check("status stays meaningful", 'ups.status "OL"' in r)
    status_to_serve = "OL"
    time.sleep(2)

    print("\n5) Upstream login")
    with _lock:
        sessions = {}
        for number, verb in upstream_commands:
            sessions.setdefault(number, []).append(verb)
    polls = [v for v in sessions.values() if "LIST" in v]
    ok &= check("every poll logs in before LIST VAR",
                polls and all(v[:3] == ["USERNAME", "PASSWORD", "LIST"] for v in polls),
                f"{len(polls)} polls, e.g. {polls[0] if polls else '-'}")
    ok &= check("no LOGIN <ups> upstream",
                not any("LOGIN" in v for v in sessions.values()))

    print("\n6) Upstream ERR is named, fault gets start and end")
    deny_all = True
    time.sleep(5)                       # past STALE_SEC: clients get reset now
    try:
        ask(PROXY_PORT, "LIST VAR ups")
    except OSError:
        pass
    deny_all = False
    time.sleep(2.5)
    r = ask(PROXY_PORT, "LIST VAR ups")
    ok &= check("serving again after the fault", 'ups.status "OL"' in r)
    text = read_file(log_file)
    ok &= check("ERR named verbatim in the fault start",
                "FAULT BEGINS: LIST VAR: ERR ACCESS-DENIED" in text)
    # Both halves, or an empty log file would pass this vacuously.
    ok &= check("no misleading 'timed out'",
                "ACCESS-DENIED" in text and "timed out" not in text.lower())
    ok &= check("fault end with duration in the log file", "FAULT ENDS after" in text)
    ok &= check("per-client reset lines stay out of the file",
                "connection reset immediately" not in text)

    print("\n7) Upstream dead -> connection reset, no stale data")
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

    print("\n8) Fault events reach InfluxDB with the token")
    with _lock:
        writes = list(influx_writes)
    kinds = [b.split(",kind=")[1].split(" ")[0] for _, _, b in writes if ",kind=" in b]
    ok &= check("fault start and end written",
                "upstream_fault" in kinds and "upstream_back" in kinds, f"kinds: {kinds}")
    ok &= check("every write carries 'Authorization: Token <token>'",
                writes and all(a == f"Token {INFLUX_TOKEN}" for _, a, _ in writes),
                f"{len(writes)} writes, e.g. {writes[0][1] if writes else '-'}")
    ok &= check("v1 /write endpoint with the database",
                writes and all(p == "/write?db=hardware" for p, _, _ in writes))
    # The token must never end up where people paste logs from.
    ok &= check("token appears in neither stdout nor the log file",
                output and INFLUX_TOKEN not in output and INFLUX_TOKEN not in read_file(log_file))
    influx.shutdown()

    print("\n--- proxy log ---")
    for line in output.strip().split("\n"):
        print("   ", line)

    print("\n" + ("ALL TESTS PASSED" if ok else "THERE WERE FAILURES"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())

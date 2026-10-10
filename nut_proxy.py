#!/usr/bin/env python3
"""nut-cache-proxy -- one real connection to the UPS, every client served from cache.

WHY THIS EXISTS
Network-attached UPSes often run a NUT server on the device itself, and that server has a
very small connection budget. In a typical homelab several `upsmon` instances (one per
host) hold persistent connections, and every additional consumer -- a metrics poller, a
shutdown watchdog, a NAS querying "device information" -- competes for the last free
slots and gets reset every so often.

Measured on the setup this was written for, before the proxy existed:

  * 11 % connection-reset rate on a 30-second metrics poller
  * 34 of 39,259 sampled points with no valid reading
  * short "Communications lost" dropouts even on the PERSISTENT upsmon connections

No single culprit -- just competition for a scarce resource.

HOW IT SOLVES THAT
One service becomes the only real peer of the UPS. This proxy holds EXACTLY ONE
connection, polls it on a fixed interval, and answers every downstream request
(`LIST VAR`, `GET VAR`) from cache. None of those create an additional connection to the
UPS. `LOGIN`/`USERNAME`/`PASSWORD`/`LOGOUT` are acknowledged locally without being
forwarded -- they only gate readable variables, and this is meant to run on a trusted
internal segment.

WHAT IS DELIBERATELY NOT IMPLEMENTED
  * No multiplexing of individual NUT sessions onto the upstream. That would be risky,
    because the UPS keeps login/primary state per connection. This does read-through
    caching instead: one view of the variables, arbitrarily many downstream readers.
  * FSD (forced shutdown) is NOT simulated locally. If a client sends `FSD <ups>` it is
    forwarded to the real UPS as a genuine one-off write. An FSD flag set by the UPS
    itself shows up in `ups.status` and passes through the normal cache anyway.
  * INSTCMD (active commands such as a test alarm) is not supported.

EVENTS INSTEAD OF SAMPLING
A metrics poller on a 30-second interval will miss a short outage entirely. That happened
here: a real mains dropout fell between two samples, and both neighbours read `OL`, mains
voltage nominal, `on_battery=0`. The series was completely gap-free -- exactly 120 points
per hour -- and showed "all fine" throughout.

A gap-free series is not evidence that nothing happened. For events shorter than the
sampling interval it will stubbornly report normality.

So this proxy detects status transitions itself. It polls the UPS every few seconds
anyway, so this costs NO additional connection -- which is the whole point, since the
scarce connection budget is the reason the proxy exists. Optional notifications go out
over ntfy, and event points to InfluxDB.

DO NOT REPORT A SELF-TEST AS A POWER FAILURE
Many UPSes run a periodic self-test: a few seconds of `OB CAL`, then back to `OL`. Without
a distinction, every self-test fires a top-priority "POWER FAILURE" alert. Here three
independent alerting paths did exactly that on the same ten-second self-test.

The distinction used: if `CAL` appears in the SAME transition as `OB` -- neither was set
in the previous poll -- it is a self-test. If `CAL` was already set BEFOREHAND (a stuck
flag, see below), a newly appearing `OB` still counts as a real alarm. An outage during a
stuck CAL flag must never be waved through as harmless.

⛔ THE STUCK CAL FLAG -- THE MOST IMPORTANT PART OF THIS FILE
`upsmon` has a special path for "last known to be calibrating and currently not
communicating". It does not wait for DEADTIME. It shuts the host down IMMEDIATELY:

    Poll UPS failed - Write error: Broken pipe
    UPS was last known to be calibrating and currently is not communicating, assuming dead
    Executing automatic power-fail shutdown

On the setup this was written for, a TWO-SECOND restart of this very container shut the
host down four seconds later. The CAL flag was six days old -- left over from a genuine
ten-second calibration that upsmon had never cleared. The same mechanism had already cost
6 h 43 min of downtime a week earlier. Raising DEADTIME does NOT help, because this path
never waits for it.

The proxy is the single point every client hangs off, so the trap is defused here: a CAL
flag is passed through unchanged while it is fresh (real self-tests last seconds), and
removed from the delivered `ups.status` once it has been standing for longer than
CAL_MAX_SEC. The RAW status is kept internally and reported.

⛔ If you run this in front of `upsmon`, understand this section before you restart the
container. A restart of the proxy while a stuck CAL flag is being delivered can shut your
machines down.

LOGGING IN UPSTREAM, AND NAMING THE ACTUAL ERROR
After rebooting itself, the UPS this was written for refused EVERY anonymous query with
`ERR ACCESS-DENIED` -- for 9 h 19 min, and across a firmware change. A single login with
a monitoring user fixed it: 28 variables immediately, and afterwards even anonymous
queries worked again. So the proxy can now log in before every `LIST VAR` / `FSD`, using
USERNAME/PASSWORD from UPSTREAM_USER/UPSTREAM_PASS (empty = anonymous, as before). It does
NOT send `LOGIN <ups>` -- that registers a monitoring client inside the UPS and was not
needed for reading.

The second finding of that day: for nine hours the log said "timed out", while the UPS
answered within 0.1 s with `ERR ACCESS-DENIED`. The reader waited for `END LIST`, which
never came. The error message hid the cause. Now every ERR line is carried verbatim as
the error kind, and every fault gets a start, any change of error kind, and an end with
its duration -- optionally into a log file (LOG_FILE; `docker logs` does not survive a
container recreate), and, once it lasts longer than FAULT_ALERT_SEC, to ntfy/InfluxDB
WITH the cause.

BEHAVIOUR ON A REAL OUTAGE -- deliberately like a dropped connection, not like empty data
If the cache has gone longer than STALE_SEC without a successful poll, the proxy still
accepts connections but closes them IMMEDIATELY with a hard reset (SO_LINGER), before
reading any request. To a client that looks like a connection error against the real UPS
(`ConnectionResetError` / `BrokenPipeError`), which every NUT client already handles.

An empty or stale answer would be far more dangerous: it would look like a successful but
contentless poll and pass silently as "UPS online, everything zero".

That is the general rule this file is built on: when you cannot answer, fail loudly.
A plausible-looking wrong answer is worse than a visible error.
"""
import asyncio
import json
import logging
import logging.handlers
import os
import socket
import struct
import time
import urllib.request

# --- Upstream: the real UPS -------------------------------------------------------
UPSTREAM_HOST = os.environ.get("UPSTREAM_HOST", "")
UPSTREAM_PORT = int(os.environ.get("UPSTREAM_PORT", "3493"))
UPS_NAME = os.environ.get("UPS_NAME", "ups")
UPS_DESCRIPTION = os.environ.get("UPS_DESCRIPTION", "UPS (via nut-cache-proxy)")
# Optional login towards the UPS (USERNAME/PASSWORD before each query). Empty = anonymous.
# Take these from the environment or an env file -- never write them into this file.
UPSTREAM_USER = os.environ.get("UPSTREAM_USER", "")
UPSTREAM_PASS = os.environ.get("UPSTREAM_PASS", "")

# --- Downstream: what the clients connect to --------------------------------------
LISTEN_HOST = os.environ.get("LISTEN_HOST", "0.0.0.0")
LISTEN_PORT = int(os.environ.get("LISTEN_PORT", "3493"))

# Keep this BELOW your clients' POLLFREQ so the cache is never the limiting factor for
# the freshness they see.
POLL_INTERVAL = float(os.environ.get("POLL_INTERVAL", "3"))

# More generous than POLL_INTERVAL, but well below upsmon's DEADTIME: the proxy should
# surface a real outage long before upsmon loses patience on its own.
STALE_SEC = float(os.environ.get("STALE_SEC", "20"))

# --- Stuck-CAL protection (see module docstring) ----------------------------------
# How long a calibration flag may stand before it is removed from the `ups.status`
# delivered to clients. Observed real calibrations lasted ~10 seconds, so 120 s is far
# beyond normal while still never interfering with a running self-test.
CAL_MAX_SEC = float(os.environ.get("CAL_MAX_SEC", "120"))

# --- Optional: event notifications ------------------------------------------------
# Off by default. The proxy works as a pure cache without any of this.
EVENT_ALERTS = os.environ.get("EVENT_ALERTS", "false").lower() == "true"
NTFY_URL = os.environ.get("NTFY_URL", "")
NTFY_TOPIC = os.environ.get("NTFY_TOPIC", "ups")
INFLUX_URL = os.environ.get("INFLUX_URL", "")
INFLUX_DB = os.environ.get("INFLUX_DB", "hardware")
# Optional API token, sent as "Authorization: Token <value>" -- what InfluxDB 3 expects on its
# v1-compatible /write endpoint. Leave empty for an InfluxDB 1.x without authentication.
INFLUX_TOKEN = os.environ.get("INFLUX_TOKEN", "")
EVENT_MEASUREMENT = os.environ.get("EVENT_MEASUREMENT", "ups_event")
EVENT_HOST_TAG = os.environ.get("EVENT_HOST_TAG", "ups")

# --- Fault log --------------------------------------------------------------------
# Optional log file, ideally on a bind mount: `docker logs` dies with every recreate, and
# then the one incident you need to analyse is gone. Empty = stdout only.
LOG_FILE = os.environ.get("LOG_FILE", "")
# How long upstream must be failing before a fault notification goes out. Short dropouts
# that recover within seconds are normal and should not ring; the log gets them anyway.
# Unlike status events this does NOT depend on EVENT_ALERTS: a blind proxy means no
# client behind it can shut down cleanly, which is a safety issue, not a statistic.
# It still only notifies if NTFY_URL / INFLUX_URL are set.
FAULT_ALERT_SEC = float(os.environ.get("FAULT_ALERT_SEC", "60"))

VERSION = "1.2"

_file_log = None
if LOG_FILE:
    try:
        _file_log = logging.getLogger("nut-cache-proxy-file")
        _h = logging.handlers.RotatingFileHandler(
            LOG_FILE, maxBytes=2_000_000, backupCount=5, encoding="utf-8")
        _h.setFormatter(logging.Formatter("%(asctime)s %(message)s", "%Y-%m-%d %H:%M:%S"))
        _file_log.addHandler(_h)
        _file_log.setLevel(logging.INFO)
        _file_log.propagate = False
    except Exception as _e:
        print(f"Log file {LOG_FILE} not usable: {_e}", flush=True)
        _file_log = None


def log(msg, to_file=True):
    """stdout always; the file only gets what the next incident analysis needs -- the
    line per rejected client (can be dozens per minute) stays out."""
    print(time.strftime("%Y-%m-%d %H:%M:%S"), msg, flush=True)
    if to_file and _file_log is not None:
        try:
            _file_log.info(msg)
        except Exception:
            pass


class UpstreamErr(Exception):
    """The UPS answered with a NUT error line (e.g. ERR ACCESS-DENIED)."""


def ntfy(title, message, priority=3, tags=None):
    """POST a JSON body to the ntfy ROOT url.

    Note: the topic goes IN the body. Appending it to the URL makes ntfy ignore the
    JSON body entirely, and you get a notification with no title and no priority.
    """
    if not NTFY_URL:
        return
    try:
        payload = json.dumps({
            "topic": NTFY_TOPIC,
            "title": title,
            "message": message,
            "priority": int(priority),
            "tags": tags or ["electric_plug"],
        }, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(
            NTFY_URL,
            data=payload,
            headers={"Content-Type": "application/json; charset=utf-8"},
            method="POST",
        )
        urllib.request.urlopen(req, timeout=10).read()
    except Exception as e:
        log(f"ntfy error: {e}")


def influx_write_event(kind, fields):
    """Write an event point to InfluxDB.

    Deliberately a SEPARATE measurement from any telemetry series: the telemetry stays an
    evenly sampled series, while events sit beside it timestamped to the second.
    """
    if not INFLUX_URL:
        return
    try:
        parts = []
        for k, v in fields.items():
            if isinstance(v, float):
                parts.append(f"{k}={v}")
            elif isinstance(v, str):
                esc = v.replace("\\", "\\\\").replace('"', '\\"')
                parts.append(f'{k}="{esc}"')
            else:
                parts.append(f"{k}={v}i")
        line = (f"{EVENT_MEASUREMENT},host={EVENT_HOST_TAG},kind={kind} "
                + ",".join(parts))
        url = f"{INFLUX_URL.rstrip('/')}/write?db={INFLUX_DB}"
        headers = {"Authorization": f"Token {INFLUX_TOKEN}"} if INFLUX_TOKEN else {}
        req = urllib.request.Request(url, data=line.encode(), method="POST", headers=headers)
        urllib.request.urlopen(req, timeout=10).read()
    except Exception as e:
        log(f"InfluxDB error on event '{kind}': {e}")


# Shared state between the poll task and the client handlers.
cache = {"vars": {}, "raw": {}, "last_success": 0.0, "polls": 0, "poll_errors": 0}


def _read_line(f, what):
    line = f.readline()
    if not line:
        raise ConnectionError(f"UPS closed the connection during {what}")
    return line.decode(errors="replace").rstrip("\r\n")


def _login(s, f):
    """USERNAME/PASSWORD, if configured. A rejected login is its own, named error --
    never skipped silently, or you end up searching again."""
    if not UPSTREAM_USER:
        return
    for command, what in ((f"USERNAME {UPSTREAM_USER}", "USERNAME"),
                          (f"PASSWORD {UPSTREAM_PASS}", "PASSWORD")):
        s.sendall((command + "\n").encode())
        reply = _read_line(f, what)
        if not reply.startswith("OK"):
            raise UpstreamErr(f"{what}: {reply}")


def upstream_list_var():
    """One `LIST VAR <ups>` against the REAL UPS.

    Reads line by line and aborts on an ERR line IMMEDIATELY, with its wording. Reading
    until `END LIST` instead turns `ERR ACCESS-DENIED` into a misleading "timed out".
    """
    with socket.create_connection((UPSTREAM_HOST, UPSTREAM_PORT), timeout=5) as s:
        f = s.makefile("rb")
        _login(s, f)
        s.sendall(f"LIST VAR {UPS_NAME}\n".encode())
        out = {}
        while True:
            line = _read_line(f, "LIST VAR")
            if line.startswith("ERR"):
                raise UpstreamErr(f"LIST VAR: {line}")
            if line.startswith("END LIST"):
                break
            if line.startswith("VAR "):
                parts = line.split(" ", 3)
                if len(parts) == 4:
                    out[parts[2]] = parts[3].strip().strip('"')
        s.sendall(b"LOGOUT\n")
    if not out:
        # An empty result is an ERROR, not an empty reading. Raising here keeps the
        # previous cache and lets STALE_SEC do its job, instead of serving nothing.
        raise ValueError("LIST VAR returned no variables -- empty or broken response")
    return out


def error_kind(e):
    """Short, stable name for the kind of error -- the basis for "error kind changed"
    and for the summary at the end of a fault."""
    if isinstance(e, UpstreamErr):
        return str(e)
    if isinstance(e, (socket.timeout, TimeoutError)):
        return "timeout (UPS not answering)"
    if isinstance(e, ConnectionRefusedError):
        return f"connection refused (port {UPSTREAM_PORT} closed)"
    if isinstance(e, ConnectionResetError):
        return "connection reset"
    if isinstance(e, OSError) and e.errno in (113, 101, 110):
        return f"UPS unreachable ({e.strerror})"
    return f"{type(e).__name__}: {e}"


# Event-detection state -- deliberately separate from the cache: the cache may go stale
# during a fault, but the edge detector should keep its last known state.
events = {"flags": None, "ob_since": 0.0, "cal_since": 0.0, "cal_filtered": False,
          "ob_is_selftest": False}


def filter_stuck_cal(v):
    """Remove a STUCK calibration flag from the delivered `ups.status`.

    See the module docstring for why this exists -- it is the difference between a
    harmless proxy restart and an unplanned shutdown of every machine behind it.

    While the flag is fresh it passes through untouched: a real self-test should stay
    visible. Only once it has stood for longer than CAL_MAX_SEC does it stop being
    information and become a trap, and then it is taken out.

    Returns (variables, messages). Delivery happens in the background so the poll
    interval never waits on it.
    """
    status = v.get("ups.status", "")
    flags = status.split()
    now = time.time()

    if "CAL" not in flags:
        if events["cal_since"]:
            duration = now - events["cal_since"]
            log(f"CAL flag cleared after {duration:.0f}s (status: {status})")
            if events["cal_filtered"]:
                events["cal_since"] = 0.0
                events["cal_filtered"] = False
                return v, [(
                    "cal_cleared", "UPS calibration flag clean again",
                    f"The stuck CAL flag cleared after {duration / 60:.0f} min.\n"
                    f"Status now: {status}.",
                    3, ["white_check_mark"],
                    {"status_text": status or "?", "cal_duration_s": round(duration, 1),
                     "filtered": 0},
                )]
            events["cal_since"] = 0.0
        return v, []

    if not events["cal_since"]:
        events["cal_since"] = now
        log(f"CAL flag set (status: {status}) -- passing through")
        return v, []

    duration = now - events["cal_since"]
    if duration < CAL_MAX_SEC:
        return v, []

    cleaned = dict(v)
    rest = [f for f in flags if f != "CAL"]
    # An empty status would be worse than a wrong one -- clients would treat it as a
    # broken response. Falling back to OL matches reality: the UPS is supplying power,
    # only its self-test flag is stuck.
    cleaned["ups.status"] = " ".join(rest) if rest else "OL"

    if not events["cal_filtered"]:
        events["cal_filtered"] = True
        log(f"CAL has been standing for {duration:.0f}s -- removing it from ups.status "
            f"from now on (raw: {status!r}, delivered: {cleaned['ups.status']!r})")
        return cleaned, [(
            "cal_stuck", "UPS: stuck calibration flag defused",
            f"The UPS has been reporting CAL continuously for "
            f"{duration / 60:.0f} min (raw status: {status}).\n\n"
            f"The proxy is now removing the flag from the status delivered to "
            f"upsmon. Without this, the next dropped connection would trigger an "
            f"immediate shutdown.\n\n"
            f"Now delivering: {cleaned['ups.status']}",
            4, ["warning", "electric_plug"],
            {"status_text": status or "?", "cal_duration_s": round(duration, 1),
             "filtered": 1},
        )]
    return cleaned, []


def _number(v, key, default=0.0):
    try:
        return float(v.get(key, default))
    except (TypeError, ValueError):
        return default


def _deliver(kind, title, text, prio, tags, fields):
    """Blocking -- only ever called through to_thread."""
    ntfy(title, text, prio, tags)
    influx_write_event(kind, fields)


def detect_status_change(v):
    """Edge detection on `ups.status`.

    Returns ready-made messages instead of delivering them, so the caller can send them
    in the background. A hanging ntfy or InfluxDB request must never slow down the poll
    interval against the UPS.
    """
    status = v.get("ups.status", "")
    flags = set(status.split())
    before = events["flags"]
    events["flags"] = flags
    now = time.time()

    if before is None:
        # The first poll after startup is not an edge -- otherwise every restart of the
        # container reports a power failure that never happened.
        log(f"Event detection active, initial status: {status or '?'}")
        if "OB" in flags:
            events["ob_since"] = now
        return []

    messages = []
    voltage = _number(v, "input.voltage")
    charge = _number(v, "battery.charge")
    runtime = _number(v, "battery.runtime")
    base = {"status_text": status or "?", "input_voltage": voltage,
            "battery_charge": charge}

    if "OB" in flags and "OB" not in before:
        events["ob_since"] = now
        # Self-test detection: CAL appears in the SAME edge as OB, because the UPS runs
        # the test by briefly switching to battery. If CAL was already set beforehand
        # (stuck flag, see filter_stuck_cal), a newly appearing OB still counts as a real
        # alarm -- an outage during a stuck CAL must never pass as harmless.
        selftest = "CAL" in flags and "CAL" not in before
        events["ob_is_selftest"] = selftest
        if selftest:
            messages.append((
                "selftest_start", "UPS self-test running",
                f"Scheduled battery test, brief switch to battery.\n"
                f"Battery {charge:.0f} %, status: {status}",
                2, ["battery"],
                dict(base, on_battery=1, duration_s=0.0),
            ))
        else:
            messages.append((
                "on_battery", "POWER FAILURE",
                f"The UPS is running on battery.\n"
                f"Battery {charge:.0f} %, runtime left {runtime / 60:.0f} min.\n"
                f"Status: {status}",
                5, ["electric_plug", "warning"],
                dict(base, on_battery=1, duration_s=0.0),
            ))
    elif "OB" not in flags and "OB" in before:
        duration = now - events["ob_since"] if events["ob_since"] else 0.0
        if events["ob_is_selftest"]:
            messages.append((
                "selftest_end", "UPS self-test finished",
                f"Duration {duration:.0f} s.\n"
                f"Input {voltage:.1f} V, battery {charge:.0f} %.\n"
                f"Status: {status}",
                2, ["battery", "white_check_mark"],
                dict(base, on_battery=0, duration_s=round(duration, 1)),
            ))
        else:
            messages.append((
                "on_line", "Mains restored",
                f"Back on mains after {duration:.0f} s on battery.\n"
                f"Input {voltage:.1f} V, battery {charge:.0f} %.\n"
                f"Status: {status}",
                4, ["electric_plug", "white_check_mark"],
                dict(base, on_battery=0, duration_s=round(duration, 1)),
            ))
        events["ob_is_selftest"] = False

    if "LB" in flags and "LB" not in before:
        messages.append((
            "low_battery", "UPS battery CRITICAL",
            f"Low-battery flag set.\n"
            f"Battery {charge:.0f} %, runtime left {runtime / 60:.0f} min.\n"
            f"Status: {status}",
            5, ["battery", "warning"],
            dict(base, on_battery=1 if "OB" in flags else 0, duration_s=0.0),
        ))

    for entry in messages:
        log(f"Event '{entry[0]}' detected -- status: {status}")
    return messages


def _notify(kind, title, text, prio, tags, fields):
    """Deliver a fault notification in the background."""
    asyncio.create_task(asyncio.to_thread(_deliver, kind, title, text, prio, tags, fields))


# Ongoing fault: start, current error kind, all kinds seen so far, notified yet?
fault = {"since": 0.0, "kind": "", "kinds": {}, "notified": False}


def fault_error(e, consecutive_errors):
    kind = error_kind(e)
    now = time.time()
    if not fault["since"]:
        fault.update(since=now, kind=kind, kinds={kind: 1}, notified=False)
        log(f"FAULT BEGINS: {kind} (upstream login: {UPSTREAM_USER or 'anonymous'})")
        return
    fault["kinds"][kind] = fault["kinds"].get(kind, 0) + 1
    if kind != fault["kind"]:
        log(f"FAULT: error kind changes after {now - fault['since']:.0f}s: "
            f"{fault['kind']} -> {kind}")
        fault["kind"] = kind
    duration = now - fault["since"]
    if not fault["notified"] and duration >= FAULT_ALERT_SEC:
        fault["notified"] = True
        log(f"FAULT has lasted {duration:.0f}s -- notifying")
        _notify("upstream_fault", "UPS: NUT not answering",
                f"No data from the UPS for {duration:.0f} s.\n"
                f"Cause according to the UPS: {kind}\n\n"
                f"While this lasts, no client behind the proxy can shut down cleanly "
                f"on a power failure.",
                4, ["warning", "electric_plug"],
                {"error": kind, "duration_s": round(duration, 1),
                 "failures": consecutive_errors})


def fault_end(consecutive_errors):
    if not fault["since"]:
        return
    duration = time.time() - fault["since"]
    kinds = ", ".join(f"{k} ({n}x)" for k, n in fault["kinds"].items())
    log(f"FAULT ENDS after {duration:.0f}s and {consecutive_errors} failed polls. "
        f"Error kinds: {kinds}")
    if fault["notified"]:
        _notify("upstream_back", "UPS: NUT reachable again",
                f"Data again after "
                f"{f'{duration / 60:.0f} min' if duration >= 120 else f'{duration:.0f} s'}.\n"
                f"Error kinds: {kinds}",
                3, ["white_check_mark", "electric_plug"],
                {"error": fault["kind"], "duration_s": round(duration, 1),
                 "failures": consecutive_errors})
    fault.update(since=0.0, kind="", kinds={}, notified=False)


async def poll_loop():
    """The ONLY thing in this process that opens connections to the real UPS."""
    consecutive_errors = 0
    while True:
        cache["polls"] += 1
        try:
            v = await asyncio.to_thread(upstream_list_var)
            cache["raw"] = v
            # The CAL protection ALWAYS runs -- it is a safety function, not a
            # notification. Only whether it notifies depends on EVENT_ALERTS.
            cleaned, cal_messages = filter_stuck_cal(v)
            cache["vars"] = cleaned
            cache["last_success"] = time.time()
            if EVENT_ALERTS:
                # Events are detected on the RAW values: filtering only applies to what
                # goes out to upsmon and friends, not to what we see ourselves.
                for kind, title, text, prio, tags, fields in (
                        detect_status_change(v) + cal_messages):
                    asyncio.create_task(asyncio.to_thread(
                        _deliver, kind, title, text, prio, tags, fields))
            if consecutive_errors > 0:
                log(f"Upstream reachable again after {consecutive_errors} failures")
                fault_end(consecutive_errors)
            consecutive_errors = 0
        except Exception as e:
            consecutive_errors += 1
            cache["poll_errors"] += 1
            fault_error(e, consecutive_errors)
            # Log the first few and then rarely, so a longer fault does not flood.
            if consecutive_errors <= 3 or consecutive_errors % 20 == 0:
                log(f"Upstream error ({consecutive_errors} in a row): {error_kind(e)}")
        await asyncio.sleep(POLL_INTERVAL)


def is_fresh():
    return (time.time() - cache["last_success"]) < STALE_SEC


def reset_close(writer):
    """Hard RST instead of a clean FIN -- see the module docstring. Downstream should
    see the same error it would get from a refused connection to the real UPS."""
    sock = writer.get_extra_info("socket")
    try:
        if sock is not None:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER,
                            struct.pack("ii", 1, 0))
    except OSError:
        pass
    writer.close()


def upstream_fsd():
    with socket.create_connection((UPSTREAM_HOST, UPSTREAM_PORT), timeout=5) as s:
        _login(s, s.makefile("rb"))
        s.sendall(f"FSD {UPS_NAME}\n".encode())
        s.recv(256)
        s.sendall(b"LOGOUT\n")


async def handle_client(reader, writer):
    peer = writer.get_extra_info("peername")

    if not is_fresh():
        log(f"{peer}: cache stale ({time.time() - cache['last_success']:.0f}s) "
            "-- connection reset immediately", to_file=False)
        reset_close(writer)
        return

    try:
        while True:
            line = await reader.readline()
            if not line:
                break
            cmd = line.decode(errors="replace").strip()
            if not cmd:
                continue

            upper = cmd.upper()
            reply = None

            if upper.startswith("USERNAME ") or upper.startswith("PASSWORD "):
                reply = "OK\n"
            elif upper.startswith("LOGIN "):
                reply = "OK\n"
            elif upper.startswith("MASTER ") or upper.startswith("PRIMARY "):
                reply = "OK MASTER-GRANTED\n"
            elif upper == "LOGOUT":
                writer.write(b"OK Goodbye\n")
                await writer.drain()
                break
            elif upper == "VER":
                reply = f"nut-cache-proxy {VERSION}\n"
            elif upper == "NETVER":
                reply = "1.2\n"
            elif upper == "LIST UPS":
                reply = (f"BEGIN LIST UPS\n"
                         f'UPS {UPS_NAME} "{UPS_DESCRIPTION}"\n'
                         f"END LIST UPS\n")
            elif upper.startswith("LIST VAR "):
                if not is_fresh():
                    log(f"{peer}: cache went stale mid-connection -- reset")
                    reset_close(writer)
                    return
                lines = [f"BEGIN LIST VAR {UPS_NAME}"]
                for k, val in cache["vars"].items():
                    lines.append(f'VAR {UPS_NAME} {k} "{val}"')
                lines.append(f"END LIST VAR {UPS_NAME}")
                reply = "\n".join(lines) + "\n"
            elif upper.startswith("GET VAR "):
                if not is_fresh():
                    log(f"{peer}: cache went stale mid-connection -- reset")
                    reset_close(writer)
                    return
                parts = cmd.split(" ", 3)
                varname = parts[3] if len(parts) > 3 else ""
                val = cache["vars"].get(varname)
                reply = ("ERR VAR-NOT-SUPPORTED\n" if val is None
                         else f'VAR {UPS_NAME} {varname} "{val}"\n')
            elif upper.startswith("FSD "):
                # Rare write command -- deliberately NOT answered from cache, but
                # forwarded to the real UPS exactly once.
                try:
                    await asyncio.to_thread(upstream_fsd)
                    reply = "OK FSD-SET\n"
                    log(f"{peer}: FSD forwarded to the real UPS")
                except Exception as e:
                    reply = "ERR FSD-FAILED\n"
                    log(f"{peer}: forwarding FSD failed: {e}")
            else:
                reply = "ERR UNKNOWN-COMMAND\n"

            if reply is not None:
                writer.write(reply.encode())
                await writer.drain()
    except (ConnectionResetError, BrokenPipeError):
        pass
    finally:
        writer.close()


async def status_log_loop():
    """One line every five minutes -- useful for the before/after comparison of
    connection load on the real UPS."""
    while True:
        await asyncio.sleep(300)
        age = time.time() - cache["last_success"]
        log(f"Status: {cache['polls']} polls, {cache['poll_errors']} errors, "
            f"cache age {age:.0f}s, {len(cache['vars'])} variables")


async def main():
    if not UPSTREAM_HOST:
        raise SystemExit(
            "UPSTREAM_HOST is not set. Point it at the NUT server inside your UPS, "
            "e.g. UPSTREAM_HOST=192.0.2.10")

    log(f"nut-cache-proxy {VERSION} starting")
    log(f"  upstream  {UPSTREAM_HOST}:{UPSTREAM_PORT} ups={UPS_NAME}")
    log(f"  listening {LISTEN_HOST}:{LISTEN_PORT}")
    log(f"  poll every {POLL_INTERVAL}s, serving stale-reset after {STALE_SEC}s")
    log(f"  stuck-CAL protection: flag removed from delivered ups.status after "
        f"{CAL_MAX_SEC:.0f}s")
    log(f"  upstream login: {UPSTREAM_USER or 'none (anonymous)'}; "
        f"log file: {LOG_FILE or 'none'}; fault notification after {FAULT_ALERT_SEC:.0f}s")
    if EVENT_ALERTS:
        log(f"  event alerts ON (ntfy {NTFY_URL or 'disabled'} topic {NTFY_TOPIC}, "
            f"influx {INFLUX_URL or 'disabled'} db {INFLUX_DB}"
            f"{', with token' if INFLUX_TOKEN else ''})")
    else:
        log("  event alerts off")

    asyncio.create_task(poll_loop())
    asyncio.create_task(status_log_loop())
    server = await asyncio.start_server(handle_client, LISTEN_HOST, LISTEN_PORT)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())

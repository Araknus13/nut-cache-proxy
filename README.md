# nut-cache-proxy

**One real connection to your UPS. Every client served from cache.**

A small read-through caching proxy that speaks the NUT (Network UPS Tools) wire protocol.
It holds exactly one persistent connection to the NUT server running inside a
network-attached UPS, polls it on a fixed interval, and answers all downstream
`LIST VAR` / `GET VAR` requests from memory.

No dependencies beyond the Python standard library. One file.

---

## The problem

Network-attached UPSes run a NUT server on the device itself, and that server usually has
a very small connection budget — often just a handful of slots.

In a homelab that fills up fast. Several `upsmon` instances (one per host) hold persistent
connections. Then add a metrics poller, a shutdown watchdog, a NAS querying "device
information", and they start fighting over the last free slots.

What that looks like in practice, measured before this proxy existed:

| | |
|---|---|
| connection resets on a 30 s metrics poller | **11 %** |
| sampled points with no valid reading | 34 of 39,259 |
| dropouts on the **persistent** `upsmon` connections | several, spread over days |

No single culprit. Just competition for a scarce resource — which makes it frustrating to
debug, because every individual client looks correctly configured.

## What this does

```
   upsmon (host A) ─┐
   upsmon (host B) ─┤
   metrics poller  ─┼──► nut-cache-proxy ──(1 connection)──► UPS
   shutdown watch  ─┤
   NAS             ─┘
```

One service becomes the only real peer of the UPS. Downstream clients talk plain NUT and
do not know the difference.

`LOGIN` / `USERNAME` / `PASSWORD` / `LOGOUT` are acknowledged locally without being
forwarded — they only gate readable variables. **This assumes a trusted internal network
segment.** Do not expose the listener to anything else.

## Quick start

Clone the repo, point `UPSTREAM_HOST` at your UPS, and bring it up — the shipped
`docker-compose.yml` builds from the Dockerfile, so there is no registry to configure:

```bash
git clone https://github.com/Araknus13/nut-cache-proxy
cd nut-cache-proxy
# edit UPSTREAM_HOST in docker-compose.yml
docker compose up -d
```

It is a single stdlib-only file, so running it without Docker works too:

```bash
UPSTREAM_HOST=192.0.2.10 python3 nut_proxy.py
```

The relevant part of the compose file:

```yaml
services:
  nut-cache-proxy:
    build: .
    container_name: nut-cache-proxy
    restart: unless-stopped
    ports:
      - "3493:3493"
    environment:
      UPSTREAM_HOST: 192.0.2.10   # the NUT server inside your UPS
      UPS_NAME: ups
```

Then point your clients at the proxy instead of the UPS:

```
# /etc/nut/upsmon.conf
MONITOR ups@nut-proxy-host 1 monuser secret primary
```

Verify:

```bash
upsc ups@nut-proxy-host
```

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `UPSTREAM_HOST` | *(required)* | Address of the NUT server inside the UPS |
| `UPSTREAM_PORT` | `3493` | Its port |
| `UPS_NAME` | `ups` | UPS name as the upstream knows it |
| `UPS_DESCRIPTION` | `UPS (via nut-cache-proxy)` | Shown in `LIST UPS` |
| `LISTEN_HOST` | `0.0.0.0` | Listener bind address |
| `LISTEN_PORT` | `3493` | Listener port |
| `POLL_INTERVAL` | `3` | Seconds between upstream polls. Keep it **below** your clients' `POLLFREQ` |
| `STALE_SEC` | `20` | After this long without a successful poll, connections are reset. Keep it **well below** upsmon's `DEADTIME` |
| `CAL_MAX_SEC` | `120` | How long a `CAL` flag may stand before it is stripped — see below |
| `EVENT_ALERTS` | `false` | Enable status-change notifications |
| `NTFY_URL` | *(empty)* | ntfy **root** URL, e.g. `http://ntfy:80`. Empty disables |
| `NTFY_TOPIC` | `ups` | ntfy topic |
| `INFLUX_URL` | *(empty)* | InfluxDB 1.x base URL. Empty disables |
| `INFLUX_DB` | `hardware` | Target database |
| `EVENT_MEASUREMENT` | `ups_event` | Measurement for event points |

## ⛔ The stuck CAL flag — read this before you restart the container

This is the part that cost real downtime, and it is the reason the proxy does something
slightly opinionated to your data.

`upsmon` has a special path for **"last known to be calibrating and currently not
communicating"**. It does not wait for `DEADTIME`. It shuts the host down immediately:

```
Poll UPS failed - Write error: Broken pipe
UPS was last known to be calibrating and currently is not communicating, assuming dead
Executing automatic power-fail shutdown
```

On the setup this was written for, a **two-second restart of this very container** shut
the host down four seconds later. The `CAL` flag was **six days old** — left over from a
genuine ten-second calibration that `upsmon` had never cleared. The same mechanism had
already cost 6 h 43 min of downtime the week before.

**Raising `DEADTIME` does not help.** This path never waits for it.

The proxy is the single point every client hangs off, so the trap is defused here:

- a `CAL` flag is **passed through unchanged** while it is fresh — real self-tests last
  seconds, and you want to see them
- once it has stood for longer than `CAL_MAX_SEC`, it is **removed** from the
  `ups.status` delivered to clients
- the **raw** status is kept internally, logged, and reported if alerts are on

Set `CAL_MAX_SEC` high enough that it can never interfere with a running self-test. The
default of 120 s is an order of magnitude above every real calibration observed here.

## Design notes

**Events instead of sampling.** A metrics poller on a 30-second interval will miss a short
outage entirely. That happened here: a real mains dropout fell between two samples, and
both neighbours read `OL`, voltage nominal, `on_battery=0`. The series was completely
gap-free — exactly 120 points per hour — and showed "all fine" throughout.

*A gap-free series is not evidence that nothing happened.* For events shorter than the
sampling interval it will stubbornly report normality.

Since the proxy polls every few seconds anyway, detecting transitions here costs **no
additional connection** — which is the whole point.

**Self-tests are not power failures.** If `CAL` appears in the *same* transition as `OB`,
it is the UPS running its scheduled battery test; that gets a quiet notification. If `CAL`
was already set *beforehand*, a newly appearing `OB` still counts as a real alarm — an
outage during a stuck CAL flag must never be waved through.

**On a real outage, fail loudly.** If the cache goes longer than `STALE_SEC` without a
successful poll, the proxy still accepts connections but closes them immediately with a
hard reset (`SO_LINGER`), before reading any request. Clients see a connection error,
which every NUT client already handles.

Serving stale or empty data would be far more dangerous: it looks like a successful but
contentless poll and passes silently as "UPS online, everything zero". A plausible-looking
wrong answer is worse than a visible error.

## What this deliberately does not do

- **No session multiplexing.** The UPS keeps login/primary state per connection, so
  forwarding individual sessions would be risky. This does read-through caching instead.
- **`FSD` is not simulated.** A client sending `FSD <ups>` has it forwarded to the real
  UPS as a genuine one-off write.
- **No `INSTCMD`.** Active commands such as test alarms are not supported.

## Tests

```bash
python test_proxy.py
```

Spins up a fake NUT upstream on localhost and drives the proxy against it — no real UPS is
touched, which matters here, because spending connections on the real device is exactly
the problem this solves.

```
1) LIST VAR served from cache
2) GET VAR, known and unknown variable
3) Connection budget: 12 client queries add no per-query upstream load
     -- 2 upstream connections (the polls only)
4) Stuck CAL flag: passed through while fresh, stripped once standing
5) Upstream dead -> connection reset, no stale data
```

## Status

Running in production since August 2026 in front of several `upsmon` instances, a metrics
poller and a shutdown watchdog. Tested against one specific network-attached UPS; the
protocol surface used (`LIST VAR`, `GET VAR`, `LIST UPS`, `VER`, `NETVER`, `FSD`) is
standard NUT, but reports from other hardware are welcome.

## License

MIT — see [LICENSE](LICENSE).

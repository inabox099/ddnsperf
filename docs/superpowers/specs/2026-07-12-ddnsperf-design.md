# Design: ddnsperf

> **Status:** Approved  
> **Owner:** florian.rindlisbacher@gmail.com  
> **Date:** 2026-07-12  
> **Version:** 0.1.0

---

## 1. Overview

`ddnsperf` is a Rust CLI tool for benchmarking and stress-testing the DDNS Update (RFC 2136) performance of a DNS server. It measures response time, throughput, and error rate under configurable load, and can automatically discover a server's maximum sustainable throughput via a PID-controlled performance test.

---

## 2. Architecture

Single Rust binary crate, structured into five modules with clear boundaries:

```
ddnsperf/
├── src/
│   ├── main.rs      # wires everything together, starts Tokio runtime
│   ├── cli.rs       # clap argument definitions and validated Config struct
│   ├── engine.rs    # load generator: task pool, rate limiter, request dispatch
│   ├── dns.rs       # hickory-dns wrapper: update construction, TSIG, record generation
│   ├── stats.rs     # atomic metrics collection, progress bar, final report
│   └── perf.rs      # PID-controlled performance test, drives engine
├── Cargo.toml
└── docs/
```

**Data flow:**
1. `cli` parses args into a `Config` struct
2. `main` hands `Config` to `engine` (fixed-rate / unlimited) or `perf` (perf-test / `--rps auto`)
3. `perf`, when active, runs the PID loop to find max RPS, then hands control to `engine`
4. `engine` spawns N Tokio tasks; a shared `Governor` rate limiter gates throughput; each task calls `dns::send_update()` and sends the `Outcome` to a stats channel
5. A dedicated stats task collects outcomes, drives the progress bar, and prints the final report on completion

**Key dependencies:**
- `tokio` — async runtime (multi-threaded)
- `hickory-dns` — DNS client with RFC 2136 dynamic update and TSIG support
- `clap` (derive) — CLI argument parsing
- `governor` — token bucket rate limiter
- `indicatif` — progress bar
- `rand` — random record selection
- `testcontainers` (dev) — DNS server containers for integration tests

---

## 3. CLI Interface

```
ddnsperf [OPTIONS] --server <ADDR> --zone <ZONE> --network <CIDR>

Required:
  -s, --server <ADDR>          DNS server address, e.g. 192.168.1.1:53 or [2001:db8::1]:53
  -z, --zone <ZONE>            DNS zone, e.g. example.com
  -n, --network <CIDR>         Subnet to generate records from, e.g. 10.0.0.0/24

Load control:
  -r, --requests <N>           Total number of DNS messages to send
                               (mutually exclusive with --duration and --perf-test)
      --duration <SECS>        Run for this many seconds
      --rps <N|auto>           Fixed target RPS, or 'auto' to run a perf test first
                               then benchmark at the discovered rate
  -c, --concurrency <N>        Concurrent Tokio tasks (default: 50)

Record options:
      --prefix <STR>           Hostname prefix (default: "host-")
      --operation <OP>         transaction | add | delete (default: transaction)
                                 transaction: add A+PTR then delete PTR+A (4 DNS messages)
                                 add:         add records only
                                 delete:      delete records only
      --record-type <TYPE>     a | aaaa | all (default: a)
                               In transaction mode, PTR is always included.
                               'all' in transaction mode sends A+AAAA+PTR (6 messages).
      --mode <MODE>            sequential | random (default: sequential)

Transport:
      --udp                    Use UDP transport (default)
      --tcp                    Use TCP transport
      --ipv4                   Force IPv4 transport to the DNS server
      --ipv6                   Force IPv6 transport to the DNS server

TSIG authentication (all three required together or all omitted):
      --tsig-name <NAME>       TSIG key name
      --tsig-algo <ALGO>       hmac-md5 | hmac-sha1 | hmac-sha256 (default: hmac-sha256)
      --tsig-secret <BASE64>   TSIG secret, base64-encoded

Performance test mode (mutually exclusive with --rps and --requests):
      --perf-test              Find and report maximum sustainable throughput via PID
                               control; does not run a subsequent benchmark
      --error-target <PCT>     Target error rate setpoint for PID controller (default: 1.0)
      --max-rps <N>            Safety cap on RPS during perf test (optional)
```

**Validation rules:**
- `--udp` and `--tcp` are mutually exclusive; `--udp` is the default
- `--ipv4` and `--ipv6` are mutually exclusive; default is inferred from server address format
- `--tsig-name`, `--tsig-algo`, `--tsig-secret` must all be present or all absent
- `--perf-test` is mutually exclusive with `--rps` and `--requests`
- `--rps auto` runs a perf test phase followed immediately by a benchmark phase; no pause between phases
- `--rps auto` with neither `--requests` nor `--duration`: benchmark phase runs until Ctrl-C
- `--record-type aaaa` or `--record-type all` requires the `--network` CIDR to be an IPv6 prefix (or a tool error is emitted)

---

## 4. Engine & Rate Control

**Concurrency model:**
- Single multi-threaded Tokio runtime
- `engine::run(config, stats_tx)` spawns N async tasks (`--concurrency`, default 50)
- A shared `Governor` token bucket rate limiter gates throughput; `--rps <N>` configures it; omitting `--rps` removes the cap
- Each task loops: acquire rate-limit token → pick next record → call `dns::send_update()` → send `Outcome` to stats channel → repeat
- Tasks stop when a shared atomic counter reaches the request limit, or a cancellation token fires (for `--duration` mode)

**Concurrency vs. RPS:** Both limits apply simultaneously — the engine never exceeds either ceiling.

**`perf` module (PID controller):**
- Wraps the engine, sampling the rolling error rate from `stats` every 500ms
- PID controller adjusts the `Governor`'s rate cap based on the error rate vs. `--error-target` setpoint; gains are tuned internally and not exposed to users
- Convergence detection: if the RPS target changes by less than 2% over 5 consecutive 500ms windows, the test is considered converged
- On convergence, reports the max sustainable RPS and either stops (`--perf-test`) or hands the discovered rate to `engine` for the benchmark phase (`--rps auto`)

---

## 5. Stats & Reporting

**Collection:**
- Each engine task sends `Outcome { latency_us: u64, success: bool }` via an unbounded Tokio MPSC channel to a dedicated stats task
- Stats task maintains: total sent, total success, total error, min/max latency, running mean (Welford's online algorithm), rolling 1-second window for live RPS and error rate

**Progress bar** (`indicatif`, updated every 250ms):
```
[00:00:12] ████████░░░░░░░░░░░░  2 400 / 10 000  RPS: 198  Errors: 0.5%  Avg: 1.2ms
```
During perf-test phase, bar shows elapsed time and current test RPS instead of a request count.

**Final report to stdout:**
```
=== ddnsperf results ===
Duration:      12.3s
Total sent:    10 000
  Successful:  9 950  (99.5%)
  Errors:      50     (0.5%)
Throughput:    812 RPS
Latency:
  Min:         0.4ms
  Mean:        1.2ms
  Max:         18.3ms

[perf-test phase]
Max sustainable RPS:  1 240  (converged after 42s, error target: 1.0%)
Benchmark ran at:     1 240 RPS
```
The perf-test section is only shown when `--perf-test` or `--rps auto` was used.

---

## 6. DNS Module

**Record generation:**
- Takes `--network` CIDR and `--prefix`, produces `(hostname, ip, ptr_name)` triples
- Sequential mode: atomic counter mod subnet size → deterministic index into the address space
- Random mode: `rand` picks a random host address within the subnet each call
- IPv4: `A` records and `x.x.x.x.in-addr.arpa` PTR names
- IPv6: `AAAA` records and `ip6.arpa` reverse PTR names

**Update construction:**
- Wraps `hickory-dns` `AsyncClient` with RFC 2136 `DynamicUpdate` message builder
- Transaction mode: two sequential DNS Update messages per task — one adding A+PTR, one deleting PTR+A
- TSIG: when `--tsig-name` is provided, wraps the client in a `TSIGSigner` at construction; all updates signed transparently

**Connection management:**
- UDP (`--udp`): one `UdpClientStream` per task — stateless, no persistent connection
- TCP (`--tcp`): one persistent `TcpClientStream` per task, reconnecting on error
- Each of the N engine tasks owns its own client instance — no shared connection pool, avoids contention at high RPS
- IPv4/IPv6 transport: `hickory-dns` streams accept a `SocketAddr`, which natively handles both address families. `--ipv4` / `--ipv6` flags force binding to a specific address family; default infers from the server address format (`[::1]:53` → IPv6, `1.2.3.4:53` → IPv4)

---

## 7. Testing Strategy

**Unit tests** (`#[cfg(test)]` in each module):
- `dns`: record generation correctness — sequential indexing, random distribution within subnet, correct PTR name formatting for IPv4 and IPv6, correct transaction message ordering
- `stats`: Welford running mean accuracy, rolling window RPS calculation, progress bar formatting
- `perf`: PID controller convergence using a mock error rate source
- `cli`: argument validation — TSIG all-or-nothing, `--ipv4`/`--ipv6` mutual exclusion, `--udp`/`--tcp` mutual exclusion, `--perf-test` vs. `--rps` mutual exclusion

**Integration tests** (`tests/`):
- Spin up a local DNS server (BIND or PowerDNS) in a test container via `testcontainers` with a test zone and TSIG key configured
- Run the tool end-to-end: transaction mode, add-only, delete-only; verify records are created/removed via resolver queries
- Run a short `--perf-test` and verify the tool terminates and reports a sensible RPS

**Manual smoke test** (documented in README):
- Run against a local BIND container with a sample config; expected output included for verification without a full CI environment

---

## 8. Open Questions

- Should `--record-type all` in transaction mode be supported in v0.1, or deferred? (Currently in scope but adds complexity to message construction.)
- Default concurrency of 50 tasks — validate this is sufficient to saturate a server at 1000+ RPS on a typical test machine, or make it auto-scale.

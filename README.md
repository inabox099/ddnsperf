# ddnsperf

A fast, TSIG-authenticated DDNS Update (RFC 2136) benchmarking tool written in Rust.

Send thousands of dynamic DNS update transactions per second against a DNS server and measure throughput, latency, and error rate. Includes a PID-controlled performance test that automatically finds the server's maximum sustainable request rate.

---

## Features

- **Full transactions** — each operation adds an A + PTR record, then deletes PTR + A (4 DNS messages); each leg timed individually
- **High concurrency** — configurable pool of async Tokio tasks
- **Rate limiting** — fixed RPS cap via token bucket (Governor)
- **Time-bounded runs** — stop after N seconds instead of a fixed request count
- **TSIG authentication** — HMAC-MD5, HMAC-SHA1, HMAC-SHA256
- **TCP and UDP** — selectable per run; IPv4/IPv6 transport forcing
- **PID perf test** — automatically discovers maximum sustainable throughput; can feed directly into a benchmark run (`--rps auto`)
- **Live progress bar** — real-time tx/s display via indicatif
- **Single-shot mode** — send one transaction to a specific hostname/IP for spot-checking

---

## Building

Requires Rust 1.75+ and Cargo.

```bash
git clone <repo>
cd ddnsperf
cargo build --release
# binary at: ./target/release/ddnsperf
```

---

## Smoke Test Setup

The integration tests and smoke tests require a local BIND9 instance with a test zone and TSIG key.

### 1. Generate a TSIG key

```bash
# Run tsig-keygen inside a temporary BIND container
docker run --rm internetsystemsconsortium/bind9:9.18 tsig-keygen -a hmac-sha256 test-key
```

> **Podman users:** replace `docker` with `podman` throughout this section.

Example output:
```
key "test-key" {
    algorithm hmac-sha256;
    secret "i8ThSp4D0SYlHviiQCCxAs4qgZtisWG845b7ttlCT6w=";
};
```

Save the `secret` value — you will use it as `--tsig-secret`.

### 2. Create the named.conf

```bash
cat > /tmp/named.conf.test << 'EOF'
key "test-key" {
    algorithm hmac-sha256;
    secret "i8ThSp4D0SYlHviiQCCxAs4qgZtisWG845b7ttlCT6w=";
};

options {
    directory "/var/cache/bind";
    listen-on port 5353 { any; };
    allow-query { any; };
};

zone "test.local" {
    type master;
    file "/var/cache/bind/db.test.local";
    allow-update { key "test-key"; };
};

zone "0.0.10.in-addr.arpa" {
    type master;
    file "/var/cache/bind/db.10.0.0";
    allow-update { key "test-key"; };
};
EOF
```

> Replace the `secret` value with the one generated in step 1.

### 3. Create the zone files

```bash
cat > /tmp/db.test.local << 'EOF'
$TTL 300
@ IN SOA ns1.test.local. admin.test.local. (
    1 ; serial
    3600 ; refresh
    900  ; retry
    604800 ; expire
    300 ) ; minimum
@ IN NS ns1.test.local.
ns1 IN A 127.0.0.1
EOF

cat > /tmp/db.10.0.0 << 'EOF'
$TTL 300
@ IN SOA ns1.test.local. admin.test.local. (1 3600 900 604800 300)
@ IN NS ns1.test.local.
EOF
```

### 4. Start the BIND container

```bash
# Docker
docker run -d --name bind-test \
  --network=host \
  -v /tmp/named.conf.test:/etc/bind/named.conf:Z \
  -v /tmp/db.test.local:/var/cache/bind/db.test.local:Z \
  -v /tmp/db.10.0.0:/var/cache/bind/db.10.0.0:Z \
  internetsystemsconsortium/bind9:9.18

# Podman
podman --root /tmp/podman-test-storage --storage-driver vfs run -d \
  --name bind-test --network=host \
  -v /tmp/named.conf.test:/etc/bind/named.conf:Z \
  -v /tmp/db.test.local:/var/cache/bind/db.test.local:Z \
  -v /tmp/db.10.0.0:/var/cache/bind/db.10.0.0:Z \
  internetsystemsconsortium/bind9:9.18
```

### 5. Verify it's working

```bash
dig @127.0.0.1 -p 5353 test.local SOA +short
# Expected: ns1.test.local. admin.test.local. 1 3600 900 604800 300
```

### 6. Run a smoke test

```bash
./target/release/ddnsperf \
  --server 127.0.0.1:5353 \
  --zone test.local. \
  --network 10.0.0.0/24 \
  --requests 20 \
  --concurrency 4 \
  --tsig-name test-key. \
  --tsig-secret "i8ThSp4D0SYlHviiQCCxAs4qgZtisWG845b7ttlCT6w="
```

Expected output:
```
=== ddnsperf results ===
Duration:      0.042s
Total sent:    20
  Successful:  20 (100.0%)
  Errors:      0 (0.0%)
Throughput:    479 RPS
Latency:
  Min:         4.429ms
  Mean:        7.955ms
  Max:         15.038ms
```

### 7. Stop the container when done

```bash
docker stop bind-test && docker rm bind-test
# or: podman --root /tmp/podman-test-storage --storage-driver vfs stop bind-test
```

---

## Usage

### Benchmark mode

Send a fixed number of transactions generated from a CIDR subnet:

```bash
ddnsperf \
  --server 192.168.1.1:53 \
  --zone example.com. \
  --network 10.0.0.0/24 \
  --requests 10000 \
  --concurrency 50 \
  --tsig-name mykey. \
  --tsig-secret "base64encodedSecret=="
```

#### Time-bounded run

Run for 60 seconds instead of a fixed count:

```bash
ddnsperf \
  --server 192.168.1.1:53 \
  --zone example.com. \
  --network 10.0.0.0/24 \
  --duration 60 \
  --concurrency 50 \
  --tsig-name mykey. \
  --tsig-secret "base64encodedSecret=="
```

#### Rate-limited run

Cap throughput at 500 transactions/second:

```bash
ddnsperf ... --requests 10000 --rps 500
```

#### Random record selection

Pick random addresses from the subnet instead of iterating sequentially:

```bash
ddnsperf ... --network 10.0.0.0/16 --mode random --requests 50000
```

#### Custom hostname prefix

Default prefix is `host-`. Override with `--prefix`:

```bash
ddnsperf ... --network 10.0.0.0/24 --prefix "workstation-"
```

Generates hostnames like `workstation-167772161.example.com.`

### Performance test mode

Automatically find the server's maximum sustainable throughput using a PID controller. The controller increases RPS until the error rate rises above `--error-target`, then stabilises.

```bash
ddnsperf \
  --server 192.168.1.1:53 \
  --zone example.com. \
  --network 10.0.0.0/24 \
  --concurrency 100 \
  --perf-test \
  --error-target 1.0 \
  --perf-duration 120 \
  --tsig-name mykey. \
  --tsig-secret "base64encodedSecret=="
```

Example output:
```
[perf-test phase]
Max sustainable RPS: 2840  (converged in 74.2s, error target: 1.0%)
=== ddnsperf results ===
Duration:      74.2s
Total sent:    87432
  Successful:  86559 (99.0%)
  Errors:      873   (1.0%)
Throughput:    1178 RPS
Latency:
  Min:         0.8ms
  Mean:        4.2ms
  Max:         312.1ms
```

### `--rps auto` — perf test then benchmark

Run a perf test first to discover max RPS, then immediately run a full benchmark at that rate:

```bash
ddnsperf \
  --server 192.168.1.1:53 \
  --zone example.com. \
  --network 10.0.0.0/24 \
  --requests 5000 \
  --concurrency 50 \
  --rps auto \
  --error-target 1.0 \
  --perf-duration 60 \
  --tsig-name mykey. \
  --tsig-secret "base64encodedSecret=="
```

### TCP transport

Use TCP instead of UDP (default):

```bash
ddnsperf ... --tcp
```

### Force IP version

Force IPv4 or IPv6 transport to the server (useful on dual-stack hosts):

```bash
ddnsperf --server [2001:db8::1]:53 ... --ipv6
ddnsperf --server 192.168.1.1:53   ... --ipv4
```

### Single-shot mode

Send exactly one transaction to a specific hostname/IP and print per-leg latency. Useful for spot-checking connectivity and TSIG:

```bash
ddnsperf \
  --server 192.168.1.1:53 \
  --zone example.com. \
  --ptr-zone 0.0.10.in-addr.arpa. \
  --hostname host-001.example.com. \
  --ip 10.0.0.1 \
  --tsig-name mykey. \
  --tsig-secret "base64encodedSecret=="
```

Output:
```
=== ddnsperf transaction result ===
  Add A:         1.297ms
  Add PTR:       0.437ms
  Delete PTR:    0.350ms
  Delete A:      0.400ms
  -----------
  Total:         2.485ms
```

### Unauthenticated updates

Omit all `--tsig-*` flags for servers that allow unauthenticated dynamic updates (lab environments only):

```bash
ddnsperf \
  --server 127.0.0.1:53 \
  --zone test.local. \
  --network 10.0.0.0/24 \
  --requests 100
```

---

## All Options

```
Options:
  -s, --server <ADDR>              DNS server address, e.g. 192.168.1.1:53 or [2001:db8::1]:53
  -z, --zone <ZONE>                DNS forward zone, e.g. example.com.

  Record source (choose one):
      --network <CIDR>             Subnet to generate records from, e.g. 10.0.0.0/24
      --prefix <STR>               Hostname prefix used with --network [default: host-]
      --mode <MODE>                sequential | random [default: sequential]
      --ptr-zone <ZONE>            Reverse DNS zone (inferred from --network if omitted)
      --hostname <FQDN>            Single hostname for single-shot mode (requires --ip)
      --ip <IPV4>                  IPv4 address for single-shot mode (requires --hostname)

  Load control:
  -r, --requests <N>               Total transactions to send
      --duration <SECS>            Run for this many seconds (conflicts with --requests)
      --rps <N|auto>               Fixed RPS cap, or 'auto' to run perf test first
  -c, --concurrency <N>            Concurrent Tokio tasks [default: 50]

  Performance test:
      --perf-test                  Find max sustainable RPS via PID (conflicts with --rps)
      --error-target <PCT>         Error rate setpoint [default: 1.0]
      --max-rps-cap <N>            Safety cap on RPS during perf test
      --perf-duration <SECS>       Max search duration [default: 120]

  Transport:
      --tcp                        Use TCP (default: UDP)
      --udp                        Use UDP (explicit; default)
      --ipv4                       Force IPv4 transport
      --ipv6                       Force IPv6 transport

  TSIG (all three required together, or all omitted):
      --tsig-name <NAME>           TSIG key name
      --tsig-secret <BASE64>       TSIG secret, base64-encoded
      --tsig-algo <ALGO>           hmac-md5 | hmac-sha1 | hmac-sha256 [default: hmac-sha256]
```

---

## How It Works

### Transaction model

Each benchmark unit is a **full DDNS transaction**: 4 sequential DNS Update messages to the same server and connection:

1. **Add A** — `host-X.zone. 300 IN A <ip>`
2. **Add PTR** — `<reversed-ip>.in-addr.arpa. 300 IN PTR host-X.zone.`
3. **Delete PTR** — remove the PTR record
4. **Delete A** — remove the A record

Stats count each DNS message individually, so `--requests 1000` sends 4 000 DNS messages.

### Concurrency model

`--concurrency` Tokio tasks run in parallel. Each task loops independently: acquire a rate-limit token (if `--rps` is set) → pick the next record → send the transaction → report the outcome. All outcomes flow through an MPSC channel to a dedicated stats collector task.

### PID controller

The `--perf-test` mode runs a discrete PID controller (sample interval 500 ms) with:

- **Process variable:** rolling error rate over the last sample window
- **Setpoint:** `--error-target` %
- **Output:** RPS adjustment applied to the Governor rate limiter
- **Gains:** Kp = 50, Ki = 5, Kd = 10 (fixed)
- **Convergence:** |ΔRPS| < 2% for 5 consecutive samples

The controller increases RPS when the server handles load cleanly and backs off when errors appear, converging on the maximum sustainable rate.

---

## Running Tests

Unit tests run without a live server:

```bash
cargo test
```

Integration tests require the BIND container from the [Smoke Test Setup](#smoke-test-setup) section. Run them individually with `--ignored`:

```bash
# Unsigned update
cargo test test_send_add_a_unsigned -- --ignored --nocapture

# TSIG-signed update
cargo test test_send_add_a_tsig -- --ignored --nocapture

# Full transaction
cargo test test_run_transaction -- --ignored --nocapture
```

---

## Architecture

```
src/
├── main.rs      — CLI entry point; routes to benchmark, perf test, or single-shot
├── cli.rs       — clap argument definitions; validated Config struct
├── dns.rs       — hickory-dns wrapper: TSIG signing, record construction, run_transaction
├── records.rs   — CIDR-based (hostname, ip, ptr_name) generator; sequential + random modes
├── engine.rs    — concurrent task pool; Governor rate limiter; cancellation via watch channel
├── perf.rs      — PID controller; shared rate limiter swap; PerfResult
└── stats.rs     — Outcome MPSC collector; Welford online mean; RunReport; progress bar
```

---

## License

MIT

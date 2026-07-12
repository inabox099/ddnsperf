# ddnsperf Phase 2: Benchmark Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn the single-transaction MVP into a real benchmark tool: generate records from a CIDR network, run N concurrent Tokio tasks at a controlled RPS, collect aggregated stats (min/mean/max latency, throughput, error rate), display a live progress bar, and print a full report.

**Architecture:** Add `engine.rs` (task pool + rate limiter) and `records.rs` (CIDR-based record generation). Extend `cli.rs` with all load-control and record-generation flags. Refactor `stats.rs` to handle per-outcome streaming aggregation via MPSC channel instead of the current single-TxResult struct. Wire everything in `main.rs`.

**Tech Stack:** Rust 2021, hickory-client 0.24 (existing), tokio 1 (existing), governor 0.6 (new — token bucket rate limiter), indicatif 0.17 (new — progress bar), ipnet 0.9 (new — CIDR parsing/iteration)

## Global Constraints

- Rust edition: 2021
- governor: "0.6"
- indicatif: "0.17"
- ipnet: "0.9"
- All existing public types in `dns.rs` and `stats.rs` must remain compilable — refactor, don't delete
- Integration tests remain `#[ignore]`; unit tests must pass without a live DNS server
- `cargo test` (non-ignored) must pass at the end of every task

---

## File Map

| File | Change | Responsibility |
|---|---|---|
| `Cargo.toml` | Modify | Add governor, indicatif, ipnet |
| `src/records.rs` | Create | CIDR-based `(hostname, ip, ptr_name)` triple generation (sequential + random) |
| `src/stats.rs` | Rewrite | `Outcome` type, `StatsCollector` (MPSC receiver task), `RunReport` (final aggregation), `print_run_report` |
| `src/engine.rs` | Create | `run_benchmark(config, stats_tx)` — task pool, Governor rate limiter, dispatch loop |
| `src/cli.rs` | Modify | Add all Phase 2 flags; `Config` gains `BenchmarkConfig` sub-struct |
| `src/main.rs` | Modify | Route through `engine::run_benchmark`; drive progress bar |

---

### Task 1: Add dependencies + records module

**Files:**
- Modify: `Cargo.toml`
- Create: `src/records.rs`
- Modify: `src/main.rs` (add `mod records;`)

**Interfaces:**
- Produces:
  ```rust
  // src/records.rs
  pub struct DnsRecord {
      pub hostname: hickory_proto::rr::Name,
      pub ip:       std::net::Ipv4Addr,
      pub ptr_name: hickory_proto::rr::Name,
  }

  pub struct RecordGenerator {
      // opaque internals
  }

  impl RecordGenerator {
      /// `network` is a /N IPv4 CIDR; `prefix` is the hostname prefix.
      /// `random` selects random vs sequential mode.
      pub fn new(network: ipnet::Ipv4Net, prefix: String, zone: hickory_proto::rr::Name, random: bool) -> Self;
      /// Returns the next record. Never returns None (wraps around or re-randomises).
      pub fn next(&self) -> DnsRecord;
  }
  ```

- [ ] **Step 1: Add dependencies to Cargo.toml**

```toml
governor  = "0.6"
indicatif = "0.17"
ipnet     = "0.9"
rand      = "0.8"
```

Add these under `[dependencies]` (rand is already a transitive dep but add it explicitly).

- [ ] **Step 2: Write failing unit tests for RecordGenerator**

Create `src/records.rs`:

```rust
use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use hickory_proto::rr::Name;
use ipnet::Ipv4Net;
use rand::Rng;

pub struct DnsRecord {
    pub hostname: Name,
    pub ip:       Ipv4Addr,
    pub ptr_name: Name,
}

pub struct RecordGenerator {
    hosts:   Vec<Ipv4Addr>,
    prefix:  String,
    zone:    Name,
    counter: Arc<AtomicU64>,
    random:  bool,
}

impl RecordGenerator {
    pub fn new(network: Ipv4Net, prefix: String, zone: Name, random: bool) -> Self {
        let hosts: Vec<Ipv4Addr> = network.hosts().collect();
        Self {
            hosts,
            prefix,
            zone,
            counter: Arc::new(AtomicU64::new(0)),
            random,
        }
    }

    pub fn next(&self) -> DnsRecord {
        let ip = if self.random {
            let idx = rand::thread_rng().gen_range(0..self.hosts.len());
            self.hosts[idx]
        } else {
            let idx = (self.counter.fetch_add(1, Ordering::Relaxed) as usize) % self.hosts.len();
            self.hosts[idx]
        };

        let o = ip.octets();
        let ptr_name = Name::from_str_relaxed(
            &format!("{}.{}.{}.{}.in-addr.arpa.", o[3], o[2], o[1], o[0])
        ).expect("valid ptr name");

        let label = format!("{}{}", self.prefix, u32::from(ip));
        let hostname = Name::from_str_relaxed(&format!("{}.{}", label, self.zone))
            .expect("valid hostname");

        DnsRecord { hostname, ip, ptr_name }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn gen() -> RecordGenerator {
        RecordGenerator::new(
            "10.0.0.0/24".parse().unwrap(),
            "host-".to_string(),
            Name::from_str("example.com.").unwrap(),
            false,
        )
    }

    #[test]
    fn sequential_wraps_around() {
        let g = gen();
        // /24 has 254 hosts; after 254 calls index should wrap
        for _ in 0..254 {
            g.next();
        }
        let rec = g.next();
        // should be back to first host (10.0.0.1)
        assert_eq!(rec.ip, "10.0.0.1".parse::<Ipv4Addr>().unwrap());
    }

    #[test]
    fn hostname_contains_prefix() {
        let g = gen();
        let rec = g.next();
        assert!(rec.hostname.to_string().starts_with("host-"));
    }

    #[test]
    fn ptr_name_is_reversed() {
        let g = gen();
        let rec = g.next();
        // 10.0.0.1 -> 1.0.0.10.in-addr.arpa.
        assert!(rec.ptr_name.to_string().ends_with(".in-addr.arpa."));
        let parts: Vec<&str> = rec.ptr_name.to_string().split('.').collect();
        assert_eq!(parts[0], "1");   // last octet first
        assert_eq!(parts[1], "0");
        assert_eq!(parts[2], "0");
        assert_eq!(parts[3], "10");
    }

    #[test]
    fn random_mode_stays_in_subnet() {
        let g = RecordGenerator::new(
            "10.0.0.0/24".parse().unwrap(),
            "h-".to_string(),
            Name::from_str("example.com.").unwrap(),
            true,
        );
        for _ in 0..100 {
            let rec = g.next();
            let octets = rec.ip.octets();
            assert_eq!(octets[0], 10);
            assert_eq!(octets[1], 0);
            assert_eq!(octets[2], 0);
            assert!(octets[3] >= 1 && octets[3] <= 254);
        }
    }
}
```

- [ ] **Step 3: Run tests — expect failure (module not registered)**

```bash
cargo test records::
```

Expected: error — module not found.

- [ ] **Step 4: Register module in main.rs**

Add `mod records;` to `src/main.rs` (after existing `mod` declarations).

- [ ] **Step 5: Run tests — expect pass**

```bash
cargo test records::tests
```

Expected:
```
test records::tests::sequential_wraps_around ... ok
test records::tests::hostname_contains_prefix ... ok
test records::tests::ptr_name_is_reversed ... ok
test records::tests::random_mode_stays_in_subnet ... ok
```

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock src/records.rs src/main.rs
git commit -m "feat(records): CIDR-based record generator with sequential and random modes"
```

---

### Task 2: Refactor stats.rs for streaming aggregation

**Files:**
- Rewrite: `src/stats.rs`

**Interfaces:**
- Consumes: nothing from other new tasks
- Produces:
  ```rust
  pub struct Outcome {
      pub latency_us: u64,
      pub success:    bool,
  }

  pub struct RunReport {
      pub duration:     std::time::Duration,
      pub total_sent:   u64,
      pub total_ok:     u64,
      pub total_err:    u64,
      pub min_us:       u64,
      pub mean_us:      f64,
      pub max_us:       u64,
      pub throughput:   f64,   // RPS over the full run
  }

  /// Spawns a Tokio task that drains `rx` and aggregates outcomes.
  /// Returns a JoinHandle; await it after closing the sender to get RunReport.
  pub fn spawn_collector(
      rx: tokio::sync::mpsc::UnboundedReceiver<Outcome>,
      start: std::time::Instant,
  ) -> tokio::task::JoinHandle<RunReport>

  pub fn print_run_report(report: &RunReport);
  ```

  Backward-compat: keep `TxResult` and `print_report` (used by tests and old main path).

- [ ] **Step 1: Write failing tests for new stats types**

```rust
// Tests to add inside src/stats.rs under #[cfg(test)]

#[test]
fn run_report_error_rate() {
    let r = RunReport {
        duration:   std::time::Duration::from_secs(1),
        total_sent: 100,
        total_ok:   95,
        total_err:  5,
        min_us:     100,
        mean_us:    500.0,
        max_us:     2000,
        throughput: 100.0,
    };
    let pct = r.total_err as f64 / r.total_sent as f64 * 100.0;
    assert!((pct - 5.0).abs() < 0.001);
}

#[test]
fn run_report_success_rate() {
    let r = RunReport {
        duration:   std::time::Duration::from_secs(1),
        total_sent: 200,
        total_ok:   200,
        total_err:  0,
        min_us:     50,
        mean_us:    200.0,
        max_us:     800,
        throughput: 200.0,
    };
    assert_eq!(r.total_err, 0);
    assert_eq!(r.total_ok, r.total_sent);
}
```

Run: `cargo test stats::tests` — expect failure (types don't exist yet).

- [ ] **Step 2: Rewrite src/stats.rs**

```rust
// src/stats.rs
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task::JoinHandle;

// ── Streaming types ──────────────────────────────────────────────────────────

pub struct Outcome {
    pub latency_us: u64,
    pub success:    bool,
}

pub struct RunReport {
    pub duration:   Duration,
    pub total_sent: u64,
    pub total_ok:   u64,
    pub total_err:  u64,
    pub min_us:     u64,
    pub mean_us:    f64,
    pub max_us:     u64,
    pub throughput: f64, // RPS over full run
}

/// Spawns a collector task. Drain `rx` until sender is dropped, then return RunReport.
pub fn spawn_collector(
    mut rx: UnboundedReceiver<Outcome>,
    start: Instant,
) -> JoinHandle<RunReport> {
    tokio::spawn(async move {
        let mut total_sent: u64 = 0;
        let mut total_ok:   u64 = 0;
        let mut total_err:  u64 = 0;
        let mut min_us: u64 = u64::MAX;
        let mut max_us: u64 = 0;
        // Welford's online mean
        let mut mean: f64 = 0.0;

        while let Some(o) = rx.recv().await {
            total_sent += 1;
            if o.success { total_ok += 1; } else { total_err += 1; }
            if o.latency_us < min_us { min_us = o.latency_us; }
            if o.latency_us > max_us { max_us = o.latency_us; }
            // Welford update
            let delta = o.latency_us as f64 - mean;
            mean += delta / total_sent as f64;
        }

        let duration = start.elapsed();
        let throughput = if duration.as_secs_f64() > 0.0 {
            total_sent as f64 / duration.as_secs_f64()
        } else {
            0.0
        };

        RunReport {
            duration,
            total_sent,
            total_ok,
            total_err,
            min_us:   if total_sent == 0 { 0 } else { min_us },
            mean_us:  mean,
            max_us,
            throughput,
        }
    })
}

pub fn print_run_report(r: &RunReport) {
    let secs = r.duration.as_secs_f64();
    println!("=== ddnsperf results ===");
    println!("Duration:      {:.3}s", secs);
    println!("Total sent:    {}", r.total_sent);
    println!("  Successful:  {} ({:.1}%)", r.total_ok,
        pct(r.total_ok, r.total_sent));
    println!("  Errors:      {} ({:.1}%)", r.total_err,
        pct(r.total_err, r.total_sent));
    println!("Throughput:    {:.0} RPS", r.throughput);
    println!("Latency:");
    println!("  Min:         {:.3}ms", r.min_us as f64 / 1000.0);
    println!("  Mean:        {:.3}ms", r.mean_us / 1000.0);
    println!("  Max:         {:.3}ms", r.max_us as f64 / 1000.0);
}

fn pct(n: u64, total: u64) -> f64 {
    if total == 0 { 0.0 } else { n as f64 / total as f64 * 100.0 }
}

// ── Legacy single-transaction types (kept for backward compat) ───────────────

pub struct TxResult {
    pub add_a_latency:   Duration,
    pub add_ptr_latency: Duration,
    pub del_ptr_latency: Duration,
    pub del_a_latency:   Duration,
}

impl TxResult {
    pub fn total(&self) -> Duration {
        self.add_a_latency + self.add_ptr_latency
            + self.del_ptr_latency + self.del_a_latency
    }
}

pub fn print_report(result: &TxResult) {
    println!("=== ddnsperf transaction result ===");
    println!("  Add A:      {:>8.3}ms", ms(result.add_a_latency));
    println!("  Add PTR:    {:>8.3}ms", ms(result.add_ptr_latency));
    println!("  Delete PTR: {:>8.3}ms", ms(result.del_ptr_latency));
    println!("  Delete A:   {:>8.3}ms", ms(result.del_a_latency));
    println!("  -----------");
    println!("  Total:      {:>8.3}ms", ms(result.total()));
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn total_sums_all_legs() {
        let r = TxResult {
            add_a_latency:   Duration::from_millis(1),
            add_ptr_latency: Duration::from_millis(2),
            del_ptr_latency: Duration::from_millis(3),
            del_a_latency:   Duration::from_millis(4),
        };
        assert_eq!(r.total(), Duration::from_millis(10));
    }

    #[test]
    fn run_report_error_rate() {
        let r = RunReport {
            duration:   Duration::from_secs(1),
            total_sent: 100,
            total_ok:   95,
            total_err:  5,
            min_us:     100,
            mean_us:    500.0,
            max_us:     2000,
            throughput: 100.0,
        };
        let pct = r.total_err as f64 / r.total_sent as f64 * 100.0;
        assert!((pct - 5.0).abs() < 0.001);
    }

    #[test]
    fn run_report_success_rate() {
        let r = RunReport {
            duration:   Duration::from_secs(1),
            total_sent: 200,
            total_ok:   200,
            total_err:  0,
            min_us:     50,
            mean_us:    200.0,
            max_us:     800,
            throughput: 200.0,
        };
        assert_eq!(r.total_err, 0);
        assert_eq!(r.total_ok, r.total_sent);
    }

    #[tokio::test]
    async fn collector_counts_outcomes() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let start = Instant::now();
        let handle = spawn_collector(rx, start);

        for i in 0..10u64 {
            tx.send(Outcome { latency_us: (i + 1) * 100, success: i < 8 }).unwrap();
        }
        drop(tx); // signal done

        let report = handle.await.unwrap();
        assert_eq!(report.total_sent, 10);
        assert_eq!(report.total_ok,   8);
        assert_eq!(report.total_err,  2);
        assert_eq!(report.min_us,   100);
        assert_eq!(report.max_us,  1000);
        // mean of 100,200,...,1000 = 550
        assert!((report.mean_us - 550.0).abs() < 1.0);
    }
}
```

- [ ] **Step 3: Run tests**

```bash
cargo test stats::tests
```

Expected:
```
test stats::tests::total_sums_all_legs ... ok
test stats::tests::run_report_error_rate ... ok
test stats::tests::run_report_success_rate ... ok
test stats::tests::collector_counts_outcomes ... ok
```

- [ ] **Step 4: Commit**

```bash
git add src/stats.rs
git commit -m "feat(stats): streaming Outcome collector with Welford mean; backward-compat TxResult kept"
```

---

### Task 3: Engine module

**Files:**
- Create: `src/engine.rs`
- Modify: `src/main.rs` (add `mod engine;`)

**Interfaces:**
- Consumes:
  - `dns::TsigConfig`, `dns::run_transaction` from `src/dns.rs`
  - `records::RecordGenerator` from `src/records.rs`
  - `stats::Outcome`, `stats::spawn_collector` from `src/stats.rs`
- Produces:
  ```rust
  pub struct BenchmarkConfig {
      pub server:       std::net::SocketAddr,
      pub zone:         hickory_proto::rr::Name,
      pub ptr_zone:     hickory_proto::rr::Name,
      pub generator:    std::sync::Arc<crate::records::RecordGenerator>,
      pub tsig:         Option<crate::dns::TsigConfig>,
      pub concurrency:  usize,
      pub total:        Option<u64>,        // None = run until duration elapses
      pub rps:          Option<u32>,        // None = unlimited
  }

  /// Runs the benchmark. Returns RunReport when done.
  pub async fn run_benchmark(
      cfg: BenchmarkConfig,
      progress: indicatif::ProgressBar,
  ) -> crate::stats::RunReport
  ```

- [ ] **Step 1: Create src/engine.rs with a unit test for the counter logic**

```rust
// src/engine.rs
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use governor::{Quota, RateLimiter};
use governor::clock::DefaultClock;
use governor::state::{InMemoryState, NotKeyed};
use indicatif::ProgressBar;
use tokio::sync::mpsc::unbounded_channel;
use nonzero_ext::nonzero;

use crate::dns::TsigConfig;
use crate::records::RecordGenerator;
use crate::stats::{Outcome, RunReport, spawn_collector};

pub struct BenchmarkConfig {
    pub server:      std::net::SocketAddr,
    pub zone:        hickory_proto::rr::Name,
    pub ptr_zone:    hickory_proto::rr::Name,
    pub generator:   Arc<RecordGenerator>,
    pub tsig:        Option<Arc<TsigConfig>>,
    pub concurrency: usize,
    pub total:       Option<u64>,
    pub rps:         Option<u32>,
}

type Limiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

pub async fn run_benchmark(cfg: BenchmarkConfig, progress: ProgressBar) -> RunReport {
    let (tx, rx) = unbounded_channel::<Outcome>();
    let start = Instant::now();
    let collector = spawn_collector(rx, start);

    // Build optional rate limiter
    let limiter: Option<Arc<Limiter>> = cfg.rps.map(|r| {
        Arc::new(RateLimiter::direct(
            Quota::per_second(nonzero_ext::NonZero::new(r).unwrap())
        ))
    });

    let sent = Arc::new(AtomicU64::new(0));
    let total = cfg.total.unwrap_or(u64::MAX);

    let mut handles = Vec::with_capacity(cfg.concurrency);

    for _ in 0..cfg.concurrency {
        let tx        = tx.clone();
        let gen       = cfg.generator.clone();
        let zone      = cfg.zone.clone();
        let ptr_zone  = cfg.ptr_zone.clone();
        let server    = cfg.server;
        let limiter   = limiter.clone();
        let sent      = sent.clone();
        let tsig_arc  = cfg.tsig.clone();
        let pb        = progress.clone();

        handles.push(tokio::spawn(async move {
            loop {
                // Check if we've hit the total
                let n = sent.fetch_add(1, Ordering::Relaxed);
                if n >= total {
                    // Put the count back so other tasks see correct total
                    sent.fetch_sub(1, Ordering::Relaxed);
                    break;
                }

                // Rate limiting
                if let Some(ref lim) = limiter {
                    lim.until_ready().await;
                }

                let rec = gen.next();

                // Clone TSIG config for this request (TsigConfig is not Clone, wrap in Option)
                let tsig = tsig_arc.as_ref().map(|t| crate::dns::TsigConfig {
                    key_name:  t.key_name.clone(),
                    algorithm: t.algorithm.clone(),
                    secret:    t.secret.clone(),
                });

                let t0 = Instant::now();
                let result = crate::dns::run_transaction(
                    server,
                    zone.clone(),
                    ptr_zone.clone(),
                    rec.hostname,
                    rec.ip,
                    tsig,
                ).await;
                let latency_us = t0.elapsed().as_micros() as u64;

                let success = result.is_ok();
                let _ = tx.send(Outcome { latency_us, success });
                pb.inc(1);
            }
        }));
    }

    // Wait for all tasks to finish
    for h in handles {
        let _ = h.await;
    }

    // Drop tx so collector sees EOF
    drop(tx);
    progress.finish_with_message("done");

    collector.await.expect("stats collector panicked")
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    /// Verifies the fetch_add / guard logic: exactly `total` increments should succeed.
    #[test]
    fn counter_stops_at_total() {
        let sent  = Arc::new(AtomicU64::new(0));
        let total: u64 = 5;
        let mut accepted = 0u64;

        for _ in 0..10 {
            let n = sent.fetch_add(1, Ordering::Relaxed);
            if n >= total {
                sent.fetch_sub(1, Ordering::Relaxed);
                break;
            }
            accepted += 1;
        }

        assert_eq!(accepted, total);
    }
}
```

- [ ] **Step 2: Add `nonzero-ext` to Cargo.toml** (governor requires it)

```toml
nonzero-ext = "0.3"
```

- [ ] **Step 3: Register engine module in main.rs**

Add `mod engine;` to `src/main.rs`.

- [ ] **Step 4: Make TsigConfig cloneable**

`TsigConfig` needs `Clone` because the engine clones it per task. Edit `src/dns.rs`:

```rust
#[derive(Clone)]
pub struct TsigConfig {
    pub key_name:  Name,
    pub algorithm: TsigAlgorithm,
    pub secret:    Vec<u8>,
}
```

Also derive `Clone` on `TsigAlgorithm` — check if it already derives it:
```bash
grep -n "derive.*Clone.*TsigAlgorithm\|TsigAlgorithm.*Clone" \
  ~/.cargo/registry/src/index.crates.io-1949cf8c6b5b557f/hickory-proto-0.24.4/src/rr/dnssec/rdata/tsig.rs
```

If `TsigAlgorithm` already derives `Clone` (it does — it's `#[derive(Clone, ...)]`), just add `#[derive(Clone)]` to `TsigConfig`.

- [ ] **Step 5: Verify it compiles**

```bash
cargo build
```

Expected: compiles. Fix any import errors (e.g. `nonzero_ext::NonZero` vs `std::num::NonZeroU32`).

If `nonzero_ext` import is awkward, replace with:
```rust
use std::num::NonZeroU32;
// ...
Quota::per_second(NonZeroU32::new(r).unwrap())
```

- [ ] **Step 6: Run unit tests**

```bash
cargo test engine::tests records::tests stats::tests cli::tests
```

Expected: all pass.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml Cargo.lock src/engine.rs src/main.rs src/dns.rs
git commit -m "feat(engine): concurrent task pool with Governor rate limiter"
```

---

### Task 4: Extend CLI for benchmark mode

**Files:**
- Modify: `src/cli.rs`

**Interfaces:**
- Consumes: `records::RecordGenerator`, `engine::BenchmarkConfig`, `dns::TsigConfig`
- Produces:
  ```rust
  // Added to Config:
  pub struct Config {
      // existing fields kept unchanged for single-tx path
      pub server:   std::net::SocketAddr,
      pub zone:     hickory_proto::rr::Name,
      pub ptr_zone: Option<hickory_proto::rr::Name>,  // now Optional
      pub hostname: Option<hickory_proto::rr::Name>,  // now Optional
      pub ip:       Option<std::net::Ipv4Addr>,       // now Optional
      pub tsig:     Option<crate::dns::TsigConfig>,
      // new benchmark fields
      pub network:      Option<ipnet::Ipv4Net>,
      pub prefix:       String,
      pub mode:         Mode,
      pub requests:     Option<u64>,
      pub concurrency:  usize,
      pub rps:          Option<u32>,
  }

  pub enum Mode { Sequential, Random }
  ```

- [ ] **Step 1: Add new CLI args and update into_config**

Replace the contents of `src/cli.rs` with:

```rust
use clap::Parser;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Mode {
    Sequential,
    Random,
}

#[derive(Parser, Debug)]
#[command(name = "ddnsperf", about = "DDNS Update performance tool")]
pub struct Args {
    // ── Required ──────────────────────────────────────────────────────────
    /// DNS server address, e.g. 192.168.1.1:53 or [2001:db8::1]:53
    #[arg(short = 's', long)]
    pub server: String,

    /// DNS forward zone, e.g. example.com.
    #[arg(short = 'z', long)]
    pub zone: String,

    // ── Record source: either --network (benchmark) or explicit --hostname/--ip ──
    /// Subnet to generate records from, e.g. 10.0.0.0/24  [benchmark mode]
    #[arg(long, conflicts_with_all = ["hostname", "ip"])]
    pub network: Option<String>,

    /// Hostname prefix used with --network (default: "host-")
    #[arg(long, default_value = "host-")]
    pub prefix: String,

    /// Record selection: sequential | random (used with --network)
    #[arg(long, default_value = "sequential")]
    pub mode: String,

    /// Reverse DNS zone (required without --network, inferred from --network if omitted)
    #[arg(long)]
    pub ptr_zone: Option<String>,

    /// Single hostname FQDN (single-shot mode, requires --ip)
    #[arg(long, conflicts_with = "network", requires = "ip")]
    pub hostname: Option<String>,

    /// Single IPv4 address (single-shot mode, requires --hostname)
    #[arg(long, conflicts_with = "network", requires = "hostname")]
    pub ip: Option<String>,

    // ── Load control ──────────────────────────────────────────────────────
    /// Total number of transactions to send [benchmark mode]
    #[arg(short = 'r', long)]
    pub requests: Option<u64>,

    /// Target transactions per second (omit for unlimited)
    #[arg(long)]
    pub rps: Option<u32>,

    /// Number of concurrent Tokio tasks (default: 50)
    #[arg(short = 'c', long, default_value_t = 50)]
    pub concurrency: usize,

    // ── TSIG ──────────────────────────────────────────────────────────────
    /// TSIG key name (requires --tsig-secret)
    #[arg(long, requires = "tsig_secret")]
    pub tsig_name: Option<String>,

    /// TSIG secret, base64-encoded (requires --tsig-name)
    #[arg(long, requires = "tsig_name")]
    pub tsig_secret: Option<String>,

    /// TSIG algorithm: hmac-md5 | hmac-sha1 | hmac-sha256
    #[arg(long, default_value = "hmac-sha256")]
    pub tsig_algo: String,
}

pub struct Config {
    pub server:      std::net::SocketAddr,
    pub zone:        hickory_proto::rr::Name,
    pub ptr_zone:    Option<hickory_proto::rr::Name>,
    pub hostname:    Option<hickory_proto::rr::Name>,
    pub ip:          Option<std::net::Ipv4Addr>,
    pub tsig:        Option<crate::dns::TsigConfig>,
    pub network:     Option<ipnet::Ipv4Net>,
    pub prefix:      String,
    pub mode:        Mode,
    pub requests:    Option<u64>,
    pub concurrency: usize,
    pub rps:         Option<u32>,
}

impl Args {
    pub fn into_config(self) -> Result<Config, String> {
        use std::str::FromStr;
        use hickory_proto::rr::Name;
        use hickory_proto::rr::dnssec::rdata::tsig::TsigAlgorithm;
        use base64::Engine as _;

        let server = self.server.parse::<std::net::SocketAddr>()
            .map_err(|e| format!("invalid --server: {}", e))?;

        let zone = Name::from_str(&self.zone)
            .map_err(|e| format!("invalid --zone: {}", e))?;

        let ptr_zone = self.ptr_zone.as_deref()
            .map(|s| Name::from_str(s).map_err(|e| format!("invalid --ptr-zone: {}", e)))
            .transpose()?;

        let hostname = self.hostname.as_deref()
            .map(|s| Name::from_str(s).map_err(|e| format!("invalid --hostname: {}", e)))
            .transpose()?;

        let ip = self.ip.as_deref()
            .map(|s| s.parse::<std::net::Ipv4Addr>().map_err(|e| format!("invalid --ip: {}", e)))
            .transpose()?;

        let network = self.network.as_deref()
            .map(|s| s.parse::<ipnet::Ipv4Net>().map_err(|e| format!("invalid --network: {}", e)))
            .transpose()?;

        let mode = match self.mode.as_str() {
            "sequential" => Mode::Sequential,
            "random"     => Mode::Random,
            other        => return Err(format!("invalid --mode: '{}' (sequential|random)", other)),
        };

        // Validate: need either --network or (--hostname + --ip)
        if network.is_none() && (hostname.is_none() || ip.is_none()) {
            return Err("provide either --network or both --hostname and --ip".to_string());
        }

        let tsig = match (self.tsig_name, self.tsig_secret) {
            (Some(name), Some(secret)) => {
                let key_name = Name::from_str(&name)
                    .map_err(|e| format!("invalid --tsig-name: {}", e))?;
                let algorithm = match self.tsig_algo.as_str() {
                    "hmac-md5"    => TsigAlgorithm::HmacMd5,
                    "hmac-sha1"   => TsigAlgorithm::HmacSha1,
                    "hmac-sha256" => TsigAlgorithm::HmacSha256,
                    other => return Err(format!("unknown --tsig-algo: '{}'", other)),
                };
                let raw = base64::engine::general_purpose::STANDARD
                    .decode(&secret)
                    .map_err(|e| format!("invalid --tsig-secret (base64): {}", e))?;
                Some(crate::dns::TsigConfig { key_name, algorithm, secret: raw })
            }
            (None, None) => None,
            _ => unreachable!("clap enforces tsig_name and tsig_secret together"),
        };

        Ok(Config {
            server, zone, ptr_zone, hostname, ip, tsig,
            network, prefix: self.prefix, mode,
            requests: self.requests,
            concurrency: self.concurrency,
            rps: self.rps,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_args() -> Args {
        Args {
            server:      "127.0.0.1:53".to_string(),
            zone:        "example.com.".to_string(),
            ptr_zone:    None,
            network:     Some("10.0.0.0/24".to_string()),
            prefix:      "host-".to_string(),
            mode:        "sequential".to_string(),
            hostname:    None,
            ip:          None,
            requests:    Some(100),
            rps:         None,
            concurrency: 50,
            tsig_name:   None,
            tsig_secret: None,
            tsig_algo:   "hmac-sha256".to_string(),
        }
    }

    #[test]
    fn network_mode_parses() {
        let cfg = base_args().into_config().expect("should parse");
        assert!(cfg.network.is_some());
        assert_eq!(cfg.concurrency, 50);
    }

    #[test]
    fn single_shot_mode_parses() {
        let mut a = base_args();
        a.network  = None;
        a.hostname = Some("host.example.com.".to_string());
        a.ip       = Some("10.0.0.1".to_string());
        a.ptr_zone = Some("0.0.10.in-addr.arpa.".to_string());
        let cfg = a.into_config().expect("should parse");
        assert!(cfg.hostname.is_some());
        assert!(cfg.ip.is_some());
    }

    #[test]
    fn neither_network_nor_hostname_errors() {
        let mut a = base_args();
        a.network  = None;
        a.hostname = None;
        a.ip       = None;
        assert!(a.into_config().is_err());
    }

    #[test]
    fn invalid_ip_returns_error() {
        let mut a = base_args();
        a.network  = None;
        a.hostname = Some("h.example.com.".to_string());
        a.ip       = Some("not-an-ip".to_string());
        assert!(a.into_config().is_err());
    }

    #[test]
    fn unknown_tsig_algo_returns_error() {
        let mut a = base_args();
        a.tsig_name   = Some("key.".to_string());
        a.tsig_secret = Some("aGVsbG8=".to_string());
        a.tsig_algo   = "hmac-sha512".to_string();
        assert!(a.into_config().is_err());
    }

    #[test]
    fn invalid_mode_returns_error() {
        let mut a = base_args();
        a.mode = "zigzag".to_string();
        assert!(a.into_config().is_err());
    }
}
```

- [ ] **Step 2: Run CLI tests**

```bash
cargo test cli::tests
```

Expected: 6 tests pass.

- [ ] **Step 3: Commit**

```bash
git add src/cli.rs
git commit -m "feat(cli): add network/prefix/mode/requests/rps/concurrency flags for benchmark mode"
```

---

### Task 5: Wire engine into main + progress bar

**Files:**
- Modify: `src/main.rs`

**Interfaces:**
- Consumes: `cli::Config`, `cli::Mode`, `engine::BenchmarkConfig`, `records::RecordGenerator`, `stats::print_run_report`, `stats::print_report` (single-shot)

- [ ] **Step 1: Rewrite main.rs**

```rust
mod cli;
mod dns;
mod engine;
mod records;
mod stats;

use std::sync::Arc;
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};

#[tokio::main]
async fn main() {
    let args = cli::Args::parse();
    let config = match args.into_config() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("error: {}", e);
            std::process::exit(1);
        }
    };

    // ── Benchmark mode: --network provided ───────────────────────────────
    if let Some(network) = config.network {
        let ptr_zone = config.ptr_zone.unwrap_or_else(|| {
            // Derive reverse zone from network (e.g. 10.0.0.0/24 -> 0.0.10.in-addr.arpa.)
            let octets = network.network().octets();
            let prefix_len = network.prefix_len();
            let zone_str = match prefix_len {
                 0..=7  => "in-addr.arpa.".to_string(),
                 8..=15 => format!("{}.in-addr.arpa.", octets[0]),
                16..=23 => format!("{}.{}.in-addr.arpa.", octets[1], octets[0]),
                _       => format!("{}.{}.{}.in-addr.arpa.", octets[2], octets[1], octets[0]),
            };
            hickory_proto::rr::Name::from_str_relaxed(&zone_str)
                .expect("derived ptr zone is valid")
        });

        let generator = Arc::new(records::RecordGenerator::new(
            network,
            config.prefix,
            config.zone.clone(),
            config.mode == cli::Mode::Random,
        ));

        let total = config.requests;

        let pb = if let Some(n) = total {
            let pb = ProgressBar::new(n);
            pb.set_style(
                ProgressStyle::with_template(
                    "[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} RPS:{per_sec} Errors: η"
                )
                .unwrap()
                .progress_chars("█░"),
            );
            pb
        } else {
            let pb = ProgressBar::new_spinner();
            pb.set_style(
                ProgressStyle::with_template("[{elapsed_precise}] {spinner} {pos} sent")
                    .unwrap(),
            );
            pb
        };

        let tsig_arc = config.tsig.map(Arc::new);

        let bench_cfg = engine::BenchmarkConfig {
            server:      config.server,
            zone:        config.zone,
            ptr_zone,
            generator,
            tsig:        tsig_arc,
            concurrency: config.concurrency,
            total,
            rps:         config.rps,
        };

        let report = engine::run_benchmark(bench_cfg, pb).await;
        stats::print_run_report(&report);

    // ── Single-shot mode: --hostname + --ip provided ──────────────────────
    } else {
        let hostname = config.hostname.expect("hostname required in single-shot mode");
        let ip       = config.ip.expect("ip required in single-shot mode");
        let ptr_zone = config.ptr_zone.unwrap_or_else(|| dns::ipv4_to_ptr_name(ip)
            .parent().expect("ptr name has parent")
            .parent().expect("ptr name has grandparent"));

        match dns::run_transaction(
            config.server,
            config.zone,
            ptr_zone,
            hostname,
            ip,
            config.tsig,
        ).await {
            Ok(result) => stats::print_report(&result),
            Err(e) => {
                eprintln!("error: {}", e);
                std::process::exit(1);
            }
        }
    }
}
```

- [ ] **Step 2: Build**

```bash
cargo build
```

Fix any compilation errors. Common issues:
- `ipv4_to_ptr_name` needs to be `pub` in `dns.rs` (it already is)
- `hickory_proto::rr::Name::from_str_relaxed` — already used in `dns.rs`, same API
- If `ptr_zone` derivation for single-shot is awkward, simplify to just require `--ptr-zone` when `--hostname` is used (add `requires = "ptr_zone"` to `--hostname` in `Args`)

- [ ] **Step 3: Run all unit tests**

```bash
cargo test
```

Expected: all non-ignored tests pass.

- [ ] **Step 4: Manual smoke test — single-shot (against live BIND)**

```bash
./target/debug/ddnsperf \
  --server 127.0.0.1:5353 \
  --zone test.local. \
  --ptr-zone 0.0.10.in-addr.arpa. \
  --hostname smoke2.test.local. \
  --ip 10.0.0.44 \
  --tsig-name test-key. \
  --tsig-secret "i8ThSp4D0SYlHviiQCCxAs4qgZtisWG845b7ttlCT6w="
```

Expected: same per-leg output as before.

- [ ] **Step 5: Manual smoke test — benchmark mode (against live BIND)**

```bash
./target/debug/ddnsperf \
  --server 127.0.0.1:5353 \
  --zone test.local. \
  --network 10.0.0.0/24 \
  --requests 20 \
  --concurrency 4 \
  --tsig-name test-key. \
  --tsig-secret "i8ThSp4D0SYlHviiQCCxAs4qgZtisWG845b7ttlCT6w="
```

Expected: progress bar runs, then report:
```
=== ddnsperf results ===
Duration:      ...s
Total sent:    20
  Successful:  20 (100.0%)
  Errors:      0  (0.0%)
Throughput:    ... RPS
Latency:
  Min:         ...ms
  Mean:        ...ms
  Max:         ...ms
```

- [ ] **Step 6: Commit**

```bash
git add src/main.rs
git commit -m "feat(main): benchmark mode with progress bar and RunReport; single-shot mode preserved"
```

---

## Self-review checklist

- No TBDs or placeholders ✓
- `TsigConfig` derives `Clone` before engine task 3 uses it ✓
- `ptr_zone` derivation in `main.rs` handles /8, /16, /24 correctly ✓
- `RecordGenerator::next` never panics on empty subnet — `Ipv4Net::hosts()` always yields ≥1 host for /31 and larger ✓
- `spawn_collector` returns `JoinHandle<RunReport>` not `JoinHandle<()>` ✓
- `engine.rs` drops `tx` before awaiting collector ✓
- All imports consistent across tasks ✓

## Open questions

- For unlimited mode (no `--requests`), the benchmark runs until Ctrl-C (SIGINT); Tokio's default SIGINT handling will terminate gracefully. If clean shutdown is needed, add a `tokio::signal::ctrl_c()` select arm in `engine.rs` — deferred to Phase 3.
- The `ptr_zone` auto-derivation in single-shot mode via `.parent().parent()` of the PTR name is fragile. Simplest fix: require `--ptr-zone` with `--hostname` in clap (add `requires = "ptr_zone"` to the `--hostname` arg definition).

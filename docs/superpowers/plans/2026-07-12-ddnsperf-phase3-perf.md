# ddnsperf Phase 3: Duration, Transport Flags, PID Perf Test

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Complete the benchmark feature set: time-bounded runs (`--duration`), TCP and forced-IP-version transport (`--tcp`/`--ipv4`/`--ipv6`), and a PID-controlled perf test that finds the server's maximum sustainable throughput (`--perf-test` / `--rps auto`).

**Architecture:** Add a `cancel` watch channel to `engine::BenchmarkConfig` for duration-bounded runs. Extend `dns.rs` with a `TransportConfig` struct controlling UDP/TCP and IPv4/IPv6 bind. Add `perf.rs` with a PID loop that wraps the engine with a dynamically-adjustable `Arc<RwLock<Arc<Limiter>>>`. Wire everything through updated `cli.rs` and `main.rs`.

**Tech Stack:** Existing stack only — no new dependencies. `tokio::sync::watch` (cancellation + rate broadcast), `tokio::sync::RwLock` (shared rate limiter swap), `hickory_client::tcp::TcpClientConnection` (TCP transport).

## Global Constraints

- Rust edition: 2021
- No new crate dependencies
- All existing unit tests must continue to pass
- Integration tests stay `#[ignore]`
- `cargo test` (non-ignored) green at end of every task

---

## File Map

| File | Change | Responsibility |
|---|---|---|
| `src/dns.rs` | Modify | Add `TransportConfig`; accept it in `run_transaction` |
| `src/engine.rs` | Modify | Add `cancel: watch::Receiver<bool>` to `BenchmarkConfig`; swap rate limiter via `Arc<RwLock>` |
| `src/perf.rs` | Create | PID controller; drives `run_benchmark` with dynamic RPS |
| `src/cli.rs` | Modify | Add `--duration`, `--tcp`/`--udp`, `--ipv4`/`--ipv6`, `--perf-test`, `--error-target`, `--max-rps` flags |
| `src/main.rs` | Modify | Route to `perf::run_perf_test` or `engine::run_benchmark`; spawn duration canceller |

---

### Task 1: Duration support (cancellation)

**Files:**
- Modify: `src/engine.rs`

**Interfaces:**
- Consumes: `tokio::sync::watch` (already in tokio)
- Produces:
  ```rust
  // BenchmarkConfig gains:
  pub cancel: tokio::sync::watch::Receiver<bool>,
  // (true = stop now)
  ```

The task loop checks `*cancel.borrow()` each iteration and breaks when `true`. A `cancel` where the sender is never triggered behaves identically to the current always-run logic.

- [ ] **Step 1: Write a unit test for cancellation**

Add to `src/engine.rs` tests:

```rust
#[tokio::test]
async fn cancel_receiver_borrow_false_by_default() {
    let (_tx, rx) = tokio::sync::watch::channel(false);
    assert!(!*rx.borrow());
}

#[tokio::test]
async fn cancel_receiver_sees_true_after_send() {
    let (tx, rx) = tokio::sync::watch::channel(false);
    tx.send(true).unwrap();
    assert!(*rx.borrow());
}
```

- [ ] **Step 2: Run tests to confirm they fail (field doesn't exist yet)**

```bash
cargo test engine::tests
```

Expected: compile error — `cancel` field missing.

- [ ] **Step 3: Add `cancel` field and check in task loop**

In `src/engine.rs`, update `BenchmarkConfig`:
```rust
pub cancel: tokio::sync::watch::Receiver<bool>,
```

Update `run_benchmark` signature (no change needed) and the task loop — replace:
```rust
loop {
    let n = sent.fetch_add(1, Ordering::Relaxed);
    if n >= total {
```
with:
```rust
loop {
    if *cancel.borrow() { break; }
    let n = sent.fetch_add(1, Ordering::Relaxed);
    if n >= total {
```

Also clone `cancel` per task:
```rust
let cancel = cancel.clone();
```
(add this alongside the other per-task clones before `tokio::spawn`)

Full updated `run_benchmark` signature:
```rust
pub async fn run_benchmark(cfg: BenchmarkConfig, progress: ProgressBar) -> RunReport {
```
No signature change — `cancel` is inside `cfg`.

- [ ] **Step 4: Fix callers in main.rs**

`BenchmarkConfig` construction in `main.rs` needs the new field. Add:
```rust
let (_cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
// ... then in BenchmarkConfig:
cancel: cancel_rx,
```

For duration support, replace `let (_cancel_tx, cancel_rx)` with:
```rust
let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);

if let Some(secs) = config.duration {
    tokio::spawn(async move {
        tokio::time::sleep(std::time::Duration::from_secs(secs)).await;
        let _ = cancel_tx.send(true);
    });
}
// else: cancel_tx is dropped immediately; no cancellation ever fires
```

- [ ] **Step 5: Add `duration` to `cli::Config` and `cli::Args`**

In `src/cli.rs`, add to `Args`:
```rust
/// Run for this many seconds (mutually exclusive with --requests)
#[arg(long, conflicts_with = "requests")]
pub duration: Option<u64>,
```

Add to `Config`:
```rust
pub duration: Option<u64>,
```

Add to `Args::into_config`:
```rust
duration: self.duration,
```

Add a CLI test:
```rust
#[test]
fn duration_and_requests_conflict() {
    // clap enforces conflicts_with at parse time, not into_config
    // just verify duration parses into Config
    let mut a = base_args();
    a.requests = None;
    // duration is added to Args; set it to Some(30)
    // (construct Args directly since clap conflict is parse-time only)
    // verify Config gets the value
    let cfg = a.into_config().expect("should parse");
    assert_eq!(cfg.duration, None); // base_args has no duration
}
```

- [ ] **Step 6: Run all tests**

```bash
cargo test
```

Expected: 17+ tests pass, 3 ignored.

- [ ] **Step 7: Commit**

```bash
git add src/engine.rs src/cli.rs src/main.rs
git commit -m "feat(engine,cli): duration-bounded runs via cancellation watch channel"
```

---

### Task 2: Transport flags (TCP, IPv4/IPv6 forcing)

**Files:**
- Modify: `src/dns.rs`
- Modify: `src/cli.rs`
- Modify: `src/main.rs`

**Interfaces:**
- Produces:
  ```rust
  // src/dns.rs
  #[derive(Clone, Debug)]
  pub enum Transport { Udp, Tcp }

  #[derive(Clone, Debug)]
  pub enum IpVersion { Auto, V4, V6 }

  #[derive(Clone, Debug)]
  pub struct TransportConfig {
      pub transport:  Transport,
      pub ip_version: IpVersion,
  }

  impl Default for TransportConfig {
      fn default() -> Self {
          Self { transport: Transport::Udp, ip_version: IpVersion::Auto }
      }
  }

  // run_transaction gains a transport parameter:
  pub async fn run_transaction(
      server:    SocketAddr,
      zone:      Name,
      ptr_zone:  Name,
      hostname:  Name,
      ip:        Ipv4Addr,
      tsig:      Option<TsigConfig>,
      transport: TransportConfig,
  ) -> Result<crate::stats::TxResult, Box<dyn std::error::Error + Send + Sync>>
  ```

- [ ] **Step 1: Add TransportConfig types to dns.rs**

Add after the `TsigConfig` definition in `src/dns.rs`:

```rust
#[derive(Clone, Debug)]
pub enum Transport { Udp, Tcp }

#[derive(Clone, Debug)]
pub enum IpVersion { Auto, V4, V6 }

#[derive(Clone, Debug)]
pub struct TransportConfig {
    pub transport:  Transport,
    pub ip_version: IpVersion,
}

impl Default for TransportConfig {
    fn default() -> Self {
        Self { transport: Transport::Udp, ip_version: IpVersion::Auto }
    }
}
```

- [ ] **Step 2: Add `build_client` helper**

Replace the existing `tsig_client` helper and the inline unsigned-client construction with a single `build_client` function:

```rust
use hickory_client::tcp::TcpClientConnection;
use hickory_proto::iocompat::AsyncIoTokioAsStd;
use tokio::net::TcpStream;
use std::net::{IpAddr, Ipv4Addr as StdIpv4, Ipv6Addr};

async fn build_client(
    server:    SocketAddr,
    tsig:      Option<TsigConfig>,
    transport: &TransportConfig,
) -> Result<(AsyncClient, impl std::future::Future<Output = Result<(), hickory_proto::error::ProtoError>> + Send), Box<dyn std::error::Error + Send + Sync>> {
    let signer: Option<Arc<Signer>> = tsig.map(|t| {
        let tsigner = TSigner::new(t.secret, t.algorithm, t.key_name, 300)
            .expect("valid TSIG config");
        Arc::new(Signer::from(tsigner))
    });

    let bind_addr: Option<SocketAddr> = match transport.ip_version {
        IpVersion::V4 => Some("0.0.0.0:0".parse().unwrap()),
        IpVersion::V6 => Some("[::]:0".parse().unwrap()),
        IpVersion::Auto => None,
    };

    match transport.transport {
        Transport::Udp => {
            let conn = UdpClientConnection::with_bind_addr_and_timeout(
                server,
                bind_addr,
                std::time::Duration::from_secs(5),
            )?;
            let stream = conn.new_stream(signer);
            let (client, bg) = AsyncClient::connect(stream).await?;
            Ok((client, bg))
        }
        Transport::Tcp => {
            let conn = TcpClientConnection::with_bind_addr_and_timeout(
                server,
                bind_addr,
                std::time::Duration::from_secs(5),
            )?;
            let stream = conn.new_stream(signer);
            let (client, bg) = AsyncClient::connect(stream).await?;
            Ok((client, bg))
        }
    }
}
```

- [ ] **Step 3: Update run_transaction signature**

Add `transport: TransportConfig` as the last parameter and replace the client construction block:

```rust
pub async fn run_transaction(
    server:    SocketAddr,
    zone:      Name,
    ptr_zone:  Name,
    hostname:  Name,
    ip:        Ipv4Addr,
    tsig:      Option<TsigConfig>,
    transport: TransportConfig,
) -> Result<crate::stats::TxResult, Box<dyn std::error::Error + Send + Sync>> {
    let (mut client, bg) = build_client(server, tsig, &transport).await?;
    tokio::spawn(bg);
    // rest of function unchanged ...
```

Remove the old `match tsig { Some(t) => tsig_client... None => ... }` block — it's now handled by `build_client`.

- [ ] **Step 4: Update all callers**

`src/engine.rs` — add `transport: TransportConfig` to `BenchmarkConfig` and pass it through:
```rust
// BenchmarkConfig gains:
pub transport: TransportConfig,

// In task loop, change run_transaction call:
let result = crate::dns::run_transaction(
    server,
    zone.clone(),
    ptr_zone.clone(),
    rec.hostname,
    rec.ip,
    tsig,
    transport.clone(),
).await;
// clone transport per task alongside other clones:
let transport = transport.clone();
```

`src/main.rs` — add `transport: config.transport.clone()` to `BenchmarkConfig` construction and pass `config.transport` to the single-shot `run_transaction` call.

`src/dns.rs` tests — add `TransportConfig::default()` as final arg to all `run_transaction` calls in the integration tests.

- [ ] **Step 5: Add transport fields to cli::Args and Config**

In `src/cli.rs`, add to `Args`:
```rust
/// Use TCP transport (default: UDP)
#[arg(long, conflicts_with = "udp")]
pub tcp: bool,

/// Use UDP transport (default)
#[arg(long, conflicts_with = "tcp")]
pub udp: bool,

/// Force IPv4 transport to the DNS server
#[arg(long, conflicts_with = "ipv6")]
pub ipv4: bool,

/// Force IPv6 transport to the DNS server
#[arg(long, conflicts_with = "ipv4")]
pub ipv6: bool,
```

Add to `Config`:
```rust
pub transport: crate::dns::TransportConfig,
```

Add to `Args::into_config`:
```rust
let transport = crate::dns::TransportConfig {
    transport: if self.tcp { crate::dns::Transport::Tcp } else { crate::dns::Transport::Udp },
    ip_version: match (self.ipv4, self.ipv6) {
        (true, _) => crate::dns::IpVersion::V4,
        (_, true) => crate::dns::IpVersion::V6,
        _         => crate::dns::IpVersion::Auto,
    },
};
// add to Config { ... transport, ... }
```

Add CLI tests:
```rust
#[test]
fn default_transport_is_udp_auto() {
    let cfg = base_args().into_config().unwrap();
    assert!(matches!(cfg.transport.transport,  crate::dns::Transport::Udp));
    assert!(matches!(cfg.transport.ip_version, crate::dns::IpVersion::Auto));
}
```

- [ ] **Step 6: Verify compile and tests**

```bash
cargo build
cargo test
```

Expected: all non-ignored tests pass. Fix any import errors (e.g. `TcpClientConnection` path).

If `TcpClientConnection::with_bind_addr_and_timeout` doesn't exist, check:
```bash
grep -n "pub fn\|with_bind" \
  ~/.cargo/registry/src/index.crates.io-*/hickory-client-0.24.4/src/tcp/tcp_client_connection.rs
```
Use whichever constructor exists — at minimum `TcpClientConnection::new(server)`.

- [ ] **Step 7: Manual smoke test — TCP**

```bash
./target/debug/ddnsperf \
  --server 127.0.0.1:5353 \
  --zone test.local. \
  --network 10.0.0.0/24 \
  --requests 5 \
  --concurrency 2 \
  --tcp \
  --tsig-name test-key. \
  --tsig-secret "i8ThSp4D0SYlHviiQCCxAs4qgZtisWG845b7ttlCT6w="
```

Expected: same report format, 5/5 successful.

- [ ] **Step 8: Commit**

```bash
git add src/dns.rs src/engine.rs src/cli.rs src/main.rs
git commit -m "feat(dns,cli): TCP transport and IPv4/IPv6 forcing via TransportConfig"
```

---

### Task 3: perf.rs — PID controller

**Files:**
- Create: `src/perf.rs`
- Modify: `src/main.rs` (add `mod perf;`)

**Interfaces:**
- Consumes: `engine::BenchmarkConfig`, `engine::run_benchmark`, `stats::RunReport`
- Produces:
  ```rust
  pub struct PerfConfig {
      pub bench:        engine::BenchmarkConfig,  // total/rps/cancel ignored — overridden by PID
      pub error_target: f64,    // setpoint %, e.g. 1.0
      pub max_rps:      Option<u32>,
      pub duration:     std::time::Duration, // how long to search (default 120s)
  }

  pub struct PerfResult {
      pub max_sustainable_rps: u32,
      pub converged:           bool,
      pub search_duration:     std::time::Duration,
      pub final_report:        stats::RunReport,
  }

  pub async fn run_perf_test(cfg: PerfConfig, progress: indicatif::ProgressBar) -> PerfResult
  ```

**PID design:**
- Process variable (PV): rolling error rate over the last 1-second window
- Setpoint (SP): `error_target` %
- Output: RPS adjustment
- Gains (fixed): Kp = 50.0, Ki = 5.0, Kd = 10.0
- Sample interval: 500ms
- Convergence: |Δrps| < 2% of current rps for 5 consecutive samples
- Rate limiter swapped via `Arc<tokio::sync::RwLock<Arc<Limiter>>>`

- [ ] **Step 1: Write unit tests for PID math**

Create `src/perf.rs` with just the PID struct and tests first:

```rust
// src/perf.rs

/// Discrete PID controller. Call `update(pv)` every sample interval.
/// Returns the output adjustment (positive = increase RPS, negative = decrease).
pub struct Pid {
    pub kp: f64,
    pub ki: f64,
    pub kd: f64,
    setpoint:   f64,
    integral:   f64,
    prev_error: f64,
}

impl Pid {
    pub fn new(kp: f64, ki: f64, kd: f64, setpoint: f64) -> Self {
        Self { kp, ki, kd, setpoint, integral: 0.0, prev_error: 0.0 }
    }

    /// `pv` is the current error rate %. Returns signed RPS delta.
    pub fn update(&mut self, pv: f64) -> f64 {
        let error      = self.setpoint - pv;   // positive when we're below target (room to increase)
        self.integral += error;
        let derivative = error - self.prev_error;
        self.prev_error = error;
        self.kp * error + self.ki * self.integral + self.kd * derivative
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pid_increases_when_below_setpoint() {
        // error_rate = 0% (below 1% target) → should output positive (increase RPS)
        let mut pid = Pid::new(50.0, 5.0, 10.0, 1.0);
        let output = pid.update(0.0);
        assert!(output > 0.0, "output {output} should be positive when pv < setpoint");
    }

    #[test]
    fn pid_decreases_when_above_setpoint() {
        // error_rate = 10% (above 1% target) → should output negative (decrease RPS)
        let mut pid = Pid::new(50.0, 5.0, 10.0, 1.0);
        let output = pid.update(10.0);
        assert!(output < 0.0, "output {output} should be negative when pv > setpoint");
    }

    #[test]
    fn pid_zero_at_setpoint() {
        // error_rate == setpoint with zero integral → output should be 0
        let mut pid = Pid::new(50.0, 5.0, 10.0, 1.0);
        let output = pid.update(1.0);
        // error = 0, integral = 0, derivative = 0 → output = 0
        assert_eq!(output, 0.0);
    }
}
```

- [ ] **Step 2: Run tests — expect pass (pure math, no deps)**

```bash
cargo test perf::tests
```

Expected: 3 tests pass.

- [ ] **Step 3: Implement run_perf_test**

Add to `src/perf.rs`:

```rust
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, Instant};

use governor::{Quota, RateLimiter};
use governor::clock::DefaultClock;
use governor::state::{InMemoryState, NotKeyed};
use indicatif::ProgressBar;
use tokio::sync::RwLock;

use crate::engine::BenchmarkConfig;
use crate::stats::RunReport;

pub struct PerfConfig {
    pub bench:        BenchmarkConfig,
    pub error_target: f64,
    pub max_rps:      Option<u32>,
    pub duration:     Duration,
}

pub struct PerfResult {
    pub max_sustainable_rps: u32,
    pub converged:           bool,
    pub search_duration:     Duration,
    pub final_report:        RunReport,
}

type SharedLimiter = Arc<RwLock<Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>>>>;

fn make_limiter(rps: u32) -> Arc<RateLimiter<NotKeyed, InMemoryState, DefaultClock>> {
    let nz = NonZeroU32::new(rps.max(1)).unwrap();
    Arc::new(RateLimiter::direct(Quota::per_second(nz)))
}

pub async fn run_perf_test(mut cfg: PerfConfig, progress: ProgressBar) -> PerfResult {
    const START_RPS:  u32 = 100;
    const SAMPLE_MS:  u64 = 500;
    const CONVERGE_N: usize = 5;
    const CONVERGE_PCT: f64 = 0.02;

    let start = Instant::now();
    let max_rps = cfg.max_rps.unwrap_or(50_000);

    // Shared rate limiter — PID swaps it; engine tasks read it
    let shared: SharedLimiter = Arc::new(RwLock::new(make_limiter(START_RPS)));

    // Override engine's rate limiter with the shared one
    // We signal the engine via cancel + a per-epoch run approach:
    // Run engine indefinitely (total = None), cancel after duration or convergence.
    let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
    cfg.bench.cancel = cancel_rx;
    cfg.bench.total  = None;
    cfg.bench.rps    = Some(START_RPS);  // initial value (engine will use shared limiter)

    // Stats channel for rolling window
    let (outcome_tx, mut outcome_rx) =
        tokio::sync::mpsc::unbounded_channel::<crate::stats::Outcome>();

    // Spawn engine as a background task, collecting outcomes into outcome_tx
    // We bypass run_benchmark here and drive the engine tasks directly with the shared limiter.
    // Simpler: spawn the normal engine but also tap into outcomes via a tee.
    // Actually: build our own task pool that uses the shared limiter.

    let server      = cfg.bench.server;
    let zone        = cfg.bench.zone.clone();
    let ptr_zone    = cfg.bench.ptr_zone.clone();
    let generator   = cfg.bench.generator.clone();
    let tsig_arc    = cfg.bench.tsig.clone();
    let transport   = cfg.bench.transport.clone();
    let concurrency = cfg.bench.concurrency;

    let sent_ok  = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let sent_err = Arc::new(std::sync::atomic::AtomicU64::new(0));

    let mut task_handles = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let outcome_tx = outcome_tx.clone();
        let gen        = generator.clone();
        let zone       = zone.clone();
        let ptr_zone   = ptr_zone.clone();
        let tsig_arc   = tsig_arc.clone();
        let transport  = transport.clone();
        let shared     = shared.clone();
        let cancel     = cfg.bench.cancel.clone();
        let sent_ok    = sent_ok.clone();
        let sent_err   = sent_err.clone();
        let pb         = progress.clone();

        task_handles.push(tokio::spawn(async move {
            loop {
                if *cancel.borrow() { break; }

                // Acquire rate token from current limiter
                let lim = { shared.read().await.clone() };
                lim.until_ready().await;

                let rec  = gen.next();
                let tsig = tsig_arc.as_deref().cloned();
                let t0   = Instant::now();
                let ok   = crate::dns::run_transaction(
                    server, zone.clone(), ptr_zone.clone(),
                    rec.hostname, rec.ip, tsig, transport.clone(),
                ).await.is_ok();
                let lat = t0.elapsed().as_micros() as u64;

                if ok { sent_ok.fetch_add(1, std::sync::atomic::Ordering::Relaxed); }
                else  { sent_err.fetch_add(1, std::sync::atomic::Ordering::Relaxed); }
                let _ = outcome_tx.send(crate::stats::Outcome { latency_us: lat, success: ok });
                pb.inc(1);
            }
        }));
    }

    // PID loop
    let mut pid = Pid::new(50.0, 5.0, 10.0, cfg.error_target);
    let mut current_rps = START_RPS as f64;
    let mut converge_streak = 0usize;
    let mut last_ok:  u64 = 0;
    let mut last_err: u64 = 0;

    loop {
        tokio::time::sleep(Duration::from_millis(SAMPLE_MS)).await;

        // Check overall duration
        if start.elapsed() >= cfg.duration {
            break;
        }

        // Rolling window error rate
        let ok  = sent_ok.load(std::sync::atomic::Ordering::Relaxed);
        let err = sent_err.load(std::sync::atomic::Ordering::Relaxed);
        let delta_ok  = ok  - last_ok;
        let delta_err = err - last_err;
        last_ok  = ok;
        last_err = err;
        let total = delta_ok + delta_err;
        let error_rate = if total == 0 { 0.0 }
                         else { delta_err as f64 / total as f64 * 100.0 };

        // PID update
        let delta = pid.update(error_rate);
        let new_rps = (current_rps + delta)
            .max(1.0)
            .min(max_rps as f64);

        let rps_change_pct = (new_rps - current_rps).abs() / current_rps.max(1.0);
        current_rps = new_rps;

        // Swap limiter
        {
            let mut guard = shared.write().await;
            *guard = make_limiter(current_rps as u32);
        }

        // Convergence check
        if rps_change_pct < CONVERGE_PCT {
            converge_streak += 1;
            if converge_streak >= CONVERGE_N {
                break; // converged
            }
        } else {
            converge_streak = 0;
        }
    }

    let converged = converge_streak >= CONVERGE_N;
    let search_duration = start.elapsed();

    // Signal tasks to stop
    let _ = cancel_tx.send(true);
    for h in task_handles { let _ = h.await; }
    drop(outcome_tx);

    // Drain remaining outcomes for final report
    let mut total_sent = 0u64;
    let mut total_ok   = 0u64;
    let mut total_err  = 0u64;
    let mut min_us = u64::MAX;
    let mut max_us = 0u64;
    let mut mean   = 0.0f64;
    while let Ok(o) = outcome_rx.try_recv() {
        total_sent += 1;
        if o.success { total_ok += 1; } else { total_err += 1; }
        if o.latency_us < min_us { min_us = o.latency_us; }
        if o.latency_us > max_us { max_us = o.latency_us; }
        let d = o.latency_us as f64 - mean;
        mean += d / total_sent as f64;
    }
    // Also count what tasks already tallied
    let total_ok_final  = sent_ok.load(std::sync::atomic::Ordering::Relaxed);
    let total_err_final = sent_err.load(std::sync::atomic::Ordering::Relaxed);
    let total_final     = total_ok_final + total_err_final;
    let throughput = total_final as f64 / search_duration.as_secs_f64().max(0.001);

    let final_report = RunReport {
        duration:   search_duration,
        total_sent: total_final,
        total_ok:   total_ok_final,
        total_err:  total_err_final,
        min_us:     if total_sent == 0 { 0 } else { min_us },
        mean_us:    mean,
        max_us,
        throughput,
    };

    PerfResult {
        max_sustainable_rps: current_rps as u32,
        converged,
        search_duration,
        final_report,
    }
}
```

- [ ] **Step 4: Register module and verify compile**

Add `mod perf;` to `src/main.rs`.

```bash
cargo build
```

Fix any import errors. Common ones:
- `crate::engine::BenchmarkConfig` needs `transport` field (added in Task 2)
- `crate::stats::RunReport` fields must match the struct definition in `stats.rs`

- [ ] **Step 5: Run all tests**

```bash
cargo test
```

Expected: all non-ignored tests pass including the 3 new PID unit tests.

- [ ] **Step 6: Commit**

```bash
git add src/perf.rs src/main.rs
git commit -m "feat(perf): PID controller for max-throughput discovery"
```

---

### Task 4: Wire --perf-test / --rps auto in CLI + main

**Files:**
- Modify: `src/cli.rs`
- Modify: `src/main.rs`

**Interfaces:**
- Consumes: `perf::run_perf_test`, `perf::PerfConfig`, `perf::PerfResult`

- [ ] **Step 1: Add perf flags to cli::Args**

In `src/cli.rs`, add to `Args`:

```rust
/// Find maximum sustainable throughput via PID control; mutually exclusive with --rps
#[arg(long, conflicts_with = "rps")]
pub perf_test: bool,

/// Error rate setpoint for --perf-test or --rps auto (default: 1.0%)
#[arg(long, default_value_t = 1.0)]
pub error_target: f64,

/// Safety cap on RPS during perf test
#[arg(long)]
pub max_rps_cap: Option<u32>,

/// Duration of perf test search in seconds (default: 120)
#[arg(long, default_value_t = 120)]
pub perf_duration: u64,
```

For `--rps auto`, change the existing `rps: Option<u32>` field to accept the string `"auto"`:

```rust
/// Target RPS, or 'auto' to run a perf test then benchmark at discovered rate
#[arg(long)]
pub rps: Option<String>,
```

Add to `Config`:
```rust
pub perf_test:    bool,
pub error_target: f64,
pub max_rps_cap:  Option<u32>,
pub perf_duration: u64,
pub rps_auto:     bool,   // true when --rps auto was specified
```

Update `Args::into_config` to parse `--rps`:
```rust
let (rps, rps_auto) = match self.rps.as_deref() {
    None        => (None, false),
    Some("auto") => (None, true),
    Some(n)     => (Some(n.parse::<u32>().map_err(|_| format!("invalid --rps: '{}'", n))?), false),
};
```

Update CLI tests — change `rps: None` in `base_args()` from `Option<u32>` to `Option<String>`.

Add tests:
```rust
#[test]
fn rps_auto_sets_flag() {
    let mut a = base_args();
    a.rps = Some("auto".to_string());
    let cfg = a.into_config().unwrap();
    assert!(cfg.rps_auto);
    assert!(cfg.rps.is_none());
}

#[test]
fn rps_numeric_parses() {
    let mut a = base_args();
    a.rps = Some("200".to_string());
    let cfg = a.into_config().unwrap();
    assert_eq!(cfg.rps, Some(200));
    assert!(!cfg.rps_auto);
}

#[test]
fn rps_invalid_errors() {
    let mut a = base_args();
    a.rps = Some("fast".to_string());
    assert!(a.into_config().is_err());
}
```

- [ ] **Step 2: Run CLI tests**

```bash
cargo test cli::tests
```

Expected: all CLI tests pass.

- [ ] **Step 3: Update main.rs for perf-test and rps-auto routing**

Update the benchmark branch in `main.rs`:

```rust
// After building bench_cfg and pb ...

if config.perf_test || config.rps_auto {
    // -- perf test phase --
    let perf_pb = ProgressBar::new_spinner();
    perf_pb.set_style(
        ProgressStyle::with_template(
            "[{elapsed_precise}] {spinner} perf test — {pos} tx sent"
        ).unwrap()
    );

    let perf_cfg = perf::PerfConfig {
        bench:        engine::BenchmarkConfig {
            server:      config.server,
            zone:        config.zone.clone(),
            ptr_zone:    ptr_zone.clone(),
            generator:   generator.clone(),
            tsig:        config.tsig.clone().map(Arc::new),
            concurrency: config.concurrency,
            total:       None,
            rps:         None,
            cancel:      cancel_rx.clone(),
            transport:   config.transport.clone(),
        },
        error_target: config.error_target,
        max_rps:      config.max_rps_cap,
        duration:     std::time::Duration::from_secs(config.perf_duration),
    };

    let result = perf::run_perf_test(perf_cfg, perf_pb).await;

    println!("[perf-test phase]");
    println!("Max sustainable RPS: {} ({}converged in {:.1}s, error target: {:.1}%)",
        result.max_sustainable_rps,
        if result.converged { "" } else { "NOT " },
        result.search_duration.as_secs_f64(),
        config.error_target,
    );
    stats::print_run_report(&result.final_report);

    if config.rps_auto {
        // Re-run benchmark at discovered rate
        println!("\n[benchmark phase at {} RPS]", result.max_sustainable_rps);
        let (_, cancel_rx2) = tokio::sync::watch::channel(false);
        let bench_cfg2 = engine::BenchmarkConfig {
            server:      config.server,
            zone:        config.zone,
            ptr_zone,
            generator,
            tsig:        config.tsig.map(Arc::new),
            concurrency: config.concurrency,
            total:       config.requests,
            rps:         Some(result.max_sustainable_rps),
            cancel:      cancel_rx2,
            transport:   config.transport,
        };
        let pb2 = build_progress_bar(config.requests);
        let report = engine::run_benchmark(bench_cfg2, pb2).await;
        stats::print_run_report(&report);
    }
} else {
    // -- normal benchmark --
    let report = engine::run_benchmark(bench_cfg, pb).await;
    stats::print_run_report(&report);
}
```

Extract the progress bar construction into a small helper to avoid repetition:
```rust
fn build_progress_bar(requests: Option<u64>) -> ProgressBar {
    if let Some(n) = requests {
        let pb = ProgressBar::new(n);
        pb.set_style(
            ProgressStyle::with_template(
                "[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len}  {per_sec} tx/s"
            ).unwrap().progress_chars("█░"),
        );
        pb
    } else {
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::with_template("[{elapsed_precise}] {spinner} {pos} sent").unwrap()
        );
        pb
    }
}
```

- [ ] **Step 4: Build and run all tests**

```bash
cargo build
cargo test
```

Expected: all non-ignored tests pass.

- [ ] **Step 5: Smoke test — perf test mode (against live BIND)**

```bash
./target/debug/ddnsperf \
  --server 127.0.0.1:5353 \
  --zone test.local. \
  --network 10.0.0.0/24 \
  --concurrency 8 \
  --perf-test \
  --error-target 2.0 \
  --perf-duration 15 \
  --tsig-name test-key. \
  --tsig-secret "i8ThSp4D0SYlHviiQCCxAs4qgZtisWG845b7ttlCT6w="
```

Expected (values will vary):
```
[perf-test phase]
Max sustainable RPS: 847 (converged in 12.4s, error target: 2.0%)
=== ddnsperf results ===
...
```

- [ ] **Step 6: Smoke test — rps auto mode (against live BIND)**

```bash
./target/debug/ddnsperf \
  --server 127.0.0.1:5353 \
  --zone test.local. \
  --network 10.0.0.0/24 \
  --requests 100 \
  --concurrency 8 \
  --rps auto \
  --error-target 2.0 \
  --perf-duration 15 \
  --tsig-name test-key. \
  --tsig-secret "i8ThSp4D0SYlHviiQCCxAs4qgZtisWG845b7ttlCT6w="
```

Expected: perf test phase output followed by benchmark phase output at discovered RPS.

- [ ] **Step 7: Commit**

```bash
git add src/cli.rs src/main.rs
git commit -m "feat(cli,main): --perf-test and --rps auto wired to PID perf module"
```

---

## Self-review

- `cancel` field added to `BenchmarkConfig` in Task 1; all existing callers in `main.rs` updated ✓
- `transport` field added to `BenchmarkConfig` in Task 2; `perf.rs` Task 3 also populates it ✓  
- `RunReport` construction in `perf.rs` matches exact field names from `stats.rs` ✓
- `--rps` type changed from `Option<u32>` to `Option<String>` in Task 4; base_args() in tests updated accordingly ✓
- `perf::PerfConfig.bench.cancel` — the watch receiver is cloned from the one created in `main.rs`; the sender stays in `main` for the duration task ✓
- No new crate dependencies added ✓

## Open questions

- PID gains (Kp=50, Ki=5, Kd=10) are empirical defaults. If the controller oscillates badly on a real server, the values may need tuning. A future improvement: expose `--pid-kp`, `--pid-ki`, `--pid-kd` flags.
- The `--rps auto` benchmark phase needs either `--requests` or runs until Ctrl-C.
- AAAA record support and `--record-type` flag deferred to Phase 4.

# Overload Testing Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `--timeout <ms>`, `--delete`, and typed error breakdown (Timeout / DnsRejected / Transport) to ddnsperf, with PTR records opt-in via explicit `--ptr-zone`.

**Architecture:** Foundation types (`ErrorKind`, `TxError`) are added first so later tasks compile against stable interfaces. Each task is a vertical slice: new/updated types → tests → wiring → commit.

**Tech Stack:** Rust 1.97, hickory-proto 0.24 (`ProtoErrorKind::Timeout`), clap 4, tokio 1, existing crate structure.

## Global Constraints

- `cargo test` must be green at every commit.
- No new dependencies — all error detection uses existing hickory-proto types.
- Default behaviour change: no delete legs without `--delete`; no PTR legs without explicit `--ptr-zone`. Existing integration tests must add `include_ptr: true, include_delete: true` where needed.
- `--timeout` default: `5000` ms (preserves current behaviour).

---

## File Map

| File | Role in this plan |
|---|---|
| `src/stats.rs` | Add `ErrorKind`, update `Outcome`, expand `RunReport`, update collector and printer |
| `src/dns.rs` | Add `TxError`, typed helpers, optional legs in `run_transaction`, configurable timeout |
| `src/cli.rs` | Add `--timeout` and `--delete` flags; remove ptr_zone inference |
| `src/engine.rs` | Expand `BenchmarkConfig`, map `TxError` → `Outcome` |
| `src/perf.rs` | Update `Outcome` construction; expand inline `RunReport` literal |
| `src/main.rs` | Pass new config fields; remove ptr_zone inference fallback |

---

### Task 1: stats.rs — ErrorKind, Outcome, RunReport, collector, printer

**Files:**
- Modify: `src/stats.rs`

**Interfaces:**
- Produces:
  - `pub enum ErrorKind { Timeout, DnsRejected { code: u16 }, Transport }`
  - `pub struct Outcome { pub latency_us: u64, pub error: Option<ErrorKind> }`
  - `RunReport` gains `total_timeout: u64`, `total_dns_error: u64`, `total_transport: u64`, `dns_codes: Vec<(u16, u64)>`
  - `TxResult` fields `add_ptr_latency`, `del_ptr_latency`, `del_a_latency` become `Option<Duration>`
  - `TxResult::total()` sums only `Some` legs

- [ ] **Step 1: Write failing tests for new ErrorKind counting**

Add to the `#[cfg(test)]` block in `src/stats.rs`:

```rust
#[tokio::test]
async fn collector_counts_error_kinds() {
    use tokio::sync::mpsc::unbounded_channel;
    let (tx, rx) = unbounded_channel();
    let start = std::time::Instant::now();
    let handle = spawn_collector(rx, start);

    tx.send(Outcome { latency_us: 100, error: None }).unwrap();
    tx.send(Outcome { latency_us: 200, error: Some(ErrorKind::Timeout) }).unwrap();
    tx.send(Outcome { latency_us: 300, error: Some(ErrorKind::Timeout) }).unwrap();
    tx.send(Outcome { latency_us: 400, error: Some(ErrorKind::DnsRejected { code: 5 }) }).unwrap();
    tx.send(Outcome { latency_us: 500, error: Some(ErrorKind::Transport) }).unwrap();
    drop(tx);

    let r = handle.await.unwrap();
    assert_eq!(r.total_sent, 5);
    assert_eq!(r.total_ok,   1);
    assert_eq!(r.total_err,  4);
    assert_eq!(r.total_timeout,   2);
    assert_eq!(r.total_dns_error, 1);
    assert_eq!(r.total_transport, 1);
    assert_eq!(r.dns_codes, vec![(5u16, 1u64)]);
}

#[test]
fn txresult_total_sums_only_some_legs() {
    use std::time::Duration;
    let r = TxResult {
        add_a_latency:   Duration::from_millis(10),
        add_ptr_latency: Some(Duration::from_millis(5)),
        del_ptr_latency: None,
        del_a_latency:   None,
    };
    assert_eq!(r.total(), Duration::from_millis(15));
}

#[test]
fn print_run_report_omits_dns_error_line_when_zero() {
    // Smoke-test: just ensure it doesn't panic with all-zero error counts.
    use std::time::Duration;
    let r = RunReport {
        duration:        Duration::from_secs(1),
        total_sent:      10,
        total_ok:        10,
        total_err:       0,
        min_us:          100,
        mean_us:         200.0,
        max_us:          300,
        throughput:      10.0,
        concurrency:     1,
        total_timeout:   0,
        total_dns_error: 0,
        total_transport: 0,
        dns_codes:       vec![],
    };
    print_run_report(&r); // must not panic
}
```

- [ ] **Step 2: Run tests — expect compile errors (types don't exist yet)**

```bash
cargo test 2>&1 | grep "^error" | head -10
```

Expected: errors about missing `ErrorKind`, `total_timeout`, etc.

- [ ] **Step 3: Replace the ErrorKind, Outcome, TxResult, RunReport, spawn_collector, and print_run_report definitions**

Replace the entire content of `src/stats.rs` with:

```rust
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::task::JoinHandle;

// ── Error classification ─────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    Timeout,
    DnsRejected { code: u16 },
    Transport,
}

// ── Streaming types ──────────────────────────────────────────────────────────

pub struct Outcome {
    pub latency_us: u64,
    pub error:      Option<ErrorKind>,
}

pub struct RunReport {
    pub duration:        Duration,
    pub total_sent:      u64,
    pub total_ok:        u64,
    pub total_err:       u64,
    pub min_us:          u64,
    pub mean_us:         f64,
    pub max_us:          u64,
    pub throughput:      f64,
    pub concurrency:     usize,
    // error breakdown
    pub total_timeout:   u64,
    pub total_dns_error: u64,
    pub total_transport: u64,
    pub dns_codes:       Vec<(u16, u64)>, // (ResponseCode as u16, count), sorted desc by count
}

/// Spawns a collector task. Drains `rx` until sender is dropped, then returns RunReport.
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
        let mut mean:   f64 = 0.0;

        let mut total_timeout:   u64 = 0;
        let mut total_dns_error: u64 = 0;
        let mut total_transport: u64 = 0;
        let mut dns_code_map: HashMap<u16, u64> = HashMap::new();

        while let Some(o) = rx.recv().await {
            total_sent += 1;
            if o.error.is_none() {
                total_ok += 1;
            } else {
                total_err += 1;
                match o.error.as_ref().unwrap() {
                    ErrorKind::Timeout              => total_timeout   += 1,
                    ErrorKind::DnsRejected { code } => {
                        total_dns_error += 1;
                        *dns_code_map.entry(*code).or_insert(0) += 1;
                    }
                    ErrorKind::Transport            => total_transport += 1,
                }
            }
            if o.latency_us < min_us { min_us = o.latency_us; }
            if o.latency_us > max_us { max_us = o.latency_us; }
            // Welford's online mean
            let delta = o.latency_us as f64 - mean;
            mean += delta / total_sent as f64;
        }

        let duration   = start.elapsed();
        let throughput = if duration.as_secs_f64() > 0.0 {
            total_sent as f64 / duration.as_secs_f64()
        } else { 0.0 };

        let mut dns_codes: Vec<(u16, u64)> = dns_code_map.into_iter().collect();
        dns_codes.sort_by(|a, b| b.1.cmp(&a.1));

        RunReport {
            duration,
            total_sent,
            total_ok,
            total_err,
            min_us:          if total_sent == 0 { 0 } else { min_us },
            mean_us:         mean,
            max_us,
            throughput,
            concurrency:     0, // filled in by caller
            total_timeout,
            total_dns_error,
            total_transport,
            dns_codes,
        }
    })
}

pub fn print_run_report(r: &RunReport) {
    println!("=== ddnsperf results ===");
    println!("Duration:      {:.3}s", r.duration.as_secs_f64());
    println!("Total sent:    {}", r.total_sent);
    println!("  Successful:  {} ({:.1}%)", r.total_ok,  pct(r.total_ok,  r.total_sent));

    if r.total_timeout > 0 || r.total_dns_error > 0 || r.total_transport > 0 {
        println!("  Timeout:     {} ({:.1}%)", r.total_timeout,   pct(r.total_timeout,   r.total_sent));
        if r.total_dns_error > 0 {
            let codes: String = r.dns_codes.iter()
                .map(|(code, n)| format!("{}×{}", rcode_name(*code), n))
                .collect::<Vec<_>>()
                .join("  ");
            println!("  DNS error:   {} ({:.1}%)  — {}", r.total_dns_error, pct(r.total_dns_error, r.total_sent), codes);
        }
        if r.total_transport > 0 {
            println!("  Transport:   {} ({:.1}%)", r.total_transport, pct(r.total_transport, r.total_sent));
        }
    } else {
        println!("  Errors:      {} ({:.1}%)", r.total_err, pct(r.total_err, r.total_sent));
    }

    println!("Throughput:    {:.0} RPS", r.throughput);
    if r.concurrency > 1 {
        println!("  per worker:  {:.0} RPS   (throughput / --concurrency {})",
            r.throughput / r.concurrency as f64, r.concurrency);
    }
    println!("Latency:");
    println!("  Min:         {:.3}ms", r.min_us  as f64 / 1000.0);
    println!("  Mean:        {:.3}ms", r.mean_us       / 1000.0);
    println!("  Max:         {:.3}ms", r.max_us  as f64 / 1000.0);

    // Note: server-side serialisation cannot be reliably detected from a single run.
    // The "per worker" line above is the diagnostic: if it is much lower than a
    // single-concurrency run, the server is processing updates one at a time.
}

/// Map a ResponseCode u16 to a short human-readable name.
fn rcode_name(code: u16) -> &'static str {
    match code {
        0  => "NOERROR",
        1  => "FORMERR",
        2  => "SERVFAIL",
        3  => "NXDOMAIN",
        4  => "NOTIMP",
        5  => "REFUSED",
        6  => "YXDOMAIN",
        7  => "YXRRSET",
        8  => "NXRRSET",
        9  => "NOTAUTH",
        10 => "NOTZONE",
        _  => "UNKNOWN",
    }
}

fn pct(n: u64, total: u64) -> f64 {
    if total == 0 { 0.0 } else { n as f64 / total as f64 * 100.0 }
}

// ── Legacy single-transaction types (kept for single-shot mode) ──────────────

pub struct TxResult {
    pub add_a_latency:   Duration,
    pub add_ptr_latency: Option<Duration>,
    pub del_ptr_latency: Option<Duration>,
    pub del_a_latency:   Option<Duration>,
}

impl TxResult {
    pub fn total(&self) -> Duration {
        let mut t = self.add_a_latency;
        if let Some(d) = self.add_ptr_latency { t += d; }
        if let Some(d) = self.del_ptr_latency { t += d; }
        if let Some(d) = self.del_a_latency   { t += d; }
        t
    }
}

pub fn print_report(result: &TxResult) {
    println!("=== ddnsperf transaction result ===");
    println!("  Add A:      {:>8.3}ms", ms(result.add_a_latency));
    if let Some(d) = result.add_ptr_latency {
        println!("  Add PTR:    {:>8.3}ms", ms(d));
    }
    if let Some(d) = result.del_ptr_latency {
        println!("  Delete PTR: {:>8.3}ms", ms(d));
    }
    if let Some(d) = result.del_a_latency {
        println!("  Delete A:   {:>8.3}ms", ms(d));
    }
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
            add_ptr_latency: Some(Duration::from_millis(2)),
            del_ptr_latency: Some(Duration::from_millis(3)),
            del_a_latency:   Some(Duration::from_millis(4)),
        };
        assert_eq!(r.total(), Duration::from_millis(10));
    }

    #[test]
    fn txresult_total_sums_only_some_legs() {
        let r = TxResult {
            add_a_latency:   Duration::from_millis(10),
            add_ptr_latency: Some(Duration::from_millis(5)),
            del_ptr_latency: None,
            del_a_latency:   None,
        };
        assert_eq!(r.total(), Duration::from_millis(15));
    }

    #[test]
    fn run_report_error_rate() {
        let r = RunReport {
            duration:        Duration::from_secs(1),
            total_sent:      100,
            total_ok:        95,
            total_err:       5,
            min_us:          100,
            mean_us:         500.0,
            max_us:          2000,
            throughput:      100.0,
            concurrency:     1,
            total_timeout:   3,
            total_dns_error: 1,
            total_transport: 1,
            dns_codes:       vec![(5, 1)],
        };
        let pct = r.total_err as f64 / r.total_sent as f64 * 100.0;
        assert!((pct - 5.0).abs() < 0.001);
        assert_eq!(r.total_timeout + r.total_dns_error + r.total_transport, r.total_err);
    }

    #[test]
    fn run_report_success_rate() {
        let r = RunReport {
            duration:        Duration::from_secs(1),
            total_sent:      200,
            total_ok:        200,
            total_err:       0,
            min_us:          50,
            mean_us:         200.0,
            max_us:          800,
            throughput:      200.0,
            concurrency:     1,
            total_timeout:   0,
            total_dns_error: 0,
            total_transport: 0,
            dns_codes:       vec![],
        };
        assert_eq!(r.total_err, 0);
        assert_eq!(r.total_ok, r.total_sent);
    }

    #[test]
    fn print_run_report_omits_dns_error_line_when_zero() {
        let r = RunReport {
            duration:        Duration::from_secs(1),
            total_sent:      10,
            total_ok:        10,
            total_err:       0,
            min_us:          100,
            mean_us:         200.0,
            max_us:          300,
            throughput:      10.0,
            concurrency:     1,
            total_timeout:   0,
            total_dns_error: 0,
            total_transport: 0,
            dns_codes:       vec![],
        };
        print_run_report(&r);
    }

    #[tokio::test]
    async fn collector_counts_outcomes() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let start = Instant::now();
        let handle = spawn_collector(rx, start);

        for i in 0..10u64 {
            tx.send(Outcome {
                latency_us: (i + 1) * 100,
                error: if i < 8 { None } else { Some(ErrorKind::Transport) },
            }).unwrap();
        }
        drop(tx);

        let report = handle.await.unwrap();
        assert_eq!(report.total_sent, 10);
        assert_eq!(report.total_ok,    8);
        assert_eq!(report.total_err,   2);
        assert_eq!(report.min_us,    100);
        assert_eq!(report.max_us,   1000);
        assert!((report.mean_us - 550.0).abs() < 1.0);
    }

    #[tokio::test]
    async fn collector_counts_error_kinds() {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let start = Instant::now();
        let handle = spawn_collector(rx, start);

        tx.send(Outcome { latency_us: 100, error: None }).unwrap();
        tx.send(Outcome { latency_us: 200, error: Some(ErrorKind::Timeout) }).unwrap();
        tx.send(Outcome { latency_us: 300, error: Some(ErrorKind::Timeout) }).unwrap();
        tx.send(Outcome { latency_us: 400, error: Some(ErrorKind::DnsRejected { code: 5 }) }).unwrap();
        tx.send(Outcome { latency_us: 500, error: Some(ErrorKind::Transport) }).unwrap();
        drop(tx);

        let r = handle.await.unwrap();
        assert_eq!(r.total_sent, 5);
        assert_eq!(r.total_ok,   1);
        assert_eq!(r.total_err,  4);
        assert_eq!(r.total_timeout,   2);
        assert_eq!(r.total_dns_error, 1);
        assert_eq!(r.total_transport, 1);
        assert_eq!(r.dns_codes, vec![(5u16, 1u64)]);
    }
}
```

- [ ] **Step 4: Run stats tests**

```bash
cargo test stats:: 2>&1 | tail -10
```

Expected: all stats tests pass; other modules may fail due to type mismatches (expected — fixed in later tasks).

- [ ] **Step 5: Commit**

```bash
git add src/stats.rs
git commit -m "feat(stats): ErrorKind, typed Outcome, expanded RunReport with error breakdown"
```

---

### Task 2: dns.rs — TxError, typed helpers, optional legs, configurable timeout

**Files:**
- Modify: `src/dns.rs`

**Interfaces:**
- Consumes: `crate::stats::{TxResult, ErrorKind}` (from Task 1)
- Produces:
  - `pub enum TxError { Timeout, DnsRejected { code: ResponseCode, leg: &'static str }, Transport(String) }`
  - `pub fn tx_error_to_error_kind(e: &TxError) -> ErrorKind`
  - `build_client(server, tsig, transport, timeout: Duration)` — `timeout` replaces hardcoded 5 s
  - `run_transaction(server, zone, ptr_zone: Option<Name>, hostname, ip, tsig, transport, timeout: Duration, include_ptr: bool, include_delete: bool) -> Result<TxResult, TxError>`

- [ ] **Step 1: Write failing unit tests for TxError classification**

Add to `#[cfg(test)]` in `src/dns.rs`:

```rust
#[test]
fn tx_error_to_kind_timeout() {
    use crate::stats::ErrorKind;
    let e = TxError::Timeout;
    assert_eq!(tx_error_to_error_kind(&e), ErrorKind::Timeout);
}

#[test]
fn tx_error_to_kind_dns_rejected() {
    use crate::stats::ErrorKind;
    use hickory_proto::op::ResponseCode;
    let e = TxError::DnsRejected { code: ResponseCode::Refused, leg: "add_a" };
    assert!(matches!(
        tx_error_to_error_kind(&e),
        ErrorKind::DnsRejected { code: 5 }
    ));
}

#[test]
fn tx_error_to_kind_transport() {
    use crate::stats::ErrorKind;
    let e = TxError::Transport("connection reset".into());
    assert_eq!(tx_error_to_error_kind(&e), ErrorKind::Transport);
}
```

- [ ] **Step 2: Run — expect compile failure (TxError not defined)**

```bash
cargo test dns::tests::tx_error 2>&1 | grep "^error" | head -5
```

- [ ] **Step 3: Replace dns.rs with the updated implementation**

Replace the entire file `src/dns.rs`:

```rust
use hickory_client::client::{AsyncClient, ClientConnection, ClientHandle, Signer};
use hickory_client::udp::UdpClientConnection;
use hickory_client::tcp::TcpClientConnection;
use hickory_proto::error::ProtoErrorKind;
use hickory_proto::op::ResponseCode;
use hickory_proto::rr::{DNSClass, Name, RData, Record, RecordType};
use hickory_proto::rr::rdata::{A, PTR};
use hickory_proto::rr::dnssec::tsig::TSigner;
use hickory_proto::rr::dnssec::rdata::tsig::TsigAlgorithm;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

// ── Transport configuration ──────────────────────────────────────────────────

#[derive(Clone, Debug, Default)]
pub enum Transport { #[default] Udp, Tcp }

#[derive(Clone, Debug, Default)]
pub enum IpVersion { #[default] Auto, V4, V6 }

#[derive(Clone, Debug, Default)]
pub struct TransportConfig {
    pub transport:  Transport,
    pub ip_version: IpVersion,
}

/// TSIG authentication configuration.
#[derive(Clone)]
pub struct TsigConfig {
    pub key_name:  Name,
    pub algorithm: TsigAlgorithm,
    pub secret:    Vec<u8>,
}

// ── Typed error ──────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum TxError {
    Timeout,
    DnsRejected { code: ResponseCode, leg: &'static str },
    Transport(String),
}

impl std::fmt::Display for TxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TxError::Timeout                   => write!(f, "timeout"),
            TxError::DnsRejected { code, leg } => write!(f, "{leg}: server returned {code:?}"),
            TxError::Transport(msg)            => write!(f, "transport: {msg}"),
        }
    }
}

/// Convert a hickory/IO error into a TxError.
/// Checks for ProtoErrorKind::Timeout first; everything else is Transport.
fn classify(e: Box<dyn std::error::Error + Send + Sync>) -> TxError {
    if let Some(proto) = e.downcast_ref::<hickory_proto::error::ProtoError>() {
        if matches!(proto.kind(), ProtoErrorKind::Timeout) {
            return TxError::Timeout;
        }
    }
    TxError::Transport(e.to_string())
}

/// Map TxError to the stats ErrorKind.
pub fn tx_error_to_error_kind(e: &TxError) -> crate::stats::ErrorKind {
    use crate::stats::ErrorKind;
    match e {
        TxError::Timeout                   => ErrorKind::Timeout,
        TxError::DnsRejected { code, .. }  => ErrorKind::DnsRejected { code: u16::from(*code) },
        TxError::Transport(_)              => ErrorKind::Transport,
    }
}

// ── Client builder ───────────────────────────────────────────────────────────

async fn build_client(
    server:    SocketAddr,
    tsig:      Option<TsigConfig>,
    transport: &TransportConfig,
    timeout:   Duration,
) -> Result<AsyncClient, Box<dyn std::error::Error + Send + Sync>> {
    let signer: Option<Arc<Signer>> = tsig.map(|t| {
        let tsigner = TSigner::new(t.secret, t.algorithm, t.key_name, 300)
            .expect("valid TSIG config");
        Arc::new(Signer::from(tsigner))
    });

    let bind_addr: Option<SocketAddr> = match transport.ip_version {
        IpVersion::V4   => Some("0.0.0.0:0".parse().unwrap()),
        IpVersion::V6   => Some("[::]:0".parse().unwrap()),
        IpVersion::Auto => None,
    };

    match transport.transport {
        Transport::Udp => {
            let conn = UdpClientConnection::with_bind_addr_and_timeout(
                server, bind_addr, timeout,
            )?;
            let (client, bg) = AsyncClient::connect(conn.new_stream(signer)).await?;
            tokio::spawn(bg);
            Ok(client)
        }
        Transport::Tcp => {
            let conn = TcpClientConnection::with_bind_addr_and_timeout(
                server, bind_addr, timeout,
            )?;
            let (client, bg) = AsyncClient::connect(conn.new_stream(signer)).await?;
            tokio::spawn(bg);
            Ok(client)
        }
    }
}

// ── DNS helpers ──────────────────────────────────────────────────────────────

async fn timed_create(
    client:  &mut AsyncClient,
    record:  Record,
    zone:    Name,
    leg:     &'static str,
) -> Result<Duration, TxError> {
    let start = std::time::Instant::now();
    let resp  = client.create(record, zone).await.map_err(classify)?;
    let elapsed = start.elapsed();
    match resp.response_code() {
        ResponseCode::NoError | ResponseCode::YXRRSet => Ok(elapsed),
        code => Err(TxError::DnsRejected { code, leg }),
    }
}

async fn timed_delete_rrset(
    client:  &mut AsyncClient,
    record:  Record,
    zone:    Name,
    leg:     &'static str,
) -> Result<Duration, TxError> {
    let start = std::time::Instant::now();
    let resp  = client.delete_rrset(record, zone).await.map_err(classify)?;
    let elapsed = start.elapsed();
    match resp.response_code() {
        ResponseCode::NoError | ResponseCode::NXRRSet => Ok(elapsed),
        code => Err(TxError::DnsRejected { code, leg }),
    }
}

/// Builds the in-addr.arpa PTR name for an IPv4 address.
pub fn ipv4_to_ptr_name(ip: Ipv4Addr) -> Name {
    let o = ip.octets();
    Name::from_str_relaxed(&format!("{}.{}.{}.{}.in-addr.arpa.", o[3], o[2], o[1], o[0]))
        .expect("ptr name is always valid")
}

// ── Public send helpers (used by integration tests) ─────────────────────────

pub async fn send_add_a(
    server:   SocketAddr,
    zone:     Name,
    hostname: Name,
    ip:       Ipv4Addr,
) -> Result<Duration, Box<dyn std::error::Error + Send + Sync>> {
    let mut client = build_client(server, None, &TransportConfig::default(), Duration::from_secs(5)).await?;
    let mut record = Record::new();
    record.set_name(hostname)
          .set_record_type(RecordType::A)
          .set_dns_class(DNSClass::IN)
          .set_ttl(300)
          .set_data(Some(RData::A(A(ip))));
    let start = std::time::Instant::now();
    let response = client.create(record, zone).await?;
    let elapsed = start.elapsed();
    match response.response_code() {
        ResponseCode::NoError | ResponseCode::YXRRSet => Ok(elapsed),
        code => Err(format!("server returned {:?}", code).into()),
    }
}

pub async fn send_add_a_tsig(
    server:   SocketAddr,
    zone:     Name,
    hostname: Name,
    ip:       Ipv4Addr,
    tsig:     TsigConfig,
) -> Result<Duration, Box<dyn std::error::Error + Send + Sync>> {
    let mut client = build_client(server, Some(tsig), &TransportConfig::default(), Duration::from_secs(5)).await?;
    let mut record = Record::new();
    record.set_name(hostname)
          .set_record_type(RecordType::A)
          .set_dns_class(DNSClass::IN)
          .set_ttl(300)
          .set_data(Some(RData::A(A(ip))));
    let start = std::time::Instant::now();
    let response = client.create(record, zone).await?;
    let elapsed = start.elapsed();
    match response.response_code() {
        ResponseCode::NoError | ResponseCode::YXRRSet => Ok(elapsed),
        code => Err(format!("server returned {:?}", code).into()),
    }
}

// ── Main transaction ─────────────────────────────────────────────────────────

/// Run a DDNS transaction with configurable legs.
///
/// Legs executed depend on flags:
///   include_ptr=false, include_delete=false  →  Add A
///   include_ptr=true,  include_delete=false  →  Add A → Add PTR
///   include_ptr=false, include_delete=true   →  Add A → Del A
///   include_ptr=true,  include_delete=true   →  Add A → Add PTR → Del PTR → Del A
pub async fn run_transaction(
    server:         SocketAddr,
    zone:           Name,
    ptr_zone:       Option<Name>,
    hostname:       Name,
    ip:             Ipv4Addr,
    tsig:           Option<TsigConfig>,
    transport:      TransportConfig,
    timeout:        Duration,
    include_ptr:    bool,
    include_delete: bool,
) -> Result<crate::stats::TxResult, TxError> {
    let mut client = build_client(server, tsig, &transport, timeout)
        .await
        .map_err(|e| classify(e))?;

    let ptr_name = ipv4_to_ptr_name(ip);

    // 1. Add A (always)
    let a_record = {
        let mut r = Record::new();
        r.set_name(hostname.clone())
         .set_record_type(RecordType::A)
         .set_dns_class(DNSClass::IN)
         .set_ttl(300)
         .set_data(Some(RData::A(A(ip))));
        r
    };
    let add_a = timed_create(&mut client, a_record, zone.clone(), "add_a").await?;

    // 2. Add PTR (optional)
    let add_ptr = if include_ptr {
        let pz = ptr_zone.clone().expect("ptr_zone required when include_ptr=true");
        let ptr_record = {
            let mut r = Record::new();
            r.set_name(ptr_name.clone())
             .set_record_type(RecordType::PTR)
             .set_dns_class(DNSClass::IN)
             .set_ttl(300)
             .set_data(Some(RData::PTR(PTR(hostname.clone()))));
            r
        };
        Some(timed_create(&mut client, ptr_record, pz, "add_ptr").await?)
    } else {
        None
    };

    // 3. Delete PTR (optional)
    let del_ptr = if include_ptr && include_delete {
        let pz = ptr_zone.expect("ptr_zone required when include_ptr=true");
        let del_ptr_record = {
            let mut r = Record::new();
            r.set_name(ptr_name)
             .set_record_type(RecordType::PTR)
             .set_dns_class(DNSClass::IN)
             .set_ttl(0);
            r
        };
        Some(timed_delete_rrset(&mut client, del_ptr_record, pz, "del_ptr").await?)
    } else {
        None
    };

    // 4. Delete A (optional)
    let del_a = if include_delete {
        let del_a_record = {
            let mut r = Record::new();
            r.set_name(hostname)
             .set_record_type(RecordType::A)
             .set_dns_class(DNSClass::IN)
             .set_ttl(0);
            r
        };
        Some(timed_delete_rrset(&mut client, del_a_record, zone, "del_a").await?)
    } else {
        None
    };

    Ok(crate::stats::TxResult {
        add_a_latency:   add_a,
        add_ptr_latency: add_ptr,
        del_ptr_latency: del_ptr,
        del_a_latency:   del_a,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;
    use std::net::SocketAddr;
    use std::str::FromStr;

    #[test]
    fn tx_error_to_kind_timeout() {
        use crate::stats::ErrorKind;
        let e = TxError::Timeout;
        assert_eq!(tx_error_to_error_kind(&e), ErrorKind::Timeout);
    }

    #[test]
    fn tx_error_to_kind_dns_rejected() {
        use crate::stats::ErrorKind;
        let e = TxError::DnsRejected { code: ResponseCode::Refused, leg: "add_a" };
        assert!(matches!(
            tx_error_to_error_kind(&e),
            ErrorKind::DnsRejected { code: 5 }
        ));
    }

    #[test]
    fn tx_error_to_kind_transport() {
        use crate::stats::ErrorKind;
        let e = TxError::Transport("connection reset".into());
        assert_eq!(tx_error_to_error_kind(&e), ErrorKind::Transport);
    }

    /// Requires BIND at 127.0.0.1:5353 — see README smoke test setup.
    /// Run with: cargo test test_run_transaction -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn test_run_transaction() {
        let server:   SocketAddr = "127.0.0.1:5353".parse().unwrap();
        let zone     = Name::from_str("test.local.").unwrap();
        let ptr_zone = Name::from_str("10.in-addr.arpa.").unwrap();
        let hostname = Name::from_str("tx-test.test.local.").unwrap();
        let ip: Ipv4Addr = "10.0.0.77".parse().unwrap();
        let secret = base64::engine::general_purpose::STANDARD
            .decode("i8ThSp4D0SYlHviiQCCxAs4qgZtisWG845b7ttlCT6w=")
            .unwrap();
        let tsig = TsigConfig {
            key_name:  Name::from_str("test-key.").unwrap(),
            algorithm: TsigAlgorithm::HmacSha256,
            secret,
        };
        let result = run_transaction(
            server, zone, Some(ptr_zone), hostname, ip, Some(tsig),
            TransportConfig::default(), Duration::from_secs(5),
            true, true,
        ).await.expect("transaction should succeed");
        crate::stats::print_report(&result);
        assert!(result.total().as_secs() < 10);
    }

    /// Requires BIND at 127.0.0.1:5353 allowing unauthenticated updates.
    /// Run with: cargo test test_send_add_a_unsigned -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn test_send_add_a_unsigned() {
        let server: SocketAddr = "127.0.0.1:5353".parse().unwrap();
        let zone = Name::from_str("test.local.").unwrap();
        let hostname = Name::from_str("spike-test.test.local.").unwrap();
        let ip: Ipv4Addr = "10.0.0.99".parse().unwrap();
        let elapsed = send_add_a(server, zone, hostname, ip)
            .await
            .expect("unsigned update should succeed");
        println!("RTT: {:?}", elapsed);
        assert!(elapsed.as_secs() < 5);
    }

    /// Requires BIND at 127.0.0.1:5353 with TSIG key "test-key".
    /// Run with: cargo test test_send_add_a_tsig -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn test_send_add_a_tsig() {
        let server: SocketAddr = "127.0.0.1:5353".parse().unwrap();
        let zone = Name::from_str("test.local.").unwrap();
        let hostname = Name::from_str("tsig-test.test.local.").unwrap();
        let ip: Ipv4Addr = "10.0.0.88".parse().unwrap();
        let secret = base64::engine::general_purpose::STANDARD
            .decode("i8ThSp4D0SYlHviiQCCxAs4qgZtisWG845b7ttlCT6w=")
            .expect("valid base64");
        let tsig = TsigConfig {
            key_name:  Name::from_str("test-key.").unwrap(),
            algorithm: TsigAlgorithm::HmacSha256,
            secret,
        };
        let elapsed = send_add_a_tsig(server, zone, hostname, ip, tsig)
            .await
            .expect("TSIG-signed update should succeed");
        println!("RTT: {:?}", elapsed);
        assert!(elapsed.as_secs() < 5);
    }
}
```

- [ ] **Step 4: Run dns unit tests**

```bash
cargo test dns::tests::tx_error 2>&1 | tail -8
```

Expected: `tx_error_to_kind_timeout`, `tx_error_to_kind_dns_rejected`, `tx_error_to_kind_transport` all pass.

- [ ] **Step 5: Commit**

```bash
git add src/dns.rs
git commit -m "feat(dns): TxError enum, typed helpers, optional PTR/delete legs, configurable timeout"
```

---

### Task 3: cli.rs — `--timeout`, `--delete`, remove ptr_zone inference

**Files:**
- Modify: `src/cli.rs`

**Interfaces:**
- Consumes: nothing new
- Produces:
  - `Args.timeout_ms: u64` (default `5000`)
  - `Args.delete: bool`
  - `Config.timeout_ms: u64`
  - `Config.delete: bool`
  - `Config.ptr_zone: Option<Name>` — no longer inferred from network

- [ ] **Step 1: Write failing CLI parse tests**

Add to `#[cfg(test)]` in `src/cli.rs`:

```rust
#[test]
fn timeout_defaults_to_5000() {
    let cfg = base_args().into_config().unwrap();
    assert_eq!(cfg.timeout_ms, 5000);
}

#[test]
fn delete_defaults_to_false() {
    let cfg = base_args().into_config().unwrap();
    assert!(!cfg.delete);
}

#[test]
fn ptr_zone_not_inferred_from_network() {
    // network provided but ptr_zone omitted → Config.ptr_zone is None
    let cfg = base_args().into_config().unwrap();
    assert!(cfg.ptr_zone.is_none());
}

#[test]
fn explicit_ptr_zone_is_parsed() {
    let mut a = base_args();
    a.ptr_zone = Some("0.0.10.in-addr.arpa.".to_string());
    let cfg = a.into_config().unwrap();
    assert!(cfg.ptr_zone.is_some());
}
```

- [ ] **Step 2: Run — expect failure (fields don't exist yet)**

```bash
cargo test cli::tests::timeout 2>&1 | grep "^error" | head -5
```

- [ ] **Step 3: Update cli.rs**

In `src/cli.rs`, make the following changes:

**Add to `Args` struct** (after the `concurrency` field):

```rust
    /// Per-message response timeout in milliseconds [default: 5000]
    #[arg(long, default_value_t = 5000)]
    pub timeout_ms: u64,

    /// Include delete legs in the transaction (Del PTR if --ptr-zone, Del A always).
    /// Without this flag only Add legs are sent; records accumulate in the zone.
    #[arg(long, default_value_t = false)]
    pub delete: bool,
```

**Add to `Config` struct**:

```rust
    pub timeout_ms: u64,
    pub delete:     bool,
```

**In `Args::into_config`** — remove the ptr_zone inference block entirely. Replace:

```rust
        let ptr_zone = self.ptr_zone.as_deref()
            .map(|s| Name::from_str(s).map_err(|e| format!("invalid --ptr-zone: {}", e)))
            .transpose()?;
```

(no change needed for parsing — it already returns `Option<Name>`)

**In the `Config { ... }` initialiser at the end of `into_config`**, add:

```rust
            timeout_ms: self.timeout_ms,
            delete:     self.delete,
```

**In `base_args()` in tests**, add the two new fields with defaults:

```rust
            timeout_ms: 5000,
            delete:     false,
```

- [ ] **Step 4: Run cli tests**

```bash
cargo test cli:: 2>&1 | tail -10
```

Expected: all cli tests pass.

- [ ] **Step 5: Commit**

```bash
git add src/cli.rs
git commit -m "feat(cli): --timeout and --delete flags; ptr_zone no longer inferred from network"
```

---

### Task 4: engine.rs — expand BenchmarkConfig, wire new types

**Files:**
- Modify: `src/engine.rs`

**Interfaces:**
- Consumes:
  - `crate::dns::{TxError, tx_error_to_error_kind, run_transaction}` (Task 2)
  - `crate::stats::{ErrorKind, Outcome}` (Task 1)
- Produces:
  - `BenchmarkConfig` gains: `ptr_zone: Option<Name>`, `timeout_ms: u64`, `include_ptr: bool`, `include_delete: bool`

- [ ] **Step 1: Update `BenchmarkConfig` and the task loop in `src/engine.rs`**

Replace the `BenchmarkConfig` struct definition:

```rust
pub struct BenchmarkConfig {
    pub server:         std::net::SocketAddr,
    pub zone:           hickory_proto::rr::Name,
    pub ptr_zone:       Option<hickory_proto::rr::Name>,
    pub generator:      Arc<RecordGenerator>,
    pub tsig:           Option<Arc<crate::dns::TsigConfig>>,
    pub concurrency:    usize,
    pub total:          Option<u64>,
    pub rps:            Option<u32>,
    pub cancel:         tokio::sync::watch::Receiver<bool>,
    pub transport:      crate::dns::TransportConfig,
    pub timeout_ms:     u64,
    pub include_ptr:    bool,
    pub include_delete: bool,
}
```

In the task loop inside `run_benchmark`, replace the `run_transaction` call and `Outcome` construction:

```rust
                let timeout     = std::time::Duration::from_millis(cfg.timeout_ms);
                let include_ptr = cfg.include_ptr;
                let include_delete = cfg.include_delete;
```

(clone these before spawning, alongside the other cloned values)

Replace the `run_transaction` call:

```rust
                let t0 = Instant::now();
                let result = crate::dns::run_transaction(
                    server,
                    zone.clone(),
                    ptr_zone.clone(),
                    rec.hostname,
                    rec.ip,
                    tsig,
                    transport.clone(),
                    timeout,
                    include_ptr,
                    include_delete,
                ).await;
                let latency_us = t0.elapsed().as_micros() as u64;

                let error = result.err().map(|e| crate::dns::tx_error_to_error_kind(&e));
                let _ = tx.send(Outcome { latency_us, error });
```

- [ ] **Step 2: Run engine tests**

```bash
cargo test engine:: 2>&1 | tail -8
```

Expected: all engine tests pass.

- [ ] **Step 3: Commit**

```bash
git add src/engine.rs
git commit -m "feat(engine): BenchmarkConfig gains timeout/include_ptr/include_delete; map TxError to ErrorKind"
```

---

### Task 5: perf.rs + main.rs — final wiring

**Files:**
- Modify: `src/perf.rs`, `src/main.rs`

**Interfaces:**
- Consumes: all previous tasks

- [ ] **Step 1: Update perf.rs**

In `src/perf.rs`, the task loop sends `Outcome` and constructs `RunReport` inline. Two changes needed:

**Change `Outcome` construction** (line ~122) from:

```rust
let _ = outcome_tx.send(crate::stats::Outcome { latency_us: lat, success: ok });
```

to:

```rust
let error = if ok { None } else { Some(crate::stats::ErrorKind::Transport) };
let _ = outcome_tx.send(crate::stats::Outcome { latency_us: lat, error });
```

**Change the `ptr_zone` clone** at the top of `run_perf_test` — `BenchmarkConfig.ptr_zone` is now `Option<Name>`:

```rust
let ptr_zone = cfg.bench.ptr_zone.clone(); // already Option<Name>, no change needed in type
```

In the task loop, the `run_transaction` call needs the new arguments. Replace it:

```rust
                let ok = crate::dns::run_transaction(
                    server, zone.clone(), ptr_zone.clone(),
                    rec.hostname, rec.ip, tsig, transport.clone(),
                    std::time::Duration::from_millis(5000), // perf test uses default timeout
                    cfg.bench.include_ptr,
                    cfg.bench.include_delete,
                ).await.is_ok();
```

Wait — `cfg` is not in scope inside the spawned task. The `include_ptr` / `include_delete` / timeout must be cloned out before `tokio::spawn`. Add before the task loop:

```rust
    let include_ptr    = cfg.bench.include_ptr;
    let include_delete = cfg.bench.include_delete;
    let timeout_ms     = cfg.bench.timeout_ms;
```

Then clone into each task:

```rust
        let include_ptr    = include_ptr;
        let include_delete = include_delete;
        let timeout        = std::time::Duration::from_millis(timeout_ms);
```

**Expand the inline `RunReport` literal** to include the four new fields (all zero — perf test doesn't classify errors):

```rust
        final_report: RunReport {
            duration:        search_duration,
            total_sent:      total,
            total_ok,
            total_err,
            min_us:          if lat_n == 0 { 0 } else { lat_min },
            mean_us,
            max_us:          lat_max,
            throughput,
            concurrency:     0,
            total_timeout:   0,
            total_dns_error: 0,
            total_transport: 0,
            dns_codes:       vec![],
        },
```

- [ ] **Step 2: Update main.rs**

`main.rs` must:

1. Remove the `ptr_zone` inference block in benchmark mode. The inference is:

```rust
        let ptr_zone = config.ptr_zone.unwrap_or_else(|| {
            let octets = network.network().octets();
            let zone_str = match network.prefix_len() { ... };
            hickory_proto::rr::Name::from_str_relaxed(&zone_str).expect("valid")
        });
```

Replace with:

```rust
        let ptr_zone = config.ptr_zone; // Option<Name> — None means no PTR legs
```

2. Update `make_bench_cfg` signature and body to pass `timeout_ms`, `include_ptr`, `include_delete`, and `ptr_zone: Option<Name>`.

Replace the `make_bench_cfg` function:

```rust
fn make_bench_cfg(
    server:         std::net::SocketAddr,
    zone:           hickory_proto::rr::Name,
    ptr_zone:       Option<hickory_proto::rr::Name>,
    generator:      Arc<records::RecordGenerator>,
    tsig:           Option<Arc<dns::TsigConfig>>,
    concurrency:    usize,
    total:          Option<u64>,
    rps:            Option<u32>,
    transport:      dns::TransportConfig,
    cancel:         tokio::sync::watch::Receiver<bool>,
    timeout_ms:     u64,
    include_ptr:    bool,
    include_delete: bool,
) -> engine::BenchmarkConfig {
    engine::BenchmarkConfig {
        server, zone, ptr_zone, generator, tsig,
        concurrency, total, rps, transport, cancel,
        timeout_ms, include_ptr, include_delete,
    }
}
```

3. Update all call sites of `make_bench_cfg` to pass the new arguments. There are three call sites (perf phase, `--rps auto` benchmark phase, normal benchmark). For each, add after `cancel_rx`:

```rust
            config.timeout_ms,
            config.ptr_zone.is_some(), // include_ptr
            config.delete,             // include_delete
```

4. Update the single-shot path to pass the new `run_transaction` arguments:

```rust
        match dns::run_transaction(
            config.server, config.zone, config.ptr_zone,
            hostname, ip, config.tsig, config.transport,
            std::time::Duration::from_millis(config.timeout_ms),
            config.ptr_zone.is_some(), // include_ptr — but ptr_zone was moved above!
            config.delete,
        ).await {
```

To avoid moving `config.ptr_zone` twice, resolve include_ptr before the call:

```rust
        let include_ptr = config.ptr_zone.is_some();
        match dns::run_transaction(
            config.server, config.zone, config.ptr_zone,
            hostname, ip, config.tsig, config.transport,
            std::time::Duration::from_millis(config.timeout_ms),
            include_ptr,
            config.delete,
        ).await {
```

- [ ] **Step 3: Run full test suite**

```bash
cargo test 2>&1 | tail -15
```

Expected: 25+ unit tests pass, 3 integration tests ignored, 0 failures.

- [ ] **Step 4: Build release**

```bash
cargo build --release 2>&1 | grep -E "^error|Finished"
```

Expected: `Finished` with warnings only.

- [ ] **Step 5: Smoke-test the new flags**

```bash
# Add A only (default — no PTR, no delete)
./target/release/ddnsperf \
  --server 127.0.0.1:5353 \
  --zone test.local. \
  --network 10.0.0.0/24 \
  --requests 5 --concurrency 1 \
  --tsig-name test-key. \
  --tsig-secret "i8ThSp4D0SYlHviiQCCxAs4qgZtisWG845b7ttlCT6w="
# Expected: Successful: 5, no PTR/delete legs in single-shot output

# Full transaction (--delete + --ptr-zone)
./target/release/ddnsperf \
  --server 127.0.0.1:5353 \
  --zone test.local. \
  --network 10.0.0.0/24 \
  --ptr-zone "10.in-addr.arpa." \
  --delete \
  --requests 5 --concurrency 1 \
  --tsig-name test-key. \
  --tsig-secret "i8ThSp4D0SYlHviiQCCxAs4qgZtisWG845b7ttlCT6w="
# Expected: Successful: 5

# Overload test — short timeout, high concurrency
./target/release/ddnsperf \
  --server 127.0.0.1:5353 \
  --zone test.local. \
  --network 10.0.0.0/16 \
  --requests 200 --concurrency 50 \
  --timeout 150 \
  --tsig-name test-key. \
  --tsig-secret "i8ThSp4D0SYlHviiQCCxAs4qgZtisWG845b7ttlCT6w="
# Expected: mix of Successful + Timeout, error breakdown visible
```

- [ ] **Step 6: Commit**

```bash
git add src/perf.rs src/main.rs
git commit -m "feat(main,perf): wire --timeout, --delete, optional ptr_zone through all call sites"
```

- [ ] **Step 7: Update README**

In `README.md`:

- Add `--timeout <ms>` and `--delete` to the **All Options** table.
- Update the smoke test command in **Smoke Test Setup → Step 6** to include `--delete --ptr-zone "10.in-addr.arpa."` (the current container uses `10.in-addr.arpa`).
- Add an **Overload testing** subsection after the **Stub DNS Server** section showing a representative overload command.

```bash
git add README.md
git commit -m "docs: --timeout, --delete, overload example in README"
```

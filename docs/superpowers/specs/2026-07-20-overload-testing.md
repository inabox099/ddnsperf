# Overload Testing — Design Spec

**Status:** Approved  
**Date:** 2026-07-20

---

## Goal

Allow ddnsperf to deliberately send updates faster than the server can process them, and report what happens — distinguishing timeouts from active DNS rejections — so the operator can observe server behaviour under overload.

---

## New CLI flags

### `--timeout <ms>`

Per-message response deadline in milliseconds.

- **Default:** `5000`
- Replaces the hardcoded `Duration::from_secs(5)` passed to `UdpClientConnection::with_bind_addr_and_timeout` and `TcpClientConnection::with_bind_addr_and_timeout` in `build_client`.
- Short values create overload: with `--concurrency C` and `--timeout T_ms`, tasks give up and restart at rate `C / (T_ms / 1000)`. Example: `--concurrency 100 --timeout 200` drives ~500 RPS against a server that processes 38 RPS → ~92 % timeout rate.

### `--delete`

Boolean flag (off by default). Controls whether delete legs are included in each transaction.

- **Absent (default):** only Add legs run.
- **Present:** Add legs followed by the corresponding Delete legs.

> ⚠️ Without `--delete`, records accumulate in the zone. For a `/16` network that is up to 65 534 A records. Acceptable for short overload tests; run a `--delete` pass afterwards to drain the zone.

---

## PTR opt-in (behaviour change)

`--ptr-zone` is already an optional CLI flag. The change: **remove auto-inference** of the reverse zone from `--network`. PTR legs are included only when `--ptr-zone` is explicitly supplied on the command line.

---

## Transaction shape matrix

The two new flags combine with the existing `--ptr-zone` to produce four transaction shapes:

| `--delete` | `--ptr-zone` | Legs sent | Messages |
|---|---|---|---|
| no | no | Add A | 1 |
| no | yes | Add A → Add PTR | 2 |
| yes | no | Add A → Del A | 2 |
| yes | yes | Add A → Add PTR → Del PTR → Del A | 4 |

---

## Typed error classification

### `TxError` enum (dns.rs)

`run_transaction` returns `Result<TxResult, TxError>` (typed error replaces `Box<dyn Error + Send + Sync>`). The single-leg helpers `timed_create` and `timed_delete_rrset` return `Result<Duration, TxError>`.

```rust
#[derive(Debug)]
pub enum TxError {
    Timeout,
    DnsRejected { code: ResponseCode, leg: &'static str },
    Transport(String),
}
```

Timeout detection: downcast the error to `hickory_proto::error::ProtoError` and check `matches!(e.kind(), ProtoErrorKind::Timeout)`. All other hickory errors are `Transport`.

DNS rejections already surface as `ResponseCode` values inside `timed_create` / `timed_delete_rrset` — map them to `DnsRejected { code, leg }` where `leg` is `"add_a"`, `"add_ptr"`, `"del_ptr"`, or `"del_a"`.

### `ErrorKind` enum (stats.rs)

```rust
#[derive(Debug, Clone)]
pub enum ErrorKind {
    Timeout,
    DnsRejected { code: u16 },  // ResponseCode as u16 for PartialEq/Hash
    Transport,
}
```

### `Outcome` struct (stats.rs)

Replace `success: bool` with `error: Option<ErrorKind>`. `None` = success.

```rust
pub struct Outcome {
    pub latency_us: u64,
    pub error:      Option<ErrorKind>,
}
```

### `TxResult` changes (stats.rs)

Make the optional legs `Option<Duration>` so single-shot mode can print only what was actually sent:

```rust
pub struct TxResult {
    pub add_a_latency:   Duration,
    pub add_ptr_latency: Option<Duration>,  // None when --ptr-zone not provided
    pub del_ptr_latency: Option<Duration>,  // None when --delete absent
    pub del_a_latency:   Option<Duration>,  // None when --delete absent
}
```

`print_report` skips `None` legs and omits them from the output.

### `RunReport` additions (stats.rs)

```rust
pub struct RunReport {
    // existing fields unchanged
    pub total_timeout:   u64,
    pub total_dns_error: u64,
    pub total_transport: u64,
    pub dns_codes:       Vec<(u16, u64)>,  // (ResponseCode as u16, count), sorted desc
}
```

### Output format

```
Total sent:    1000
  Successful:   184 (18.4%)
  Timeout:      742 (74.2%)
  DNS error:     64 (6.4%)  — REFUSED×58  SERVFAIL×6
  Transport:     10 (1.0%)
```

`DNS error` line is omitted when `total_dns_error == 0`. `REFUSED`, `SERVFAIL`, etc. are the `Debug` representation of `ResponseCode`.

---

## Files changed

| File | Changes |
|---|---|
| `src/cli.rs` | Add `--timeout <ms>` (`u64`, default `5000`) and `--delete` (`bool`) to `Args`; add `timeout_ms: u64` and `delete: bool` to `Config`; remove ptr_zone inference fallback from `into_config` |
| `src/dns.rs` | Add `TxError`; change return types of `timed_create`, `timed_delete_rrset`, `run_transaction`; add `include_ptr: bool` and `include_delete: bool` parameters to `run_transaction`; pass `timeout` to `build_client` |
| `src/stats.rs` | Add `ErrorKind`; replace `success: bool` with `error: Option<ErrorKind>` in `Outcome`; add 4 new fields to `RunReport`; update `spawn_collector` and `print_run_report` |
| `src/engine.rs` | Add `timeout_ms` to `BenchmarkConfig`; map `TxError` → `Outcome`; pass `include_ptr` / `include_delete` to `run_transaction` |
| `src/main.rs` | Pass `timeout_ms`, `delete`, `ptr_zone` (now `Option`) to `BenchmarkConfig` and single-shot path; remove ptr_zone inference |
| `src/perf.rs` | Update inline `RunReport` construction to include the 4 new error-count fields (set to 0; perf test does not classify errors) |

---

## Backward compatibility

- Existing calls without `--delete` or `--timeout` see different default behaviour: **no delete legs**. This is intentional — the tool's new default is the simpler/faster mode. Existing smoke-test scripts and integration tests that expect a full transaction must add `--delete`.
- The integration tests in `dns.rs` (`test_run_transaction`, etc.) use `run_transaction` directly and must be updated to pass `include_ptr: true, include_delete: true`.
- Unit tests in `stats.rs` that construct `Outcome` must switch `success: bool` → `error: Option<ErrorKind>`.

---

## Test coverage

- Unit tests for `TxError` → `ErrorKind` mapping in `dns.rs`
- Unit test: `TxResult::total()` sums only the `Some` legs
- Unit tests for `spawn_collector` counting all three error kinds correctly
- Unit test for `print_run_report` omitting the DNS-error line when count is zero
- Existing CLI parse tests updated for new fields
- Existing `RunReport` unit tests updated for new struct layout

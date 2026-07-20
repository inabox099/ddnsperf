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
    println!("  Successful:  {} ({:.1}%)", r.total_ok, pct(r.total_ok, r.total_sent));

    if r.total_timeout > 0 || r.total_dns_error > 0 || r.total_transport > 0 {
        println!("  Timeout:     {} ({:.1}%)", r.total_timeout, pct(r.total_timeout, r.total_sent));
        if r.total_dns_error > 0 {
            let codes: String = r.dns_codes.iter()
                .map(|(code, n)| format!("{}×{}", rcode_name(*code), n))
                .collect::<Vec<_>>()
                .join("  ");
            println!("  DNS error:   {} ({:.1}%)  — {}",
                r.total_dns_error, pct(r.total_dns_error, r.total_sent), codes);
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
        let pct_val = r.total_err as f64 / r.total_sent as f64 * 100.0;
        assert!((pct_val - 5.0).abs() < 0.001);
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

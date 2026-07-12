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

        while let Some(o) = rx.recv().await {
            total_sent += 1;
            if o.success { total_ok += 1; } else { total_err += 1; }
            if o.latency_us < min_us { min_us = o.latency_us; }
            if o.latency_us > max_us { max_us = o.latency_us; }
            // Welford's online mean
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
    println!("=== ddnsperf results ===");
    println!("Duration:      {:.3}s", r.duration.as_secs_f64());
    println!("Total sent:    {}", r.total_sent);
    println!("  Successful:  {} ({:.1}%)", r.total_ok,  pct(r.total_ok,  r.total_sent));
    println!("  Errors:      {} ({:.1}%)", r.total_err, pct(r.total_err, r.total_sent));
    println!("Throughput:    {:.0} RPS", r.throughput);
    println!("Latency:");
    println!("  Min:         {:.3}ms", r.min_us  as f64 / 1000.0);
    println!("  Mean:        {:.3}ms", r.mean_us       / 1000.0);
    println!("  Max:         {:.3}ms", r.max_us  as f64 / 1000.0);
}

fn pct(n: u64, total: u64) -> f64 {
    if total == 0 { 0.0 } else { n as f64 / total as f64 * 100.0 }
}

// ── Legacy single-transaction types (kept for single-shot mode) ──────────────

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
        drop(tx);

        let report = handle.await.unwrap();
        assert_eq!(report.total_sent, 10);
        assert_eq!(report.total_ok,    8);
        assert_eq!(report.total_err,   2);
        assert_eq!(report.min_us,    100);
        assert_eq!(report.max_us,   1000);
        // mean of 100,200,...,1000 = 550
        assert!((report.mean_us - 550.0).abs() < 1.0);
    }
}

use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use governor::{Quota, RateLimiter};
use governor::clock::DefaultClock;
use governor::state::{InMemoryState, NotKeyed};
use indicatif::ProgressBar;
use tokio::sync::RwLock;

use crate::engine::BenchmarkConfig;
use crate::stats::RunReport;

// ── PID controller ────────────────────────────────────────────────────────────

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

    /// `pv` is current error rate %. Returns signed RPS delta (positive = increase).
    pub fn update(&mut self, pv: f64) -> f64 {
        let error      = self.setpoint - pv;
        self.integral += error;
        let derivative = error - self.prev_error;
        self.prev_error = error;
        self.kp * error + self.ki * self.integral + self.kd * derivative
    }
}

// ── Perf test ─────────────────────────────────────────────────────────────────

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

pub async fn run_perf_test(cfg: PerfConfig, progress: ProgressBar) -> PerfResult {
    const START_RPS:    u32   = 100;
    const SAMPLE_MS:    u64   = 500;
    const CONVERGE_N:   usize = 5;
    const CONVERGE_PCT: f64   = 0.02;

    let start   = Instant::now();
    let max_rps = cfg.max_rps.unwrap_or(50_000);

    let shared: SharedLimiter = Arc::new(RwLock::new(make_limiter(START_RPS)));

    let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);

    let server      = cfg.bench.server;
    let zone        = cfg.bench.zone.clone();
    let ptr_zone    = cfg.bench.ptr_zone.clone();
    let generator   = cfg.bench.generator.clone();
    let tsig_arc    = cfg.bench.tsig.clone();
    let transport   = cfg.bench.transport.clone();
    let concurrency = cfg.bench.concurrency;

    let sent_ok  = Arc::new(AtomicU64::new(0));
    let sent_err = Arc::new(AtomicU64::new(0));
    let (outcome_tx, mut outcome_rx) =
        tokio::sync::mpsc::unbounded_channel::<crate::stats::Outcome>();

    let mut task_handles = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        let outcome_tx = outcome_tx.clone();
        let gen        = generator.clone();
        let zone       = zone.clone();
        let ptr_zone   = ptr_zone.clone();
        let tsig_arc   = tsig_arc.clone();
        let transport  = transport.clone();
        let shared     = shared.clone();
        let cancel     = cancel_rx.clone();
        let sent_ok    = sent_ok.clone();
        let sent_err   = sent_err.clone();
        let pb         = progress.clone();

        task_handles.push(tokio::spawn(async move {
            loop {
                if *cancel.borrow() { break; }

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

                if ok { sent_ok.fetch_add(1, Ordering::Relaxed); }
                else  { sent_err.fetch_add(1, Ordering::Relaxed); }
                let _ = outcome_tx.send(crate::stats::Outcome { latency_us: lat, success: ok });
                pb.inc(1);
            }
        }));
    }

    // PID loop
    let mut pid = Pid::new(50.0, 5.0, 10.0, cfg.error_target);
    let mut current_rps     = START_RPS as f64;
    let mut converge_streak = 0usize;
    let mut last_ok:  u64   = 0;
    let mut last_err: u64   = 0;

    loop {
        tokio::time::sleep(Duration::from_millis(SAMPLE_MS)).await;

        if start.elapsed() >= cfg.duration { break; }

        let ok  = sent_ok.load(Ordering::Relaxed);
        let err = sent_err.load(Ordering::Relaxed);
        let delta_ok  = ok  - last_ok;
        let delta_err = err - last_err;
        last_ok  = ok;
        last_err = err;
        let total      = delta_ok + delta_err;
        let error_rate = if total == 0 { 0.0 }
                         else { delta_err as f64 / total as f64 * 100.0 };

        let delta   = pid.update(error_rate);
        let new_rps = (current_rps + delta).max(1.0).min(max_rps as f64);
        let change  = (new_rps - current_rps).abs() / current_rps.max(1.0);
        current_rps = new_rps;

        { *shared.write().await = make_limiter(current_rps as u32); }

        if change < CONVERGE_PCT {
            converge_streak += 1;
            if converge_streak >= CONVERGE_N { break; }
        } else {
            converge_streak = 0;
        }
    }

    let converged       = converge_streak >= CONVERGE_N;
    let search_duration = start.elapsed();

    let _ = cancel_tx.send(true);
    for h in task_handles { let _ = h.await; }
    drop(outcome_tx);

    // Build report from atomics (outcome channel may have some in flight; drain what we can)
    let mut lat_min = u64::MAX;
    let mut lat_max = 0u64;
    let mut lat_sum = 0u64;
    let mut lat_n   = 0u64;
    while let Ok(o) = outcome_rx.try_recv() {
        lat_n   += 1;
        lat_sum += o.latency_us;
        if o.latency_us < lat_min { lat_min = o.latency_us; }
        if o.latency_us > lat_max { lat_max = o.latency_us; }
    }

    let total_ok  = sent_ok.load(Ordering::Relaxed);
    let total_err = sent_err.load(Ordering::Relaxed);
    let total     = total_ok + total_err;
    let throughput = total as f64 / search_duration.as_secs_f64().max(0.001);
    let mean_us    = if lat_n == 0 { 0.0 } else { lat_sum as f64 / lat_n as f64 };

    progress.finish_with_message("done");

    PerfResult {
        max_sustainable_rps: current_rps as u32,
        converged,
        search_duration,
        final_report: RunReport {
            duration:   search_duration,
            total_sent: total,
            total_ok,
            total_err,
            min_us:     if lat_n == 0 { 0 } else { lat_min },
            mean_us,
            max_us:     lat_max,
            throughput,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pid_increases_when_below_setpoint() {
        let mut pid = Pid::new(50.0, 5.0, 10.0, 1.0);
        let output = pid.update(0.0);
        assert!(output > 0.0, "output {output} should be positive when pv < setpoint");
    }

    #[test]
    fn pid_decreases_when_above_setpoint() {
        let mut pid = Pid::new(50.0, 5.0, 10.0, 1.0);
        let output = pid.update(10.0);
        assert!(output < 0.0, "output {output} should be negative when pv > setpoint");
    }

    #[test]
    fn pid_zero_at_setpoint() {
        let mut pid = Pid::new(50.0, 5.0, 10.0, 1.0);
        let output = pid.update(1.0);
        assert_eq!(output, 0.0);
    }
}

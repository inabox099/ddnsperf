use std::num::NonZeroU32;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use governor::{Quota, RateLimiter};
use governor::clock::DefaultClock;
use governor::state::{InMemoryState, NotKeyed};
use indicatif::ProgressBar;
use tokio::sync::mpsc::unbounded_channel;

use crate::records::RecordGenerator;
use crate::stats::{Outcome, RunReport, spawn_collector};

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

type Limiter = RateLimiter<NotKeyed, InMemoryState, DefaultClock>;

pub async fn run_benchmark(cfg: BenchmarkConfig, progress: ProgressBar) -> RunReport {
    let (tx, rx) = unbounded_channel::<Outcome>();
    let start = Instant::now();
    let collector = spawn_collector(rx, start);

    let limiter: Option<Arc<Limiter>> = cfg.rps.and_then(|r| {
        NonZeroU32::new(r).map(|nz| Arc::new(RateLimiter::direct(Quota::per_second(nz))))
    });

    let sent  = Arc::new(AtomicU64::new(0));
    let total = cfg.total.unwrap_or(u64::MAX);

    let ptr_zone    = cfg.ptr_zone.clone();
    let timeout_ms     = cfg.timeout_ms;
    let include_ptr    = cfg.include_ptr;
    let include_delete = cfg.include_delete;

    let mut handles = Vec::with_capacity(cfg.concurrency);

    for _ in 0..cfg.concurrency {
        let tx         = tx.clone();
        let gen        = cfg.generator.clone();
        let zone       = cfg.zone.clone();
        let ptr_zone   = ptr_zone.clone();
        let server     = cfg.server;
        let limiter    = limiter.clone();
        let sent       = sent.clone();
        let tsig_arc   = cfg.tsig.clone();
        let pb         = progress.clone();
        let cancel     = cfg.cancel.clone();
        let transport  = cfg.transport.clone();
        let timeout    = std::time::Duration::from_millis(timeout_ms);
        let include_ptr    = include_ptr;
        let include_delete = include_delete;

        handles.push(tokio::spawn(async move {
            loop {
                if *cancel.borrow() { break; }
                let n = sent.fetch_add(1, Ordering::Relaxed);
                if n >= total {
                    sent.fetch_sub(1, Ordering::Relaxed);
                    break;
                }

                if let Some(ref lim) = limiter {
                    lim.until_ready().await;
                }

                let rec  = gen.next();
                let tsig = tsig_arc.as_deref().cloned();

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
                pb.inc(1);
            }
        }));
    }

    for h in handles {
        let _ = h.await;
    }

    drop(tx);
    progress.finish_with_message("done");

    let mut report = collector.await.expect("stats collector panicked");
    report.concurrency = cfg.concurrency;
    report
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

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
}

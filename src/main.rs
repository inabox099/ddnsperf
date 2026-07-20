mod cli;
mod dns;
mod engine;
mod perf;
mod records;
mod stats;

use std::sync::Arc;
use std::time::Duration;
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};

fn build_progress_bar(requests: Option<u64>) -> ProgressBar {
    if let Some(n) = requests {
        let pb = ProgressBar::new(n);
        pb.set_style(
            ProgressStyle::with_template(
                "[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len}  {per_sec} tx/s"
            )
            .unwrap()
            .progress_chars("█░"),
        );
        pb
    } else {
        let pb = ProgressBar::new_spinner();
        pb.set_style(
            ProgressStyle::with_template("[{elapsed_precise}] {spinner} {pos} sent").unwrap(),
        );
        pb
    }
}

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

#[tokio::main]
async fn main() {
    let args = cli::Args::parse();
    let config = match args.into_config() {
        Ok(c) => c,
        Err(e) => { eprintln!("error: {}", e); std::process::exit(1); }
    };

    // ── Benchmark mode: --network provided ───────────────────────────────────
    if let Some(network) = config.network {
        let ptr_zone    = config.ptr_zone; // None = no PTR legs
        let include_ptr = ptr_zone.is_some();
        let include_delete = config.delete;
        let timeout_ms  = config.timeout_ms;

        let generator = Arc::new(records::RecordGenerator::new(
            network, config.prefix, config.zone.clone(),
            config.mode == cli::Mode::Random,
        ));
        let tsig_arc = config.tsig.map(Arc::new);

        if config.perf_test || config.rps_auto {
            // ── Perf test phase ──────────────────────────────────────────────
            let (cancel_tx_perf, cancel_rx_perf) = tokio::sync::watch::channel(false);
            let perf_pb = ProgressBar::new_spinner();
            perf_pb.set_style(
                ProgressStyle::with_template(
                    "[{elapsed_precise}] {spinner} perf test — {pos} tx sent"
                ).unwrap()
            );

            let perf_cfg = perf::PerfConfig {
                bench: make_bench_cfg(
                    config.server, config.zone.clone(), ptr_zone.clone(),
                    generator.clone(), tsig_arc.clone(),
                    config.concurrency, None, None, config.transport.clone(),
                    cancel_rx_perf, timeout_ms, include_ptr, include_delete,
                ),
                error_target: config.error_target,
                max_rps:      config.max_rps_cap,
                duration:     Duration::from_secs(config.perf_duration),
            };
            // Suppress unused-variable warning — cancel_tx_perf kept alive so
            // channel stays open; perf module cancels via its own internal tx.
            let _ = cancel_tx_perf;

            let result = perf::run_perf_test(perf_cfg, perf_pb).await;

            println!("\n[perf-test phase]");
            println!("Max sustainable RPS: {}  ({}converged in {:.1}s, error target: {:.1}%)",
                result.max_sustainable_rps,
                if result.converged { "" } else { "NOT " },
                result.search_duration.as_secs_f64(),
                config.error_target,
            );
            stats::print_run_report(&result.final_report);

            if config.rps_auto {
                // ── Benchmark phase at discovered RPS ────────────────────────
                println!("\n[benchmark phase at {} RPS]", result.max_sustainable_rps);
                let (cancel_tx2, cancel_rx2) = tokio::sync::watch::channel(false);
                if let Some(secs) = config.duration {
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_secs(secs)).await;
                        let _ = cancel_tx2.send(true);
                    });
                }
                let bench_cfg2 = make_bench_cfg(
                    config.server, config.zone, ptr_zone,
                    generator, tsig_arc,
                    config.concurrency, config.requests,
                    Some(result.max_sustainable_rps),
                    config.transport, cancel_rx2,
                    timeout_ms, include_ptr, include_delete,
                );
                let pb2 = build_progress_bar(config.requests);
                let report = engine::run_benchmark(bench_cfg2, pb2).await;
                stats::print_run_report(&report);
            }
        } else {
            // ── Normal benchmark ─────────────────────────────────────────────
            let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
            if let Some(secs) = config.duration {
                tokio::spawn(async move {
                    tokio::time::sleep(Duration::from_secs(secs)).await;
                    let _ = cancel_tx.send(true);
                });
            }
            let bench_cfg = make_bench_cfg(
                config.server, config.zone, ptr_zone, generator, tsig_arc,
                config.concurrency, config.requests, config.rps,
                config.transport, cancel_rx,
                timeout_ms, include_ptr, include_delete,
            );
            let pb = build_progress_bar(config.requests);
            let report = engine::run_benchmark(bench_cfg, pb).await;
            stats::print_run_report(&report);
        }

    // ── Single-shot mode ──────────────────────────────────────────────────────
    } else {
        let hostname    = config.hostname.expect("hostname required");
        let ip          = config.ip.expect("ip required");
        let include_ptr = config.ptr_zone.is_some();
        let timeout     = Duration::from_millis(config.timeout_ms);
        match dns::run_transaction(
            config.server, config.zone, config.ptr_zone,
            hostname, ip, config.tsig, config.transport,
            timeout, include_ptr, config.delete,
        ).await {
            Ok(result) => stats::print_report(&result),
            Err(e)     => { eprintln!("error: {}", e); std::process::exit(1); }
        }
    }
}

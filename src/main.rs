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

    // ── Benchmark mode: --network provided ───────────────────────────────────
    if let Some(network) = config.network {
        let ptr_zone = config.ptr_zone.unwrap_or_else(|| {
            let octets = network.network().octets();
            let zone_str = match network.prefix_len() {
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

        let pb = if let Some(n) = config.requests {
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
                ProgressStyle::with_template("[{elapsed_precise}] {spinner} {pos} sent")
                    .unwrap(),
            );
            pb
        };

        let bench_cfg = engine::BenchmarkConfig {
            server:      config.server,
            zone:        config.zone,
            ptr_zone,
            generator,
            tsig:        config.tsig.map(Arc::new),
            concurrency: config.concurrency,
            total:       config.requests,
            rps:         config.rps,
        };

        let report = engine::run_benchmark(bench_cfg, pb).await;
        stats::print_run_report(&report);

    // ── Single-shot mode: --hostname + --ip provided ──────────────────────────
    } else {
        let hostname = config.hostname.expect("hostname required in single-shot mode");
        let ip       = config.ip.expect("ip required in single-shot mode");
        let ptr_zone = config.ptr_zone.unwrap_or_else(|| {
            // Derive /24 reverse zone from the IP (e.g. 10.0.0.55 → 0.0.10.in-addr.arpa.)
            let o = ip.octets();
            hickory_proto::rr::Name::from_str_relaxed(
                &format!("{}.{}.{}.in-addr.arpa.", o[2], o[1], o[0])
            ).expect("derived ptr zone is valid")
        });

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

mod cli;
mod dns;
mod engine;
mod records;
mod stats;

use std::sync::Arc;
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

        // Duration cancellation
        let (cancel_tx, cancel_rx) = tokio::sync::watch::channel(false);
        if let Some(secs) = config.duration {
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(secs)).await;
                let _ = cancel_tx.send(true);
            });
            // else: cancel_tx dropped → channel never fires
        }

        let bench_cfg = engine::BenchmarkConfig {
            server:      config.server,
            zone:        config.zone,
            ptr_zone,
            generator,
            tsig:        config.tsig.map(Arc::new),
            concurrency: config.concurrency,
            total:       config.requests,
            rps:         config.rps,
            cancel:      cancel_rx,
        };

        let pb = build_progress_bar(config.requests);
        let report = engine::run_benchmark(bench_cfg, pb).await;
        stats::print_run_report(&report);

    // ── Single-shot mode: --hostname + --ip provided ──────────────────────────
    } else {
        let hostname = config.hostname.expect("hostname required in single-shot mode");
        let ip       = config.ip.expect("ip required in single-shot mode");
        let ptr_zone = config.ptr_zone.unwrap_or_else(|| {
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

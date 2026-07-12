mod cli;
mod dns;
mod records;
mod stats;

use clap::Parser;

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

    match dns::run_transaction(
        config.server,
        config.zone,
        config.ptr_zone,
        config.hostname,
        config.ip,
        config.tsig,
    )
    .await
    {
        Ok(result) => stats::print_report(&result),
        Err(e) => {
            eprintln!("error: {}", e);
            std::process::exit(1);
        }
    }
}

use clap::Parser;

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Mode {
    Sequential,
    Random,
}

#[derive(Parser, Debug)]
#[command(name = "ddnsperf", about = "DDNS Update performance tool")]
pub struct Args {
    // ── Required ──────────────────────────────────────────────────────────
    /// DNS server address, e.g. 192.168.1.1:53 or [2001:db8::1]:53
    #[arg(short = 's', long)]
    pub server: String,

    /// DNS forward zone, e.g. example.com.
    #[arg(short = 'z', long)]
    pub zone: String,

    // ── Record source ──────────────────────────────────────────────────────
    /// Subnet to generate records from, e.g. 10.0.0.0/24  [benchmark mode]
    #[arg(long, conflicts_with_all = ["hostname", "ip"])]
    pub network: Option<String>,

    /// Hostname prefix used with --network (default: "host-")
    #[arg(long, default_value = "host-")]
    pub prefix: String,

    /// Record selection: sequential | random
    #[arg(long, default_value = "sequential")]
    pub mode: String,

    /// Reverse DNS zone (inferred from --network if omitted; required with --hostname)
    #[arg(long)]
    pub ptr_zone: Option<String>,

    /// Single hostname FQDN for single-shot mode (requires --ip and --ptr-zone)
    #[arg(long, conflicts_with = "network", requires = "ip")]
    pub hostname: Option<String>,

    /// IPv4 address for single-shot mode (requires --hostname)
    #[arg(long, conflicts_with = "network", requires = "hostname")]
    pub ip: Option<String>,

    // ── Load control ──────────────────────────────────────────────────────
    /// Total number of transactions to send [benchmark mode]
    #[arg(short = 'r', long)]
    pub requests: Option<u64>,

    /// Target transactions per second (omit for unlimited)
    #[arg(long)]
    pub rps: Option<u32>,

    /// Number of concurrent Tokio tasks (default: 50)
    #[arg(short = 'c', long, default_value_t = 50)]
    pub concurrency: usize,

    // ── TSIG ──────────────────────────────────────────────────────────────
    /// TSIG key name (requires --tsig-secret)
    #[arg(long, requires = "tsig_secret")]
    pub tsig_name: Option<String>,

    /// TSIG secret, base64-encoded (requires --tsig-name)
    #[arg(long, requires = "tsig_name")]
    pub tsig_secret: Option<String>,

    /// TSIG algorithm: hmac-md5 | hmac-sha1 | hmac-sha256
    #[arg(long, default_value = "hmac-sha256")]
    pub tsig_algo: String,
}

pub struct Config {
    pub server:      std::net::SocketAddr,
    pub zone:        hickory_proto::rr::Name,
    pub ptr_zone:    Option<hickory_proto::rr::Name>,
    pub hostname:    Option<hickory_proto::rr::Name>,
    pub ip:          Option<std::net::Ipv4Addr>,
    pub tsig:        Option<crate::dns::TsigConfig>,
    pub network:     Option<ipnet::Ipv4Net>,
    pub prefix:      String,
    pub mode:        Mode,
    pub requests:    Option<u64>,
    pub concurrency: usize,
    pub rps:         Option<u32>,
}

impl Args {
    pub fn into_config(self) -> Result<Config, String> {
        use std::str::FromStr;
        use hickory_proto::rr::Name;
        use hickory_proto::rr::dnssec::rdata::tsig::TsigAlgorithm;
        use base64::Engine as _;

        let server = self.server.parse::<std::net::SocketAddr>()
            .map_err(|e| format!("invalid --server: {}", e))?;

        let zone = Name::from_str(&self.zone)
            .map_err(|e| format!("invalid --zone: {}", e))?;

        let ptr_zone = self.ptr_zone.as_deref()
            .map(|s| Name::from_str(s).map_err(|e| format!("invalid --ptr-zone: {}", e)))
            .transpose()?;

        let hostname = self.hostname.as_deref()
            .map(|s| Name::from_str(s).map_err(|e| format!("invalid --hostname: {}", e)))
            .transpose()?;

        let ip = self.ip.as_deref()
            .map(|s| s.parse::<std::net::Ipv4Addr>().map_err(|e| format!("invalid --ip: {}", e)))
            .transpose()?;

        let network = self.network.as_deref()
            .map(|s| s.parse::<ipnet::Ipv4Net>().map_err(|e| format!("invalid --network: {}", e)))
            .transpose()?;

        let mode = match self.mode.as_str() {
            "sequential" => Mode::Sequential,
            "random"     => Mode::Random,
            other        => return Err(format!("invalid --mode '{}' (sequential|random)", other)),
        };

        if network.is_none() && (hostname.is_none() || ip.is_none()) {
            return Err("provide either --network or both --hostname and --ip".to_string());
        }

        let tsig = match (self.tsig_name, self.tsig_secret) {
            (Some(name), Some(secret)) => {
                let key_name = Name::from_str(&name)
                    .map_err(|e| format!("invalid --tsig-name: {}", e))?;
                let algorithm = match self.tsig_algo.as_str() {
                    "hmac-md5"    => TsigAlgorithm::HmacMd5,
                    "hmac-sha1"   => TsigAlgorithm::HmacSha1,
                    "hmac-sha256" => TsigAlgorithm::HmacSha256,
                    other => return Err(format!("unknown --tsig-algo: '{}'", other)),
                };
                let raw = base64::engine::general_purpose::STANDARD
                    .decode(&secret)
                    .map_err(|e| format!("invalid --tsig-secret (base64): {}", e))?;
                Some(crate::dns::TsigConfig { key_name, algorithm, secret: raw })
            }
            (None, None) => None,
            _ => unreachable!("clap enforces tsig_name and tsig_secret together"),
        };

        Ok(Config {
            server, zone, ptr_zone, hostname, ip, tsig,
            network, prefix: self.prefix, mode,
            requests: self.requests,
            concurrency: self.concurrency,
            rps: self.rps,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_args() -> Args {
        Args {
            server:      "127.0.0.1:53".to_string(),
            zone:        "example.com.".to_string(),
            ptr_zone:    None,
            network:     Some("10.0.0.0/24".to_string()),
            prefix:      "host-".to_string(),
            mode:        "sequential".to_string(),
            hostname:    None,
            ip:          None,
            requests:    Some(100),
            rps:         None,
            concurrency: 50,
            tsig_name:   None,
            tsig_secret: None,
            tsig_algo:   "hmac-sha256".to_string(),
        }
    }

    #[test]
    fn network_mode_parses() {
        let cfg = base_args().into_config().expect("should parse");
        assert!(cfg.network.is_some());
        assert_eq!(cfg.concurrency, 50);
    }

    #[test]
    fn single_shot_mode_parses() {
        let mut a = base_args();
        a.network  = None;
        a.hostname = Some("host.example.com.".to_string());
        a.ip       = Some("10.0.0.1".to_string());
        a.ptr_zone = Some("0.0.10.in-addr.arpa.".to_string());
        let cfg = a.into_config().expect("should parse");
        assert!(cfg.hostname.is_some());
        assert!(cfg.ip.is_some());
    }

    #[test]
    fn neither_network_nor_hostname_errors() {
        let mut a = base_args();
        a.network  = None;
        a.hostname = None;
        a.ip       = None;
        assert!(a.into_config().is_err());
    }

    #[test]
    fn invalid_ip_returns_error() {
        let mut a = base_args();
        a.network  = None;
        a.hostname = Some("h.example.com.".to_string());
        a.ip       = Some("not-an-ip".to_string());
        assert!(a.into_config().is_err());
    }

    #[test]
    fn unknown_tsig_algo_returns_error() {
        let mut a = base_args();
        a.tsig_name   = Some("key.".to_string());
        a.tsig_secret = Some("aGVsbG8=".to_string());
        a.tsig_algo   = "hmac-sha512".to_string();
        assert!(a.into_config().is_err());
    }

    #[test]
    fn invalid_mode_returns_error() {
        let mut a = base_args();
        a.mode = "zigzag".to_string();
        assert!(a.into_config().is_err());
    }
}

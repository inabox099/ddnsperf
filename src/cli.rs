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

    /// Run for this many seconds instead of a fixed request count
    #[arg(long, conflicts_with = "requests")]
    pub duration: Option<u64>,

    /// Target transactions per second, or 'auto' to run a perf test first
    #[arg(long)]
    pub rps: Option<String>,

    /// Number of concurrent Tokio tasks (default: 50)
    #[arg(short = 'c', long, default_value_t = 50)]
    pub concurrency: usize,

    // ── Perf test ──────────────────────────────────────────────────────────
    /// Find max sustainable throughput via PID (mutually exclusive with --rps)
    #[arg(long, conflicts_with = "rps")]
    pub perf_test: bool,

    /// Error rate setpoint for --perf-test or --rps auto (default: 1.0%)
    #[arg(long, default_value_t = 1.0)]
    pub error_target: f64,

    /// Safety cap on RPS during perf test
    #[arg(long)]
    pub max_rps_cap: Option<u32>,

    /// Duration of perf test search in seconds (default: 120)
    #[arg(long, default_value_t = 120)]
    pub perf_duration: u64,

    // ── Transport ─────────────────────────────────────────────────────────
    /// Use TCP transport (default: UDP)
    #[arg(long, conflicts_with = "udp")]
    pub tcp: bool,

    /// Use UDP transport (default)
    #[arg(long, conflicts_with = "tcp")]
    pub udp: bool,

    /// Force IPv4 transport to the DNS server
    #[arg(long, conflicts_with = "ipv6")]
    pub ipv4: bool,

    /// Force IPv6 transport to the DNS server
    #[arg(long, conflicts_with = "ipv4")]
    pub ipv6: bool,

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
    pub duration:    Option<u64>,
    pub rps:         Option<u32>,
    pub rps_auto:    bool,
    pub concurrency: usize,
    pub perf_test:   bool,
    pub error_target: f64,
    pub max_rps_cap: Option<u32>,
    pub perf_duration: u64,
    pub transport:   crate::dns::TransportConfig,
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

        let (rps, rps_auto) = match self.rps.as_deref() {
            None          => (None, false),
            Some("auto")  => (None, true),
            Some(n)       => (Some(n.parse::<u32>().map_err(|_| format!("invalid --rps: '{}'", n))?), false),
        };

        let transport = crate::dns::TransportConfig {
            transport: if self.tcp { crate::dns::Transport::Tcp } else { crate::dns::Transport::Udp },
            ip_version: match (self.ipv4, self.ipv6) {
                (true, _) => crate::dns::IpVersion::V4,
                (_, true) => crate::dns::IpVersion::V6,
                _         => crate::dns::IpVersion::Auto,
            },
        };

        Ok(Config {
            server, zone, ptr_zone, hostname, ip, tsig,
            network, prefix: self.prefix, mode,
            requests: self.requests,
            duration: self.duration,
            concurrency: self.concurrency,
            rps,
            rps_auto,
            perf_test:    self.perf_test,
            error_target: self.error_target,
            max_rps_cap:  self.max_rps_cap,
            perf_duration: self.perf_duration,
            transport,
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
            duration:    None,
            rps:         None,
            perf_test:   false,
            error_target: 1.0,
            max_rps_cap: None,
            perf_duration: 120,
            concurrency: 50,
            tcp:         false,
            udp:         false,
            ipv4:        false,
            ipv6:        false,
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

    #[test]
    fn default_transport_is_udp_auto() {
        let cfg = base_args().into_config().unwrap();
        assert!(matches!(cfg.transport.transport,  crate::dns::Transport::Udp));
        assert!(matches!(cfg.transport.ip_version, crate::dns::IpVersion::Auto));
    }

    #[test]
    fn tcp_flag_sets_tcp_transport() {
        let mut a = base_args();
        a.tcp = true;
        let cfg = a.into_config().unwrap();
        assert!(matches!(cfg.transport.transport, crate::dns::Transport::Tcp));
    }

    #[test]
    fn rps_auto_sets_flag() {
        let mut a = base_args();
        a.rps = Some("auto".to_string());
        let cfg = a.into_config().unwrap();
        assert!(cfg.rps_auto);
        assert!(cfg.rps.is_none());
    }

    #[test]
    fn rps_numeric_parses() {
        let mut a = base_args();
        a.rps = Some("200".to_string());
        let cfg = a.into_config().unwrap();
        assert_eq!(cfg.rps, Some(200));
        assert!(!cfg.rps_auto);
    }

    #[test]
    fn rps_invalid_errors() {
        let mut a = base_args();
        a.rps = Some("fast".to_string());
        assert!(a.into_config().is_err());
    }
}

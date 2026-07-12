use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "ddnsperf", about = "DDNS Update performance tool")]
pub struct Args {
    /// DNS server address, e.g. 192.168.1.1:53 or [2001:db8::1]:53
    #[arg(short = 's', long)]
    pub server: String,

    /// DNS forward zone, e.g. example.com.
    #[arg(short = 'z', long)]
    pub zone: String,

    /// Reverse DNS zone, e.g. 0.0.10.in-addr.arpa.
    #[arg(long)]
    pub ptr_zone: String,

    /// Hostname to create (FQDN), e.g. host-001.example.com.
    #[arg(long)]
    pub hostname: String,

    /// IPv4 address to assign
    #[arg(long)]
    pub ip: String,

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
    pub server:   std::net::SocketAddr,
    pub zone:     hickory_proto::rr::Name,
    pub ptr_zone: hickory_proto::rr::Name,
    pub hostname: hickory_proto::rr::Name,
    pub ip:       std::net::Ipv4Addr,
    pub tsig:     Option<crate::dns::TsigConfig>,
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

        let ptr_zone = Name::from_str(&self.ptr_zone)
            .map_err(|e| format!("invalid --ptr-zone: {}", e))?;

        let hostname = Name::from_str(&self.hostname)
            .map_err(|e| format!("invalid --hostname: {}", e))?;

        let ip = self.ip.parse::<std::net::Ipv4Addr>()
            .map_err(|e| format!("invalid --ip: {}", e))?;

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

        Ok(Config { server, zone, ptr_zone, hostname, ip, tsig })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_args() -> Args {
        Args {
            server:      "127.0.0.1:53".to_string(),
            zone:        "example.com.".to_string(),
            ptr_zone:    "1.168.192.in-addr.arpa.".to_string(),
            hostname:    "host.example.com.".to_string(),
            ip:          "192.168.1.99".to_string(),
            tsig_name:   None,
            tsig_secret: None,
            tsig_algo:   "hmac-sha256".to_string(),
        }
    }

    #[test]
    fn valid_args_parse_without_tsig() {
        let config = base_args().into_config().expect("should parse");
        assert_eq!(config.ip.to_string(), "192.168.1.99");
    }

    #[test]
    fn invalid_ip_returns_error() {
        let mut args = base_args();
        args.ip = "not-an-ip".to_string();
        assert!(args.into_config().is_err());
    }

    #[test]
    fn unknown_tsig_algo_returns_error() {
        let mut args = base_args();
        args.tsig_name   = Some("key.".to_string());
        args.tsig_secret = Some("aGVsbG8=".to_string()); // "hello" base64
        args.tsig_algo   = "hmac-sha512".to_string();
        assert!(args.into_config().is_err());
    }
}

use std::net::Ipv4Addr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use hickory_proto::rr::Name;
use ipnet::Ipv4Net;
use rand::Rng;

pub struct DnsRecord {
    pub hostname: Name,
    pub ip:       Ipv4Addr,
    pub ptr_name: Name,
}

pub struct RecordGenerator {
    hosts:   Vec<Ipv4Addr>,
    prefix:  String,
    zone:    Name,
    counter: Arc<AtomicU64>,
    random:  bool,
}

impl RecordGenerator {
    pub fn new(network: Ipv4Net, prefix: String, zone: Name, random: bool) -> Self {
        let hosts: Vec<Ipv4Addr> = network.hosts().collect();
        assert!(!hosts.is_empty(), "network must contain at least one host address");
        Self {
            hosts,
            prefix,
            zone,
            counter: Arc::new(AtomicU64::new(0)),
            random,
        }
    }

    pub fn next(&self) -> DnsRecord {
        let ip = if self.random {
            let idx = rand::thread_rng().gen_range(0..self.hosts.len());
            self.hosts[idx]
        } else {
            let idx = (self.counter.fetch_add(1, Ordering::Relaxed) as usize) % self.hosts.len();
            self.hosts[idx]
        };

        let o = ip.octets();
        let ptr_name = Name::from_str_relaxed(
            &format!("{}.{}.{}.{}.in-addr.arpa.", o[3], o[2], o[1], o[0])
        ).expect("valid ptr name");

        let label = format!("{}{}", self.prefix, u32::from(ip));
        let hostname = Name::from_str_relaxed(&format!("{}.{}", label, self.zone))
            .expect("valid hostname");

        DnsRecord { hostname, ip, ptr_name }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    fn gen() -> RecordGenerator {
        RecordGenerator::new(
            "10.0.0.0/24".parse().unwrap(),
            "host-".to_string(),
            Name::from_str("example.com.").unwrap(),
            false,
        )
    }

    #[test]
    fn sequential_wraps_around() {
        let g = gen();
        // /24 has 254 hosts; after 254 calls index should wrap
        for _ in 0..254 {
            g.next();
        }
        let rec = g.next();
        // should be back to first host (10.0.0.1)
        assert_eq!(rec.ip, "10.0.0.1".parse::<Ipv4Addr>().unwrap());
    }

    #[test]
    fn hostname_contains_prefix() {
        let g = gen();
        let rec = g.next();
        assert!(rec.hostname.to_string().starts_with("host-"));
    }

    #[test]
    fn ptr_name_is_reversed() {
        let g = gen();
        let rec = g.next();
        // 10.0.0.1 -> 1.0.0.10.in-addr.arpa.
        assert!(rec.ptr_name.to_string().ends_with(".in-addr.arpa."));
        let ptr_str = rec.ptr_name.to_string();
        let parts: Vec<&str> = ptr_str.split('.').collect();
        assert_eq!(parts[0], "1");   // last octet first
        assert_eq!(parts[1], "0");
        assert_eq!(parts[2], "0");
        assert_eq!(parts[3], "10");
    }

    #[test]
    fn random_mode_stays_in_subnet() {
        let g = RecordGenerator::new(
            "10.0.0.0/24".parse().unwrap(),
            "h-".to_string(),
            Name::from_str("example.com.").unwrap(),
            true,
        );
        for _ in 0..100 {
            let rec = g.next();
            let octets = rec.ip.octets();
            assert_eq!(octets[0], 10);
            assert_eq!(octets[1], 0);
            assert_eq!(octets[2], 0);
            assert!(octets[3] >= 1 && octets[3] <= 254);
        }
    }
}

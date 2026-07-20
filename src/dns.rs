use hickory_client::error::{ClientError, ClientErrorKind};
use hickory_client::client::{AsyncClient, ClientConnection, ClientHandle, Signer};
use hickory_client::udp::UdpClientConnection;
use hickory_client::tcp::TcpClientConnection;
use hickory_proto::error::ProtoErrorKind;
use hickory_proto::op::ResponseCode;
use hickory_proto::rr::{DNSClass, Name, RData, Record, RecordType};
use hickory_proto::rr::rdata::{A, PTR};
use hickory_proto::rr::dnssec::tsig::TSigner;
use hickory_proto::rr::dnssec::rdata::tsig::TsigAlgorithm;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

// ── Transport configuration ──────────────────────────────────────────────────

#[derive(Clone, Debug, Default)]
pub enum Transport { #[default] Udp, Tcp }

#[derive(Clone, Debug, Default)]
pub enum IpVersion { #[default] Auto, V4, V6 }

#[derive(Clone, Debug, Default)]
pub struct TransportConfig {
    pub transport:  Transport,
    pub ip_version: IpVersion,
}

/// TSIG authentication configuration.
#[derive(Clone)]
pub struct TsigConfig {
    pub key_name:  Name,
    pub algorithm: TsigAlgorithm,
    pub secret:    Vec<u8>,
}

// ── Typed error ──────────────────────────────────────────────────────────────

#[derive(Debug)]
pub enum TxError {
    Timeout,
    DnsRejected { code: ResponseCode, leg: &'static str },
    Transport(String),
}

impl std::fmt::Display for TxError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TxError::Timeout                   => write!(f, "timeout"),
            TxError::DnsRejected { code, leg } => write!(f, "{leg}: server returned {code:?}"),
            TxError::Transport(msg)            => write!(f, "transport: {msg}"),
        }
    }
}

/// Convert a ClientError into a TxError (used for create/delete_rrset calls).
fn classify_client(e: ClientError) -> TxError {
    match e.kind() {
        ClientErrorKind::Timeout => TxError::Timeout,
        _                        => TxError::Transport(e.to_string()),
    }
}

/// Convert a generic boxed error into a TxError (used for build_client failures).
fn classify(e: Box<dyn std::error::Error + Send + Sync>) -> TxError {
    if let Some(proto) = e.downcast_ref::<hickory_proto::error::ProtoError>() {
        if matches!(proto.kind(), ProtoErrorKind::Timeout) {
            return TxError::Timeout;
        }
    }
    TxError::Transport(e.to_string())
}

/// Map TxError to the stats ErrorKind.
pub fn tx_error_to_error_kind(e: &TxError) -> crate::stats::ErrorKind {
    use crate::stats::ErrorKind;
    match e {
        TxError::Timeout                   => ErrorKind::Timeout,
        TxError::DnsRejected { code, .. }  => ErrorKind::DnsRejected { code: u16::from(*code) },
        TxError::Transport(_)              => ErrorKind::Transport,
    }
}

// ── Client builder ───────────────────────────────────────────────────────────

async fn build_client(
    server:    SocketAddr,
    tsig:      Option<TsigConfig>,
    transport: &TransportConfig,
    timeout:   Duration,
) -> Result<AsyncClient, Box<dyn std::error::Error + Send + Sync>> {
    let signer: Option<Arc<Signer>> = tsig.map(|t| {
        let tsigner = TSigner::new(t.secret, t.algorithm, t.key_name, 300)
            .expect("valid TSIG config");
        Arc::new(Signer::from(tsigner))
    });

    let bind_addr: Option<SocketAddr> = match transport.ip_version {
        IpVersion::V4   => Some("0.0.0.0:0".parse().unwrap()),
        IpVersion::V6   => Some("[::]:0".parse().unwrap()),
        IpVersion::Auto => None,
    };

    match transport.transport {
        Transport::Udp => {
            let conn = UdpClientConnection::with_bind_addr_and_timeout(
                server, bind_addr, timeout,
            )?;
            let (client, bg) = AsyncClient::connect(conn.new_stream(signer)).await?;
            tokio::spawn(bg);
            Ok(client)
        }
        Transport::Tcp => {
            let conn = TcpClientConnection::with_bind_addr_and_timeout(
                server, bind_addr, timeout,
            )?;
            let (client, bg) = AsyncClient::connect(conn.new_stream(signer)).await?;
            tokio::spawn(bg);
            Ok(client)
        }
    }
}

// ── DNS helpers ──────────────────────────────────────────────────────────────

async fn timed_create(
    client: &mut AsyncClient,
    record: Record,
    zone:   Name,
    leg:    &'static str,
) -> Result<Duration, TxError> {
    let start = std::time::Instant::now();
    let resp  = client.create(record, zone).await.map_err(classify_client)?;;
    let elapsed = start.elapsed();
    match resp.response_code() {
        ResponseCode::NoError | ResponseCode::YXRRSet => Ok(elapsed),
        code => Err(TxError::DnsRejected { code, leg }),
    }
}

async fn timed_delete_rrset(
    client: &mut AsyncClient,
    record: Record,
    zone:   Name,
    leg:    &'static str,
) -> Result<Duration, TxError> {
    let start = std::time::Instant::now();
    let resp  = client.delete_rrset(record, zone).await.map_err(classify_client)?;;
    let elapsed = start.elapsed();
    match resp.response_code() {
        ResponseCode::NoError | ResponseCode::NXRRSet => Ok(elapsed),
        code => Err(TxError::DnsRejected { code, leg }),
    }
}

/// Builds the in-addr.arpa PTR name for an IPv4 address.
pub fn ipv4_to_ptr_name(ip: Ipv4Addr) -> Name {
    let o = ip.octets();
    Name::from_str_relaxed(&format!("{}.{}.{}.{}.in-addr.arpa.", o[3], o[2], o[1], o[0]))
        .expect("ptr name is always valid")
}

// ── Public send helpers (used by integration tests) ──────────────────────────

pub async fn send_add_a(
    server:   SocketAddr,
    zone:     Name,
    hostname: Name,
    ip:       Ipv4Addr,
) -> Result<Duration, Box<dyn std::error::Error + Send + Sync>> {
    let mut client = build_client(server, None, &TransportConfig::default(), Duration::from_secs(5)).await?;
    let mut record = Record::new();
    record.set_name(hostname)
          .set_record_type(RecordType::A)
          .set_dns_class(DNSClass::IN)
          .set_ttl(300)
          .set_data(Some(RData::A(A(ip))));
    let start = std::time::Instant::now();
    let response = client.create(record, zone).await?;
    let elapsed = start.elapsed();
    match response.response_code() {
        ResponseCode::NoError | ResponseCode::YXRRSet => Ok(elapsed),
        code => Err(format!("server returned {:?}", code).into()),
    }
}

pub async fn send_add_a_tsig(
    server:   SocketAddr,
    zone:     Name,
    hostname: Name,
    ip:       Ipv4Addr,
    tsig:     TsigConfig,
) -> Result<Duration, Box<dyn std::error::Error + Send + Sync>> {
    let mut client = build_client(server, Some(tsig), &TransportConfig::default(), Duration::from_secs(5)).await?;
    let mut record = Record::new();
    record.set_name(hostname)
          .set_record_type(RecordType::A)
          .set_dns_class(DNSClass::IN)
          .set_ttl(300)
          .set_data(Some(RData::A(A(ip))));
    let start = std::time::Instant::now();
    let response = client.create(record, zone).await?;
    let elapsed = start.elapsed();
    match response.response_code() {
        ResponseCode::NoError | ResponseCode::YXRRSet => Ok(elapsed),
        code => Err(format!("server returned {:?}", code).into()),
    }
}

// ── Main transaction ──────────────────────────────────────────────────────────

/// Run a DDNS transaction with configurable legs.
///
/// Legs executed depend on flags:
///   include_ptr=false, include_delete=false  →  Add A
///   include_ptr=true,  include_delete=false  →  Add A → Add PTR
///   include_ptr=false, include_delete=true   →  Add A → Del A
///   include_ptr=true,  include_delete=true   →  Add A → Add PTR → Del PTR → Del A
pub async fn run_transaction(
    server:         SocketAddr,
    zone:           Name,
    ptr_zone:       Option<Name>,
    hostname:       Name,
    ip:             Ipv4Addr,
    tsig:           Option<TsigConfig>,
    transport:      TransportConfig,
    timeout:        Duration,
    include_ptr:    bool,
    include_delete: bool,
) -> Result<crate::stats::TxResult, TxError> {
    let mut client = build_client(server, tsig, &transport, timeout)
        .await
        .map_err(classify)?;

    let ptr_name = ipv4_to_ptr_name(ip);

    // 1. Add A (always)
    let a_record = {
        let mut r = Record::new();
        r.set_name(hostname.clone())
         .set_record_type(RecordType::A)
         .set_dns_class(DNSClass::IN)
         .set_ttl(300)
         .set_data(Some(RData::A(A(ip))));
        r
    };
    let add_a = timed_create(&mut client, a_record, zone.clone(), "add_a").await?;

    // 2. Add PTR (optional)
    let add_ptr = if include_ptr {
        let pz = ptr_zone.clone().expect("ptr_zone required when include_ptr=true");
        let ptr_record = {
            let mut r = Record::new();
            r.set_name(ptr_name.clone())
             .set_record_type(RecordType::PTR)
             .set_dns_class(DNSClass::IN)
             .set_ttl(300)
             .set_data(Some(RData::PTR(PTR(hostname.clone()))));
            r
        };
        Some(timed_create(&mut client, ptr_record, pz, "add_ptr").await?)
    } else {
        None
    };

    // 3. Delete PTR (optional)
    let del_ptr = if include_ptr && include_delete {
        let pz = ptr_zone.expect("ptr_zone required when include_ptr=true");
        let del_ptr_record = {
            let mut r = Record::new();
            r.set_name(ptr_name)
             .set_record_type(RecordType::PTR)
             .set_dns_class(DNSClass::IN)
             .set_ttl(0);
            r
        };
        Some(timed_delete_rrset(&mut client, del_ptr_record, pz, "del_ptr").await?)
    } else {
        None
    };

    // 4. Delete A (optional)
    let del_a = if include_delete {
        let del_a_record = {
            let mut r = Record::new();
            r.set_name(hostname)
             .set_record_type(RecordType::A)
             .set_dns_class(DNSClass::IN)
             .set_ttl(0);
            r
        };
        Some(timed_delete_rrset(&mut client, del_a_record, zone, "del_a").await?)
    } else {
        None
    };

    Ok(crate::stats::TxResult {
        add_a_latency:   add_a,
        add_ptr_latency: add_ptr,
        del_ptr_latency: del_ptr,
        del_a_latency:   del_a,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;
    use std::net::SocketAddr;
    use std::str::FromStr;

    #[test]
    fn tx_error_to_kind_timeout() {
        use crate::stats::ErrorKind;
        let e = TxError::Timeout;
        assert_eq!(tx_error_to_error_kind(&e), ErrorKind::Timeout);
    }

    #[test]
    fn tx_error_to_kind_dns_rejected() {
        use crate::stats::ErrorKind;
        let e = TxError::DnsRejected { code: ResponseCode::Refused, leg: "add_a" };
        assert!(matches!(
            tx_error_to_error_kind(&e),
            ErrorKind::DnsRejected { code: 5 }
        ));
    }

    #[test]
    fn tx_error_to_kind_transport() {
        use crate::stats::ErrorKind;
        let e = TxError::Transport("connection reset".into());
        assert_eq!(tx_error_to_error_kind(&e), ErrorKind::Transport);
    }

    /// Requires BIND at 127.0.0.1:5353 — see README smoke test setup.
    /// Run with: cargo test test_run_transaction -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn test_run_transaction() {
        let server: SocketAddr = "127.0.0.1:5353".parse().unwrap();
        let zone     = Name::from_str("test.local.").unwrap();
        let ptr_zone = Name::from_str("10.in-addr.arpa.").unwrap();
        let hostname = Name::from_str("tx-test.test.local.").unwrap();
        let ip: Ipv4Addr = "10.0.0.77".parse().unwrap();
        let secret = base64::engine::general_purpose::STANDARD
            .decode("i8ThSp4D0SYlHviiQCCxAs4qgZtisWG845b7ttlCT6w=")
            .unwrap();
        let tsig = TsigConfig {
            key_name:  Name::from_str("test-key.").unwrap(),
            algorithm: TsigAlgorithm::HmacSha256,
            secret,
        };
        let result = run_transaction(
            server, zone, Some(ptr_zone), hostname, ip, Some(tsig),
            TransportConfig::default(), Duration::from_secs(5),
            true, true,
        ).await.expect("transaction should succeed");
        crate::stats::print_report(&result);
        assert!(result.total().as_secs() < 10);
    }

    /// Requires BIND at 127.0.0.1:5353 allowing unauthenticated updates.
    /// Run with: cargo test test_send_add_a_unsigned -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn test_send_add_a_unsigned() {
        let server: SocketAddr = "127.0.0.1:5353".parse().unwrap();
        let zone = Name::from_str("test.local.").unwrap();
        let hostname = Name::from_str("spike-test.test.local.").unwrap();
        let ip: Ipv4Addr = "10.0.0.99".parse().unwrap();
        let elapsed = send_add_a(server, zone, hostname, ip)
            .await
            .expect("unsigned update should succeed");
        println!("RTT: {:?}", elapsed);
        assert!(elapsed.as_secs() < 5);
    }

    /// Requires BIND at 127.0.0.1:5353 with TSIG key "test-key".
    /// Run with: cargo test test_send_add_a_tsig -- --ignored --nocapture
    #[tokio::test]
    #[ignore]
    async fn test_send_add_a_tsig() {
        let server: SocketAddr = "127.0.0.1:5353".parse().unwrap();
        let zone = Name::from_str("test.local.").unwrap();
        let hostname = Name::from_str("tsig-test.test.local.").unwrap();
        let ip: Ipv4Addr = "10.0.0.88".parse().unwrap();
        let secret = base64::engine::general_purpose::STANDARD
            .decode("i8ThSp4D0SYlHviiQCCxAs4qgZtisWG845b7ttlCT6w=")
            .expect("valid base64");
        let tsig = TsigConfig {
            key_name:  Name::from_str("test-key.").unwrap(),
            algorithm: TsigAlgorithm::HmacSha256,
            secret,
        };
        let elapsed = send_add_a_tsig(server, zone, hostname, ip, tsig)
            .await
            .expect("TSIG-signed update should succeed");
        println!("RTT: {:?}", elapsed);
        assert!(elapsed.as_secs() < 5);
    }
}

use hickory_client::client::{AsyncClient, ClientConnection, ClientHandle, Signer};
use hickory_client::udp::UdpClientConnection;
use hickory_proto::rr::{DNSClass, Name, RData, Record, RecordType};
use hickory_proto::rr::rdata::A;
use hickory_proto::rr::dnssec::tsig::TSigner;
use hickory_proto::rr::dnssec::rdata::tsig::TsigAlgorithm;
use hickory_proto::op::ResponseCode;
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

/// TSIG authentication configuration.
pub struct TsigConfig {
    pub key_name:  Name,
    pub algorithm: TsigAlgorithm,
    pub secret:    Vec<u8>, // raw bytes; caller decodes base64
}

/// Builds an AsyncClient signed with TSIG.
async fn tsig_client(
    server: SocketAddr,
    tsig: TsigConfig,
) -> Result<(AsyncClient, impl std::future::Future<Output = Result<(), hickory_proto::error::ProtoError>> + Send), Box<dyn std::error::Error + Send + Sync>> {
    let tsigner = TSigner::new(tsig.secret, tsig.algorithm, tsig.key_name, 300)?;
    let signer = Arc::new(Signer::from(tsigner));
    let conn = UdpClientConnection::new(server)?;
    let stream = conn.new_stream(Some(signer));
    let (client, bg) = AsyncClient::connect(stream).await?;
    Ok((client, bg))
}

/// Sends a single unsigned A-record add update to the server.
/// Returns the round-trip time on success.
pub async fn send_add_a(
    server: SocketAddr,
    zone: Name,
    hostname: Name,
    ip: Ipv4Addr,
) -> Result<Duration, Box<dyn std::error::Error + Send + Sync>> {
    let conn = UdpClientConnection::new(server)?;
    let stream = conn.new_stream(None);
    let (mut client, bg) = AsyncClient::connect(stream).await?;
    tokio::spawn(bg);

    let mut record = Record::new();
    record
        .set_name(hostname)
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

/// Sends a single TSIG-signed A-record add update to the server.
/// Returns the round-trip time on success.
pub async fn send_add_a_tsig(
    server: SocketAddr,
    zone: Name,
    hostname: Name,
    ip: Ipv4Addr,
    tsig: TsigConfig,
) -> Result<Duration, Box<dyn std::error::Error + Send + Sync>> {
    let (mut client, bg) = tsig_client(server, tsig).await?;
    tokio::spawn(bg);

    let mut record = Record::new();
    record
        .set_name(hostname)
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

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;
    use std::str::FromStr;

    /// Requires BIND at 127.0.0.1:5353 with zone "test.local" allowing unauthenticated updates.
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
        assert!(elapsed.as_secs() < 5, "response took too long");
    }

    /// Requires BIND at 127.0.0.1:5353 with TSIG key "test-key" (hmac-sha256) on zone "test.local".
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

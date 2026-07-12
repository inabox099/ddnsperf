use hickory_client::client::{AsyncClient, ClientConnection, ClientHandle};
use hickory_client::udp::UdpClientConnection;
use hickory_proto::rr::{DNSClass, Name, RData, Record, RecordType};
use hickory_proto::rr::rdata::A;
use hickory_proto::op::ResponseCode;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;

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

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    /// Requires a local BIND/named server at 127.0.0.1:5353 with zone "test.local"
    /// allowing unauthenticated dynamic updates from 127.0.0.1.
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
}

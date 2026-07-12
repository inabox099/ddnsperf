# ddnsperf MVP Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a working CLI binary that sends a single TSIG-signed DDNS Update (RFC 2136) A record add transaction (add A+PTR, then delete PTR+A) to a DNS server and prints latency + result.

**Architecture:** Single Rust binary crate with four focused modules: `cli` (clap args → `Config`), `dns` (hickory-dns wrapper: record construction, TSIG signing, send), `stats` (minimal result printer), and `main` (wire everything together). The spike in Task 2 deliberately validates the hickory-dns API before any CLI plumbing is added.

**Tech Stack:** Rust 2021, hickory-client 0.24, hickory-proto 0.24, tokio 1 (full), clap 4 (derive), base64 0.22

## Global Constraints

- Rust edition: 2021
- hickory-client: 0.24 (exact minor version pinned in Cargo.toml)
- hickory-proto: 0.24
- tokio: 1 with `features = ["full"]`
- clap: 4 with `features = ["derive"]`
- All DNS operations are async (no blocking DNS calls)
- TSIG algorithm default: hmac-sha256
- Transaction = add A + add PTR + delete PTR + delete A (4 DNS messages), each timed individually

---

## File Map

| File | Responsibility |
|---|---|
| `Cargo.toml` | Dependencies and binary declaration |
| `src/main.rs` | Parse CLI → call engine → print report |
| `src/cli.rs` | clap structs + `Config` validated output type |
| `src/dns.rs` | `send_update()`: connection, TSIG signer, record construction, send |
| `src/stats.rs` | `TxResult` type + `print_report()` |

---

### Task 1: Project scaffold

**Files:**
- Create: `Cargo.toml`
- Create: `src/main.rs`

**Interfaces:**
- Produces: compilable binary stub; no logic yet

- [ ] **Step 1: Create Cargo.toml**

```toml
[package]
name = "ddnsperf"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "ddnsperf"
path = "src/main.rs"

[dependencies]
hickory-client = { version = "0.24", features = ["dns-over-native-tls"] }
hickory-proto  = "0.24"
tokio          = { version = "1", features = ["full"] }
clap           = { version = "4", features = ["derive"] }
base64         = "0.22"

[dev-dependencies]
tokio = { version = "1", features = ["full"] }
```

- [ ] **Step 2: Create src/main.rs**

```rust
#[tokio::main]
async fn main() {
    println!("ddnsperf");
}
```

- [ ] **Step 3: Verify it compiles**

```bash
cargo build
```

Expected: compiles with no errors. Warnings about unused deps are fine.

- [ ] **Step 4: Commit**

```bash
git add Cargo.toml src/main.rs
git commit -m "chore: project scaffold"
```

---

### Task 2: Unsigned DNS update spike

**Files:**
- Create: `src/dns.rs`

**Interfaces:**
- Produces:
  ```rust
  pub async fn send_add_a(
      server: std::net::SocketAddr,
      zone: hickory_proto::rr::Name,
      hostname: hickory_proto::rr::Name,
      ip: std::net::Ipv4Addr,
  ) -> Result<std::time::Duration, Box<dyn std::error::Error + Send + Sync>>
  ```

> **Note:** This task spikes the hickory-dns API against a real DNS server. Run a local BIND instance (see setup below) before running the test. The test is marked `#[ignore]` so CI skips it; run it manually with `-- --ignored`.

- [ ] **Step 1: Write a failing integration test in dns.rs**

```rust
// src/dns.rs

use hickory_client::client::{AsyncClient, ClientHandle};
use hickory_client::udp::UdpClientStream;
use hickory_proto::rr::{DNSClass, Name, RData, Record, RecordType};
use hickory_proto::rr::rdata::A;
use std::net::{Ipv4Addr, SocketAddr};
use std::time::Duration;
use tokio::net::UdpSocket;

pub async fn send_add_a(
    server: SocketAddr,
    zone: Name,
    hostname: Name,
    ip: Ipv4Addr,
) -> Result<Duration, Box<dyn std::error::Error + Send + Sync>> {
    let stream = UdpClientStream::<UdpSocket>::new(server);
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

    use hickory_proto::op::ResponseCode;
    match response.response_code() {
        ResponseCode::NoError | ResponseCode::YXRRSet => Ok(elapsed),
        code => Err(format!("server returned {:?}", code).into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::str::FromStr;

    /// Requires a local BIND server at 127.0.0.1:5353 with zone "test.local" allowing
    /// unauthenticated dynamic updates from 127.0.0.1.
    /// Run with: cargo test test_send_add_a_unsigned -- --ignored
    #[tokio::test]
    #[ignore]
    async fn test_send_add_a_unsigned() {
        let server: SocketAddr = "127.0.0.1:5353".parse().unwrap();
        let zone = Name::from_str("test.local.").unwrap();
        let hostname = Name::from_str("spike-test.test.local.").unwrap();
        let ip: Ipv4Addr = "10.0.0.99".parse().unwrap();

        let elapsed = send_add_a(server, zone, hostname, ip)
            .await
            .expect("update should succeed");

        println!("RTT: {:?}", elapsed);
        assert!(elapsed.as_secs() < 5, "response took too long");
    }
}
```

- [ ] **Step 2: Register dns module in main.rs**

Add to `src/main.rs`:
```rust
mod dns;

#[tokio::main]
async fn main() {
    println!("ddnsperf");
}
```

- [ ] **Step 3: Verify it compiles**

```bash
cargo build
```

Expected: compiles. If hickory API has changed from what's shown, adjust imports to match — the `ClientHandle` trait and `AsyncClient::connect` are stable across 0.24.x.

- [ ] **Step 4: Start a local BIND container for the manual test**

```bash
# named.conf.test — save this to /tmp/named.conf.test
cat > /tmp/named.conf.test << 'EOF'
options {
    directory "/var/cache/bind";
    listen-on port 5353 { any; };
    allow-query { any; };
};
zone "test.local" {
    type master;
    file "/etc/bind/db.test.local";
    allow-update { any; };
};
EOF

cat > /tmp/db.test.local << 'EOF'
$TTL 300
@ IN SOA ns1.test.local. admin.test.local. (
    1 ; serial
    3600 ; refresh
    900 ; retry
    604800 ; expire
    300 ) ; minimum
@ IN NS ns1.test.local.
ns1 IN A 127.0.0.1
EOF

docker run -d --name bind-test \
  -p 5353:5353/udp \
  -v /tmp/named.conf.test:/etc/bind/named.conf \
  -v /tmp/db.test.local:/etc/bind/db.test.local \
  internetsystemsconsortium/bind9:9.18
```

- [ ] **Step 5: Run the integration test**

```bash
cargo test test_send_add_a_unsigned -- --ignored --nocapture
```

Expected output:
```
RTT: 1.234ms
test dns::tests::test_send_add_a_unsigned ... ok
```

If the BIND container returns `REFUSED`, the `allow-update` ACL is not applied — confirm the zone config was mounted correctly.

- [ ] **Step 6: Commit**

```bash
git add src/dns.rs src/main.rs
git commit -m "feat(dns): unsigned DDNS add spike passes"
```

---

### Task 3: TSIG-signed DNS update

**Files:**
- Modify: `src/dns.rs`

**Interfaces:**
- Consumes: `send_add_a` from Task 2
- Produces:
  ```rust
  pub struct TsigConfig {
      pub key_name: hickory_proto::rr::Name,
      pub algorithm: hickory_proto::rr::dnssec::tsig::TsigAlgorithm,
      pub secret: Vec<u8>,   // raw bytes (caller decodes base64)
  }

  pub async fn send_add_a_tsig(
      server: std::net::SocketAddr,
      zone: hickory_proto::rr::Name,
      hostname: hickory_proto::rr::Name,
      ip: std::net::Ipv4Addr,
      tsig: TsigConfig,
  ) -> Result<std::time::Duration, Box<dyn std::error::Error + Send + Sync>>
  ```

- [ ] **Step 1: Add TSIG types and start a BIND container with TSIG**

First, generate a TSIG key for testing:
```bash
# Generate key
tsig-keygen -a hmac-sha256 test-key > /tmp/test.key
cat /tmp/test.key
# Output looks like:
# key "test-key" {
#     algorithm hmac-sha256;
#     secret "base64secret==";
# };
```

Add the key to `/tmp/named.conf.test` (restart the container after):
```
key "test-key" {
    algorithm hmac-sha256;
    secret "BASE64_FROM_ABOVE";
};

zone "test.local" {
    type master;
    file "/etc/bind/db.test.local";
    allow-update { key "test-key"; };  # replace "any" with key-only
};
```

- [ ] **Step 2: Write the failing TSIG test**

Add to `src/dns.rs`:
```rust
use hickory_proto::rr::dnssec::tsig::TsigAlgorithm;

pub struct TsigConfig {
    pub key_name: Name,
    pub algorithm: TsigAlgorithm,
    pub secret: Vec<u8>,
}

/// Requires a local BIND server with TSIG key "test-key" (hmac-sha256) on zone "test.local".
/// Run with: cargo test test_send_add_a_tsig -- --ignored
#[cfg(test)]
mod tsig_tests {
    use super::*;
    use std::str::FromStr;

    #[tokio::test]
    #[ignore]
    async fn test_send_add_a_tsig() {
        let server: SocketAddr = "127.0.0.1:5353".parse().unwrap();
        let zone = Name::from_str("test.local.").unwrap();
        let hostname = Name::from_str("tsig-test.test.local.").unwrap();
        let ip: Ipv4Addr = "10.0.0.88".parse().unwrap();

        // Replace with actual base64 secret from tsig-keygen output
        let secret = base64::engine::general_purpose::STANDARD
            .decode("BASE64SECRET==")
            .expect("valid base64");

        let tsig = TsigConfig {
            key_name: Name::from_str("test-key.").unwrap(),
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
```

Add `base64` import at the top of `dns.rs`:
```rust
use base64::Engine as _;
```

- [ ] **Step 3: Run the test to confirm it fails (function not yet implemented)**

```bash
cargo test test_send_add_a_tsig -- --ignored 2>&1 | head -20
```

Expected: compile error — `send_add_a_tsig` not defined yet.

- [ ] **Step 4: Implement send_add_a_tsig**

In hickory-client 0.24, TSIG signing for dynamic updates is done by building a `DnsMultiplexer` with a `TsigSigner`. Add this to `src/dns.rs`:

```rust
use hickory_client::client::AsyncDnssecClient;
use hickory_proto::rr::dnssec::tsig::{TsigAlgorithm, TSIGRecord};
use hickory_proto::rr::rdata::tsig::TsigKey;

pub async fn send_add_a_tsig(
    server: SocketAddr,
    zone: Name,
    hostname: Name,
    ip: Ipv4Addr,
    tsig: TsigConfig,
) -> Result<Duration, Box<dyn std::error::Error + Send + Sync>> {
    // Build TSIG key signer
    let key = TsigKey::new(
        tsig.key_name.clone(),
        tsig.algorithm,
        tsig.secret,
    );

    let stream = UdpClientStream::<UdpSocket>::with_timeout(
        server,
        std::time::Duration::from_secs(5),
    );

    let (mut client, bg) = AsyncClient::with_signer(stream, key).await?;
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

    use hickory_proto::op::ResponseCode;
    match response.response_code() {
        ResponseCode::NoError | ResponseCode::YXRRSet => Ok(elapsed),
        code => Err(format!("server returned {:?}", code).into()),
    }
}
```

> **API note:** If `AsyncClient::with_signer` or `TsigKey::new` do not exist in the version you have installed, check `cargo doc --open -p hickory-client` for the actual signer API. The alternative is `hickory_client::client::Client::new` (the sync client) with a signer argument, then wrapping async. The exact TSIG signer constructor changed between 0.24.x patch releases — consult the docs before assuming the signature above is literal.

- [ ] **Step 5: Run the TSIG test**

```bash
cargo test test_send_add_a_tsig -- --ignored --nocapture
```

Expected:
```
RTT: 1.8ms
test dns::tsig_tests::test_send_add_a_tsig ... ok
```

If server returns `NOTAUTH`, the TSIG secret doesn't match the server's key — re-check the base64 string in the test.

- [ ] **Step 6: Commit**

```bash
git add src/dns.rs
git commit -m "feat(dns): TSIG-signed update passes integration test"
```

---

### Task 4: Full transaction (add A+PTR, delete PTR+A)

**Files:**
- Modify: `src/dns.rs`
- Create: `src/stats.rs`

**Interfaces:**
- Consumes: `send_add_a_tsig` from Task 3
- Produces:
  ```rust
  // src/stats.rs
  pub struct TxResult {
      pub add_a_latency:   std::time::Duration,
      pub add_ptr_latency: std::time::Duration,
      pub del_ptr_latency: std::time::Duration,
      pub del_a_latency:   std::time::Duration,
  }

  pub fn print_report(result: &TxResult);

  // src/dns.rs
  pub async fn run_transaction(
      server: std::net::SocketAddr,
      zone: hickory_proto::rr::Name,
      ptr_zone: hickory_proto::rr::Name,
      hostname: hickory_proto::rr::Name,
      ip: std::net::Ipv4Addr,
      tsig: Option<TsigConfig>,
  ) -> Result<crate::stats::TxResult, Box<dyn std::error::Error + Send + Sync>>
  ```

- [ ] **Step 1: Create src/stats.rs**

```rust
// src/stats.rs
use std::time::Duration;

pub struct TxResult {
    pub add_a_latency:   Duration,
    pub add_ptr_latency: Duration,
    pub del_ptr_latency: Duration,
    pub del_a_latency:   Duration,
}

impl TxResult {
    pub fn total(&self) -> Duration {
        self.add_a_latency
            + self.add_ptr_latency
            + self.del_ptr_latency
            + self.del_a_latency
    }
}

pub fn print_report(result: &TxResult) {
    println!("=== ddnsperf transaction result ===");
    println!("  Add A:      {:>8.3}ms", ms(result.add_a_latency));
    println!("  Add PTR:    {:>8.3}ms", ms(result.add_ptr_latency));
    println!("  Delete PTR: {:>8.3}ms", ms(result.del_ptr_latency));
    println!("  Delete A:   {:>8.3}ms", ms(result.del_a_latency));
    println!("  -----------");
    println!("  Total:      {:>8.3}ms", ms(result.total()));
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn total_sums_all_legs() {
        let r = TxResult {
            add_a_latency:   Duration::from_millis(1),
            add_ptr_latency: Duration::from_millis(2),
            del_ptr_latency: Duration::from_millis(3),
            del_a_latency:   Duration::from_millis(4),
        };
        assert_eq!(r.total(), Duration::from_millis(10));
    }
}
```

- [ ] **Step 2: Run the stats unit test**

```bash
cargo test stats::tests -- --nocapture
```

Expected: `test stats::tests::total_sums_all_legs ... ok`

- [ ] **Step 3: Add PTR helpers and run_transaction to dns.rs**

Add to `src/dns.rs` (below existing functions):

```rust
use hickory_proto::rr::rdata::PTR;

/// Builds the in-addr.arpa PTR name for an IPv4 address.
/// e.g. 10.0.0.99 -> 99.0.0.10.in-addr.arpa.
pub fn ipv4_to_ptr_name(ip: Ipv4Addr) -> Name {
    let octs = ip.octets();
    Name::from_str(&format!(
        "{}.{}.{}.{}.in-addr.arpa.",
        octs[3], octs[2], octs[1], octs[0]
    ))
    .expect("ptr name is valid")
}

async fn timed_create(
    client: &mut AsyncClient,
    record: Record,
    zone: Name,
) -> Result<Duration, Box<dyn std::error::Error + Send + Sync>> {
    let start = std::time::Instant::now();
    let resp = client.create(record, zone).await?;
    let elapsed = start.elapsed();
    use hickory_proto::op::ResponseCode;
    match resp.response_code() {
        ResponseCode::NoError | ResponseCode::YXRRSet => Ok(elapsed),
        code => Err(format!("server returned {:?}", code).into()),
    }
}

async fn timed_delete_rrset(
    client: &mut AsyncClient,
    record: Record,
    zone: Name,
) -> Result<Duration, Box<dyn std::error::Error + Send + Sync>> {
    let start = std::time::Instant::now();
    let resp = client.delete_rrset(record, zone).await?;
    let elapsed = start.elapsed();
    use hickory_proto::op::ResponseCode;
    match resp.response_code() {
        ResponseCode::NoError | ResponseCode::NXRRSet => Ok(elapsed),
        code => Err(format!("server returned {:?}", code).into()),
    }
}

pub async fn run_transaction(
    server: SocketAddr,
    zone: Name,
    ptr_zone: Name,
    hostname: Name,
    ip: Ipv4Addr,
    tsig: Option<TsigConfig>,
) -> Result<crate::stats::TxResult, Box<dyn std::error::Error + Send + Sync>> {
    // Build client (TSIG or plain)
    let mut client: AsyncClient = match tsig {
        Some(t) => {
            let key = TsigKey::new(t.key_name, t.algorithm, t.secret);
            let stream = UdpClientStream::<UdpSocket>::with_timeout(
                server,
                Duration::from_secs(5),
            );
            let (c, bg) = AsyncClient::with_signer(stream, key).await?;
            tokio::spawn(bg);
            c
        }
        None => {
            let stream = UdpClientStream::<UdpSocket>::new(server);
            let (c, bg) = AsyncClient::connect(stream).await?;
            tokio::spawn(bg);
            c
        }
    };

    let ptr_name = ipv4_to_ptr_name(ip);

    // 1. Add A
    let a_record = {
        let mut r = Record::new();
        r.set_name(hostname.clone())
         .set_record_type(RecordType::A)
         .set_dns_class(DNSClass::IN)
         .set_ttl(300)
         .set_data(Some(RData::A(A(ip))));
        r
    };
    let add_a = timed_create(&mut client, a_record, zone.clone()).await?;

    // 2. Add PTR
    let ptr_record = {
        let mut r = Record::new();
        r.set_name(ptr_name.clone())
         .set_record_type(RecordType::PTR)
         .set_dns_class(DNSClass::IN)
         .set_ttl(300)
         .set_data(Some(RData::PTR(PTR(hostname.clone()))));
        r
    };
    let add_ptr = timed_create(&mut client, ptr_record, ptr_zone.clone()).await?;

    // 3. Delete PTR
    let del_ptr_record = {
        let mut r = Record::new();
        r.set_name(ptr_name)
         .set_record_type(RecordType::PTR)
         .set_dns_class(DNSClass::IN)
         .set_ttl(0)
         .set_data(None);
        r
    };
    let del_ptr = timed_delete_rrset(&mut client, del_ptr_record, ptr_zone).await?;

    // 4. Delete A
    let del_a_record = {
        let mut r = Record::new();
        r.set_name(hostname)
         .set_record_type(RecordType::A)
         .set_dns_class(DNSClass::IN)
         .set_ttl(0)
         .set_data(None);
        r
    };
    let del_a = timed_delete_rrset(&mut client, del_a_record, zone).await?;

    Ok(crate::stats::TxResult {
        add_a_latency:   add_a,
        add_ptr_latency: add_ptr,
        del_ptr_latency: del_ptr,
        del_a_latency:   del_a,
    })
}

#[cfg(test)]
mod transaction_tests {
    use super::*;
    use std::str::FromStr;
    use base64::Engine as _;

    /// Requires BIND at 127.0.0.1:5353 with zones "test.local" and
    /// "0.0.10.in-addr.arpa", both allowing updates with TSIG key "test-key".
    /// Run with: cargo test test_run_transaction -- --ignored
    #[tokio::test]
    #[ignore]
    async fn test_run_transaction() {
        let server: SocketAddr = "127.0.0.1:5353".parse().unwrap();
        let zone = Name::from_str("test.local.").unwrap();
        let ptr_zone = Name::from_str("0.0.10.in-addr.arpa.").unwrap();
        let hostname = Name::from_str("tx-test.test.local.").unwrap();
        let ip: Ipv4Addr = "10.0.0.77".parse().unwrap();

        let secret = base64::engine::general_purpose::STANDARD
            .decode("BASE64SECRET==")
            .unwrap();

        let tsig = TsigConfig {
            key_name: Name::from_str("test-key.").unwrap(),
            algorithm: TsigAlgorithm::HmacSha256,
            secret,
        };

        let result = run_transaction(server, zone, ptr_zone, hostname, ip, Some(tsig))
            .await
            .expect("transaction should succeed");

        crate::stats::print_report(&result);
        assert!(result.total().as_secs() < 10);
    }
}
```

- [ ] **Step 4: Add ptr_zone to the reverse zone in BIND config**

Append to `/tmp/named.conf.test` and restart the container:
```
zone "0.0.10.in-addr.arpa" {
    type master;
    file "/etc/bind/db.10.0.0";
    allow-update { key "test-key"; };
};
```

Create `/tmp/db.10.0.0`:
```
$TTL 300
@ IN SOA ns1.test.local. admin.test.local. (1 3600 900 604800 300)
@ IN NS ns1.test.local.
```

```bash
docker restart bind-test
```

- [ ] **Step 5: Run unit test + transaction integration test**

```bash
cargo test stats::tests
cargo test test_run_transaction -- --ignored --nocapture
```

Expected:
```
=== ddnsperf transaction result ===
  Add A:         1.234ms
  Add PTR:       0.987ms
  Delete PTR:    1.102ms
  Delete A:      0.876ms
  -----------
  Total:         4.199ms
test dns::transaction_tests::test_run_transaction ... ok
```

- [ ] **Step 6: Commit**

```bash
git add src/dns.rs src/stats.rs
git commit -m "feat(dns,stats): full transaction (add A+PTR, delete PTR+A)"
```

---

### Task 5: CLI wiring

**Files:**
- Create: `src/cli.rs`
- Modify: `src/main.rs`

**Interfaces:**
- Consumes: `run_transaction` from Task 4, `TsigConfig` from Task 3, `print_report` from Task 4
- Produces: working `ddnsperf` binary

- [ ] **Step 1: Create src/cli.rs**

```rust
// src/cli.rs
use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "ddnsperf", about = "DDNS Update performance tool")]
pub struct Args {
    /// DNS server address, e.g. 192.168.1.1:53 or [2001:db8::1]:53
    #[arg(short = 's', long)]
    pub server: String,

    /// DNS forward zone, e.g. example.com
    #[arg(short = 'z', long)]
    pub zone: String,

    /// Reverse DNS zone, e.g. 0.0.10.in-addr.arpa
    #[arg(long)]
    pub ptr_zone: String,

    /// Hostname to create (FQDN), e.g. host-001.example.com
    #[arg(long)]
    pub hostname: String,

    /// IPv4 address to assign
    #[arg(long)]
    pub ip: String,

    /// TSIG key name
    #[arg(long, requires = "tsig_secret")]
    pub tsig_name: Option<String>,

    /// TSIG secret, base64-encoded
    #[arg(long, requires = "tsig_name")]
    pub tsig_secret: Option<String>,

    /// TSIG algorithm: hmac-md5 | hmac-sha1 | hmac-sha256 (default: hmac-sha256)
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
        use hickory_proto::rr::dnssec::tsig::TsigAlgorithm;
        use base64::Engine as _;

        let server = self.server.parse::<std::net::SocketAddr>()
            .map_err(|e| format!("invalid server address: {}", e))?;

        let zone = Name::from_str(&self.zone)
            .map_err(|e| format!("invalid zone: {}", e))?;

        let ptr_zone = Name::from_str(&self.ptr_zone)
            .map_err(|e| format!("invalid ptr_zone: {}", e))?;

        let hostname = Name::from_str(&self.hostname)
            .map_err(|e| format!("invalid hostname: {}", e))?;

        let ip = self.ip.parse::<std::net::Ipv4Addr>()
            .map_err(|e| format!("invalid ip: {}", e))?;

        let tsig = match (self.tsig_name, self.tsig_secret) {
            (Some(name), Some(secret)) => {
                let key_name = Name::from_str(&name)
                    .map_err(|e| format!("invalid tsig key name: {}", e))?;
                let algorithm = match self.tsig_algo.as_str() {
                    "hmac-md5"    => TsigAlgorithm::HmacMd5,
                    "hmac-sha1"   => TsigAlgorithm::HmacSha1,
                    "hmac-sha256" => TsigAlgorithm::HmacSha256,
                    other => return Err(format!("unknown tsig algorithm: {}", other)),
                };
                let raw = base64::engine::general_purpose::STANDARD
                    .decode(&secret)
                    .map_err(|e| format!("invalid base64 tsig secret: {}", e))?;
                Some(crate::dns::TsigConfig { key_name, algorithm, secret: raw })
            }
            (None, None) => None,
            _ => unreachable!("clap requires both tsig_name and tsig_secret together"),
        };

        Ok(Config { server, zone, ptr_zone, hostname, ip, tsig })
    }
}
```

- [ ] **Step 2: Write a unit test for Config validation**

Add to `src/cli.rs`:
```rust
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
        args.tsig_secret = Some("aGVsbG8=".to_string()); // "hello" in base64
        args.tsig_algo   = "hmac-sha512".to_string();
        assert!(args.into_config().is_err());
    }
}
```

- [ ] **Step 3: Run cli unit tests**

```bash
cargo test cli::tests
```

Expected: all 3 tests pass.

- [ ] **Step 4: Wire everything in main.rs**

```rust
// src/main.rs
mod cli;
mod dns;
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
```

- [ ] **Step 5: Build and run a manual smoke test**

```bash
cargo build --release

./target/release/ddnsperf \
  --server 127.0.0.1:5353 \
  --zone test.local. \
  --ptr-zone 0.0.10.in-addr.arpa. \
  --hostname smoke.test.local. \
  --ip 10.0.0.55 \
  --tsig-name test-key. \
  --tsig-algo hmac-sha256 \
  --tsig-secret "BASE64SECRET=="
```

Expected output:
```
=== ddnsperf transaction result ===
  Add A:         1.234ms
  Add PTR:       0.987ms
  Delete PTR:    1.102ms
  Delete A:      0.876ms
  -----------
  Total:         4.199ms
```

- [ ] **Step 6: Commit**

```bash
git add src/cli.rs src/main.rs
git commit -m "feat(cli): wire CLI → transaction → report; MVP complete"
```

---

## What's NOT in this MVP

The following are deferred to the next plan (full benchmark engine):

- `--requests` / `--duration` / `--rps` / `--concurrency` (Task loop + engine)
- `--perf-test` / `--rps auto` (perf module / PID controller)
- Progress bar (`indicatif`)
- Full stats (min/mean/max latency, percentiles, error rate)
- IPv6 (AAAA records)
- `--mode sequential|random` / `--prefix` / `--network` CIDR generation
- `--udp` / `--tcp` / `--ipv4` / `--ipv6` flags (currently always UDP + auto)

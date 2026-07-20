/// Minimal DNS UPDATE stub server for benchmarking ddnsperf without a real DNS server.
///
/// Responds NOERROR to every incoming DNS message (UDP + TCP) without touching any
/// zone state. Use this to measure the maximum throughput of the ddnsperf Rust engine
/// itself, free from BIND / container overhead.
///
/// TSIG note: the stub cannot sign responses, so run ddnsperf WITHOUT --tsig-* flags.
///
/// Usage:
///   # terminal 1
///   ./target/release/stub-dns [ADDR]          # default: 127.0.0.1:5354
///
///   # terminal 2 — no --tsig-* flags needed
///   ./target/release/ddnsperf \
///     --server 127.0.0.1:5354 \
///     --zone test.local. \
///     --network 10.0.0.0/16 \
///     --requests 100000 --concurrency 50

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, UdpSocket};

/// Build a minimal 12-byte DNS response:
///   - Copies message ID (bytes 0-1) from the request.
///   - Sets QR=1 while preserving the request OPCODE.
///   - RCODE = NOERROR (0), all other flag bits cleared.
///   - All four section-count fields = 0.
fn make_noerror_response(request: &[u8]) -> [u8; 12] {
    let mut resp = [0u8; 12];
    resp[0] = request[0]; // ID high byte
    resp[1] = request[1]; // ID low byte
    // Flags byte 0: set QR=1 (bit 7), keep OPCODE bits 6-3, clear AA/TC/RD
    resp[2] = (request[2] & 0b0111_1000) | 0b1000_0000;
    // Flags byte 1: RCODE=0, RA/Z/AD/CD all 0
    resp[3] = 0x00;
    // Bytes 4-11 (section counts) already zero-initialised
    resp
}

async fn run_udp(addr: String, counter: Arc<AtomicU64>) {
    let sock = UdpSocket::bind(&addr).await.expect("UDP bind failed");
    let mut buf = vec![0u8; 4096];
    loop {
        let (len, src) = match sock.recv_from(&mut buf).await {
            Ok(v) => v,
            Err(e) => { eprintln!("udp recv error: {e}"); continue; }
        };
        if len < 12 { continue; }
        let resp = make_noerror_response(&buf[..len]);
        if sock.send_to(&resp, src).await.is_ok() {
            counter.fetch_add(1, Ordering::Relaxed);
        }
    }
}

async fn run_tcp(addr: String, counter: Arc<AtomicU64>) {
    let listener = TcpListener::bind(&addr).await.expect("TCP bind failed");
    loop {
        let (mut stream, _) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => { eprintln!("tcp accept error: {e}"); continue; }
        };
        let counter = counter.clone();
        tokio::spawn(async move {
            let mut len_buf = [0u8; 2];
            let mut msg_buf = vec![0u8; 4096];
            loop {
                if stream.read_exact(&mut len_buf).await.is_err() { break; }
                let msg_len = u16::from_be_bytes(len_buf) as usize;
                if msg_len < 12 || msg_len > msg_buf.len() { break; }
                if stream.read_exact(&mut msg_buf[..msg_len]).await.is_err() { break; }

                let resp = make_noerror_response(&msg_buf[..msg_len]);
                let prefix = (resp.len() as u16).to_be_bytes();
                if stream.write_all(&prefix).await.is_err() { break; }
                if stream.write_all(&resp).await.is_err() { break; }
                counter.fetch_add(1, Ordering::Relaxed);
            }
        });
    }
}

async fn print_stats(counter: Arc<AtomicU64>) {
    let mut last = 0u64;
    let mut t = Instant::now();
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let now = counter.load(Ordering::Relaxed);
        let delta = now - last;
        let elapsed = t.elapsed().as_secs_f64();
        println!("  {delta:>8} msg/s   (total: {now})   [{elapsed:.2}s window]");
        last = now;
        t = Instant::now();
    }
}

#[tokio::main]
async fn main() {
    let addr = std::env::args().nth(1).unwrap_or_else(|| "127.0.0.1:5354".to_string());

    println!("stub-dns: listening on {addr} (UDP + TCP)");
    println!("         responds NOERROR to every DNS message — no zone state");
    println!();
    println!("Benchmark without TSIG (stub cannot sign responses):");
    println!("  ./target/release/ddnsperf \\");
    println!("    --server {addr} \\");
    println!("    --zone test.local. \\");
    println!("    --network 10.0.0.0/16 \\");
    println!("    --requests 100000 --concurrency 50");
    println!();
    println!("--- msgs/s (server side) ---");

    let counter = Arc::new(AtomicU64::new(0));

    let udp = tokio::spawn(run_udp(addr.clone(), counter.clone()));
    let tcp = tokio::spawn(run_tcp(addr.clone(), counter.clone()));
    let stats = tokio::spawn(print_stats(counter));

    let _ = tokio::join!(udp, tcp, stats);
}

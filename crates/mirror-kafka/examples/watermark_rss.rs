//! The process RSS while asking a partition's high watermark many times,
//! with a new librdkafka client per query (`fresh`, what
//! `fetch_high_watermark` does) or one `WatermarkClient` for all (`reuse`).
//! The broker is librdkafka's in-process mock cluster, unless `BOOTSTRAP`
//! names one.
//!
//!   cargo run --release -p mirror-kafka --example watermark_rss -- fresh 20000
//!   cargo run --release -p mirror-kafka --example watermark_rss -- reuse 20000
//!
//! Prints VmRSS at every tenth of the queries. Linux only.

use std::time::{Duration, Instant};

use rdkafka::mocking::MockCluster;

fn rss_kib() -> i64 {
    std::fs::read_to_string("/proc/self/status")
        .expect("/proc/self/status")
        .lines()
        .find_map(|l| l.strip_prefix("VmRSS:"))
        .and_then(|v| v.split_whitespace().next()?.parse().ok())
        .expect("VmRSS")
}

fn main() {
    let mut args = std::env::args().skip(1);
    let mode = args.next().unwrap_or_else(|| "fresh".into());
    let n: usize = args.next().map_or(20_000, |s| s.parse().expect("count"));
    let mock;
    let bootstrap = match std::env::var("BOOTSTRAP") {
        Ok(b) => b,
        Err(_) => {
            mock = MockCluster::new(1).expect("mock cluster");
            mock.create_topic("t", 1, 1).expect("topic");
            mock.bootstrap_servers()
        }
    };
    let timeout = Duration::from_secs(5);
    let reused =
        (mode == "reuse").then(|| mirror_kafka::WatermarkClient::new(&bootstrap).expect("client"));
    let query = || match &reused {
        Some(client) => client.high_watermark("t", 0, timeout),
        None => mirror_kafka::fetch_high_watermark(&bootstrap, "t", 0, timeout),
    };
    // Warm up: the first queries connect and fill allocator caches.
    for _ in 0..100 {
        query().expect("high watermark");
    }
    let base = rss_kib();
    let t0 = Instant::now();
    println!("{mode}: after warm-up rss={base} KiB");
    for i in 1..=n {
        query().expect("high watermark");
        if i % (n / 10).max(1) == 0 {
            let grown = rss_kib() - base;
            println!(
                "{mode}: queries={i} rss_growth={grown} KiB ({:.0} B/query) t={:.0}s",
                grown as f64 * 1024.0 / i as f64,
                t0.elapsed().as_secs_f64()
            );
        }
    }
}

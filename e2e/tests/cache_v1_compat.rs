//! Ad-hoc compatibility probe: spin up the real `Yolean/kafka-keyvalue`
//! image alongside mirror-v3's `/cache/v1`, point both at the same
//! topic with the same fixture stream, and compare HTTP responses
//! byte-for-byte.
//!
//! Marked `#[ignore]` so it only runs when explicitly invoked:
//!
//!     cargo test -p mirror-e2e --test cache_v1_compat -- --ignored --nocapture
//!
//! Output is a Markdown report printed to stdout; the operator copies
//! it into `KAFKA_KEYVALUE_DROPIN_REPLACEMENT.md` for triage. The
//! test itself does *not* fail on divergence — every diff is
//! intentional input to the triage doc.

use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use mirror_core::CacheState;
use mirror_e2e::kafka_helpers::{
    create_topic, produce_records, produce_records_with_nullable_values,
};
use mirror_e2e::mirror_runner::{spawn_kafka_to_filesystem, FsMirrorSpec};
use mirror_fs::FlushTriggers;
use std::sync::Arc;
use testcontainers::core::{ContainerPort, WaitFor};
use testcontainers::runners::AsyncRunner;
use testcontainers::{ContainerAsync, GenericImage, ImageExt};

const KKV_IMAGE: &str = "ghcr.io/yolean/kafka-keyvalue";
const KKV_TAG_AND_DIGEST: &str =
    "7fa31f42731fc20a77988b478a3896732cc3dc88@sha256:01461015a75545b2f8d426e1e8bed5129dd1a79ca7081c40c6961559043d77f3";
const REDPANDA_IMAGE: &str = "docker.io/redpandadata/redpanda";
const REDPANDA_TAG: &str = "latest";

const TOPIC: &str = "compat-probe";
const KKV_GROUP: &str = "kkv-compat-probe";

#[derive(Debug)]
struct Resp {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

async fn fetch(client: &reqwest::Client, base: &str, method: reqwest::Method, path: &str) -> Resp {
    let url = format!("{base}{path}");
    let req = client.request(method, &url);
    match req.send().await {
        Ok(r) => {
            let status = r.status().as_u16();
            let headers: Vec<(String, String)> = r
                .headers()
                .iter()
                .map(|(k, v)| {
                    (
                        k.as_str().to_lowercase(),
                        v.to_str().unwrap_or("").to_string(),
                    )
                })
                .collect();
            let body = r.bytes().await.map(|b| b.to_vec()).unwrap_or_default();
            Resp {
                status,
                headers,
                body,
            }
        }
        Err(e) => Resp {
            status: 0,
            headers: vec![("error".into(), e.to_string())],
            body: Vec::new(),
        },
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore]
async fn compare_kkv_and_mirror_v3_cache_v1() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,testcontainers=warn")),
        )
        .try_init();

    let (broker_host_port, broker_docker_port, _redpanda) = start_redpanda_dual_listeners()
        .await
        .expect("start redpanda");
    let host_bootstrap = format!("localhost:{broker_host_port}");
    let docker_bootstrap = format!("host.docker.internal:{broker_docker_port}");
    tracing::info!(host = %host_bootstrap, docker = %docker_bootstrap, "redpanda ready");

    create_topic(&host_bootstrap, TOPIC, 1)
        .await
        .expect("topic");

    // Fixture set: covers the typical update / overwrite / tombstone
    // pattern plus a handful of edge keys we want to triage.
    produce_records(
        &host_bootstrap,
        TOPIC,
        0,
        &[
            ("k1".into(), "v1".into()),
            ("k2".into(), "v2".into()),
            ("k3".into(), "v3".into()),
            ("k1".into(), "v1-updated".into()),
            ("empty-value".into(), "".into()),
            ("special/chars".into(), "with/slashes".into()),
            ("plus+key".into(), "plusvalue".into()),
            ("ünıçødé".into(), "unicode-value".into()),
        ],
    )
    .await
    .expect("seed");
    produce_records_with_nullable_values(&host_bootstrap, TOPIC, 0, &[("k2".into(), None)])
        .await
        .expect("tombstone");

    // Bootstrap watermark used by mirror-v3's readiness gate.
    let bootstrap_hwm = {
        let bs = host_bootstrap.clone();
        tokio::task::spawn_blocking(move || {
            mirror_kafka::fetch_high_watermark(&bs, TOPIC, 0, Duration::from_secs(5))
        })
        .await
        .unwrap()
        .expect("hwm") as u64
    };
    tracing::info!(bootstrap_hwm, "captured source hwm");

    // Spin up KKV — talks to redpanda via the docker-internal listener.
    let kkv_host_port = portpicker::pick_unused_port().expect("port");
    let _kkv = start_kkv(kkv_host_port, &docker_bootstrap, TOPIC)
        .await
        .expect("start kkv");

    // Spin up mirror-v3 in-process. Append mode with cache-v1.
    let root = tempfile::tempdir().expect("tempdir");
    let cache_state = Arc::new(CacheState::new());
    cache_state.register_mirror("compat", bootstrap_hwm);
    let mirror_addr = {
        let port = portpicker::pick_unused_port().expect("port");
        std::net::SocketAddr::from(([127, 0, 0, 1], port))
    };
    let mirror_state = Arc::clone(&cache_state);
    let (mirror_shutdown_tx, mirror_shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    let mirror_server = tokio::spawn(async move {
        let signal = async move {
            let _ = mirror_shutdown_rx.await;
        };
        let _ = mirror_cache::serve(mirror_addr, mirror_state, signal).await;
    });
    let binding = mirror_fs::CacheBinding {
        state: Arc::clone(&cache_state),
        mirror_name: "compat".into(),
    };
    let flush = FlushTriggers {
        max_time: Duration::from_secs(3600),
        max_bytes: u64::MAX,
        max_offsets: u64::MAX,
        daily_at_utc_seconds: None,
    };
    let mirror = spawn_kafka_to_filesystem(FsMirrorSpec {
        source_bootstrap: host_bootstrap.clone(),
        source_topic: TOPIC.into(),
        partition: 0,
        group_id: "mirror-v3-compat".into(),
        root: root.path().to_path_buf(),
        destination_name: "compat".into(),
        format: mirror_envelope::Format::Parquet,
        compression: mirror_envelope::ParquetCompression::Zstd1,
        keys: mirror_envelope::ColumnType::Utf8,
        values: mirror_envelope::ColumnType::Utf8,
        compaction: None,
        cache: Some(binding),
        flush,
    })
    .expect("spawn mirror");

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    let kkv_base = format!("http://localhost:{kkv_host_port}");
    let mirror_base = format!("http://{mirror_addr}");

    // Wait until both are caught up.
    wait_until_200(
        &client,
        &kkv_base,
        "/cache/v1/raw/k1",
        Duration::from_secs(90),
    )
    .await
    .expect("kkv never reached ready");
    wait_until_200(
        &client,
        &mirror_base,
        "/cache/v1/raw/k1",
        Duration::from_secs(30),
    )
    .await
    .expect("mirror never reached ready");

    // Battery of probes. (label, method, path)
    let probes: Vec<(&str, reqwest::Method, &str)> = vec![
        (
            "raw / latest value",
            reqwest::Method::GET,
            "/cache/v1/raw/k1",
        ),
        (
            "raw / overwritten",
            reqwest::Method::GET,
            "/cache/v1/raw/k3",
        ),
        ("raw / tombstoned", reqwest::Method::GET, "/cache/v1/raw/k2"),
        ("raw / unknown", reqwest::Method::GET, "/cache/v1/raw/never"),
        (
            "raw / empty value",
            reqwest::Method::GET,
            "/cache/v1/raw/empty-value",
        ),
        (
            "raw / slash in key (raw)",
            reqwest::Method::GET,
            "/cache/v1/raw/special/chars",
        ),
        (
            "raw / slash in key (url-encoded)",
            reqwest::Method::GET,
            "/cache/v1/raw/special%2Fchars",
        ),
        (
            "raw / plus key",
            reqwest::Method::GET,
            "/cache/v1/raw/plus+key",
        ),
        (
            "raw / unicode key",
            reqwest::Method::GET,
            "/cache/v1/raw/%C3%BCn%C4%B1%C3%A7%C3%B8d%C3%A9",
        ),
        (
            "raw / trailing slash, no key",
            reqwest::Method::GET,
            "/cache/v1/raw/",
        ),
        (
            "raw / no trailing slash",
            reqwest::Method::GET,
            "/cache/v1/raw",
        ),
        ("keys", reqwest::Method::GET, "/cache/v1/keys"),
        ("values", reqwest::Method::GET, "/cache/v1/values"),
        (
            "offset / known",
            reqwest::Method::GET,
            &*format!("/cache/v1/offset/{TOPIC}/0").leak(),
        ),
        (
            "offset / unknown partition",
            reqwest::Method::GET,
            &*format!("/cache/v1/offset/{TOPIC}/99").leak(),
        ),
        (
            "offset / unknown topic",
            reqwest::Method::GET,
            "/cache/v1/offset/nope/0",
        ),
        (
            "offset / empty topic",
            reqwest::Method::GET,
            "/cache/v1/offset//0",
        ),
        (
            "offset / partition not an int",
            reqwest::Method::GET,
            &*format!("/cache/v1/offset/{TOPIC}/x").leak(),
        ),
        (
            "404 / unknown path",
            reqwest::Method::GET,
            "/cache/v1/unknown",
        ),
        ("405 / POST raw", reqwest::Method::POST, "/cache/v1/raw/k1"),
        (
            "405 / DELETE raw",
            reqwest::Method::DELETE,
            "/cache/v1/raw/k1",
        ),
        ("404 / root", reqwest::Method::GET, "/"),
        (
            "openapi.json (mirror-only)",
            reqwest::Method::GET,
            "/openapi.json",
        ),
    ];

    let mut findings: Vec<Finding> = Vec::with_capacity(probes.len());
    for (label, method, path) in probes {
        let k = fetch(&client, &kkv_base, method.clone(), path).await;
        let m = fetch(&client, &mirror_base, method.clone(), path).await;
        findings.push(Finding {
            label: label.to_string(),
            method: method.to_string(),
            path: path.to_string(),
            kkv: k,
            mirror: m,
        });
    }

    let report = render_markdown(&findings, &host_bootstrap, bootstrap_hwm);
    println!(
        "\n\n--- BEGIN COMPATIBILITY REPORT ---\n{report}\n--- END COMPATIBILITY REPORT ---\n"
    );

    mirror.abort();
    let _ = mirror_shutdown_tx.send(());
    let _ = mirror_server.await;
}

struct Finding {
    label: String,
    method: String,
    path: String,
    kkv: Resp,
    mirror: Resp,
}

fn render_markdown(findings: &[Finding], host_bootstrap: &str, hwm: u64) -> String {
    let mut s = String::new();
    s.push_str("# `/cache/v1` compatibility probe — KKV vs mirror-v3\n\n");
    s.push_str(&format!(
        "Generated by `cargo test -p mirror-e2e --test cache_v1_compat -- --ignored --nocapture`.\n\n\
         - **KKV image:** `{KKV_IMAGE}:{KKV_TAG_AND_DIGEST}`\n\
         - **Source broker:** `{host_bootstrap}` (`{TOPIC}`, hwm at probe = {hwm})\n\n"
    ));
    s.push_str("Each row is one HTTP probe issued against both servers. Bodies are shown as quoted UTF-8 where printable, or as `hex(...)` otherwise. The `Status` and `Body` columns flag `=` when identical, `≠` when not.\n\n");
    for f in findings {
        s.push_str(&format!("## {} — `{} {}`\n\n", f.label, f.method, f.path));
        let kkv_offsets = header(&f.kkv, "x-kkv-last-seen-offsets");
        let mirror_offsets = header(&f.mirror, "x-kkv-last-seen-offsets");
        let kkv_ct = header(&f.kkv, "content-type");
        let mirror_ct = header(&f.mirror, "content-type");
        s.push_str(&format!(
            "| | KKV | mirror-v3 | Diff |\n|---|---|---|---|\n\
             | Status | `{}` | `{}` | {} |\n\
             | `Content-Type` | `{}` | `{}` | {} |\n\
             | `x-kkv-last-seen-offsets` | `{}` | `{}` | {} |\n\
             | Body | {} | {} | {} |\n\n",
            f.kkv.status,
            f.mirror.status,
            mark(f.kkv.status == f.mirror.status),
            kkv_ct,
            mirror_ct,
            mark(kkv_ct == mirror_ct),
            kkv_offsets,
            mirror_offsets,
            mark(kkv_offsets == mirror_offsets),
            body_repr(&f.kkv.body),
            body_repr(&f.mirror.body),
            mark(f.kkv.body == f.mirror.body),
        ));
    }
    s
}

fn header(r: &Resp, name: &str) -> String {
    r.headers
        .iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.clone())
        .unwrap_or_else(|| "<absent>".into())
}

fn mark(b: bool) -> &'static str {
    if b {
        "="
    } else {
        "≠"
    }
}

fn body_repr(b: &[u8]) -> String {
    if b.is_empty() {
        return "`<empty>`".into();
    }
    match std::str::from_utf8(b) {
        Ok(s) if s.chars().all(|c| c == '\n' || !c.is_control()) => {
            if s.len() > 200 {
                format!("`{:?}…`", &s[..200])
            } else {
                format!("`{s:?}`")
            }
        }
        _ => {
            let head: String = b.iter().take(40).map(|b| format!("{b:02x}")).collect();
            format!(
                "`hex({head}{}` ({} bytes)`",
                if b.len() > 40 { "…" } else { "" },
                b.len()
            )
        }
    }
}

async fn wait_until_200(
    client: &reqwest::Client,
    base: &str,
    path: &str,
    deadline: Duration,
) -> Result<()> {
    let stop = Instant::now() + deadline;
    let url = format!("{base}{path}");
    while Instant::now() < stop {
        if let Ok(r) = client.get(&url).send().await {
            if r.status() == reqwest::StatusCode::OK {
                return Ok(());
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    anyhow::bail!("never reached 200 within {deadline:?}: {url}")
}

async fn start_redpanda_dual_listeners() -> Result<(u16, u16, ContainerAsync<GenericImage>)> {
    let host_port = portpicker::pick_unused_port().context("port host")?;
    let docker_port = portpicker::pick_unused_port().context("port docker")?;
    let container = GenericImage::new(REDPANDA_IMAGE, REDPANDA_TAG)
        .with_exposed_port(ContainerPort::Tcp(9092))
        .with_exposed_port(ContainerPort::Tcp(29092))
        .with_wait_for(WaitFor::message_on_stderr("Successfully started Redpanda"))
        .with_cmd([
            "redpanda".into(),
            "start".into(),
            "--mode".into(),
            "dev-container".into(),
            "--smp".into(),
            "1".into(),
            "--kafka-addr".into(),
            "external://0.0.0.0:9092,internal://0.0.0.0:29092".into(),
            "--advertise-kafka-addr".into(),
            format!(
                "external://localhost:{host_port},internal://host.docker.internal:{docker_port}"
            ),
        ])
        .with_mapped_port(host_port, ContainerPort::Tcp(9092))
        .with_mapped_port(docker_port, ContainerPort::Tcp(29092))
        .start()
        .await
        .context("redpanda")?;
    wait_for_metadata(&format!("localhost:{host_port}")).await?;
    Ok((host_port, docker_port, container))
}

async fn start_kkv(
    host_port: u16,
    docker_bootstrap: &str,
    topic: &str,
) -> Result<ContainerAsync<GenericImage>> {
    GenericImage::new(KKV_IMAGE, KKV_TAG_AND_DIGEST)
        .with_exposed_port(ContainerPort::Tcp(8080))
        // KKV is a Quarkus app; the startup banner ends with the
        // listener line below. If the image's banner changes we'll
        // see a timeout, not a silent stall.
        .with_wait_for(WaitFor::message_on_stdout("Listening on:"))
        .with_env_var("KAFKA_BOOTSTRAP", docker_bootstrap)
        .with_env_var("KAFKA_INCOMING_TOPIC", topic)
        .with_env_var("KAFKA_GROUP_ID", KKV_GROUP)
        // Default is "latest" — would race with our pre-produced
        // fixture; "earliest" guarantees KKV picks the whole set up.
        .with_env_var("ONUPDATE_AFTER_OFFSET", "earliest")
        .with_mapped_port(host_port, ContainerPort::Tcp(8080))
        .start()
        .await
        .context("kkv container")
}

async fn wait_for_metadata(bootstrap: &str) -> Result<()> {
    use rdkafka::config::ClientConfig;
    use rdkafka::consumer::{BaseConsumer, Consumer};
    use rdkafka::util::Timeout;

    let bs = bootstrap.to_string();
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let bs = bs.clone();
        let ok = tokio::task::spawn_blocking(move || -> Result<()> {
            let c: BaseConsumer = ClientConfig::new()
                .set("bootstrap.servers", &bs)
                .set("group.id", "compat-probe")
                .create()
                .context("probe")?;
            c.fetch_metadata(None, Timeout::After(Duration::from_secs(2)))
                .map(|_| ())
                .map_err(|e| anyhow::anyhow!("metadata: {e}"))
        })
        .await
        .map_err(|e| anyhow::anyhow!("join: {e}"))?;
        if ok.is_ok() {
            return Ok(());
        }
        if Instant::now() > deadline {
            return ok;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

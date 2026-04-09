use anyhow::{Context, Result};
use clap::Parser;
use opra_pcap_replayer::{arrow_sink, opra_decoder, telemetry, trino_client};
use reqwest::Client;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::info;

#[derive(Parser, Debug)]
#[command(
    name = "opra-pcap-replayer",
    version,
    about = "OPRA PCAP → Arrow/Iceberg"
)]
struct Cli {
    /// Directory that contains ny4-small-10k/1m/10m sample PCAP files.
    #[arg(long, default_value_t=String::from("./pcap_samples"))]
    pcap_dir: String,
    /// Optional Rayon worker count (defaults to available logical CPUs).
    #[arg(long)]
    parallel: Option<usize>,
}

#[tokio::main()]
/// Entry point: read PCAP once, decode headers/trades, and write both parquet outputs.
async fn main() -> Result<()> {
    telemetry::init();
    let cli = Cli::parse();
    info!("starting ingest run: {:?}", cli);

    preflight_minio_endpoint()
        .await
        .context("MinIO endpoint preflight failed before ingest")?;

    let auto_create_trino = std::env::var("OPRA_AUTO_CREATE_TRINO_SCHEMA")
        .unwrap_or_else(|_| String::from("true"))
        .eq_ignore_ascii_case("true");
    if auto_create_trino {
        trino_client::ensure_trino_schema_and_trades_table()
            .await
            .context("failed to auto-create Trino schema/table")?;
        info!("ensured Trino schema/table for OPRA trades");
    }

    let parallel = cli
        .parallel
        .filter(|threads| *threads > 0)
        .unwrap_or_else(|| {
            // Added parallelism with Rayon after benchmarks showed significant batch improvement over 1m+ packets.
            // NOTE: this only works in the batch scenario where currently the packet size can fit into memory.
            std::thread::available_parallelism()
                .map(std::num::NonZeroUsize::get)
                .unwrap_or(1)
        });

    let sample_paths = sample_pcap_paths(&cli.pcap_dir);
    let loaded_pcaps = read_sample_pcaps(&sample_paths).await?;
    for (pcap_path, pcap_bytes) in loaded_pcaps {
        process_single_pcap(&pcap_path, pcap_bytes, parallel).await?;
    }

    Ok(())
}

async fn preflight_minio_endpoint() -> Result<()> {
    let endpoint =
        std::env::var("OPRA_MINIO_ENDPOINT").unwrap_or_else(|_| String::from("http://localhost:9000"));
    let health_url = format!("{}/minio/health/live", endpoint.trim_end_matches('/'));
    let root_url = endpoint.clone();

    let client = Client::builder()
        .timeout(std::time::Duration::from_secs(3))
        .build()
        .context("failed to build reqwest client for MinIO preflight")?;

    let proxy_http = std::env::var("HTTP_PROXY").unwrap_or_default();
    let proxy_https = std::env::var("HTTPS_PROXY").unwrap_or_default();
    let proxy_all = std::env::var("ALL_PROXY").unwrap_or_default();
    let no_proxy = std::env::var("NO_PROXY").unwrap_or_default();

    let health_result = client.get(&health_url).send().await;
    if let Ok(resp) = health_result {
        info!(
            "MinIO preflight: {} -> HTTP {} (endpoint: {})",
            health_url,
            resp.status(),
            endpoint
        );
        return Ok(());
    }

    let root_result = client.get(&root_url).send().await;
    match root_result {
        Ok(resp) => {
            info!(
                "MinIO root preflight: {} -> HTTP {} (health endpoint unavailable)",
                root_url,
                resp.status()
            );
            Ok(())
        }
        Err(error) => Err(anyhow::anyhow!(
            "unable to reach MinIO endpoint '{}'. health_url='{}', root_url='{}'. \
HTTP_PROXY='{}' HTTPS_PROXY='{}' ALL_PROXY='{}' NO_PROXY='{}'. \
Hint: if MinIO is local, unset proxy vars or set NO_PROXY=127.0.0.1,localhost. \
If MinIO serves TLS, use OPRA_MINIO_ENDPOINT=https://... . Root cause: {}",
            endpoint,
            health_url,
            root_url,
            proxy_http,
            proxy_https,
            proxy_all,
            no_proxy,
            error
        )),
    }
}

async fn process_single_pcap(
    pcap_path: &str,
    pcap_bytes: Arc<[u8]>,
    parallel: usize,
) -> Result<()> {
    info!("processing {} ({} bytes)", pcap_path, pcap_bytes.len());

    // Decode pcap headers into simple schema.
    let pcap_for_headers = Arc::clone(&pcap_bytes);
    let (stats, rows) = tokio::task::spawn_blocking(move || {
        opra_decoder::decode_pcap_headers_schema(&pcap_for_headers, parallel)
    })
    .await
    .context("row decode task failed to join")??;
    info!(
        "decoded {} packets, {} messages (skeleton), {} header rows for {}",
        stats.packets,
        stats.messages,
        rows.len(),
        pcap_path
    );

    // Write OPRA headers schema to local file.
    let local_path = local_parquet_output_path(pcap_path, "header");
    let local_path = arrow_sink::write_header_parquet(&local_path, &rows)?;
    info!("wrote {:?} with {} header rows", &local_path, rows.len());
    upload_parquet_to_minio(&local_path, "opra_headers").await?;

    // Decode pcap trades schema.
    let pcap_for_trades = Arc::clone(&pcap_bytes);
    let trade_rows = tokio::task::spawn_blocking(move || {
        opra_decoder::decode_pcap_trades_schema(&pcap_for_trades, parallel)
    })
    .await
    .context("trade decode task failed to join")??;

    // Write OPRA trades schema to local file.
    let trades_path = local_parquet_output_path(pcap_path, "trades");
    let trades_path = arrow_sink::write_trades_parquet(&trades_path, &trade_rows)?;
    info!(
        "wrote {:?} with {} decoded trade rows",
        &trades_path,
        trade_rows.len()
    );
    upload_parquet_to_minio(&trades_path, "opra_trades").await?;

    Ok(())
}

async fn upload_parquet_to_minio(local_path: &Path, dataset_prefix: &str) -> Result<()> {
    let endpoint =
        std::env::var("OPRA_MINIO_ENDPOINT").unwrap_or_else(|_| String::from("http://localhost:9000"));
    let access_key =
        std::env::var("OPRA_MINIO_ACCESS_KEY").unwrap_or_else(|_| String::from("minioadmin"));
    let secret_key =
        std::env::var("OPRA_MINIO_SECRET_KEY").unwrap_or_else(|_| String::from("minioadmin"));
    let bucket = std::env::var("OPRA_MINIO_BUCKET").unwrap_or_else(|_| String::from("market"));
    let base_prefix =
        std::env::var("OPRA_MINIO_PREFIX").unwrap_or_else(|_| String::from("bronze"));

    let filename = local_path
        .file_name()
        .and_then(std::ffi::OsStr::to_str)
        .filter(|name| !name.is_empty())
        .unwrap_or("output.parquet");
    let key = format!("{base_prefix}/{dataset_prefix}/{filename}");

    arrow_sink::upload_to_minio(
        &endpoint,
        &access_key,
        &secret_key,
        &bucket,
        &key,
        local_path,
    )
    .await
    .with_context(|| {
        format!(
            "failed to upload {:?} to s3://{}/{} via endpoint {}",
            local_path, bucket, key, endpoint
        )
    })?;

    info!(
        "uploaded {:?} to s3://{}/{} via endpoint {}",
        local_path, bucket, key, endpoint
    );
    Ok(())
}

fn sample_pcap_paths(pcap_dir: &str) -> [String; 3] {
    let root = Path::new(pcap_dir);
    [
        root.join("ny4-small-10k.pcap")
            .to_string_lossy()
            .into_owned(),
        root.join("ny4-small-1m.pcap")
            .to_string_lossy()
            .into_owned(),
        root.join("ny4-small-10m.pcap")
            .to_string_lossy()
            .into_owned(),
    ]
}

async fn read_sample_pcaps(paths: &[String; 3]) -> Result<Vec<(String, Arc<[u8]>)>> {
    let (ten_k, one_m, ten_m) = tokio::try_join!(
        opra_decoder::read_pcap_file(&paths[0]),
        opra_decoder::read_pcap_file(&paths[1]),
        opra_decoder::read_pcap_file(&paths[2]),
    )?;

    Ok(vec![
        (paths[0].clone(), Arc::<[u8]>::from(ten_k)),
        (paths[1].clone(), Arc::<[u8]>::from(one_m)),
        (paths[2].clone(), Arc::<[u8]>::from(ten_m)),
    ])
}

/// Build a local parquet output path from the PCAP filename and schema suffix.
fn local_parquet_output_path(pcap_path: &str, schema: &str) -> String {
    let input = Path::new(pcap_path);
    let stem = Path::new(pcap_path)
        .file_stem()
        .and_then(std::ffi::OsStr::to_str)
        .filter(|name| !name.is_empty())
        .unwrap_or("opra");
    let parent = input.parent().unwrap_or(Path::new("."));
    let output_path: PathBuf = parent.join(format!("{stem}_{schema}.parquet"));
    output_path.to_string_lossy().into_owned()
}

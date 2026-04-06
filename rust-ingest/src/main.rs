use clap::Parser;
use anyhow::{Context, Result};
use std::path::Path;
use std::sync::Arc;
use tracing::info;
mod telemetry;
mod opra_decoder;
mod arrow_sink;

#[derive(Parser, Debug)]
#[command(name="opra-pcap-replayer", version, about="OPRA PCAP → Arrow/Iceberg")]
struct Args {
    /// Path to an OPRA PCAP file
    #[arg(long)]
    pcap: String,
    /// Reserved for future cloud upload flow (currently local-only output).
    #[arg(long, default_value_t=String::from("s3://unused/local"))]
    bucket: String,
    /// Iceberg REST catalog endpoint (optional in skeleton)
    #[arg(long, default_value_t=String::from("http://localhost:8181"))]
    iceberg_catalog: String,
    /// MinIO/S3 endpoint <http://host:9000>
    #[arg(long, default_value_t=String::from("http://127.0.0.1:9000"))]
    minio_endpoint: String,
    #[arg(long, default_value_t=String::from("minioadmin"))]
    access_key: String,
    #[arg(long, default_value_t=String::from("minioadmin"))]
    secret_key: String,
}

#[tokio::main()]
async fn main() -> Result<()> {
    telemetry::init();
    let args = Args::parse();
    info!("starting ingest: {:?}", args);

    // Arc original pcap byte stream so it can be referenced counted across threads for both headers & trades schema
    let pcap_bytes = opra_decoder::read_pcap_file(&args.pcap).await?.into();

    // Added parallelism with Rayon after benchmarks showed significant batch improvement over 1m+ packets
    // NOTE: this only works in the batch scenario where currently the packet size can fit into memory, if the PCAPs
    // were larger we'd need to refactor to Async Streaming with the buffer in batches.
    let parallel = std::thread::available_parallelism()
        .map(std::num::NonZeroUsize::get)
        .unwrap_or(1);

    // Decode pcap headers into simple schema
    let pcap_for_headers = Arc::clone(&pcap_bytes);
    let (stats, rows) = tokio::task::spawn_blocking(move ||
        opra_decoder::decode_pcap_headers_schema(&pcap_for_headers, parallel))
        .await
        .context("row decode task failed to join")??;
    info!(
        "decoded {} packets, {} messages (skeleton), {} header rows",
        stats.packets,
        stats.messages,
        rows.len()
    );

    let local_path = local_parquet_output_path(&args.pcap);
    let local_path = arrow_sink::write_header_parquet(&local_path, &rows)?;
    info!("wrote {:?} with {} header rows", &local_path, rows.len());

    let pcap_for_trades = Arc::clone(&pcap_bytes);
    let trade_rows = tokio::task::spawn_blocking(move || {
        opra_decoder::decode_pcap_trades_schema(&pcap_for_trades.clone(), parallel)
    })
    .await
    .context("trade decode task failed to join")??;

    let trades_path = local_parquet_output_path(&args.pcap);
    let trades_path = arrow_sink::write_trades_parquet(&trades_path, &trade_rows)?;
    info!(
        "wrote {:?} with {} decoded trade rows",
        &trades_path,
        trade_rows.len()
    );

    Ok(())
}

fn local_parquet_output_path(pcap_path: &str) -> String {
    let stem = Path::new(pcap_path)
        .file_stem()
        .and_then(std::ffi::OsStr::to_str)
        .filter(|name| !name.is_empty())
        .unwrap_or("opra");
    format!("./pcap_samples/{}_header.parquet", stem)
}

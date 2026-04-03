use clap::Parser;
use anyhow::{Context, Result};
use std::path::Path;
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
    /// Parquet row group target size (bytes)
    #[arg(long, default_value_t=128*1024*1024)]
    row_group_bytes: usize,
    /// Dry-run: parse and count messages only
    #[arg(long, default_value_t=false)]
    dry_run: bool,
    /// Also decode quote/trade-like OPRA message rows and write a trades parquet.
    #[arg(long, default_value_t=false)]
    decode_trades: bool,
    /// Decode quote rows into a DataBento-like cmbp-1 shape and write an MBP parquet.
    #[arg(long, default_value_t=false)]
    decode_mbp: bool,
}

#[tokio::main()]
async fn main() -> Result<()> {
    telemetry::init();
    let args = Args::parse();
    info!("starting ingest: {:?}", args);

    // Async ability to read bytes from N files at once; dev is just on 1 file
    let pcap_bytes = opra_decoder::read_pcap_file(&args.pcap).await?;

    // Local-first decode flow for parser development.
    let (stats, rows) = tokio::task::spawn_blocking(move ||
        opra_decoder::decode_pcap(&pcap_bytes))
        .await
        .context("row decode task failed to join")??;
    info!(
        "decoded {} packets, {} messages (skeleton), {} parsed rows",
        stats.packets,
        stats.messages,
        rows.len()
    );

    if args.dry_run {
        info!("dry run complete");
        return Ok(());
    }

    let local_path = local_parquet_output_path(&args.pcap);
    let local_path = arrow_sink::write_parsed_parquet(&local_path, &rows)?;
    info!("wrote {:?} with {} parsed rows", &local_path, rows.len());

    if args.decode_trades {
        let pcap_bytes = opra_decoder::read_pcap_file(&args.pcap).await?;
        let parallel = std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(1);
        let trade_rows = tokio::task::spawn_blocking(move || {
            opra_decoder::decode_trades_with_parallelism(&pcap_bytes, parallel)
        })
        .await
        .context("trade decode task failed to join")??;

        let trades_path = local_trades_parquet_output_path(&args.pcap);
        let trades_path = arrow_sink::write_trades_parquet(&trades_path, &trade_rows)?;
        info!(
            "wrote {:?} with {} decoded trade rows",
            &trades_path,
            trade_rows.len()
        );
    }

    if args.decode_mbp {
        let pcap_bytes = opra_decoder::read_pcap_file(&args.pcap).await?;
        let parallel = std::thread::available_parallelism()
            .map(std::num::NonZeroUsize::get)
            .unwrap_or(1);
        let mbp_rows = tokio::task::spawn_blocking(move || {
            opra_decoder::decode_mbp_with_parallelism(&pcap_bytes, parallel)
        })
        .await
        .context("mbp decode task failed to join")??;

        let mbp_path = local_mbp_parquet_output_path(&args.pcap);
        let mbp_path = arrow_sink::write_mbp_parquet(&mbp_path, &mbp_rows)?;
        info!("wrote {:?} with {} decoded mbp rows", &mbp_path, mbp_rows.len());
    }
    Ok(())
}

fn local_parquet_output_path(pcap_path: &str) -> String {
    let stem = Path::new(pcap_path)
        .file_stem()
        .and_then(std::ffi::OsStr::to_str)
        .filter(|name| !name.is_empty())
        .unwrap_or("opra");
    format!("./pcap_samples/{}_parsed.parquet", stem)
}

fn local_trades_parquet_output_path(pcap_path: &str) -> String {
    let stem = Path::new(pcap_path)
        .file_stem()
        .and_then(std::ffi::OsStr::to_str)
        .filter(|name| !name.is_empty())
        .unwrap_or("opra");
    format!("./pcap_samples/{}_trades.parquet", stem)
}

fn local_mbp_parquet_output_path(pcap_path: &str) -> String {
    let stem = Path::new(pcap_path)
        .file_stem()
        .and_then(std::ffi::OsStr::to_str)
        .filter(|name| !name.is_empty())
        .unwrap_or("opra");
    format!("./pcap_samples/{}_mbp.parquet", stem)
}

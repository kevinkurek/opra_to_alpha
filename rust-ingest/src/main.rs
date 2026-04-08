use anyhow::{Context, Result};
use clap::Parser;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::info;
mod arrow_sink;
mod opra_decoder;
mod telemetry;

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

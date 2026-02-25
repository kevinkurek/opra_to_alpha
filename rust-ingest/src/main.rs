use clap::Parser;
use anyhow::Result;
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
    /// <s3://bucket/prefix> for bronze output (Parquet/Arrow IPC)
    #[arg(long)]
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
    /// Parallel decode workers
    #[arg(long, default_value_t=4)]
    parallel: usize,
    /// Parquet row group target size (bytes)
    #[arg(long, default_value_t=128*1024*1024)]
    row_group_bytes: usize,
    /// Dry-run: parse and count messages only
    #[arg(long, default_value_t=false)]
    dry_run: bool,
}

#[tokio::main()]
async fn main() -> Result<()> {
    telemetry::init();
    let args = Args::parse();
    info!("starting ingest: {:?}", args);

    let stats = opra_decoder::decode_pcap(&args.pcap, args.parallel).await?;
    info!("decoded {} packets, {} messages (skeleton)", stats.packets, stats.messages);

    if args.dry_run {
        info!("dry run complete");
        return Ok(());
    }

    // 1) Write demo parquet locally (replace with real batches later)
    let local_path = "./pcap_samples/demo_bronze.parquet";
    let local_path = arrow_sink::write_demo_parquet(local_path)?;
    info!("wrote {:?}", &local_path);

    // 2) Parse s3 URL like s3://market/bronze/opra_pcap/
    let (bucket, prefix) = parse_s3_url(&args.bucket)?;
    // simple object key name
    let ts_key = format!("{}demo_bronze.parquet", normalize_prefix(&prefix));

    // 3) Upload to MinIO
    arrow_sink::upload_to_minio(
        &args.minio_endpoint,
        &args.access_key,
        &args.secret_key,
        &bucket,
        &ts_key,
        &local_path,
    ).await?;

    info!("uploaded to s3://{}/{}", bucket, ts_key);
    Ok(())
}

fn parse_s3_url(url: &str) -> Result<(String, String)> {
    // expect s3://bucket/prefix/...
    let u = url.strip_prefix("s3://").ok_or_else(|| anyhow::anyhow!("bucket must start with s3://"))?;
    let mut parts = u.splitn(2, '/');
    let bucket = parts.next().unwrap_or_default().to_string();
    let prefix = parts.next().unwrap_or("").to_string();
    if bucket.is_empty() {
        return Err(anyhow::anyhow!("missing bucket name"));
    }
    Ok((bucket, prefix))
}

fn normalize_prefix(p: &str) -> String {
    match p {
        "" => String::new(),
        s if s.ends_with('/') => s.to_string(),
        s => format!("{s}/"),
    }
}
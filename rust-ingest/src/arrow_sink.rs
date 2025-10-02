use anyhow::Result;
use arrow::array::{Int64Builder, StringBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub fn write_demo_parquet(path: &str) -> Result<PathBuf> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("symbol", DataType::Utf8, false),
        Field::new("msg_count", DataType::Int64, false),
    ]));

    let mut sym = StringBuilder::new();
    let mut cnt = Int64Builder::new();
    sym.append_value("AAPL240927C00190000");
    cnt.append_value(42);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(sym.finish()), Arc::new(cnt.finish())],
    )?;

    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(PathBuf::from(path))
}

pub async fn upload_to_minio(
    endpoint: &str,
    access_key: &str,
    secret_key: &str,
    bucket: &str,
    key: &str,
    local_path: &Path,
) -> Result<()> {
    use aws_config::meta::region::RegionProviderChain;
    use aws_config::BehaviorVersion;
    use aws_sdk_s3::config::Credentials;
    use aws_sdk_s3::primitives::ByteStream;
    use aws_sdk_s3::Client;

    // Static creds & custom endpoint (MinIO)
    let creds = Credentials::new(access_key, secret_key, None, None, "static");
    let region_provider = RegionProviderChain::first_try("us-east-1");
    let conf = aws_config::defaults(BehaviorVersion::v2025_08_07())
        .credentials_provider(creds)
        .region(region_provider.region().await)
        .endpoint_url(endpoint)
        .load()
        .await;
    let s3 = Client::new(&conf);

    // Create bucket if missing (idempotent)
    let buckets = s3.list_buckets().send().await?;
    let exists = buckets
        .buckets()
        .iter()
        .any(|b| b.name.as_deref() == Some(bucket));
    if !exists {
        // MinIO in "us-east-1" generally allows simple create
        let _ = s3.create_bucket().bucket(bucket).send().await;
    }

    // Upload
    let body = ByteStream::from_path(local_path.to_path_buf()).await?;
    s3.put_object()
        .bucket(bucket)
        .key(key)
        .body(body)
        .send()
        .await?;

    Ok(())
}
use crate::opra_decoder::{DecodedTradeRow, HeaderOpraRow};
use anyhow::Result;
use arrow::array::{Int64Builder, StringBuilder, UInt64Builder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::info;

/// Write packet-level OPRA header rows to a parquet file.
pub fn write_header_parquet(path: &str, rows: &[HeaderOpraRow]) -> Result<PathBuf> {
    info!(
        "writing parsed parquet to {:?} with {} rows",
        path,
        rows.len()
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new("packet_index", DataType::UInt64, false),
        Field::new("udp_payload_len", DataType::UInt64, false),
        Field::new("block_size", DataType::UInt64, false),
        Field::new("messages_in_block", DataType::UInt64, false),
    ]));

    let mut packet_index = UInt64Builder::new();
    let mut udp_payload_len = UInt64Builder::new();
    let mut block_size = UInt64Builder::new();
    let mut messages_in_block = UInt64Builder::new();

    for row in rows {
        packet_index.append_value(row.packet_index);
        udp_payload_len.append_value(row.udp_payload_len);
        block_size.append_value(row.block_size);
        messages_in_block.append_value(row.messages_in_block);
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(packet_index.finish()),
            Arc::new(udp_payload_len.finish()),
            Arc::new(block_size.finish()),
            Arc::new(messages_in_block.finish()),
        ],
    )?;

    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(PathBuf::from(path))
}

/// Write normalized quote/trade rows to parquet with nullable fields where data may be absent.
pub fn write_trades_parquet(path: &str, rows: &[DecodedTradeRow]) -> Result<PathBuf> {
    info!(
        "writing trades parquet to {:?} with {} rows",
        path,
        rows.len()
    );

    let schema = Arc::new(Schema::new(vec![
        Field::new("packet_index", DataType::UInt64, false),
        Field::new("block_sequence", DataType::UInt64, false),
        Field::new("block_timestamp_ns", DataType::UInt64, false),
        Field::new("block_timestamp_utc", DataType::Utf8, false),
        Field::new("message_index_in_block", DataType::UInt64, false),
        Field::new("participant", DataType::Utf8, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("type_code", DataType::Utf8, false),
        Field::new("indicator", DataType::Utf8, false),
        Field::new("symbol_root", DataType::Utf8, true),
        Field::new("osi_symbol", DataType::Utf8, true),
        Field::new("bid", DataType::Int64, true),
        Field::new("ask", DataType::Int64, true),
        Field::new("bid_size", DataType::UInt64, true),
        Field::new("ask_size", DataType::UInt64, true),
        Field::new("price", DataType::Int64, true),
        Field::new("size", DataType::UInt64, true),
        Field::new("side", DataType::Utf8, true),
        Field::new("action", DataType::Utf8, true),
        Field::new("flags", DataType::UInt64, true),
    ]));

    let mut packet_index = UInt64Builder::new();
    let mut block_sequence = UInt64Builder::new();
    let mut block_timestamp_ns = UInt64Builder::new();
    let mut block_timestamp_utc = StringBuilder::new();
    let mut message_index_in_block = UInt64Builder::new();
    let mut participant = StringBuilder::new();
    let mut category = StringBuilder::new();
    let mut type_code = StringBuilder::new();
    let mut indicator = StringBuilder::new();
    let mut symbol_root = StringBuilder::new();
    let mut osi_symbol = StringBuilder::new();
    let mut bid = Int64Builder::new();
    let mut ask = Int64Builder::new();
    let mut bid_size = UInt64Builder::new();
    let mut ask_size = UInt64Builder::new();
    let mut price = Int64Builder::new();
    let mut size = UInt64Builder::new();
    let mut side = StringBuilder::new();
    let mut action = StringBuilder::new();
    let mut flags = UInt64Builder::new();

    for row in rows {
        packet_index.append_value(row.packet_index);
        block_sequence.append_value(row.block_sequence);
        block_timestamp_ns.append_value(row.block_timestamp_ns);
        block_timestamp_utc.append_value(&row.block_timestamp_utc);
        message_index_in_block.append_value(row.message_index_in_block);
        participant.append_value(&row.participant);
        category.append_value(&row.category);
        type_code.append_value(&row.type_code);
        indicator.append_value(&row.indicator);

        if let Some(v) = row.symbol_root.as_ref() {
            symbol_root.append_value(v);
        } else {
            symbol_root.append_null();
        }
        if let Some(v) = row.osi_symbol.as_ref() {
            osi_symbol.append_value(v);
        } else {
            osi_symbol.append_null();
        }
        if let Some(v) = row.bid {
            bid.append_value(v);
        } else {
            bid.append_null();
        }
        if let Some(v) = row.ask {
            ask.append_value(v);
        } else {
            ask.append_null();
        }
        if let Some(v) = row.bid_size {
            bid_size.append_value(v);
        } else {
            bid_size.append_null();
        }
        if let Some(v) = row.ask_size {
            ask_size.append_value(v);
        } else {
            ask_size.append_null();
        }
        if let Some(v) = row.price {
            price.append_value(v);
        } else {
            price.append_null();
        }
        if let Some(v) = row.size {
            size.append_value(v);
        } else {
            size.append_null();
        }
        if let Some(v) = row.side.as_ref() {
            side.append_value(v);
        } else {
            side.append_null();
        }
        if let Some(v) = row.action.as_ref() {
            action.append_value(v);
        } else {
            action.append_null();
        }
        if let Some(v) = row.flags {
            flags.append_value(v);
        } else {
            flags.append_null();
        }
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(packet_index.finish()),
            Arc::new(block_sequence.finish()),
            Arc::new(block_timestamp_ns.finish()),
            Arc::new(block_timestamp_utc.finish()),
            Arc::new(message_index_in_block.finish()),
            Arc::new(participant.finish()),
            Arc::new(category.finish()),
            Arc::new(type_code.finish()),
            Arc::new(indicator.finish()),
            Arc::new(symbol_root.finish()),
            Arc::new(osi_symbol.finish()),
            Arc::new(bid.finish()),
            Arc::new(ask.finish()),
            Arc::new(bid_size.finish()),
            Arc::new(ask_size.finish()),
            Arc::new(price.finish()),
            Arc::new(size.finish()),
            Arc::new(side.finish()),
            Arc::new(action.finish()),
            Arc::new(flags.finish()),
        ],
    )?;

    let file = File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema, None)?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(PathBuf::from(path))
}

#[allow(dead_code)]
/// Upload a local file to MinIO/S3 and create the bucket first if it does not exist.
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
    use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
    use aws_sdk_s3::Client;
    use tokio::io::AsyncReadExt;

    // Static creds & custom endpoint (MinIO)
    let creds = Credentials::new(access_key, secret_key, None, None, "static");
    let region_provider = RegionProviderChain::first_try("us-east-1");
    let conf = aws_config::defaults(BehaviorVersion::v2025_08_07())
        .credentials_provider(creds)
        .region(region_provider.region().await)
        .endpoint_url(endpoint)
        .load()
        .await;
    let force_path_style = std::env::var("OPRA_MINIO_FORCE_PATH_STYLE")
        .map(|value| value.eq_ignore_ascii_case("true"))
        .unwrap_or(true);
    let s3_config = aws_sdk_s3::config::Builder::from(&conf)
        .force_path_style(force_path_style)
        .build();
    let s3 = Client::from_conf(s3_config);

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

    // Upload strategy:
    // - Small files: single PutObject
    // - Large files: multipart upload with conservative 8 MiB parts to avoid
    //   MinIO's "chunk too big: choose chunk size <= 16MiB" constraint.
    let file_size = tokio::fs::metadata(local_path)
        .await
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    const SINGLE_UPLOAD_LIMIT_BYTES: u64 = 16 * 1024 * 1024;
    const MULTIPART_PART_SIZE_BYTES: usize = 8 * 1024 * 1024;

    if file_size <= SINGLE_UPLOAD_LIMIT_BYTES {
        let body = ByteStream::from_path(local_path.to_path_buf()).await?;
        s3.put_object()
            .bucket(bucket)
            .key(key)
            .body(body)
            .send()
            .await?;
        return Ok(());
    }

    let create_upload = s3
        .create_multipart_upload()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;
    let upload_id = create_upload
        .upload_id()
        .map(str::to_string)
        .ok_or_else(|| anyhow::anyhow!("multipart upload did not return upload_id"))?;

    let mut file = tokio::fs::File::open(local_path).await?;
    let mut parts: Vec<CompletedPart> = Vec::new();
    let mut part_number: i32 = 1;
    let mut remaining = file_size;

    let upload_result: Result<()> = async {
        while remaining > 0 {
            let target_len = usize::try_from(remaining.min(MULTIPART_PART_SIZE_BYTES as u64))
                .unwrap_or(MULTIPART_PART_SIZE_BYTES);
            let mut buffer = vec![0_u8; target_len];
            // Important: use read_exact so all non-final parts are exactly part-sized.
            file.read_exact(&mut buffer).await?;

            let upload_part = s3
                .upload_part()
                .bucket(bucket)
                .key(key)
                .upload_id(&upload_id)
                .part_number(part_number)
                .body(ByteStream::from(buffer))
                .send()
                .await?;

            let e_tag = upload_part
                .e_tag()
                .map(str::to_string)
                .ok_or_else(|| anyhow::anyhow!("upload_part missing etag for part {part_number}"))?;
            let completed_part = CompletedPart::builder()
                .part_number(part_number)
                .e_tag(e_tag)
                .build();
            parts.push(completed_part);
            remaining = remaining.saturating_sub(u64::try_from(target_len).unwrap_or(0));
            part_number = part_number.saturating_add(1);
        }

        let completed_upload = CompletedMultipartUpload::builder()
            .set_parts(Some(parts))
            .build();
        s3.complete_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(completed_upload)
            .send()
            .await?;
        Ok(())
    }
    .await;

    if let Err(error) = upload_result {
        let _ = s3
            .abort_multipart_upload()
            .bucket(bucket)
            .key(key)
            .upload_id(upload_id)
            .send()
            .await;
        return Err(error);
    }

    Ok(())
}

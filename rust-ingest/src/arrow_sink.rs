use anyhow::Result;
use arrow::array::{Float64Builder, Int64Builder, StringBuilder, UInt64Builder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tracing::info;
use crate::opra_decoder::{DecodedMbpRow, DecodedTradeRow, ParsedOpraRow};

pub fn write_demo_parquet(path: &str) -> Result<PathBuf> {

    // logging
    info!("writing demo parquet to {:?}", path);

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

pub fn write_parsed_parquet(path: &str, rows: &[ParsedOpraRow]) -> Result<PathBuf> {
    info!("writing parsed parquet to {:?} with {} rows", path, rows.len());

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

pub fn write_trades_parquet(path: &str, rows: &[DecodedTradeRow]) -> Result<PathBuf> {
    info!("writing trades parquet to {:?} with {} rows", path, rows.len());

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
        Field::new("bid", DataType::Float64, true),
        Field::new("ask", DataType::Float64, true),
        Field::new("bid_size", DataType::UInt64, true),
        Field::new("ask_size", DataType::UInt64, true),
        Field::new("price", DataType::Float64, true),
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
    let mut bid = Float64Builder::new();
    let mut ask = Float64Builder::new();
    let mut bid_size = UInt64Builder::new();
    let mut ask_size = UInt64Builder::new();
    let mut price = Float64Builder::new();
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

pub fn write_mbp_parquet(path: &str, rows: &[DecodedMbpRow]) -> Result<PathBuf> {
    info!("writing mbp parquet to {:?} with {} rows", path, rows.len());

    let schema = Arc::new(Schema::new(vec![
        Field::new("ts_event", DataType::UInt64, false),
        Field::new("ts_recv", DataType::UInt64, false),
        Field::new("ts_event_utc", DataType::Utf8, false),
        Field::new("rtype", DataType::UInt64, false),
        Field::new("publisher_id", DataType::UInt64, false),
        Field::new("instrument_id", DataType::UInt64, true),
        Field::new("action", DataType::Utf8, true),
        Field::new("side", DataType::Utf8, true),
        Field::new("price", DataType::Float64, true),
        Field::new("size", DataType::UInt64, true),
        Field::new("flags", DataType::UInt64, true),
        Field::new("ts_in_delta", DataType::Int64, false),
        Field::new("bid_px_00", DataType::Float64, true),
        Field::new("ask_px_00", DataType::Float64, true),
        Field::new("bid_sz_00", DataType::UInt64, true),
        Field::new("ask_sz_00", DataType::UInt64, true),
        Field::new("bid_pb_00", DataType::UInt64, false),
        Field::new("ask_pb_00", DataType::UInt64, false),
        Field::new("symbol", DataType::Utf8, true),
    ]));

    let mut ts_event = UInt64Builder::new();
    let mut ts_recv = UInt64Builder::new();
    let mut ts_event_utc = StringBuilder::new();
    let mut rtype = UInt64Builder::new();
    let mut publisher_id = UInt64Builder::new();
    let mut instrument_id = UInt64Builder::new();
    let mut action = StringBuilder::new();
    let mut side = StringBuilder::new();
    let mut price = Float64Builder::new();
    let mut size = UInt64Builder::new();
    let mut flags = UInt64Builder::new();
    let mut ts_in_delta = Int64Builder::new();
    let mut bid_px_00 = Float64Builder::new();
    let mut ask_px_00 = Float64Builder::new();
    let mut bid_sz_00 = UInt64Builder::new();
    let mut ask_sz_00 = UInt64Builder::new();
    let mut bid_pb_00 = UInt64Builder::new();
    let mut ask_pb_00 = UInt64Builder::new();
    let mut symbol = StringBuilder::new();

    for row in rows {
        ts_event.append_value(row.ts_event);
        ts_recv.append_value(row.ts_recv);
        ts_event_utc.append_value(&row.ts_event_utc);
        rtype.append_value(row.rtype);
        publisher_id.append_value(row.publisher_id);
        if let Some(v) = row.instrument_id {
            instrument_id.append_value(v);
        } else {
            instrument_id.append_null();
        }
        if let Some(v) = row.action.as_ref() {
            action.append_value(v);
        } else {
            action.append_null();
        }
        if let Some(v) = row.side.as_ref() {
            side.append_value(v);
        } else {
            side.append_null();
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
        if let Some(v) = row.flags {
            flags.append_value(v);
        } else {
            flags.append_null();
        }
        ts_in_delta.append_value(row.ts_in_delta);
        if let Some(v) = row.bid_px_00 {
            bid_px_00.append_value(v);
        } else {
            bid_px_00.append_null();
        }
        if let Some(v) = row.ask_px_00 {
            ask_px_00.append_value(v);
        } else {
            ask_px_00.append_null();
        }
        if let Some(v) = row.bid_sz_00 {
            bid_sz_00.append_value(v);
        } else {
            bid_sz_00.append_null();
        }
        if let Some(v) = row.ask_sz_00 {
            ask_sz_00.append_value(v);
        } else {
            ask_sz_00.append_null();
        }
        bid_pb_00.append_value(row.bid_pb_00);
        ask_pb_00.append_value(row.ask_pb_00);
        if let Some(v) = row.symbol.as_ref() {
            symbol.append_value(v);
        } else {
            symbol.append_null();
        }
    }

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(ts_event.finish()),
            Arc::new(ts_recv.finish()),
            Arc::new(ts_event_utc.finish()),
            Arc::new(rtype.finish()),
            Arc::new(publisher_id.finish()),
            Arc::new(instrument_id.finish()),
            Arc::new(action.finish()),
            Arc::new(side.finish()),
            Arc::new(price.finish()),
            Arc::new(size.finish()),
            Arc::new(flags.finish()),
            Arc::new(ts_in_delta.finish()),
            Arc::new(bid_px_00.finish()),
            Arc::new(ask_px_00.finish()),
            Arc::new(bid_sz_00.finish()),
            Arc::new(ask_sz_00.finish()),
            Arc::new(bid_pb_00.finish()),
            Arc::new(ask_pb_00.finish()),
            Arc::new(symbol.finish()),
        ],
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

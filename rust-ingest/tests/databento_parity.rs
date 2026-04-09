use anyhow::{anyhow, Context, Result};
use opra_pcap_replayer::{databento_client, opra_decoder};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

// Run
// cargo test --test databento_parity

#[derive(Debug, Clone, PartialEq)]
struct NormalizedTrade {
    ts_ns: u64,
    price: f64,
    size: u64,
}

fn normalize_price(px: f64) -> f64 {
    // Databento uses fixed-point nanodollar internally; decoder outputs f64.
    // Round to 4 decimals for practical parity checks across both sources.
    (px * 10_000.0).round() / 10_000.0
}

#[tokio::test]
async fn opra_decoder_trade_parity_with_databento_first_five() -> Result<()> {
    let _ = dotenvy::dotenv();

    let symbol = "SPY   230822P00436000";
    let start = "2023-08-22T14:30:00.000000Z";
    let end = "2023-08-22T14:31:00.000000Z";
    let start_dt = OffsetDateTime::parse(start, &Rfc3339)
        .map_err(|error| anyhow!("invalid start datetime: {error}"))?;
    let end_dt = OffsetDateTime::parse(end, &Rfc3339)
        .map_err(|error| anyhow!("invalid end datetime: {error}"))?;
    let start_ns = u64::try_from(start_dt.unix_timestamp_nanos())
        .map_err(|_| anyhow!("start datetime is negative and unsupported for this parity test"))?;
    let end_ns = u64::try_from(end_dt.unix_timestamp_nanos())
        .map_err(|_| anyhow!("end datetime is negative and unsupported for this parity test"))?;

    let pcap_path = "pcap_samples/ny4-small-1m.pcap";
    println!("PCAP Path: {:#?}", pcap_path);
    let pcap_bytes = opra_decoder::read_pcap_file(pcap_path).await?;
    let decoded_rows = opra_decoder::decode_pcap_trades_schema(&pcap_bytes, 1)?;

    let mut total_a_rows = 0_usize;
    let mut a_rows_in_window = 0_usize;
    let mut a_rows_for_symbol = 0_usize;
    let mut opra_norm: Vec<NormalizedTrade> = Vec::with_capacity(5);

    // Rows are already ordered by decoder output, so we can take first 5 matches directly.
    for row in decoded_rows {
        if row.category != "a" {
            continue;
        }
        total_a_rows = total_a_rows.saturating_add(1);

        if row.block_timestamp_ns < start_ns || row.block_timestamp_ns > end_ns {
            continue;
        }
        a_rows_in_window = a_rows_in_window.saturating_add(1);

        let Some(row_symbol) = row.osi_symbol else {
            continue;
        };
        if row_symbol != symbol {
            continue;
        }
        a_rows_for_symbol = a_rows_for_symbol.saturating_add(1);

        let Some(price) = row.price else {
            continue;
        };
        let Some(size) = row.size else {
            continue;
        };
        opra_norm.push(NormalizedTrade {
            ts_ns: row.block_timestamp_ns,
            price: normalize_price(price),
            size,
        });
        if opra_norm.len() == 5 {
            break;
        }
    }

    // Databento fetch_db_trades
    let db_rows = databento_client::fetch_db_trades(symbol, start_dt, end_dt)
        .await
        .with_context(|| format!("failed fetching Databento trades for symbol {symbol}"))?;

    let db_norm: Vec<NormalizedTrade> = db_rows
        .into_iter()
        .take(5)
        .map(|trade| NormalizedTrade {
            ts_ns: trade.hd.ts_event,
            price: normalize_price(trade.price_f64()),
            size: u64::from(trade.size),
        })
        .collect();

    if opra_norm.is_empty() {
        return Err(anyhow!(
            "no OPRA rows found for symbol={symbol}, start={start}, end={end} (total_a_rows={total_a_rows}, a_rows_in_window={a_rows_in_window}, a_rows_for_symbol={a_rows_for_symbol})"
        ));
    }
    if db_norm.is_empty() {
        return Err(anyhow!(
            "no Databento rows found for symbol={symbol}, start={start}, end={end}"
        ));
    }

    // Compare the first 5 between Rust Ingestion Pipeline and Databento Fetch
    let take_n = 5_usize.min(opra_norm.len()).min(db_norm.len());
    let opra_first = &opra_norm[..take_n];
    let db_first = &db_norm[..take_n];

    println!("symbol: {symbol}");
    println!("opra first {take_n}: {opra_first:#?}");
    println!("databento first {take_n}: {db_first:#?}");

    for idx in 0..take_n {
        let opra = &opra_first[idx];
        let db = &db_first[idx];

        let ts_delta_ns = opra.ts_ns.abs_diff(db.ts_ns);
        assert!(
            ts_delta_ns <= 1_000_000_000,
            "timestamp mismatch at idx {idx}: opra={} db={} (|delta|={}ns)",
            opra.ts_ns,
            db.ts_ns,
            ts_delta_ns
        );
        assert!(
            (opra.price - db.price).abs() <= 0.01,
            "price mismatch at idx {idx}: opra={} db={}",
            opra.price,
            db.price
        );
        assert_eq!(
            opra.size, db.size,
            "size mismatch at idx {idx}: opra={} db={}",
            opra.size, db.size
        );
    }

    Ok(())
}

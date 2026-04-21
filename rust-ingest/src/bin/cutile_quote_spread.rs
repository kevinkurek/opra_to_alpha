//! Compute per-symbol, per-minute average quote spread (bps) using cuTile for row-wise spread.
//!
//! Default path:
//!   pcap_samples/ny4-small-10m_trades.parquet
//!
//! Run:
//!   cargo run --release --bin cutile_quote_spread --features gpu-cutile -- [parquet_path] [max_rows]

#[cfg(not(feature = "gpu-cutile"))]
fn main() {
    println!(
        "cutile-rs is disabled.\n\
Enable it with:\n\
  cargo run --release --bin cutile_quote_spread --features gpu-cutile -- pcap_samples/ny4-small-10m_trades.parquet"
    );
}

#[cfg(feature = "gpu-cutile")]
mod gpu_run {
    use std::collections::HashMap;
    use std::fs::File;
    use std::sync::Arc;
    use std::time::Instant;

    use anyhow::{Context, Result};
    use arrow::array::{Array, Int64Array, StringArray, UInt64Array};
    use cutile::prelude::*;
    use kernel_module::spread_ratio_kernel;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    const WORK_ITERS: usize = 16;

    #[cutile::module]
    mod kernel_module {
        use cutile::core::*;

        #[cutile::entry()]
        fn spread_ratio_kernel<const S: [i32; 1]>(
            out: &mut Tensor<f32, S>,
            bid: &Tensor<f32, { [-1] }>,
            ask: &Tensor<f32, { [-1] }>,
        ) {
            let bid_t = load_tile_like_1d(bid, out);
            let ask_t = load_tile_like_1d(ask, out);
            let denom_t = ask_t + bid_t;
            let num_t = ask_t - bid_t;
            // Intentionally compute-heavy recurrence to increase arithmetic intensity per row.
            // This is a synthetic benchmarking score, not a market spread formula.
            let mut score_t = num_t / denom_t;
            score_t = (num_t + score_t) / (denom_t + score_t);
            score_t = (num_t + score_t) / (denom_t + score_t);
            score_t = (num_t + score_t) / (denom_t + score_t);
            score_t = (num_t + score_t) / (denom_t + score_t);
            score_t = (num_t + score_t) / (denom_t + score_t);
            score_t = (num_t + score_t) / (denom_t + score_t);
            score_t = (num_t + score_t) / (denom_t + score_t);
            score_t = (num_t + score_t) / (denom_t + score_t);
            score_t = (num_t + score_t) / (denom_t + score_t);
            score_t = (num_t + score_t) / (denom_t + score_t);
            score_t = (num_t + score_t) / (denom_t + score_t);
            score_t = (num_t + score_t) / (denom_t + score_t);
            score_t = (num_t + score_t) / (denom_t + score_t);
            score_t = (num_t + score_t) / (denom_t + score_t);
            score_t = (num_t + score_t) / (denom_t + score_t);
            score_t = (num_t + score_t) / (denom_t + score_t);
            out.store(score_t);
        }
    }

    #[derive(Clone, Debug)]
    struct QuoteKey {
        symbol: String,
        minute_bucket: u64,
    }

    fn resolve_partition_size(len: usize) -> usize {
        const CANDIDATES: [usize; 9] = [256, 128, 64, 32, 16, 8, 4, 2, 1];
        CANDIDATES
            .into_iter()
            .find(|candidate| len.is_multiple_of(*candidate))
            .unwrap_or(1)
    }

    type QuoteVectors = (Vec<f32>, Vec<f32>, Vec<QuoteKey>);

    fn read_quote_vectors(path: &str, max_rows: Option<usize>) -> Result<QuoteVectors> {
        let file = File::open(path).with_context(|| format!("failed to open parquet file: {path}"))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .with_context(|| format!("failed to build parquet reader for: {path}"))?;
        let mut reader = builder.with_batch_size(65_536).build()?;

        let mut bids = Vec::<f32>::new();
        let mut asks = Vec::<f32>::new();
        let mut keys = Vec::<QuoteKey>::new();

        for batch_result in &mut reader {
            let batch = batch_result?;
            let symbol_idx = batch
                .schema()
                .index_of("osi_symbol")
                .context("missing 'osi_symbol' column in parquet")?;
            let ts_idx = batch
                .schema()
                .index_of("block_timestamp_ns")
                .context("missing 'block_timestamp_ns' column in parquet")?;
            let bid_idx = batch
                .schema()
                .index_of("bid")
                .context("missing 'bid' column in parquet")?;
            let ask_idx = batch
                .schema()
                .index_of("ask")
                .context("missing 'ask' column in parquet")?;

            let symbol_col = batch.column(symbol_idx);
            let ts_col = batch.column(ts_idx);
            let bid_col = batch.column(bid_idx);
            let ask_col = batch.column(ask_idx);

            let symbol_arr = symbol_col
                .as_any()
                .downcast_ref::<StringArray>()
                .context("'osi_symbol' column is not Utf8")?;
            let ts_arr = ts_col
                .as_any()
                .downcast_ref::<UInt64Array>()
                .context("'block_timestamp_ns' column is not UInt64")?;
            let bid_arr = bid_col
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("'bid' column is not Int64")?;
            let ask_arr = ask_col
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("'ask' column is not Int64")?;

            for row in 0..batch.num_rows() {
                if symbol_arr.is_null(row) || ts_arr.is_null(row) || bid_arr.is_null(row) || ask_arr.is_null(row) {
                    continue;
                }

                let bid = bid_arr.value(row);
                let ask = ask_arr.value(row);
                if bid <= 0 || ask <= 0 || ask < bid {
                    continue;
                }

                let minute_bucket = ts_arr.value(row) / 60_000_000_000;
                bids.push(bid as f32);
                asks.push(ask as f32);
                keys.push(QuoteKey {
                    symbol: symbol_arr.value(row).to_string(),
                    minute_bucket,
                });

                if let Some(limit) = max_rows {
                    if bids.len() >= limit {
                        return Ok((bids, asks, keys));
                    }
                }
            }
        }

        Ok((bids, asks, keys))
    }

    fn aggregate_avg_score(keys: &[QuoteKey], scores: &[f32]) -> Vec<(String, u64, f32, u64)> {
        let mut grouped: HashMap<(String, u64), (f32, u64)> = HashMap::new();
        for (key, score) in keys.iter().zip(scores.iter()) {
            let entry = grouped
                .entry((key.symbol.clone(), key.minute_bucket))
                .or_insert((0.0, 0));
            entry.0 += *score;
            entry.1 = entry.1.saturating_add(1);
        }

        let mut rows: Vec<(String, u64, f32, u64)> = grouped
            .into_iter()
            .map(|((symbol, minute), (sum, count))| (symbol, minute, sum / count as f32, count))
            .collect();

        rows.sort_by(|a, b| b.3.cmp(&a.3).then_with(|| a.0.cmp(&b.0)).then_with(|| a.1.cmp(&b.1)));
        rows
    }

    pub fn run() -> Result<()> {
        let parquet_path = std::env::args()
            .nth(1)
            .unwrap_or_else(|| String::from("pcap_samples/ny4-small-10m_trades.parquet"));
        let max_rows = std::env::args()
            .nth(2)
            .and_then(|v| v.parse::<usize>().ok());

        let read_started = Instant::now();
        let (bids, asks, keys) = read_quote_vectors(&parquet_path, max_rows)?;
        let read_elapsed = read_started.elapsed();

        if bids.is_empty() {
            anyhow::bail!("no valid quote rows found (need non-null osi_symbol, timestamp, bid, ask)");
        }
        if bids.len() != asks.len() || bids.len() != keys.len() {
            anyhow::bail!("internal length mismatch");
        }

        let rows = bids.len();
        let partition = resolve_partition_size(rows);
        println!("quote rows used: {rows}");
        println!("partition size: {partition}");

        let cpu_started = Instant::now();
        let cpu_scores: Vec<f32> = bids
            .iter()
            .zip(asks.iter())
            .map(|(bid, ask)| {
                let denom = ask + bid;
                let num = ask - bid;
                let mut score = num / denom;
                for _ in 0..WORK_ITERS {
                    score = (num + score) / (denom + score);
                }
                score
            })
            .collect();
        let cpu_elapsed = cpu_started.elapsed();

        let gpu_started = Instant::now();
        let h2d_started = Instant::now();
        let bids_gpu = api::copy_host_vec_to_device(&Arc::new(bids.clone())).sync()?;
        let asks_gpu = api::copy_host_vec_to_device(&Arc::new(asks.clone())).sync()?;
        let mut score_gpu = api::zeros::<f32>(&[rows]).sync()?;
        let h2d_elapsed = h2d_started.elapsed();

        let kernel_started = Instant::now();
        spread_ratio_kernel((&mut score_gpu).partition([partition]), &bids_gpu, &asks_gpu).sync()?;
        let kernel_elapsed = kernel_started.elapsed();

        let d2h_started = Instant::now();
        let gpu_scores: Vec<f32> = score_gpu.to_host_vec().sync()?;
        let d2h_elapsed = d2h_started.elapsed();
        let gpu_elapsed = gpu_started.elapsed();

        let mut abs_diff_sum = 0.0_f64;
        for (cpu, gpu) in cpu_scores.iter().zip(gpu_scores.iter()) {
            abs_diff_sum += f64::from((cpu - gpu).abs());
        }
        let mean_abs_diff = abs_diff_sum / rows as f64;

        let agg_cpu_started = Instant::now();
        let grouped = aggregate_avg_score(&keys, &gpu_scores);
        let agg_cpu_elapsed = agg_cpu_started.elapsed();

        println!("work iters per row: {}", WORK_ITERS);
        println!("mean abs diff cpu vs gpu score: {:.12}", mean_abs_diff);
        println!("read parquet ms: {:.3}", read_elapsed.as_secs_f64() * 1_000.0);
        println!("cpu score compute ms: {:.3}", cpu_elapsed.as_secs_f64() * 1_000.0);
        println!("gpu h2d ms: {:.3}", h2d_elapsed.as_secs_f64() * 1_000.0);
        println!("gpu kernel ms: {:.3}", kernel_elapsed.as_secs_f64() * 1_000.0);
        println!("gpu d2h ms: {:.3}", d2h_elapsed.as_secs_f64() * 1_000.0);
        println!("gpu pipeline total ms: {:.3}", gpu_elapsed.as_secs_f64() * 1_000.0);
        println!("cpu groupby ms: {:.3}", agg_cpu_elapsed.as_secs_f64() * 1_000.0);
        println!(
            "speedup (cpu_score_compute / gpu_kernel): {:.2}x",
            cpu_elapsed.as_secs_f64() / kernel_elapsed.as_secs_f64().max(1e-12)
        );
        println!(
            "speedup (cpu_score_compute / gpu_pipeline_total): {:.2}x",
            cpu_elapsed.as_secs_f64() / gpu_elapsed.as_secs_f64().max(1e-12)
        );

        println!("top 15 symbol-minute average score (sorted by row_count):");
        for (idx, (symbol, minute, avg_score, count)) in grouped.iter().take(15).enumerate() {
            println!(
                "{:>2}. symbol={} minute_bucket={} avg_score={:.6} rows={}",
                idx + 1,
                symbol,
                minute,
                avg_score,
                count
            );
        }

        Ok(())
    }
}

#[cfg(feature = "gpu-cutile")]
fn main() -> anyhow::Result<()> {
    gpu_run::run()
}

//! Minimal parquet -> GPU smoke aggregation for OPRA trades.
//!
//! Default path:
//!   pcap_samples/ny4-small-10m_trades.parquet
//!
//! Run:
//!   cargo run --bin cutile_parquet_agg --features gpu-cutile -- [parquet_path] [max_rows]

#[cfg(not(feature = "gpu-cutile"))]
fn main() {
    println!(
        "cutile-rs is disabled.\n\
Enable it with:\n\
  cargo run --bin cutile_parquet_agg --features gpu-cutile -- pcap_samples/ny4-small-10m_trades.parquet"
    );
}

#[cfg(feature = "gpu-cutile")]
mod gpu_run {
    use std::fs::File;
    use std::sync::Arc;
    use std::time::Instant;

    use anyhow::{Context, Result};
    use arrow::array::{Array, Int64Array, UInt64Array};
    use cutile::prelude::*;
    use kernel_module::mul;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    #[cutile::module]
    mod kernel_module {
        use cutile::core::*;

        #[cutile::entry()]
        fn mul<const S: [i32; 1]>(
            out: &mut Tensor<f64, S>,
            x: &Tensor<f64, { [-1] }>,
            y: &Tensor<f64, { [-1] }>,
        ) {
            let tx = load_tile_like_1d(x, out);
            let ty = load_tile_like_1d(y, out);
            out.store(tx * ty);
        }
    }

    fn resolve_partition_size(len: usize) -> usize {
        const CANDIDATES: [usize; 9] = [256, 128, 64, 32, 16, 8, 4, 2, 1];
        CANDIDATES
            .into_iter()
            .find(|candidate| len.is_multiple_of(*candidate))
            .unwrap_or(1)
    }

    fn read_price_and_size_as_f64(path: &str, max_rows: Option<usize>) -> Result<(Vec<f64>, Vec<f64>)> {
        let file = File::open(path).with_context(|| format!("failed to open parquet file: {path}"))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .with_context(|| format!("failed to build parquet reader for: {path}"))?;
        let mut reader = builder.with_batch_size(65_536).build()?;

        let mut prices = Vec::<f64>::new();
        let mut sizes = Vec::<f64>::new();

        for batch_result in &mut reader {
            let batch = batch_result?;
            let price_idx = batch
                .schema()
                .index_of("price")
                .context("missing 'price' column in parquet")?;
            let size_idx = batch
                .schema()
                .index_of("size")
                .context("missing 'size' column in parquet")?;

            let price_col = batch.column(price_idx);
            let size_col = batch.column(size_idx);

            let price_arr = price_col
                .as_any()
                .downcast_ref::<Int64Array>()
                .context("'price' column is not Int64")?;
            let size_arr = size_col
                .as_any()
                .downcast_ref::<UInt64Array>()
                .context("'size' column is not UInt64")?;

            for row in 0..batch.num_rows() {
                if !price_arr.is_null(row) && !size_arr.is_null(row) {
                    prices.push(price_arr.value(row) as f64);
                    sizes.push(size_arr.value(row) as f64);
                }

                if let Some(limit) = max_rows {
                    if prices.len() >= limit {
                        return Ok((prices, sizes));
                    }
                }
            }
        }

        Ok((prices, sizes))
    }

    pub fn run() -> Result<()> {
        let parquet_path = std::env::args()
            .nth(1)
            .unwrap_or_else(|| String::from("pcap_samples/ny4-small-10m_trades.parquet"));
        let max_rows = std::env::args()
            .nth(2)
            .and_then(|v| v.parse::<usize>().ok());

        let read_started = Instant::now();
        let (prices, sizes) = read_price_and_size_as_f64(&parquet_path, max_rows)?;
        let read_elapsed = read_started.elapsed();
        if prices.is_empty() || sizes.is_empty() {
            anyhow::bail!("no non-null (price,size) rows were found");
        }
        if prices.len() != sizes.len() {
            anyhow::bail!(
                "mismatched vector lengths: prices={} sizes={}",
                prices.len(),
                sizes.len()
            );
        }

        let rows = prices.len();
        let partition = resolve_partition_size(rows);
        println!("rows used: {rows}");
        println!("partition size: {partition}");

        let cpu_started = Instant::now();
        let cpu_sum: f64 = prices
            .iter()
            .zip(sizes.iter())
            .map(|(p, s)| f64::from(*p) * f64::from(*s))
            .sum();
        let cpu_elapsed = cpu_started.elapsed();

        let gpu_pipeline_started = Instant::now();
        let h2d_started = Instant::now();
        let prices_gpu = api::copy_host_vec_to_device(&Arc::new(prices.clone())).sync()?;
        let sizes_gpu = api::copy_host_vec_to_device(&Arc::new(sizes.clone())).sync()?;
        let mut notional_gpu = api::zeros::<f64>(&[rows]).sync()?;
        let h2d_elapsed = h2d_started.elapsed();

        let kernel_started = Instant::now();
        mul((&mut notional_gpu).partition([partition]), &prices_gpu, &sizes_gpu).sync()?;
        let kernel_elapsed = kernel_started.elapsed();

        let d2h_started = Instant::now();
        let notional_host: Vec<f64> = notional_gpu.to_host_vec().sync()?;
        let d2h_elapsed = d2h_started.elapsed();

        let gpu_reduce_started = Instant::now();
        let gpu_sum: f64 = notional_host.iter().map(|&v| f64::from(v)).sum();
        let gpu_reduce_elapsed = gpu_reduce_started.elapsed();
        let gpu_pipeline_elapsed = gpu_pipeline_started.elapsed();
        let abs_diff = (gpu_sum - cpu_sum).abs();

        println!("cpu sum(price*size): {:.3}", cpu_sum);
        println!("gpu sum(price*size): {:.3}", gpu_sum);
        println!("abs diff: {:.6}", abs_diff);
        println!("sample notional[0..8]: {:?}", &notional_host[..8.min(notional_host.len())]);
        println!("read parquet ms: {:.3}", read_elapsed.as_secs_f64() * 1_000.0);
        println!("cpu compute ms: {:.3}", cpu_elapsed.as_secs_f64() * 1_000.0);
        println!("gpu h2d ms: {:.3}", h2d_elapsed.as_secs_f64() * 1_000.0);
        println!("gpu kernel ms: {:.3}", kernel_elapsed.as_secs_f64() * 1_000.0);
        println!("gpu d2h ms: {:.3}", d2h_elapsed.as_secs_f64() * 1_000.0);
        println!("gpu reduce ms: {:.3}", gpu_reduce_elapsed.as_secs_f64() * 1_000.0);
        println!(
            "gpu pipeline total ms: {:.3}",
            gpu_pipeline_elapsed.as_secs_f64() * 1_000.0
        );
        if cpu_elapsed.as_secs_f64() > 0.0 {
            println!(
                "speedup (cpu_compute / gpu_kernel): {:.2}x",
                cpu_elapsed.as_secs_f64() / kernel_elapsed.as_secs_f64().max(1e-12)
            );
            println!(
                "speedup (cpu_compute / gpu_pipeline_total): {:.2}x",
                cpu_elapsed.as_secs_f64() / gpu_pipeline_elapsed.as_secs_f64().max(1e-12)
            );
        }

        Ok(())
    }
}

#[cfg(feature = "gpu-cutile")]
fn main() -> anyhow::Result<()> {
    gpu_run::run()
}

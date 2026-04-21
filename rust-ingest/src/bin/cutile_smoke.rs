//! Smallest possible cuTile smoke test for this repo.
//!
//! Run without GPU feature:
//!   cargo run --bin cutile_smoke
//!
//! Run with cuTile enabled (Linux + CUDA toolchain required):
//!   cargo run --bin cutile_smoke --features gpu-cutile

#[cfg(not(feature = "gpu-cutile"))]
fn main() {
    println!(
        "cutile-rs is disabled.\n\
Enable it with:\n\
  cargo run --bin cutile_smoke --features gpu-cutile\n\
\n\
Note: cuTile currently requires Linux + CUDA + supported NVIDIA GPU."
    );
}

#[cfg(feature = "gpu-cutile")]
mod gpu_run {
    use anyhow::Result;
    use cutile::prelude::*;
    use kernel_module::add;

    #[cutile::module]
    mod kernel_module {
        use cutile::core::*;

        #[cutile::entry()]
        fn add<const S: [i32; 2]>(
            z: &mut Tensor<f32, S>,
            x: &Tensor<f32, { [-1, -1] }>,
            y: &Tensor<f32, { [-1, -1] }>,
        ) {
            let tile_x = load_tile_like_2d(x, z);
            let tile_y = load_tile_like_2d(y, z);
            z.store(tile_x + tile_y);
        }
    }

    pub fn run() -> Result<()> {
        let x = api::ones::<f32>(&[32, 32]).sync()?;
        let y = api::ones::<f32>(&[32, 32]).sync()?;
        let mut z = api::zeros::<f32>(&[32, 32]).sync()?;

        add((&mut z).partition([4, 4]), &x, &y).sync()?;

        let z_host: Vec<f32> = z.dup().to_host_vec().sync()?;
        let expected_len = 32 * 32;
        let expected_sum = 2.0_f32 * expected_len as f32;
        let actual_sum: f32 = z_host.iter().copied().sum();

        println!("cutile_smoke: kernel launched successfully");
        println!("z len: {}/{}", z_host.len(), expected_len);
        println!("z[0..8]: {:?}", &z_host[..8.min(z_host.len())]);
        println!("sum(z): {:.3} (expected {:.3})", actual_sum, expected_sum);

        if z_host.len() != expected_len {
            anyhow::bail!(
                "unexpected output length: got {}, expected {}",
                z_host.len(),
                expected_len
            );
        }
        if z_host.iter().any(|&v| (v - 2.0).abs() > 1e-5) {
            anyhow::bail!("validation failed: output tensor is not all 2.0 values");
        }

        Ok(())
    }
}

#[cfg(feature = "gpu-cutile")]
fn main() -> anyhow::Result<()> {
    gpu_run::run()
}

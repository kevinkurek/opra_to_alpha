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
        println!("cutile_smoke: kernel launched successfully");
        Ok(())
    }
}

#[cfg(feature = "gpu-cutile")]
fn main() -> anyhow::Result<()> {
    gpu_run::run()
}

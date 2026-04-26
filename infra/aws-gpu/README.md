# AWS GPU (Terraform)

This folder provisions a temporary NVIDIA GPU EC2 host for running:

```bash
cargo run --bin cutile_smoke --features gpu-cutile
```

## Environment Used (Terraform Defaults)

The Terraform in this folder is currently set up to launch:

- Instance type: `g5.xlarge` (default in `variables.tf`)
- AMI source: AWS DLAMI SSM parameter  
  `/aws/service/deeplearning/ami/x86_64/base-oss-nvidia-driver-gpu-ubuntu-22.04/latest/ami-id`
- OS family: Ubuntu 22.04 (from the DLAMI path above)
- SSH user: `ubuntu`

Quick GPU/host profile for this setup:

- GPU: NVIDIA A10G (1x)
- GPU memory: ~23 GB VRAM (`23028 MiB` shown in `nvidia-smi` from our run logs)
- GPU power limit shown: 300 W
- Typical `g5.xlarge` host sizing: 4 vCPU, 16 GiB RAM

If you override `instance_type` or `ami_id` in `terraform.tfvars`, your GPU/OS specs can differ from the above.

## Quick Start

```bash
cp terraform.tfvars.example terraform.tfvars
# edit values as needed

./up.sh
./run-cutile-smoke.sh
./down.sh
```

## How to actually connect with VS Code Remote SSH
1) Make sure you have the Remote SSH extension installed in VS Code.
2) Run `./up.sh` to provision the EC2 instance.
3) Note the public IP address output by `./up.sh` (e.g. `ec2-3-123-45-67.compute-1.amazonaws.com`).
4) In VS Code, open the Command Palette (Cmd+Shift+P) and select "Remote-SSH: Connect to Host...".
5) If you have an existing SSH config, the new host should automatically appear in the list. If not, you can add it manually to your `~/.ssh/config` on your local computer:
```Host my-aws-gpu
    HostName ec2-x-xxx-xxx-xxx.compute-1.amazonaws.com
    User ubuntu
    IdentityFile ~/.ssh/my-aws-gpu.pem
```
6) Select the host from the Remote-SSH list to connect. VS Code will establish an SSH connection to the EC2 instance and open a new window.
7) Run `./run-cutile-smoke.sh` since it will add `opra_to_alpha` repo and run the cutile smoke test on the GPU instance. You should see the output in the VS Code terminal.
8) You should now be able to see files like `opra_to_alpha/rust-ingest/bin/cutile_quote_spread` which WILL be highlighted since you're in an environment where the GPU binaries are built and run.
9) You may need to install `sudo apt install cargo` on the EC2 instance if you want to build new GPU binaries directly on the host.
10) Might need to have this at the root directory next ot Cargo.toml on the EC2 instance to ensure `rustfmt` and `clippy` are available for GPU code development:
```bash
# rust-toolchain.toml
[toolchain]
channel = "stable"
components = ["rustfmt", "clippy"]
```
11) When done, run `./down.sh` from your local terminal to destroy the EC2 instance and avoid ongoing costs.

## How Cutile Tiling Works (Conceptual)
CuTile RS — 1 Page Mental Model

1) What it is
CuTile RS is a Rust-based DSL + runtime for writing GPU kernels over tensors using a tile-based abstraction.
Instead of managing threads, blocks, and memory manually (like CUDA), you write math over tiles and let the runtime handle execution.

---

2) Core Idea
You do NOT write code for the entire dataset.

You write code for:
    → ONE TILE (a small 2D chunk of the tensor)

The runtime:
    → splits the full tensor into tiles
    → runs your kernel on each tile in parallel on the GPU

---

3) CPU vs GPU mindset

CPU:
    for i in 0..N:
        out[i] = f(x[i])

GPU (CuTile):
    define f(tile)
    → runtime applies f to ALL tiles at once

---

4) Tensors

A Tensor is just an N-dimensional numeric array on the GPU.

Example:
    shape = [32, 32]

    [x00 x01 x02 ...]
    [x10 x11 x12 ...]
    ...

Type annotation:
    Tensor<f32, {[-1, -1]}> = 2D tensor, dynamic size

---

5) Partitioning (MOST IMPORTANT CONCEPT)

Host code controls tiling:

    add((&mut z).partition([4, 4]), &x, &y)

This means:
    tile size = 4 x 4

So for a 32x32 tensor:
    32 / 4 = 8 tiles per dimension
    → 8 x 8 = 64 total tiles

Each tile triggers ONE kernel execution.

---

6) Kernel (what you write)

    #[cutile::entry()]
    fn add(z: &mut Tensor, x: &Tensor, y: &Tensor) {
        let tx = load_tile_like_2d(x, z);
        let ty = load_tile_like_2d(y, z);
        z.store(tx + ty);
    }

Interpretation:
    "For the current output tile z:
        - load matching tile from x
        - load matching tile from y
        - compute
        - store result back into z"

---

7) How tiles map to data

Full tensor (4x4), partition([2,2]):

    [A A | B B]
    [A A | B B]
    -------------
    [C C | D D]
    [C C | D D]

Tiles:
    A = tile(0,0)
    B = tile(0,1)
    C = tile(1,0)
    D = tile(1,1)

Runtime runs:
    add(A), add(B), add(C), add(D) in parallel

---

8) load_tile_like_2d

    load_tile_like_2d(x, z)

Means:
    "Load the portion of x that aligns with this output tile z"

So tile coordinates determine what slice of x is loaded.

---

9) Execution Flow

Host (CPU):
    - create tensors
    - choose tile size (partition)
    - launch kernel

GPU:
    - splits work into tiles
    - assigns tiles across threads/warps/blocks
    - executes kernel for each tile

---

10) What CuTile abstracts away

You do NOT manage:
    - threads
    - warps
    - blocks
    - memory coalescing manually

You DO control:
    - tensor shapes
    - tile sizes
    - math inside kernel

---

11) Final mental model

Think:

    partition → defines tiles
    kernel → defines math per tile
    runtime → applies kernel to all tiles in parallel

Or even tighter:

    "Write math for a small 2D block — GPU runs it everywhere."

## Example Tile Workflow

```bash
CuTile RS — load_tile_like_2d Example (Full Walkthrough)

START: Host defines 2D tensors (already 2D, no conversion inside kernel)

x =
[ 1   2   3   4 ]
[ 5   6   7   8 ]
[ 9  10  11  12 ]
[13  14  15  16 ]

y =
[10  20  30  40]
[50  60  70  80]
[90  91  92  93]
[94  95  96  97]

Host call:
partition([2,2])

→ This means tile size = 2x2
→ 4x4 tensor becomes 4 tiles total (2x2 grid of tiles)

--------------------------------------------------

FULL TENSOR VIEW (with tile boundaries)

[ 1   2 |  3   4 ]
[ 5   6 |  7   8 ]
-----------------
[ 9  10 | 11  12 ]
[13  14 | 15  16 ]

Same layout for y.

--------------------------------------------------

KERNEL LOGIC (runs once per tile)

fn add(z, x, y):
    tile_x = load_tile_like_2d(x, z)
    tile_y = load_tile_like_2d(y, z)
    z.store(tile_x + tile_y)

IMPORTANT:
z = current output tile (NOT full tensor)

--------------------------------------------------

KERNEL CALL 1 → tile (0,0)

z =
[ ?  ? ]
[ ?  ? ]

load_tile_like_2d(x, z) →
[1 2]
[5 6]

load_tile_like_2d(y, z) →
[10 20]
[50 60]

compute →
[11 22]
[55 66]

store into top-left of output

--------------------------------------------------

KERNEL CALL 2 → tile (0,1)

z =
[ ?  ? ]
[ ?  ? ]

load_tile_like_2d(x, z) →
[3 4]
[7 8]

load_tile_like_2d(y, z) →
[30 40]
[70 80]

compute →
[33 44]
[77 88]

store into top-right

--------------------------------------------------

KERNEL CALL 3 → tile (1,0)

z =
[ ?  ? ]
[ ?  ? ]

load_tile_like_2d(x, z) →
[ 9 10]
[13 14]

load_tile_like_2d(y, z) →
[90 91]
[94 95]

compute →
[ 99 101]
[107 109]

store into bottom-left

--------------------------------------------------

KERNEL CALL 4 → tile (1,1)

z =
[ ?  ? ]
[ ?  ? ]

load_tile_like_2d(x, z) →
[11 12]
[15 16]

load_tile_like_2d(y, z) →
[92 93]
[96 97]

compute →
[103 105]
[111 113]

store into bottom-right

--------------------------------------------------

FINAL OUTPUT

[ 11  22  33  44 ]
[ 55  66  77  88 ]
[ 99 101 103 105 ]
[107 109 111 113 ]

--------------------------------------------------

KEY TAKEAWAYS

1) Tensors are already 2D (host defines shape)
2) partition([2,2]) defines tile size = 2x2
3) Kernel runs once per tile (4 total calls here)
4) z = current output tile
5) load_tile_like_2d(x, z) = "load matching slice of x for this tile"
6) No implicit 1D → 2D conversion inside kernel
```

## Run Real Parquet Aggregation On GPU

Upload one parquet file and run a basic GPU aggregation (`price * size`, then sum):

```bash
./run-cutile-parquet-agg.sh
```

This script runs the binary in release mode (`cargo run --release ...`) for more realistic CPU/GPU timing.

Optional:

```bash
# custom parquet path + optional row cap for fast first test
./run-cutile-parquet-agg.sh ../../rust-ingest/pcap_samples/ny4-small-10m_trades.parquet 200000
```

## Run Quote Metric: Per-Symbol, Per-Minute Avg Spread (bps)

This runs a cuTile kernel for row-wise spread and then groups by `(osi_symbol, minute_bucket)`:

```bash
./run-cutile-quote-spread.sh
```

Optional:

```bash
./run-cutile-quote-spread.sh ../../rust-ingest/pcap_samples/ny4-small-10m_trades.parquet 500000
```

## Notes

- SSH key pair is generated automatically by Terraform and written to `.ssh/`.
- `down.sh` maps to `terraform destroy -auto-approve`; always run it when done.

## Example output - just stand up GPU kernel and get success
```bash
./run-cutile-smoke.sh
>> A lot of NVIDIA and Rust setup output, ~5min, ending with running the cutile smoke binary on the EC2 instance.

Running cutile smoke binary on EC2...
Tue Apr 21 15:51:54 2026       
+-----------------------------------------------------------------------------------------+
| NVIDIA-SMI 580.126.09             Driver Version: 580.126.09     CUDA Version: 13.0     |
+-----------------------------------------+------------------------+----------------------+
| GPU  Name                 Persistence-M | Bus-Id          Disp.A | Volatile Uncorr. ECC |
| Fan  Temp   Perf          Pwr:Usage/Cap |           Memory-Usage | GPU-Util  Compute M. |
|                                         |                        |               MIG M. |
|=========================================+========================+======================|
|   0  NVIDIA A10G                    On  |   00000000:00:1E.0 Off |                    0 |
|  0%   21C    P8             11W /  300W |       0MiB /  23028MiB |      0%      Default |
|                                         |                        |                  N/A |
+-----------------------------------------+------------------------+----------------------+

+-----------------------------------------------------------------------------------------+
| Processes:                                                                              |
|  GPU   GI   CI              PID   Type   Process name                        GPU Memory |
|        ID   ID                                                               Usage      |
|=========================================================================================|
|  No running processes found                                                             |
+-----------------------------------------------------------------------------------------+
nvcc: NVIDIA (R) Cuda compiler driver
21.1.8
    Updating crates.io index
    Updating git repository `https://github.com/NVlabs/cutile-rs`
    Updating git submodule `https://github.com/NVIDIA/cuda-tile.git`
 Downloading crates ...
  Downloaded adler2 v2.0.1
  Downloaded alloc-no-stdlib v2.0.4
  Downloaded glob v0.3.3
  ....
nvcc: NVIDIA (R) Cuda compiler driver
21.1.8
tileiras: NVIDIA (R) Cuda Tile IR optimizing assembler
Copyright (c) 2005-2026 NVIDIA Corporation
Built on Fri_Mar_20_00:02:49_PDT_2026
Cuda compilation tools, release 13.2, V13.2.78
Build local.local.37668154_
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.27s
     Running `target/debug/cutile_smoke`
    cutile_smoke: kernel launched successfully
    z len: 1024/1024
    z[0..8]: [2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0, 2.0]
    sum(z): 2048.000 (expected 2048.000)
(base) kevinkurek@MacBook-Pro aws-gpu % 
```

## Example output - parquet aggregation on GPU

```bash
./run-cutile-parquet-agg.sh

Using SSH user: ubuntu
Syncing rust-ingest sources...
Uploading parquet: /Users/kevinkurek/Desktop/github/opra_to_alpha/rust-ingest/pcap_samples/ny4-small-10m_trades.parquet
Running GPU parquet aggregation on EC2...
tileiras: NVIDIA (R) Cuda Tile IR optimizing assembler
   Compiling opra-pcap-replayer v0.1.0 (/home/ubuntu/opra_to_alpha/rust-ingest)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 1.31s
     Running `target/debug/cutile_parquet_agg pcap_samples/ny4-small-10m_trades.parquet`
rows used: 5486
partition size: 2
cpu sum(price*size): 1488257553746561178009600.000
gpu sum(price*size): 1488257553746561178009600.000
abs diff: 0.000000
sample notional[0..8]: [2320000000.0, 130000000.0, 1310000000.0, 320000000.0, 250000000.0, 2320000000.0, 5000000000.0, 11600000000.0]
```

### CPU vs GPU debug
- CPU and GPU became accurate at f64 precision because at f32 precision the large notional values caused floating point errors.
- First we just make sure that the CPU and GPU computations produce the same results for the notional sums.
- The CPU is significantly faster for this small dataset, but the GPU pipeline is necessary for larger datasets where the parallelism can be leveraged.


### GPU quote spread aggregation vs CPU, f64 precision.

```bash
(base) kevinkurek@MacBook-Pro aws-gpu % ./run-cutile-quote-spread.sh
Using SSH user: ubuntu
Syncing rust-ingest sources...
Uploading parquet: /Users/kevinkurek/Desktop/github/opra_to_alpha/rust-ingest/pcap_samples/ny4-small-10m_trades.parquet
Running GPU quote spread aggregation on EC2...
tileiras: NVIDIA (R) Cuda Tile IR optimizing assembler
   Compiling opra-pcap-replayer v0.1.0 (/home/ubuntu/opra_to_alpha/rust-ingest)
    Finished `release` profile [optimized] target(s) in 3.42s
     Running `target/release/cutile_quote_spread pcap_samples/ny4-small-10m_trades.parquet`
quote rows used: 20829552
partition size: 16
mean abs diff cpu vs gpu spread_bps: 0.000000000000
read parquet ms: 3812.748
cpu spread compute ms: 100.544
gpu h2d ms: 597.120
gpu kernel ms: 563.942
gpu d2h ms: 190.863
gpu pipeline total ms: 1351.926
cpu groupby ms: 1562.684
speedup (cpu_spread_compute / gpu_kernel): 0.18x
speedup (cpu_spread_compute / gpu_pipeline_total): 0.07x
top 15 symbol-minute average spread_bps (sorted by row_count):
 1. symbol=SPY   230822P00439000 minute_bucket=28211910 avg_spread_bps=470.500893 rows=23737
 2. symbol=SPY   230823P00439000 minute_bucket=28211910 avg_spread_bps=279.354271 rows=22122
 3. symbol=SPY   230822C00440000 minute_bucket=28211910 avg_spread_bps=502.017784 rows=21170
 4. symbol=SPY   230823C00439000 minute_bucket=28211910 avg_spread_bps=210.527876 rows=20874
 5. symbol=SPY   230825C00440000 minute_bucket=28211910 avg_spread_bps=133.902029 rows=20694
 6. symbol=SPY   230823P00438000 minute_bucket=28211910 avg_spread_bps=339.125314 rows=20621
 7. symbol=SPY   230822C00439000 minute_bucket=28211910 avg_spread_bps=246.544628 rows=20172
 8. symbol=SPY   230822P00438000 minute_bucket=28211910 avg_spread_bps=460.921207 rows=20157
 9. symbol=SPY   230823C00440000 minute_bucket=28211910 avg_spread_bps=262.529748 rows=19933
10. symbol=SPY   230825P00439000 minute_bucket=28211910 avg_spread_bps=147.972320 rows=19722
11. symbol=SPY   230825C00439000 minute_bucket=28211910 avg_spread_bps=122.633726 rows=19682
12. symbol=SPY   230825P00440000 minute_bucket=28211910 avg_spread_bps=135.551406 rows=19595
13. symbol=SPY   230823C00441000 minute_bucket=28211910 avg_spread_bps=238.721458 rows=19379
14. symbol=SPY   230825P00438000 minute_bucket=28211910 avg_spread_bps=165.237244 rows=19047
15. symbol=SPY   230822P00440000 minute_bucket=28211910 avg_spread_bps=292.986872 rows=18937
``` 

### GPU quote score aggregation vs CPU, f32 with harder math (`work iters = 16`)

```bash
(base) kevinkurek@MacBook-Pro aws-gpu % ./run-cutile-quote-spread.sh

Using SSH user: ubuntu
Syncing rust-ingest sources...
Uploading parquet: /Users/kevinkurek/Desktop/github/opra_to_alpha/rust-ingest/pcap_samples/ny4-small-10m_trades.parquet
Running GPU quote spread aggregation on EC2...
tileiras: NVIDIA (R) Cuda Tile IR optimizing assembler
   Compiling opra-pcap-replayer v0.1.0 (/home/ubuntu/opra_to_alpha/rust-ingest)
    Finished `release` profile [optimized] target(s) in 3.43s
     Running `target/release/cutile_quote_spread pcap_samples/ny4-small-10m_trades.parquet`
quote rows used: 20829552
partition size: 16
work iters per row: 16
mean abs diff cpu vs gpu score: 0.000000000000
read parquet ms: 3798.059
cpu score compute ms: 210.834
gpu h2d ms: 474.695
gpu kernel ms: 156.418
gpu d2h ms: 53.116
gpu pipeline total ms: 684.230
cpu groupby ms: 1532.265
speedup (cpu_score_compute / gpu_kernel): 1.35x
speedup (cpu_score_compute / gpu_pipeline_total): 0.31x
```

What this output means:
- `work iters per row: 16` means each row does repeated arithmetic steps, so the kernel is more compute-heavy than basic spread.
- `mean abs diff cpu vs gpu score: 0.000000000000` confirms CPU and GPU matched numerically for this benchmark score.
- Apples-to-apples math comparison is `cpu score compute ms` vs `gpu kernel ms`.
- In this run, GPU kernel is faster for compute-only (`156.418 ms` vs `210.834 ms`), so GPU wins on math stage.
- `gpu pipeline total ms` is still slower than CPU compute because transfer overhead dominates (`h2d + d2h`).
- `cpu groupby ms` is a separate CPU aggregation phase (hash/group by symbol-minute) and is not included in GPU kernel timing.

Takeaway:
- Harder math per row moved this workload into a regime where GPU kernel outperforms CPU compute.
- End-to-end still has copy costs, so next gains come from reducing transfer overhead or moving more of the pipeline to GPU.

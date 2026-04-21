# AWS GPU (Terraform)

This folder provisions a temporary NVIDIA GPU EC2 host for running:

```bash
cargo run --bin cutile_smoke --features gpu-cutile
```

## Quick Start

```bash
cp terraform.tfvars.example terraform.tfvars
# edit values as needed

./up.sh
./run-cutile-smoke.sh
./down.sh
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
```bash

(base) kevinkurek@MacBook-Pro aws-gpu % ./run-cutile-parquet-agg.sh

Using SSH user: ubuntu
Syncing rust-ingest sources...
Uploading parquet: /Users/kevinkurek/Desktop/github/opra_to_alpha/rust-ingest/pcap_samples/ny4-small-10m_trades.parquet
Running GPU parquet aggregation on EC2...
tileiras: NVIDIA (R) Cuda Tile IR optimizing assembler
   Compiling opra-pcap-replayer v0.1.0 (/home/ubuntu/opra_to_alpha/rust-ingest)
    Finished `dev` profile [unoptimized + debuginfo] target(s) in 1.23s
     Running `target/debug/cutile_parquet_agg pcap_samples/ny4-small-10m_trades.parquet`
rows used: 5486
partition size: 2
cpu sum(price*size): 1488257553746561178009600.000
gpu sum(price*size): 1488257553746561178009600.000
abs diff: 0.000000
sample notional[0..8]: [2320000000.0, 130000000.0, 1310000000.0, 320000000.0, 250000000.0, 2320000000.0, 5000000000.0, 11600000000.0]
read parquet ms: 67865.210
cpu compute ms: 0.110
gpu h2d ms: 477.855
gpu kernel ms: 250.067
gpu d2h ms: 0.106
gpu reduce ms: 0.076
gpu pipeline total ms: 728.104
speedup (cpu_compute / gpu_kernel): 0.00x
speedup (cpu_compute / gpu_pipeline_total): 0.00x
```

```bash
# ran in release mode
rows used: 5486
partition size: 2
cpu sum(price*size): 1488257553746561178009600.000
gpu sum(price*size): 1488257553746561178009600.000
abs diff: 0.000000
sample notional[0..8]: [2320000000.0, 130000000.0, 1310000000.0, 320000000.0, 250000000.0, 2320000000.0, 5000000000.0, 11600000000.0]
read parquet ms: 3899.058
cpu compute ms: 0.006
gpu h2d ms: 1457.467
gpu kernel ms: 119.573
gpu d2h ms: 0.087
gpu reduce ms: 0.005
gpu pipeline total ms: 1577.132
speedup (cpu_compute / gpu_kernel): 0.00x
speedup (cpu_compute / gpu_pipeline_total): 0.00x
```


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

What this output means:
- `cpu spread compute ms` vs `gpu kernel ms` is the apples-to-apples math-only comparison.
- In this f64 run, the CPU is faster for the row math (`100.544 ms` CPU vs `563.942 ms` GPU kernel).
- `gpu pipeline total ms` includes copy overhead (`h2d` + kernel + `d2h`), so it is always larger than kernel-only.
- `cpu groupby ms` is a different stage (string hash/group aggregation by symbol+minute), so do not compare it directly to GPU kernel.

### What "harder math per row" means

The original spread kernel is a very light formula per row:
- `spread_bps = 10_000 * (ask - bid) / ((ask + bid)/2)`

This is only a few arithmetic operations, so the workload is memory/transfer heavy and CPU often wins.

"Harder math per row" means we intentionally add more arithmetic on each row before writing output:
- same inputs (`bid`, `ask`)
- more repeated compute steps per row (iterative recurrence)
- same row count

Why this helps:
- GPUs are strongest when each row has enough compute work.
- More math per row increases arithmetic intensity and can move the comparison from transfer-bound to compute-bound.
- That is why the later f32 + iterative run can show GPU kernel speedup even when simple spread does not.

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

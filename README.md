# 🧊 Trino + Iceberg + Postgres + MinIO + Airflow + Superset

A full local **lakehouse stack** for data ingestion, query federation, and orchestration—integrating Trino, Apache Iceberg, MinIO (S3-compatible object store), Postgres (metadata + Airflow DB), and Apache Airflow.  
Rust-based OPRA PCAP ingestion is also included for feed replay into MinIO.

![](datalake.jpg)

---

## Rough tree structure (with depth excluded for clarity)
```bash
tree -L 4 -I 'node_modules|__pycache__|logs|plugins|superset|debug|release' -P '*.yaml|*.properties|*.yml|*.rs|*.xml|*.pcap'
>>
├── airflow-docker
│   ├── config
│   ├── dags
│   └── docker-compose.yaml # sets up airflow
├── superset
│   └── docker-compose-non-dev.yaml # sets up superset
├── research
│   └── environment.yml
├── rust-ingest
│   ├── pcap_samples
│   │   └── ny4-opra-new-a-20230822T143000.pcap
│   ├── src
│   │   ├── arrow_sink.rs
│   │   ├── main.rs
│   │   ├── opra_decoder.rs
│   │   └── telemetry.rs
│   └── target
└── trino
    ├── docker-compose.yaml # sets up trino, minio, hive-metastore, hive-postgres db
    ├── etc
    │   ├── catalog
    │   │   ├── hive.properties
    │   │   └── iceberg.properties
    │   └── config.properties
    ├── hadoop
    │   └── conf
    │       └── core-site.xml
    ├── hive
    │   ├── conf
    │   │   └── hive-site.xml
    │   └── lib
    └── jars
```

---

## 🧠 Wireshark / tshark Utilities

```bash
brew install wireshark   # provides tshark & capinfos
cd rust-ingest/pcap_samples
tshark -r ny4-opra-new-a-20230822T143000.pcap -c 5
>>
    1   0.000000 162.69.45.40 → 224.0.204.40 UDP 154 45040 → 45040 Len=108
    2   0.000003 162.69.45.37 → 224.0.204.37 UDP 96 45037 → 45037 Len=50
    3   0.000004 162.69.45.38 → 224.0.204.38 UDP 212 45038 → 45038 Len=166
    4   0.000008 162.69.45.38 → 224.0.204.38 UDP 96 45038 → 45038 Len=50
    5   0.000009 162.69.45.37 → 224.0.204.37 UDP 154 45037 → 45037 Len=108

# check how many packets there are in total
capinfos -c ./pcap_samples/ny4-opra-new-a-20230822T143000.pcap
File name:           ./pcap_samples/ny4-opra-new-a-20230822T143000.pcap
Number of packets:   79 M

# export 1 packet to a json for easier inspection
tshark -r ny4-opra-new-a-20230822T143000.pcap -c 1 -T json > example_packets.json
```

| **Column** | **Example** | **Description** |
|-------------|--------------|-----------------|
| **No.** | `1` | Sequential frame number in the capture file. |
| **Time** | `0.000000` | Seconds since start of capture — useful for latency analysis. |
| **Source** | `162.69.45.40` | Source IP address (unicast sender of the OPRA feed). |
| **→** | `→` | Direction of the packet flow. |
| **Destination** | `224.0.204.40` | Multicast group address — identifies the OPRA channel. |
| **Protocol** | `UDP` | Transport protocol (OPRA uses UDP multicast). |
| **Length** | `154` | Total frame size (bytes on wire, including headers). |
| **Info** | `45040 → 45040 Len=108` | UDP layer summary: source port, destination port, and payload size. |

---

### OPRA Spec Primer

This parser is based on OPRA Pillar binary transmission structure:

- Transmission Block:
  - Block Header: `21` bytes
  - Block Data: one or more OPRA messages
  - Optional pad byte (`0x00`) when needed for even block length
- Message:
  - Message Header: `12` bytes
  - Message Body: format depends on `category + type + indicator`

Current OPRA references:

- OPRA Pillar Output Specification (Dec 6, 2024): <https://cdn.opraplan.com/documents/OPRA_Pillar_Output_Specification.pdf>
- OPRA Pillar Input Specification (Jul 25, 2024): <https://cdn.opraplan.com/documents/OPRA_Pillar_Input_Specification.pdf>

#### Block Header Layout (21 bytes)

| **Offset** | **Length** | **Field** |
|------------|------------|-----------|
| `0` | `1` | Version |
| `1..3` | `2` | Block Size (big-endian) |
| `3` | `1` | Data Feed Indicator (`'O'`) |
| `4` | `1` | Retransmission Indicator |
| `5` | `1` | Session Indicator |
| `6..10` | `4` | Block Sequence Number |
| `10` | `1` | Messages In Block |
| `11..19` | `8` | Block Timestamp (`sec` + `nsec`) |
| `19..21` | `2` | Block Checksum |

#### Message Header Layout (12 bytes)

| **Offset** | **Length** | **Field** |
|------------|------------|-----------|
| `0` | `1` | Participant ID |
| `1` | `1` | Message Category |
| `2` | `1` | Message Type |
| `3` | `1` | Message Indicator |
| `4..8` | `4` | Transaction ID |
| `8..12` | `4` | Participant Reference Number |

#### Byte Example

From a real OPRA payload prefix used in tests:

```text
06 00 6c 4f 20 00 15 30 b0 03 03 64 e4 c6 67 3b 97 bf 00 1e ...
```

Decoded:

- `block_size` = `0x006c` = `108`
- feed indicator = `0x4f` = `'O'`
- `messages_in_block` = `0x03` = `3`
- `block_sequence` = `0x1530b003` = `355512323`
- timestamp bytes split into seconds + nanoseconds

#### What Our Code Does Today

1. Strip Ethernet/VLAN/IPv4/UDP and isolate UDP payload.
2. Parse OPRA block header.
3. Split block data into fixed-size messages for that block.
4. Parse 12-byte message header.
5. Decode `q` and `k` quote-family rows into `DecodedTradeRow`.

#### What “Production-Like” Still Requires

- Full dispatch by `category + type + indicator` across OPRA message families (not just `q/k`)
- Exact appendage handling (none/single/double) where spec requires it
- Full trade-print family parsing to populate true trade fields (`price/size/conditions/...`)
- Category-specific handling for variable-length administrative/control messages

---

### Example Trade Parquet Schema (`*_trades.parquet`)

```bash
# decode block rows + trade/quote-like rows into local parquet
cd rust-ingest
cargo run --release -- --pcap ./pcap_samples/ny4-small-10k.pcap --decode-trades

# inspect resulting trade parquet in Python
python - <<'PY'
import pandas as pd
df = pd.read_parquet("./pcap_samples/ny4-small-10k_trades.parquet")
print(df.head(5))
PY
```

Current v1 output columns:

| **Column** | **How it is parsed** | **Meaning** |
|-------------|----------------------|-------------|
| `packet_index` | Index from `par_iter().enumerate()` | Packet position in the PCAP file. |
| `block_sequence` | OPRA block header bytes `6..10` (big-endian) | Sequence number of the OPRA transmission block. |
| `block_timestamp_ns` | OPRA block header sec+nsec bytes `11..19` | Block event time in nanoseconds since epoch. |
| `message_index_in_block` | Message loop index within block | Position of message inside the block. |
| `participant` | Message header byte `0` | Participant ID from OPRA message header. |
| `category` | Message header byte `1` | OPRA message category (v1 decodes `q` and `k`). |
| `type_code` | Message header byte `2` | Message type code from OPRA header. |
| `indicator` | Message header byte `3` | Message indicator from OPRA header. |
| `symbol_root` | Body bytes (`q`: `0..4`, `k`: `0..5`) | Root option symbol string. |
| `osi_symbol` | Derived from root + exp block + strike | Normalized OSI-like symbol string. |
| `bid`, `ask` | Body numeric fields with OPRA denominator rules | Decoded quote prices. |
| `bid_size`, `ask_size` | Body size fields | Quote sizes. |
| `price`, `size`, `side`, `action` | Reserved nullable fields in v1 | Placeholders for true trade-print decoding. |
| `flags` | Nullable in current `q`/`k` parser | Reserved for condition/flags when mapped for a message family. |

How it is decoded:
1. Ethernet/VLAN/IPv4/UDP headers are stripped to isolate UDP payload (`decode_trades_with_parallelism` -> `decode_trade_rows_from_frame` -> `extract_udp_payload`).
2. OPRA block header is parsed (`block_size`, `messages_in_block`, sequence, timestamp) (`parse_block_header`).
3. Block body is split into fixed-size messages for that block (`decode_trade_rows_from_frame` message slicing loop).
4. OPRA 12-byte message header is parsed per message (`parse_message_header`).
5. For categories `q` and `k`, quote fields are decoded and written to `*_trades.parquet` (`parse_short_quote_row` / `parse_long_quote_row` -> `write_trades_parquet`).

#### Concrete decode example (single packet -> one parquet row)

Example OPRA UDP payload prefix (hex, from a parser test fixture):

```text
06 00 6c 4f 20 00 15 30 b0 03 03 64 e4 c6 67 3b 97 bf 00 1e ...
```

1. Strip transport wrappers:
`extract_udp_payload()` walks the frame as:
Ethernet (14 bytes) -> optional VLAN (+4) -> IPv4 (`ihl * 4`) -> UDP (8 bytes) -> payload slice.
Only frames with IPv4 + UDP are kept.
Function path: `decode_trades_with_parallelism` -> `decode_trade_rows_from_frame` -> `extract_udp_payload`.

2. Parse OPRA block header (first 21 bytes of UDP payload):
- `block_size` = bytes `[1..3]` = `0x006c` = `108`
- feed indicator = byte `[3]` = `0x4f` = `'O'`
- `messages_in_block` = byte `[10]` = `0x03` = `3`
- `block_sequence` = bytes `[6..10]` = `0x1530b003` = `355512323`
- timestamp = sec bytes `[11..15]` + nsec bytes `[15..19]`
Function path: `parse_block_header`.

3. Parse block body:
- Body starts at byte `21`
- Body ends at byte `block_size` (`108`)
- Body length is `108 - 21 = 87`
- With `messages_in_block = 3`, each message is `87 / 3 = 29` bytes
Function path: `decode_trade_rows_from_frame` (computes `msg_len`, then slices each message).

4. Parse OPRA message header (first 12 bytes of each message):
- byte `0`: `participant`
- byte `1`: `category`
- byte `2`: `type_code`
- byte `3`: `indicator`
- bytes `4..12`: transaction/reference fields (kept for routing, not all emitted yet)
Function path: `parse_message_header`.

5. Parse category-specific body (`q` and `k` currently):
- If `category == 'q'` (29-byte message): parse short quote body fields
  (`symbol_root`, expiration block, strike, bid/ask, sizes)
- If `category == 'k'` (43-byte message): parse long quote body fields
- Build `osi_symbol` from root + expiration + call/put + strike
- Emit one parquet row with block/message metadata plus decoded quote columns
Function path: `parse_short_quote_row` / `parse_long_quote_row` -> return `DecodedTradeRow` -> `arrow_sink::write_trades_parquet`.

Note: current `*_trades.parquet` is quote-focused (`q`/`k`) research output. It is intentionally not full OPRA trade-print coverage yet.

---

## PCAP Structure Overview
```bash
# High level structure
PCAP Packet Record
└── Ethernet Frame
      ├── Ethernet Header (eth.*)
      ├── VLAN Header (vlan.*)
      └── IPv4 Packet (ip.*)
            ├── IPv4 Header
            └── UDP Datagram (udp.*)
                  ├── UDP Header
                  └── UDP Payload (udp.payload / data.data)
                        └── OPRA Transmission Block
                              └── OPRA Messages

# With Depth
PCAP FILE
├── Global Header
└── PCAP Packet Record(s)
      ├── Timestamp
      ├── Captured Length
      ├── Original Length
      └── Raw Ethernet Frame  <── actual network data starts here
            ├── Ethernet Header (L2, 14 bytes)
            │     ├── Destination MAC
            │     ├── Source MAC
            │     └── EtherType
            │           ├── 0x0800 → IPv4 directly
            │           └── 0x8100 / 0x88a8 → VLAN tag present
            │
            ├── [Optional] VLAN Header (4 bytes, if EtherType = 0x8100 / 0x88a8)
            │     ├── Priority / DEI
            │     ├── VLAN ID
            │     └── Inner EtherType = 0x0800 (IPv4)
            │
            └── Ethernet Payload (after optional VLAN)
                  ├── IPv4 Header (L3, 20–60 bytes)
                  │     ├── Version + Header Length
                  │     ├── Total Packet Length
                  │     ├── Protocol = 17 (UDP)
                  │     ├── Source IP
                  │     └── Destination IP (OPRA multicast group)
                  │
                  └── IPv4 Payload
                        ├── UDP Header (L4, 8 bytes)
                        │     ├── Source Port
                        │     ├── Destination Port
                        │     ├── UDP Length
                        │     └── Checksum
                        │
                        └── UDP Payload
                              ├── OPRA Transmission Block
                              │     ├── Block Header (21 bytes)
                              │     │     ├── Block Size
                              │     │     ├── Data Feed Indicator ('O')
                              │     │     ├── Retransmission Indicator
                              │     │     ├── Session Indicator
                              │     │     ├── Block Sequence Number
                              │     │     ├── Messages In Block
                              │     │     ├── Timestamp (sec + ns)
                              │     │     └── Checksum
                              │     │
                              │     └── Block Data
                              │           ├── Message #1
                              │           │     ├── 12-byte Message Header
                              │           │     └── Message Body
                              │           ├── Message #2
                              │           │     ├── Header
                              │           │     └── Body
                              │           └── ...
                              │
                              └── (Optional pad byte if block length is odd)
```

---

## 🐳 Global Docker Network
Make all services share one network:
```bash

# This is already set inside of airflow-docker/docker-compose.yaml which will
# create the network automatically if it doesn't exist
networks:
  default:
    # external: true # Uncomment if you already created this network with docker network create lakehouse, if not, it will be created automatically when you run docker-compose up
    name: lakehouse

# Already inside of trino/docker-compose.yaml, superset/docker-compose-non-dev.yaml
networks:
  default:
    external: true # will connect to lakehouse network assuming you already stood up airflow-docker which creates it
    name: lakehouse
```

---

## 🪶 Apache Airflow
**Port:** `8080`  
**Username:** `airflow`  
**Password:** `airflow`

```bash
cd airflow-docker
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.1.0/docker-compose.yaml'
docker compose up airflow-init -d
docker compose up -d
docker ps  # Should show multiple apache/airflow:3.1.0 containers
```

---

## ☁️ MinIO (S3-Compatible Storage)
**Port:** `9000` (API), `9001` (Console)  
**Credentials:**  
```
Username: minioadmin
Password: minioadmin
```

```bash
cd trino
docker compose up -d
docker ps
```

---

## ⚙️ Trino 477
**Port:** `8081`  
**Login:** Any username; no password required for default setup.

**Example Superset Connection Strings**
```bash
trino://trino@trino-trino-1:8081/iceberg
# Format:
# trino://{username}:{password}@{hostname}:{port}/{catalog}
```

---

## 📊 Apache Superset
**Port:** `8088`  
**Username:** `admin`  
**Password:** `admin`

```bash
cd superset
git clone https://github.com/apache/superset.git
# check if you already have sqlalchemy-trino in your local requirements
grep -n "sqlalchemy-trino" ./docker/requirements-local.txt

# if not then run this command to add it
# echo "sqlalchemy-trino" >> ./docker/requirements-local.txt

docker compose -f docker-compose-non-dev.yml up -d
```

**Add Trino Connection:**
1. In Superset, Settings -> Database Connections -> click **+ Database**.  
2. Set **Name:** `Trino`  
3. Set **URI:** `trino://trino@trino:8081/iceberg`
4. In **Advanced**, ensure the following boxes are unchecked:
   - `CREATE TABLE AS`
   - `CREATE VIEW AS`
   - `ALLOW DDL`
   - `ALLOW DML`

---

## 🦀 Rust Ingest Rayon vs Single-Threaded Performance

Findings: The single-threaded run is faster than Rayon for smaller PCAP workloads. For example on 10k packet PCAPs, single-threaded completes in ~119.75us while Rayon with 12 threads takes ~402.75us.
However, as the PCAP workload increased in file sizes closer to 10 million, Rayon performed that in 180 ms, whereas the single-threaded performed it in 211 ms.
It seems that Rayon is the winner as PCAP file sizes grow larger.

```bash
# get a workable sized PCAP to test on
tcpdump -r pcap_samples/ny4-opra-new-a-20230822T143000.pcap -c 1000000 -w pcap_samples/ny4-small-1m.pcap

# run the benchmark
cd rust-ingest

# run all 3 files; 10k, 1m, and 10m pcap samples
cargo bench --bench decode_pcap -- --noplot --sample-size 100

# single-file override
OPRA_BENCH_PCAP=pcap_samples/ny4-small-10k.pcap cargo bench --bench decode_pcap -- --noplot --sample-size 100 --measurement-time 2
```



---

## 🦀 Rust Ingest (OPRA PCAP → Parquet → MinIO)

```bash
cd rust-ingest
cargo build --release # release build for better performance
cd pcap_samples
unzstd ny4-opra-new-a-20230822T143000.pcap.zst # unzip the zst file to get the pcap
cd ..

# most simple dev on 1m pcap
cargo run --release -- --pcap ./pcap_samples/ny4-small-1m.pcap --bucket s3://market/bronze/opra_pcap/ --dry-run


# Dry-run with cargo
cargo run --release -- --pcap ./pcap_samples/ny4-opra-new-a-20230822T143000.pcap   --bucket s3://market/bronze/opra_pcap/   --minio-endpoint http://127.0.0.1:9000   --access-key minioadmin   --secret-key minioadmin   --parallel 4   --row-group-bytes 134217728  --dry-run
>>
2026-02-25T03:12:31.862594Z  INFO opra_pcap_replayer: decoded 79920468 packets, 0 messages (skeleton)
2026-02-25T03:12:31.862617Z  INFO opra_pcap_replayer: dry run complete

# Real run with cargo
cargo run --release -- --pcap ./pcap_samples/ny4-opra-new-a-20230822T143000.pcap   --bucket s3://market/bronze/opra_pcap/   --minio-endpoint http://127.0.0.1:9000   --access-key minioadmin   --secret-key minioadmin   --parallel 4   --row-group-bytes 134217728

# Run dry-run directly with the compiled binary (after cargo build --release)
target/release/opra-pcap-replayer   --pcap ./pcap_samples/ny4-opra-new-a-20230822T143000.pcap   --bucket s3://market/bronze/opra_pcap/   --minio-endpoint http://127.0.0.1:9000   --access-key minioadmin   --secret-key minioadmin   --parallel 4   --row-group-bytes 134217728   --dry-run

# Run real directly with the compiled binary (after cargo build --release)
target/release/opra-pcap-replayer   --pcap ./pcap_samples/ny4-opra-new-a-20230822T143000.pcap   --bucket s3://market/bronze/opra_pcap/   --minio-endpoint http://127.0.0.1:9000   --access-key minioadmin   --secret-key minioadmin   --parallel 4   --row-group-bytes 134217728
```

Real Run Expected output:
```
INFO opra_pcap_replayer: decoded 0 packets, 0 messages (skeleton)
INFO opra_pcap_replayer: wrote "./demo_bronze.parquet"
INFO opra_pcap_replayer: uploaded to s3://market/bronze/opra_pcap/demo_bronze.parquet
```

---

## 🧑‍🔬 Creating Iceberg Schemas & Tables inside Superset
### Note: Trino can also be used directly for this, but doing it through Superset allows you to verify the connection and permissions from the UI. It also assumes you've already ingested data into MinIO using the Rust OPRA PCAP replayer section above and that you've connected Superset to Trino as described in the Superset section.

```sql
-- Create bronze schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS iceberg.bronze;

-- Drop old table if you want a clean rebuild
DROP TABLE IF EXISTS iceberg.bronze.demo_bronze;

-- Create bronze table from the external hive staging table
CREATE TABLE iceberg.bronze.demo_bronze
WITH (
  format = 'PARQUET',
  location = 's3://warehouse/bronze/demo_bronze/'
) AS
SELECT *
FROM hive.stage.demo_bronze_ext;
```


---

## 🔧 Debugging & Maintenance

```bash
# View containers attached to network
docker network inspect lakehouse --format '{{json .Containers}}' | jq .

# List active containers
docker ps

# Test Trino Schemas present
docker exec -it trino-trino-1 trino   --server http://localhost:8081   --execute "SHOW SCHEMAS FROM iceberg;"

# Test Trino table creation
docker exec -it trino-trino-1 trino   --server http://localhost:8081   --execute "CREATE TABLE iceberg.bronze.demo_bronze (
      symbol VARCHAR,
      msg_count INTEGER
    )
    WITH (format='PARQUET', location='s3://warehouse/bronze/demo_bronze/');"
```

---

## 🚨 Common Errors & References

| **Issue** | **Description / Fix** |
|------------|-----------------------|
| `UnsupportedFileSystem s3:` | Related GitHub: [Trino discussion #21372](https://github.com/trinodb/trino/discussions/21372) — Kevin added comment there. Japanese blog: [Bedrock](https://blog.bedrock.day/09e466d8ce0ff1fa81ef) |
| **Superset DB Connection Failure** | Follow [Trino.io Episode #12](https://trino.io/episodes/12.html) under *Demo: Superset querying Trino*. |
| **Airflow in Docker** | Reference official [Airflow Compose guide](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html). |

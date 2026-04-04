# 🦀 Rust OPRA PCAP Ingestion & Decoding Pipeline
### 🧊 Trino + Iceberg + Postgres + MinIO + Airflow + Superset

Rust-based OPRA PCAP ingestion.
A full local **lakehouse stack** for data ingestion, query federation, and orchestration—integrating Trino, Apache Iceberg, MinIO (S3-compatible object store), Postgres (metadata + Airflow DB), and Apache Airflow.

---

```bash
# run the rust ingest binary on a sample pcap
cd rust-ingest
cargo run --release -- --pcap ./pcap_samples/ny4-small-10k.pcap --decode-trades
>>
  # Output 2 schemas:
  1. wrote parsed parquet to "./pcap_samples/ny4-small-10k_parsed.parquet" with 10000 rows
  2. wrote "./pcap_samples/ny4-small-10k_trades.parquet" with 14440 decoded trade rows

  parsed schema (`*_parsed.parquet`):
  - packet_index: UInt64 NOT NULL
  - udp_payload_len: UInt64 NOT NULL
  - block_size: UInt64 NOT NULL
  - messages_in_block: UInt64 NOT NULL

  trades schema (`*_trades.parquet`):
  - packet_index: UInt64 NOT NULL
  - block_sequence: UInt64 NOT NULL
  - block_timestamp_ns: UInt64 NOT NULL
  - block_timestamp_utc: Utf8 NOT NULL
  - message_index_in_block: UInt64 NOT NULL
  - participant: Utf8 NOT NULL
  - category: Utf8 NOT NULL
  - type_code: Utf8 NOT NULL
  - indicator: Utf8 NOT NULL
  - symbol_root: Utf8 NULL
  - osi_symbol: Utf8 NULL
  - bid: Float64 NULL
  - ask: Float64 NULL
  - bid_size: UInt64 NULL
  - ask_size: UInt64 NULL
  - price: Float64 NULL
  - size: UInt64 NULL
  - side: Utf8 NULL
  - action: Utf8 NULL
  - flags: UInt64 NULL
```


---

## Project structure
```bash
tree -L 4 -I 'node_modules|__pycache__|logs|plugins|superset|debug|release' -P '*.yaml|*.properties|*.yml|*.rs|*.xml|*.pcap'
>>
├── airflow-docker
│   ├── config
│   ├── dags
│   └── docker-compose.yaml         # sets up airflow
├── superset
│   └── docker-compose-non-dev.yaml # sets up superset
├── research
│   └── environment.yml
├── rust-ingest                     # actual PCAP ingestion & parsing
│   ├── pcap_samples
│   │   └── ny4-opra-new-a-20230822T143000.pcap
│   ├── src
│   │   ├── arrow_sink.rs
│   │   ├── main.rs
│   │   ├── opra_decoder.rs
│   │   └── telemetry.rs
│   └── target
└── trino
    ├── docker-compose.yaml         # sets up trino, minio, hive-metastore, hive-postgres db
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

- OPRA Pillar Output Specification (Feb 20, 2026): <https://cdn.opraplan.com/documents/OPRA_Pillar_Output_Specification.pdf>
- OPRA Pillar Input Specification (Feb 20, 2026): <https://cdn.opraplan.com/documents/OPRA_Pillar_Input_Specification.pdf>

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

#### OPRA PCAP Decoding Example

When decoding an OPRA PCAP, it helps to think in layers rather than assuming one packet equals one quote. An Ethernet frame contains an IP packet, the IP packet contains a UDP datagram, the UDP payload contains an OPRA block, and that OPRA block contains one or more OPRA messages. The OPRA block starts with a single block header that applies to the whole block. After that, each individual OPRA message has its own message header followed by its own body. So yes, there can be multiple message headers inside one block, because a single block may carry multiple OPRA messages.

The block header is the outer framing for the OPRA payload. It tells you things like ordering, timing, and how many messages you should expect to parse from this block. The message header is different: it applies only to one message and tells you what that message is, such as a long quote (`k`), short quote (`q`), trade, and so on. In practice, your parser reads the block header once, then loops over the message count, reading one message header and one message body at a time.

A useful mental model is:

```text
UDP payload
└── OPRA block
    ├── block header
    ├── message header #1
    ├── message body #1
    ├── message header #2
    ├── message body #2
    └── ...
```

Here is a synthetic but realistic raw byte example for a single OPRA block carrying one `k` quote message:

```text
00 2B 01 00 00 00 00 10 01 00 00 00 5F 37 59 DF 00 00 00 00 00 # 21 byte OPRA block header
43 6B 20 41 00 BC 61 4E 00 00 00 00                            # 12 byte OPRA message header
53 50 59 20 20 00 57 11 17 42 00 07 6A 50 42                   # OPRA `k` message body
00 00 00 9D 00 00 00 19 00 00 00 A0 00 00 00 12                # OPRA `k` message body continuation
```

The first 21 bytes are the OPRA block header:

```text
00 2B                    -> block size
01                       -> feed indicator
00                       -> retransmission indicator
00                       -> session indicator
00 00 00 10              -> block sequence number
01                       -> message count
00 00 00 5F 37 59 DF     -> block timestamp
00 00 00 00              -> checksum
00                       -> reserved
```

That decodes conceptually to something like:

```json
{
  "block_size": 43,
  "block_sequence": 16,
  "message_count": 1,
  "timestamp_raw": 1597463007
}
```

Immediately after the block header comes the 12-byte OPRA message header:

```text
43                       -> participant id = 'C'
6B                       -> message category = 'k'
20                       -> message type = ' '
41                       -> message indicator = 'A'
00 BC 61 4E              -> transaction id = 12345678
00 00 00 00              -> participant reference number = 0
```

That decodes to:

```json
{
  "participant_id": "C",
  "message_category": "k",
  "message_type": " ",
  "message_indicator": "A",
  "transaction_id": 12345678,
  "participant_reference_number": 0
}
```

The remaining bytes are the `k` message body:

```text
53 50 59 20 20           -> security symbol = "SPY  "
00                       -> reserved
57                       -> expiration month code = 'W'
11                       -> expiration day = 17
17                       -> expiration year offset = 23
42                       -> strike denominator code = 'B'
00 07 6A 50              -> strike price raw = 486000
42                       -> premium price denominator code = 'B'
00 00 00 9D              -> bid price raw = 157
00 00 00 19              -> bid size raw = 25
00 00 00 A0              -> ask price raw = 160
00 00 00 12              -> ask size raw = 18
```

Now decode the business meaning of those fields. The symbol is `SPY`. The month code `W` means a November put. The day is `17`. The year byte is stored as an offset from 2000, so `23` means `2023`. The strike denominator code `B` means divide the raw strike integer by 100, so `486000` becomes `4860.00`. The premium denominator code `B` also means divide by 100, so the bid raw value `157` becomes `1.57` and the ask raw value `160` becomes `1.60`. The sizes remain integer contract sizes.

So the fully decoded quote becomes:

```json
{
  "participant_id": "C",
  "message_category": "k",
  "message_type": " ",
  "message_indicator": "A",
  "transaction_id": 12345678,
  "participant_reference_number": 0,
  "security_symbol": "SPY",
  "expiration_date": "2023-11-17",
  "option_side": "put",
  "strike_price": 4860.00,
  "bid_price": 1.57,
  "bid_size": 25,
  "ask_price": 1.60,
  "ask_size": 18
}
```

A Rust-oriented mental model for this same message is:

```rust
struct OpraBlockHeader {
    block_size: u16,
    feed_indicator: u8,
    retransmission_indicator: u8,
    session_indicator: u8,
    block_sequence_number: u32,
    message_count: u8,
    block_timestamp: u64,
    checksum: u32,
    reserved: u8,
}

struct OpraMessageHeader {
    participant_id: u8,
    message_category: u8,
    message_type: u8,
    message_indicator: u8,
    transaction_id: u32,
    participant_reference_number: u32,
}

struct KQuoteBody {
    security_symbol: [u8; 5],
    reserved: u8,
    expiration_month_code: u8,
    expiration_day: u8,
    expiration_year_offset: u8,
    strike_price_denominator_code: u8,
    strike_price_raw: u32,
    premium_price_denominator_code: u8,
    bid_price_raw: u32,
    bid_size_raw: u32,
    ask_price_raw: u32,
    ask_size_raw: u32,
}
```

A simple parsing flow in Rust looks like this:

```rust
fn parse_opra_block(input: &[u8]) {
    let (rest, block_header) = parse_block_header(input).unwrap();

    let mut cursor = rest;
    for _ in 0..block_header.message_count {
        let (rest_after_header, msg_header) = parse_message_header(cursor).unwrap();

        cursor = match msg_header.message_category {
            b'k' => {
                let (rest_after_body, body) = parse_k_quote(rest_after_header).unwrap();
                println!("{msg_header:?} {body:?}");
                rest_after_body
            }
            b'q' => {
                let (rest_after_body, body) = parse_q_quote(rest_after_header).unwrap();
                println!("{msg_header:?} {body:?}");
                rest_after_body
            }
            _ => {
                panic!("unsupported message category: {}", msg_header.message_category as char);
            }
        };
    }
}
```

The key takeaway is that the block header is the outer container for a batch of messages, the message header determines how to interpret each individual message, and the actual market data lives in the message body.

#### What Our Code Does Today

1. Strip Ethernet/VLAN/IPv4/UDP and isolate UDP payload.
2. Parse OPRA block header.
3. Split block data into fixed-size messages for that block.
4. Parse 12-byte message header.
5. Decode `q` and `k` quote-family rows into `DecodedTradeRow`.

Current decoder architecture in code:

- Frame loop: `decode_trades_with_parallelism` -> `decode_trade_rows_from_frame`
- Message keying: build `MessageDispatchKey { category, type_code, indicator }`
- Spec routing: `classify_message_family(...)` -> `decode_message_by_spec(...)`
- Family parser (implemented): `parse_short_quote_row` (`q`), `parse_long_quote_row` (`k`), `parse_equity_index_last_sale_row` (`a`)
- Row sink: `arrow_sink::write_trades_parquet(...)`

This gives us a production-style extension point: add a new family parser and wire it in
`decode_message_by_spec` without changing the rest of the pipeline.

#### What “Production” Still Requires

- Full dispatch by `category + type + indicator` across OPRA message families (not just `q/k`)
- Exact appendage handling (none/single/double) where spec requires it
- Broader trade-print family parsing beyond initial `a` implementation to populate all true trade fields (`price/size/conditions/...`)
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

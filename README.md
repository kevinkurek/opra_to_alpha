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

## 🦀 Rust Ingest (OPRA PCAP → Parquet → MinIO)

```bash
cd rust-ingest
cargo build --release # release build for better performance
cd pcap_samples
unzstd ny4-opra-new-a-20230822T143000.pcap
cd ..

# Dry-run with cargo
cargo run --release -- --pcap ./pcap_samples/ny4-opra-new-a-20230822T143000.pcap   --bucket s3://market/bronze/opra_pcap/   --minio-endpoint http://127.0.0.1:9000   --access-key minioadmin   --secret-key minioadmin   --parallel 4   --row-group-bytes 134217728  --dry-run

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

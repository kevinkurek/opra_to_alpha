![](datalake.jpg)

# Trino with Iceberg, Postgres, MinIO, and Airflow

### Airflow Port 8080
Quickstart docker compose link: https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html
```bash
cd airflow-docker
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.1.0/docker-compose.yaml'
docker compose up airflow-init -d
docker compose up -d
docker ps
>>
Multiple apache/airflow:3.1.0 containers
```

### Trino Port 8081 & MinIO Port 9000
```bash
cd trino
docker compose up -d
docker ps

# connection strings that works in Superset when iceberg.jdbc-catalog.catalog-schema=iceberg is commented out?
# trino://trino@host.docker.internal:8081
# trino://trino@trino-trino-1:8081/iceberg
# trino://trino@trino-trino-1:8081/jdbc

# outline
# trino://{username}:{password}@{hostname}:{port}/{catalog}
```

### Superset Port 8088

* Getting superset to setup locally on docker with trino wasn't simple.
- Had to use these directions: https://trino.io/episodes/12.html under `Demo: Superset querying Trino to create visualization dashboard` to get the database set up.

```bash
cd superset
git clone https://github.com/apache/superset.git
echo "sqlalchemy-trino" >> ./docker/requirements-local.txt
docker compose -f docker-compose-non-dev.yml up -d
```
Inside of Trino
Click the +Database button.
Set Name to “Trino” and URI to `trino://trino@host.docker.internal:8081` and click Add.
- make sure you select in advanced the non-checked boxes of CREATE TABLE AS, CREATE VIEW AS, ALLOW DDL and DML


### Rust Ingest Run
```bash
cd rust-ingest
cargo build --release
cd pcap_sample unzstd ny4-opra-new-a-20230822T143000.pcap
cd ..
target/release/opra-pcap-replayer --pcap ./pcap_samples/ny4-opra-new-a-20230822T143000.pcap  --bucket s3://market/bronze/opra_pcap/  --minio-endpoint http://127.0.0.1:9000  --access-key minioadmin --secret-key minioadmin  --parallel 4 --row-group-bytes 134217728
>>
2025-10-02T00:28:42.973075Z  INFO opra_pcap_replayer: decoded 0 packets, 0 messages (skeleton)
2025-10-02T00:28:42.979472Z  INFO opra_pcap_replayer: wrote "./demo_bronze.parquet"
2025-10-02T00:28:43.179312Z  INFO opra_pcap_replayer: uploaded to s3://market/bronze/opra_pcap/demo_bronze.parquet
```

### Wireshark
```bash
brew install wireshark   # gives tshark & capinfos
capinfos ny4-opra-new-a-20230822T143000.pcap
tshark -r ny4-opra-new-a-20230822T143000.pcap -c 20
```

### Unified Docker Compose
Either 
* Make a unified docker compose OR
* make a shared external network
```bash
docker network create lakehouse

# add inside each docker-compose.yaml
networks:
  default:
    external: true
    name: lakehouse
```


docker exec -it airflow-docker-postgres-1 psql -U airflow -d airflow -c "CREATE DATABASE iceberg;"
docker exec -it airflow-docker-postgres-1 psql -U airflow -d airflow -c "CREATE USER etl WITH PASSWORD 'demopass';"
docker exec -it airflow-docker-postgres-1 psql -U airflow -d airflow -c "GRANT ALL PRIVILEGES ON DATABASE iceberg TO etl;"


### Useful docker commands while debugging
```bash

# check which containers are on network
docker network inspect lakehouse --format '{{json .Containers}}' | jq .

# check which containers running
docker ps
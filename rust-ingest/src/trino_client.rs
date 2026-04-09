use anyhow::{anyhow, Context, Result};
use reqwest::Client;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct TrinoError {
    message: String,
}

#[derive(Debug, Deserialize)]
struct TrinoResponse {
    #[serde(rename = "nextUri")]
    next_uri: Option<String>,
    error: Option<TrinoError>,
}

async fn execute_trino_sql(
    client: &Client,
    endpoint: &str,
    user: &str,
    catalog: &str,
    schema: &str,
    sql: &str,
) -> Result<()> {
    let statement_url = format!("{}/v1/statement", endpoint.trim_end_matches('/'));
    let mut response: TrinoResponse = client
        .post(statement_url)
        .header("X-Trino-User", user)
        .header("X-Trino-Catalog", catalog)
        .header("X-Trino-Schema", schema)
        .body(sql.to_string())
        .send()
        .await
        .with_context(|| format!("failed to send Trino SQL: {sql}"))?
        .error_for_status()
        .context("trino returned non-success status for statement submission")?
        .json()
        .await
        .context("failed to deserialize initial Trino response")?;

    if let Some(error) = response.error.take() {
        return Err(anyhow!("trino statement failed: {}", error.message));
    }

    while let Some(next_uri) = response.next_uri.take() {
        response = client
            .get(next_uri)
            .header("X-Trino-User", user)
            .send()
            .await
            .context("failed polling Trino query status")?
            .error_for_status()
            .context("trino returned non-success status while polling query")?
            .json()
            .await
            .context("failed to deserialize polled Trino response")?;

        if let Some(error) = response.error.take() {
            return Err(anyhow!("trino statement failed: {}", error.message));
        }
    }

    Ok(())
}

pub async fn ensure_trino_schema_and_trades_table() -> Result<()> {
    let endpoint =
        std::env::var("OPRA_TRINO_ENDPOINT").unwrap_or_else(|_| String::from("http://localhost:8081"));
    let user = std::env::var("OPRA_TRINO_USER").unwrap_or_else(|_| String::from("trino"));
    let catalog = std::env::var("OPRA_TRINO_CATALOG").unwrap_or_else(|_| String::from("hive"));
    let schema = std::env::var("OPRA_TRINO_SCHEMA").unwrap_or_else(|_| String::from("bronze"));
    let table = std::env::var("OPRA_TRINO_TRADES_TABLE")
        .unwrap_or_else(|_| String::from("opra_trades_ext"));
    let hive_catalog =
        std::env::var("OPRA_TRINO_HIVE_CATALOG").unwrap_or_else(|_| String::from("hive"));
    let hive_table = std::env::var("OPRA_TRINO_HIVE_TRADES_TABLE")
        .unwrap_or_else(|_| String::from("opra_trades_ext"));

    let bucket = std::env::var("OPRA_MINIO_BUCKET").unwrap_or_else(|_| String::from("market"));
    let prefix = std::env::var("OPRA_MINIO_PREFIX").unwrap_or_else(|_| String::from("bronze"));
    let external_location = std::env::var("OPRA_TRINO_TRADES_LOCATION")
        .unwrap_or_else(|_| format!("s3://{bucket}/{prefix}/opra_trades/"));

    let client = Client::new();

    // Always ensure a Hive external table over parquet files in MinIO.
    // This is the source relation we can query directly and also project into other catalogs.
    let create_hive_schema_sql = format!("CREATE SCHEMA IF NOT EXISTS {hive_catalog}.{schema}");
    execute_trino_sql(
        &client,
        &endpoint,
        &user,
        &hive_catalog,
        &schema,
        &create_hive_schema_sql,
    )
    .await?;

    let create_hive_table_sql = format!(
        "CREATE TABLE IF NOT EXISTS {hive_catalog}.{schema}.{hive_table} (
            packet_index BIGINT,
            block_sequence BIGINT,
            block_timestamp_ns BIGINT,
            block_timestamp_utc VARCHAR,
            message_index_in_block BIGINT,
            participant VARCHAR,
            category VARCHAR,
            type_code VARCHAR,
            indicator VARCHAR,
            symbol_root VARCHAR,
            osi_symbol VARCHAR,
            bid BIGINT,
            ask BIGINT,
            bid_size BIGINT,
            ask_size BIGINT,
            price BIGINT,
            size BIGINT,
            side VARCHAR,
            action VARCHAR,
            flags BIGINT
        )
        WITH (
            format = 'PARQUET',
            external_location = '{external_location}'
        )"
    );
    execute_trino_sql(
        &client,
        &endpoint,
        &user,
        &hive_catalog,
        &schema,
        &create_hive_table_sql,
    )
    .await?;

    // If target catalog is hive, we're done.
    if catalog.eq_ignore_ascii_case("hive") {
        return Ok(());
    }

    // For non-hive catalogs (e.g. iceberg), expose the relation via a view.
    let create_target_schema_sql = format!("CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}");
    execute_trino_sql(
        &client,
        &endpoint,
        &user,
        &catalog,
        &schema,
        &create_target_schema_sql,
    )
    .await?;

    let create_view_sql = format!(
        "CREATE OR REPLACE VIEW {catalog}.{schema}.{table} AS
         SELECT * FROM {hive_catalog}.{schema}.{hive_table}"
    );
    execute_trino_sql(
        &client,
        &endpoint,
        &user,
        &catalog,
        &schema,
        &create_view_sql,
    )
    .await?;

    Ok(())
}

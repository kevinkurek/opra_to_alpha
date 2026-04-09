use anyhow::Result;
use opra_pcap_replayer::databento_client;
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

// Run
// cargo run --bin databento_pull

#[tokio::main]
async fn main() -> Result<()> {
    // loads .env if present
    let _ = dotenvy::dotenv();

    let symbol = "SPY   230822P00436000";
    let start = "2023-08-22T14:30:00.000000Z";
    let end = "2023-08-22T14:31:00.000000Z";

    // parse start and end to unix_timestamp
    let start_dt = OffsetDateTime::parse(start, &Rfc3339).unwrap();
    let end_dt = OffsetDateTime::parse(end, &Rfc3339).unwrap();

    // fetch trades
    let result = databento_client::fetch_db_trades(symbol, start_dt, end_dt).await?;
    println!("{:#?}", result.iter().take(5).collect::<Vec<_>>());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fetch_db_trades_api_connects_and_returns_rows() {
        // loads .env if present
        let _ = dotenvy::dotenv();

        let symbol = "SPY   230822P00436000";
        let start = "2023-08-22T14:30:00.000000Z";
        let end = "2023-08-22T14:31:00.000000Z";

        // parse start and end to unix_timestamp
        let start_dt = OffsetDateTime::parse(start, &Rfc3339).unwrap();
        let end_dt = OffsetDateTime::parse(end, &Rfc3339).unwrap();

        // fetch trades
        let result = databento_client::fetch_db_trades(symbol, start_dt, end_dt).await;

        // Assert
        assert!(result.is_ok(), "expected Ok(), but {result:?}");
        let trades = result.unwrap();
        assert!(trades.len() > 0);
        assert!(trades.len() <= 1000);
    }
}

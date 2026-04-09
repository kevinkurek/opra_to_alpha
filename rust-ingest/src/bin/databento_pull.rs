use std::num::NonZeroU64;
use anyhow::Result;

use databento::{
    dbn::{Schema, SType, TradeMsg},
    historical::timeseries::GetRangeParams,
    HistoricalClient,
};
use time::{format_description::well_known::Rfc3339, OffsetDateTime};

#[tokio::main]
async fn main () -> Result<()> {
    
    // loads .env if present
    let _ = dotenvy::dotenv(); 

    let symbol = "SPY   230822P00436000";
    let start = "2023-08-22T14:30:00.000000Z";
    let end = "2023-08-22T14:31:00.000000Z";

    // parse start and end to unix_timestamp
    let start_dt = OffsetDateTime::parse(start, &Rfc3339).unwrap();
    let end_dt = OffsetDateTime::parse(end, &Rfc3339).unwrap();

    // fetch trades
    let result = fetch_db_trades(symbol, start_dt, end_dt).await?;
    println!("{:#?}", result.iter().take(5).collect::<Vec<_>>());
    Ok(())
}

async fn fetch_db_trades(
    symbol: &str,
    start: OffsetDateTime,
    end: OffsetDateTime,
) -> anyhow::Result<Vec<TradeMsg>> {

    // uses DATABENTO_API_KEY from env, like os.getenv(...) in Python
    let mut client = HistoricalClient::builder().key_from_env()?.build()?;

    let params = GetRangeParams::builder()
        .dataset("OPRA.PILLAR")
        .schema(Schema::Trades)
        .stype_in(SType::RawSymbol)
        .symbols(symbol)
        .date_time_range(start..end)
        .limit(Some(NonZeroU64::new(1000).expect("1000 is non-zero")))
        .build();

    let mut decoder = client.timeseries().get_range(&params).await?;

    let mut db_trades = Vec::new();
    while let Some(trade) = decoder.decode_record::<TradeMsg>().await? {
        db_trades.push(trade.to_owned());
    }

    Ok(db_trades)
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
        let result = fetch_db_trades(symbol, start_dt, end_dt).await;

        // Assert
        assert!(result.is_ok(), "expected Ok(), but {result:?}");
        let trades = result.unwrap();
        assert!(trades.len() > 0);
        assert!(trades.len() <= 1000);
    }
}
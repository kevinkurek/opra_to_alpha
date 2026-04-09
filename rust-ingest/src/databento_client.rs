use std::num::NonZeroU64;

use databento::{
    dbn::{SType, Schema, TradeMsg},
    historical::timeseries::GetRangeParams,
    HistoricalClient,
};
use time::OffsetDateTime;

pub async fn fetch_db_trades(
    symbol: &str,
    start: OffsetDateTime,
    end: OffsetDateTime,
) -> anyhow::Result<Vec<TradeMsg>> {
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

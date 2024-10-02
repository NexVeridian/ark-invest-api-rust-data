use anyhow::{Error, Result};
use polars::prelude::*;

pub fn defualt_df(ticker: &[Option<&str>], company: &[Option<&str>]) -> Result<DataFrame, Error> {
    let target_len = ticker.len() + 1;
    let df = df![
        "date" => vec!["2024-01-01"; target_len],
        "ticker" => [ticker, &[Some("TSLA")]].concat(),
        "cusip" => vec!["TESLA"; target_len],
        "company" => [company, &[Some("TESLA")]].concat(),
        "market_value" => vec![10; target_len],
        "shares" => vec![10; target_len],
        "share_price" => vec![100.00; target_len],
        "weight" => vec![10.00; target_len],
    ]?;
    Ok(df)
}

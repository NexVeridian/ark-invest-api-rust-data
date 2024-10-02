use anyhow::{Error, Result};
use polars::prelude::*;

use crate::{ticker::DataSource, util::df::DF};

pub fn data_source(data_source: DataSource, mut df: DF) -> Result<DF, Error> {
    let df = match data_source {
        DataSource::ArkVenture => df_format_arkvx(df)?,
        DataSource::Ark => df,
        DataSource::Shares21 => df_format_21shares(df)?,
        DataSource::ArkEurope | DataSource::Rize => {
            df = df_format_europe_csv(df)?;
            df = df_format_europe_arkfundsio(df)?;
            df_format_europe(df)?
        }
    };
    Ok(df)
}

pub fn df_format_21shares(df: DF) -> Result<DF, Error> {
    let mut df = df.collect()?;

    if df.get_column_names().contains(&"Weightings") {
        df = df
            .lazy()
            .rename(
                vec![
                    "Date",
                    "StockTicker",
                    "CUSIP",
                    "SecurityName",
                    "Shares",
                    "Price",
                    "MarketValue",
                    "Weightings",
                ],
                vec![
                    "date",
                    "ticker",
                    "cusip",
                    "company",
                    "shares",
                    "share_price",
                    "market_value",
                    "weight",
                ],
            )
            .collect()?;

        _ = df.drop_in_place("Account");
        _ = df.drop_in_place("NetAssets");
        _ = df.drop_in_place("SharesOutstanding");
        _ = df.drop_in_place("CreationUnits");
        _ = df.drop_in_place("MoneyMarketFlag");
    }

    Ok(df.into())
}

pub fn df_format_arkvx(df: DF) -> Result<DF, Error> {
    let mut df = df.collect()?;

    if df.get_column_names().contains(&"CUSIP") {
        df = df
            .lazy()
            .rename(vec!["CUSIP", "weight (%)"], vec!["cusip", "weight"])
            .collect()?;
    }
    if df.get_column_names().contains(&"weight (%)") {
        df = df
            .lazy()
            .rename(vec!["weight (%)"], vec!["weight"])
            .collect()?;
    }

    if !df.get_column_names().contains(&"market_value") {
        df = df
            .lazy()
            .with_columns([
                Series::new("market_value", [None::<i64>]).lit(),
                Series::new("shares", [None::<i64>]).lit(),
                Series::new("share_price", [None::<f64>]).lit(),
            ])
            .collect()?;
    }

    Ok(df.into())
}

pub fn df_format_europe(df: DF) -> Result<DF, Error> {
    let mut df = df.collect()?;

    if df.get_column_names().contains(&"Currency") {
        _ = df.drop_in_place("Currency");

        df = df
            .lazy()
            .rename(
                vec!["name", "ISIN", "Weight"],
                vec!["company", "cusip", "weight"],
            )
            .with_columns([
                Series::new("date", [chrono::Local::now().date_naive()]).lit(),
                Series::new("ticker", [None::<String>]).lit(),
                Series::new("market_value", [None::<i64>]).lit(),
                Series::new("shares", [None::<i64>]).lit(),
                Series::new("share_price", [None::<f64>]).lit(),
            ])
            .collect()?;
    }

    Ok(df.into())
}

pub fn df_format_europe_arkfundsio(df: DF) -> Result<DF, Error> {
    let mut df = df.collect()?;

    if df
        .get_column_names()
        .eq(&["company", "cusip", "date", "fund", "weight", "weight_rank"])
    {
        _ = df.drop_in_place("fund");
        _ = df.drop_in_place("weight_rank");

        df = df
            .lazy()
            .with_columns([
                Series::new("ticker", [None::<String>]).lit(),
                Series::new("market_value", [None::<i64>]).lit(),
                Series::new("shares", [None::<i64>]).lit(),
                Series::new("share_price", [None::<f64>]).lit(),
            ])
            .collect()?;
    }
    Ok(df.into())
}

pub fn df_format_europe_csv(df: DF) -> Result<DF, Error> {
    let mut df = df.collect()?;

    if df.get_column_names().contains(&"_duplicated_0") {
        df = df.slice(2, df.height());

        df = df
            .clone()
            .lazy()
            .rename(df.get_column_names(), ["company", "cusip", "weight"])
            .with_columns([
                Series::new("date", [chrono::Local::now().date_naive()]).lit(),
                Series::new("ticker", [None::<String>]).lit(),
                Series::new("market_value", [None::<i64>]).lit(),
                Series::new("shares", [None::<i64>]).lit(),
                Series::new("share_price", [None::<f64>]).lit(),
            ])
            .collect()?;
    }

    Ok(df.into())
}

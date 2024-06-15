use anyhow::{Error, Result};
use chrono::NaiveDate;
use polars::datatypes::DataType;
use pretty_assertions::assert_eq;
use serial_test::serial;

use ark_invest_api_rust_data::util::ticker::Ticker;
use ark_invest_api_rust_data::util::*;

#[test]
#[serial]
fn get_api_arkk() -> Result<(), Error> {
    let df = Ark::new(
        Source::ApiIncremental,
        Ticker::ARKK,
        Some("data/test".to_owned()),
    )?
    .get_api(NaiveDate::from_ymd_opt(2023, 5, 18), None)?;

    let expected = [
        "company",
        "cusip",
        "date",
        "market_value",
        "share_price",
        "shares",
        "ticker",
        "weight",
        "weight_rank",
    ];
    let actual = df.get_column_names();

    assert!(
        actual == expected || actual == expected[..expected.len() - 1],
        "Column names are wrong"
    );
    Ok(())
}

#[test]
#[serial]
fn get_api_format_arkk() -> Result<(), Error> {
    let dfl = Ark::new(
        Source::ApiIncremental,
        Ticker::ARKK,
        Some("data/test".to_owned()),
    )?
    .get_api(NaiveDate::from_ymd_opt(2023, 5, 18), None)?;
    let df = Ark::df_format(dfl.into())?.collect()?;

    assert_eq!(
        (df.get_column_names(), df.dtypes(), df.shape().1 > 1),
        (
            vec![
                "date",
                "ticker",
                "cusip",
                "company",
                "market_value",
                "shares",
                "share_price",
                "weight",
            ],
            vec![
                DataType::Date,
                DataType::Utf8,
                DataType::Utf8,
                DataType::Utf8,
                DataType::Int64,
                DataType::Int64,
                DataType::Float64,
                DataType::Float64,
            ],
            true
        )
    );
    Ok(())
}

#[test]
#[serial]
fn get_api_format_arkvx() -> Result<(), Error> {
    let dfl = Ark::new(
        Source::ApiIncremental,
        Ticker::ARKVX,
        Some("data/test".to_owned()),
    )?
    .get_api(NaiveDate::from_ymd_opt(2023, 1, 1), None)?;
    let df = Ark::df_format(dfl.into())?.collect()?;

    assert_eq!(
        (df.get_column_names(), df.dtypes(), df.shape().1 > 1),
        (
            vec![
                "date",
                "ticker",
                "cusip",
                "company",
                "market_value",
                "shares",
                "share_price",
                "weight",
            ],
            vec![
                DataType::Date,
                DataType::Utf8,
                DataType::Utf8,
                DataType::Utf8,
                DataType::Int64,
                DataType::Int64,
                DataType::Float64,
                DataType::Float64,
            ],
            true
        )
    );
    Ok(())
}

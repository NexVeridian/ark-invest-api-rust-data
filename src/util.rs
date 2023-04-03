use glob::glob;
use polars::datatypes::DataType;
use polars::lazy::dsl::StrpTimeOptions;
use polars::prelude::*;
use polars::prelude::{DataFrame, UniqueKeepStrategy};
use reqwest::blocking::Client;
use std::error::Error;
use std::fs::File;
use std::io::Cursor;
use std::result::Result;
use strum_macros::EnumIter;

#[derive(strum_macros::Display, EnumIter, Clone, Copy)]
pub enum Ticker {
    ARKVC,
    ARKF,
    ARKG,
    ARKK,
    ARKQ,
    ARKW,
    ARKX,
}

impl Ticker {
    pub fn value(&self) -> &str {
        match *self {
            Ticker::ARKVC => "ARKVC",
            Ticker::ARKF => "FINTECH_INNOVATION",
            Ticker::ARKG => "GENOMIC_REVOLUTION",
            Ticker::ARKK => "INNOVATION",
            Ticker::ARKQ => "AUTONOMOUS_TECH._&_ROBOTICS",
            Ticker::ARKW => "NEXT_GENERATION_INTERNET",
            Ticker::ARKX => "SPACE_EXPLORATION_&_INNOVATION",
        }
    }
}

pub fn merge_csv_to_parquet(folder: Ticker) -> Result<(), Box<dyn Error>> {
    let mut dfs = vec![];

    for x in glob(&format!("data/csv/{}/*", folder.to_string()))?.filter_map(Result::ok) {
        dfs.push(LazyCsvReader::new(x).finish()?);
    }

    let df = concat(dfs, false, true)?;

    write_parquet(folder, df_format(folder, df)?)?;
    Ok(())
}

pub fn update_parquet(ticker: Ticker) -> Result<(), Box<dyn Error>> {
    let update = get_csv(ticker)?;

    let mut df = read_parquet(ticker)?;

    df = concat(vec![df, update], false, true)?.unique_stable(None, UniqueKeepStrategy::First);

    write_parquet(ticker, df.collect()?)?;
    Ok(())
}

pub fn read_parquet(ticker: Ticker) -> Result<LazyFrame, Box<dyn Error>> {
    let df = LazyFrame::scan_parquet(
        format!("data/parquet/{}.parquet", ticker.to_string()),
        ScanArgsParquet::default(),
    )?;
    Ok(df)
}

pub fn write_parquet(ticker: Ticker, mut df: DataFrame) -> Result<(), Box<dyn Error>> {
    ParquetWriter::new(File::create(format!(
        "data/parquet/{}.parquet",
        ticker.to_string()
    ))?)
    .finish(&mut df)?;

    Ok(())
}

pub fn df_format(folder: Ticker, mut dfl: LazyFrame) -> Result<DataFrame, Box<dyn Error>> {
    match folder {
        Ticker::ARKVC => {
            dfl = dfl.rename(vec!["CUSIP", "weight (%)"], vec!["cusip", "weight"]);

            let df = dfl
                .with_columns(vec![
                    col("date").str().strptime(StrpTimeOptions {
                        date_dtype: DataType::Date,
                        fmt: Some("%m/%d/%Y".into()),
                        strict: false,
                        exact: true,
                        cache: false,
                        tz_aware: false,
                        utc: false,
                    }),
                    col("weight")
                        .str()
                        .extract(r"[0-9]*\.[0-9]+", 0)
                        .cast(DataType::Float64),
                ])
                .filter(col("date").is_not_null())
                .collect()?;

            Ok(df)
        }
        _ => {
            let mut df = dfl.collect()?;

            if let Ok(_) = df.rename("market_value_($)", "market_value") {}
            if let Ok(_) = df.rename("weight_(%)", "weight") {}

            if let Ok(x) = df
                .clone()
                .lazy()
                .with_column(col("date").cast(DataType::Date))
                .filter(col("date").is_not_null())
                .collect()
            {
                df = x
            } else if let Ok(x) = df
                .clone()
                .lazy()
                .filter(col("date").is_not_null())
                .collect()
            {
                df = x
            }

            Ok(df)
        }
    }
}

pub fn get_csv(ticker: Ticker) -> Result<LazyFrame, Box<dyn Error>> {
    let data: Vec<u8>;
    let request;
    match ticker {
        Ticker::ARKVC => {
            request = Client::new()
					.get("https://ark-ventures.com/wp-content/uploads/funds-etf-csv/ARK_VENTURE_FUND_HOLDINGS.csv")
        }
        _ => {
            request = Client::new().get(format!(
                "https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_{}_ETF_{}_HOLDINGS.csv",
                ticker.value(),
                ticker.to_string()
            ))
        }
    }
    data = request.send()?.text()?.bytes().collect();

    let df = CsvReader::new(Cursor::new(data))
        .has_header(true)
        .finish()?
        .lazy();

    Ok(df)
}

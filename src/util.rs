use anyhow::{anyhow, Error, Result};
use chrono::{Duration, NaiveDate};
use data_reader::Reader;
use df::{DF, DFS};
use glob::glob;
use polars::datatypes::DataType;
use polars::lazy::dsl::StrptimeOptions;
use polars::prelude::*;
use std::fs::{create_dir_all, File};
use std::path::Path;
use strum_macros::EnumString;

use ticker::{DataSource, Ticker};
pub mod data_reader;
pub mod df;
mod format;
pub mod ticker;

#[derive(Debug, Default, EnumString, Clone, Copy, PartialEq)]
pub enum Source {
    // Reads Parquet file if exists
    Read,
    // From ARK Invest
    Ark,
    // From api.NexVeridian.com
    #[default]
    ApiIncremental,
    // From api.NexVeridian.com, not usually nessisary, use ApiIncremental
    ApiFull,
    // From arkfunds.io/api, avoid using, use ApiIncremental instead
    ArkFundsIoIncremental,
    // From arkfunds.io/api, avoid using, use ApiFull instead
    ArkFundsIoFull,
}

#[derive(Clone)]
pub struct Ark {
    pub df: DF,
    ticker: Ticker,
    path: Option<String>,
}
impl Ark {
    pub fn new(source: Source, ticker: Ticker, path: Option<String>) -> Result<Self, Error> {
        let existing_file = Self::read_parquet(&ticker, path.as_ref()).is_ok();

        let mut ark = Self {
            df: match existing_file {
                true => Self::read_parquet(&ticker, path.as_ref())?,
                false => DF::DataFrame(df!["date" => [""],]?),
            },
            ticker,
            path,
        };

        let update = match (source, existing_file) {
            (Source::Read, false) => {
                panic!("Can not read from file. file is empty, does not exist, or is locked")
            }
            (Source::Read, true) => None,
            (Source::Ark, _) => Some(ark.get_csv_ark()?),
            (Source::ApiIncremental, true) | (Source::ArkFundsIoIncremental, true) => {
                let last_day = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap()
                    + Duration::days(ark.df.clone().collect()?.column("date")?.max().unwrap());
                Some(ark.get_api(Some(last_day), Some(&source))?)
            }
            _ => Some(ark.get_api(None, Some(&source))?),
        };

        if let Some(update) = update {
            if existing_file {
                ark.df = Self::concat_df(vec![
                    Self::df_format(ark.df, None)?,
                    Self::df_format(update.into(), None)?,
                ])?;
            } else {
                ark.df = Self::df_format(update.into(), None)?;
            }
        }

        Ok(ark)
    }

    pub fn collect(self) -> Result<DataFrame, Error> {
        self.df.collect()
    }

    pub fn write_parquet(self) -> Result<Self, Error> {
        // with format df
        let ark = self.format()?;
        Self::write_df_parquet(
            match &ark.path {
                Some(path) => format!("{}/{}.parquet", path, ark.ticker),
                None => format!("data/parquet/{}.parquet", ark.ticker),
            },
            ark.df.clone(),
        )?;
        Ok(ark)
    }

    fn write_df_parquet(path: String, df: DF) -> Result<(), Error> {
        if let Some(parent) = Path::new(&path).parent() {
            if !parent.exists() {
                create_dir_all(parent)?;
            }
        }
        ParquetWriter::new(File::create(&path)?).finish(&mut df.collect()?)?;
        Ok(())
    }

    fn read_parquet(ticker: &Ticker, path: Option<&String>) -> Result<DF, Error> {
        let df = LazyFrame::scan_parquet(
            match path {
                Some(p) => format!("{}/{}.parquet", p, ticker),
                None => format!("data/parquet/{}.parquet", ticker),
            },
            ScanArgsParquet::default(),
        )?;
        Ok(df.into())
    }

    pub fn sort(mut self) -> Result<Self, Error> {
        self.df = Self::df_sort(self.df)?;
        Ok(self)
    }

    pub fn df_sort(df: DF) -> Result<DF, Error> {
        Ok(df
            .collect()?
            .sort(["date", "weight"], vec![false, true], false)?
            .into())
    }

    fn concat_df(dfs: Vec<DF>) -> Result<DF, Error> {
        // with dedupe
        let df = concat(
            dfs.lazy(),
            UnionArgs {
                ..Default::default()
            },
        )?;
        Self::dedupe(df.into())
    }

    pub fn dedupe(mut df: DF) -> Result<DF, Error> {
        df = df
            .lazy()
            .unique_stable(None, UniqueKeepStrategy::First)
            .into();
        Ok(df)
    }

    pub fn format(mut self) -> Result<Self, Error> {
        // self.df = Self::df_format(self.df, Some(self.ticker.data_source()))?;
        self.df = Self::df_format(self.df, None)?;
        Ok(self)
    }

    pub fn df_format(df: DF, data_source: Option<DataSource>) -> Result<DF, Error> {
        let mut df = df.collect()?;
        match data_source {
            Some(ds) => {
                df = format::data_source(ds, df.into())?.collect()?;
            }
            None => {
                df = format::df_format_europe_csv(df.into())?.collect()?;
                df = format::df_format_europe_arkfundsio(df.into())?.collect()?;
                df = format::df_format_21shares(df.into())?.collect()?;
                df = format::df_format_arkvx(df.into())?.collect()?;
                df = format::df_format_europe(df.into())?.collect()?;
            }
        }

        if df.get_column_names().contains(&"market_value_($)") {
            df = df
                .lazy()
                .rename(vec!["market_value_($)"], vec!["market_value"])
                .collect()?;
        }
        if df.get_column_names().contains(&"weight_($)") {
            df = df
                .lazy()
                .rename(vec!["weight_(%)"], vec!["weight"])
                .collect()?;
        }
        if df.get_column_names().contains(&"market value ($)") {
            df = df
                .lazy()
                .rename(
                    vec!["market value ($)", "weight (%)"],
                    vec!["market_value", "weight"],
                )
                .collect()?;
        }
        if df.get_column_names().contains(&"weight ($)") {
            df = df
                .lazy()
                .rename(vec!["weight (%)"], vec!["weight"])
                .collect()?;
        }

        if df.get_column_names().contains(&"fund") {
            _ = df.drop_in_place("fund");
        }
        if df.get_column_names().contains(&"weight_rank") {
            _ = df.drop_in_place("weight_rank");
        }

        if df.get_column_names().contains(&"") {
            let mut cols = df.get_column_names();
            cols.retain(|&item| !item.is_empty());
            df = df.select(cols)?;
        }

        if !df.fields().contains(&Field::new("date", DataType::Date)) {
            let date_format =
                |mut df: DataFrame, format: Option<String>| -> Result<DataFrame, Error> {
                    df = df
                        .lazy()
                        .with_column(col("date").str().strptime(
                            DataType::Date,
                            StrptimeOptions {
                                format,
                                strict: false,
                                ..Default::default()
                            },
                        ))
                        .collect()?;

                    if df.column("date").unwrap().null_count() > df.height() / 10 {
                        return Err(anyhow!("wrong date format"));
                    }

                    Ok(df)
                };

            if let Ok(x) = date_format(df.clone(), Some("%m/%d/%Y".into())) {
                df = x
            } else if let Ok(x) = date_format(df.clone(), Some("%Y/%m/%d".into())) {
                df = x
            } else if let Ok(x) = date_format(df.clone(), None) {
                df = x
            }
        }

        df = format::Ticker::all(df.into())?.collect()?;

        let mut expressions: Vec<Expr> = vec![];

        if df.fields().contains(&Field::new("weight", DataType::Utf8)) {
            expressions.push(
                col("weight")
                    .str()
                    .replace(lit("%"), lit(""), true)
                    .cast(DataType::Float64),
            );
        }

        if df.fields().contains(&Field::new(
            "date",
            DataType::Datetime(TimeUnit::Milliseconds, None),
        )) {
            expressions.push(col("date").cast(DataType::Date));
        }

        if df
            .fields()
            .contains(&Field::new("market_value", DataType::Utf8))
        {
            expressions.push(
                col("market_value")
                    .str()
                    .replace(lit("$"), lit(""), true)
                    .str()
                    .replace_all(lit(","), lit(""), true)
                    .cast(DataType::Float64)
                    .cast(DataType::Int64),
            );
        }

        if df
            .fields()
            .contains(&Field::new("market_value", DataType::Float64))
        {
            expressions.push(col("market_value").cast(DataType::Int64));
        }

        if df.fields().contains(&Field::new("shares", DataType::Utf8)) {
            expressions.push(
                col("shares")
                    .str()
                    .replace_all(lit(","), lit(""), true)
                    .cast(DataType::Int64),
            );
        }

        // rename values
        expressions.push(
            col("ticker")
                .str()
                .replace_all(lit(" FP"), lit(""), true)
                .str()
                .replace_all(lit(" UQ"), lit(""), true)
                .str()
                .replace_all(lit(" UF"), lit(""), true)
                .str()
                .replace_all(lit(" UN"), lit(""), true)
                .str()
                .replace_all(lit(" UW"), lit(""), true)
                .str()
                .replace_all(lit("/U"), lit(""), true)
                .str()
                .replace_all(lit(" CN"), lit(""), true)
                .str()
                .replace(lit("DKNN"), lit("DKNG"), true)
                .str()
                .rstrip(None),
        );
        expressions.push(
            col("company")
                .str()
                .replace_all(lit("-A"), lit(""), true)
                .str()
                .replace_all(lit("- A"), lit(""), true)
                .str()
                .replace_all(lit("CL A"), lit(""), true)
                .str()
                .replace_all(lit("CLASS A"), lit(""), true)
                .str()
                .replace_all(lit("Inc"), lit(""), true)
                .str()
                .replace_all(lit("INC"), lit(""), true)
                .str()
                .replace_all(lit("incorporated"), lit(""), true)
                .str()
                .replace_all(lit("LTD"), lit(""), true)
                .str()
                .replace_all(lit("CORP"), lit(""), true)
                .str()
                .replace_all(lit("CORPORATION"), lit(""), true)
                .str()
                .replace_all(lit("Corporation"), lit(""), true)
                .str()
                .replace_all(lit("- C"), lit(""), true)
                .str()
                .replace_all(lit("-"), lit(""), true)
                .str()
                .replace_all(lit(","), lit(""), true)
                .str()
                .replace_all(lit("."), lit(""), true)
                .str()
                .replace(lit("HLDGS"), lit(""), true)
                .str()
                .replace(lit("HOLDINGS"), lit(""), true)
                .str()
                .replace(lit("Holdings"), lit(""), true)
                .str()
                .replace(lit(" HOLDIN"), lit(""), true)
                .str()
                .replace(lit("ORATION"), lit(""), true)
                .str()
                .replace(lit(" PLC"), lit(""), true)
                .str()
                .replace(lit(" AG"), lit(""), true)
                .str()
                .replace(lit(" ADR"), lit(""), true)
                .str()
                .replace(lit(" SA"), lit(""), true)
                .str()
                .replace(lit(" NV"), lit(""), true)
                .str()
                .replace(lit(" SE"), lit(""), true)
                .str()
                .replace(lit(" CL C"), lit(""), true)
                .str()
                .replace(lit("COINBASE GLOBAL"), lit("COINBASE"), true)
                .str()
                .replace(lit("Coinbase Global"), lit("Coinbase"), true)
                .str()
                .replace(lit("Blackdaemon"), lit("Blockdaemon"), true)
                .str()
                .replace(lit("DISCOVERY"), lit("Dassault Systemes"), true)
                .str()
                .replace(lit("Space Investment"), lit("SpaceX"), true)
                .str()
                .replace(
                    lit("Space Exploration Technologies Corp"),
                    lit("SpaceX"),
                    true,
                )
                .str()
                .replace(
                    lit("Space Exploration Technologies Co"),
                    lit("SpaceX"),
                    true,
                )
                .str()
                .rstrip(None),
        );

        df = df
            .lazy()
            .with_columns(expressions)
            .filter(col("date").is_not_null())
            .collect()?;

        if !df.get_column_names().contains(&"share_price")
            && df.get_column_names().contains(&"market_value")
        {
            df = df
                .lazy()
                .with_column(
                    (col("market_value").cast(DataType::Float64)
                        / col("shares").cast(DataType::Float64))
                    .alias("share_price")
                    .cast(DataType::Float64)
                    .round(2),
                )
                .collect()?
        }

        let mut expressions: Vec<Expr> = vec![];

        if df
            .fields()
            .contains(&Field::new("market_value", DataType::Float64))
        {
            expressions.push(col("market_value").cast(DataType::Int64));
        }
        if df
            .fields()
            .contains(&Field::new("shares", DataType::Float64))
        {
            expressions.push(col("shares").cast(DataType::Int64));
        }
        if df
            .fields()
            .contains(&Field::new("share_price", DataType::Int64))
        {
            expressions.push(col("share_price").cast(DataType::Float64));
        }
        if df.fields().contains(&Field::new("weight", DataType::Int64)) {
            expressions.push(col("weight").cast(DataType::Float64));
        }

        df = df.lazy().with_columns(expressions).collect()?;

        if df.get_column_names().contains(&"share_price") {
            df = df.select([
                "date",
                "ticker",
                "cusip",
                "company",
                "market_value",
                "shares",
                "share_price",
                "weight",
            ])?;
        } else if !df
            .get_column_names()
            .eq(&["date", "ticker", "cusip", "company", "weight"])
        {
            df = df.select(["date", "ticker", "cusip", "company", "weight"])?;
        }

        Ok(df.into())
    }

    pub fn get_api(
        &self,
        last_day: Option<NaiveDate>,
        source: Option<&Source>,
    ) -> Result<DataFrame, Error> {
        let default_start_day = "2000-01-01";
        let url = match (self.ticker.data_source(), last_day, source) {
            (DataSource::ArkEurope, Some(last_day), _) => format!(
                "https://api.nexveridian.com/ark_holdings?ticker={}&start={}",
                self.ticker, last_day
            ),
            (DataSource::ArkEurope, None, _) => format!(
                "https://api.nexveridian.com/ark_holdings?ticker={}&start={}",
                self.ticker, default_start_day
            ),

            // arkfunds.io
            (_, Some(last_day), Some(Source::ArkFundsIoIncremental)) => format!(
                "https://arkfunds.io/api/v2/etf/holdings?symbol={}&date_from={}",
                self.ticker, last_day
            ),
            (_, None, Some(Source::ArkFundsIoIncremental)) => format!(
                "https://arkfunds.io/api/v2/etf/holdings?symbol={}&date_from={}",
                self.ticker, default_start_day
            ),
            (_, _, Some(Source::ArkFundsIoFull)) => format!(
                "https://arkfunds.io/api/v2/etf/holdings?symbol={}&date_from={}",
                self.ticker, default_start_day
            ),

            // api.nexveridian.com
            (_, Some(last_day), _) => format!(
                "https://api.nexveridian.com/ark_holdings?ticker={}&start={}",
                self.ticker, last_day
            ),
            (_, None, _) => format!(
                "https://api.nexveridian.com/ark_holdings?ticker={}&start={}",
                self.ticker, default_start_day
            ),
        };

        let mut df = Reader::Json.get_data_url(url)?;
        df = match source {
            Some(Source::ArkFundsIoIncremental) | Some(Source::ArkFundsIoFull) => df
                .column("holdings")?
                .clone()
                .explode()?
                .struct_()?
                .clone()
                .unnest(),
            _ => df,
        };
        Ok(df)
    }

    pub fn get_csv_ark(&self) -> Result<DataFrame, Error> {
        let url = self.ticker.get_url();
        Reader::Csv.get_data_url(url)
    }

    pub fn merge_old_csv_to_parquet(ticker: Ticker, path: Option<String>) -> Result<Self, Error> {
        let mut dfs = vec![];
        for x in glob(&format!("data/csv/{}/*", ticker))?.filter_map(Result::ok) {
            dfs.push(LazyCsvReader::new(x).finish()?);
        }

        let mut df = concat(
            dfs,
            UnionArgs {
                ..Default::default()
            },
        )?
        .into();

        if Self::read_parquet(&ticker, path.as_ref()).is_ok() {
            let df_old = Self::read_parquet(&ticker, path.as_ref())?;
            df = Self::concat_df(vec![
                Self::df_format(df_old, None)?,
                Self::df_format(df, None)?,
            ])?;
            df = Self::df_format(df, None)?;
        }
        Ok(Self { df, ticker, path })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::*;
    use pretty_assertions::assert_eq;
    use serial_test::serial;
    use std::fs;

    #[test]
    #[serial]
    fn read_write_parquet() -> Result<(), Error> {
        let test_df = defualt_df(&[Some("COIN")], &[Some("COINBASE")])?;

        Ark::write_df_parquet("data/test/ARKK.parquet".into(), test_df.clone().into())?;
        let read = Ark::new(Source::Read, Ticker::ARKK, Some("data/test".to_owned()))?.collect()?;
        fs::remove_file("data/test/ARKK.parquet")?;

        assert_eq!(read, test_df);
        Ok(())
    }

    #[test]
    #[serial]
    fn arkw_format_arkb() -> Result<(), Error> {
        let test_df = defualt_df(
            &[None::<&str>, Some("ARKB"), Some("ARKB")],
            &[
                Some("ARK BITCOIN ETF HOLDCO (ARKW)"),
                Some("ARK BITCOIN ETF HOLDCO (ARKW)"),
                Some("ARKB"),
            ],
        )?;

        Ark::write_df_parquet("data/test/ARKW.parquet".into(), test_df.clone().into())?;
        let read = Ark::new(Source::Read, Ticker::ARKW, Some("data/test".to_owned()))?.collect()?;
        fs::remove_file("data/test/ARKW.parquet")?;

        let df = Ark::df_format(read.into(), None)?.collect()?;
        assert_eq!(
            df,
            defualt_df(
                &[Some("ARKB"), Some("ARKB"), Some("ARKB")],
                &[Some("ARKB"), Some("ARKB"), Some("ARKB")]
            )?,
        );

        Ok(())
    }

    #[test]
    #[serial]
    fn arkf_format_arkb() -> Result<(), Error> {
        let test_df = defualt_df(
            &[None::<&str>, Some("ARKB"), Some("ARKB")],
            &[
                Some("ARK BITCOIN ETF HOLDCO (ARKF)"),
                Some("ARK BITCOIN ETF HOLDCO (ARKF)"),
                Some("ARKB"),
            ],
        )?;
        Ark::write_df_parquet("data/test/ARKF.parquet".into(), test_df.clone().into())?;
        let read = Ark::new(Source::Read, Ticker::ARKF, Some("data/test".to_owned()))?.collect()?;
        fs::remove_file("data/test/ARKF.parquet")?;

        let df = Ark::df_format(read.into(), None)?.collect()?;
        assert_eq!(
            df,
            defualt_df(
                &[Some("ARKB"), Some("ARKB"), Some("ARKB")],
                &[Some("ARKB"), Some("ARKB"), Some("ARKB")]
            )?,
        );

        Ok(())
    }
}

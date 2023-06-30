use chrono::NaiveDate;
use glob::glob;
use polars::datatypes::DataType;
use polars::lazy::dsl::StrptimeOptions;
use polars::prelude::*;
use reqwest::blocking::Client;
use serde_json::Value;
use std::error::Error;
use std::fs::{create_dir_all, File};
use std::io::Cursor;
use std::path::Path;
use std::result::Result;
use strum_macros::EnumIter;

#[derive(strum_macros::Display, EnumIter, Clone, Copy, PartialEq)]
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

#[derive(Clone)]
pub enum DF {
    LazyFrame(LazyFrame),
    DataFrame(DataFrame),
}
impl From<LazyFrame> for DF {
    fn from(lf: LazyFrame) -> Self {
        DF::LazyFrame(lf)
    }
}
impl From<DataFrame> for DF {
    fn from(df: DataFrame) -> Self {
        DF::DataFrame(df)
    }
}
impl DF {
    pub fn collect(self) -> Result<DataFrame, Box<dyn Error>> {
        match self {
            DF::LazyFrame(x) => Ok(x.collect()?),
            DF::DataFrame(x) => Ok(x),
        }
    }
    pub fn lazy(self) -> LazyFrame {
        match self {
            DF::LazyFrame(x) => x,
            DF::DataFrame(x) => x.lazy(),
        }
    }
}
trait DFS {
    fn lazy(self) -> Vec<LazyFrame>;
    fn collect(self) -> Vec<DataFrame>;
}
impl DFS for Vec<DF> {
    fn lazy(self) -> Vec<LazyFrame> {
        self.into_iter().map(|df| df.lazy()).collect()
    }
    fn collect(self) -> Vec<DataFrame> {
        self.into_iter().map(|df| df.collect().unwrap()).collect()
    }
}

pub enum Source {
    Read,
    Ark,
    ApiIncremental,
    ApiFull,
}
pub struct Ark {
    pub df: DF,
    ticker: Ticker,
    path: Option<String>,
}
impl Ark {
    pub fn new(
        source: Source,
        ticker: Ticker,
        path: Option<String>,
    ) -> Result<Self, Box<dyn Error>> {
        let existing_file = Self::read_parquet(ticker, path.clone()).is_ok();

        let mut ark = Self {
            df: match existing_file {
                true => Self::read_parquet(ticker, path.clone())?,
                false => DF::DataFrame(df!["date" => [""],]?),
            },
            ticker,
            path,
        };

        let update = match (source, existing_file) {
            (Source::Read, false) => {
                panic!("Can not read from file, file is empty, does not exist, or is locked")
            }
            (Source::Read, true) => None,
            (Source::Ark, _) => Some(ark.get_csv_ark()?),
            (Source::ApiIncremental, true) => {
                let last_day = ark
                    .df
                    .clone()
                    .collect()?
                    .column("date")
                    .unwrap()
                    .max()
                    .and_then(NaiveDate::from_num_days_from_ce_opt);
                Some(ark.get_api(last_day)?)
            }
            (Source::ApiIncremental, false) | (Source::ApiFull, _) => Some(ark.get_api(None)?),
        };

        if let Some(update) = update {
            if existing_file {
                ark.df = Self::concat_df(vec![
                    Self::df_format(ark.df)?,
                    Self::df_format(update.into())?,
                ])?;
            } else {
                ark.df = Self::df_format(update.into())?;
            }
        }

        Ok(ark)
    }

    pub fn collect(self) -> Result<DataFrame, Box<dyn Error>> {
        self.df.collect()
    }

    pub fn write_parquet(self) -> Result<Self, Box<dyn Error>> {
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

    fn write_df_parquet(path: String, df: DF) -> Result<(), Box<dyn Error>> {
        if let Some(parent) = Path::new(&path).parent() {
            if !parent.exists() {
                create_dir_all(parent)?;
            }
        }
        ParquetWriter::new(File::create(&path)?).finish(&mut df.collect()?)?;
        Ok(())
    }

    fn read_parquet(ticker: Ticker, path: Option<String>) -> Result<DF, Box<dyn Error>> {
        let df = LazyFrame::scan_parquet(
            match path {
                Some(p) => format!("{}/{}.parquet", p, ticker),
                None => format!("data/parquet/{}.parquet", ticker),
            },
            ScanArgsParquet::default(),
        )?;
        Ok(df.into())
    }

    pub fn sort(mut self) -> Result<Self, Box<dyn Error>> {
        self.df = Self::df_sort(self.df.clone())?;
        Ok(self)
    }

    pub fn df_sort(df: DF) -> Result<DF, Box<dyn Error>> {
        Ok(df
            .collect()?
            .sort(["date", "weight"], vec![false, true])?
            .into())
    }

    fn concat_df(dfs: Vec<DF>) -> Result<DF, Box<dyn Error>> {
        // with dedupe
        let df = concat(dfs.lazy(), false, true)?;
        Self::dedupe(df.into())
    }

    pub fn dedupe(mut df: DF) -> Result<DF, Box<dyn Error>> {
        df = df
            .lazy()
            .unique_stable(None, UniqueKeepStrategy::First)
            .into();
        Ok(df)
    }

    pub fn format(mut self) -> Result<Self, Box<dyn Error>> {
        self.df = Self::df_format(self.df.clone())?;
        Ok(self)
    }

    pub fn df_format(df: DF) -> Result<DF, Box<dyn Error>> {
        let mut df = df.collect()?;

        if df.get_column_names().contains(&"market_value_($)") {
            df = df
                .lazy()
                .rename(
                    vec!["market_value_($)", "weight_(%)"],
                    vec!["market_value", "weight"],
                )
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
        if df.get_column_names().contains(&"CUSIP") {
            df = df
                .lazy()
                .rename(vec!["CUSIP", "weight (%)"], vec!["cusip", "weight"])
                .collect()?;
        }

        // if df.rename("market_value_($)", "market_value").is_ok() {}
        // if df.rename("market value ($)", "market_value").is_ok() {}
        // if df.rename("weight_(%)", "weight").is_ok() {}
        // if df.rename("weight (%)", "weight").is_ok() {}
        // if df.rename("CUSIP", "cusip").is_ok() {}

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
            let date_format = |df: DataFrame, format: &str| -> Result<DataFrame, Box<dyn Error>> {
                Ok(df
                    .lazy()
                    .with_column(col("date").str().strptime(
                        DataType::Date,
                        StrptimeOptions {
                            format: Some(format.into()),
                            strict: false,
                            exact: true,
                            cache: true,
                        },
                    ))
                    .collect()?)
            };

            if let Ok(x) = date_format(df.clone(), "%m/%d/%Y") {
                df = x
            }
            if let Ok(x) = date_format(df.clone(), "%Y/%m/%d") {
                df = x
            }
        }

        let mut expressions: Vec<Expr> = vec![];

        if df.fields().contains(&Field::new("weight", DataType::Utf8)) {
            expressions.push(
                col("weight")
                    .str()
                    .replace(lit("%"), lit(""), true)
                    .cast(DataType::Float64),
            );
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

        if df.fields().contains(&Field::new("shares", DataType::Utf8)) {
            expressions.push(
                col("shares")
                    .str()
                    .replace_all(lit(","), lit(""), true)
                    .cast(DataType::Int64),
            );
        }

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

    pub fn get_api(&self, last_day: Option<NaiveDate>) -> Result<LazyFrame, Box<dyn Error>> {
        let url = match (self.ticker, last_day) {
            (self::Ticker::ARKVC, Some(last_day)) => format!(
                "https://api.nexveridian.com/arkvc_holdings?start={}",
                last_day
            ),
            (tic, Some(last_day)) => format!(
                "https://api.nexveridian.com/ark_holdings?ticker={}&start={}",
                tic, last_day
            ),
            (self::Ticker::ARKVC, None) => "https://api.nexveridian.com/arkvc_holdings".to_owned(),
            (tic, None) => {
                format!("https://api.nexveridian.com/ark_holdings?ticker={}", tic)
            }
        };
        Reader::Json.get_data_url(url)
    }

    pub fn get_csv_ark(&self) -> Result<LazyFrame, Box<dyn Error>> {
        let url = match self.ticker {
            self::Ticker::ARKVC => "https://ark-ventures.com/wp-content/uploads/funds-etf-csv/ARK_VENTURE_FUND_HOLDINGS.csv".to_owned(),
            _ => format!("https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_{}_ETF_{}_HOLDINGS.csv", self.ticker.value(), self.ticker),
        };
        Reader::Csv.get_data_url(url)
    }

    pub fn merge_old_csv_to_parquet(
        ticker: Ticker,
        path: Option<String>,
    ) -> Result<Self, Box<dyn Error>> {
        let mut dfs = vec![];
        for x in glob(&format!("data/csv/{}/*", ticker))?.filter_map(Result::ok) {
            dfs.push(LazyCsvReader::new(x).finish()?);
        }
        let mut df = concat(dfs, false, true)?.into();

        if Self::read_parquet(ticker, path.clone()).is_ok() {
            let df_old = Self::read_parquet(ticker, path.clone())?;
            df = Self::concat_df(vec![Self::df_format(df_old)?, Self::df_format(df)?])?
        }

        Ok(Self { df, ticker, path })
    }
}

pub enum Reader {
    Csv,
    Json,
}

impl Reader {
    pub fn get_data_url(&self, url: String) -> Result<LazyFrame, Box<dyn Error>> {
        let response = Client::builder()
        .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3")
        .build()?.get(url).send()?;

        if !response.status().is_success() {
            return Err(format!(
                "HTTP request failed with status code: {:?}",
                response.status()
            )
            .into());
        }

        let data = response.text()?.into_bytes();

        let df: LazyFrame = match self {
            Self::Csv => CsvReader::new(Cursor::new(data))
                .has_header(true)
                .finish()?
                .lazy(),
            Self::Json => {
                let json_string = String::from_utf8(data)?;
                let json: Value = serde_json::from_str(&json_string)?;
                JsonReader::new(Cursor::new(json.to_string()))
                    .finish()?
                    .lazy()
            }
        };

        Ok(df)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::fs;

    #[test]
    #[serial]
    fn read_write_parquet() -> Result<(), Box<dyn Error>> {
        let test_df = df![
            "date" => ["2023-01-01"],
            "ticker" => ["TSLA"],
            "cusip" => ["123abc"],
            "company" => ["Tesla"],
            "market_value" => [100],
            "shares" => [10],
            "share_price" => [10],
            "weight" => [10.00]
        ]?;

        Ark::write_df_parquet("data/test/ARKK.parquet".into(), test_df.clone().into())?;
        let read = Ark::new(Source::Read, Ticker::ARKK, Some("data/test".to_owned()))?.collect()?;
        fs::remove_file("data/test/ARKK.parquet")?;

        assert_eq!(read, test_df);
        Ok(())
    }
}

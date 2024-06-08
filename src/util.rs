use anyhow::{anyhow, Error, Result};
use chrono::{Duration, NaiveDate};
use glob::glob;
use polars::datatypes::DataType;
use polars::lazy::dsl::StrptimeOptions;
use polars::prelude::*;
use reqwest::blocking::Client;
use serde_json::Value;
use std::fs::{create_dir_all, File};
use std::io::Cursor;
use std::path::Path;
use strum_macros::{EnumIter, EnumString};

#[derive(Debug, Default, strum_macros::Display, EnumIter, Clone, Copy, PartialEq)]
pub enum Ticker {
    ARKVX,
    ARKF,
    ARKG,
    #[default]
    ARKK,
    ARKQ,
    ARKW,
    ARKX,
    ARKA,
    ARKZ,
    ARKC,
    ARKD,
    ARKY,
    ARKB,
    PRNT,
    IZRL,
}
impl Ticker {
    pub fn value(&self) -> &str {
        match *self {
            Ticker::ARKVX => "ARK_VENTURE_FUND_ARKVX_HOLDINGS.csv",
            Ticker::ARKF => "FINTECH_INNOVATION",
            Ticker::ARKG => "GENOMIC_REVOLUTION",
            Ticker::ARKK => "INNOVATION",
            Ticker::ARKQ => "AUTONOMOUS_TECH._&_ROBOTICS",
            Ticker::ARKW => "NEXT_GENERATION_INTERNET",
            Ticker::ARKX => "SPACE_EXPLORATION_&_INNOVATION",
            Ticker::ARKA => "ARKA",
            Ticker::ARKZ => "ARKZ",
            Ticker::ARKC => "ARKC",
            Ticker::ARKD => "ARKD",
            Ticker::ARKY => "ARKY",
            Ticker::ARKB => "21SHARES_BITCOIN",
            Ticker::PRNT => "THE_3D_PRINTING",
            Ticker::IZRL => "ISRAEL_INNOVATIVE_TECHNOLOGY",
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
    pub fn collect(self) -> Result<DataFrame, Error> {
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
                    Self::df_format(ark.df)?,
                    Self::df_format(update.into())?,
                ])?;
            } else {
                ark.df = Self::df_format(update.into())?;
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
        self.df = Self::df_format(self.df)?;
        Ok(self)
    }

    fn df_format_21shares(df: DF) -> Result<DF, Error> {
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

    fn df_format_arkvx(df: DF) -> Result<DF, Error> {
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
                    Series::new("share_price", [None::<i64>]).lit(),
                ])
                .collect()?;
        }

        Ok(df.into())
    }

    pub fn df_format(df: DF) -> Result<DF, Error> {
        let mut df = Self::df_format_21shares(df)?.collect()?;
        df = Self::df_format_arkvx(df.into())?.collect()?;

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

        // format arkw, ARK BITCOIN ETF HOLDCO (ARKW) to ARKB
        if let Ok(x) = df
            .clone()
            .lazy()
            .with_columns(vec![
                when(col("company").eq(lit("ARK BITCOIN ETF HOLDCO (ARKW)")))
                    .then(lit("ARKB"))
                    .otherwise(col("ticker"))
                    .alias("ticker"),
                when(col("company").eq(lit("ARK BITCOIN ETF HOLDCO (ARKW)")))
                    .then(lit("ARKB"))
                    .otherwise(col("company"))
                    .alias("company"),
            ])
            .with_columns(vec![
                when(col("company").eq(lit("ARK BITCOIN ETF HOLDCO (ARKF)")))
                    .then(lit("ARKB"))
                    .otherwise(col("ticker"))
                    .alias("ticker"),
                when(col("company").eq(lit("ARK BITCOIN ETF HOLDCO (ARKF)")))
                    .then(lit("ARKB"))
                    .otherwise(col("company"))
                    .alias("company"),
            ])
            .collect()
        {
            df = x;
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

        if df
            .fields()
            .contains(&Field::new("shares", DataType::Float64))
        {
            expressions.push(col("shares").cast(DataType::Int64));
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

        // run expressions
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

    pub fn get_api(
        &self,
        last_day: Option<NaiveDate>,
        source: Option<&Source>,
    ) -> Result<DataFrame, Error> {
        let default_start_day = "2000-01-01";
        let url = match (&self.ticker, last_day, source) {
            (self::Ticker::ARKVX, Some(last_day), _) => format!(
                "https://api.nexveridian.com/ark_holdings?ticker=ARKVX&start={}",
                last_day
            ),
            (self::Ticker::ARKVX, None, _) => format!(
                "https://api.nexveridian.com/ark_holdings?ticker=ARKVX&start={}",
                default_start_day
            ),

            (tic, Some(last_day), Some(Source::ArkFundsIoIncremental)) => format!(
                "https://arkfunds.io/api/v2/etf/holdings?symbol={}&date_from={}",
                tic, last_day
            ),
            (tic, None, Some(Source::ArkFundsIoFull)) => format!(
                "https://arkfunds.io/api/v2/etf/holdings?symbol={}&date_from={}",
                tic, default_start_day
            ),

            (tic, Some(last_day), _) => format!(
                "https://api.nexveridian.com/ark_holdings?ticker={}&start={}",
                tic, last_day
            ),
            (tic, None, _) => format!(
                "https://api.nexveridian.com/ark_holdings?ticker={}&start={}",
                tic, default_start_day
            ),
        };

        let mut df = Reader::Json.get_data_url(url)?;
        df = match source {
            Some(Source::ArkFundsIoIncremental) | Some(Source::ArkFundsIoFull) => {
                df = df
                    .column("holdings")?
                    .clone()
                    .explode()?
                    .struct_()?
                    .clone()
                    .unnest();
                df
            }
            _ => df,
        };
        Ok(df)
    }

    pub fn get_csv_ark(&self) -> Result<DataFrame, Error> {
        let url = match self.ticker {
            self::Ticker::ARKVX => format!("https://assets.ark-funds.com/fund-documents/funds-etf-csv/{}", self.ticker.value()),
            self::Ticker::ARKA | self::Ticker::ARKZ | self::Ticker::ARKC | self::Ticker::ARKD |
            self::Ticker::ARKY => format!("https://cdn.21shares-funds.com/uploads/fund-documents/us-bank/holdings/product/current/{}-Export.csv", self.ticker.value()),
            _ => format!("https://assets.ark-funds.com/fund-documents/funds-etf-csv/ARK_{}_ETF_{}_HOLDINGS.csv", self.ticker.value(), self.ticker),
        };
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
            df = Self::concat_df(vec![Self::df_format(df_old)?, Self::df_format(df)?])?;
            df = Self::df_format(df)?;
        }
        Ok(Self { df, ticker, path })
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Reader {
    Csv,
    Json,
}

impl Reader {
    pub fn get_data_url(&self, url: String) -> Result<DataFrame, Error> {
        let response = Client::builder()
            .user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36")
            .gzip(true)
            .build()?
            .get(url)
            .send()?;

        if !response.status().is_success() {
            return Err(anyhow!(
                "HTTP request failed with status code: {:?}",
                response.status()
            ));
        }

        let data = response.text()?.into_bytes();

        let df = match self {
            Self::Csv => CsvReader::new(Cursor::new(data))
                .has_header(true)
                .finish()?,
            Self::Json => {
                let json_string = String::from_utf8(data)?;
                let json: Value = serde_json::from_str(&json_string)?;
                JsonReader::new(Cursor::new(json.to_string())).finish()?
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
    fn read_write_parquet() -> Result<(), Error> {
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

    #[test]
    #[serial]
    fn arkw_format_arkb() -> Result<(), Error> {
        let test_df = df![
            "date" => ["2024-01-01", "2024-01-02"],
            "ticker" => [None::<&str>, Some("TSLA")],
            "cusip" => ["123abc", "TESLA"],
            "company" => ["ARK BITCOIN ETF HOLDCO (ARKW)", "TESLA"],
            "market_value" => [100, 400],
            "shares" => [10, 20],
            "share_price" => [10, 20],
            "weight" => [10.00, 20.00]
        ]?;

        Ark::write_df_parquet("data/test/ARKW.parquet".into(), test_df.clone().into())?;
        let read = Ark::new(Source::Read, Ticker::ARKW, Some("data/test".to_owned()))?.collect()?;
        fs::remove_file("data/test/ARKW.parquet")?;

        let df = Ark::df_format(read.into())?.collect()?;
        assert_eq!(
            df,
            df![
                "date" => ["2024-01-01", "2024-01-02"],
                "ticker" => ["ARKB", "TSLA"],
                "cusip" => ["123abc", "TESLA"],
                "company" => ["ARKB", "TESLA"],
                "market_value" => [100, 400],
                "shares" => [10, 20],
                "share_price" => [10, 20],
                "weight" => [10.00, 20.00]
            ]?
        );

        Ok(())
    }

    #[test]
    #[serial]
    fn arkf_format_arkb() -> Result<(), Error> {
        let test_df = df![
            "date" => ["2024-01-01", "2024-01-02"],
            "ticker" => [None::<&str>, Some("TSLA")],
            "cusip" => ["123abc", "TESLA"],
            "company" => ["ARK BITCOIN ETF HOLDCO (ARKF)", "TESLA"],
            "market_value" => [100, 400],
            "shares" => [10, 20],
            "share_price" => [10, 20],
            "weight" => [10.00, 20.00]
        ]?;

        Ark::write_df_parquet("data/test/ARKF.parquet".into(), test_df.clone().into())?;
        let read = Ark::new(Source::Read, Ticker::ARKF, Some("data/test".to_owned()))?.collect()?;
        fs::remove_file("data/test/ARKF.parquet")?;

        let df = Ark::df_format(read.into())?.collect()?;
        assert_eq!(
            df,
            df![
                "date" => ["2024-01-01", "2024-01-02"],
                "ticker" => ["ARKB", "TSLA"],
                "cusip" => ["123abc", "TESLA"],
                "company" => ["ARKB", "TESLA"],
                "market_value" => [100, 400],
                "shares" => [10, 20],
                "share_price" => [10, 20],
                "weight" => [10.00, 20.00]
            ]?
        );

        Ok(())
    }
}

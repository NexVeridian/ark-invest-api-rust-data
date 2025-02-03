use anyhow::{anyhow, Error};
use polars::frame::DataFrame;
use polars::io::SerReader;
use polars::prelude::{CsvReader, JsonReader};
use reqwest::blocking::Client;
use serde_json::Value;
use std::io::Cursor;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reader {
    Csv,
    Json,
}

impl Reader {
    pub fn get_data_url(&self, url: String) -> anyhow::Result<DataFrame, Error> {
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

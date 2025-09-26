use anyhow::{Error, Result};
use ark_invest_api_rust_data::{util::ticker::Ticker, *};
use clokwerk::{AsyncScheduler, Job, TimeUnits};
use futures::future::join_all;
use lazy_static::lazy_static;
use polars::prelude::DataFrame;
use rand::Rng;
use std::env;
use std::str::FromStr;
use std::thread;
use strum::IntoEnumIterator;
use tokio::task;
use tokio::time::Duration;

lazy_static! {
    static ref SOURCE: Source = match env::var("ARK_SOURCE") {
        Ok(val) =>
            Source::from_str(val.as_str()).expect("Env string ARK_SOURCE is not in enum Source"),
        Err(_) => Source::ApiIncremental,
    };
}

fn print_df(ticker: &Ticker, df: &DataFrame) {
    println!(
        "Ticker: {:#?}\nShape: {:?}\n{:#?}",
        ticker,
        df.shape(),
        df.tail(Some(1))
    );
}

fn csv_merge() -> Result<(), Error> {
    for ticker in Ticker::iter() {
        if !std::path::Path::new(&format!("./data/csv/{ticker}")).exists() {
            continue;
        }

        let df = Ark::merge_old_csv_to_parquet(ticker, None)?
            .format()?
            .sort()?
            .write_parquet()?
            .collect()?;
        print_df(&ticker, &df);
    }
    Ok(())
}

fn ark_plan(ticker: Ticker) -> Result<(), Error> {
    println!("Starting: {ticker:#?}");
    let sec = Duration::from_secs(rand::rng().random_range(30 * 60..=4 * 60 * 60));
    // sleep(sec).await;
    thread::sleep(sec);

    let df = Ark::new(*SOURCE, ticker, None)?
        .format()?
        .write_parquet()?
        .collect()?;

    print_df(&ticker, &df);
    Ok(())
}

async fn spawn_ark_plan(ticker: Ticker) -> Result<(), Error> {
    task::spawn_blocking(move || ark_plan(ticker).unwrap())
        .await
        .unwrap();
    Ok(())
}

async fn ark_etf() {
    let futures = Ticker::iter()
        .filter(|&x| {
            x != Ticker::ARKA
                && x != Ticker::ARKC
                && x != Ticker::ARKD
                && x != Ticker::ARKY
                && x != Ticker::ARKZ
        })
        .map(spawn_ark_plan)
        .collect::<Vec<_>>();

    join_all(futures).await;
}

#[tokio::main]
async fn main() {
    let mut scheduler = AsyncScheduler::new();
    println!("Scheduler Started");

    if env::var("STARTUP_CSV_MERGE")
        .map(|v| v == "true")
        .unwrap_or(false)
    {
        println!("Merging CSVs to Parquet");
        csv_merge().unwrap();
    }

    if env::var("STARTUP_ARK_ETF")
        .map(|v| v == "true")
        .unwrap_or(false)
    {
        ark_etf().await;
    }

    scheduler.every(1.day()).at("10:00 am").run(ark_etf);
    scheduler.every(1.day()).at("10:00 pm").run(ark_etf);

    loop {
        scheduler.run_pending().await;
        // tokio::time::sleep(Duration::from_millis(10)).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

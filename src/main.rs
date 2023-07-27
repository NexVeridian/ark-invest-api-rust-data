use clokwerk::{AsyncScheduler, Job, TimeUnits};
use futures::future::join_all;
use lazy_static::lazy_static;
use polars::prelude::DataFrame;
use rand::Rng;
use std::env;
use std::error::Error;
use std::result::Result;
use std::str::FromStr;
use std::thread;
use strum::IntoEnumIterator;
use tokio::task;
use tokio::time::Duration;

mod util;
use util::*;

lazy_static! {
    static ref SOURCE: Source = match env::var("ARK_SOURCE") {
        Ok(val) =>
            Source::from_str(val.as_str()).expect("Env string ARK_SOURCE is not in enum Source"),
        Err(_e) => Source::ApiIncremental,
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

fn csv_merge() -> Result<(), Box<dyn Error>> {
    for ticker in Ticker::iter() {
        let df = Ark::merge_old_csv_to_parquet(ticker, None)?
            .format()?
            .sort()?
            .write_parquet()?
            .collect()?;
        print_df(&ticker, &df);
    }
    Ok(())
}

fn ark_plan(ticker: Ticker) -> Result<(), Box<dyn Error>> {
    println!("Starting: {:#?}", ticker);
    let sec = Duration::from_secs(rand::thread_rng().gen_range(5 * 60..=30 * 60));
    // sleep(sec).await;
    thread::sleep(sec);

    let df = Ark::new(*SOURCE, ticker, None)?
        .format()?
        .write_parquet()?
        .collect()?;

    print_df(&ticker, &df);
    Ok(())
}

async fn spawn_ark_plan(ticker: Ticker) -> Result<(), Box<dyn Error + Send>> {
    task::spawn_blocking(move || ark_plan(ticker).unwrap())
        .await
        .unwrap();
    Ok(())
}

async fn ark_etf() {
    let futures = Ticker::iter()
        .filter(|&x| x != Ticker::ARKVC)
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

    scheduler
        .every(5.day())
        .at("11:30 pm")
        .run(|| async { if spawn_ark_plan(Ticker::ARKVC).await.is_ok() {} });

    loop {
        scheduler.run_pending().await;
        // tokio::time::sleep(Duration::from_millis(10)).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

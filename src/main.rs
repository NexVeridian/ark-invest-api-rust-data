use clokwerk::{AsyncScheduler, Job, TimeUnits};
use futures::future::join_all;
use rand::Rng;
use std::error::Error;
use std::result::Result;
use std::thread;
use strum::IntoEnumIterator;
use tokio::task;
use tokio::time::{sleep, Duration};

mod util;
use util::*;

#[tokio::main]
async fn main() {
    let mut scheduler = AsyncScheduler::new();
    println!("Scheduler Started");

    fn ark_plan(ticker: Ticker) -> Result<(), Box<dyn Error>> {
        println!("Starting: {:#?}", ticker);
        let sec = Duration::from_secs(rand::thread_rng().gen_range(5 * 60..=30 * 60));
        // sleep(sec).await;
        thread::sleep(sec);

        let df = Ark::new(Source::Ark, ticker, None)?
            .format()?
            .write_parquet()?
            .collect()?;

        println!("Ticker: {:#?}\n{:#?}", ticker, df.tail(Some(1)));
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

    // ark_etf().await;
    scheduler.every(1.day()).at("11:30 pm").run(ark_etf);

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

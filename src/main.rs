use clokwerk::Interval::*;
use clokwerk::{AsyncScheduler, Job, TimeUnits};
use polars::prelude::LazyFrame;
use polars::prelude::*;
use std::error::Error;
use std::result::Result;
use std::time::Duration;
use strum::IntoEnumIterator;

mod util;
use util::*;

#[tokio::main]
async fn main() {
    let mut scheduler = AsyncScheduler::new();
    scheduler.every(1.day()).at("11:30 pm").run(|| async {
        for x in Ticker::iter() {
            let plan = || -> Result<(), Box<dyn Error>> {
                let df = LazyFrame::scan_parquet(
                    format!("data/old/{}/part.0.parquet", x),
                    ScanArgsParquet::default(),
                )?;
                let df = df_format(x, df)?;
                write_parquet(x, df)?;
                Ok(())
            };

            if let Ok(_) = plan() {}
        }
    });

    let dfn = read_parquet(Ticker::ARKF).unwrap().collect().unwrap();
    println!("{:#?}", dfn);
    loop {
        scheduler.run_pending().await;
        // tokio::time::sleep(Duration::from_millis(10)).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

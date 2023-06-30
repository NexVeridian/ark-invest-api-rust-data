use clokwerk::{AsyncScheduler, Job, TimeUnits};
// use polars::prelude::LazyFrame;
// use polars::prelude::*;
use rand::Rng;
use std::error::Error;
use std::result::Result;
use std::time::Duration;
use strum::IntoEnumIterator;

mod util;
use util::*;
#[tokio::main]
async fn main() {
    let mut scheduler = AsyncScheduler::new();
    println!("Scheduler Started");
    scheduler.every(1.day()).at("11:30 pm").run(|| async {
        for x in Ticker::iter() {
            if x == Ticker::ARKVC {
                continue;
            }
            let plan = || -> Result<(), Box<dyn Error>> {
                let df = Ark::new(Source::Ark, x, None)?
                    .format()?
                    .write_parquet()?
                    .collect()?;

                println!("{:#?}", df.head(Some(1)));
                Ok(())
            };

            if plan().is_ok() {}
            let sec = rand::thread_rng().gen_range(10..=30);
            tokio::time::sleep(Duration::from_secs(sec)).await;
        }
    });

    loop {
        scheduler.run_pending().await;
        // tokio::time::sleep(Duration::from_millis(10)).await;
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
}

// fn main() -> Result<(), Box<dyn std::error::Error>> {
//     let csv = Ark::merge_old_csv_to_parquet(Ticker::ARKK, None)?
//         .format()?
//         .write_parquet()?
//         .collect()?;
//     println!("{:#?}", csv);
//     let read = Ark::new(Source::Read, Ticker::ARKK, None)?.collect()?;
//     println!("{:#?}", read.dtypes());
//     println!("{:#?}", read.get_column_names());
//     println!("{:#?}", read);
//     let api = Ark::new(Source::ApiFull, Ticker::ARKK, None)?.collect()?;
//     println!("{:#?}", api);

//     let ark = Ark::new(Source::Ark, Ticker::ARKK, None)?.collect()?;
//     println!("{:#?}", ark);

//     let ark = Ark::new(Source::Ark, Ticker::ARKVC, None)?.collect()?;
//     println!("{:#?}", ark);
//     Ok(())
// }

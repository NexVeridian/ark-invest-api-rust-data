// use clokwerk::{AsyncScheduler, Job, TimeUnits};
// use polars::prelude::LazyFrame;
// use polars::prelude::*;
// use std::error::Error;
// use std::result::Result;
// use std::time::Duration;
// use strum::IntoEnumIterator;

mod util;
use util::*;
// #[tokio::main]
// async fn main() {
//     let mut scheduler = AsyncScheduler::new();
//     scheduler.every(1.day()).at("11:30 pm").run(|| async {
//         for x in Ticker::iter() {
//             let plan = || -> Result<(), Box<dyn Error>> {
//                 let df = LazyFrame::scan_parquet(
//                     format!("data/old/{}/part.0.parquet", x),
//                     ScanArgsParquet::default(),
//                 )?;
//                 let df = df_format(x, df)?;
//                 write_parquet(x, df)?;
//                 Ok(())
//             };

//             if plan().is_ok() {}
//         }
//     });

//     let dfn = read_parquet(Ticker::ARKF).unwrap().collect().unwrap();
//     println!("{:#?}", dfn);
//     loop {
//         scheduler.run_pending().await;
//         // tokio::time::sleep(Duration::from_millis(10)).await;
//         tokio::time::sleep(Duration::from_secs(1)).await;
//     }
// }

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // let csv = Ark::merge_old_csv_to_parquet(Ticker::ARKK, None)
    //     .unwrap()
    //     .format()
    //     .unwrap()
    //     .write_parquet()
    //     .unwrap()
    //     .collect()
    //     .unwrap();
    // println!("{:#?}", csv);

    let read = Ark::new(Source::Read, Ticker::ARKK, None)?.collect()?;
    println!("{:#?}", read.dtypes());
    println!("{:#?}", read.get_column_names());
    println!("{:#?}", read);

    // let api = Ark::new(Source::ApiFull, Ticker::ARKK, None)
    //     .unwrap()
    //     .collect()
    //     .unwrap();
    // println!("{:#?}", api);

    // let ark = Ark::new(Source::Ark, Ticker::ARKK, None)?.collect()?;
    // println!("{:#?}", ark);

    // let ark = Ark::new(Source::Ark, Ticker::ARKVC, None)
    //     .unwrap()
    //     .collect()
    //     .unwrap();
    // println!("{:#?}", ark);
    Ok(())
}

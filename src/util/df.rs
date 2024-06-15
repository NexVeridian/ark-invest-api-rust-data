use anyhow::Error;
use polars::frame::DataFrame;
use polars::prelude::{IntoLazy, LazyFrame};

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
    pub fn collect(self) -> anyhow::Result<DataFrame, Error> {
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

#[allow(clippy::upper_case_acronyms, dead_code)]
pub trait DFS {
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

use anyhow::Error;
use polars::frame::DataFrame;
use polars::prelude::{IntoLazy, LazyFrame};

#[derive(Clone)]
pub enum DF {
    LazyFrame(Box<LazyFrame>),
    DataFrame(Box<DataFrame>),
}

impl From<LazyFrame> for DF {
    fn from(lf: LazyFrame) -> Self {
        Self::LazyFrame(Box::new(lf))
    }
}

impl From<DataFrame> for DF {
    fn from(df: DataFrame) -> Self {
        Self::DataFrame(Box::new(df))
    }
}

impl DF {
    pub fn collect(self) -> anyhow::Result<DataFrame, Error> {
        match self {
            Self::LazyFrame(x) => Ok(x.collect()?),
            Self::DataFrame(x) => Ok(*x),
        }
    }
    pub fn lazy(self) -> LazyFrame {
        match self {
            Self::LazyFrame(x) => *x,
            Self::DataFrame(x) => x.lazy(),
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

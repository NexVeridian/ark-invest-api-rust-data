
use anyhow::{Error, Result};
use polars::prelude::*;
use strum::IntoEnumIterator;
use strum_macros::EnumIter;

use crate::util::df::DF;

#[allow(clippy::upper_case_acronyms, non_camel_case_types)]
#[derive(Debug, strum_macros::Display, EnumIter, Clone, Copy, PartialEq)]
pub enum Ticker {
    ARKW,
    MKFG,
    CASH_USD,
}

impl Ticker {
    pub fn all(mut df: DF) -> Result<DF, Error> {
        for ticker in Ticker::iter() {
            df = ticker.format(df)?;
        }
        Ok(df)
    }

    pub fn format(&self, df: DF) -> Result<DF, Error> {
        match self {
            Ticker::ARKW => Self::arkw(df),
            Ticker::MKFG => Self::mkfg(df),
            Ticker::CASH_USD => Self::cash_usd(df),
        }
    }

    fn arkw(df: DF) -> Result<DF, Error> {
        let mut df = df.collect()?;

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

        Ok(df.into())
    }

    fn mkfg(df: DF) -> Result<DF, Error> {
        let mut df = df.collect()?;

        if let Ok(x) = df
            .clone()
            .lazy()
            .with_columns(vec![when(col("ticker").eq(lit("MARKFORGEDG")))
                .then(lit("MKFG"))
                .otherwise(col("ticker"))
                .alias("ticker")])
            .collect()
        {
            df = x;
        }

        Ok(df.into())
    }

    fn cash_usd(df: DF) -> Result<DF, Error> {
        let mut df = df.collect()?;

        let exprs = |company: &str| -> Vec<Expr> {
            vec![
                when(col("company").eq(lit(company)))
                    .then(lit("CASH USD"))
                    .otherwise(col("ticker"))
                    .alias("ticker"),
                when(col("company").eq(lit(company)))
                    .then(lit("CASH USD"))
                    .otherwise(col("company"))
                    .alias("company"),
            ]
        };

        if let Ok(x) = df
            .clone()
            .lazy()
            .with_columns(exprs("Cash & Cash Equivalents"))
            .with_columns(exprs("GOLDMAN FS TRSY OBLIG INST 468"))
            .collect()
        {
            df = x;
        }

        Ok(df.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_utils::*;
    use pretty_assertions::assert_eq;
    use rstest::rstest;

    #[rstest]
    #[case::mkfg(
		Ticker::MKFG,
		defualt_df(
            &[Some("MKFG"), Some("MARKFORGEDG")],
            &[Some("MARKFORGEDG"), Some("MARKFORGEDG")],
        )?,
		defualt_df(
			&[Some("MKFG"), Some("MKFG")],
			&[Some("MARKFORGEDG"), Some("MARKFORGEDG")]
		)?,
	)]
    #[case::arkb(
		Ticker::ARKW,
		defualt_df(
            &[None::<&str>, Some("ARKB"), Some("ARKB"), Some("ARKB")],
            &[
                Some("ARK BITCOIN ETF HOLDCO (ARKW)"),
                Some("ARK BITCOIN ETF HOLDCO (ARKW)"),
                Some("ARK BITCOIN ETF HOLDCO (ARKF)"),
                Some("ARKB"),
            ],
        )?,
		defualt_df(
			&[Some("ARKB"), Some("ARKB"), Some("ARKB"), Some("ARKB")],
			&[Some("ARKB"), Some("ARKB"), Some("ARKB"), Some("ARKB")],
		)?,
	)]
    #[case::cash_usd(
		Ticker::CASH_USD,
		defualt_df(
			&[None::<&str>, None::<&str>],
			&[Some("Cash & Cash Equivalents"), Some("GOLDMAN FS TRSY OBLIG INST 468")],
		)?,
		defualt_df(
			&[Some("CASH USD"), Some("CASH USD")],
			&[Some("CASH USD"), Some("CASH USD")],
		)?,
	)]
    fn matrix(
        #[case] ticker: Ticker,
        #[case] input: DataFrame,
        #[case] expected: DataFrame,
    ) -> Result<(), Error> {
        let test_df = input;
        let formatted_df = ticker.format(test_df.into())?.collect()?;
        assert_eq!(formatted_df, expected,);
        Ok(())
    }
}

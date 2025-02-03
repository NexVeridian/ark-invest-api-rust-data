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
    XYZ,
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
            Ticker::XYZ => Self::xyz(df),
            Ticker::CASH_USD => Self::cash_usd(df),
        }
    }

    fn get_expr(target_col: &str, current: &str, new: &str) -> Vec<Expr> {
        match target_col {
            "company" => vec![
                when(col(target_col).eq(lit(current)))
                    .then(lit(new))
                    .otherwise(col("ticker"))
                    .alias("ticker"),
                when(col(target_col).eq(lit(current)))
                    .then(lit(new))
                    .otherwise(col("company"))
                    .alias("company"),
            ],
            "ticker" => vec![
                when(col(target_col).eq(lit(current)))
                    .then(lit(new))
                    .otherwise(col("company"))
                    .alias("company"),
                when(col(target_col).eq(lit(current)))
                    .then(lit(new))
                    .otherwise(col("ticker"))
                    .alias("ticker"),
            ],
            _ => panic!("Invalid target column"),
        }
    }

    fn arkw(df: DF) -> Result<DF, Error> {
        let mut df = df.collect()?;

        if let Ok(x) = df
            .clone()
            .lazy()
            .with_columns(Self::get_expr(
                "company",
                "ARK BITCOIN ETF HOLDCO (ARKW)",
                "ARKB",
            ))
            .with_columns(Self::get_expr(
                "company",
                "ARK BITCOIN ETF HOLDCO (ARKF)",
                "ARKB",
            ))
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
            .with_columns(vec![when(col("company").eq(lit("MARKFORGEDG")))
                .then(lit("MKFG"))
                .otherwise(col("ticker"))
                .alias("ticker")])
            .collect()
        {
            df = x;
        }

        Ok(df.into())
    }

    fn xyz(df: DF) -> Result<DF, Error> {
        let mut df = df.collect()?;

        if let Ok(x) = df
            .clone()
            .lazy()
            .with_columns(vec![when(col("company").eq(lit("BLOCK")))
                .then(lit("XYZ"))
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

        if let Ok(x) = df
            .clone()
            .lazy()
            .with_columns(Self::get_expr(
                "company",
                "Cash & Cash Equivalents",
                "CASH_USD",
            ))
            .with_columns(Self::get_expr(
                "company",
                "CASH & CASH EQUIVALENTS",
                "CASH_USD",
            ))
            .with_columns(Self::get_expr(
                "company",
                "GOLDMAN FS TRSY OBLIG INST 468",
                "CASH_USD",
            ))
            .with_columns(Self::get_expr("company", "Cash & Other", "CASH_USD"))
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
            &[Some("MKFG"), None::<&str>],
            &[Some("MARKFORGEDG"), Some("MARKFORGEDG")],
        )?,
		defualt_df(
			&[Some("MKFG"), Some("MKFG")],
			&[Some("MARKFORGEDG"), Some("MARKFORGEDG")]
		)?,
	)]
    #[case::xyz(
        Ticker::XYZ,
        defualt_df(
            &[Some("SQ"), Some("YXZ")],
            &[Some("BLOCK"), Some("BLOCK")],
        )?,
        defualt_df(
            &[Some("XYZ"), Some("XYZ")],
            &[Some("BLOCK"), Some("BLOCK")],
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
			&[None::<&str>, None::<&str>, None::<&str>, Some("CASH&Other")],
			&[Some("Cash & Cash Equivalents"), Some("CASH & CASH EQUIVALENTS"), Some("GOLDMAN FS TRSY OBLIG INST 468"), Some("Cash & Other")],
		)?,
		defualt_df(
			&[Some("CASH_USD"), Some("CASH_USD"), Some("CASH_USD"), Some("CASH_USD")],
			&[Some("CASH_USD"), Some("CASH_USD"), Some("CASH_USD"), Some("CASH_USD")],
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

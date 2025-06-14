use anyhow::{Error, Result};
use polars::prelude::*;
use strum::IntoEnumIterator;
use strum_macros::EnumIter;

use crate::util::df::DF;

#[allow(clippy::upper_case_acronyms, non_camel_case_types)]
#[derive(Debug, strum_macros::Display, EnumIter, Clone, Copy, PartialEq, Eq)]
pub enum Ticker {
    ARKW,
    CRLC,
    CRWV,
    DKNG,
    ETOR,
    MKFG,
    LUNR,
    XYZ,
    CASH_USD,
    TSM,
    RKLB,
}

impl Ticker {
    pub fn all(mut df: DF) -> Result<DF, Error> {
        for ticker in Self::iter() {
            df = ticker.format(df)?;
        }
        Ok(df)
    }

    pub fn format(&self, df: DF) -> Result<DF, Error> {
        match self {
            Self::ARKW => Self::arkw(df),
            Self::CRLC => Self::crlc(df),
            Self::CRWV => Self::crwv(df),
            Self::DKNG => Self::dkng(df),
            Self::ETOR => Self::etor(df),
            Self::MKFG => Self::mkfg(df),
            Self::LUNR => Self::lunr(df),
            Self::XYZ => Self::xyz(df),
            Self::CASH_USD => Self::cash_usd(df),
            Self::TSM => Self::tsm(df),
            Self::RKLB => Self::rklb(df),
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

    fn crlc(df: DF) -> Result<DF, Error> {
        let mut df = df.collect()?;

        if let Ok(x) = df
            .clone()
            .lazy()
            .with_columns(vec![when(col("company").eq(lit("CIRCLE INTERNET GROUP")))
                .then(lit("CRLC"))
                .otherwise(col("ticker"))
                .alias("ticker")])
            .collect()
        {
            df = x;
        }

        Ok(df.into())
    }

    fn crwv(df: DF) -> Result<DF, Error> {
        let mut df = df.collect()?;

        if let Ok(x) = df
            .clone()
            .lazy()
            .with_columns(vec![when(col("company").eq(lit("COREWEAVE")))
                .then(lit("CRWV"))
                .otherwise(col("ticker"))
                .alias("ticker")])
            .collect()
        {
            df = x;
        }

        Ok(df.into())
    }

    fn dkng(df: DF) -> Result<DF, Error> {
        let mut df = df.collect()?;

        if let Ok(x) = df
            .clone()
            .lazy()
            .with_columns(vec![when(col("company").eq(lit("AFTKINGS")))
                .then(lit("DRAFTKINGS"))
                .otherwise(col("company"))
                .alias("company")])
            .collect()
        {
            df = x;
        }

        Ok(df.into())
    }

    fn etor(df: DF) -> Result<DF, Error> {
        let mut df = df.collect()?;

        if let Ok(x) = df
            .clone()
            .lazy()
            .with_columns(vec![when(col("company").eq(lit("ETORO GROUP")))
                .then(lit("ETOR"))
                .otherwise(col("ticker"))
                .alias("ticker")])
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

    fn lunr(df: DF) -> Result<DF, Error> {
        let mut df = df.collect()?;

        if let Ok(x) = df
            .clone()
            .lazy()
            .with_columns(vec![when(col("company").eq(lit("INTUITIVE MACHINES")))
                .then(lit("LUNR"))
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
            .with_columns(vec![when(col("company").eq(lit("Block")))
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

    fn tsm(df: DF) -> Result<DF, Error> {
        let mut df = df.collect()?;

        if let Ok(x) = df
            .clone()
            .lazy()
            .with_columns(vec![when(col("company").eq(lit("TAIWANMICONDUCTORSP")))
                .then(lit("TMSC"))
                .otherwise(col("company"))
                .alias("company")])
            .collect()
        {
            df = x;
        }

        Ok(df.into())
    }

    fn rklb(df: DF) -> Result<DF, Error> {
        let mut df = df.collect()?;

        if let Ok(x) = df
            .clone()
            .lazy()
            .with_columns(vec![
                when(col("company").eq(lit("ROCKET LAB")))
                    .then(lit("RKLB"))
                    .otherwise(col("ticker"))
                    .alias("ticker"),
                when(col("company").eq(lit("ROCKET LAB USA")))
                    .then(lit("ROCKET LAB"))
                    .otherwise(col("company"))
                    .alias("company")
            ])
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
    #[case::crlc(
        Ticker::CRLC,
        defualt_df(
            &[Some("CRLC"), None::<&str>],
            &[Some("CIRCLE INTERNET GROUP"), Some("CIRCLE INTERNET GROUP")],
        )?,
        defualt_df(
            &[Some("CRLC"), Some("CRLC")],
            &[Some("CIRCLE INTERNET GROUP"), Some("CIRCLE INTERNET GROUP")]
        )?,
    )]
    #[case::crwv(
        Ticker::CRWV,
        defualt_df(
            &[Some("CRWV"), None::<&str>],
            &[Some("COREWEAVE"), Some("COREWEAVE")],
        )?,
        defualt_df(
            &[Some("CRWV"), Some("CRWV")],
            &[Some("COREWEAVE"), Some("COREWEAVE")]
        )?,
    )]
    #[case::etor(
        Ticker::ETOR,
        defualt_df(
            &[Some("ETOR"), None::<&str>],
            &[Some("ETORO GROUP"), Some("ETORO GROUP")],
        )?,
        defualt_df(
            &[Some("ETOR"), Some("ETOR")],
            &[Some("ETORO GROUP"), Some("ETORO GROUP")]
        )?,
    )]
    #[case::dkng(
        Ticker::DKNG,
        defualt_df(
            &[Some("DKNG")],
            &[Some("AFTKINGS")],
        )?,
        defualt_df(
            &[Some("DKNG")],
            &[Some("DRAFTKINGS")]
        )?,
    )]
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
    #[case::lunr(
		Ticker::LUNR,
		defualt_df(
            &[Some("LUNR"), None::<&str>],
            &[Some("INTUITIVE MACHINES"), Some("INTUITIVE MACHINES")]
        )?,
		defualt_df(
			&[Some("LUNR"), Some("LUNR")],
			&[Some("INTUITIVE MACHINES"), Some("INTUITIVE MACHINES")]
		)?,
	)]
    #[case::xyz(
        Ticker::XYZ,
        defualt_df(
            &[Some("SQ"), Some("SQ"), Some("XYZ"), Some("XYZ")],
            &[Some("Block"), Some("BLOCK"), Some("Block"), Some("BLOCK")],
        )?,
        defualt_df(
            &[Some("XYZ"), Some("XYZ"), Some("XYZ"), Some("XYZ")],
            &[Some("Block"), Some("BLOCK"), Some("Block"), Some("BLOCK")],
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
    #[case::tsm(
        Ticker::TSM,
        defualt_df(
            &[Some("TSM")],
            &[Some("TAIWANMICONDUCTORSP")],
        )?,
        defualt_df(
            &[Some("TSM")],
            &[Some("TMSC")]
        )?,
    )]
    #[case::rklb(
        Ticker::RKLB,
        defualt_df(
            &[Some("RKLB"), Some("RKLB"), None::<&str>],
            &[Some("ROCKET LAB"), Some("ROCKET LAB USA"), Some("ROCKET LAB")],
        )?,
        defualt_df(
            &[Some("RKLB"), Some("RKLB"), Some("RKLB")],
            &[Some("ROCKET LAB"), Some("ROCKET LAB"), Some("ROCKET LAB")]
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

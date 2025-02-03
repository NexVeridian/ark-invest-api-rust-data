use strum_macros::EnumIter;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum DataSource {
    ArkVenture,
    Ark,
    Shares21,
    ArkEurope,
    Rize,
}

#[allow(clippy::upper_case_acronyms, non_camel_case_types)]
#[derive(Debug, Default, strum_macros::Display, EnumIter, Clone, Copy, PartialEq, Eq)]
pub enum Ticker {
    ARKVX,

    ARKF,
    ARKG,
    #[default]
    ARKK,
    ARKQ,
    ARKW,
    ARKX,

    ARKA,
    ARKZ,
    ARKC,
    ARKD,
    ARKY,
    ARKB,

    PRNT,
    IZRL,

    EUROPE_ARKI,
    EUROPE_ARKG,
    EUROPE_ARKK,

    CYBR,
    CYCL,
    FOOD,
    LIFE,
    LUSA,
    NFRA,
    PMNT,
}

impl Ticker {
    pub const fn value(&self) -> &str {
        match *self {
            Self::ARKVX => "ARK_VENTURE_FUND_ARKVX_HOLDINGS.csv",

            Self::ARKF => "FINTECH_INNOVATION",
            Self::ARKG => "GENOMIC_REVOLUTION",
            Self::ARKK => "INNOVATION",
            Self::ARKQ => "AUTONOMOUS_TECH._&_ROBOTICS",
            Self::ARKW => "NEXT_GENERATION_INTERNET",
            Self::ARKX => "SPACE_EXPLORATION_&_INNOVATION",

            Self::ARKA => "ARKA",
            Self::ARKZ => "ARKZ",
            Self::ARKC => "ARKC",
            Self::ARKD => "ARKD",
            Self::ARKY => "ARKY",
            Self::ARKB => "21SHARES_BITCOIN",

            Self::PRNT => "THE_3D_PRINTING",
            Self::IZRL => "ISRAEL_INNOVATIVE_TECHNOLOGY",

            Self::EUROPE_ARKI => "artificial-intelligence-robotics",
            Self::EUROPE_ARKG => "genomic-revolution",
            Self::EUROPE_ARKK => "innovation",

            Self::CYBR => "cybersecurity-and-data-privacy",
            Self::CYCL => "circular-economy-enablers",
            Self::FOOD => "sustainable-future-of-food",
            Self::LIFE => "environmental-impact-100",
            Self::LUSA => "usa-environmental-impact",
            Self::NFRA => "global-sustainable-infrastructure",
            Self::PMNT => "digital-payments-economy",
        }
    }

    pub const fn data_source(&self) -> DataSource {
        match *self {
            Self::ARKVX => DataSource::ArkVenture,

            Self::ARKF | Self::ARKG | Self::ARKK | Self::ARKQ | Self::ARKW | Self::ARKX => {
                DataSource::Ark
            }

            Self::ARKA | Self::ARKZ | Self::ARKC | Self::ARKD | Self::ARKY | Self::ARKB => {
                DataSource::Shares21
            }

            Self::PRNT | Self::IZRL => DataSource::Ark,

            Self::EUROPE_ARKI | Self::EUROPE_ARKG | Self::EUROPE_ARKK => DataSource::ArkEurope,

            Self::CYBR
            | Self::CYCL
            | Self::FOOD
            | Self::LIFE
            | Self::LUSA
            | Self::NFRA
            | Self::PMNT => DataSource::Rize,
        }
    }

    pub fn get_url(&self) -> String {
        match self.data_source() {
            DataSource::ArkVenture => format!("https://assets.ark-funds.com/fund-documents/funds-etf-csv/{}", self.value()),
            DataSource::Ark => format!("https://assets.ark-funds.com/fund-documents/funds-etf-csv/ARK_{}_ETF_{}_HOLDINGS.csv", self.value(), self),
            DataSource::Shares21 => format!("https://cdn.21shares-funds.com/uploads/fund-documents/us-bank/holdings/product/current/{}-Export.csv", self.value()),
            DataSource::ArkEurope | DataSource::Rize => format!("https://europe.ark-funds.com/funds/{}/full-fund-holdings-download/", self.value()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;
    use rstest::rstest;

    #[rstest]
    #[case(Ticker::ARKVX, "https://assets.ark-funds.com/fund-documents/funds-etf-csv/ARK_VENTURE_FUND_ARKVX_HOLDINGS.csv")]
    #[case(Ticker::ARKK, "https://assets.ark-funds.com/fund-documents/funds-etf-csv/ARK_INNOVATION_ETF_ARKK_HOLDINGS.csv")]
    #[case(Ticker::ARKA, "https://cdn.21shares-funds.com/uploads/fund-documents/us-bank/holdings/product/current/ARKA-Export.csv")]
    #[case(Ticker::EUROPE_ARKI, "https://europe.ark-funds.com/funds/artificial-intelligence-robotics/full-fund-holdings-download/")]
    #[case(Ticker::CYBR, "https://europe.ark-funds.com/funds/cybersecurity-and-data-privacy/full-fund-holdings-download/")]
    fn get_url(#[case] input: Ticker, #[case] expected: String) {
        assert_eq!(input.get_url(), expected)
    }
}

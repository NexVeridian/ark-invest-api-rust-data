use strum_macros::EnumIter;

#[derive(Clone, Copy, PartialEq)]
pub enum DataSource {
    ArkVenture,
    Ark,
    Shares21,
    ArkEurope,
    Rize,
}

#[allow(clippy::upper_case_acronyms, non_camel_case_types)]
#[derive(Debug, Default, strum_macros::Display, EnumIter, Clone, Copy, PartialEq)]
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
    pub fn value(&self) -> &str {
        match *self {
            Ticker::ARKVX => "ARK_VENTURE_FUND_ARKVX_HOLDINGS.csv",

            Ticker::ARKF => "FINTECH_INNOVATION",
            Ticker::ARKG => "GENOMIC_REVOLUTION",
            Ticker::ARKK => "INNOVATION",
            Ticker::ARKQ => "AUTONOMOUS_TECH._&_ROBOTICS",
            Ticker::ARKW => "NEXT_GENERATION_INTERNET",
            Ticker::ARKX => "SPACE_EXPLORATION_&_INNOVATION",

            Ticker::ARKA => "ARKA",
            Ticker::ARKZ => "ARKZ",
            Ticker::ARKC => "ARKC",
            Ticker::ARKD => "ARKD",
            Ticker::ARKY => "ARKY",
            Ticker::ARKB => "21SHARES_BITCOIN",

            Ticker::PRNT => "THE_3D_PRINTING",
            Ticker::IZRL => "ISRAEL_INNOVATIVE_TECHNOLOGY",

            Ticker::EUROPE_ARKI => "artificial-intelligence-robotics",
            Ticker::EUROPE_ARKG => "genomic-revolution",
            Ticker::EUROPE_ARKK => "innovation",

            Ticker::CYBR => "cybersecurity-and-data-privacy",
            Ticker::CYCL => "circular-economy-enablers",
            Ticker::FOOD => "sustainable-future-of-food",
            Ticker::LIFE => "environmental-impact-100",
            Ticker::LUSA => "usa-environmental-impact",
            Ticker::NFRA => "global-sustainable-infrastructure",
            Ticker::PMNT => "digital-payments-economy",
        }
    }

    pub fn data_source(&self) -> DataSource {
        match *self {
            Ticker::ARKVX => DataSource::ArkVenture,

            Ticker::ARKF
            | Ticker::ARKG
            | Ticker::ARKK
            | Ticker::ARKQ
            | Ticker::ARKW
            | Ticker::ARKX => DataSource::Ark,

            Ticker::ARKA
            | Ticker::ARKZ
            | Ticker::ARKC
            | Ticker::ARKD
            | Ticker::ARKY
            | Ticker::ARKB => DataSource::Shares21,

            Ticker::PRNT | Ticker::IZRL => DataSource::Ark,

            Ticker::EUROPE_ARKI | Ticker::EUROPE_ARKG | Ticker::EUROPE_ARKK => {
                DataSource::ArkEurope
            }

            Ticker::CYBR
            | Ticker::CYCL
            | Ticker::FOOD
            | Ticker::LIFE
            | Ticker::LUSA
            | Ticker::NFRA
            | Ticker::PMNT => DataSource::Rize,
        }
    }
}

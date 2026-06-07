#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PathEndReason {
    HorizonReached,
    Delisted,
    Halted,
    Merged,
    EndOfData,
}

impl PathEndReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::HorizonReached => "horizon_reached",
            Self::Delisted => "delisted",
            Self::Halted => "halted",
            Self::Merged => "merged",
            Self::EndOfData => "end_of_data",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TerminalValueSource {
    LastBarHorizon,
    LastBarPreDelist,
    LastBarHalted,
    LastBarUnderDeal,
    LastBarEndOfData,
}

impl TerminalValueSource {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::LastBarHorizon => "last_bar_horizon",
            Self::LastBarPreDelist => "last_bar_pre_delist",
            Self::LastBarHalted => "last_bar_halted",
            Self::LastBarUnderDeal => "last_bar_under_deal",
            Self::LastBarEndOfData => "last_bar_end_of_data",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Phase0ExitReason {
    BreakevenRule,
    OpenAtEnd,
}

impl Phase0ExitReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::BreakevenRule => "breakeven_rule",
            Self::OpenAtEnd => "open_at_end",
        }
    }
}

// =============================================================================
// Derived-layer enums.
//
// These three enums describe terminal/exit attributes of the **derived**
// `per_trade_summary` / `per_trade_bar_path` tables that the observation-pivot
// RFC (v6) demoted from primary engine outputs to a ~30-line projection over
// `forward_outcomes` + `forward_path_short` (RFC §16). The engine itself does
// **not** materialize these enums during the pivot's eight primary tables;
// they belong to the spec-compatibility derivation layer.
//
// Do not wire these into the daily_observation / forward_outcomes /
// forward_path_short writers. Terminal-event state on those primary tables
// lives in `terminal_event_type` (Utf8 enum string, RFC §9 last block).
// =============================================================================

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

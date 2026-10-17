//! Core types: market snapshots, events and transitions.

use std::collections::BTreeMap;

use chrono::NaiveDateTime;
use serde::{Deserialize, Serialize};

/// One dispatch interval as published in a DispatchIS file.
#[derive(Debug, Clone, Default)]
pub struct DispatchSnapshot {
    pub settlement: NaiveDateTime,
    pub regions: BTreeMap<String, RegionInterval>,
    pub constraints: Vec<ConstraintRow>,
    pub interconnectors: Vec<InterconnectorRow>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RegionInterval {
    pub rrp: f64,
    pub demand: Option<f64>,
    pub available_gen: Option<f64>,
    pub net_interchange: Option<f64>,
}

/// RHS and LHS are kept for the replay engine; the detector uses marginal and violation.
#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct ConstraintRow {
    pub id: String,
    pub rhs: Option<f64>,
    pub lhs: Option<f64>,
    pub marginal: f64,
    pub violation: f64,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct InterconnectorRow {
    pub id: String,
    pub flow: f64,
    pub export_limit: Option<f64>,
    pub import_limit: Option<f64>,
    pub marginal: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Severity {
    Floor,
    High,
    Extreme,
    Cap,
}

impl Severity {
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "floor" => Some(Self::Floor),
            "high" => Some(Self::High),
            "extreme" => Some(Self::Extreme),
            "cap" => Some(Self::Cap),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Floor => "floor",
            Self::High => "high",
            Self::Extreme => "extreme",
            Self::Cap => "cap",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Floor => "near the price floor",
            Self::High => "above $5,000/MWh",
            Self::Extreme => "above $10,000/MWh",
            Self::Cap => "at the market price cap",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    HighPrice,
    LowPrice,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    Developing,
    Confirmed,
    Closed,
    Discarded,
}

impl Status {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Developing => "developing",
            Self::Confirmed => "confirmed",
            Self::Closed => "closed",
            Self::Discarded => "discarded",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Flag {
    pub kind: String,
    pub first_interval: String,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConstraintAgg {
    pub id: String,
    pub binding_intervals: u32,
    pub max_abs_marginal: f64,
    pub violated_intervals: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct InterconnectorAgg {
    pub id: String,
    pub intervals_at_limit: u32,
    pub direction: String,
}

/// A market event. Times are market time ISO strings with +10:00.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Event {
    pub id: String,
    pub region: String,
    pub kind: EventKind,
    pub status: Status,
    pub severity: Severity,
    pub trading_day: String,
    pub start: String,
    pub last_active: String,
    pub end: Option<String>,
    pub peak_price: f64,
    pub peak_interval: String,
    pub intervals_active: u32,
    pub intervals_above_high: u32,
    pub intervals_above_extreme: u32,
    pub mean_price: f64,
    pub min_price: f64,
    pub flags: Vec<Flag>,
    pub constraints: Vec<ConstraintAgg>,
    pub interconnectors_at_limit: Vec<InterconnectorAgg>,
    pub demand_start: Option<f64>,
    pub demand_peak: Option<f64>,
    /// live (outcomes only), next_day (bids and unit data public), archive (MMSDM).
    pub data_tier: String,
    pub updated_at: String,
}

impl Event {
    pub fn has_flag(&self, kind: &str) -> bool {
        self.flags.iter().any(|f| f.kind == kind)
    }

    pub fn start_time(&self) -> Option<NaiveDateTime> {
        crate::time::parse_market(&self.start)
    }

    pub fn end_time(&self) -> Option<NaiveDateTime> {
        self.end.as_deref().and_then(crate::time::parse_market)
    }

    pub fn last_active_time(&self) -> Option<NaiveDateTime> {
        crate::time::parse_market(&self.last_active)
    }
}

pub const TRANSITIONS: &[&str] =
    &["opened", "confirmed", "escalated", "flagged", "closed", "discarded", "data_ready"];

#[derive(Debug, Clone, PartialEq)]
pub enum TransitionKind {
    Opened,
    Confirmed,
    Escalated { from: Severity, to: Severity },
    Flagged { flag: String },
    Closed,
    Discarded,
    DataReady { tier: String },
}

impl TransitionKind {
    pub fn name(&self) -> &'static str {
        match self {
            Self::Opened => "opened",
            Self::Confirmed => "confirmed",
            Self::Escalated { .. } => "escalated",
            Self::Flagged { .. } => "flagged",
            Self::Closed => "closed",
            Self::Discarded => "discarded",
            Self::DataReady { .. } => "data_ready",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Transition {
    pub kind: TransitionKind,
    pub event: Event,
}

/// What subscribers receive: one item per transition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotificationItem {
    pub seq: i64,
    pub event_id: String,
    pub transition: String,
    pub title: String,
    pub summary: String,
    pub url: String,
    pub created_at: String,
    pub public: bool,
    pub event: Event,
}

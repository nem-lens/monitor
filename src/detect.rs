//! Event detector. Pure and deterministic: feed it dispatch snapshots in time
//! order and it returns transitions. State is serialisable so the service can
//! restart mid-event without losing it.

use std::collections::{BTreeMap, VecDeque};

use chrono::{NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::config::DetectCfg;
use crate::model::*;
use crate::time::{iso_market, trading_day};

const SERIES_LEN: usize = 48;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Point {
    t: NaiveDateTime,
    price: f64,
    demand: Option<f64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Draft {
    event: Event,
    /// Prices since the event opened, including the quiet tail.
    prices: Vec<(NaiveDateTime, f64)>,
    quiet: u32,
    /// Every binding constraint seen while active; the event shows the top 10.
    /// Ranking only a truncated list dropped constraints that later caught up.
    #[serde(default)]
    constraints: Vec<ConstraintAgg>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct RegionState {
    series: VecDeque<Point>,
    high: Option<Draft>,
    low: Option<Draft>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DetectorState {
    regions: BTreeMap<String, RegionState>,
    last_settlement: Option<NaiveDateTime>,
}

pub struct Detector {
    cfg: DetectCfg,
    state: DetectorState,
}

#[derive(Clone, Copy, PartialEq)]
enum Side {
    High,
    Low,
}

impl Detector {
    pub fn new(cfg: DetectCfg) -> Self {
        Self {
            cfg,
            state: DetectorState::default(),
        }
    }

    pub fn restore(cfg: DetectCfg, json: &str) -> anyhow::Result<Self> {
        Ok(Self {
            cfg,
            state: serde_json::from_str(json)?,
        })
    }

    pub fn state_json(&self) -> String {
        serde_json::to_string(&self.state).expect("state serialises")
    }

    /// Set the market price cap the `cap` severity is measured against
    /// (it changes each financial year; historical scans follow it).
    pub fn set_market_price_cap(&mut self, cap: f64) {
        self.cfg.market_price_cap = cap;
    }

    pub fn last_settlement(&self) -> Option<NaiveDateTime> {
        self.state.last_settlement
    }

    pub fn open_events(&self) -> Vec<Event> {
        self.state
            .regions
            .values()
            .flat_map(|r| r.high.iter().chain(r.low.iter()))
            .map(|d| d.event.clone())
            .collect()
    }

    fn severity(&self, p: f64) -> Severity {
        if p >= self.cfg.market_price_cap * self.cfg.cap_fraction {
            Severity::Cap
        } else if p >= self.cfg.extreme_price {
            Severity::Extreme
        } else {
            Severity::High
        }
    }

    /// Process one interval. Snapshots at or before the last processed interval are ignored.
    pub fn ingest(&mut self, snap: &DispatchSnapshot) -> Vec<Transition> {
        if let Some(last) = self.state.last_settlement {
            if snap.settlement <= last {
                return Vec::new();
            }
        }
        self.state.last_settlement = Some(snap.settlement);

        let prices: Vec<(&String, f64)> = snap.regions.iter().map(|(r, v)| (r, v.rrp)).collect();
        let max = prices.iter().map(|p| p.1).fold(f64::MIN, f64::max);
        let min = prices.iter().map(|p| p.1).fold(f64::MAX, f64::min);
        let min_region = prices
            .iter()
            .find(|p| p.1 == min)
            .map(|p| p.0.clone())
            .unwrap_or_default();

        let mut out = Vec::new();
        let regions = self.cfg.regions.clone();
        for region in &regions {
            let Some(ri) = snap.regions.get(region) else {
                continue;
            };
            let mut rs = self.state.regions.remove(region).unwrap_or_default();
            rs.series.push_back(Point {
                t: snap.settlement,
                price: ri.rrp,
                demand: ri.demand,
            });
            while rs.series.len() > SERIES_LEN {
                rs.series.pop_front();
            }
            let sep = (ri.rrp - min, min_region.clone(), min);
            out.extend(self.step(Side::High, region, &mut rs, snap, ri, sep, max));
            out.extend(self.step(Side::Low, region, &mut rs, snap, ri, sep_none(), max));
            self.state.regions.insert(region.clone(), rs);
        }
        out
    }

    #[allow(clippy::too_many_arguments)]
    fn step(
        &self,
        side: Side,
        region: &str,
        rs: &mut RegionState,
        snap: &DispatchSnapshot,
        ri: &RegionInterval,
        sep: (f64, String, f64),
        _max: f64,
    ) -> Vec<Transition> {
        let c = &self.cfg;
        let t = snap.settlement;
        let p = ri.rrp;
        let triggers = match side {
            Side::High => p >= c.high_price,
            Side::Low => p <= c.floor_price,
        };
        let active = match side {
            Side::High => p >= c.close_price,
            Side::Low => p <= 0.0,
        };
        let existing = match side {
            Side::High => rs.high.take(),
            Side::Low => rs.low.take(),
        };
        let mut out = Vec::new();

        let mut draft = match existing {
            Some(d) => d,
            None if triggers => {
                let ev = Event {
                    id: format!(
                        "{}-{}-{}",
                        t.format("%Y%m%d%H%M"),
                        region,
                        if side == Side::High { "high" } else { "low" }
                    ),
                    region: region.to_string(),
                    kind: if side == Side::High {
                        EventKind::HighPrice
                    } else {
                        EventKind::LowPrice
                    },
                    status: Status::Developing,
                    severity: if side == Side::High {
                        self.severity(p)
                    } else {
                        Severity::Floor
                    },
                    trading_day: trading_day(t).to_string(),
                    start: iso_market(t),
                    last_active: iso_market(t),
                    end: None,
                    peak_price: p,
                    peak_interval: iso_market(t),
                    intervals_active: 0,
                    intervals_above_high: 0,
                    intervals_above_extreme: 0,
                    mean_price: p,
                    min_price: p,
                    flags: Vec::new(),
                    constraints: Vec::new(),
                    interconnectors_at_limit: Vec::new(),
                    demand_start: ri.demand,
                    demand_peak: ri.demand,
                    data_tier: "live".into(),
                    updated_at: now_iso(),
                };
                out.push(Transition {
                    kind: TransitionKind::Opened,
                    event: ev.clone(),
                });
                Draft {
                    event: ev,
                    prices: Vec::new(),
                    quiet: 0,
                    constraints: Vec::new(),
                }
            }
            None => return out,
        };

        draft.prices.push((t, p));
        let ev = &mut draft.event;
        let was_confirmed = ev.status == Status::Confirmed;

        if active {
            draft.quiet = 0;
            ev.last_active = iso_market(t);
            ev.intervals_active += 1;
            let extreme_price = match side {
                Side::High => p > ev.peak_price,
                Side::Low => p < ev.peak_price,
            };
            if extreme_price || ev.intervals_active == 1 {
                ev.peak_price = p;
                ev.peak_interval = iso_market(t);
            }
            if p >= c.high_price {
                ev.intervals_above_high += 1;
            }
            if p >= c.extreme_price {
                ev.intervals_above_extreme += 1;
            }
            if side == Side::Low && p <= c.floor_price {
                // For low events, count intervals at or below the floor threshold.
                ev.intervals_above_high += 1;
            }
            if let Some(d) = ri.demand {
                ev.demand_peak = Some(ev.demand_peak.map_or(d, |x| x.max(d)));
            }
            aggregate_network(&mut draft.constraints, ev, snap);
        } else {
            draft.quiet += 1;
        }

        // Mean and min over the active span (exclude the quiet tail).
        let last_active = ev.last_active_time().unwrap_or(t);
        let span: Vec<f64> = draft
            .prices
            .iter()
            .filter(|(pt, _)| *pt <= last_active)
            .map(|(_, v)| *v)
            .collect();
        if !span.is_empty() {
            ev.mean_price = span.iter().sum::<f64>() / span.len() as f64;
            ev.min_price = span.iter().cloned().fold(f64::MAX, f64::min);
        }

        // Confirmation.
        if ev.status == Status::Developing {
            let confirm = ev.intervals_above_high >= c.confirm_intervals
                || (side == Side::High && p >= c.confirm_immediately_price);
            if confirm {
                ev.status = Status::Confirmed;
            }
        }

        // Escalation (high side only).
        if side == Side::High && active {
            let s = self.severity(p);
            if s > ev.severity {
                let from = ev.severity;
                ev.severity = s;
                if was_confirmed {
                    out.push(Transition {
                        kind: TransitionKind::Escalated { from, to: s },
                        event: ev.clone(),
                    });
                }
            }
        }

        // Flags (high side).
        let mut new_public_flag = None;
        if side == Side::High {
            if !ev.has_flag("whiplash") {
                if let Some(detail) = whiplash(&rs_series_prices(&rs.series, c.whiplash_window), c)
                {
                    ev.flags.push(Flag {
                        kind: "whiplash".into(),
                        first_interval: iso_market(t),
                        detail,
                    });
                    new_public_flag = Some("whiplash".to_string());
                }
            }
            if !ev.has_flag("price_separation") && active && sep.0 >= c.separation_price {
                ev.flags.push(Flag {
                    kind: "price_separation".into(),
                    first_interval: iso_market(t),
                    detail: format!(
                        "{} {} vs {} {}/MWh",
                        region,
                        dollars(p),
                        sep.1,
                        dollars(sep.2)
                    ),
                });
            }
            if !ev.has_flag("demand_ramp") {
                if let Some(ramp) = demand_ramp(&rs.series, c.demand_ramp_window) {
                    if ramp >= c.demand_ramp_mw {
                        ev.flags.push(Flag {
                            kind: "demand_ramp".into(),
                            first_interval: iso_market(t),
                            detail: format!("demand up {:.0} MW within an hour", ramp),
                        });
                    }
                }
            }
        }

        ev.updated_at = now_iso();

        if !was_confirmed && ev.status == Status::Confirmed {
            out.push(Transition {
                kind: TransitionKind::Confirmed,
                event: ev.clone(),
            });
        } else if was_confirmed {
            if let Some(flag) = new_public_flag {
                out.push(Transition {
                    kind: TransitionKind::Flagged { flag },
                    event: ev.clone(),
                });
            }
        }

        // Closing.
        if draft.quiet >= c.quiet_intervals {
            let ev = &mut draft.event;
            ev.end = Some(ev.last_active.clone());
            ev.updated_at = now_iso();
            if ev.status == Status::Confirmed {
                ev.status = Status::Closed;
                out.push(Transition {
                    kind: TransitionKind::Closed,
                    event: ev.clone(),
                });
            } else {
                ev.status = Status::Discarded;
                out.push(Transition {
                    kind: TransitionKind::Discarded,
                    event: ev.clone(),
                });
            }
        } else {
            match side {
                Side::High => rs.high = Some(draft),
                Side::Low => rs.low = Some(draft),
            }
        }
        out
    }
}

fn sep_none() -> (f64, String, f64) {
    (0.0, String::new(), 0.0)
}

fn now_iso() -> String {
    Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

fn rs_series_prices(series: &VecDeque<Point>, window: usize) -> Vec<f64> {
    let n = series.len();
    series
        .iter()
        .skip(n.saturating_sub(window))
        .map(|p| p.price)
        .collect()
}

/// Count direction reversals between consecutive large price moves: the
/// "hog cycle" signature of lagged price response.
pub fn whiplash(prices: &[f64], c: &DetectCfg) -> Option<String> {
    if prices.len() < 3 {
        return None;
    }
    let diffs: Vec<f64> = prices.windows(2).map(|w| w[1] - w[0]).collect();
    let reversals = diffs
        .windows(2)
        .filter(|d| {
            d[0] * d[1] < 0.0 && d[0].abs() >= c.whiplash_step && d[1].abs() >= c.whiplash_step
        })
        .count();
    if reversals >= c.whiplash_reversals {
        let lo = prices.iter().cloned().fold(f64::MAX, f64::min);
        let hi = prices.iter().cloned().fold(f64::MIN, f64::max);
        Some(format!(
            "{} large reversals within {} intervals, swinging between {} and {}/MWh",
            reversals,
            prices.len(),
            dollars(lo),
            dollars(hi)
        ))
    } else {
        None
    }
}

fn demand_ramp(series: &VecDeque<Point>, window: usize) -> Option<f64> {
    let n = series.len();
    let recent: Vec<f64> = series
        .iter()
        .skip(n.saturating_sub(window))
        .filter_map(|p| p.demand)
        .collect();
    let last = *recent.last()?;
    let min = recent.iter().cloned().fold(f64::MAX, f64::min);
    Some(last - min)
}

fn aggregate_network(all: &mut Vec<ConstraintAgg>, ev: &mut Event, snap: &DispatchSnapshot) {
    if all.is_empty() {
        // State saved before the full list existed: continue from the event's.
        all.clone_from(&ev.constraints);
    }
    for c in &snap.constraints {
        let binding = c.marginal.abs() > 1e-6;
        let violated = c.violation > 1e-6;
        if !binding && !violated {
            continue;
        }
        match all.iter_mut().find(|a| a.id == c.id) {
            Some(a) => {
                a.binding_intervals += binding as u32;
                a.violated_intervals += violated as u32;
                a.max_abs_marginal = a.max_abs_marginal.max(c.marginal.abs());
            }
            None => all.push(ConstraintAgg {
                id: c.id.clone(),
                binding_intervals: binding as u32,
                max_abs_marginal: c.marginal.abs(),
                violated_intervals: violated as u32,
            }),
        }
    }
    all.sort_by(|a, b| {
        b.binding_intervals.cmp(&a.binding_intervals).then(
            b.max_abs_marginal
                .partial_cmp(&a.max_abs_marginal)
                .unwrap_or(std::cmp::Ordering::Equal),
        )
    });
    ev.constraints = all.iter().take(10).cloned().collect();

    for ic in &snap.interconnectors {
        let at_export = ic.export_limit.is_some_and(|l| ic.flow >= l - 1.0);
        let at_import = ic.import_limit.is_some_and(|l| ic.flow <= l + 1.0);
        if !(at_export || at_import) {
            continue;
        }
        let dir = if at_export { "export" } else { "import" };
        match ev
            .interconnectors_at_limit
            .iter_mut()
            .find(|a| a.id == ic.id && a.direction == dir)
        {
            Some(a) => a.intervals_at_limit += 1,
            None => ev.interconnectors_at_limit.push(InterconnectorAgg {
                id: ic.id.clone(),
                intervals_at_limit: 1,
                direction: dir.into(),
            }),
        }
    }
}

/// A price with its dollar sign, sign first: "$23,107", "-$1,000".
pub fn dollars(v: f64) -> String {
    if v < 0.0 {
        format!("-${}", money(-v))
    } else {
        format!("${}", money(v))
    }
}

/// $23,107 style formatting with thousands separators, no decimals above $100.
pub fn money(v: f64) -> String {
    let neg = v < 0.0;
    let a = v.abs();
    let s = if a >= 100.0 {
        format!("{:.0}", a)
    } else {
        format!("{:.2}", a)
    };
    let (int, frac) = match s.split_once('.') {
        Some((i, f)) => (i.to_string(), format!(".{f}")),
        None => (s.clone(), String::new()),
    };
    let mut grouped = String::new();
    for (i, ch) in int.chars().enumerate() {
        if i > 0 && (int.len() - i) % 3 == 0 {
            grouped.push(',');
        }
        grouped.push(ch);
    }
    format!("{}{}{}", if neg { "-" } else { "" }, grouped, frac)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time::parse_market;

    fn snap(t: &str, nsw: f64, qld: f64) -> DispatchSnapshot {
        let mut s = DispatchSnapshot {
            settlement: parse_market(t).unwrap(),
            ..Default::default()
        };
        s.regions.insert(
            "NSW1".into(),
            RegionInterval {
                rrp: nsw,
                demand: Some(11000.0),
                ..Default::default()
            },
        );
        s.regions.insert(
            "QLD1".into(),
            RegionInterval {
                rrp: qld,
                demand: Some(7000.0),
                ..Default::default()
            },
        );
        s
    }

    fn run(prices: &[f64]) -> (Vec<Transition>, Detector) {
        let mut d = Detector::new(DetectCfg::default());
        let mut t = parse_market("2026/10/12 13:00:00").unwrap();
        let mut out = Vec::new();
        for p in prices {
            t += chrono::Duration::minutes(5);
            out.extend(d.ingest(&snap(&t.format("%Y/%m/%d %H:%M:%S").to_string(), *p, 150.0)));
        }
        (out, d)
    }

    fn names(ts: &[Transition]) -> Vec<&'static str> {
        ts.iter().map(|t| t.kind.name()).collect()
    }

    #[test]
    fn single_blip_is_discarded() {
        let (ts, _) = run(&[
            200.0, 6000.0, 300.0, 250.0, 240.0, 230.0, 220.0, 210.0, 200.0,
        ]);
        assert_eq!(names(&ts), vec!["opened", "discarded"]);
    }

    #[test]
    fn sustained_spike_confirms_and_closes() {
        let (ts, _) = run(&[
            200.0, 6000.0, 7000.0, 8000.0, 900.0, 300.0, 300.0, 300.0, 300.0, 300.0, 300.0,
        ]);
        assert_eq!(names(&ts), vec!["opened", "confirmed", "closed"]);
        let closed = &ts.last().unwrap().event;
        assert_eq!(closed.intervals_above_high, 3);
        assert_eq!(closed.peak_price, 8000.0);
        assert_eq!(closed.end.as_deref(), Some("2026-10-12T13:20:00+10:00"));
    }

    #[test]
    fn cap_price_confirms_immediately_and_escalates() {
        let (ts, _) = run(&[200.0, 6000.0, 6500.0, 7000.0, 12000.0, 23107.0]);
        assert_eq!(
            names(&ts),
            vec!["opened", "confirmed", "escalated", "escalated"]
        );
        assert_eq!(ts.last().unwrap().event.severity, Severity::Cap);
        let (ts, _) = run(&[200.0, 23107.0]);
        assert_eq!(names(&ts), vec!["opened", "confirmed"]);
    }

    #[test]
    fn whiplash_is_flagged() {
        let (ts, d) = run(&[
            200.0, 1300.0, 766.0, 23000.0, 400.0, 9800.0, 1500.0, 17000.0, 900.0,
        ]);
        assert!(names(&ts).contains(&"flagged"));
        assert!(d.open_events()[0].has_flag("whiplash"));
        assert!(d.open_events()[0].has_flag("price_separation"));
    }

    #[test]
    fn state_round_trips() {
        let (_, d) = run(&[200.0, 6000.0, 7000.0]);
        let json = d.state_json();
        let mut d2 = Detector::restore(DetectCfg::default(), &json).unwrap();
        assert_eq!(d2.open_events().len(), 1);
        let t = snap("2026/10/12 13:20:00", 8000.0, 150.0);
        let ts = d2.ingest(&t);
        assert_eq!(names(&ts), vec!["confirmed"]);
        // Duplicate interval is ignored.
        assert!(d2.ingest(&t).is_empty());
    }

    #[test]
    fn low_price_event() {
        let (ts, _) = run(&[
            50.0, -950.0, -1000.0, -990.0, 20.0, 30.0, 30.0, 30.0, 30.0, 30.0,
        ]);
        assert_eq!(names(&ts), vec!["opened", "confirmed", "closed"]);
        assert_eq!(ts[0].event.kind, EventKind::LowPrice);
        assert_eq!(ts.last().unwrap().event.peak_price, -1000.0);
    }

    #[test]
    fn money_format() {
        assert_eq!(money(23107.0), "23,107");
        assert_eq!(money(-1000.0), "-1,000");
        assert_eq!(money(74.5), "74.50");
        assert_eq!(money(1234567.0), "1,234,567");
        assert_eq!(money(0.0), "0.00");
        assert_eq!(money(-0.5), "-0.50");
        assert_eq!(money(999.6), "1,000");
        assert_eq!(dollars(23107.0), "$23,107");
        assert_eq!(dollars(-1000.0), "-$1,000");
        assert_eq!(dollars(-0.5), "-$0.50");
        assert_eq!(dollars(0.0), "$0.00");
    }

    /// Shaped on the synthetic 12 October fixture (scripts/make_fixture.py); not AEMO data.
    const WHIPSAW: [f64; 27] = [
        220.0, 1300.0, 766.0, 23107.0, 410.0, 9800.0, 1450.0, 16900.0, 880.0, 12400.0, 5400.0,
        15200.0, 1900.0, 8700.0, 6300.0, 5600.0, 7400.0, 5200.0, 920.0, 480.0, 350.0, 300.0, 300.0,
        300.0, 300.0, 300.0, 300.0,
    ];

    fn snaps_from(start: &str, prices: &[(f64, f64)]) -> Vec<DispatchSnapshot> {
        let mut t = parse_market(start).unwrap();
        prices
            .iter()
            .map(|(nsw, qld)| {
                t += chrono::Duration::minutes(5);
                snap(&t.format("%Y/%m/%d %H:%M:%S").to_string(), *nsw, *qld)
            })
            .collect()
    }

    fn nsw_only(prices: &[f64]) -> Vec<(f64, f64)> {
        prices.iter().map(|p| (*p, 150.0)).collect()
    }

    /// Transitions with the wall-clock `updated_at` blanked, for comparison.
    fn comparable(ts: Vec<Transition>) -> Vec<(TransitionKind, Event)> {
        ts.into_iter()
            .map(|mut t| {
                t.event.updated_at.clear();
                (t.kind, t.event)
            })
            .collect()
    }

    #[test]
    fn thresholds_are_inclusive() {
        let quiet = [300.0; 6];
        let (ts, _) = run(&[[200.0, 5000.0, 5000.0, 5000.0].as_slice(), &quiet].concat());
        assert_eq!(names(&ts), ["opened", "confirmed", "closed"]);
        let (ts, _) = run(&[[200.0, 4999.99, 4999.99, 4999.99].as_slice(), &quiet].concat());
        assert!(ts.is_empty());
        // Exactly the immediate-confirmation price.
        let (ts, _) = run(&[200.0, 15000.0]);
        assert_eq!(names(&ts), ["opened", "confirmed"]);
    }

    #[test]
    fn closes_after_exactly_quiet_intervals() {
        let (ts, d) = run(&[6000.0, 7000.0, 8000.0, 300.0, 300.0, 300.0, 300.0, 300.0]);
        assert_eq!(names(&ts), ["opened", "confirmed"]);
        assert_eq!(d.open_events().len(), 1);
        let (ts, d) = run(&[
            6000.0, 7000.0, 8000.0, 300.0, 300.0, 300.0, 300.0, 300.0, 300.0,
        ]);
        assert_eq!(names(&ts), ["opened", "confirmed", "closed"]);
        assert!(d.open_events().is_empty());
    }

    #[test]
    fn a_price_between_close_and_high_resets_the_quiet_count() {
        let (ts, d) = run(&[
            6000.0, 7000.0, 8000.0, 300.0, 300.0, 300.0, 300.0, 300.0, 2000.0, 300.0, 300.0, 300.0,
            300.0, 300.0,
        ]);
        assert_eq!(names(&ts), ["opened", "confirmed"]);
        assert_eq!(d.open_events()[0].last_active, "2026-10-12T13:45:00+10:00");
    }

    #[test]
    fn escalation_before_confirmation_is_silent() {
        let (ts, _) = run(&[6000.0, 12000.0, 7000.0]);
        assert_eq!(names(&ts), ["opened", "confirmed"]);
        assert_eq!(ts[1].event.severity, Severity::Extreme);
        assert_eq!(ts[0].event.severity, Severity::High);
    }

    #[test]
    fn event_trading_day_follows_the_04_00_boundary() {
        let mut d = Detector::new(DetectCfg::default());
        let ts: Vec<Transition> = snaps_from("2026/10/13 03:50:00", &nsw_only(&[16000.0]))
            .iter()
            .flat_map(|s| d.ingest(s))
            .collect();
        assert_eq!(
            ts[0].event.trading_day, "2026-10-12",
            "03:55 belongs to the previous day"
        );
        let mut d = Detector::new(DetectCfg::default());
        let ts: Vec<Transition> = snaps_from("2026/10/13 04:00:00", &nsw_only(&[16000.0]))
            .iter()
            .flat_map(|s| d.ingest(s))
            .collect();
        assert_eq!(ts[0].event.trading_day, "2026-10-13");
    }

    #[test]
    fn regions_are_tracked_independently() {
        let mut d = Detector::new(DetectCfg::default());
        let prices = [
            (16000.0, 150.0),
            (300.0, 16000.0),
            (300.0, 300.0),
            (300.0, 300.0),
            (300.0, 300.0),
            (300.0, 300.0),
            (300.0, 300.0),
        ];
        let ts: Vec<Transition> = snaps_from("2026/10/12 13:00:00", &prices)
            .iter()
            .flat_map(|s| d.ingest(s))
            .collect();
        let summary: Vec<(&str, &str)> = ts
            .iter()
            .map(|t| (t.event.region.as_str(), t.kind.name()))
            .collect();
        assert_eq!(
            summary,
            [
                ("NSW1", "opened"),
                ("NSW1", "confirmed"),
                ("QLD1", "opened"),
                ("QLD1", "confirmed"),
                ("NSW1", "closed"),
            ]
        );
        assert_eq!(d.open_events()[0].region, "QLD1");
    }

    #[test]
    fn earlier_and_duplicate_snapshots_are_ignored() {
        let s = snaps_from("2026/10/12 13:00:00", &nsw_only(&[6000.0, 7000.0]));
        let mut d = Detector::new(DetectCfg::default());
        d.ingest(&s[1]);
        let before = d.state_json();
        assert!(d.ingest(&s[0]).is_empty());
        assert!(d.ingest(&s[1]).is_empty());
        assert_eq!(d.state_json(), before);
    }

    #[test]
    fn a_missing_region_does_not_count_towards_closing() {
        // Data gaps are not quiet intervals; the event stays open.
        let mut d = Detector::new(DetectCfg::default());
        let mut snaps = snaps_from("2026/10/12 13:00:00", &nsw_only(&[6000.0, 7000.0, 8000.0]));
        let mut gap = snaps_from("2026/10/12 13:15:00", &[(0.0, 150.0); 8]);
        for s in &mut gap {
            s.regions.remove("NSW1");
        }
        snaps.extend(gap);
        let ts: Vec<Transition> = snaps.iter().flat_map(|s| d.ingest(s)).collect();
        assert_eq!(names(&ts), ["opened", "confirmed"]);
        assert_eq!(d.open_events().len(), 1);
    }

    #[test]
    fn separation_flag_needs_the_full_gap() {
        let mut d = Detector::new(DetectCfg::default());
        for s in snaps_from("2026/10/12 13:00:00", &[(6000.0, 1500.0), (6000.0, 1500.0)]) {
            d.ingest(&s);
        }
        assert!(
            !d.open_events()[0].has_flag("price_separation"),
            "gap 4,500"
        );
        let mut d = Detector::new(DetectCfg::default());
        for s in snaps_from("2026/10/12 13:00:00", &[(6000.0, 1000.0)]) {
            d.ingest(&s);
        }
        assert!(d.open_events()[0].has_flag("price_separation"), "gap 5,000");
    }

    #[test]
    fn demand_ramp_is_flagged() {
        let mut d = Detector::new(DetectCfg::default());
        let mut snaps = snaps_from("2026/10/12 13:00:00", &nsw_only(&[200.0; 12]));
        snaps.extend(snaps_from("2026/10/12 14:00:00", &nsw_only(&[6000.0])));
        for (i, s) in snaps.iter_mut().enumerate() {
            s.regions.get_mut("NSW1").unwrap().demand = Some(10_000.0 + 140.0 * i as f64);
        }
        for s in &snaps {
            d.ingest(s);
        }
        // 11 steps of 140 MW = 1,540 MW within the 12-interval window.
        assert!(d.open_events()[0].has_flag("demand_ramp"));
    }

    #[test]
    fn restoring_state_at_any_interval_matches_an_uninterrupted_run() {
        let snaps = snaps_from("2026/10/12 13:55:00", &nsw_only(&WHIPSAW));
        let mut whole = Detector::new(DetectCfg::default());
        let expected = comparable(snaps.iter().flat_map(|s| whole.ingest(s)).collect());
        assert_eq!(
            expected.iter().map(|(k, _)| k.name()).collect::<Vec<_>>(),
            ["opened", "confirmed", "flagged", "closed"]
        );
        for k in 0..=snaps.len() {
            let mut a = Detector::new(DetectCfg::default());
            let mut got: Vec<Transition> = snaps[..k].iter().flat_map(|s| a.ingest(s)).collect();
            let mut b = Detector::restore(DetectCfg::default(), &a.state_json()).unwrap();
            got.extend(snaps[k..].iter().flat_map(|s| b.ingest(s)));
            assert_eq!(comparable(got), expected, "restart after interval {k}");
        }
    }

    #[test]
    fn whipsaw_series_counts_match_the_fixture() {
        let (ts, _) = run(&WHIPSAW);
        let closed = &ts.last().unwrap().event;
        assert_eq!(closed.status, Status::Closed);
        assert_eq!(closed.peak_price, 23107.0);
        assert_eq!(closed.intervals_above_high, 11);
        assert_eq!(closed.severity, Severity::Cap);
    }
}

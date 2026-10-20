//! A plain-text fact sheet for one event, built from its public library
//! files only (AEMO data), and the check that an account written from it
//! states no number, unit or constraint the sheet does not hold.
//!
//! The sheet is the only input an AI-written account of the event may use
//! (nem-lens/library issue 1). Every figure is printed as an account should
//! quote it, so [`check`] can match the account's figures exactly.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::path::Path;

use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDateTime};
use nem_lens_aemo::event::{
    is_dc_link, market_price_cap, Constraints, Dispatch, Headline, InterconnectorSeries,
    Interconnectors, Notices, Series, Unavailability,
};
use nem_lens_aemo::time::region_name;
use serde::de::DeserializeOwned;

use crate::detect::dollars;

/// The most constraints, units and notices listed; the sheet says how many
/// more there are.
const MAX_CONSTRAINTS: usize = 8;
const MAX_UNITS: usize = 10;
const MAX_NOTICES: usize = 12;
const MAX_PER_KIND: usize = 3;
/// Longest text quoted from AEMO (descriptions, rebid reasons, titles).
const MAX_QUOTE: usize = 200;
/// A flow this close to its limit, MW, counts as at the limit.
const AT_LIMIT_MW: f64 = 1.0;
/// Notice kinds listed first.
const KEY_NOTICES: &[&str] = &[
    "RESERVE",
    "DIRECTION",
    "INTERVENTION",
    "ADMINISTERED",
    "POWER SYSTEM",
    "LOAD",
    "INTER-REGIONAL",
];
/// Routine notice kinds, listed last.
const ROUTINE_NOTICES: &[&str] = &[
    "PRICES SUBJECT TO REVIEW",
    "PRICES UNCHANGED",
    "SETTLEMENTS RESIDUE",
];
/// Notice kinds left out: IT maintenance, unrelated to the event.
const SKIP_NOTICES: &[&str] = &["MARKET SYSTEMS"];
/// AEMO's constraint naming: an ID starting `F_` is a frequency control
/// (FCAS) requirement, not a limit on energy flows.
fn fcas(id: &str) -> bool {
    id.starts_with("F_")
}

fn load<T: DeserializeOwned>(dir: &Path, name: &str) -> Result<T> {
    let p = dir.join(name);
    let text = std::fs::read_to_string(&p).with_context(|| format!("reading {}", p.display()))?;
    serde_json::from_str(&text).with_context(|| format!("parsing {}", p.display()))
}

fn at(s: &str) -> Result<NaiveDateTime> {
    Ok(DateTime::parse_from_rfc3339(s)
        .with_context(|| format!("not a time: {s}"))?
        .naive_local())
}

fn hm(t: NaiveDateTime) -> String {
    t.format("%H:%M").to_string()
}

/// A whole number with thousands separators.
fn whole(v: f64) -> String {
    let n = v.round() as i64;
    let digits = n.unsigned_abs().to_string();
    let mut out = String::new();
    for (i, c) in digits.chars().enumerate() {
        if i > 0 && (digits.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    if n < 0 {
        format!("-{out}")
    } else {
        out
    }
}

/// `n` of `thing`, plural when `n` is not 1.
fn count(n: usize, thing: &str) -> String {
    format!("{n} {thing}{}", if n == 1 { "" } else { "s" })
}

fn mw(v: f64) -> String {
    format!("{} MW", whole(v))
}

/// A price, $/MWh.
fn price(v: f64) -> String {
    format!("{}/MWh", dollars(v))
}

/// AEMO text, on one line, cut to [`MAX_QUOTE`] characters.
fn quote(s: &str) -> String {
    let one: String = s.split_whitespace().collect::<Vec<_>>().join(" ");
    if one.chars().count() <= MAX_QUOTE {
        one
    } else {
        let cut: String = one.chars().take(MAX_QUOTE).collect();
        format!("{}...", cut.trim_end())
    }
}

/// The fact sheet for the event whose public files are in `dir`
/// (`library/events/<id>/`).
pub fn build(dir: &Path) -> Result<String> {
    let h: Headline = load(dir, "headline.json")?;
    let series: Series = load(dir, "series.json")?;
    let dispatch: Dispatch = load(dir, "dispatch.json")?;
    let ics: Interconnectors = load(dir, "interconnectors.json")?;
    let cons: Constraints = load(dir, "constraints.json")?;
    let unav: Unavailability = load(dir, "unavailability.json")?;
    let notices: Notices = load(dir, "notices.json")?;

    let region = h.region.as_str();
    let name = region_name(region);
    let (start, end, peak_at) = (at(&h.start)?, at(&h.end)?, at(&h.peak_at)?);
    let times = series
        .intervals
        .iter()
        .map(|s| at(s))
        .collect::<Result<Vec<_>>>()?;
    let span: Vec<usize> = (0..times.len())
        .filter(|&i| times[i] >= start && times[i] <= end)
        .collect();
    let peak = times
        .iter()
        .position(|t| *t == peak_at)
        .context("the peak is not in the series")?;
    let n = span.len();
    let tag = |t: &str| h.tags.iter().any(|x| x == t);
    let mut s = String::new();
    let mut line = |text: String| {
        s.push_str(&text);
        s.push('\n');
    };

    line(format!(
        "Fact sheet for event {}, from its public files (AEMO data).",
        h.event_id
    ));
    line(String::new());
    line("## The event".into());
    line(format!("Region: {name} ({region})."));
    line(format!("Trading day: {}.", start.format("%-d %B %Y")));
    line(if n == 1 {
        format!(
            "Event span: one 5-minute dispatch interval, ending {} market time (AEST).",
            hm(end)
        )
    } else {
        format!(
            "Event span: {} to {} market time (AEST), {} minutes, {n} dispatch intervals of 5 minutes.",
            hm(start),
            hm(end),
            h.duration_minutes
        )
    });
    let settlement = if tag("settlement:30-minute") { 30 } else { 5 };
    line(format!("Settlement period: {settlement} minutes."));
    if tag("intervention:yes") {
        line(
            "AEMO ran an intervention pricing run in the window: a direction or RERT was in place."
                .into(),
        );
    }

    let rs = series
        .regions
        .get(region)
        .context("no series for the event region")?;
    let p = |i: usize| rs.price.get(i).copied().flatten();
    line(String::new());
    line("## Prices".into());
    let cap = market_price_cap(peak_at);
    line(format!(
        "Peak price: {} at {}.",
        price(h.peak_price),
        hm(peak_at)
    ));
    line(format!(
        "Market price cap: {}; the peak was {:.1}% of it.",
        price(cap),
        100.0 * h.peak_price / cap
    ));
    line(format!(
        "Mean price over the span: {}.",
        price(h.mean_price)
    ));
    line(format!(
        "Intervals at or above $5,000/MWh: {} of {n}.",
        h.intervals_above_5000
    ));
    if let Some((i, lo)) = span
        .iter()
        .filter_map(|&i| p(i).map(|v| (i, v)))
        .min_by(|a, b| a.1.total_cmp(&b.1))
    {
        line(format!(
            "Lowest price in the span: {} at {}.",
            price(lo),
            hm(times[i])
        ));
    }
    if let Some(v) = p(0) {
        line(format!(
            "Price at the start of the window ({}, before the span): {}.",
            hm(times[0]),
            price(v)
        ));
    }
    line(format!(
        "Cost to load over the span (AEMO price times demand): ${:.1} million.",
        h.cost_to_load / 1e6
    ));
    let others: Vec<String> = series
        .regions
        .iter()
        .filter(|(r, _)| r.as_str() != region)
        .filter_map(|(r, x)| {
            let v = x.price.get(peak).copied().flatten()?;
            Some(format!("{} {}", region_name(r), price(v)))
        })
        .collect();
    if !others.is_empty() {
        line(format!("Other regions at the peak: {}.", others.join("; ")));
    }

    line(String::new());
    line(format!("## Demand and supply in {name}"));
    let get = |v: &Vec<Option<f64>>, i: usize| v.get(i).copied().flatten();
    if let Some(d) = get(&rs.demand, peak) {
        line(format!("Demand at the peak: {}.", mw(d)));
    }
    line(format!(
        "Maximum demand in the span: {}.",
        mw(h.max_demand_mw)
    ));
    if let Some(a) = get(&rs.available_generation, peak) {
        line(format!(
            "Available generation at the peak (AEMO): {}.",
            mw(a)
        ));
    }
    let margin = |i: usize| Some(get(&rs.available_generation, i)? - get(&rs.demand, i)?);
    if let Some(m) = margin(peak) {
        line(format!(
            "Available generation minus demand at the peak: {}.",
            mw(m)
        ));
    }
    if let Some((i, m)) = span
        .iter()
        .filter_map(|&i| margin(i).map(|m| (i, m)))
        .min_by(|a, b| a.1.total_cmp(&b.1))
    {
        line(format!(
            "Lowest available generation minus demand in the span: {} at {}.",
            mw(m),
            hm(times[i])
        ));
    }
    if let Some(x) = get(&rs.net_interchange, peak) {
        line(if x < 0.0 {
            format!("Net interchange at the peak: importing {}.", mw(-x))
        } else {
            format!("Net interchange at the peak: exporting {}.", mw(x))
        });
    }
    // Contingency reserve ratio: plant and DC links only, not the network.
    let crr = |i: usize| {
        Some((
            rs.contingency.get(i)?.as_deref()?,
            get(&rs.contingency_mw, i)?,
            get(&rs.reserve_mw, i)?,
            get(&rs.crr, i)?,
        ))
    };
    let trip = |(id, c, r, ratio): (&str, f64, f64, f64)| {
        let what = if is_dc_link(id) {
            format!("DC link {id} (its flow in)")
        } else {
            format!("unit {id}")
        };
        let gap = if r < c {
            format!("{} short of it", mw(c - r))
        } else {
            format!("{} more than it", mw(r - c))
        };
        format!(
            "largest single loss {what}, {}; reserve to replace it {} (other units' available headroom plus room to import), {gap}; ratio {ratio:.2}",
            mw(c),
            mw(r)
        )
    };
    if let Some(x) = crr(peak) {
        line(format!(
            "Contingency reserve at the peak (plant and DC links only, not network outages): {}.",
            trip(x)
        ));
    }
    if let Some((i, x)) = span
        .iter()
        .filter_map(|&i| crr(i).map(|x| (i, x)))
        .min_by(|a, b| a.1 .3.total_cmp(&b.1 .3))
    {
        if i != peak {
            line(format!(
                "Lowest contingency reserve ratio in the span, at {}: {}.",
                hm(times[i]),
                trip(x)
            ));
        }
    }

    let mut fuels: Vec<(&String, f64, f64)> = dispatch
        .fuels
        .iter()
        .filter_map(|(f, v)| Some((f, *v.get(peak)?, *v.first()?)))
        .filter(|(_, at_peak, first)| at_peak.abs() >= 0.5 || first.abs() >= 0.5)
        .collect();
    fuels.sort_by(|a, b| b.1.total_cmp(&a.1));
    if !fuels.is_empty() {
        let parts: Vec<String> = fuels
            .iter()
            .map(|(f, v, first)| {
                // In words, as an account would put it ("fell by 596").
                let d = (v - first).round();
                let change = if d > 0.0 {
                    format!("up {}", whole(d))
                } else if d < 0.0 {
                    format!("down {}", whole(-d))
                } else {
                    "no change".to_string()
                };
                format!("{f} {} ({change})", whole(*v))
            })
            .collect();
        line(format!(
            "Dispatch by fuel in {name} at the peak, MW (change from {}): {}.",
            hm(times[0]),
            parts.join("; ")
        ));
    }

    line(String::new());
    line(format!("## Units out or derated in {name}"));
    let mut units: Vec<(f64, &_)> = unav
        .units
        .iter()
        .filter(|u| u.region == region)
        .map(|u| {
            let avail = u.availability.get(peak).copied().flatten().unwrap_or(0.0);
            ((u.capacity_mw - avail).max(0.0), u)
        })
        .filter(|(short, _)| *short >= 0.5)
        .collect();
    units.sort_by(|a, b| b.0.total_cmp(&a.0));
    if units.is_empty() {
        line(format!(
            "None: no unit in {name} was out or derated at the peak."
        ));
    } else {
        let total: f64 = units.iter().map(|(x, _)| x).sum();
        line(format!(
            "{} out or derated at the peak, {} short of capacity in total (capacity minus AEMO availability). The largest:",
            count(units.len(), "unit"),
            mw(total)
        ));
        for (short, u) in units.iter().take(MAX_UNITS) {
            let avail = u.availability.get(peak).copied().flatten().unwrap_or(0.0);
            let mut t = format!(
                "- {} ({}, capacity {}): {}, {} available at the peak, {} short.",
                u.duid,
                u.fuel.as_deref().unwrap_or("fuel unknown"),
                mw(u.capacity_mw),
                if u.kind == "outage" { "out" } else { "derated" },
                mw(avail),
                mw(*short)
            );
            let mut reasons: Vec<String> = Vec::new();
            for r in u.rebids.iter().rev() {
                let q = quote(&r.explanation);
                if !q.is_empty() && !reasons.contains(&q) {
                    reasons.push(q);
                }
            }
            reasons.truncate(2);
            if !reasons.is_empty() {
                let _ = write!(
                    t,
                    " Rebid reasons in force: \"{}\".",
                    reasons.join("\"; \"")
                );
            }
            line(t);
        }
        if units.len() > MAX_UNITS {
            line(format!("({} more not listed.)", units.len() - MAX_UNITS));
        }
    }
    line(String::new());
    line("## Network constraints in the span".into());
    let mut bound: Vec<(usize, f64, bool, &_)> = cons
        .constraints
        .iter()
        .filter_map(|c| {
            let m = |i: usize| c.marginal.get(i).copied().flatten().filter(|v| *v != 0.0);
            let count = span.iter().filter(|&&i| m(i).is_some()).count();
            let largest = span
                .iter()
                .filter_map(|&i| m(i))
                .map(f64::abs)
                .fold(0.0, f64::max);
            (count > 0).then_some((count, largest, m(peak).is_some(), c))
        })
        .collect();
    // Listed: network limits in the event region (by AEMO's constraint
    // naming, whose first character is the region), inter-regional ones, and
    // any that set a limit on the region's interconnectors in the span;
    // those binding at the peak first, then by marginal value. Marginal
    // values rank them but are not printed: values far above the price cap
    // mark a constraint that could not be met, and read as prices mislead.
    let touching: Vec<&InterconnectorSeries> = ics
        .interconnectors
        .iter()
        .filter(|i| i.from_region == region || i.to_region == region)
        .collect();
    let setters: std::collections::HashSet<&str> = touching
        .iter()
        .flat_map(|ic| {
            span.iter()
                .flat_map(move |&i| [ic.import_set_by.get(i), ic.export_set_by.get(i)])
        })
        .flatten()
        .flatten()
        .map(String::as_str)
        .collect();
    let letter = region.chars().next().unwrap_or('?');
    // Constraints dispatch could not meet (AEMO VIOLATIONDEGREE), FCAS ones
    // included: those named for the region or setting one of its
    // interconnector limits. Single-unit ones are left out.
    let mut violated: Vec<(usize, f64, Option<f64>, &_)> = cons
        .constraints
        .iter()
        .filter(|c| !c.id.starts_with(['#', '$']))
        .filter(|c| {
            let id = c.id.as_str();
            id.starts_with(letter)
                || id.starts_with(&format!("F_{letter}"))
                || id.starts_with(&format!("C_{letter}_"))
                || setters.contains(id)
        })
        .filter_map(|c| {
            let v = |i: usize| c.violation.get(i).copied().flatten();
            let count = span.iter().filter(|&&i| v(i).is_some()).count();
            let largest = span
                .iter()
                .filter_map(|&i| v(i))
                .map(f64::abs)
                .fold(0.0, f64::max);
            // Under 1 (MW for most) is rounding-level, often Mvar limits on
            // idle solar and wind farms.
            (count > 0 && largest >= 1.0).then_some((count, largest, v(peak), c))
        })
        .collect();
    violated.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.3.id.cmp(&b.3.id)));
    if !violated.is_empty() {
        line(format!(
            "{} in or into {name} could not be met by AEMO's dispatch (violated) in the span. When dispatch cannot meet a constraint, its penalty can set regional prices, up to the market price cap:",
            count(violated.len(), "constraint")
        ));
        for (k, largest, at_peak, c) in &violated {
            let mut t = format!(
                "- {}{}: violated in {k} of {n} intervals, by up to {}",
                c.id,
                if c.description.trim().is_empty() {
                    String::new()
                } else {
                    format!(" (\"{}\")", quote(&c.description))
                },
                mw(*largest)
            );
            if let Some(v) = at_peak {
                let _ = write!(t, "; by {} at the peak", mw(v.abs()));
            }
            t.push('.');
            line(t);
        }
    }
    let (mut k_fcas, mut single, mut elsewhere) = (0, 0, 0);
    bound.retain(|b| {
        let id = b.3.id.as_str();
        if fcas(id) {
            k_fcas += 1;
        } else if id.starts_with(['#', '$']) || b.3.description.trim().is_empty() {
            single += 1;
        } else if id.starts_with(letter)
            || id.starts_with('I')
            || id.starts_with(&format!("C_{letter}_"))
            || setters.contains(id)
        {
            return true;
        } else {
            elsewhere += 1;
        }
        false
    });
    bound.sort_by(|a, b| b.2.cmp(&a.2).then(b.1.total_cmp(&a.1)).then(b.0.cmp(&a.0)));
    if bound.is_empty() {
        line(format!(
            "No network constraint in or into {name} was binding in the span."
        ));
    } else {
        line(format!(
            "{} in or into {name} had a non-zero AEMO marginal value (were binding) in the span. Those binding at the peak first, then by AEMO's marginal value:",
            count(bound.len(), "network constraint")
        ));
        for (count, _, at_peak, c) in bound.iter().take(MAX_CONSTRAINTS) {
            let mut t = format!(
                "- {}: \"{}\". Binding in {count} of {n} intervals{}.",
                c.id,
                quote(&c.description),
                if *at_peak {
                    ", including at the peak"
                } else {
                    ""
                },
            );
            if c.outage {
                t.push_str(" AEMO describes it as applying for a network outage.");
            }
            line(t);
        }
        if bound.len() > MAX_CONSTRAINTS {
            line(format!(
                "({} more not listed.)",
                bound.len() - MAX_CONSTRAINTS
            ));
        }
    }
    for (k, what, where_) in [
        (elsewhere, "network constraint", " elsewhere in the NEM"),
        (
            single,
            "constraint",
            " on a single unit or without an AEMO description",
        ),
        (
            k_fcas,
            "frequency control (FCAS) requirement constraint",
            "",
        ),
    ] {
        if k > 0 {
            line(format!(
                "Also binding, not listed: {}{where_}.",
                count(k, what)
            ));
        }
    }

    line(String::new());
    line(format!("## Interconnectors to and from {name}"));
    let described: BTreeMap<&str, &str> = cons
        .constraints
        .iter()
        .filter(|c| !c.description.trim().is_empty())
        .map(|c| (c.id.as_str(), c.description.as_str()))
        .collect();
    if touching.is_empty() {
        line(format!("None: {name} has no interconnector in the data."));
    }
    for ic in touching {
        line(interconnector(ic, region, &span, peak, n, &described));
    }

    line(String::new());
    line("## AEMO market notices around the event".into());
    let relevant: Vec<&_> = notices
        .notices
        .iter()
        .filter(|x| !SKIP_NOTICES.iter().any(|k| x.kind.contains(k)))
        .collect();
    if relevant.is_empty() {
        let mut t = "None held for this event.".to_string();
        if start >= at("2024-08-01T00:00:00+10:00")? {
            t.push_str(" AEMO's archive holds no market notices from August 2024 on, so their absence says nothing about what happened.");
        }
        line(t);
    } else {
        // Key kinds first, then those issued nearest the event, at most
        // MAX_PER_KIND of a kind; shown in time order.
        let key = |k: &str| {
            if KEY_NOTICES.iter().any(|p| k.contains(p)) {
                2
            } else if ROUTINE_NOTICES.iter().any(|p| k.contains(p)) {
                0
            } else {
                1
            }
        };
        let mut list: Vec<(NaiveDateTime, &_)> = relevant
            .iter()
            .map(|x| Ok((at(&x.issued)?, *x)))
            .collect::<Result<_>>()?;
        let distance = |t: NaiveDateTime| {
            if t < start {
                start - t
            } else if t > end {
                t - end
            } else {
                chrono::Duration::zero()
            }
        };
        list.sort_by(|a, b| {
            key(&b.1.kind)
                .cmp(&key(&a.1.kind))
                .then(distance(a.0).cmp(&distance(b.0)))
        });
        let mut per_kind: BTreeMap<&str, usize> = BTreeMap::new();
        let mut shown: BTreeMap<(NaiveDateTime, &str), &_> = BTreeMap::new();
        let routine = list.iter().filter(|(_, x)| key(&x.kind) == 0).count();
        list.retain(|(_, x)| key(&x.kind) > 0);
        for (t, x) in &list {
            let k = per_kind.entry(x.kind.as_str()).or_default();
            if *k < MAX_PER_KIND && shown.len() < MAX_NOTICES {
                *k += 1;
                shown.insert((*t, x.id.as_str()), *x);
            }
        }
        line(format!(
            "{} (market systems maintenance left out). Those nearest the event, in time order:",
            count(relevant.len(), "notice")
        ));
        for x in shown.values() {
            let t = at(&x.issued)?;
            line(format!(
                "- {} {}, {}: {}",
                t.format("%-d %B"),
                hm(t),
                x.kind.to_lowercase(),
                quote(&x.title)
            ));
        }
        let more = relevant.len() - shown.len() - routine;
        if more > 0 {
            line(format!("({more} more not listed.)"));
        }
        if routine > 0 {
            line(format!(
                "({routine} routine notices not listed: prices subject to review or confirmed, settlement residues.)"
            ));
        }
    }
    Ok(s)
}

/// One interconnector: its flow at the peak and when it sat at a limit.
fn interconnector(
    ic: &InterconnectorSeries,
    region: &str,
    span: &[usize],
    peak: usize,
    n: usize,
    described: &BTreeMap<&str, &str>,
) -> String {
    let get = |v: &Vec<Option<f64>>, i: usize| v.get(i).copied().flatten();
    let (from, to) = (region_name(&ic.from_region), region_name(&ic.to_region));
    let mut t = format!("- {} ({from} to {to}): ", ic.id);
    match get(&ic.flow, peak) {
        Some(f) => {
            let into = (ic.to_region == region) == (f > 0.0);
            let dir = if into { "into" } else { "out of" };
            let _ = write!(
                t,
                "{} {dir} {} at the peak.",
                mw(f.abs()),
                region_name(region)
            );
        }
        None => t.push_str("no flow recorded at the peak."),
    }
    // (intervals at or beyond the limit, constraint IDs that set it,
    // intervals beyond it) per direction.
    let mut forward = (0, BTreeMap::<&str, usize>::new(), 0);
    let mut back = (0, BTreeMap::<&str, usize>::new(), 0);
    for &i in span {
        let Some(f) = get(&ic.flow, i) else { continue };
        if let Some(l) = get(&ic.export_limit, i).filter(|l| f >= l - AT_LIMIT_MW) {
            forward.0 += 1;
            forward.2 += usize::from(f > l + AT_LIMIT_MW);
            if let Some(Some(by)) = ic.export_set_by.get(i) {
                *forward.1.entry(by.as_str()).or_default() += 1;
            }
        }
        if let Some(l) = get(&ic.import_limit, i).filter(|l| f <= l + AT_LIMIT_MW) {
            back.0 += 1;
            back.2 += usize::from(f < l - AT_LIMIT_MW);
            if let Some(Some(by)) = ic.import_set_by.get(i) {
                *back.1.entry(by.as_str()).or_default() += 1;
            }
        }
    }
    // How far the flow at the peak went past each direction's limit.
    let past = [
        get(&ic.flow, peak)
            .zip(get(&ic.export_limit, peak))
            .map(|(f, l)| f - l),
        get(&ic.flow, peak)
            .zip(get(&ic.import_limit, peak))
            .map(|(f, l)| l - f),
    ];
    // Each direction's limit at the peak, signed in that direction: AEMO's
    // import limit bounds the reverse flow, so it is negated. A limit below
    // zero forces flow the other way.
    let limits = [
        get(&ic.export_limit, peak),
        get(&ic.import_limit, peak).map(|l| -l),
    ];
    for ((((count, by, beyond), (a, b)), limit), past) in
        [(forward, (from, to)), (back, (to, from))]
            .into_iter()
            .zip(limits)
            .zip(past)
    {
        if count == 0 {
            continue;
        }
        if beyond > 0 {
            let _ = write!(
                t,
                " At or beyond its limit for flow from {a} to {b} in {count} of {n} intervals, beyond it (the limit not met) in {beyond}"
            );
        } else {
            let _ = write!(
                t,
                " At its limit for flow from {a} to {b} in {count} of {n} intervals"
            );
        }
        if let Some(l) = limit {
            let _ = write!(t, " (that limit was {} at the peak", mw(l));
            if l < 0.0 {
                let _ = write!(t, ", below zero, requiring flow from {b} to {a}");
            }
            if let Some(p) = past.filter(|p| *p > AT_LIMIT_MW) {
                let _ = write!(t, "; the flow was {} past it at the peak", mw(p));
            }
            t.push(')');
        }
        if let Some((id, _)) = by.iter().max_by_key(|(_, c)| **c) {
            let _ = write!(t, ", limit mostly set by {id}");
            if let Some(d) = described.get(id) {
                let _ = write!(t, " (\"{}\")", quote(d));
            }
        }
        t.push('.');
    }
    t
}

/// What a figure or name in an account is matched as.
#[derive(Debug, PartialEq, Eq, Hash)]
enum Token {
    /// A number, without `$`, commas or a trailing `%`.
    Number(String),
    /// A clock time, `14:20`.
    Time(String),
    /// A unit, region or constraint ID: has a capital and a digit, or one
    /// of the characters constraint IDs use.
    Name(String),
}

fn tokens(text: &str) -> Vec<Token> {
    let inside = |c: char| c.is_alphanumeric() || "_>+-:.,$%^=#<".contains(c);
    let mut words = Vec::new();
    for raw in text.split(|c: char| !inside(c)) {
        let w = raw.trim_matches(|c: char| ".,;:%".contains(c));
        // A hyphenated word without capitals ("30-minute") is words, not an ID.
        if w.contains('-') && !w.starts_with('-') && !w.chars().any(|c| c.is_ascii_uppercase()) {
            words.extend(w.split('-'));
        } else {
            words.push(w);
        }
    }
    let mut out = Vec::new();
    for w in words {
        if w.is_empty() {
            continue;
        }
        let neg = w.starts_with('-');
        let number = w
            .trim_start_matches('-')
            .trim_start_matches('$')
            .replace(',', "");
        if !number.is_empty()
            && number.chars().all(|c| c.is_ascii_digit() || c == '.')
            && number.chars().next().is_some_and(|c| c.is_ascii_digit())
        {
            let number = number.trim_end_matches('.');
            out.push(Token::Number(format!(
                "{}{number}",
                if neg { "-" } else { "" }
            )));
        } else if w.len() == 5
            && w.as_bytes()[2] == b':'
            && w.chars().filter(char::is_ascii_digit).count() == 4
        {
            out.push(Token::Time(w.to_string()));
        } else {
            let capital = w.chars().any(|c| c.is_ascii_uppercase());
            let digit = w.chars().any(|c| c.is_ascii_digit());
            let id_chars = w.chars().any(|c| "_>+^:=<#".contains(c));
            if capital && (digit || id_chars) {
                out.push(Token::Name(w.to_string()));
            }
        }
    }
    out
}

/// The figures, times and names in `account` that `sheet` does not hold,
/// in order of first appearance. Empty means the account passes.
pub fn check(sheet: &str, account: &str) -> Vec<String> {
    let known: std::collections::HashSet<Token> = tokens(sheet).into_iter().collect();
    let mut missing = Vec::new();
    for t in tokens(account) {
        if !known.contains(&t) {
            let s = match &t {
                Token::Number(x) => format!("number {x}"),
                Token::Time(x) => format!("time {x}"),
                Token::Name(x) => format!("name {x}"),
            };
            if !missing.contains(&s) {
                missing.push(s);
            }
        }
    }
    missing
}

#[cfg(test)]
mod tests {
    use super::*;

    const SHEET: &str = "Peak price: $23,107/MWh at 14:20.\n\
        - N>>N-NIL_3: \"Out = Nil\". Binding in 16 of 16 intervals; largest marginal value $1,234.50/MWh.\n\
        - ER01 (coal, capacity 720 MW): out, 0 MW available at the peak.\n\
        Market price cap: $23,200/MWh; the peak was 99.6% of it. Region: NSW (NSW1).";

    #[test]
    fn an_account_quoting_the_sheet_passes() {
        let account = "Prices peaked at $23,107/MWh at 14:20, 99.6% of the $23,200/MWh cap. \
            ER01, a 720 MW coal unit in NSW1, was out, and N>>N-NIL_3 bound in 16 intervals.";
        assert_eq!(check(SHEET, account), Vec::<String>::new());
    }

    #[test]
    fn figures_and_names_not_in_the_sheet_fail() {
        let account = "Prices reached $23,500/MWh at 14:25 while ER02 and N>>N-NIL_4 bound; \
            about 9,000 MW was needed.";
        assert_eq!(
            check(SHEET, account),
            [
                "number 23500",
                "time 14:25",
                "name ER02",
                "name N>>N-NIL_4",
                "number 9000"
            ]
        );
    }

    #[test]
    fn plain_words_are_ignored_but_their_numbers_are_not() {
        // Capitalised words and punctuation are not figures; a number in a
        // hyphenated word is.
        let account = "AEMO's notices (the RERT kind) mention a 30-minute period.";
        assert_eq!(check(SHEET, account), ["number 30"]);
        // Signs, dollars and units around a figure.
        let sheet = "Lowest price in the span: -$1,000/MWh at 15:05.";
        assert_eq!(
            check(sheet, "It fell to -$1,000/MWh."),
            Vec::<String>::new()
        );
        assert_eq!(check(sheet, "It fell to $1,000/MWh."), ["number 1000"]);
        assert_eq!(whole(-1234.4), "-1,234");
        assert_eq!(whole(999.5), "1,000");
    }

    #[test]
    fn a_flow_past_its_limit_is_called_so() {
        // 16 Nov 2020: the Victoria-to-NSW limit required 447 MW to flow
        // into Victoria; 232 MW did.
        let ic = InterconnectorSeries {
            id: "VIC1-NSW1".into(),
            from_region: "VIC1".into(),
            to_region: "NSW1".into(),
            flow: vec![Some(-232.0)],
            import_limit: vec![Some(-1214.7)],
            export_limit: vec![Some(-446.9)],
            import_set_by: vec![None],
            export_set_by: vec![Some("N::N_CNSD_2".into())],
        };
        let described = BTreeMap::from([("N::N_CNSD_2", "Out = Canberra - Stockdill (3C)")]);
        let t = interconnector(&ic, "NSW1", &[0], 0, 1, &described);
        assert!(t.contains("232 MW out of NSW"), "{t}");
        assert!(t.contains("beyond it (the limit not met) in 1"), "{t}");
        assert!(t.contains("requiring flow from NSW to Victoria"), "{t}");
        assert!(t.contains("the flow was 215 MW past it at the peak"), "{t}");
        assert!(
            t.contains("N::N_CNSD_2 (\"Out = Canberra - Stockdill (3C)\")"),
            "{t}"
        );
    }
}

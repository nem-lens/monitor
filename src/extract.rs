//! The public event files: everything in the replay library, built from a
//! bundle's AEMO tables alone (event schema 2, `nem_lens_aemo::event`).
//!
//! No engine output is read or written here. Every figure is AEMO's own,
//! pricing run only, or simple arithmetic on it (means, sums, price x
//! demand).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use arrow_array::{
    Array, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray,
};
use chrono::Datelike;
use chrono::{Duration, NaiveDateTime};
use nem_lens_aemo::event::{
    five_minute_settlement, fuel_of, interconnector_ends, is_outage_description, market_price_cap,
    season, ConstraintSeries, Constraints, Dispatch, Headline, InterconnectorSeries,
    Interconnectors, Notice, Notices, Rebid, RegionSeries, Series, Unavailability,
    UnitAvailability, SCHEMA,
};
use nem_lens_aemo::time::{iso_market, trading_day};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::{Deserialize, Serialize};

use crate::bundle::Manifest;
use crate::library::event_span;

/// Market notices kept in a bundle by `bundle add-notices`.
pub const NOTICES_FILE: &str = "market_notices.json";

/// A notice as fetched: its NEMWeb file, URL and hash, and the parsed text.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FetchedNotice {
    pub file: String,
    pub url: String,
    pub sha256: String,
    pub notice: Notice,
}

/// Every public file of one event.
pub struct PublicFiles {
    pub headline: Headline,
    pub series: Series,
    pub dispatch: Dispatch,
    pub interconnectors: Interconnectors,
    pub constraints: Constraints,
    pub unavailability: Unavailability,
    pub notices: Notices,
}

impl PublicFiles {
    /// (file name, JSON) for each file, in the library's order; the
    /// headline is its public view.
    pub fn to_json(&self) -> Result<Vec<(&'static str, Vec<u8>)>> {
        Ok(vec![
            ("headline.json", pretty(&self.headline.public_view())?),
            ("series.json", pretty(&self.series)?),
            ("dispatch.json", pretty(&self.dispatch)?),
            ("interconnectors.json", pretty(&self.interconnectors)?),
            ("constraints.json", pretty(&self.constraints)?),
            ("unavailability.json", pretty(&self.unavailability)?),
            ("notices.json", pretty(&self.notices)?),
        ])
    }
}

fn pretty<T: Serialize>(v: &T) -> Result<Vec<u8>> {
    let mut out = serde_json::to_vec_pretty(v)?;
    out.push(b'\n');
    Ok(out)
}

/// Where a record's tables are: `DIR/bundle/` in the private store, or
/// `DIR` itself for a bundle as the monitor writes it.
pub fn tables_dir(dir: &Path) -> PathBuf {
    let nested = dir.join("bundle");
    if nested.is_dir() {
        nested
    } else {
        dir.to_path_buf()
    }
}

// ------------------------------------------------------------- tables

#[derive(Debug, Clone, PartialEq)]
enum V {
    Null,
    I(i64),
    F(f64),
    S(String),
    T(NaiveDateTime),
}

impl V {
    fn f(&self) -> Option<f64> {
        match self {
            V::I(x) => Some(*x as f64),
            V::F(x) => Some(*x),
            V::S(s) => s.trim().parse().ok(),
            _ => None,
        }
    }
    fn s(&self) -> Option<&str> {
        match self {
            V::S(s) => Some(s.as_str()),
            _ => None,
        }
    }
    fn t(&self) -> Option<NaiveDateTime> {
        match self {
            V::T(t) => Some(*t),
            _ => None,
        }
    }
}

/// A Parquet table as rows of loosely typed values (AEMO's column types
/// vary between windows: RRP is integral in some).
#[derive(Default)]
struct Tbl {
    cols: HashMap<String, usize>,
    rows: Vec<Vec<V>>,
}

impl Tbl {
    fn read(path: &Path) -> Result<Tbl> {
        let mut t = Tbl::default();
        if !path.exists() {
            return Ok(t);
        }
        let f = fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(f)?.build()?;
        for b in reader {
            let b: RecordBatch = b?;
            let names: Vec<String> = b
                .schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect();
            for n in &names {
                if !t.cols.contains_key(n) {
                    let i = t.cols.len();
                    t.cols.insert(n.clone(), i);
                }
            }
            for r in 0..b.num_rows() {
                let mut row = vec![V::Null; t.cols.len()];
                for (c, n) in names.iter().enumerate() {
                    row[t.cols[n]] = cell(b.column(c).as_ref(), r);
                }
                t.rows.push(row);
            }
        }
        // Rows from early batches may be shorter if a later batch added columns.
        let w = t.cols.len();
        for row in &mut t.rows {
            row.resize(w, V::Null);
        }
        Ok(t)
    }

    fn get<'a>(&self, row: &'a [V], col: &str) -> &'a V {
        static NULL: V = V::Null;
        self.cols.get(col).map_or(&NULL, |&i| &row[i])
    }

    /// Rows of the pricing run (INTERVENTION absent or 0).
    fn pricing(&self) -> impl Iterator<Item = &Vec<V>> {
        self.rows
            .iter()
            .filter(|r| self.get(r, "INTERVENTION").f().is_none_or(|x| x == 0.0))
    }
}

fn cell(a: &dyn Array, i: usize) -> V {
    if a.is_null(i) {
        return V::Null;
    }
    let any = a.as_any();
    if let Some(x) = any.downcast_ref::<Float64Array>() {
        V::F(x.value(i))
    } else if let Some(x) = any.downcast_ref::<Int64Array>() {
        V::I(x.value(i))
    } else if let Some(x) = any.downcast_ref::<StringArray>() {
        V::S(x.value(i).to_string())
    } else if let Some(x) = any.downcast_ref::<TimestampMillisecondArray>() {
        chrono::DateTime::from_timestamp_millis(x.value(i)).map_or(V::Null, |t| V::T(t.naive_utc()))
    } else {
        V::Null
    }
}

// ------------------------------------------------------------- units

struct UnitInfo {
    region: String,
    dispatch_type: String,
    schedule_type: String,
}

/// DUID -> (fuel, capacity MW) from DUALLOC and GENUNITS, latest
/// allocation in force at `at`.
fn fuels(dir: &Path, at: NaiveDateTime) -> Result<HashMap<String, (Option<&'static str>, f64)>> {
    let alloc = Tbl::read(&dir.join("static_dualloc.parquet"))?;
    let units = Tbl::read(&dir.join("static_genunits.parquet"))?;
    let mut sets: HashMap<String, ((NaiveDateTime, i64), Vec<String>)> = HashMap::new();
    for r in &alloc.rows {
        let (Some(duid), Some(gs), Some(eff)) = (
            alloc.get(r, "DUID").s(),
            alloc.get(r, "GENSETID").s(),
            alloc.get(r, "EFFECTIVEDATE").t(),
        ) else {
            continue;
        };
        if eff > at {
            continue;
        }
        let v = alloc.get(r, "VERSIONNO").f().unwrap_or(0.0) as i64;
        let e = sets
            .entry(duid.to_string())
            .or_insert(((eff, v), Vec::new()));
        if (eff, v) > e.0 {
            *e = ((eff, v), Vec::new());
        }
        if (eff, v) == e.0 && !e.1.iter().any(|g| g == gs) {
            e.1.push(gs.to_string());
        }
    }
    let mut genset: HashMap<String, (Option<&'static str>, f64)> = HashMap::new();
    for r in &units.rows {
        if let Some(gs) = units.get(r, "GENSETID").s() {
            let fuel = units.get(r, "CO2E_ENERGY_SOURCE").s().and_then(fuel_of);
            let cap = units
                .get(r, "MAXCAPACITY")
                .f()
                .or_else(|| units.get(r, "REGISTEREDCAPACITY").f())
                .unwrap_or(0.0);
            genset.insert(gs.to_string(), (fuel, cap));
        }
    }
    Ok(sets
        .into_iter()
        .map(|(duid, (_, gs))| {
            let fuel = gs.iter().find_map(|g| genset.get(g).and_then(|x| x.0));
            let cap = gs.iter().filter_map(|g| genset.get(g)).map(|x| x.1).sum();
            (duid, (fuel, cap))
        })
        .collect())
}

fn unit_info(dir: &Path) -> Result<HashMap<String, UnitInfo>> {
    let d = Tbl::read(&dir.join("static_dudetailsummary.parquet"))?;
    let mut out = HashMap::new();
    for r in &d.rows {
        if let Some(duid) = d.get(r, "DUID").s() {
            out.insert(
                duid.to_string(),
                UnitInfo {
                    region: d.get(r, "REGIONID").s().unwrap_or_default().to_string(),
                    dispatch_type: d.get(r, "DISPATCHTYPE").s().unwrap_or_default().to_string(),
                    schedule_type: d
                        .get(r, "SCHEDULE_TYPE")
                        .s()
                        .unwrap_or_default()
                        .to_string(),
                },
            );
        }
    }
    Ok(out)
}

// ------------------------------------------------------------- build

/// Interval ends in a bundle's price table (pricing run).
pub fn price_intervals(dir: &Path) -> Result<BTreeSet<NaiveDateTime>> {
    let price = Tbl::read(&tables_dir(dir).join("dispatch_price.parquet"))?;
    Ok(price
        .pricing()
        .filter_map(|r| price.get(r, "SETTLEMENTDATE").t())
        .collect())
}

fn round(x: f64, places: i32) -> f64 {
    let k = 10f64.powi(places);
    (x * k).round() / k
}

/// Build every public file for the record or bundle at `dir`.
pub fn build(dir: &Path) -> Result<PublicFiles> {
    let m = Manifest::load(dir)?;
    let tdir = tables_dir(dir);
    let (start, end) = event_span(&m)?;
    let window_start =
        nem_lens_aemo::time::parse_market(&m.window_start).context("manifest window_start")?;
    let window_end =
        nem_lens_aemo::time::parse_market(&m.window_end).context("manifest window_end")?;

    // Series: price and region totals.
    let price = Tbl::read(&tdir.join("dispatch_price.parquet"))?;
    let sum = Tbl::read(&tdir.join("dispatch_regionsum.parquet"))?;
    let mut times: BTreeSet<NaiveDateTime> = BTreeSet::new();
    for r in price.pricing() {
        if let Some(t) = price.get(r, "SETTLEMENTDATE").t() {
            if window_start <= t && t <= window_end {
                times.insert(t);
            }
        }
    }
    if times.is_empty() {
        anyhow::bail!("bundle has no dispatch prices in its window");
    }
    let times: Vec<NaiveDateTime> = times.into_iter().collect();
    let idx: HashMap<NaiveDateTime, usize> =
        times.iter().enumerate().map(|(i, t)| (*t, i)).collect();
    let n = times.len();
    let intervals: Vec<String> = times.iter().map(|t| iso_market(*t)).collect();
    let mut regions: BTreeMap<String, RegionSeries> = BTreeMap::new();
    let blank = || RegionSeries {
        price: vec![None; n],
        demand: vec![None; n],
        available_generation: vec![None; n],
        net_interchange: vec![None; n],
    };
    for r in price.pricing() {
        let (Some(t), Some(reg)) = (
            price.get(r, "SETTLEMENTDATE").t(),
            price.get(r, "REGIONID").s(),
        ) else {
            continue;
        };
        if let Some(&i) = idx.get(&t) {
            regions.entry(reg.to_string()).or_insert_with(blank).price[i] = price.get(r, "RRP").f();
        }
    }
    for r in sum.pricing() {
        let (Some(t), Some(reg)) = (sum.get(r, "SETTLEMENTDATE").t(), sum.get(r, "REGIONID").s())
        else {
            continue;
        };
        if let Some(&i) = idx.get(&t) {
            let s = regions.entry(reg.to_string()).or_insert_with(blank);
            s.demand[i] = sum.get(r, "TOTALDEMAND").f();
            s.available_generation[i] = sum.get(r, "AVAILABLEGENERATION").f();
            s.net_interchange[i] = sum.get(r, "NETINTERCHANGE").f();
        }
    }
    let series = Series {
        schema: SCHEMA,
        intervals: intervals.clone(),
        regions,
    };

    // Headline over the event span, in the event region.
    let ev = series
        .regions
        .get(&m.region)
        .with_context(|| format!("no {} figures in the bundle", m.region))?;
    let span: Vec<usize> = (0..n)
        .filter(|&i| start <= times[i] && times[i] <= end)
        .collect();
    let mut peak = (f64::NEG_INFINITY, String::new());
    let (mut total, mut count, mut above, mut cost, mut max_demand) = (0.0, 0, 0u32, 0.0, 0.0f64);
    for &i in &span {
        let Some(p) = ev.price[i] else { continue };
        if p > peak.0 {
            peak = (p, intervals[i].clone());
        }
        total += p;
        count += 1;
        if p >= 5000.0 {
            above += 1;
        }
        if let Some(d) = ev.demand[i] {
            cost += p * d / 12.0;
            max_demand = max_demand.max(d);
        }
    }
    if count == 0 {
        anyhow::bail!("no {} prices in the event span", m.region);
    }
    // Tags: AEMO data only (see nem_lens_aemo::event::TAG_KEYS).
    let cap = market_price_cap(start);
    let span_prices: Vec<f64> = span.iter().filter_map(|&i| ev.price[i]).collect();
    let mut tags = vec![
        format!("year:{}", start.year()),
        format!("season:{}", season(start.month())),
        format!("region:{}", m.region),
        format!(
            "severity:{}",
            if peak.0 >= 0.95 * cap {
                "cap"
            } else if peak.0 >= 10_000.0 {
                "extreme"
            } else {
                "high"
            }
        ),
    ];
    if peak.0 >= cap - 0.5 {
        tags.push("price:at-cap".into());
    }
    if span_prices.iter().any(|p| *p < 0.0) {
        tags.push("price:negative".into());
    }
    if span_prices.iter().any(|p| *p <= -999.5) {
        tags.push("price:floor".into());
    }
    let (mut apc, mut suspended, mut intervention) = (false, false, false);
    let (mut mpc, mut mpf) = (false, false);
    for r in &price.rows {
        let (Some(t), Some(reg)) = (
            price.get(r, "SETTLEMENTDATE").t(),
            price.get(r, "REGIONID").s(),
        ) else {
            continue;
        };
        if price.get(r, "INTERVENTION").f().is_some_and(|x| x != 0.0)
            && window_start <= t
            && t <= window_end
        {
            intervention = true;
        }
        if reg == m.region && start <= t && t <= end {
            // APCFLAG is a bitmask: 1 = administered price cap applied,
            // 4 = price capped at the market price cap, 8 = price at the
            // floor. Other bits (16 is common in June 2022) are not
            // tagged until their meaning is confirmed.
            let flag = price.get(r, "APCFLAG").f().unwrap_or(0.0) as i64;
            apc |= flag & 1 != 0;
            mpc |= flag & 4 != 0;
            mpf |= flag & 8 != 0;
            suspended |= price
                .get(r, "MARKETSUSPENDEDFLAG")
                .f()
                .is_some_and(|x| x != 0.0);
        }
    }
    if mpc && !tags.iter().any(|t| t == "price:at-cap") {
        tags.push("price:at-cap".into());
    }
    if mpf && !tags.iter().any(|t| t == "price:floor") {
        tags.push("price:floor".into());
    }
    if apc {
        tags.push("pricing:administered".into());
    }
    if suspended {
        tags.push("pricing:suspended".into());
    }
    if intervention {
        tags.push("intervention:yes".into());
    }
    tags.push(
        if five_minute_settlement(start) {
            "settlement:5-minute"
        } else {
            "settlement:30-minute"
        }
        .into(),
    );
    if !tdir.join("bids_dayoffer.parquet").exists() {
        tags.push("bids:missing".into());
    } else if !tdir.join("bids_offerperiod.parquet").exists() {
        tags.push("bids:no-band-availability".into());
    }
    let headline = Headline {
        schema: SCHEMA,
        event_id: m.event_id.clone(),
        region: m.region.clone(),
        trading_day: trading_day(start).to_string(),
        start: iso_market(start),
        end: iso_market(end),
        duration_minutes: (span.len() * 5) as u32,
        peak_price: round(peak.0, 2),
        peak_at: peak.1,
        mean_price: round(total / count as f64, 2),
        intervals_above_5000: above,
        cost_to_load: round(cost, 0),
        max_demand_mw: round(max_demand, 1),
        tags,
        baseline_mean_price: None,
        scenarios: None,
    };

    // Units: dispatch by fuel and unavailability (next-day data).
    let info = unit_info(&tdir)?;
    let fuel = fuels(&tdir, window_start)?;
    let fuel_of_unit = |duid: &str| -> Option<&'static str> {
        fuel.get(duid).and_then(|f| f.0).or_else(|| {
            info.get(duid)
                .filter(|u| u.dispatch_type == "BIDIRECTIONAL")
                .map(|_| "battery")
        })
    };
    let ud = Tbl::read(&tdir.join("unit_dispatch.parquet"))?;
    let mut fuels_mw: BTreeMap<String, Vec<f64>> = BTreeMap::new();
    let mut avail: BTreeMap<String, Vec<Option<f64>>> = BTreeMap::new();
    for r in ud.pricing() {
        let (Some(t), Some(duid)) = (ud.get(r, "SETTLEMENTDATE").t(), ud.get(r, "DUID").s()) else {
            continue;
        };
        let (Some(&i), Some(u)) = (idx.get(&t), info.get(duid)) else {
            continue;
        };
        if u.region != m.region {
            continue;
        }
        if matches!(u.dispatch_type.as_str(), "GENERATOR" | "BIDIRECTIONAL") {
            let f = fuel_of_unit(duid).unwrap_or("other");
            let mw = ud.get(r, "TOTALCLEARED").f().unwrap_or(0.0);
            fuels_mw
                .entry(f.to_string())
                .or_insert_with(|| vec![0.0; n])[i] += mw;
        }
        avail
            .entry(duid.to_string())
            .or_insert_with(|| vec![None; n])[i] = ud.get(r, "AVAILABILITY").f();
    }
    for v in fuels_mw.values_mut() {
        for x in v.iter_mut() {
            *x = round(*x, 1);
        }
    }
    let dispatch = Dispatch {
        schema: SCHEMA,
        region: m.region.clone(),
        intervals: intervals.clone(),
        fuels: fuels_mw,
    };

    let units = unavailable_units(&tdir, &m.region, &info, &fuel, &avail, window_end, &times)?;
    let unavailability = Unavailability {
        schema: SCHEMA,
        intervals: intervals.clone(),
        units,
    };

    // Interconnectors.
    let ic = Tbl::read(&tdir.join("dispatch_interconnectorres.parquet"))?;
    let mut ics: BTreeMap<String, InterconnectorSeries> = BTreeMap::new();
    for r in ic.pricing() {
        let (Some(t), Some(id)) = (
            ic.get(r, "SETTLEMENTDATE").t(),
            ic.get(r, "INTERCONNECTORID").s(),
        ) else {
            continue;
        };
        let Some(&i) = idx.get(&t) else { continue };
        let s = ics.entry(id.to_string()).or_insert_with(|| {
            let (a, b) = interconnector_ends(id).unwrap_or(("", ""));
            InterconnectorSeries {
                id: id.to_string(),
                from_region: a.into(),
                to_region: b.into(),
                flow: vec![None; n],
                import_limit: vec![None; n],
                export_limit: vec![None; n],
                import_set_by: vec![None; n],
                export_set_by: vec![None; n],
            }
        });
        s.flow[i] = ic.get(r, "MWFLOW").f();
        s.import_limit[i] = ic.get(r, "IMPORTLIMIT").f();
        s.export_limit[i] = ic.get(r, "EXPORTLIMIT").f();
        s.import_set_by[i] = ic.get(r, "IMPORTGENCONID").s().map(str::to_string);
        s.export_set_by[i] = ic.get(r, "EXPORTGENCONID").s().map(str::to_string);
    }
    let interconnectors = Interconnectors {
        schema: SCHEMA,
        intervals: intervals.clone(),
        interconnectors: ics.into_values().collect(),
    };

    // Binding constraints with AEMO's descriptions.
    let gc = Tbl::read(&tdir.join("static_gencondata.parquet"))?;
    let mut desc: HashMap<String, ((NaiveDateTime, i64), String)> = HashMap::new();
    for r in &gc.rows {
        let (Some(id), Some(eff)) = (gc.get(r, "GENCONID").s(), gc.get(r, "EFFECTIVEDATE").t())
        else {
            continue;
        };
        let v = gc.get(r, "VERSIONNO").f().unwrap_or(0.0) as i64;
        let d = gc.get(r, "DESCRIPTION").s().unwrap_or_default().to_string();
        let key = (eff, v);
        if eff <= window_end && desc.get(id).is_none_or(|(k, _)| key > *k) {
            desc.insert(id.trim().to_string(), (key, d));
        }
    }
    let dc = Tbl::read(&tdir.join("dispatch_constraint.parquet"))?;
    let mut cons: BTreeMap<String, Vec<Option<f64>>> = BTreeMap::new();
    for r in dc.pricing() {
        let (Some(t), Some(id)) = (
            dc.get(r, "SETTLEMENTDATE").t(),
            dc.get(r, "CONSTRAINTID").s(),
        ) else {
            continue;
        };
        if let Some(&i) = idx.get(&t) {
            cons.entry(id.to_string()).or_insert_with(|| vec![None; n])[i] =
                dc.get(r, "MARGINALVALUE").f();
        }
    }
    let mut constraints: Vec<ConstraintSeries> = cons
        .into_iter()
        .filter_map(|(id, marginal)| {
            let binding = marginal.iter().flatten().filter(|m| m.abs() > 1e-9).count() as u32;
            (binding > 0).then(|| {
                let description = desc.get(&id).map(|d| d.1.clone()).unwrap_or_default();
                ConstraintSeries {
                    outage: is_outage_description(&description),
                    id,
                    description,
                    marginal,
                    intervals_binding: binding,
                }
            })
        })
        .collect();
    constraints.sort_by(|a, b| {
        b.intervals_binding
            .cmp(&a.intervals_binding)
            .then(a.id.cmp(&b.id))
    });
    let constraints = Constraints {
        schema: SCHEMA,
        intervals: intervals.clone(),
        constraints,
    };

    // Market notices issued from six hours before the window to its end.
    let mut notices: Vec<Notice> = match fs::read(tdir.join(NOTICES_FILE)) {
        Ok(b) => serde_json::from_slice::<Vec<FetchedNotice>>(&b)?
            .into_iter()
            .map(|f| f.notice)
            .collect(),
        Err(_) => Vec::new(),
    };
    let from = window_start - Duration::hours(6);
    notices.retain(|x| {
        nem_lens_aemo::time::parse_market(&x.issued).is_some_and(|t| from <= t && t <= window_end)
    });
    notices.sort_by(|a, b| a.issued.cmp(&b.issued).then(a.id.cmp(&b.id)));
    let notices = Notices {
        schema: SCHEMA,
        notices,
    };

    Ok(PublicFiles {
        headline,
        series,
        dispatch,
        interconnectors,
        constraints,
        unavailability,
        notices,
    })
}

/// Scheduled generators in `region` (not semi-scheduled, not batteries)
/// of at least 30 MW whose AEMO availability was zero throughout the window
/// ("outage") or averaged at least max(30 MW, 10%) below capacity
/// ("derated"), with the rebid explanations in force. Units with no
/// registered energy source in GENUNITS (AEMO's dummy generators, such as
/// DG_NSW1) are left out when the bundle has GENUNITS.
fn unavailable_units(
    dir: &Path,
    region: &str,
    info: &HashMap<String, UnitInfo>,
    fuel: &HashMap<String, (Option<&'static str>, f64)>,
    avail: &BTreeMap<String, Vec<Option<f64>>>,
    window_end: NaiveDateTime,
    times: &[NaiveDateTime],
) -> Result<Vec<UnitAvailability>> {
    // Physical availability offered (PASAAVAILABILITY), a fallback capacity.
    let periods = Tbl::read(&dir.join("bids_offerperiod.parquet"))?;
    let mut pasa: HashMap<String, f64> = HashMap::new();
    for r in &periods.rows {
        if periods.get(r, "BIDTYPE").s() != Some("ENERGY") {
            continue;
        }
        if let (Some(d), Some(p)) = (
            periods.get(r, "DUID").s(),
            periods.get(r, "PASAAVAILABILITY").f(),
        ) {
            let e = pasa.entry(d.to_string()).or_default();
            *e = e.max(p);
        }
    }
    let days: BTreeSet<_> = times.iter().map(|t| trading_day(*t)).collect();
    let offers = Tbl::read(&dir.join("bids_dayoffer.parquet"))?;
    let mut rebids: HashMap<String, Vec<(NaiveDateTime, String)>> = HashMap::new();
    for r in &offers.rows {
        if offers.get(r, "BIDTYPE").s() != Some("ENERGY") {
            continue;
        }
        let (Some(duid), Some(day), Some(at)) = (
            offers.get(r, "DUID").s(),
            offers.get(r, "SETTLEMENTDATE").t(),
            offers.get(r, "OFFERDATE").t(),
        ) else {
            continue;
        };
        let text = offers
            .get(r, "REBIDEXPLANATION")
            .s()
            .unwrap_or_default()
            .trim()
            .to_string();
        if at <= window_end && days.contains(&day.date()) && !text.is_empty() {
            rebids.entry(duid.to_string()).or_default().push((at, text));
        }
    }
    let mut out = Vec::new();
    for (duid, series) in avail {
        let Some(u) = info.get(duid) else { continue };
        if u.region != region || u.dispatch_type != "GENERATOR" || u.schedule_type != "SCHEDULED" {
            continue;
        }
        let (f, reg_cap) = fuel.get(duid).copied().unwrap_or((None, 0.0));
        if f.is_none() && !fuel.is_empty() {
            continue;
        }
        let cap = if reg_cap > 0.0 {
            reg_cap
        } else {
            pasa.get(duid).copied().unwrap_or(0.0)
        };
        let vals: Vec<f64> = series.iter().flatten().copied().collect();
        if cap < 30.0 || vals.is_empty() {
            continue;
        }
        let max = vals.iter().copied().fold(0.0, f64::max);
        let mean = vals.iter().sum::<f64>() / vals.len() as f64;
        let kind = if max < 1.0 {
            "outage"
        } else if cap - mean >= (0.1 * cap).max(30.0) {
            "derated"
        } else {
            continue;
        };
        let mut rb = rebids.remove(duid).unwrap_or_default();
        rb.sort();
        rb.dedup_by(|a, b| a.1 == b.1);
        let keep = rb.len().saturating_sub(5);
        out.push(UnitAvailability {
            duid: duid.clone(),
            region: u.region.clone(),
            fuel: f.map(str::to_string),
            capacity_mw: round(cap, 1),
            availability: series.clone(),
            kind: kind.into(),
            rebids: rb
                .into_iter()
                .skip(keep)
                .map(|(t, e)| Rebid {
                    offer_time: iso_market(t),
                    explanation: e,
                })
                .collect(),
        });
    }
    // Largest shortfall first.
    let short = |u: &UnitAvailability| {
        let v: Vec<f64> = u.availability.iter().flatten().copied().collect();
        u.capacity_mw - v.iter().sum::<f64>() / v.len().max(1) as f64
    };
    out.sort_by(|a, b| short(b).total_cmp(&short(a)).then(a.duid.cmp(&b.duid)));
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_coerce_across_column_types() {
        assert_eq!(V::I(5).f(), Some(5.0));
        assert_eq!(V::S(" 2.5 ".into()).f(), Some(2.5));
        assert_eq!(V::Null.f(), None);
        assert_eq!(V::S("x".into()).s(), Some("x"));
    }

    #[test]
    fn tables_dir_prefers_the_record_layout() {
        let root = std::env::temp_dir().join(format!("nem-extract-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root).unwrap();
        assert_eq!(tables_dir(&root), root);
        fs::create_dir_all(root.join("bundle")).unwrap();
        assert_eq!(tables_dir(&root), root.join("bundle"));
        fs::remove_dir_all(&root).unwrap();
    }
}

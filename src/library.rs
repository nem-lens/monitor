//! The public event library: headline results and browser inputs for each
//! event, as static files for GitHub Pages.
//!
//! ```text
//! LIB/index.json                         every event's headline, newest first
//! LIB/events/<id>/headline.json          the headline (see [`Headline`])
//! LIB/events/<id>/snapshots.json.gz      engine intervals for the event span, for nemcf-wasm
//! LIB/events/<id>/scenarios/<name>.json  the scenarios run, as starting points
//! LIB/events/<id>/scenarios/index.json   {schema, scenarios:[{name, file}]}, headline order
//! LIB/events/<id>/manifest.json          the bundle manifest: AEMO sources and hashes
//! ```
//!
//! Only the fields in [`Headline`] are taken from the replay results; the
//! rest of what the engine writes stays with the bundle. Headline figures
//! cover the event span: intervals ending from the event's start to its end,
//! inclusive, in the event's region. The browser computes a dropped
//! scenario's headline the same way.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use chrono::{Duration, NaiveDateTime};
use nem_lens_aemo::time::{iso_market, parse_market, trading_day};
use serde::{Deserialize, Serialize};

use crate::bundle::{self, Manifest};

pub const SCHEMA: u32 = 1;
const BASELINE: &str = "baseline";

const README: &str = "# NEM Lens event library

Headline results and replay inputs for NEM price events detected by
[nem-monitor](https://github.com/nem-lens/monitor), replayed with
[nemcf](https://github.com/nem-lens/engine). Written by
`nem-monitor library add`; do not edit by hand.

- `index.json`: every event's headline, newest first.
- `events/<id>/headline.json`: peak price, duration, baseline replay mean
  price and each scenario's change in mean price, over the event span.
- `events/<id>/snapshots.json.gz`: the engine's intervals for the event span,
  for running scenarios in the browser.
- `events/<id>/scenarios/`: the scenarios run; `index.json` lists each
  name and file, in headline order.
- `events/<id>/manifest.json`: the AEMO source files and their SHA-256.

Times are interval ends in market time with the offset
(`2026-10-12T14:20:00+10:00`); compare them as times, not as strings with
the offset stripped from one side.

Source data © AEMO, used under AEMO's copyright permissions. Not affiliated
with AEMO.
";

/// The published summary of one event. Field changes need a schema bump.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Headline {
    pub schema: u32,
    pub event_id: String,
    pub region: String,
    pub trading_day: String,
    pub start: String,
    pub end: String,
    /// Highest historical regional price in the span, $/MWh.
    pub peak_price: f64,
    pub duration_minutes: u32,
    /// Mean replayed baseline price over the span, $/MWh.
    pub baseline_mean_price: f64,
    pub scenarios: Vec<ScenarioHeadline>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScenarioHeadline {
    pub name: String,
    /// Scenario mean price minus baseline mean price over the span, $/MWh.
    pub mean_price_change: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Index {
    pub schema: u32,
    pub events: Vec<Headline>,
}

/// Where `add` reads from.
pub struct Inputs<'a> {
    pub bundle: &'a Path,
    /// nemcf replay output: prices.csv, and snapshots.json unless `snapshots` is set.
    pub replay: &'a Path,
    pub snapshots: Option<&'a Path>,
    /// Scenario files; those whose `name` was replayed are published.
    pub scenarios: Option<&'a Path>,
}

/// Interval ends of the event itself, recovered from the bundle window
/// (see [`bundle::Window::for_event`]).
pub fn event_span(m: &Manifest) -> Result<(NaiveDateTime, NaiveDateTime)> {
    let ws = parse_market(&m.window_start).context("bad window_start in manifest")?;
    let we = parse_market(&m.window_end).context("bad window_end in manifest")?;
    let (start, end) = (ws + Duration::hours(2), we - Duration::hours(1));
    if end < start {
        bail!("bundle window is shorter than its margins");
    }
    Ok((start, end))
}

fn intervals(start: NaiveDateTime, end: NaiveDateTime) -> usize {
    ((end - start).num_minutes() / 5 + 1) as usize
}

fn round2(x: f64) -> f64 {
    (x * 100.0).round() / 100.0
}

/// Baseline mean and each complete scenario's change, from nemcf's
/// prices.csv. A run missing any span interval is left out.
pub fn headline_prices(
    csv_text: &str,
    region: &str,
    start: NaiveDateTime,
    end: NaiveDateTime,
) -> Result<(f64, Vec<ScenarioHeadline>)> {
    let mut rdr = csv::Reader::from_reader(csv_text.as_bytes());
    let headers = rdr.headers()?.clone();
    let col = |name: &str| {
        headers
            .iter()
            .position(|h| h == name)
            .with_context(|| format!("prices.csv has no {name} column"))
    };
    let (ct, cr, crun, cp) = (
        col("settlement")?,
        col("region")?,
        col("run")?,
        col("price")?,
    );
    // run -> interval -> price; first-seen run order is kept for output.
    let mut order: Vec<String> = Vec::new();
    let mut runs: BTreeMap<String, BTreeMap<NaiveDateTime, f64>> = BTreeMap::new();
    for row in rdr.records() {
        let row = row?;
        if row.get(cr) != Some(region) {
            continue;
        }
        let Some(t) = row.get(ct).and_then(parse_market) else {
            bail!("bad settlement in prices.csv: {:?}", row.get(ct));
        };
        if t < start || t > end {
            continue;
        }
        let run = row.get(crun).unwrap_or_default().to_string();
        let price: f64 = row
            .get(cp)
            .unwrap_or_default()
            .parse()
            .with_context(|| format!("bad price for {run} at {t}"))?;
        if !runs.contains_key(&run) {
            order.push(run.clone());
        }
        runs.entry(run).or_default().insert(t, price);
    }
    let n = intervals(start, end);
    let mean = |m: &BTreeMap<NaiveDateTime, f64>| m.values().sum::<f64>() / m.len() as f64;
    let base = runs
        .get(BASELINE)
        .filter(|m| m.len() == n)
        .with_context(|| format!("prices.csv has no complete baseline for {region}"))?;
    let base_mean = mean(base);
    let mut scenarios = Vec::new();
    for name in order.iter().filter(|r| *r != BASELINE) {
        let m = &runs[name];
        if m.len() != n {
            tracing::warn!("{name}: {} of {n} intervals; not published", m.len());
            continue;
        }
        scenarios.push(ScenarioHeadline {
            name: name.clone(),
            mean_price_change: round2(mean(m) - base_mean),
        });
    }
    Ok((round2(base_mean), scenarios))
}

/// The intervals of a nemcf snapshots file that end within the span, in the
/// file's own form: a plain array of intervals, or packed
/// (`{schema, constraints, intervals}`, whose shared equations are kept
/// whole so the intervals' indexes stay valid). Every span interval must be
/// present.
pub fn trim_snapshots(json: &[u8], start: NaiveDateTime, end: NaiveDateTime) -> Result<Vec<u8>> {
    let mut doc: serde_json::Value = serde_json::from_slice(json).context("snapshots.json")?;
    let all = match &mut doc {
        serde_json::Value::Array(a) => a,
        serde_json::Value::Object(o) => match o.get_mut("intervals") {
            Some(serde_json::Value::Array(a)) => a,
            _ => bail!("packed snapshots have no intervals array"),
        },
        _ => bail!("snapshots.json is neither an array nor packed"),
    };
    let mut seen = BTreeSet::new();
    all.retain(|iv| {
        let t = iv
            .get("settlement")
            .and_then(|s| s.as_str())
            .and_then(parse_market);
        match t {
            Some(t) if start <= t && t <= end => seen.insert(t),
            _ => false,
        }
    });
    let n = intervals(start, end);
    if seen.len() != n {
        bail!(
            "snapshots cover {} of the {n} event intervals ({} to {})",
            seen.len(),
            iso_market(start),
            iso_market(end)
        );
    }
    Ok(serde_json::to_vec(&doc)?)
}

fn gzip(data: &[u8]) -> Result<Vec<u8>> {
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::best());
    enc.write_all(data)?;
    Ok(enc.finish()?)
}

fn safe_id(id: &str) -> Result<&str> {
    let ok = !id.is_empty()
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    if !ok {
        bail!("unexpected event id {id:?}");
    }
    Ok(id)
}

fn write_json<T: Serialize>(path: &Path, v: &T) -> Result<()> {
    let mut text = serde_json::to_string_pretty(v)?;
    text.push('\n');
    fs::write(path, text).with_context(|| format!("writing {}", path.display()))
}

/// Publish one event into the library at `lib`, replacing any earlier
/// entry for it, and rebuild the index.
pub fn add(inputs: &Inputs, lib: &Path) -> Result<Headline> {
    let m = Manifest::load(inputs.bundle)?;
    let id = safe_id(&m.event_id)?;
    let (start, end) = event_span(&m)?;

    let peak_price = bundle::read_series(inputs.bundle, &m.region)?
        .iter()
        .filter(|p| parse_market(&p.t).is_some_and(|t| start <= t && t <= end))
        .map(|p| p.rrp)
        .fold(f64::NEG_INFINITY, f64::max);
    if !peak_price.is_finite() {
        bail!("bundle has no {} prices in the event span", m.region);
    }

    let prices_path = inputs.replay.join("prices.csv");
    let prices = fs::read_to_string(&prices_path)
        .with_context(|| format!("reading {}", prices_path.display()))?;
    let (baseline_mean_price, scenarios) = headline_prices(&prices, &m.region, start, end)?;

    let snap_path: PathBuf = inputs
        .snapshots
        .map(Path::to_path_buf)
        .unwrap_or_else(|| inputs.replay.join("snapshots.json"));
    let snap = fs::read(&snap_path).with_context(|| format!("reading {}", snap_path.display()))?;
    let snap = gzip(&trim_snapshots(&snap, start, end)?)?;

    let headline = Headline {
        schema: SCHEMA,
        event_id: id.to_string(),
        region: m.region.clone(),
        trading_day: trading_day(start).to_string(),
        start: iso_market(start),
        end: iso_market(end),
        peak_price: round2(peak_price),
        duration_minutes: (intervals(start, end) * 5) as u32,
        baseline_mean_price,
        scenarios,
    };

    let dir = lib.join("events").join(id);
    if dir.exists() {
        fs::remove_dir_all(&dir)?;
    }
    fs::create_dir_all(&dir)?;
    write_json(&dir.join("headline.json"), &headline)?;
    fs::write(dir.join("snapshots.json.gz"), snap)?;
    fs::copy(
        inputs.bundle.join(bundle::MANIFEST),
        dir.join(bundle::MANIFEST),
    )?;
    if let Some(src) = inputs.scenarios {
        copy_scenarios(src, &dir.join("scenarios"), &headline.scenarios)?;
    }
    fs::write(lib.join("README.md"), README)?;
    write_index(lib)?;
    Ok(headline)
}

/// Copy the scenario files whose `name` was published.
/// The browser cannot list a folder, so scenarios/index.json names each
/// file, in headline order. A name in several files takes the first by file
/// name, as nemcf does.
fn copy_scenarios(src: &Path, dst: &Path, published: &[ScenarioHeadline]) -> Result<()> {
    let names: BTreeSet<&str> = published.iter().map(|s| s.name.as_str()).collect();
    let mut files: BTreeMap<String, String> = BTreeMap::new();
    let mut entries: Vec<PathBuf> = fs::read_dir(src)
        .with_context(|| format!("reading {}", src.display()))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .collect();
    entries.sort();
    for p in entries {
        let v: serde_json::Value = match fs::read(&p)
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
        {
            Some(v) => v,
            None => continue,
        };
        let Some(name) = v.get("name").and_then(|n| n.as_str()) else {
            continue;
        };
        let file = p
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        if names.contains(name) && !files.contains_key(name) {
            fs::create_dir_all(dst)?;
            fs::copy(&p, dst.join(&file))?;
            files.insert(name.to_string(), file);
        }
    }
    if files.is_empty() {
        return Ok(());
    }
    let list: Vec<ScenarioFile> = published
        .iter()
        .filter_map(|s| {
            files.get(&s.name).map(|f| ScenarioFile {
                name: s.name.clone(),
                file: f.clone(),
            })
        })
        .collect();
    write_json(
        &dst.join("index.json"),
        &ScenarioIndex {
            schema: SCHEMA,
            scenarios: list,
        },
    )
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ScenarioIndex {
    pub schema: u32,
    pub scenarios: Vec<ScenarioFile>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ScenarioFile {
    pub name: String,
    pub file: String,
}

/// Rebuild index.json from every events/<id>/headline.json, newest first.
pub fn write_index(lib: &Path) -> Result<Index> {
    let mut events = Vec::new();
    let root = lib.join("events");
    if root.is_dir() {
        for e in fs::read_dir(&root)? {
            let p = e?.path().join("headline.json");
            if p.is_file() {
                let text = fs::read_to_string(&p)?;
                events.push(
                    serde_json::from_str::<Headline>(&text)
                        .with_context(|| format!("parsing {}", p.display()))?,
                );
            }
        }
    }
    events.sort_by(|a, b| b.start.cmp(&a.start).then(a.event_id.cmp(&b.event_id)));
    let index = Index {
        schema: SCHEMA,
        events,
    };
    write_json(&lib.join("index.json"), &index)?;
    Ok(index)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t(s: &str) -> NaiveDateTime {
        parse_market(s).unwrap()
    }

    const PRICES: &str = "settlement,region,run,price,historical_price,price_setter,deficit_mw
2026-10-12T14:15:00+10:00,NSW1,baseline,900,800,X b1,0.0
2026-10-12T14:20:00+10:00,NSW1,baseline,1000,900,X b1,0.0
2026-10-12T14:20:00+10:00,QLD1,baseline,50,40,Y b1,0.0
2026-10-12T14:25:00+10:00,NSW1,baseline,3000,2900,X b2,0.0
2026-10-12T14:20:00+10:00,NSW1,coal back,100,900,Z b1,0.0
2026-10-12T14:25:00+10:00,NSW1,coal back,200,2900,Z b1,0.0
2026-10-12T14:20:00+10:00,NSW1,partial,100,900,Z b1,0.0
";

    #[test]
    fn headline_covers_the_span_and_skips_incomplete_runs() {
        let (base, sc) = headline_prices(
            PRICES,
            "NSW1",
            t("2026-10-12T14:20:00"),
            t("2026-10-12T14:25:00"),
        )
        .unwrap();
        assert_eq!(base, 2000.0);
        assert_eq!(
            sc,
            vec![ScenarioHeadline {
                name: "coal back".into(),
                mean_price_change: -1850.0
            }]
        );
    }

    #[test]
    fn headline_needs_a_complete_baseline() {
        let err = headline_prices(
            PRICES,
            "NSW1",
            t("2026-10-12T14:20:00"),
            t("2026-10-12T14:30:00"),
        )
        .unwrap_err();
        assert!(err.to_string().contains("baseline"), "{err}");
    }

    #[test]
    fn snapshots_are_trimmed_to_the_span() {
        let json = br#"[{"settlement":"2026-10-12T14:15:00+10:00","x":1},
            {"settlement":"2026-10-12T14:20:00+10:00","x":2},
            {"settlement":"2026-10-12T14:25:00+10:00","x":3}]"#;
        let out = trim_snapshots(json, t("2026-10-12T14:20:00"), t("2026-10-12T14:25:00")).unwrap();
        let v: Vec<serde_json::Value> = serde_json::from_slice(&out).unwrap();
        assert_eq!(v.len(), 2);
        assert_eq!(v[0]["x"], 2);
        assert!(trim_snapshots(json, t("2026-10-12T14:20:00"), t("2026-10-12T14:30:00")).is_err());
    }

    #[test]
    fn packed_snapshots_keep_their_shared_equations() {
        let json = br#"{"schema":1,"constraints":[{"id":"C1"}],"intervals":[
            {"settlement":"2026-10-12T14:15:00+10:00","constraint_rhs":[[0,1.0]]},
            {"settlement":"2026-10-12T14:20:00+10:00","constraint_rhs":[[0,2.0]]}]}"#;
        let out = trim_snapshots(json, t("2026-10-12T14:20:00"), t("2026-10-12T14:20:00")).unwrap();
        let v: serde_json::Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["schema"], 1);
        assert_eq!(v["constraints"][0]["id"], "C1");
        assert_eq!(v["intervals"].as_array().unwrap().len(), 1);
        assert_eq!(v["intervals"][0]["constraint_rhs"][0][1], 2.0);
    }

    #[test]
    fn index_is_newest_first() {
        let lib = std::env::temp_dir().join(format!("nem-lib-{}", std::process::id()));
        let _ = fs::remove_dir_all(&lib);
        for (id, start) in [
            ("a", "2026-10-01T10:00:00+10:00"),
            ("b", "2026-10-12T14:20:00+10:00"),
        ] {
            let dir = lib.join("events").join(id);
            fs::create_dir_all(&dir).unwrap();
            let h = Headline {
                schema: SCHEMA,
                event_id: id.into(),
                region: "NSW1".into(),
                trading_day: "2026-10-12".into(),
                start: start.into(),
                end: start.into(),
                peak_price: 1.0,
                duration_minutes: 5,
                baseline_mean_price: 1.0,
                scenarios: vec![],
            };
            write_json(&dir.join("headline.json"), &h).unwrap();
        }
        let idx = write_index(&lib).unwrap();
        let ids: Vec<&str> = idx.events.iter().map(|e| e.event_id.as_str()).collect();
        assert_eq!(ids, ["b", "a"]);
        fs::remove_dir_all(&lib).unwrap();
    }

    #[test]
    fn scenario_index_follows_headline_order() {
        let root = std::env::temp_dir().join(format!("nem-lib-sc-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let (src, dst) = (root.join("src"), root.join("dst"));
        fs::create_dir_all(&src).unwrap();
        for (file, name) in [
            ("a.json", "zeta"),
            ("b.json", "alpha"),
            ("c.json", "alpha"),
            ("d.json", "not run"),
        ] {
            fs::write(src.join(file), format!(r#"{{"name":"{name}","ops":[]}}"#)).unwrap();
        }
        fs::write(src.join("bad.json"), "{").unwrap();
        let published = ["alpha", "zeta"].map(|n| ScenarioHeadline {
            name: n.into(),
            mean_price_change: 0.0,
        });
        copy_scenarios(&src, &dst, &published).unwrap();
        let idx: ScenarioIndex =
            serde_json::from_str(&fs::read_to_string(dst.join("index.json")).unwrap()).unwrap();
        let got: Vec<(&str, &str)> = idx
            .scenarios
            .iter()
            .map(|s| (s.name.as_str(), s.file.as_str()))
            .collect();
        assert_eq!(got, [("alpha", "b.json"), ("zeta", "a.json")]);
        assert!(!dst.join("c.json").exists() && !dst.join("d.json").exists());
        fs::remove_dir_all(&root).unwrap();
    }

    #[test]
    fn event_ids_are_path_safe() {
        assert!(safe_id("202610121420-NSW1-high").is_ok());
        assert!(safe_id("../x").is_err());
        assert!(safe_id("").is_err());
    }
}

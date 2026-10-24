//! The public event library: the replay dashboard's data, as static files
//! for GitHub Pages. It is the public view of the event record
//! (`nem_lens_aemo::event`, schema 2): only what is directly derived from
//! AEMO's published figures, for real events the monitor detected.
//!
//! ```text
//! LIB/index.json                        every event's headline, newest first
//! LIB/events/<id>/headline.json         AEMO headline figures over the event span
//! LIB/events/<id>/manifest.json         AEMO sources and SHA-256
//! LIB/events/<id>/series.json           regional price, demand, availability, interchange
//! LIB/events/<id>/dispatch.json         dispatch by fuel in the event region
//! LIB/events/<id>/interconnectors.json  flows, AEMO limits and what set them
//! LIB/events/<id>/constraints.json      binding constraints, AEMO marginal values
//! LIB/events/<id>/unavailability.json   units on outage or derated, rebid explanations
//! LIB/events/<id>/notices.json          AEMO market notices around the event
//! ```
//!
//! Every file passes `event::check_public_file` before it is written, so a
//! private field or file cannot reach the library. Nothing here reads
//! engine output.

use std::fs;
use std::path::Path;

use anyhow::{bail, Context, Result};
use chrono::{Duration, NaiveDateTime};
use nem_lens_aemo::event::{check_public_file, Headline, Index, SCHEMA};
use nem_lens_aemo::time::parse_market;

use crate::bundle::{self, Manifest};
use crate::extract;

const README: &str = "# NEM Lens event library

Price events in Australia's National Electricity Market detected by
[nem-monitor](https://github.com/nem-lens/monitor), with AEMO's own figures
for each: the public view of the NEM Lens event record (schema 2). Written
by `nem-monitor library add`; do not edit by hand.

- `index.json`: every event's headline, newest first.
- `events/<id>/headline.json`: peak and mean price, intervals at or above
  $5,000/MWh, cost to load (price x demand) and maximum demand over the
  event span.
- `events/<id>/series.json`: each region's price, demand, available
  generation and net interchange per interval, and its contingency reserve
  ratio (below).
- `events/<id>/dispatch.json`: dispatch by fuel in the event region.
- `events/<id>/interconnectors.json`: flows, AEMO's limits and the
  constraints that set them.
- `events/<id>/constraints.json`: binding generic constraints with AEMO's
  marginal values and descriptions.
- `events/<id>/unavailability.json`: units on outage or derated, with their
  rebid explanations.
- `events/<id>/notices.json`: AEMO market notices around the event.
- `events/<id>/manifest.json`: the AEMO source files and their SHA-256.

Times are interval ends in market time with the offset
(`2026-10-12T14:20:00+10:00`); compare them as times. Earlier schema 1
files (replay figures, snapshots, scenarios) remain in this repository's
history only.

## Contingency reserve ratio

The contingency reserve ratio (CRR) is inspired by WattClarity's
Instantaneous Reserve Plant Margin (IRPM), which Global-Roam has shown in
NEMwatch since the early years of the NEM: surplus available generation
across the NEM divided by aggregate demand, a real-time, non-price
indicator of how tight supply is ([WattClarity on the
IRPM](https://wattclarity.com.au/?p=22205)). The CRR keeps the IRPM's idea
of reading tightness from available plant in each dispatch interval, but
asks a narrower question: could the region replace the loss of its largest
single element?

| | IRPM (WattClarity) | CRR (here) |
| --- | --- | --- |
| Area | The whole NEM, or an economic island | One region, with room to raise imports up to AEMO's interconnector limits |
| Spare | Available generation less demand | Every other unit's headroom (availability less dispatch), plus import room, less the tripped element's own headroom |
| Scaled by | Demand | The largest single contingency: the biggest unit's dispatched output (DUID), or a DC link's flow into the region |
| Reading | A percentage; under about 15% NEM-wide, volatility is likely | A ratio; below 1, the region could not have replaced that loss |

Why it differs:

- **Scaled by the largest contingency, not demand.** Security and reserve
  are judged against credible contingencies, in the spirit of AEMO's LOR2
  (reserve below the largest credible contingency). The same percentage of
  demand means a different thing in South Australia and in New South Wales.
- **Per region, with AEMO's import limits.** Library events are regional.
  AEMO's limits already include the network constraints on imports, which
  covers some of what economic islands do for the IRPM.
- **Units as DUIDs, at their dispatched output.** The ratio moves with
  dispatch. A DUID can aggregate machines, and transmission protection
  often trips a whole station.
- **DC links count, AC links do not.** Losing Basslink, Murraylink or
  Directlink is a single loss to the importing region. AC links are
  network contingencies, left out like other network outages.
- **Availability, not deliverability.** As in the IRPM, ramp rates and
  whether slow-start plant is online are not considered.

Limits: plant and DC links only, so an event driven by a network
constraint can show a high CRR at the price cap. Import room assumes the
neighbouring regions have plant to send. It is about energy cover after a
trip, not frequency control. It is arithmetic on AEMO's DISPATCH
UNIT_SOLUTION and INTERCONNECTORRES (pricing run).

Source data © AEMO, used under AEMO's copyright permissions. Not affiliated
with AEMO.
";

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

/// Publish the event in `dir` (a bundle, or a record in the private store)
/// into the library at `lib`, replacing any earlier entry for it, and
/// rebuild the indexes.
pub fn add(dir: &Path, lib: &Path) -> Result<Headline> {
    let h = add_event(dir, lib)?;
    write_index(lib)?;
    Ok(h)
}

/// Publish every record in the private store at `store`, then rebuild the
/// indexes once. Records that fail are reported and skipped.
pub fn rebuild(store: &Path, lib: &Path) -> Result<(usize, Vec<String>)> {
    let mut dirs: Vec<_> = fs::read_dir(store)?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.join(bundle::MANIFEST).is_file())
        .collect();
    dirs.sort();
    let (mut ok, mut failed) = (0, Vec::new());
    for d in &dirs {
        match add_event(d, lib) {
            Ok(_) => ok += 1,
            Err(e) => failed.push(format!("{}: {e:#}", d.display())),
        }
    }
    write_index(lib)?;
    Ok((ok, failed))
}

/// Write one event's public files, without touching the indexes.
fn add_event(dir: &Path, lib: &Path) -> Result<Headline> {
    let m = Manifest::load(dir)?;
    let id = safe_id(&m.event_id)?.to_string();
    let files = extract::build(dir)?;
    let mut out = files.to_json()?;
    out.push(("manifest.json", fs::read(dir.join(bundle::MANIFEST))?));
    // The gate: every file, before anything is written.
    for (name, bytes) in &out {
        check_public_file(name, bytes).with_context(|| format!("refusing to publish {name}"))?;
    }
    let target = lib.join("events").join(&id);
    if target.exists() {
        fs::remove_dir_all(&target)?;
    }
    fs::create_dir_all(&target)?;
    for (name, bytes) in &out {
        fs::write(target.join(name), bytes)
            .with_context(|| format!("writing {}", target.join(name).display()))?;
    }
    fs::write(lib.join("README.md"), README)?;
    Ok(files.headline.public_view())
}

/// Rebuild index.json from every `events/<id>/headline.json`, newest first,
/// and `years/<YYYY>.json` with each year's events.
pub fn write_index(lib: &Path) -> Result<Index> {
    let mut events = Vec::new();
    let root = lib.join("events");
    if root.is_dir() {
        for e in fs::read_dir(&root)? {
            let p = e?.path().join("headline.json");
            if p.is_file() {
                let text = fs::read_to_string(&p)?;
                let h: Headline = serde_json::from_str(&text)
                    .with_context(|| format!("{}: not a schema {SCHEMA} headline", p.display()))?;
                events.push(h.public_view());
            }
        }
    }
    events.sort_by(|a, b| b.start.cmp(&a.start).then(a.event_id.cmp(&b.event_id)));
    let index = Index {
        schema: SCHEMA,
        events,
    };
    let write = |path: &Path, idx: &Index| -> Result<()> {
        let mut text = serde_json::to_vec_pretty(idx)?;
        text.push(b'\n');
        // The index holds headlines only: the headline gate applies.
        check_public_file("headline.json", &text).context("index")?;
        fs::write(path, text)?;
        Ok(())
    };
    write(&lib.join("index.json"), &index)?;
    let years = lib.join("years");
    if years.exists() {
        fs::remove_dir_all(&years)?;
    }
    fs::create_dir_all(&years)?;
    let mut by_year: std::collections::BTreeMap<String, Vec<Headline>> = Default::default();
    for e in &index.events {
        by_year
            .entry(e.start.get(..4).unwrap_or("unknown").to_string())
            .or_default()
            .push(e.clone());
    }
    for (y, events) in by_year {
        write(
            &years.join(format!("{y}.json")),
            &Index {
                schema: SCHEMA,
                events,
            },
        )?;
    }
    Ok(index)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headline(id: &str, start: &str) -> Headline {
        Headline {
            schema: SCHEMA,
            event_id: id.into(),
            region: "NSW1".into(),
            trading_day: "2026-10-12".into(),
            start: start.into(),
            end: start.into(),
            duration_minutes: 5,
            peak_price: 1.0,
            peak_at: start.into(),
            mean_price: 1.0,
            intervals_above_5000: 0,
            cost_to_load: 1.0,
            max_demand_mw: 1.0,
            tags: vec!["year:2026".into()],
            baseline_mean_price: Some(9.0),
            scenarios: None,
        }
    }

    #[test]
    fn index_is_newest_first_and_public() {
        let lib = std::env::temp_dir().join(format!("nem-lib-{}", std::process::id()));
        let _ = fs::remove_dir_all(&lib);
        for (id, start) in [
            ("a", "2026-10-01T10:00:00+10:00"),
            ("b", "2026-10-12T14:20:00+10:00"),
        ] {
            let dir = lib.join("events").join(id);
            fs::create_dir_all(&dir).unwrap();
            fs::write(
                dir.join("headline.json"),
                serde_json::to_vec(&headline(id, start)).unwrap(),
            )
            .unwrap();
        }
        let idx = write_index(&lib).unwrap();
        let ids: Vec<&str> = idx.events.iter().map(|e| e.event_id.as_str()).collect();
        assert_eq!(ids, ["b", "a"]);
        // A private field in a stored headline never reaches the index.
        let text = fs::read_to_string(lib.join("index.json")).unwrap();
        assert!(!text.contains("baseline_mean_price"));
        assert!(lib.join("years").join("2026.json").exists());
        fs::remove_dir_all(&lib).unwrap();
    }

    #[test]
    fn event_ids_are_path_safe() {
        assert!(safe_id("202610121420-NSW1-high").is_ok());
        assert!(safe_id("../x").is_err());
        assert!(safe_id("").is_err());
    }
}

//! Historical events from AEMO's MMSDM monthly archive.
//!
//! `history scan` replays every dispatch interval of the archived months
//! (DISPATCHPRICE and DISPATCHREGIONSUM, pricing run) through the same
//! [`Detector`] the live monitor runs, so historical events are defined
//! exactly as live ones. The market price cap moves each financial year,
//! and the detector's `cap` severity follows it.
//!
//! The archive is read from a local tree laid out as
//! `<root>/<YYYY_MM>/<TABLE>/*.zip` (or `.CSV`).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{NaiveDate, NaiveDateTime};
use nem_lens_aemo::event::Notice;
use nem_lens_aemo::files::visit_csvs;

/// Monthly archive members are streamed, and DISPATCHCONSTRAINT unzips to
/// several GB; the 1 GB limit for live NEMWeb files does not fit them.
const ARCHIVE_MEMBER_BYTES: u64 = 64 << 30;
use nem_lens_aemo::time::{iso_market, parse_market};
use nem_lens_aemo::{visit_report, DispatchSnapshot, RegionInterval};
use serde::{Deserialize, Serialize};

use crate::bundle::{NextDay, Output, Source, TableBuf, Window};
use crate::config::DetectCfg;
use crate::detect::Detector;
use crate::extract::{FetchedNotice, NOTICES_FILE};
use crate::model::{Event, EventKind, TransitionKind};

pub use nem_lens_aemo::event::market_price_cap;

/// A closed historical event with what the archive says about how its
/// prices were set.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoricalEvent {
    pub event: Event,
    /// Market price cap in force at the event's start, $/MWh.
    pub market_price_cap: f64,
    /// Intervals in the event (start to end, its region) with the
    /// administered price cap applied (APCFLAG bit 1).
    pub apc_intervals: u32,
    /// Intervals in the event with the market suspended (MARKETSUSPENDEDFLAG = 1).
    pub suspended_intervals: u32,
}

/// `YYYY_MM` folders from `from` to `to` inclusive.
pub fn months(from: &str, to: &str) -> Result<Vec<String>> {
    let parse = |s: &str| {
        NaiveDate::parse_from_str(&format!("{s}_01"), "%Y_%m_%d")
            .with_context(|| format!("not a month: {s} (use YYYY_MM)"))
    };
    let (mut d, end) = (parse(from)?, parse(to)?);
    let mut out = Vec::new();
    while d <= end {
        out.push(d.format("%Y_%m").to_string());
        d = d
            .checked_add_months(chrono::Months::new(1))
            .context("month overflow")?;
    }
    Ok(out)
}

/// Archive files for one table and month, sorted.
pub fn table_files(root: &Path, month: &str, table: &str) -> Vec<PathBuf> {
    let Ok(rd) = fs::read_dir(root.join(month).join(table)) else {
        return Vec::new();
    };
    let mut v: Vec<PathBuf> = rd
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            let n = p.to_string_lossy().to_ascii_lowercase();
            n.ends_with(".zip") || n.ends_with(".csv")
        })
        .collect();
    v.sort();
    v
}

/// Stream every CSV row of an archive file (zip or CSV) through `f`.
pub fn visit_file(
    path: &Path,
    f: &mut dyn FnMut(&nem_lens_aemo::Record) -> Result<()>,
) -> Result<()> {
    let bytes = fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    if path
        .to_string_lossy()
        .to_ascii_lowercase()
        .ends_with(".zip")
    {
        visit_csvs(&bytes, ARCHIVE_MEMBER_BYTES, |_, r: &mut dyn Read| {
            visit_report(r, |rec| f(rec))
        })
    } else {
        visit_report(bytes.as_slice(), |rec| f(rec))
    }
}

fn pricing(r: &nem_lens_aemo::Record) -> bool {
    r.text("INTERVENTION").is_none_or(|v| v == "0")
}

/// (interval, region) -> (APC applied, market suspended).
type Flags = HashMap<(NaiveDateTime, String), (bool, bool)>;

/// One month's dispatch snapshots (regions only) and price flags.
fn read_month(
    root: &Path,
    month: &str,
) -> Result<(BTreeMap<NaiveDateTime, DispatchSnapshot>, Flags)> {
    let mut snaps: BTreeMap<NaiveDateTime, DispatchSnapshot> = BTreeMap::new();
    let mut flags: Flags = HashMap::new();
    for path in table_files(root, month, "DISPATCHPRICE") {
        visit_file(&path, &mut |r| {
            if r.key != "DISPATCH_PRICE" || !pricing(r) {
                return Ok(());
            }
            let (Some(t), Some(region), Some(rrp)) = (
                r.text("SETTLEMENTDATE").and_then(parse_market),
                r.text("REGIONID"),
                r.num("RRP"),
            ) else {
                return Ok(());
            };
            let s = snaps.entry(t).or_insert_with(|| DispatchSnapshot {
                settlement: t,
                ..Default::default()
            });
            s.regions.insert(
                region.to_string(),
                RegionInterval {
                    rrp,
                    ..Default::default()
                },
            );
            // APCFLAG bit 1: administered price cap applied.
            let apc = r.num("APCFLAG").is_some_and(|x| (x as i64) & 1 != 0);
            let susp = r.num("MARKETSUSPENDEDFLAG").is_some_and(|x| x != 0.0);
            if apc || susp {
                flags.insert((t, region.to_string()), (apc, susp));
            }
            Ok(())
        })?;
    }
    for path in table_files(root, month, "DISPATCHREGIONSUM") {
        visit_file(&path, &mut |r| {
            if r.key != "DISPATCH_REGIONSUM" || !pricing(r) {
                return Ok(());
            }
            let (Some(t), Some(region)) = (
                r.text("SETTLEMENTDATE").and_then(parse_market),
                r.text("REGIONID"),
            ) else {
                return Ok(());
            };
            if let Some(ri) = snaps.get_mut(&t).and_then(|s| s.regions.get_mut(region)) {
                ri.demand = r.num("TOTALDEMAND");
                ri.available_gen = r.num("AVAILABLEGENERATION");
                ri.net_interchange = r.num("NETINTERCHANGE");
            }
            Ok(())
        })?;
    }
    Ok((snaps, flags))
}

/// Run the detector over the archived months and return every closed
/// high-price event (low-price events are counted but not returned).
pub fn scan(
    root: &Path,
    months: &[String],
    cfg: DetectCfg,
) -> Result<(Vec<HistoricalEvent>, usize)> {
    let mut det = Detector::new(cfg);
    let mut out = Vec::new();
    let mut low = 0;
    let mut all_flags: Flags = HashMap::new();
    for month in months {
        let (snaps, flags) = read_month(root, month)?;
        if snaps.is_empty() {
            eprintln!("{month}: no DISPATCHPRICE rows");
            continue;
        }
        eprintln!("{month}: {} intervals", snaps.len());
        all_flags.extend(flags);
        for snap in snaps.values() {
            det.set_market_price_cap(market_price_cap(snap.settlement));
            for tr in det.ingest(snap) {
                if tr.kind != TransitionKind::Closed {
                    continue;
                }
                let ev = tr.event;
                if ev.kind != EventKind::HighPrice {
                    low += 1;
                    continue;
                }
                let (start, end) = (ev.start_time(), ev.end_time());
                let (mut apc, mut susp) = (0, 0);
                if let (Some(a), Some(b)) = (start, end) {
                    let mut t = a;
                    while t <= b {
                        if let Some(&(x, y)) = all_flags.get(&(t, ev.region.clone())) {
                            apc += x as u32;
                            susp += y as u32;
                        }
                        t += chrono::Duration::minutes(5);
                    }
                }
                out.push(HistoricalEvent {
                    market_price_cap: start.map_or(0.0, market_price_cap),
                    apc_intervals: apc,
                    suspended_intervals: susp,
                    event: Event {
                        data_tier: "archive".into(),
                        ..ev
                    },
                });
            }
        }
        // Flags are only needed while an event can still be open.
        let keep_from = det.last_settlement().map(|t| t - chrono::Duration::days(3));
        if let Some(k) = keep_from {
            all_flags.retain(|(t, _), _| *t >= k);
        }
    }
    Ok((out, low))
}

// ------------------------------------------------------------- bundles

/// MMSDM tables sliced into a historical bundle: (archive folder, table
/// name, bundle file, how rows are matched to an event). A row matches when
/// its table key ends with `_<name>`: the report part varies across
/// archive years (BIDDAYOFFER is `OFFER_BIDDAYOFFER` in 2020).
const BUNDLE_TABLES: [(&str, &str, &str, Match); 10] = [
    (
        "DISPATCHPRICE",
        "PRICE",
        "dispatch_price.parquet",
        Match::Interval,
    ),
    (
        "DISPATCHREGIONSUM",
        "REGIONSUM",
        "dispatch_regionsum.parquet",
        Match::Interval,
    ),
    (
        "DISPATCHCONSTRAINT",
        "CONSTRAINT",
        "dispatch_constraint.parquet",
        Match::Interval,
    ),
    (
        "DISPATCHINTERCONNECTORRES",
        "INTERCONNECTORRES",
        "dispatch_interconnectorres.parquet",
        Match::Interval,
    ),
    (
        "DISPATCHLOAD",
        "UNIT_SOLUTION",
        "unit_dispatch.parquet",
        Match::Interval,
    ),
    (
        "DISPATCHOFFERTRK",
        "OFFERTRK",
        "offer_trk.parquet",
        Match::Interval,
    ),
    // Every bid version (with its rebid explanation) where the archive has
    // it; otherwise the final bid of each day.
    (
        "BIDDAYOFFER",
        "BIDDAYOFFER",
        "bids_dayoffer.parquet",
        Match::TradingDay,
    ),
    (
        "BIDDAYOFFER_D",
        "BIDDAYOFFER_D",
        "bids_dayoffer.parquet",
        Match::TradingDay,
    ),
    (
        "BIDPEROFFER_D",
        "BIDPEROFFER_D",
        "bids_offerperiod.parquet",
        Match::Period,
    ),
    ("MARKETNOTICEDATA", "MARKETNOTICEDATA", "", Match::Notice),
];

#[derive(Clone, Copy, PartialEq)]
enum Match {
    /// SETTLEMENTDATE (interval end) inside the window.
    Interval,
    /// SETTLEMENTDATE (trading day) one of the window's trading days.
    TradingDay,
    /// INTERVAL_DATETIME inside the window.
    Period,
    /// EFFECTIVEDATE from six hours before the window to its end.
    Notice,
}

const MMSDM_URL: &str = "https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM";

/// One event's bundle while its rows are gathered.
struct Job {
    event: Event,
    window: Window,
    days: BTreeSet<NaiveDate>,
    bufs: HashMap<&'static str, TableBuf>,
    notices: Vec<FetchedNotice>,
    used: BTreeSet<PathBuf>,
}

impl Job {
    fn wants(&self, m: Match, r: &nem_lens_aemo::Record) -> bool {
        let at = |c: &str| r.text(c).and_then(parse_market);
        match m {
            Match::Interval => at("SETTLEMENTDATE").is_some_and(|t| self.window.contains(t)),
            Match::TradingDay => {
                at("SETTLEMENTDATE").is_some_and(|t| self.days.contains(&t.date()))
            }
            Match::Period => match at("INTERVAL_DATETIME") {
                Some(t) => self.window.contains(t),
                None => at("SETTLEMENTDATE").is_some_and(|t| self.days.contains(&t.date())),
            },
            Match::Notice => at("EFFECTIVEDATE").is_some_and(|t| {
                self.window.start - chrono::Duration::hours(6) <= t && t <= self.window.end
            }),
        }
    }
}

/// The NEMWeb URL of an archive file, for a month given as YYYY_MM.
pub(crate) fn mmsdm_url(month: &str, file: &str) -> String {
    let y = &month[..4];
    format!(
        "{MMSDM_URL}/{y}/MMSDM_{month}/MMSDM_Historical_Data_SQLLoader/DATA/{}",
        file.replace('#', "%23")
    )
}

/// Months whose archive files an event's window (and its notices) can
/// fall in.
fn months_of(w: &Window) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut t = w.start - chrono::Duration::hours(6);
    while t <= w.end + chrono::Duration::days(1) {
        out.insert(t.format("%Y_%m").to_string());
        t += chrono::Duration::hours(6);
    }
    out
}

/// Build bundles for `events` from the archive at `root` into
/// `bundles/<id>/`: the same Parquet files, with AEMO's column names, as a
/// live bundle, plus market notices from MARKETNOTICEDATA. Every archive
/// file read is listed in the manifest with its SHA-256. Returns the
/// bundle directories.
pub fn build_bundles(root: &Path, events: &[Event], bundles: &Path) -> Result<Vec<PathBuf>> {
    let mut jobs: Vec<Job> = Vec::new();
    for ev in events {
        let w = Window::for_event(ev).with_context(|| format!("{} has no end", ev.id))?;
        let days = w.trading_periods().into_iter().map(|(d, _, _)| d).collect();
        jobs.push(Job {
            event: ev.clone(),
            window: w,
            days,
            bufs: HashMap::new(),
            notices: Vec::new(),
            used: BTreeSet::new(),
        });
    }
    let months: BTreeSet<String> = jobs.iter().flat_map(|j| months_of(&j.window)).collect();
    let mut hashes: HashMap<PathBuf, (String, u64)> = HashMap::new();
    for month in &months {
        let here: Vec<usize> = (0..jobs.len())
            .filter(|&i| months_of(&jobs[i].window).contains(month))
            .collect();
        for (folder, name, file, how) in BUNDLE_TABLES {
            let suffix = format!("_{name}");
            // A full BIDDAYOFFER supersedes BIDDAYOFFER_D for the same month.
            if folder == "BIDDAYOFFER_D" && !table_files(root, month, "BIDDAYOFFER").is_empty() {
                continue;
            }
            for path in table_files(root, month, folder) {
                let mut hit = vec![false; jobs.len()];
                visit_file(&path, &mut |r| {
                    if !r.key.ends_with(&suffix) {
                        return Ok(());
                    }
                    for &i in &here {
                        let j = &mut jobs[i];
                        if !j.wants(how, r) {
                            continue;
                        }
                        hit[i] = true;
                        if how == Match::Notice {
                            let issued = r.text("EFFECTIVEDATE").and_then(parse_market);
                            j.notices.push(FetchedNotice {
                                file: path
                                    .file_name()
                                    .map(|n| n.to_string_lossy().to_string())
                                    .unwrap_or_default(),
                                url: String::new(),
                                sha256: String::new(),
                                notice: Notice {
                                    id: r.text("NOTICEID").unwrap_or_default().to_string(),
                                    kind: r.text("TYPEID").unwrap_or_default().to_string(),
                                    issued: issued.map(iso_market).unwrap_or_default(),
                                    title: r
                                        .text("EXTERNALREFERENCE")
                                        .unwrap_or_default()
                                        .to_string(),
                                    reason: r.get("REASON").unwrap_or_default().trim().to_string(),
                                },
                            });
                        } else {
                            j.bufs.entry(file).or_default().add(&nem_lens_aemo::Table {
                                columns: r.header.columns.clone(),
                                rows: vec![r.values().map(str::to_string).collect()],
                            });
                        }
                    }
                    Ok(())
                })?;
                for (i, h) in hit.iter().enumerate() {
                    if *h {
                        jobs[i].used.insert(path.clone());
                    }
                }
            }
        }
    }

    let mut dirs = Vec::new();
    for j in jobs {
        let dir = bundles.join(&j.event.id);
        if dir.exists() {
            fs::remove_dir_all(&dir)?;
        }
        fs::create_dir_all(&dir)?;
        let mut m = crate::bundle::base_manifest(&j.event, j.window);
        for path in &j.used {
            let (sha, bytes) = match hashes.get(path) {
                Some(x) => x.clone(),
                None => {
                    let b = fs::read(path)?;
                    let x = (crate::bundle::sha256_hex(&b), b.len() as u64);
                    hashes.insert(path.clone(), x.clone());
                    x
                }
            };
            let month = path
                .parent()
                .and_then(|p| p.parent())
                .and_then(|p| p.file_name())
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();
            let name = path
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default();
            m.sources.push(Source {
                url: mmsdm_url(&month, &name),
                file: name,
                sha256: sha,
                bytes,
                role: format!("mmsdm:{month}"),
            });
        }
        let mut intervals = BTreeSet::new();
        for (_, _, file, how) in BUNDLE_TABLES {
            if how == Match::Notice || m.outputs.iter().any(|o| o.file == file) {
                continue;
            }
            let Some(buf) = j.bufs.get(file).filter(|b| b.has_columns()) else {
                continue;
            };
            let sha = buf.write_parquet(&dir.join(file))?;
            m.outputs.push(Output {
                file: file.into(),
                table: file.trim_end_matches(".parquet").to_uppercase(),
                rows: buf.len(),
                sha256: sha,
            });
        }
        // Intervals present, from the price table.
        if let Ok(t) = crate::extract::price_intervals(&dir) {
            intervals.extend(t);
        }
        m.intervals_found = intervals.iter().filter(|t| j.window.contains(**t)).count();
        let has = |f: &str| m.outputs.iter().any(|o| o.file == f);
        let mut notes = vec!["Built from AEMO's MMSDM monthly archive.".to_string()];
        if !has("bids_dayoffer.parquet") {
            notes.push("No BIDDAYOFFER or BIDDAYOFFER_D in the archive for this window: rebid explanations are absent.".into());
        }
        if !has("bids_offerperiod.parquet") {
            notes.push("No BIDPEROFFER_D in the archive for this window (full BIDPEROFFER is not sliced): band availabilities are absent.".into());
        }
        m.next_day = NextDay {
            status: if has("unit_dispatch.parquet") {
                "added"
            } else {
                "unavailable"
            }
            .into(),
            trading_days: j.days.iter().map(|d| d.to_string()).collect(),
            note: "from the MMSDM archive".into(),
        };
        m.notes = notes.join(" ");
        // Notices, with the archive file they came from.
        let mut notices = j.notices;
        notices.sort_by(|a, b| {
            a.notice
                .issued
                .cmp(&b.notice.issued)
                .then(a.notice.id.cmp(&b.notice.id))
        });
        notices.dedup_by(|a, b| a.notice.id == b.notice.id);
        for n in &mut notices {
            if let Some(s) = m.sources.iter().find(|s| s.file == n.file) {
                n.url = s.url.clone();
                n.sha256 = s.sha256.clone();
            }
        }
        let text = serde_json::to_vec_pretty(&notices)?;
        fs::write(dir.join(NOTICES_FILE), &text)?;
        m.outputs.push(Output {
            file: NOTICES_FILE.into(),
            table: "MARKET_NOTICE_MARKETNOTICEDATA".into(),
            rows: notices.len(),
            sha256: crate::bundle::sha256_hex(&text),
        });
        m.save(&dir)?;
        dirs.push(dir);
    }
    Ok(dirs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn archive_urls_name_the_month_folder() {
        assert_eq!(
            mmsdm_url("2025_10", "PUBLIC_ARCHIVE#BIDDAYOFFER#FILE01#202510010000.zip"),
            "https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/2025/MMSDM_2025_10/\
             MMSDM_Historical_Data_SQLLoader/DATA/PUBLIC_ARCHIVE%23BIDDAYOFFER%23FILE01%23202510010000.zip"
        );
    }

    #[test]
    fn months_are_inclusive() {
        assert_eq!(
            months("2020_11", "2021_02").unwrap(),
            ["2020_11", "2020_12", "2021_01", "2021_02"]
        );
        assert!(months("2020-11", "2021_02").is_err());
    }
}

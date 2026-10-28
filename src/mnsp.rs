//! Add MNSP (Basslink) offers to a record, from the MMSDM archive.
//!
//! An MNSP link is dispatched as its offer allows: its MAXAVAIL caps flow
//! below the interconnector's limit, and its price bands price the flow
//! (see `nem_lens_aemo::mnsp`). Records built before 28 Oct 2026 have none
//! of it, so nemcf let Basslink run to its limit: on 26 Jun 2025 it
//! flowed to 594 MW against AEMO's 556 to 497, and priced Tasmania with
//! Victoria.
//!
//! Three files go into `bundle/`:
//!
//! - `static_mnsp_interconnector.parquet`: the links (MNSP_INTERCONNECTOR),
//!   every version effective by the window end.
//! - `mnsp_offerperiod.parquet`: the period rows (MNSP_BIDOFFERPERIOD, or
//!   MNSP_PEROFFER before five-minute settlement) of the versions in force
//!   in the window, for the window's periods. Basslink rebids every five
//!   minutes, so the rest would be most of the table.
//! - `mnsp_dayoffer.parquet`: those versions' prices (MNSP_DAYOFFER), found
//!   in the window's months or, for a standing offer, up to two months
//!   before.

use std::collections::{BTreeSet, HashSet};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{Duration, NaiveDate, NaiveDateTime};
use nem_lens_aemo::bids::period_of;
use nem_lens_aemo::mnsp::{table_of, MnspOffers, MnspTable};
use nem_lens_aemo::time::parse_market;

use crate::bundle::{Manifest, Output, Source, TableBuf, Window};
use crate::extract::tables_dir;
use crate::history::{mmsdm_url, table_files, visit_file};

/// The files this writes into `bundle/`.
pub const FILES: [&str; 3] = [
    "static_mnsp_interconnector.parquet",
    "mnsp_offerperiod.parquet",
    "mnsp_dayoffer.parquet",
];

/// What happened to one record.
pub enum Added {
    /// Rows written: links, period offers, day offers.
    Done {
        links: usize,
        periods: usize,
        days: usize,
    },
    /// Left as it was, and why.
    Skipped(String),
}

/// (link, trading day, offer time, version) of an offer version.
type Version = (String, NaiveDate, NaiveDateTime, Option<u32>);

/// Add the MNSP tables to the record (or bundle) at `dir` from the MMSDM
/// archive at `root`, replacing any already there.
pub fn add(dir: &Path, root: &Path) -> Result<Added> {
    let mut m = Manifest::load(dir)?;
    let start = parse_market(&m.window_start).context("manifest window_start")?;
    let end = parse_market(&m.window_end).context("manifest window_end")?;
    let w = Window { start, end };
    // A month's period file holds trading days from the 2nd of the month to
    // the 1st of the next (2025_01: 2 Jan to 1 Feb), so a window on the 1st
    // is in the previous month's file: read one month back.
    let window_months = months(&w, 1);
    let earlier = months(&w, 2);

    // Interval ends in the window, and each one's period.
    let mut ends = Vec::new();
    let mut t = w.start;
    while t <= w.end {
        ends.push(t);
        t += Duration::minutes(5);
    }

    // Pass 1: every period row of the window's trading days, parsed, to
    // find the versions in force.
    let days: BTreeSet<NaiveDate> = ends.iter().map(|&t| period_of(t).0).collect();
    let mut offers = MnspOffers::default();
    let mut used: BTreeSet<PathBuf> = BTreeSet::new();
    // Before five-minute settlement (1 Oct 2021) dispatch used 30-minute
    // offers, MNSP_PEROFFER; some archive months from March 2021 also carry
    // MNSP_BIDOFFERPERIOD (five-minute, the new format). Take MNSP_PEROFFER
    // for such windows where the archive has it.
    let five_minute = w.start >= parse_market("2021/10/01 04:05:00").expect("valid time");
    let period_files = |month: &str| {
        let per = table_files(root, month, "MNSP_PEROFFER");
        let bid = table_files(root, month, "MNSP_BIDOFFERPERIOD");
        if (!five_minute && !per.is_empty()) || bid.is_empty() {
            per
        } else {
            bid
        }
    };
    for month in &window_months {
        for path in period_files(month) {
            visit_file(&path, &mut |r| {
                if table_of(r.key) == Some(MnspTable::PeriodOffer) {
                    let day = r
                        .text("TRADINGDATE")
                        .or_else(|| r.text("SETTLEMENTDATE"))
                        .and_then(parse_market)
                        .map(|t| t.date());
                    if day.is_some_and(|d| days.contains(&d)) {
                        offers.push(r)?;
                    }
                }
                Ok(())
            })?;
        }
    }
    if offers.periods.is_empty() {
        return Ok(Added::Skipped(
            "no MNSP period offers in the archive for the window".into(),
        ));
    }
    let links: BTreeSet<String> = offers.periods.iter().map(|p| p.link_id.clone()).collect();
    let mut wanted: HashSet<Version> = HashSet::new();
    let mut periods_of: HashSet<(Version, u16)> = HashSet::new();
    for &t in &ends {
        let cutoff = t - Duration::minutes(5);
        let (_, period) = period_of(t);
        for link in &links {
            if let Some((_, p)) = offers.effective(link, t, cutoff) {
                let v = (
                    p.link_id.clone(),
                    p.trading_date,
                    p.offer_time,
                    p.version_no,
                );
                periods_of.insert((v.clone(), period));
                wanted.insert(v);
            }
        }
    }

    // Pass 2: the raw rows of those versions, for the periods they serve.
    let mut period_buf = TableBuf::default();
    for month in &window_months {
        for path in period_files(month) {
            let mut hit = false;
            visit_file(&path, &mut |r| {
                if table_of(r.key) != Some(MnspTable::PeriodOffer) {
                    return Ok(());
                }
                let mut one = MnspOffers::default();
                if one.push(r).is_err() {
                    return Ok(());
                }
                let p = &one.periods[0];
                let v = (
                    p.link_id.clone(),
                    p.trading_date,
                    p.offer_time,
                    p.version_no,
                );
                if wanted.contains(&v)
                    && (p.period_from..=p.period_to).any(|q| periods_of.contains(&(v.clone(), q)))
                {
                    hit = true;
                    period_buf.add(&nem_lens_aemo::Table {
                        columns: r.header.columns.clone(),
                        rows: vec![r.values().map(str::to_string).collect()],
                    });
                }
                Ok(())
            })?;
            if hit {
                used.insert(path);
            }
        }
    }

    // Day offers of those versions: same day where there is one, else the
    // same offer time on an earlier day (a standing offer).
    let mut day_buf = TableBuf::default();
    let mut found: HashSet<(String, NaiveDateTime, Option<u32>)> = HashSet::new();
    for month in earlier.iter().rev() {
        for path in table_files(root, month, "MNSP_DAYOFFER") {
            let mut hit = false;
            visit_file(&path, &mut |r| {
                if table_of(r.key) != Some(MnspTable::DayOffer) {
                    return Ok(());
                }
                let mut one = MnspOffers::default();
                if one.push(r).is_err() {
                    return Ok(());
                }
                let d = &one.day[0];
                let matches = wanted.iter().any(|(link, _, at, ver)| {
                    *link == d.link_id
                        && *at == d.offer_time
                        && (ver.is_none() || d.version_no.is_none() || *ver == d.version_no)
                });
                if matches && found.insert((d.link_id.clone(), d.offer_time, d.version_no)) {
                    hit = true;
                    day_buf.add(&nem_lens_aemo::Table {
                        columns: r.header.columns.clone(),
                        rows: vec![r.values().map(str::to_string).collect()],
                    });
                }
                Ok(())
            })?;
            if hit {
                used.insert(path);
            }
        }
    }

    // Links: every version effective by the window end, from the newest
    // month that has the table.
    let mut link_buf = TableBuf::default();
    let mut seen = HashSet::new();
    for month in earlier.iter().rev() {
        for path in table_files(root, month, "MNSP_INTERCONNECTOR") {
            let mut hit = false;
            visit_file(&path, &mut |r| {
                if table_of(r.key) != Some(MnspTable::Link) {
                    return Ok(());
                }
                let effective = r.text("EFFECTIVEDATE").and_then(parse_market);
                let key = (
                    r.text("LINKID").unwrap_or_default().to_string(),
                    r.text("EFFECTIVEDATE").unwrap_or_default().to_string(),
                    r.text("VERSIONNO").unwrap_or_default().to_string(),
                );
                if effective.is_some_and(|e| e <= w.end) && seen.insert(key) {
                    hit = true;
                    link_buf.add(&nem_lens_aemo::Table {
                        columns: r.header.columns.clone(),
                        rows: vec![r.values().map(str::to_string).collect()],
                    });
                }
                Ok(())
            })?;
            if hit {
                used.insert(path);
            }
        }
        if link_buf.len() > 0 {
            break;
        }
    }

    let tdir = tables_dir(dir);
    let mut written = [0usize; 3];
    for (k, (file, buf)) in FILES
        .iter()
        .zip([&link_buf, &period_buf, &day_buf])
        .enumerate()
    {
        m.outputs.retain(|o| o.file != *file);
        let path = tdir.join(file);
        if !buf.has_columns() {
            if path.exists() {
                std::fs::remove_file(&path)?;
            }
            continue;
        }
        let sha = buf.write_parquet(&path)?;
        written[k] = buf.len();
        m.outputs.push(Output {
            file: file.to_string(),
            table: file.trim_end_matches(".parquet").to_uppercase(),
            rows: buf.len(),
            sha256: sha,
        });
    }
    for path in used {
        let file = path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();
        if m.sources.iter().any(|s| s.file == file) {
            continue;
        }
        let month = path
            .parent()
            .and_then(|p| p.parent())
            .and_then(|p| p.file_name())
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();
        let bytes = std::fs::read(&path)?;
        m.sources.push(Source {
            url: mmsdm_url(&month, &file),
            sha256: crate::bundle::sha256_hex(&bytes),
            bytes: bytes.len() as u64,
            role: format!("mmsdm:{month}"),
            file,
        });
    }
    m.save(dir)?;
    Ok(Added::Done {
        links: written[0],
        periods: written[1],
        days: written[2],
    })
}

/// The months (YYYY_MM) the window's trading days fall in, and `back`
/// months before the first.
fn months(w: &Window, back: u32) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    let mut t = w.start - Duration::hours(6);
    while t <= w.end + Duration::days(1) {
        out.insert(t.format("%Y_%m").to_string());
        t += Duration::hours(6);
    }
    if let Some(first) = out.iter().next().cloned() {
        let (mut y, mut mo): (i32, u32) = (
            first[..4].parse().unwrap_or(0),
            first[5..].parse().unwrap_or(1),
        );
        for _ in 0..back {
            if mo == 1 {
                y -= 1;
                mo = 12;
            } else {
                mo -= 1;
            }
            out.insert(format!("{y}_{mo:02}"));
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn months_cover_the_window_and_earlier() {
        let t = |s| parse_market(s).unwrap();
        let w = Window {
            start: t("2020/01/01 02:00:00"),
            end: t("2020/01/01 06:00:00"),
        };
        // The window starts in the trading day of 31 Dec 2019.
        let m = months(&w, 2);
        assert_eq!(
            m.into_iter().collect::<Vec<_>>(),
            ["2019_10", "2019_11", "2019_12", "2020_01"]
        );
    }
}

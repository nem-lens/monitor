//! Restore the FCAS day offers to records slimmed before slimming kept them.
//!
//! Until 24 Oct 2026, `store slim` kept ENERGY day offers only, so a
//! record's FCAS period offers (quantities) had no price bands. This
//! rebuilds `bids_dayoffer` from the same archive files the record was
//! built from, exactly as `history build` slices it, and slims it again:
//! the ENERGY rows come back as they were, and the FCAS versions the
//! period offers name are kept as well.

use std::collections::BTreeSet;
use std::path::Path;

use anyhow::{bail, Context, Result};
use chrono::NaiveDate;
use nem_lens_aemo::time::parse_market;

use crate::bundle::{Manifest, TableBuf, Window};
use crate::extract::tables_dir;
use crate::history::{table_files, visit_file};

const DAY_OFFER: &str = "bids_dayoffer.parquet";

/// What happened to one record.
pub enum Restored {
    /// Rows in `bids_dayoffer` before and after.
    Done { before: usize, after: usize },
    /// Left as it was, and why.
    Skipped(String),
}

/// Rebuild and re-slim `bids_dayoffer` for the record at `dir` from the
/// MMSDM archive at `root`.
pub fn restore(dir: &Path, root: &Path) -> Result<Restored> {
    let m = Manifest::load(dir)?;
    let tdir = tables_dir(dir);
    if !tdir.join(DAY_OFFER).exists() {
        return Ok(Restored::Skipped("no bids_dayoffer".into()));
    }
    let start = parse_market(&m.window_start).context("manifest window_start")?;
    let end = parse_market(&m.window_end).context("manifest window_end")?;
    let w = Window { start, end };
    let days: BTreeSet<NaiveDate> = w.trading_periods().into_iter().map(|(d, _, _)| d).collect();

    // The archive files the record's day offers came from.
    let sources: BTreeSet<String> = m
        .sources
        .iter()
        // PUBLIC_ARCHIVE#BIDDAYOFFER#… from Aug 2024, PUBLIC_DVD_BIDDAYOFFER_…
        // before; the files `history build` read for the day offers.
        .filter(|s| s.role.starts_with("mmsdm:") && s.file.contains("BIDDAYOFFER"))
        .map(|s| s.file.clone())
        .collect();
    if sources.is_empty() {
        return Ok(Restored::Skipped(
            "day offers not from the MMSDM archive".into(),
        ));
    }

    // As `history build`: months the window can fall in; a full BIDDAYOFFER
    // supersedes BIDDAYOFFER_D for the same month; rows whose SETTLEMENTDATE
    // is one of the window's trading days.
    let mut months = BTreeSet::new();
    let mut t = w.start - chrono::Duration::hours(6);
    while t <= w.end + chrono::Duration::days(1) {
        months.insert(t.format("%Y_%m").to_string());
        t += chrono::Duration::hours(6);
    }
    let mut buf = TableBuf::default();
    let mut used = BTreeSet::new();
    for month in &months {
        for (folder, name) in [
            ("BIDDAYOFFER", "_BIDDAYOFFER"),
            ("BIDDAYOFFER_D", "_BIDDAYOFFER_D"),
        ] {
            if folder == "BIDDAYOFFER_D" && !table_files(root, month, "BIDDAYOFFER").is_empty() {
                continue;
            }
            for path in table_files(root, month, folder) {
                let file = path
                    .file_name()
                    .map(|n| n.to_string_lossy().to_string())
                    .unwrap_or_default();
                let mut hit = false;
                visit_file(&path, &mut |r| {
                    if !r.key.ends_with(name) {
                        return Ok(());
                    }
                    let on_day = r
                        .text("SETTLEMENTDATE")
                        .and_then(parse_market)
                        .is_some_and(|t| days.contains(&t.date()));
                    if on_day {
                        hit = true;
                        buf.add(&nem_lens_aemo::Table {
                            columns: r.header.columns.clone(),
                            rows: vec![r.values().map(str::to_string).collect()],
                        });
                    }
                    Ok(())
                })?;
                if hit {
                    used.insert(file);
                }
            }
        }
    }
    // Every file read must be one the record was built from (a restored
    // record also lists the BIDDAYOFFER_D files added below).
    if used.is_empty() || !used.is_subset(&sources) {
        bail!(
            "the archive's day-offer files ({}) differ from the record's ({})",
            used.into_iter().collect::<Vec<_>>().join(", "),
            sources.into_iter().collect::<Vec<_>>().join(", ")
        );
    }

    // Standing offers, submitted before the trading day, are in force in
    // BIDPEROFFER_D but have no BIDDAYOFFER row for the day; BIDDAYOFFER_D
    // has them. Add the FCAS versions the period offers name that are still
    // missing (ENERGY is left as the record had it).
    let wanted = crate::slim::period_versions(&tdir)?;
    let key = |r: &nem_lens_aemo::Record| -> Option<crate::slim::OfferVersion> {
        let ms = |c: &str| Some(parse_market(r.text(c)?)?.and_utc().timestamp_millis());
        Some((
            r.text("DUID")?.to_string(),
            r.text("BIDTYPE")?.to_string(),
            ms("SETTLEMENTDATE")?,
            r.text("DIRECTION").map(|d| d.to_string()),
            ms("OFFERDATE")?,
        ))
    };
    let mut have = std::collections::HashSet::new();
    for month in &months {
        for path in table_files(root, month, "BIDDAYOFFER") {
            visit_file(&path, &mut |r| {
                if r.key.ends_with("_BIDDAYOFFER") {
                    if let Some(k) = key(r).filter(|k| wanted.contains(k)) {
                        have.insert(k);
                    }
                }
                Ok(())
            })?;
        }
    }
    let mut added = Vec::new();
    for month in &months {
        if table_files(root, month, "BIDDAYOFFER").is_empty() {
            continue; // BIDDAYOFFER_D was read in full above
        }
        for path in table_files(root, month, "BIDDAYOFFER_D") {
            let mut hit = false;
            visit_file(&path, &mut |r| {
                if !r.key.ends_with("_BIDDAYOFFER_D") {
                    return Ok(());
                }
                if let Some(k) = key(r) {
                    if k.1 != "ENERGY" && wanted.contains(&k) && have.insert(k) {
                        hit = true;
                        buf.add(&nem_lens_aemo::Table {
                            columns: r.header.columns.clone(),
                            rows: vec![r.values().map(str::to_string).collect()],
                        });
                    }
                }
                Ok(())
            })?;
            if hit {
                added.push((month.clone(), path));
            }
        }
    }
    let before = m
        .outputs
        .iter()
        .find(|o| o.file == DAY_OFFER)
        .map_or(0, |o| o.rows);
    // Column types as the record has them: they were inferred when it was
    // built, and parsing has changed since (offer times with milliseconds
    // were text then). The slim keeps what the slimmed record kept, plus
    // the FCAS versions.
    let schema = {
        let f = std::fs::File::open(tdir.join(DAY_OFFER))?;
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(f)?
            .schema()
            .as_ref()
            .clone()
    };
    buf.write_parquet_as(&tdir.join(DAY_OFFER), &schema)?;
    crate::slim::slim(dir)?;
    let mut m = Manifest::load(dir)?;
    for (month, path) in added {
        let file = path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_default();
        if m.sources.iter().any(|s| s.file == file) {
            continue;
        }
        let bytes = std::fs::read(&path)?;
        m.sources.push(crate::bundle::Source {
            url: crate::history::mmsdm_url(&month, &file),
            sha256: crate::bundle::sha256_hex(&bytes),
            bytes: bytes.len() as u64,
            role: format!("mmsdm:{month}"),
            file,
        });
    }
    m.save(dir)?;
    let after = m
        .outputs
        .iter()
        .find(|o| o.file == DAY_OFFER)
        .map_or(0, |o| o.rows);
    Ok(Restored::Done { before, after })
}

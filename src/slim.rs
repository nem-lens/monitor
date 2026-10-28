//! Slim an event record down to the rows its readers can use.
//!
//! A record holds AEMO tables sliced for the event, but two tables carry far
//! more than any reader looks at (19 Oct 2026, about 60% of the store):
//!
//! - `bids_dayoffer`: every rebid of the trading day, for every unit and
//!   service. nemcf reads, for each interval, the latest ENERGY version
//!   submitted at or before its cutoff (the OFFERTRK offer time, else the
//!   interval end), and for FCAS the versions the period offers name
//!   (BIDPEROFFER_D holds the one in force each period). The public files
//!   read the last five distinct ENERGY rebid reasons per unit up to the
//!   window end.
//! - the constraint equation tables: every version of each invoked
//!   constraint back to 2016. nemcf resolves each invoked version once, to
//!   that version or, when it is missing or has no usable header, the
//!   nearest usable one (earlier first). The public files read the latest
//!   version effective by the window end, for descriptions.
//!
//! Slimming keeps exactly those rows, so nemcf builds the same snapshots
//! and `library add` writes the same public files from a slimmed record.
//! It is idempotent. Other tables are left as they are.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use arrow_array::{
    Array, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampMillisecondArray,
};
use chrono::NaiveDateTime;
use nem_lens_aemo::time::{parse_market, trading_day};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};

use crate::bundle::{sha256_hex, Manifest};
use crate::extract::tables_dir;

const DAY_OFFER: &str = "bids_dayoffer.parquet";
const GENCONDATA: &str = "static_gencondata.parquet";
/// Tables keyed by constraint version (GENCONID, EFFECTIVEDATE, VERSIONNO).
const EQUATION_TABLES: [&str; 4] = [
    GENCONDATA,
    "static_spdconnectionpointconstraint.parquet",
    "static_spdinterconnectorconstraint.parquet",
    "static_spdregionconstraint.parquet",
];
/// Rebid reasons the public files show per unit (`extract::unavailable_units`).
const REBID_REASONS: usize = 5;

/// What slimming removed, kept in the manifest.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct Slimmed {
    pub generator: String,
    pub tables: Vec<SlimTable>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SlimTable {
    pub file: String,
    /// Rows in the record as built, before any slimming.
    pub rows_before: usize,
    pub rows_after: usize,
}

/// (GENCONID, effective date in ms, version)
type Version = (String, i64, i64);

/// Slim the record (or bundle) at `dir` in place.
pub fn slim(dir: &Path) -> Result<Slimmed> {
    let mut m = Manifest::load(dir)?;
    let tdir = tables_dir(dir);
    let window_start = parse_market(&m.window_start).context("manifest window_start")?;
    let window_end = parse_market(&m.window_end).context("manifest window_end")?;
    let mut done = m.slimmed.take().unwrap_or_default();
    done.generator = concat!("nem-monitor ", env!("CARGO_PKG_VERSION")).into();

    let mut results: Vec<(String, usize, usize, String)> = Vec::new();
    if tdir.join(GENCONDATA).exists() && tdir.join("dispatch_constraint.parquet").exists() {
        let keep = equation_versions(&tdir, window_end)?;
        for file in EQUATION_TABLES {
            let path = tdir.join(file);
            if !path.exists() {
                continue;
            }
            let r = rewrite(&path, |b| {
                let id = strings(b, "GENCONID");
                let eff = millis(b, "EFFECTIVEDATE");
                let ver = ints(b, "VERSIONNO");
                (0..b.num_rows())
                    .map(|i| match (&id[i], eff[i], ver[i]) {
                        (Some(id), Some(e), Some(v)) => keep.contains(&(id.clone(), e, v)),
                        _ => false,
                    })
                    .collect()
            })?;
            results.push((file.to_string(), r.0, r.1, r.2));
        }
    }
    if tdir.join(DAY_OFFER).exists() {
        let keep = offer_rows(&tdir, window_start, window_end)?;
        let mut row = 0;
        let r = rewrite(&tdir.join(DAY_OFFER), |b| {
            let mask = (row..row + b.num_rows())
                .map(|i| keep.contains(&i))
                .collect();
            row += b.num_rows();
            mask
        })?;
        results.push((DAY_OFFER.to_string(), r.0, r.1, r.2));
    }

    for (file, before, after, sha) in results {
        if let Some(o) = m.outputs.iter_mut().find(|o| o.file == file) {
            o.rows = after;
            o.sha256 = sha;
        }
        match done.tables.iter_mut().find(|t| t.file == file) {
            Some(t) => t.rows_after = after,
            None => done.tables.push(SlimTable {
                file,
                rows_before: before,
                rows_after: after,
            }),
        }
    }
    m.slimmed = Some(done.clone());
    m.save(dir)?;
    Ok(done)
}

/// The equation versions nemcf resolves the invoked ones to, plus each
/// constraint's latest version effective by `window_end`.
fn equation_versions(tdir: &Path, window_end: NaiveDateTime) -> Result<HashSet<Version>> {
    let mut invoked: BTreeSet<Version> = BTreeSet::new();
    for b in read_batches(&tdir.join("dispatch_constraint.parquet"))? {
        let id = strings(&b, "CONSTRAINTID");
        let eff = millis(&b, "GENCONID_EFFECTIVEDATE");
        let ver = ints(&b, "GENCONID_VERSIONNO");
        for i in 0..b.num_rows() {
            if let (Some(id), Some(e), Some(v)) = (&id[i], eff[i], ver[i]) {
                invoked.insert((id.clone(), e, v));
            }
        }
    }
    // Usable versions, as nemcf's `Equations::resolve` sees them: a header
    // row with a constraint type. Sorted per constraint, oldest first.
    let mut usable: HashMap<String, BTreeSet<(i64, i64)>> = HashMap::new();
    let mut latest: HashMap<String, (i64, i64)> = HashMap::new();
    let end_ms = window_end.and_utc().timestamp_millis();
    for b in read_batches(&tdir.join(GENCONDATA))? {
        let id = strings(&b, "GENCONID");
        let eff = millis(&b, "EFFECTIVEDATE");
        let ver = ints(&b, "VERSIONNO");
        let kind = strings(&b, "CONSTRAINTTYPE");
        for i in 0..b.num_rows() {
            let (Some(id), Some(e), Some(v)) = (&id[i], eff[i], ver[i]) else {
                continue;
            };
            if matches!(kind[i].as_deref(), Some("<=" | ">=" | "=")) {
                usable.entry(id.clone()).or_default().insert((e, v));
            }
            if e <= end_ms && latest.get(id).is_none_or(|k| (e, v) > *k) {
                latest.insert(id.clone(), (e, v));
            }
        }
    }
    let mut keep: HashSet<Version> = HashSet::new();
    for (id, e, v) in &invoked {
        keep.insert((id.clone(), *e, *v));
        let Some(vs) = usable.get(id) else { continue };
        if vs.contains(&(*e, *v)) {
            continue;
        }
        let pick = vs
            .range(..(*e, *v))
            .next_back()
            .or_else(|| vs.range((*e, *v)..).next());
        if let Some((pe, pv)) = pick {
            keep.insert((id.clone(), *pe, *pv));
        }
    }
    for (id, (e, v)) in latest {
        keep.insert((id, e, v));
    }
    Ok(keep)
}

/// Indexes of the day-offer rows to keep: ENERGY offers from the version in
/// force at each unit's earliest cutoff up to the window end, and the rows
/// behind the public files' rebid reasons.
fn offer_rows(
    tdir: &Path,
    window_start: NaiveDateTime,
    window_end: NaiveDateTime,
) -> Result<HashSet<usize>> {
    let ms = |t: NaiveDateTime| t.and_utc().timestamp_millis();
    let (start_ms, end_ms) = (ms(window_start), ms(window_end));

    // Each unit's earliest cutoff: its first OFFERTRK energy offer time, or
    // the first interval of the window.
    let mut earliest: HashMap<String, i64> = HashMap::new();
    let trk = tdir.join("offer_trk.parquet");
    if trk.exists() {
        for b in read_batches(&trk)? {
            let duid = strings(&b, "DUID");
            let kind = strings(&b, "BIDTYPE");
            let at = millis(&b, "BIDOFFERDATE");
            for i in 0..b.num_rows() {
                if let (Some(d), Some("ENERGY"), Some(t)) = (&duid[i], kind[i].as_deref(), at[i]) {
                    let e = earliest.entry(d.clone()).or_insert(t);
                    *e = (*e).min(t);
                }
            }
        }
    }

    // Trading days of the window's priced intervals, as the public files
    // take them.
    let mut days = BTreeSet::new();
    let price = tdir.join("dispatch_price.parquet");
    if price.exists() {
        for b in read_batches(&price)? {
            for t in millis(&b, "SETTLEMENTDATE").into_iter().flatten() {
                if (start_ms..=end_ms).contains(&t) {
                    if let Some(t) = chrono::DateTime::from_timestamp_millis(t) {
                        days.insert(trading_day(t.naive_utc()));
                    }
                }
            }
        }
    }

    struct Row {
        duid: String,
        day: i64,
        direction: Option<String>,
        at: i64,
        reason: String,
    }
    let mut rows: Vec<Option<Row>> = Vec::new();
    for b in read_batches(&tdir.join(DAY_OFFER))? {
        let duid = strings(&b, "DUID");
        let kind = strings(&b, "BIDTYPE");
        let day = millis(&b, "SETTLEMENTDATE");
        let at = millis(&b, "OFFERDATE");
        let dir = strings(&b, "DIRECTION");
        let why = strings(&b, "REBIDEXPLANATION");
        for i in 0..b.num_rows() {
            rows.push(match (&duid[i], kind[i].as_deref(), day[i], at[i]) {
                (Some(d), Some("ENERGY"), Some(day), Some(at)) => Some(Row {
                    duid: d.clone(),
                    day,
                    direction: dir[i].clone(),
                    at,
                    reason: why[i].as_deref().unwrap_or("").trim().to_string(),
                }),
                _ => None,
            });
        }
    }

    let mut keep = HashSet::new();
    // nemcf's FCAS: the day offers whose versions the period offers name.
    // BIDPEROFFER_D holds the version in force in each period, so these are
    // exactly the price bands that go with the record's FCAS quantities.
    let fcas_versions = period_versions(tdir)?;
    if !fcas_versions.is_empty() {
        let mut i = 0;
        for b in read_batches(&tdir.join(DAY_OFFER))? {
            let duid = strings(&b, "DUID");
            let kind = strings(&b, "BIDTYPE");
            let day = millis(&b, "SETTLEMENTDATE");
            let at = millis(&b, "OFFERDATE");
            let dir = strings(&b, "DIRECTION");
            for k in 0..b.num_rows() {
                if let (Some(d), Some(s), Some(day), Some(at)) = (&duid[k], &kind[k], day[k], at[k])
                {
                    if s != "ENERGY"
                        && fcas_versions.contains(&(d.clone(), s.clone(), day, dir[k].clone(), at))
                    {
                        keep.insert(i + k);
                    }
                }
            }
            i += b.num_rows();
        }
    }
    // nemcf: per (unit, trading day, direction), versions from the latest
    // at or before the unit's earliest cutoff to the window end.
    let mut floor: HashMap<(&str, i64, Option<&str>), i64> = HashMap::new();
    for r in rows.iter().flatten() {
        let cutoff = earliest
            .get(&r.duid)
            .copied()
            .unwrap_or(start_ms)
            .min(start_ms);
        if r.at <= cutoff {
            let f = floor
                .entry((&r.duid, r.day, r.direction.as_deref()))
                .or_insert(r.at);
            *f = (*f).max(r.at);
        }
    }
    for (i, r) in rows.iter().enumerate() {
        let Some(r) = r else { continue };
        let lo = floor
            .get(&(r.duid.as_str(), r.day, r.direction.as_deref()))
            .copied()
            .unwrap_or(i64::MIN);
        if lo <= r.at && r.at <= end_ms {
            keep.insert(i);
        }
    }

    // Public files: per unit, (offer time, reason) sorted and deduplicated
    // on runs of the same reason, last five. Keep the rows from the start
    // of the fifth-last run on, so the same five come out.
    let mut reasons: BTreeMap<&str, Vec<(i64, &str, usize)>> = BTreeMap::new();
    for (i, r) in rows.iter().enumerate() {
        let Some(r) = r else { continue };
        let on_day = chrono::DateTime::from_timestamp_millis(r.day)
            .is_some_and(|d| days.contains(&d.naive_utc().date()));
        if r.at <= end_ms && on_day && !r.reason.is_empty() {
            reasons
                .entry(&r.duid)
                .or_default()
                .push((r.at, &r.reason, i));
        }
    }
    for list in reasons.values_mut() {
        list.sort();
        let mut starts = Vec::new();
        for (k, item) in list.iter().enumerate() {
            if k == 0 || list[k - 1].1 != item.1 {
                starts.push(k);
            }
        }
        let from = starts
            .len()
            .checked_sub(REBID_REASONS)
            .map_or(0, |n| starts[n]);
        keep.extend(list[from..].iter().map(|x| x.2));
    }
    Ok(keep)
}

/// (DUID, BIDTYPE, trading day ms, DIRECTION, OFFERDATE ms) of a version
/// the period offers name.
pub(crate) type OfferVersion = (String, String, i64, Option<String>, i64);

/// Every FCAS version the period offers name.
pub(crate) fn period_versions(tdir: &Path) -> Result<HashSet<OfferVersion>> {
    Ok(all_period_versions(tdir)?
        .into_iter()
        .filter(|v| v.1 != "ENERGY")
        .collect())
}

/// Every version, ENERGY and FCAS, the period offers name.
pub(crate) fn all_period_versions(tdir: &Path) -> Result<HashSet<OfferVersion>> {
    let mut out = HashSet::new();
    let path = tdir.join("bids_offerperiod.parquet");
    if !path.exists() {
        return Ok(out);
    }
    // Before 5-minute settlement (Oct 2021) BIDPEROFFER_D named these
    // TRADINGDATE and OFFERDATETIME, as aemo's parser reads them.
    let either = |b: &RecordBatch, a: &str, c: &str| {
        if b.column_by_name(a).is_some() {
            millis(b, a)
        } else {
            millis(b, c)
        }
    };
    for b in read_batches(&path)? {
        let duid = strings(&b, "DUID");
        let kind = strings(&b, "BIDTYPE");
        let day = either(&b, "TRADINGDATE", "SETTLEMENTDATE");
        let at = either(&b, "OFFERDATETIME", "OFFERDATE");
        let dir = strings(&b, "DIRECTION");
        for i in 0..b.num_rows() {
            if let (Some(d), Some(s), Some(day), Some(at)) = (&duid[i], &kind[i], day[i], at[i]) {
                out.insert((d.clone(), s.clone(), day, dir[i].clone(), at));
            }
        }
    }
    Ok(out)
}

/// Rewrite a Parquet file keeping the rows `mask` selects, batch by batch.
/// Returns rows before, rows after and the new file's SHA-256.
fn rewrite(
    path: &Path,
    mut mask: impl FnMut(&RecordBatch) -> Vec<bool>,
) -> Result<(usize, usize, String)> {
    let batches = read_batches(path)?;
    let Some(schema) = batches.first().map(|b| b.schema()) else {
        let bytes = fs::read(path)?;
        return Ok((0, 0, sha256_hex(&bytes)));
    };
    let props = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    let mut buf = Vec::new();
    let mut w = ArrowWriter::try_new(&mut buf, schema, Some(props))?;
    let (mut before, mut after) = (0, 0);
    for b in &batches {
        let keep = BooleanArray::from(mask(b));
        let kept = arrow_select::filter::filter_record_batch(b, &keep)?;
        before += b.num_rows();
        after += kept.num_rows();
        w.write(&kept)?;
    }
    w.close()?;
    fs::write(path, &buf).with_context(|| format!("writing {}", path.display()))?;
    Ok((before, after, sha256_hex(&buf)))
}

fn read_batches(path: &Path) -> Result<Vec<RecordBatch>> {
    let f = fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(f)?.build()?;
    Ok(reader.collect::<std::result::Result<_, _>>()?)
}

/// A text column's values (None where null or absent).
fn strings(b: &RecordBatch, name: &str) -> Vec<Option<String>> {
    let col = b.column_by_name(name);
    let a = col.and_then(|c| c.as_any().downcast_ref::<StringArray>());
    (0..b.num_rows())
        .map(|i| a.filter(|a| !a.is_null(i)).map(|a| a.value(i).to_string()))
        .collect()
}

/// A timestamp column as milliseconds. A column whose values did not all
/// parse when the record was built (such as offer times with milliseconds)
/// was stored as text, so text is parsed too.
fn millis(b: &RecordBatch, name: &str) -> Vec<Option<i64>> {
    let col = b.column_by_name(name);
    if let Some(a) = col.and_then(|c| c.as_any().downcast_ref::<TimestampMillisecondArray>()) {
        return (0..b.num_rows())
            .map(|i| (!a.is_null(i)).then(|| a.value(i)))
            .collect();
    }
    strings(b, name)
        .into_iter()
        .map(|s| Some(parse_market(&s?)?.and_utc().timestamp_millis()))
        .collect()
}

/// An integer column, also read from Float64 (a column is typed by the
/// values the archive held).
fn ints(b: &RecordBatch, name: &str) -> Vec<Option<i64>> {
    let col = b.column_by_name(name);
    let i64s = col.and_then(|c| c.as_any().downcast_ref::<Int64Array>());
    let f64s = col.and_then(|c| c.as_any().downcast_ref::<Float64Array>());
    (0..b.num_rows())
        .map(|i| match (i64s, f64s) {
            (Some(a), _) if !a.is_null(i) => Some(a.value(i)),
            (_, Some(a)) if !a.is_null(i) => Some(a.value(i) as i64),
            _ => None,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bundle::TableBuf;
    use nem_lens_aemo::report::Table;

    fn write(dir: &Path, file: &str, columns: &[&str], rows: &[&[&str]]) {
        let mut buf = TableBuf::default();
        buf.add(&Table {
            columns: columns.iter().map(|c| c.to_string()).collect(),
            rows: rows
                .iter()
                .map(|r| r.iter().map(|v| v.to_string()).collect())
                .collect(),
        });
        buf.write_parquet(&dir.join(file)).unwrap();
    }

    fn column(dir: &Path, file: &str, name: &str) -> Vec<String> {
        read_batches(&dir.join(file))
            .unwrap()
            .iter()
            .flat_map(|b| {
                let m = millis(b, name);
                let s = strings(b, name);
                let i = ints(b, name);
                (0..b.num_rows())
                    .map(|k| match (&s[k], m[k], i[k]) {
                        (Some(s), _, _) => s.clone(),
                        (_, Some(t), _) => chrono::DateTime::from_timestamp_millis(t)
                            .unwrap()
                            .format("%H:%M")
                            .to_string(),
                        (_, _, Some(v)) => v.to_string(),
                        _ => String::new(),
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    /// A record with a 10:00-12:00 window on 12 Oct 2026.
    fn record() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "nem-slim-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("bundle")).unwrap();
        let m = serde_json::json!({
            "format": 1, "generator": "t", "event_id": "e1", "region": "NSW1",
            "window_start": "2026-10-12T10:00:00+10:00", "window_end": "2026-10-12T12:00:00+10:00",
            "intervals_expected": 25, "intervals_found": 25, "created_at": "x", "updated_at": "x",
            "sources": [], "outputs": [], "next_day": {"status": "done", "trading_days": [], "note": ""}, "notes": ""
        });
        fs::write(dir.join("manifest.json"), serde_json::to_vec(&m).unwrap()).unwrap();
        write(
            &dir.join("bundle"),
            "dispatch_price.parquet",
            &["SETTLEMENTDATE", "REGIONID"],
            &[
                &["2026/10/12 10:00:00", "NSW1"],
                &["2026/10/12 12:00:00", "NSW1"],
            ],
        );
        dir
    }

    #[test]
    fn equations_keep_the_versions_nemcf_resolves_to() {
        let dir = record();
        let b = dir.join("bundle");
        // A: version 2 invoked and usable. B: version 5 invoked but not
        // archived, so nemcf takes 4, the latest earlier usable one.
        write(
            &b,
            "dispatch_constraint.parquet",
            &[
                "CONSTRAINTID",
                "GENCONID_EFFECTIVEDATE",
                "GENCONID_VERSIONNO",
            ],
            &[
                &["A", "2026/09/02 00:00:00", "2"],
                &["B", "2026/10/05 00:00:00", "5"],
            ],
        );
        let gc: &[&[&str]] = &[
            &["A", "2026/09/01 00:00:00", "1", "<=", "a1"],
            &["A", "2026/09/02 00:00:00", "2", "<=", "a2"],
            &["A", "2026/10/20 00:00:00", "3", "<=", "a3"], // after the window
            &["B", "2026/08/01 00:00:00", "3", ">=", "b3"],
            &["B", "2026/09/01 00:00:00", "4", ">=", "b4"],
            &["B", "2026/09/15 00:00:00", "1", "", "b no type"], // unusable, but latest by the window end
        ];
        write(
            &b,
            GENCONDATA,
            &[
                "GENCONID",
                "EFFECTIVEDATE",
                "VERSIONNO",
                "CONSTRAINTTYPE",
                "DESCRIPTION",
            ],
            gc,
        );
        write(
            &b,
            "static_spdconnectionpointconstraint.parquet",
            &[
                "GENCONID",
                "EFFECTIVEDATE",
                "VERSIONNO",
                "CONNECTIONPOINTID",
                "FACTOR",
            ],
            &[
                &["A", "2026/09/01 00:00:00", "1", "CP1", "1"],
                &["A", "2026/09/02 00:00:00", "2", "CP1", "1"],
                &["A", "2026/09/02 00:00:00", "2", "CP2", "0.5"],
                &["B", "2026/08/01 00:00:00", "3", "CP1", "1"],
                &["B", "2026/09/01 00:00:00", "4", "CP3", "1"],
            ],
        );
        slim(&dir).unwrap();
        assert_eq!(
            column(&b, GENCONDATA, "DESCRIPTION"),
            ["a2", "b4", "b no type"]
        );
        assert_eq!(
            column(
                &b,
                "static_spdconnectionpointconstraint.parquet",
                "CONNECTIONPOINTID"
            ),
            ["CP1", "CP2", "CP3"]
        );
        // Idempotent.
        let first = Manifest::load(&dir).unwrap().slimmed.unwrap();
        let again = slim(&dir).unwrap();
        assert_eq!(first.tables, again.tables);
        assert_eq!(again.tables[0].rows_before, 6);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn offer_times_stored_as_text_are_read() {
        // Records built before parse_market took milliseconds hold such
        // columns as text.
        let a = StringArray::from(vec![Some("2024/08/01 05:06:39.000"), None]);
        let schema = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "OFFERDATE",
            arrow_schema::DataType::Utf8,
            true,
        )]);
        let b = RecordBatch::try_new(std::sync::Arc::new(schema), vec![std::sync::Arc::new(a)])
            .unwrap();
        let t = parse_market("2024/08/01 05:06:39").unwrap();
        assert_eq!(
            millis(&b, "OFFERDATE"),
            [Some(t.and_utc().timestamp_millis()), None]
        );
    }

    #[test]
    fn offers_keep_versions_in_force_and_the_last_five_reasons() {
        let dir = record();
        let b = dir.join("bundle");
        // OFFERTRK: U1's earliest offer in force was submitted at 02:00.
        write(
            &b,
            "offer_trk.parquet",
            &["SETTLEMENTDATE", "DUID", "BIDTYPE", "BIDOFFERDATE"],
            &[
                &["2026/10/12 10:00:00", "U1", "ENERGY", "2026/10/12 02:00:00"],
                &["2026/10/12 11:00:00", "U1", "ENERGY", "2026/10/12 11:00:00"],
            ],
        );
        let day = "2026/10/12 00:00:00";
        let rows: &[&[&str]] = &[
            &["U1", "ENERGY", day, "2026/10/11 13:00:00", "r1"], // superseded
            &["U1", "ENERGY", day, "2026/10/12 01:00:00", "r1"], // superseded
            &["U1", "ENERGY", day, "2026/10/12 02:00:00", "r2"], // in force at 10:00
            &["U1", "ENERGY", day, "2026/10/12 03:00:00", "r3"],
            &["U1", "ENERGY", day, "2026/10/12 11:00:00", "r4"],
            &["U1", "ENERGY", day, "2026/10/12 13:00:00", "r5"], // after the window
            &["U1", "RAISE6SEC", day, "2026/10/12 03:00:00", "fcas"],
            // U2: no OFFERTRK rows, so its cutoff is the window start. Six
            // runs of reasons, so the five shown start at 04:30.
            &["U2", "ENERGY", day, "2026/10/12 04:00:00", "a"],
            &["U2", "ENERGY", day, "2026/10/12 04:30:00", "b"],
            &["U2", "ENERGY", day, "2026/10/12 05:00:00", "b"],
            &["U2", "ENERGY", day, "2026/10/12 06:00:00", "c"],
            &["U2", "ENERGY", day, "2026/10/12 07:00:00", "d"],
            &["U2", "ENERGY", day, "2026/10/12 08:00:00", "e"],
            &["U2", "ENERGY", day, "2026/10/12 09:00:00", "f"],
        ];
        write(
            &b,
            DAY_OFFER,
            &[
                "DUID",
                "BIDTYPE",
                "SETTLEMENTDATE",
                "OFFERDATE",
                "REBIDEXPLANATION",
            ],
            rows,
        );
        slim(&dir).unwrap();
        assert_eq!(
            column(&b, DAY_OFFER, "OFFERDATE"),
            // U1: 02:00 to 11:00 for nemcf; its four reasons (r1 to r4) all
            // show, so r1's rows (11 Oct 13:00, 01:00) stay too.
            // U2: 09:00 is in force at 10:00; 04:30 starts the fifth-last run.
            [
                "13:00", "01:00", "02:00", "03:00", "11:00", "04:30", "05:00", "06:00", "07:00",
                "08:00", "09:00"
            ]
        );
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn fcas_offers_keep_the_versions_the_period_offers_name() {
        let dir = record();
        let b = dir.join("bundle");
        let day = "2026/10/12 00:00:00";
        write(
            &b,
            DAY_OFFER,
            &["DUID", "BIDTYPE", "SETTLEMENTDATE", "OFFERDATE"],
            &[
                &["U1", "RAISE6SEC", day, "2026/10/12 03:00:00"], // named
                &["U1", "RAISE6SEC", day, "2026/10/12 05:00:00"], // not named
                &["U1", "LOWERREG", day, "2026/07/01 09:00:00"],  // a standing offer, named
                &["U1", "LOWERREG", day, "2026/10/12 03:00:00"],  // another service's time
            ],
        );
        write(
            &b,
            "bids_offerperiod.parquet",
            &["DUID", "BIDTYPE", "SETTLEMENTDATE", "OFFERDATE", "PERIODID"],
            &[
                &["U1", "RAISE6SEC", day, "2026/10/12 03:00:00", "72"],
                &["U1", "RAISE6SEC", day, "2026/10/12 03:00:00", "73"],
                &["U1", "LOWERREG", day, "2026/07/01 09:00:00", "72"],
            ],
        );
        slim(&dir).unwrap();
        assert_eq!(column(&b, DAY_OFFER, "BIDTYPE"), ["RAISE6SEC", "LOWERREG"]);
        assert_eq!(column(&b, DAY_OFFER, "OFFERDATE"), ["03:00", "09:00"]);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn fcas_versions_read_pre_5ms_period_columns() {
        // Before Oct 2021 BIDPEROFFER_D names the day and version
        // TRADINGDATE and OFFERDATETIME.
        let dir = record();
        let b = dir.join("bundle");
        let day = "2020/11/17 00:00:00";
        write(
            &b,
            DAY_OFFER,
            &["DUID", "BIDTYPE", "SETTLEMENTDATE", "OFFERDATE"],
            &[
                &["U1", "RAISE6SEC", day, "2020/11/16 14:54:16"],
                &["U1", "RAISE6SEC", day, "2020/11/17 01:00:00"],
            ],
        );
        write(
            &b,
            "bids_offerperiod.parquet",
            &[
                "DUID",
                "BIDTYPE",
                "TRADINGDATE",
                "OFFERDATETIME",
                "PERIODID",
            ],
            &[&["U1", "RAISE6SEC", day, "2020/11/16 14:54:16", "20"]],
        );
        slim(&dir).unwrap();
        assert_eq!(column(&b, DAY_OFFER, "OFFERDATE"), ["14:54"]);
        fs::remove_dir_all(&dir).unwrap();
    }
}

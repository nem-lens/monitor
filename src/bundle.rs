//! Event bundles: the AEMO data for one confirmed event, kept as Parquet.
//!
//! The monitor stores event data only; NEMWeb and MMSDM stay the ground truth
//! for everything else. When an event closes, its window (start minus 2 h to
//! end plus 1 h, as interval ends in market time) is sliced from the raw
//! DispatchIS buffer into `bundles/<event_id>/`. When next-day data is public,
//! bids, unit dispatch and offer tracking for the window are added. Each
//! bundle's `manifest.json` lists every AEMO source file with its URL and
//! SHA-256, and every output with its row count and SHA-256.
//!
//! Columns keep AEMO's names. Types are inferred per column: integers, floats,
//! market-time timestamps (naive, AEST, interval end) or text.

use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_array::{
    Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use chrono::{Duration, NaiveDate, NaiveDateTime, Utc};
use nem_lens_aemo::bids::period_of;
use nem_lens_aemo::files::{visit_csvs, MAX_MEMBER_BYTES};
use nem_lens_aemo::report::{parse_report_where, Record, Table, Tables};
use nem_lens_aemo::time::{iso_market, parse_market};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::model::Event;
use crate::store::SeriesPoint;

pub const MANIFEST: &str = "manifest.json";

/// DispatchIS tables kept in a bundle, and their output files.
pub const DISPATCH_TABLES: [(&str, &str); 4] = [
    ("DISPATCH_PRICE", "dispatch_price.parquet"),
    ("DISPATCH_REGIONSUM", "dispatch_regionsum.parquet"),
    ("DISPATCH_CONSTRAINT", "dispatch_constraint.parquet"),
    (
        "DISPATCH_INTERCONNECTORRES",
        "dispatch_interconnectorres.parquet",
    ),
];

/// Next-day tables kept in a bundle, and their output files.
pub const NEXT_DAY_TABLES: [(&str, &str); 4] = [
    ("DISPATCH_UNIT_SOLUTION", "unit_dispatch.parquet"),
    ("DISPATCH_OFFERTRK", "offer_trk.parquet"),
    ("BIDS_BIDDAYOFFER", "bids_dayoffer.parquet"),
    ("BIDS_BIDOFFERPERIOD_SPARSE", "bids_offerperiod.parquet"),
];

/// Interval ends from `start` to `end`, inclusive, in market time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Window {
    pub start: NaiveDateTime,
    pub end: NaiveDateTime,
}

impl Window {
    /// Start minus 2 h to end plus 1 h. `None` until the event has an end.
    pub fn for_event(ev: &Event) -> Option<Self> {
        Some(Self {
            start: ev.start_time()? - Duration::hours(2),
            end: ev.end_time()? + Duration::hours(1),
        })
    }

    pub fn contains(&self, t: NaiveDateTime) -> bool {
        self.start <= t && t <= self.end
    }

    pub fn intervals(&self) -> usize {
        ((self.end - self.start).num_minutes() / 5 + 1) as usize
    }

    /// Trading days the window touches, each with the periods it covers.
    pub fn trading_periods(&self) -> Vec<(NaiveDate, u16, u16)> {
        let mut out: Vec<(NaiveDate, u16, u16)> = Vec::new();
        let mut t = self.start;
        while t <= self.end {
            let (day, p) = period_of(t);
            match out.last_mut() {
                Some((d, _, to)) if *d == day => *to = p,
                _ => out.push((day, p, p)),
            }
            t += Duration::minutes(5);
        }
        out
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Source {
    pub file: String,
    pub url: String,
    pub sha256: String,
    pub bytes: u64,
    /// "dispatchis" or the next-day directory it came from.
    pub role: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Output {
    pub file: String,
    pub table: String,
    pub rows: usize,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct NextDay {
    /// "pending", "added" or "unavailable".
    pub status: String,
    pub trading_days: Vec<String>,
    pub note: String,
}

/// A generic constraint version: (GENCONID, EFFECTIVEDATE, VERSIONNO).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ConstraintVersion {
    pub constraint_id: String,
    pub effective_date: String,
    pub version_no: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct StaticData {
    /// MMSDM archive months searched, e.g. "2026_09".
    pub months: Vec<String>,
    pub invoked_versions: usize,
    pub equations_found: usize,
    /// Versions of invoked constraints other than those invoked, sliced for
    /// nemcf's fallback when an invoked version is not archived.
    #[serde(default)]
    pub other_versions: usize,
    /// Invoked constraints with no version at all in the months searched.
    #[serde(default)]
    pub ids_without_any_version: Vec<String>,
    /// Invoked versions with no GENCONDATA row in the months searched.
    pub missing_equations: Vec<ConstraintVersion>,
    pub dudetail_month: Option<String>,
    /// Month whose INTERCONNECTORCONSTRAINT history was sliced.
    #[serde(default)]
    pub interconnector_month: Option<String>,
    /// Month whose DUALLOC and GENUNITS (unit fuels and capacities) were
    /// sliced.
    #[serde(default)]
    pub fuel_month: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Manifest {
    pub format: u32,
    pub generator: String,
    pub event_id: String,
    pub region: String,
    pub window_start: String,
    pub window_end: String,
    pub intervals_expected: usize,
    pub intervals_found: usize,
    pub created_at: String,
    pub updated_at: String,
    pub sources: Vec<Source>,
    pub outputs: Vec<Output>,
    pub next_day: NextDay,
    pub notes: String,
    /// MMSDM equations and unit details, added by `bundle add-static`.
    #[serde(default)]
    pub static_data: Option<StaticData>,
}

impl Manifest {
    pub fn load(dir: &Path) -> Result<Self> {
        let text = fs::read_to_string(dir.join(MANIFEST))
            .with_context(|| format!("reading {}", dir.join(MANIFEST).display()))?;
        Ok(serde_json::from_str(&text)?)
    }

    fn save(&self, dir: &Path) -> Result<()> {
        fs::write(dir.join(MANIFEST), serde_json::to_vec_pretty(self)?)?;
        Ok(())
    }
}

fn now_iso() -> String {
    Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

/// Rows of one AEMO table gathered across files. Columns are the union of
/// every header seen, so a version change that adds a column still fits.
#[derive(Debug, Default)]
pub struct TableBuf {
    columns: Vec<String>,
    index: HashMap<String, usize>,
    rows: Vec<Vec<Option<String>>>,
}

impl TableBuf {
    fn col(&mut self, name: &str) -> usize {
        if let Some(i) = self.index.get(name) {
            return *i;
        }
        self.columns.push(name.to_string());
        self.index.insert(name.to_string(), self.columns.len() - 1);
        self.columns.len() - 1
    }

    pub fn add(&mut self, t: &Table) {
        let map: Vec<usize> = t.columns.iter().map(|c| self.col(c)).collect();
        for row in &t.rows {
            let mut out = vec![None; self.columns.len()];
            for (src, v) in row.iter().enumerate() {
                if let Some(dst) = map.get(src) {
                    let v = v.trim();
                    if !v.is_empty() {
                        out[*dst] = Some(v.to_string());
                    }
                }
            }
            self.rows.push(out);
        }
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    fn values(&self, i: usize) -> impl Iterator<Item = Option<&str>> {
        self.rows
            .iter()
            .map(move |r| r.get(i).and_then(|v| v.as_deref()))
    }

    /// Infer a type per column and build an Arrow batch. Numbers are Float64
    /// unless the column is a key or count ([`integer_column`]), so a column
    /// has the same type in every bundle whatever values a window held.
    pub fn to_batch(&self) -> Result<RecordBatch> {
        let mut fields = Vec::new();
        let mut arrays: Vec<ArrayRef> = Vec::new();
        for (i, name) in self.columns.iter().enumerate() {
            let present: Vec<&str> = self.values(i).flatten().collect();
            let (dt, arr): (DataType, ArrayRef) = if integer_column(name)
                && !present.is_empty()
                && present.iter().all(|v| v.parse::<i64>().is_ok())
            {
                let a: Int64Array = self
                    .values(i)
                    .map(|v| v.and_then(|s| s.parse().ok()))
                    .collect();
                (DataType::Int64, Arc::new(a))
            } else if !present.is_empty() && present.iter().all(|v| v.parse::<f64>().is_ok()) {
                let a: Float64Array = self
                    .values(i)
                    .map(|v| v.and_then(|s| s.parse().ok()))
                    .collect();
                (DataType::Float64, Arc::new(a))
            } else if !present.is_empty() && present.iter().all(|v| parse_market(v).is_some()) {
                let a: TimestampMillisecondArray = self
                    .values(i)
                    .map(|v| {
                        v.and_then(parse_market)
                            .map(|t| t.and_utc().timestamp_millis())
                    })
                    .collect();
                (
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    Arc::new(a),
                )
            } else {
                let a: StringArray = self.values(i).collect();
                (DataType::Utf8, Arc::new(a))
            };
            fields.push(Field::new(name, dt, true));
            arrays.push(arr);
        }
        Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)
    }

    /// Write as Parquet; returns the file's SHA-256.
    pub fn write_parquet(&self, path: &Path) -> Result<String> {
        let batch = self.to_batch()?;
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build();
        let mut buf = Vec::new();
        let mut w = ArrowWriter::try_new(&mut buf, batch.schema(), Some(props))?;
        w.write(&batch)?;
        w.close()?;
        fs::write(path, &buf).with_context(|| format!("writing {}", path.display()))?;
        Ok(sha256_hex(&buf))
    }
}

/// AEMO columns that are identifiers, run numbers, flags or period indexes:
/// stored as Int64. Every other numeric column is Float64.
pub fn integer_column(name: &str) -> bool {
    matches!(
        name,
        "RUNNO"
            | "INTERVENTION"
            | "DISPATCHINTERVAL"
            | "PERIODID"
            | "PERIODIDTO"
            | "DISPATCHMODE"
            | "AGCSTATUS"
            | "SEMIDISPATCHCAP"
            | "APCFLAG"
            | "MARKETSUSPENDEDFLAG"
            | "RECALL_PERIOD"
    ) || name.ends_with("VERSIONNO")
        || name.ends_with("_FLAG")
}

/// A raw DispatchIS zip in the buffer.
pub struct RawFile {
    pub name: String,
    pub url: String,
    pub bytes: Vec<u8>,
}

fn base_manifest(ev: &Event, w: Window) -> Manifest {
    Manifest {
        format: 1,
        generator: concat!("nem-monitor ", env!("CARGO_PKG_VERSION")).into(),
        event_id: ev.id.clone(),
        region: ev.region.clone(),
        window_start: iso_market(w.start),
        window_end: iso_market(w.end),
        intervals_expected: w.intervals(),
        intervals_found: 0,
        created_at: now_iso(),
        updated_at: now_iso(),
        sources: Vec::new(),
        outputs: Vec::new(),
        next_day: NextDay {
            status: "pending".into(),
            ..Default::default()
        },
        notes: String::new(),
        static_data: None,
    }
}

/// Write the DispatchIS part of a bundle from the raw files in the window.
pub fn write_dispatch(dir: &Path, ev: &Event, w: Window, raw: &[RawFile]) -> Result<Manifest> {
    fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    let mut m = base_manifest(ev, w);
    let mut bufs: HashMap<&str, TableBuf> = HashMap::new();
    let mut intervals = BTreeSet::new();
    for f in raw {
        m.sources.push(Source {
            file: f.name.clone(),
            url: f.url.clone(),
            sha256: sha256_hex(&f.bytes),
            bytes: f.bytes.len() as u64,
            role: "dispatchis".into(),
        });
        for (_, csv) in nem_lens_aemo::files::unzip_csvs(&f.bytes)? {
            let tables = nem_lens_aemo::parse_report(&csv)?;
            for (key, _) in DISPATCH_TABLES {
                if let Some(t) = tables.get(key) {
                    bufs.entry(key).or_default().add(t);
                    for row in &t.rows {
                        if let Some(sd) = t.col(row, "SETTLEMENTDATE").and_then(parse_market) {
                            intervals.insert(sd);
                        }
                    }
                }
            }
        }
    }
    m.intervals_found = intervals.iter().filter(|t| w.contains(**t)).count();
    if m.intervals_found < m.intervals_expected {
        m.notes = format!(
            "{} of {} intervals in the window were in the raw buffer",
            m.intervals_found, m.intervals_expected
        );
    }
    for (key, file) in DISPATCH_TABLES {
        // A table with no rows in any file has no columns either; Parquet needs
        // a schema, so it is left out and its absence is the record.
        let Some(buf) = bufs.remove(key).filter(|b| !b.columns.is_empty()) else {
            continue;
        };
        let sha = buf.write_parquet(&dir.join(file))?;
        m.outputs.push(Output {
            file: file.into(),
            table: key.into(),
            rows: buf.len(),
            sha256: sha,
        });
    }
    m.save(dir)?;
    Ok(m)
}

/// A next-day file to slice into the bundle.
pub struct NextDayFile {
    /// NEMWeb directory, e.g. Next_Day_Dispatch.
    pub source: String,
    pub name: String,
    pub url: String,
    pub bytes: Vec<u8>,
}

fn in_window(r: &Record, col: &str, w: &Window) -> bool {
    r.text(col)
        .and_then(parse_market)
        .is_some_and(|t| w.contains(t))
}

/// Keep the rows of next-day tables that fall in the window.
fn keep_next_day(r: &Record, w: &Window, days: &[(NaiveDate, u16, u16)]) -> bool {
    let day_of = |col: &str| r.text(col).and_then(parse_market).map(|t| t.date());
    match r.key {
        "DISPATCH_UNIT_SOLUTION" | "DISPATCH_OFFERTRK" => in_window(r, "SETTLEMENTDATE", w),
        // Every version for the days touched: choosing the one in force needs them all.
        "BIDS_BIDDAYOFFER" => {
            day_of("SETTLEMENTDATE").is_some_and(|d| days.iter().any(|x| x.0 == d))
        }
        "BIDS_BIDOFFERPERIOD_SPARSE" => {
            let (Some(d), Some(a), Some(b)) = (
                day_of("TRADINGDATE"),
                r.num("PERIODID"),
                r.num("PERIODIDTO"),
            ) else {
                return false;
            };
            days.iter()
                .any(|(day, from, to)| *day == d && a <= f64::from(*to) && b >= f64::from(*from))
        }
        _ => false,
    }
}

/// Slice next-day files into an existing bundle and update its manifest.
pub fn add_next_day(dir: &Path, files: &[NextDayFile], missing: &[String]) -> Result<Manifest> {
    let mut m = Manifest::load(dir)?;
    let w = Window {
        start: parse_market(&m.window_start).context("manifest window_start")?,
        end: parse_market(&m.window_end).context("manifest window_end")?,
    };
    let days = w.trading_periods();
    let mut bufs: HashMap<String, TableBuf> = HashMap::new();
    for f in files {
        m.sources.retain(|s| s.file != f.name);
        m.sources.push(Source {
            file: f.name.clone(),
            url: f.url.clone(),
            sha256: sha256_hex(&f.bytes),
            bytes: f.bytes.len() as u64,
            role: f.source.clone(),
        });
        visit_csvs(&f.bytes, MAX_MEMBER_BYTES, |_, r: &mut dyn Read| {
            let tables = parse_report_where(r, |rec| keep_next_day(rec, &w, &days))?;
            for (key, _) in NEXT_DAY_TABLES {
                if let Some(t) = tables.get(key) {
                    bufs.entry(key.to_string()).or_default().add(t);
                }
            }
            Ok(())
        })?;
    }
    for (key, file) in NEXT_DAY_TABLES {
        if let Some(buf) = bufs.remove(key) {
            let sha = buf.write_parquet(&dir.join(file))?;
            m.outputs.retain(|o| o.file != file);
            m.outputs.push(Output {
                file: file.into(),
                table: key.into(),
                rows: buf.len(),
                sha256: sha,
            });
        }
    }
    m.next_day = NextDay {
        status: if files.is_empty() {
            "unavailable"
        } else {
            "added"
        }
        .into(),
        trading_days: days.iter().map(|d| d.0.to_string()).collect(),
        note: if missing.is_empty() {
            String::new()
        } else {
            format!("not available: {}", missing.join(", "))
        },
    };
    m.updated_at = now_iso();
    m.save(dir)?;
    Ok(m)
}

/// MMSDM tables added by [`add_static`]: (table key, folder under the month,
/// output file).
pub const STATIC_TABLES: [(&str, &str, &str); 4] = [
    ("GENCONDATA_NULL", "GENCONDATA", "static_gencondata.parquet"),
    (
        "SPDCPC_NULL",
        "SPDCONNECTIONPOINTCONSTRAINT",
        "static_spdconnectionpointconstraint.parquet",
    ),
    (
        "SPDICC_NULL",
        "SPDINTERCONNECTORCONSTRAINT",
        "static_spdinterconnectorconstraint.parquet",
    ),
    (
        "SPDRC_NULL",
        "SPDREGIONCONSTRAINT",
        "static_spdregionconstraint.parquet",
    ),
];
const INTERCONNECTOR: (&str, &str, &str) = (
    "MARKET_CONFIG_INTERCONNECTORCONSTRAINT",
    "INTERCONNECTORCONSTRAINT",
    "static_interconnectorconstraint.parquet",
);
const DUDETAIL: (&str, &str, &str) = (
    "PARTICIPANT_REGISTRATION_DUDETAILSUMMARY",
    "DUDETAILSUMMARY",
    "static_dudetailsummary.parquet",
);
const DUALLOC: (&str, &str, &str) = (
    "PARTICIPANT_REGISTRATION_DUALLOC",
    "DUALLOC",
    "static_dualloc.parquet",
);
const GENUNITS: (&str, &str, &str) = (
    "PARTICIPANT_REGISTRATION_GENUNITS",
    "GENUNITS",
    "static_genunits.parquet",
);

/// Constraint versions invoked in the bundle's dispatch_constraint table.
fn invoked_versions(dir: &Path) -> Result<BTreeSet<ConstraintVersion>> {
    let mut out = BTreeSet::new();
    for b in read_batches(&dir.join("dispatch_constraint.parquet"))? {
        let (Some(id), Some(eff), Some(ver)) = (
            column::<StringArray>(&b, "CONSTRAINTID"),
            column::<TimestampMillisecondArray>(&b, "GENCONID_EFFECTIVEDATE"),
            column::<Int64Array>(&b, "GENCONID_VERSIONNO"),
        ) else {
            anyhow::bail!(
                "dispatch_constraint.parquet lacks GENCONID_EFFECTIVEDATE or GENCONID_VERSIONNO"
            );
        };
        for i in 0..b.num_rows() {
            if id.is_null(i) || eff.is_null(i) || ver.is_null(i) {
                continue;
            }
            let Some(t) = chrono::DateTime::from_timestamp_millis(eff.value(i)) else {
                continue;
            };
            out.insert(ConstraintVersion {
                constraint_id: id.value(i).to_string(),
                effective_date: iso_market(t.naive_utc()),
                version_no: ver.value(i),
            });
        }
    }
    Ok(out)
}

fn month_dir(t: NaiveDateTime) -> String {
    t.format("%Y_%m").to_string()
}

/// The `YYYY_MM` folders under an MMSDM root, sorted.
fn month_dirs(mmsdm: &Path) -> Vec<String> {
    let mut v: Vec<String> = fs::read_dir(mmsdm)
        .map(|rd| {
            rd.filter_map(|e| e.ok())
                .filter(|e| e.path().is_dir())
                .map(|e| e.file_name().to_string_lossy().to_string())
                .filter(|n| {
                    n.len() == 7
                        && n.as_bytes()[4] == b'_'
                        && n.bytes()
                            .enumerate()
                            .all(|(i, b)| i == 4 || b.is_ascii_digit())
                })
                .collect()
        })
        .unwrap_or_default();
    v.sort();
    v
}

/// CSV or zip files in `<mmsdm>/<month>/<folder>/`, sorted.
fn mmsdm_files(mmsdm: &Path, month: &str, folder: &str) -> Vec<PathBuf> {
    let Ok(rd) = fs::read_dir(mmsdm.join(month).join(folder)) else {
        return Vec::new();
    };
    let mut v: Vec<PathBuf> = rd
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| {
            let n = p.to_string_lossy().to_ascii_lowercase();
            n.ends_with(".csv") || n.ends_with(".zip")
        })
        .collect();
    v.sort();
    v
}

/// Stream the CSV rows of an MMSDM file (plain CSV or zip) through `keep`.
fn read_mmsdm(path: &Path, mut keep: impl FnMut(&Record) -> bool) -> Result<(Vec<u8>, Tables)> {
    let bytes = fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    let mut tables = Tables::new();
    if path
        .to_string_lossy()
        .to_ascii_lowercase()
        .ends_with(".zip")
    {
        visit_csvs(&bytes, MAX_MEMBER_BYTES, |_, r: &mut dyn Read| {
            for (k, t) in parse_report_where(r, &mut keep)? {
                let e: &mut Table = tables.entry(k).or_default();
                e.columns = t.columns;
                e.rows.extend(t.rows);
            }
            Ok(())
        })?;
    } else {
        tables = parse_report_where(bytes.as_slice(), &mut keep)?;
    }
    Ok((bytes, tables))
}

/// Add the MMSDM equations for the constraints the event invoked, and the
/// unit registration details in force at the window start, from a local
/// MMSDM tree laid out as `<mmsdm>/<YYYY_MM>/<TABLE>/*.CSV`.
///
/// Every version of each invoked GENCONID is sliced, from every month up to
/// the window's, not only the exact versions invoked: when an invoked
/// version is not archived yet (the current month's archive appears early
/// the next month), nemcf falls back to another version that reproduces
/// AEMO's published LHS. Rows repeated across months are deduplicated.
/// Pre-2016 tables are keyed with an empty sub-table (`GENCONDATA_`) and are
/// read as the `_NULL` tables.
pub fn add_static(dir: &Path, mmsdm: &Path) -> Result<Manifest> {
    let mut m = Manifest::load(dir)?;
    let window_start = parse_market(&m.window_start).context("manifest window_start")?;
    let window_end = parse_market(&m.window_end).context("manifest window_end")?;
    let invoked = invoked_versions(dir)?;
    let ids: BTreeSet<&str> = invoked.iter().map(|v| v.constraint_id.as_str()).collect();
    let last = month_dir(window_end);
    let months: Vec<String> = month_dirs(mmsdm)
        .into_iter()
        .filter(|mo| *mo <= last)
        .collect();

    let mut bufs: HashMap<&str, TableBuf> = HashMap::new();
    let mut seen_rows: BTreeSet<String> = BTreeSet::new();
    let mut found: BTreeSet<ConstraintVersion> = BTreeSet::new();
    let mut sliced: BTreeSet<ConstraintVersion> = BTreeSet::new();
    for month in &months {
        for (key, folder, _) in STATIC_TABLES {
            for path in mmsdm_files(mmsdm, month, folder) {
                let (bytes, tables) = read_mmsdm(&path, |r| {
                    r.text("GENCONID").is_some_and(|c| ids.contains(c.trim()))
                })?;
                let old_key = key.trim_end_matches("NULL");
                let mut contributed = false;
                for t in [tables.get(key), tables.get(old_key)].into_iter().flatten() {
                    // Dedupe rows repeated across months on the version key
                    // plus the term's identity (connection point,
                    // interconnector or region, and service).
                    let mut kept = Table {
                        columns: t.columns.clone(),
                        rows: Vec::new(),
                    };
                    for row in &t.rows {
                        let part = |c: &str| t.col(row, c).unwrap_or("").trim().to_string();
                        let version = || -> Option<ConstraintVersion> {
                            Some(ConstraintVersion {
                                constraint_id: part("GENCONID"),
                                effective_date: iso_market(parse_market(&part("EFFECTIVEDATE"))?),
                                version_no: part("VERSIONNO").parse().ok()?,
                            })
                        };
                        let Some(v) = version() else { continue };
                        let id = format!(
                            "{key}|{}|{}|{}|{}{}{}|{}",
                            v.constraint_id,
                            v.effective_date,
                            v.version_no,
                            part("CONNECTIONPOINTID"),
                            part("INTERCONNECTORID"),
                            part("REGIONID"),
                            part("BIDTYPE")
                        );
                        if seen_rows.insert(id) {
                            if key == "GENCONDATA_NULL" {
                                if invoked.contains(&v) {
                                    found.insert(v.clone());
                                }
                                sliced.insert(v);
                            }
                            kept.rows.push(row.clone());
                        }
                    }
                    contributed |= !t.rows.is_empty();
                    bufs.entry(key).or_default().add(&kept);
                }
                // Sources are the files holding matching rows, duplicates
                // included; every month searched is in static_data.months.
                if contributed {
                    record_source(&mut m, &path, &bytes, month);
                }
            }
        }
    }

    // Unit details and interconnector limits: each month's file is a full
    // history, so take the newest month at or before the window.
    let latest = |folder: &str| {
        month_dirs(mmsdm)
            .into_iter()
            .filter(|n| *n <= month_dir(window_start))
            .rev()
            .find(|mo| !mmsdm_files(mmsdm, mo, folder).is_empty())
    };

    // Every version effective by the window end, for the bundle's
    // interconnectors: nemcf restores an interconnector's nominal limit when
    // the constraint that set it is modelled.
    let ics = interconnector_ids(dir)?;
    let (ic_key, ic_folder, _) = INTERCONNECTOR;
    let interconnector_month = latest(ic_folder);
    if let Some(month) = &interconnector_month {
        for path in mmsdm_files(mmsdm, month, ic_folder) {
            let (bytes, tables) = read_mmsdm(&path, |r| {
                r.text("EFFECTIVEDATE")
                    .and_then(parse_market)
                    .is_some_and(|e| e <= window_end)
                    && ics.as_ref().is_none_or(|s| {
                        r.text("INTERCONNECTORID")
                            .is_some_and(|i| s.contains(i.trim()))
                    })
            })?;
            if let Some(t) = tables.get(ic_key) {
                bufs.entry(ic_key).or_default().add(t);
            }
            record_source(&mut m, &path, &bytes, month);
        }
    }

    // Unit details: rows in force at the window start.
    let units = unit_ids(dir)?;
    let (key, folder, file) = DUDETAIL;
    let mut dudetail_month = None;
    if let Some(month) = latest(folder).as_ref() {
        for path in mmsdm_files(mmsdm, month, folder) {
            let (bytes, tables) = read_mmsdm(&path, |r| {
                let in_force = r
                    .text("START_DATE")
                    .and_then(parse_market)
                    .is_some_and(|s| s <= window_start)
                    && r.text("END_DATE")
                        .and_then(parse_market)
                        .is_none_or(|e| e > window_start);
                let wanted = units
                    .as_ref()
                    .is_none_or(|u| r.text("DUID").is_some_and(|d| u.contains(d)));
                in_force && wanted
            })?;
            if let Some(t) = tables.get(key) {
                bufs.entry(key).or_default().add(t);
            }
            record_source(&mut m, &path, &bytes, month);
        }
        dudetail_month = Some(month.clone());
    }

    // Unit fuels and capacities: each DUID's generating sets (DUALLOC,
    // allocations effective by the window start), then those sets
    // (GENUNITS). Both are full histories, so the newest month will do.
    let (al_key, al_folder, _) = DUALLOC;
    let (gu_key, gu_folder, _) = GENUNITS;
    let fuel_month = latest(al_folder).filter(|mo| !mmsdm_files(mmsdm, mo, gu_folder).is_empty());
    if let Some(month) = &fuel_month {
        let mut gensets: BTreeSet<String> = BTreeSet::new();
        for path in mmsdm_files(mmsdm, month, al_folder) {
            let (bytes, tables) = read_mmsdm(&path, |r| {
                r.text("EFFECTIVEDATE")
                    .and_then(parse_market)
                    .is_some_and(|e| e <= window_start)
                    && units
                        .as_ref()
                        .is_none_or(|u| r.text("DUID").is_some_and(|d| u.contains(d.trim())))
            })?;
            if let Some(t) = tables.get(al_key) {
                for row in &t.rows {
                    if let Some(g) = t.col(row, "GENSETID") {
                        gensets.insert(g.trim().to_string());
                    }
                }
                bufs.entry(al_key).or_default().add(t);
            }
            record_source(&mut m, &path, &bytes, month);
        }
        for path in mmsdm_files(mmsdm, month, gu_folder) {
            let (bytes, tables) = read_mmsdm(&path, |r| {
                r.text("GENSETID")
                    .is_some_and(|g| gensets.contains(g.trim()))
            })?;
            if let Some(t) = tables.get(gu_key) {
                bufs.entry(gu_key).or_default().add(t);
            }
            record_source(&mut m, &path, &bytes, month);
        }
    }

    for (key, _, file) in STATIC_TABLES.iter().copied().chain([
        INTERCONNECTOR,
        (key, folder, file),
        DUALLOC,
        GENUNITS,
    ]) {
        if let Some(buf) = bufs.remove(key) {
            let sha = buf.write_parquet(&dir.join(file))?;
            m.outputs.retain(|o| o.file != file);
            m.outputs.push(Output {
                file: file.into(),
                table: key.into(),
                rows: buf.len(),
                sha256: sha,
            });
        }
    }
    let missing: Vec<ConstraintVersion> = invoked
        .iter()
        .filter(|v| !found.contains(v))
        .cloned()
        .collect();
    let with_any: BTreeSet<&str> = sliced.iter().map(|v| v.constraint_id.as_str()).collect();
    m.static_data = Some(StaticData {
        months,
        invoked_versions: invoked.len(),
        equations_found: invoked.len() - missing.len(),
        other_versions: sliced.len() - found.len(),
        ids_without_any_version: ids
            .iter()
            .filter(|c| !with_any.contains(*c))
            .map(|c| c.to_string())
            .collect(),
        missing_equations: missing,
        dudetail_month,
        interconnector_month,
        fuel_month,
    });
    m.updated_at = now_iso();
    m.save(dir)?;
    Ok(m)
}

/// NEMWeb market notice files (`NEMITWEB1_MKTNOTICE_<YYYYMMDD>.R<id>`) in a
/// directory listing, with their dates.
pub fn notice_files(listing: &str) -> Vec<(String, NaiveDate)> {
    const TAG: &str = "NEMITWEB1_MKTNOTICE_";
    let mut out = BTreeSet::new();
    let mut rest = listing;
    while let Some(i) = rest.find(TAG) {
        let tail = &rest[i..];
        let end = tail
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_' || c == '.'))
            .unwrap_or(tail.len());
        let name = &tail[..end];
        if let Some(d) = name
            .get(TAG.len()..TAG.len() + 8)
            .and_then(|d| NaiveDate::parse_from_str(d, "%Y%m%d").ok())
        {
            if name[TAG.len() + 8..].starts_with(".R") {
                out.insert((name.to_string(), d));
            }
        }
        rest = &tail[end.max(1)..];
    }
    out.into_iter().collect()
}

/// Add the AEMO market notices issued from six hours before the bundle's
/// window to its end, fetched from NEMWeb's `Market_Notice` directory
/// (`base` is its URL). They are kept as `market_notices.json`, each with
/// its file, URL and SHA-256, which also go into the manifest's sources.
/// NEMWeb keeps recent notices only, so run this soon after the event.
pub async fn add_notices(dir: &Path, client: &reqwest::Client, base: &str) -> Result<usize> {
    use crate::extract::{FetchedNotice, NOTICES_FILE};
    let mut m = Manifest::load(dir)?;
    let ws = parse_market(&m.window_start).context("manifest window_start")?;
    let we = parse_market(&m.window_end).context("manifest window_end")?;
    let from = ws - Duration::hours(6);
    let base = base.trim_end_matches('/');
    let listing = crate::nemweb::fetch_text(client, &format!("{base}/")).await?;
    let mut kept = Vec::new();
    for (name, day) in notice_files(&listing) {
        if day < from.date() || day > we.date() {
            continue;
        }
        let url = format!("{base}/{name}");
        let bytes = crate::nemweb::fetch_bytes(client, &url).await?;
        let Some(notice) =
            nem_lens_aemo::event::parse_market_notice(&String::from_utf8_lossy(&bytes))
        else {
            continue;
        };
        let at = parse_market(&notice.issued);
        if !at.is_some_and(|t| from <= t && t <= we) {
            continue;
        }
        m.sources.retain(|s| s.file != name);
        m.sources.push(Source {
            file: name.clone(),
            url: url.clone(),
            sha256: sha256_hex(&bytes),
            bytes: bytes.len() as u64,
            role: "market_notice".into(),
        });
        kept.push(FetchedNotice {
            file: name,
            url,
            sha256: sha256_hex(&bytes),
            notice,
        });
    }
    let text = serde_json::to_vec_pretty(&kept)?;
    fs::write(dir.join(NOTICES_FILE), &text)?;
    m.outputs.retain(|o| o.file != NOTICES_FILE);
    m.outputs.push(Output {
        file: NOTICES_FILE.into(),
        table: "MARKET_NOTICE".into(),
        rows: kept.len(),
        sha256: sha256_hex(&text),
    });
    m.updated_at = now_iso();
    m.save(dir)?;
    Ok(kept.len())
}

fn record_source(m: &mut Manifest, path: &Path, bytes: &[u8], month: &str) {
    let file = path
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_default();
    m.sources.retain(|s| s.file != file);
    m.sources.push(Source {
        file,
        // MMSDM files come from the monthly archive; the local copy is what was hashed.
        url: String::new(),
        sha256: sha256_hex(bytes),
        bytes: bytes.len() as u64,
        role: format!("mmsdm:{month}"),
    });
}

/// DUIDs in unit_dispatch.parquet, if the bundle has next-day data.
fn unit_ids(dir: &Path) -> Result<Option<BTreeSet<String>>> {
    ids_in(&dir.join("unit_dispatch.parquet"), "DUID")
}

/// Interconnectors in dispatch_interconnectorres.parquet, if present.
fn interconnector_ids(dir: &Path) -> Result<Option<BTreeSet<String>>> {
    ids_in(
        &dir.join("dispatch_interconnectorres.parquet"),
        "INTERCONNECTORID",
    )
}

/// The distinct values of a text column, or `None` if the file is absent.
fn ids_in(path: &Path, col: &str) -> Result<Option<BTreeSet<String>>> {
    if !path.exists() {
        return Ok(None);
    }
    let mut out = BTreeSet::new();
    for b in read_batches(path)? {
        if let Some(d) = column::<StringArray>(&b, col) {
            for i in 0..b.num_rows() {
                if !d.is_null(i) {
                    out.insert(d.value(i).to_string());
                }
            }
        }
    }
    Ok(Some(out))
}

pub fn dir_for(bundle_root: &str, event_id: &str) -> PathBuf {
    Path::new(bundle_root).join(event_id)
}

fn read_batches(path: &Path) -> Result<Vec<RecordBatch>> {
    let f = fs::File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(f)?.build()?;
    Ok(reader.collect::<std::result::Result<_, _>>()?)
}

fn column<'a, T: 'static>(b: &'a RecordBatch, name: &str) -> Option<&'a T> {
    b.column_by_name(name)?.as_any().downcast_ref::<T>()
}

/// Price and demand for one region from a bundle, pricing run only.
pub fn read_series(dir: &Path, region: &str) -> Result<Vec<SeriesPoint>> {
    let mut demand: HashMap<i64, f64> = HashMap::new();
    for b in read_batches(&dir.join("dispatch_regionsum.parquet"))? {
        let (Some(t), Some(r), Some(d)) = (
            column::<TimestampMillisecondArray>(&b, "SETTLEMENTDATE"),
            column::<StringArray>(&b, "REGIONID"),
            column::<Float64Array>(&b, "TOTALDEMAND"),
        ) else {
            continue;
        };
        let run = column::<Int64Array>(&b, "INTERVENTION");
        for i in 0..b.num_rows() {
            if r.value(i) == region
                && run.is_none_or(|x| x.is_null(i) || x.value(i) == 0)
                && !d.is_null(i)
            {
                demand.insert(t.value(i), d.value(i));
            }
        }
    }
    let mut out = Vec::new();
    for b in read_batches(&dir.join("dispatch_price.parquet"))? {
        let (Some(t), Some(r)) = (
            column::<TimestampMillisecondArray>(&b, "SETTLEMENTDATE"),
            column::<StringArray>(&b, "REGIONID"),
        ) else {
            continue;
        };
        // RRP is integral in some windows, so accept either numeric type.
        let rrp_f = column::<Float64Array>(&b, "RRP");
        let rrp_i = column::<Int64Array>(&b, "RRP");
        let run = column::<Int64Array>(&b, "INTERVENTION");
        for i in 0..b.num_rows() {
            if r.value(i) != region || run.is_some_and(|x| !x.is_null(i) && x.value(i) != 0) {
                continue;
            }
            let rrp = match (rrp_f, rrp_i) {
                (Some(a), _) if !a.is_null(i) => a.value(i),
                (_, Some(a)) if !a.is_null(i) => a.value(i) as f64,
                _ => continue,
            };
            let Some(ts) = chrono::DateTime::from_timestamp_millis(t.value(i)) else {
                continue;
            };
            out.push(SeriesPoint {
                t: iso_market(ts.naive_utc()),
                rrp,
                demand: demand.get(&t.value(i)).copied(),
            });
        }
    }
    out.sort_by(|a, b| a.t.cmp(&b.t));
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use nem_lens_aemo::bids::PERIODS_PER_DAY;

    fn t(s: &str) -> NaiveDateTime {
        parse_market(s).unwrap()
    }

    #[test]
    fn notice_files_are_found_in_a_listing() {
        let listing = r#"<A HREF="/Reports/CURRENT/Market_Notice/NEMITWEB1_MKTNOTICE_20261012.R145591">x</A>
            <A HREF="/Reports/CURRENT/Market_Notice/NEMITWEB1_MKTNOTICE_20261010.R145544">y</A>
            <A HREF="/Reports/CURRENT/Market_Notice/NEMITWEB1_MKTNOTICE_bad.R1">z</A>"#;
        let f = notice_files(listing);
        assert_eq!(f.len(), 2);
        assert_eq!(f[1].0, "NEMITWEB1_MKTNOTICE_20261012.R145591");
        assert_eq!(f[1].1, NaiveDate::from_ymd_opt(2026, 10, 12).unwrap());
    }

    #[test]
    fn window_covers_two_hours_before_and_one_after() {
        let ev: Event = serde_json::from_value(serde_json::json!({
            "id": "e", "region": "NSW1", "kind": "high_price", "status": "closed",
            "severity": "cap", "trading_day": "2026-10-12", "start": "2026-10-12T14:20:00+10:00",
            "last_active": "2026-10-12T15:35:00+10:00", "end": "2026-10-12T15:35:00+10:00",
            "peak_price": 23107.0, "peak_interval": "2026-10-12T14:20:00+10:00", "intervals_active": 16,
            "intervals_above_high": 11, "intervals_above_extreme": 5, "mean_price": 8966.0, "min_price": 67.0,
            "flags": [], "constraints": [], "interconnectors_at_limit": [], "demand_start": null,
            "demand_peak": null, "data_tier": "live", "updated_at": "x"
        }))
        .unwrap();
        let w = Window::for_event(&ev).unwrap();
        assert_eq!(w.start, t("2026/10/12 12:20:00"));
        assert_eq!(w.end, t("2026/10/12 16:35:00"));
        assert_eq!(w.intervals(), 52);
        let d = NaiveDate::from_ymd_opt(2026, 10, 12).unwrap();
        assert_eq!(w.trading_periods(), [(d, 100, 151)]);
    }

    #[test]
    fn windows_across_04_00_touch_two_trading_days() {
        let w = Window {
            start: t("2026/10/13 03:00:00"),
            end: t("2026/10/13 05:00:00"),
        };
        let d12 = NaiveDate::from_ymd_opt(2026, 10, 12).unwrap();
        let d13 = NaiveDate::from_ymd_opt(2026, 10, 13).unwrap();
        assert_eq!(
            w.trading_periods(),
            [(d12, 276, PERIODS_PER_DAY), (d13, 1, 12)]
        );
    }

    #[test]
    fn column_types_are_inferred_and_headers_unioned() {
        let mut buf = TableBuf::default();
        buf.add(&Table {
            columns: vec![
                "SETTLEMENTDATE".into(),
                "RUNNO".into(),
                "X".into(),
                "S".into(),
            ],
            rows: vec![
                vec![
                    "2026/10/12 14:35:00".into(),
                    "1".into(),
                    "1.5".into(),
                    "NSW1".into(),
                ],
                vec![
                    "2026/10/12 14:40:00".into(),
                    "".into(),
                    "2".into(),
                    "QLD1".into(),
                ],
            ],
        });
        buf.add(&Table {
            columns: vec!["S".into(), "NEW".into()],
            rows: vec![vec!["SA1".into(), "7".into()]],
        });
        let b = buf.to_batch().unwrap();
        let types: Vec<(String, DataType)> = b
            .schema()
            .fields()
            .iter()
            .map(|f| (f.name().clone(), f.data_type().clone()))
            .collect();
        let expected = [
            (
                "SETTLEMENTDATE",
                DataType::Timestamp(TimeUnit::Millisecond, None),
            ),
            ("RUNNO", DataType::Int64),
            ("X", DataType::Float64),
            ("S", DataType::Utf8),
            // Whole values in an ordinary column still get the stable float type.
            ("NEW", DataType::Float64),
        ]
        .map(|(n, d)| (n.to_string(), d));
        assert_eq!(types, expected);
        assert_eq!(b.num_rows(), 3);
        let n = column::<Int64Array>(&b, "RUNNO").unwrap();
        assert!(n.is_null(1) && n.is_null(2) && n.value(0) == 1);
    }

    fn test_event() -> Event {
        serde_json::from_value(serde_json::json!({
            "id": "e1", "region": "NSW1", "kind": "high_price", "status": "closed",
            "severity": "cap", "trading_day": "2026-10-12", "start": "2026-10-12T14:20:00+10:00",
            "last_active": "2026-10-12T15:35:00+10:00", "end": "2026-10-12T15:35:00+10:00",
            "peak_price": 23107.0, "peak_interval": "2026-10-12T14:20:00+10:00", "intervals_active": 11,
            "intervals_above_high": 11, "intervals_above_extreme": 5, "mean_price": 8966.0, "min_price": 67.0,
            "flags": [], "constraints": [], "interconnectors_at_limit": [], "demand_start": null,
            "demand_peak": null, "data_tier": "live", "updated_at": "x"
        }))
        .unwrap()
    }

    #[test]
    fn empty_raw_buffer_writes_a_manifest_without_tables() {
        let dir = std::env::temp_dir().join(format!("nem-bundle-empty-{}", std::process::id()));
        let ev = test_event();
        let m = write_dispatch(&dir, &ev, Window::for_event(&ev).unwrap(), &[]).unwrap();
        assert!(m.outputs.is_empty());
        assert_eq!(m.intervals_found, 0);
        assert!(m.notes.contains("0 of 52"));
        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn add_static_matches_versions_by_archive_month() {
        let root = std::env::temp_dir().join(format!("nem-bundle-static-{}", std::process::id()));
        let dir = root.join("bundle");
        fs::create_dir_all(&dir).unwrap();
        let ev = test_event();
        base_manifest(&ev, Window::for_event(&ev).unwrap())
            .save(&dir)
            .unwrap();
        // Invoked: A (effective 1 Oct, so in the September archive too), B (in
        // September), C (no archive month available).
        let mut dc = TableBuf::default();
        dc.add(&Table {
            columns: [
                "SETTLEMENTDATE",
                "CONSTRAINTID",
                "GENCONID_EFFECTIVEDATE",
                "GENCONID_VERSIONNO",
            ]
            .map(String::from)
            .to_vec(),
            rows: [
                ["2026/10/12 14:20:00", "A", "2026/10/01 00:00:00", "2"],
                ["2026/10/12 14:25:00", "A", "2026/10/01 00:00:00", "2"],
                ["2026/10/12 14:20:00", "B", "2026/09/15 00:00:00", "1"],
                ["2026/10/12 14:20:00", "C", "2025/01/10 00:00:00", "3"],
            ]
            .map(|r| r.map(String::from).to_vec())
            .to_vec(),
        });
        dc.write_parquet(&dir.join("dispatch_constraint.parquet"))
            .unwrap();

        let mmsdm = root.join("mmsdm");
        let write = |month: &str, folder: &str, body: &str| {
            let d = mmsdm.join(month).join(folder);
            fs::create_dir_all(&d).unwrap();
            fs::write(
                d.join(format!("PUBLIC_ARCHIVE#{folder}#FILE01#{month}.CSV")),
                body,
            )
            .unwrap();
        };
        let gencon = |rows: &str| {
            format!("I,GENCONDATA,NULL,7,EFFECTIVEDATE,VERSIONNO,GENCONID,CONSTRAINTTYPE,CONSTRAINTVALUE\n{rows}")
        };
        // A appears in both September and October; A v1 is not the invoked version.
        write(
            "2026_09",
            "GENCONDATA",
            &gencon(
                "D,GENCONDATA,NULL,7,\"2026/10/01 00:00:00\",2,A,<=,100\n\
             D,GENCONDATA,NULL,7,\"2026/10/01 00:00:00\",1,A,<=,90\n\
             D,GENCONDATA,NULL,7,\"2026/09/15 00:00:00\",1,B,>=,5\n",
            ),
        );
        write(
            "2026_10",
            "GENCONDATA",
            &gencon("D,GENCONDATA,NULL,7,\"2026/10/01 00:00:00\",2,A,<=,100\n"),
        );
        write(
            "2026_09",
            "SPDCONNECTIONPOINTCONSTRAINT",
            "I,SPDCPC,NULL,2,CONNECTIONPOINTID,EFFECTIVEDATE,VERSIONNO,GENCONID,FACTOR,BIDTYPE\n\
             D,SPDCPC,NULL,2,NBAY1,\"2026/10/01 00:00:00\",2,A,1,ENERGY\n\
             D,SPDCPC,NULL,2,NBAY2,\"2026/10/01 00:00:00\",2,A,0.5,ENERGY\n",
        );
        write(
            "2026_10",
            "SPDCONNECTIONPOINTCONSTRAINT",
            "I,SPDCPC,NULL,2,CONNECTIONPOINTID,EFFECTIVEDATE,VERSIONNO,GENCONID,FACTOR,BIDTYPE\n\
             D,SPDCPC,NULL,2,NBAY1,\"2026/10/01 00:00:00\",2,A,1,ENERGY\n",
        );
        // C's invoked version is not archived, but an older version is, in
        // the pre-2016 form (empty sub-table). D was never invoked.
        write(
            "2012_07",
            "GENCONDATA",
            "I,GENCONDATA,,5,EFFECTIVEDATE,VERSIONNO,GENCONID,CONSTRAINTTYPE,CONSTRAINTVALUE\n\
             D,GENCONDATA,,5,\"2012/07/02 00:00:00\",2,C,<=,50\n\
             D,GENCONDATA,,5,\"2012/07/02 00:00:00\",1,D,<=,10\n",
        );
        write(
            "2012_07",
            "SPDCONNECTIONPOINTCONSTRAINT",
            "I,SPDCPC,,2,CONNECTIONPOINTID,EFFECTIVEDATE,VERSIONNO,GENCONID,FACTOR,BIDTYPE\n\
             D,SPDCPC,,2,NBAY1,\"2012/07/02 00:00:00\",2,C,1,ENERGY\n",
        );
        // A full history each month; the version effective after the window
        // is left out.
        write(
            "2026_09",
            "INTERCONNECTORCONSTRAINT",
            "I,MARKET_CONFIG,INTERCONNECTORCONSTRAINT,1,EFFECTIVEDATE,VERSIONNO,INTERCONNECTORID,IMPORTLIMIT,EXPORTLIMIT\n\
             D,MARKET_CONFIG,INTERCONNECTORCONSTRAINT,1,\"1998/08/28 00:00:00\",1,V-SN,2124,1724\n\
             D,MARKET_CONFIG,INTERCONNECTORCONSTRAINT,1,\"2026/11/01 00:00:00\",1,V-SN,2200,1800\n",
        );
        // After the window: not searched.
        write(
            "2026_11",
            "GENCONDATA",
            &gencon("D,GENCONDATA,NULL,7,\"2026/11/02 00:00:00\",3,A,<=,100\n"),
        );
        write(
            "2026_09",
            "DUDETAILSUMMARY",
            "I,PARTICIPANT_REGISTRATION,DUDETAILSUMMARY,7,DUID,START_DATE,END_DATE,REGIONID,TRANSMISSIONLOSSFACTOR\n\
             D,PARTICIPANT_REGISTRATION,DUDETAILSUMMARY,7,BW01,\"2020/01/01 00:00:00\",\"2999/12/31 00:00:00\",NSW1,0.98\n\
             D,PARTICIPANT_REGISTRATION,DUDETAILSUMMARY,7,BW01,\"2010/01/01 00:00:00\",\"2020/01/01 00:00:00\",NSW1,0.95\n",
        );

        let m = add_static(&dir, &mmsdm).unwrap();
        let s = m.static_data.clone().unwrap();
        assert_eq!(s.months, ["2012_07", "2026_09", "2026_10"]);
        assert_eq!((s.invoked_versions, s.equations_found), (3, 2));
        assert_eq!(s.missing_equations[0].constraint_id, "C");
        assert_eq!(s.other_versions, 2, "A v1 and C v2");
        assert!(s.ids_without_any_version.is_empty());
        assert_eq!(s.dudetail_month.as_deref(), Some("2026_09"));
        let rows = |f: &str| m.outputs.iter().find(|o| o.file == f).map(|o| o.rows);
        assert_eq!(
            rows("static_gencondata.parquet"),
            Some(4),
            "A v2 once, A v1, B, C v2; not D or November's A v3"
        );
        assert_eq!(
            rows("static_spdconnectionpointconstraint.parquet"),
            Some(3),
            "deduplicated across months, plus the pre-2016 C row"
        );
        assert!(m.sources.iter().any(|s| s.role == "mmsdm:2012_07"));
        assert!(!m.sources.iter().any(|s| s.role == "mmsdm:2026_11"));
        assert_eq!(
            rows("static_dudetailsummary.parquet"),
            Some(1),
            "registration in force only"
        );
        assert!(m.sources.iter().any(|s| s.role == "mmsdm:2026_10"));
        assert_eq!(Manifest::load(&dir).unwrap(), m);
        fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn parquet_round_trips_and_hash_matches_the_file() {
        let dir = std::env::temp_dir().join(format!("nem-bundle-test-{}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        let mut buf = TableBuf::default();
        buf.add(&Table {
            columns: vec![
                "SETTLEMENTDATE".into(),
                "REGIONID".into(),
                "INTERVENTION".into(),
                "RRP".into(),
            ],
            rows: vec![
                vec![
                    "2026/10/12 14:20:00".into(),
                    "NSW1".into(),
                    "0".into(),
                    "23107".into(),
                ],
                vec![
                    "2026/10/12 14:20:00".into(),
                    "NSW1".into(),
                    "1".into(),
                    "99".into(),
                ],
                vec![
                    "2026/10/12 14:25:00".into(),
                    "NSW1".into(),
                    "0".into(),
                    "410.5".into(),
                ],
                vec![
                    "2026/10/12 14:25:00".into(),
                    "QLD1".into(),
                    "0".into(),
                    "150".into(),
                ],
            ],
        });
        let path = dir.join("dispatch_price.parquet");
        let sha = buf.write_parquet(&path).unwrap();
        assert_eq!(sha, sha256_hex(&fs::read(&path).unwrap()));
        let mut rs = TableBuf::default();
        rs.add(&Table {
            columns: vec![
                "SETTLEMENTDATE".into(),
                "REGIONID".into(),
                "TOTALDEMAND".into(),
            ],
            rows: vec![vec![
                "2026/10/12 14:25:00".into(),
                "NSW1".into(),
                "12650.2".into(),
            ]],
        });
        rs.write_parquet(&dir.join("dispatch_regionsum.parquet"))
            .unwrap();
        let s = read_series(&dir, "NSW1").unwrap();
        let got: Vec<(&str, f64, Option<f64>)> =
            s.iter().map(|p| (p.t.as_str(), p.rrp, p.demand)).collect();
        assert_eq!(
            got,
            [
                ("2026-10-12T14:20:00+10:00", 23107.0, None),
                ("2026-10-12T14:25:00+10:00", 410.5, Some(12650.2)),
            ]
        );
        fs::remove_dir_all(&dir).ok();
    }
}

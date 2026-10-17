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
use nem_lens_aemo::report::{parse_report_where, Record, Table};
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

    /// Infer a type per column and build an Arrow batch.
    pub fn to_batch(&self) -> Result<RecordBatch> {
        let mut fields = Vec::new();
        let mut arrays: Vec<ArrayRef> = Vec::new();
        for (i, name) in self.columns.iter().enumerate() {
            let present: Vec<&str> = self.values(i).flatten().collect();
            let (dt, arr): (DataType, ArrayRef) =
                if !present.is_empty() && present.iter().all(|v| v.parse::<i64>().is_ok()) {
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
        let buf = bufs.remove(key).unwrap_or_default();
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
            columns: vec!["SETTLEMENTDATE".into(), "N".into(), "X".into(), "S".into()],
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
            ("N", DataType::Int64),
            ("X", DataType::Float64),
            ("S", DataType::Utf8),
            ("NEW", DataType::Int64),
        ]
        .map(|(n, d)| (n.to_string(), d));
        assert_eq!(types, expected);
        assert_eq!(b.num_rows(), 3);
        let n = column::<Int64Array>(&b, "N").unwrap();
        assert!(n.is_null(1) && n.is_null(2) && n.value(0) == 1);
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

//! SQLite persistence. One file, WAL mode; every operation is short.

use anyhow::Result;
use rusqlite::{params, Connection, OptionalExtension};

use crate::model::{DispatchSnapshot, Event, NotificationItem};

pub fn open(path: &str) -> Result<Connection> {
    let conn = Connection::open(path)?;
    conn.pragma_update(None, "journal_mode", "WAL")?;
    conn.pragma_update(None, "synchronous", "NORMAL")?;
    conn.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS seen_files (
            name TEXT PRIMARY KEY, kind TEXT NOT NULL, processed_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS region_intervals (
            settlement TEXT NOT NULL, region TEXT NOT NULL, rrp REAL NOT NULL,
            demand REAL, available_gen REAL, net_interchange REAL,
            PRIMARY KEY (settlement, region));
        CREATE TABLE IF NOT EXISTS events (
            id TEXT PRIMARY KEY, region TEXT NOT NULL, kind TEXT NOT NULL, status TEXT NOT NULL,
            severity TEXT NOT NULL, trading_day TEXT NOT NULL, start TEXT NOT NULL, end_time TEXT,
            peak REAL NOT NULL, data_tier TEXT NOT NULL, updated_at TEXT NOT NULL, json TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS events_start ON events (start);
        CREATE TABLE IF NOT EXISTS notifications (
            seq INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL, transition TEXT NOT NULL,
            public INTEGER NOT NULL, created_at TEXT NOT NULL, json TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS detector_state (id INTEGER PRIMARY KEY CHECK (id = 1), json TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS data_ready (
            source TEXT NOT NULL, trading_day TEXT NOT NULL, filename TEXT NOT NULL, seen_at TEXT NOT NULL,
            PRIMARY KEY (source, trading_day));
        CREATE TABLE IF NOT EXISTS webhooks (
            id TEXT PRIMARY KEY, url TEXT NOT NULL, secret TEXT NOT NULL, manage_hash TEXT NOT NULL,
            regions TEXT, min_severity TEXT NOT NULL, status TEXT NOT NULL, created_at TEXT NOT NULL,
            created_ip TEXT, failures INTEGER NOT NULL DEFAULT 0, last_ok TEXT);
        CREATE TABLE IF NOT EXISTS trigger_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT, trigger_name TEXT NOT NULL, event_id TEXT NOT NULL,
            transition TEXT NOT NULL, started_at TEXT NOT NULL, finished_at TEXT, exit_code INTEGER,
            output_tail TEXT);
        "#,
    )?;
    Ok(conn)
}

fn now() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true)
}

pub fn is_seen(c: &Connection, name: &str) -> Result<bool> {
    Ok(
        c.query_row("SELECT 1 FROM seen_files WHERE name = ?1", [name], |_| {
            Ok(())
        })
        .optional()?
        .is_some(),
    )
}

pub fn mark_seen(c: &Connection, name: &str, kind: &str) -> Result<()> {
    c.execute(
        "INSERT OR IGNORE INTO seen_files (name, kind, processed_at) VALUES (?1, ?2, ?3)",
        params![name, kind, now()],
    )?;
    Ok(())
}

pub fn save_snapshot(c: &Connection, s: &DispatchSnapshot) -> Result<()> {
    let t = crate::time::iso_market(s.settlement);
    for (region, r) in &s.regions {
        c.execute(
            "INSERT OR REPLACE INTO region_intervals VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                t,
                region,
                r.rrp,
                r.demand,
                r.available_gen,
                r.net_interchange
            ],
        )?;
    }
    Ok(())
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct SeriesPoint {
    pub t: String,
    pub rrp: f64,
    pub demand: Option<f64>,
}

pub fn series(c: &Connection, region: &str, from: &str, to: &str) -> Result<Vec<SeriesPoint>> {
    let mut st = c.prepare(
        "SELECT settlement, rrp, demand FROM region_intervals
         WHERE region = ?1 AND settlement >= ?2 AND settlement <= ?3 ORDER BY settlement",
    )?;
    let rows = st.query_map(params![region, from, to], |r| {
        Ok(SeriesPoint {
            t: r.get(0)?,
            rrp: r.get(1)?,
            demand: r.get(2)?,
        })
    })?;
    Ok(rows.collect::<std::result::Result<_, _>>()?)
}

pub fn recent_series(c: &Connection, region: &str, n: usize) -> Result<Vec<SeriesPoint>> {
    let mut st = c.prepare(
        "SELECT settlement, rrp, demand FROM region_intervals WHERE region = ?1
         ORDER BY settlement DESC LIMIT ?2",
    )?;
    let rows = st.query_map(params![region, n as i64], |r| {
        Ok(SeriesPoint {
            t: r.get(0)?,
            rrp: r.get(1)?,
            demand: r.get(2)?,
        })
    })?;
    let mut v: Vec<SeriesPoint> = rows.collect::<std::result::Result<_, _>>()?;
    v.reverse();
    Ok(v)
}

pub fn latest_settlement(c: &Connection) -> Result<Option<String>> {
    Ok(
        c.query_row("SELECT MAX(settlement) FROM region_intervals", [], |r| {
            r.get(0)
        })?,
    )
}

pub fn upsert_event(c: &Connection, e: &Event) -> Result<()> {
    c.execute(
        "INSERT OR REPLACE INTO events
         (id, region, kind, status, severity, trading_day, start, end_time, peak, data_tier, updated_at, json)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
        params![
            e.id,
            e.region,
            serde_json::to_value(e.kind)?.as_str().unwrap_or_default(),
            e.status.as_str(),
            e.severity.as_str(),
            e.trading_day,
            e.start,
            e.end,
            e.peak_price,
            e.data_tier,
            e.updated_at,
            serde_json::to_string(e)?
        ],
    )?;
    Ok(())
}

pub fn get_event(c: &Connection, id: &str) -> Result<Option<Event>> {
    let json: Option<String> = c
        .query_row("SELECT json FROM events WHERE id = ?1", [id], |r| r.get(0))
        .optional()?;
    Ok(match json {
        Some(j) => Some(serde_json::from_str(&j)?),
        None => None,
    })
}

pub struct EventFilter<'a> {
    pub region: Option<&'a str>,
    pub status: Option<&'a str>,
    pub include_discarded: bool,
    pub limit: usize,
}

pub fn list_events(c: &Connection, f: &EventFilter) -> Result<Vec<Event>> {
    let mut sql = String::from("SELECT json FROM events WHERE 1=1");
    let mut args: Vec<String> = Vec::new();
    if let Some(r) = f.region {
        args.push(r.to_string());
        sql.push_str(&format!(" AND region = ?{}", args.len()));
    }
    if let Some(s) = f.status {
        args.push(s.to_string());
        sql.push_str(&format!(" AND status = ?{}", args.len()));
    }
    if !f.include_discarded {
        sql.push_str(" AND status != 'discarded'");
    }
    sql.push_str(&format!(" ORDER BY start DESC LIMIT {}", f.limit.min(500)));
    let mut st = c.prepare(&sql)?;
    let rows = st.query_map(rusqlite::params_from_iter(args.iter()), |r| {
        r.get::<_, String>(0)
    })?;
    let mut out = Vec::new();
    for j in rows {
        out.push(serde_json::from_str(&j?)?);
    }
    Ok(out)
}

/// Confirmed or closed events still on the live tier whose trading day is on or before `day`.
pub fn events_awaiting_next_day(c: &Connection, day: &str) -> Result<Vec<Event>> {
    let mut st = c.prepare(
        "SELECT json FROM events WHERE data_tier = 'live' AND status IN ('confirmed', 'closed')
         AND trading_day <= ?1",
    )?;
    let rows = st.query_map([day], |r| r.get::<_, String>(0))?;
    let mut out = Vec::new();
    for j in rows {
        out.push(serde_json::from_str(&j?)?);
    }
    Ok(out)
}

pub fn add_notification(c: &Connection, item: &NotificationItem) -> Result<i64> {
    c.execute(
        "INSERT INTO notifications (event_id, transition, public, created_at, json) VALUES (?1, ?2, ?3, ?4, '')",
        params![item.event_id, item.transition, item.public as i64, item.created_at],
    )?;
    let seq = c.last_insert_rowid();
    let mut stored = item.clone();
    stored.seq = seq;
    c.execute(
        "UPDATE notifications SET json = ?1 WHERE seq = ?2",
        params![serde_json::to_string(&stored)?, seq],
    )?;
    Ok(seq)
}

pub fn list_notifications(
    c: &Connection,
    public_only: bool,
    since: i64,
    limit: usize,
) -> Result<Vec<NotificationItem>> {
    let mut st = c.prepare(
        "SELECT json FROM notifications WHERE (?1 = 0 OR public = 1) AND seq > ?2 ORDER BY seq DESC LIMIT ?3",
    )?;
    let rows = st.query_map(
        params![public_only as i64, since, limit.min(500) as i64],
        |r| r.get::<_, String>(0),
    )?;
    let mut out = Vec::new();
    for j in rows {
        out.push(serde_json::from_str(&j?)?);
    }
    Ok(out)
}

pub fn save_state(c: &Connection, json: &str) -> Result<()> {
    c.execute(
        "INSERT OR REPLACE INTO detector_state (id, json) VALUES (1, ?1)",
        [json],
    )?;
    Ok(())
}

pub fn load_state(c: &Connection) -> Result<Option<String>> {
    Ok(
        c.query_row("SELECT json FROM detector_state WHERE id = 1", [], |r| {
            r.get(0)
        })
        .optional()?,
    )
}

/// Record a daily file; returns true when it is new.
pub fn record_data_ready(c: &Connection, source: &str, day: &str, filename: &str) -> Result<bool> {
    let n = c.execute(
        "INSERT OR IGNORE INTO data_ready (source, trading_day, filename, seen_at) VALUES (?1, ?2, ?3, ?4)",
        params![source, day, filename, now()],
    )?;
    Ok(n > 0)
}

/// Latest trading day for which every listed source has a file.
pub fn latest_complete_day(c: &Connection, sources: &[String]) -> Result<Option<String>> {
    if sources.is_empty() {
        return Ok(None);
    }
    let placeholders = (1..=sources.len())
        .map(|i| format!("?{i}"))
        .collect::<Vec<_>>()
        .join(",");
    let sql = format!(
        "SELECT trading_day FROM data_ready WHERE source IN ({placeholders})
         GROUP BY trading_day HAVING COUNT(DISTINCT source) = {} ORDER BY trading_day DESC LIMIT 1",
        sources.len()
    );
    Ok(
        c.query_row(&sql, rusqlite::params_from_iter(sources.iter()), |r| {
            r.get(0)
        })
        .optional()?,
    )
}

pub fn prune(c: &Connection, retain_days: i64) -> Result<()> {
    let cutoff =
        crate::time::iso_market(crate::time::now_market() - chrono::Duration::days(retain_days));
    c.execute(
        "DELETE FROM region_intervals WHERE settlement < ?1",
        [&cutoff],
    )?;
    c.execute("DELETE FROM seen_files WHERE processed_at < ?1", [&cutoff])?;
    Ok(())
}

// ---- webhooks ----

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct Webhook {
    pub id: String,
    pub url: String,
    pub secret: String,
    pub regions: Option<Vec<String>>,
    pub min_severity: String,
    pub failures: i64,
}

pub fn count_webhooks(c: &Connection) -> Result<usize> {
    Ok(c.query_row(
        "SELECT COUNT(*) FROM webhooks WHERE status = 'active'",
        [],
        |r| r.get::<_, i64>(0),
    )? as usize)
}

pub fn insert_webhook(
    c: &Connection,
    w: &Webhook,
    manage_hash: &str,
    created_ip: &str,
) -> Result<()> {
    c.execute(
        "INSERT INTO webhooks (id, url, secret, manage_hash, regions, min_severity, status, created_at, created_ip)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'active', ?7, ?8)",
        params![
            w.id,
            w.url,
            w.secret,
            manage_hash,
            w.regions.as_ref().map(|r| r.join(",")),
            w.min_severity,
            now(),
            created_ip
        ],
    )?;
    Ok(())
}

pub fn active_webhooks(c: &Connection) -> Result<Vec<Webhook>> {
    let mut st = c.prepare(
        "SELECT id, url, secret, regions, min_severity, failures FROM webhooks WHERE status = 'active'",
    )?;
    let rows = st.query_map([], |r| {
        let regions: Option<String> = r.get(3)?;
        Ok(Webhook {
            id: r.get(0)?,
            url: r.get(1)?,
            secret: r.get(2)?,
            regions: regions.map(|s| s.split(',').map(|x| x.to_string()).collect()),
            min_severity: r.get(4)?,
            failures: r.get(5)?,
        })
    })?;
    Ok(rows.collect::<std::result::Result<_, _>>()?)
}

pub fn get_webhook_manage_hash(c: &Connection, id: &str) -> Result<Option<(String, String)>> {
    Ok(c.query_row(
        "SELECT manage_hash, status FROM webhooks WHERE id = ?1",
        [id],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )
    .optional()?)
}

pub fn get_webhook(c: &Connection, id: &str) -> Result<Option<Webhook>> {
    Ok(active_webhooks(c)?.into_iter().find(|w| w.id == id))
}

pub fn delete_webhook(c: &Connection, id: &str) -> Result<()> {
    c.execute("DELETE FROM webhooks WHERE id = ?1", [id])?;
    Ok(())
}

/// Record a delivery result; returns the new consecutive failure count.
pub fn webhook_result(c: &Connection, id: &str, ok: bool, disable_after: i64) -> Result<i64> {
    if ok {
        c.execute(
            "UPDATE webhooks SET failures = 0, last_ok = ?1 WHERE id = ?2",
            params![now(), id],
        )?;
        return Ok(0);
    }
    c.execute(
        "UPDATE webhooks SET failures = failures + 1 WHERE id = ?1",
        [id],
    )?;
    let f: i64 = c
        .query_row("SELECT failures FROM webhooks WHERE id = ?1", [id], |r| {
            r.get(0)
        })
        .unwrap_or(0);
    if f >= disable_after {
        c.execute(
            "UPDATE webhooks SET status = 'disabled' WHERE id = ?1",
            [id],
        )?;
    }
    Ok(f)
}

// ---- trigger runs ----

pub fn start_run(c: &Connection, trigger: &str, event_id: &str, transition: &str) -> Result<i64> {
    c.execute(
        "INSERT INTO trigger_runs (trigger_name, event_id, transition, started_at) VALUES (?1, ?2, ?3, ?4)",
        params![trigger, event_id, transition, now()],
    )?;
    Ok(c.last_insert_rowid())
}

pub fn finish_run(c: &Connection, id: i64, exit_code: Option<i32>, tail: &str) -> Result<()> {
    c.execute(
        "UPDATE trigger_runs SET finished_at = ?1, exit_code = ?2, output_tail = ?3 WHERE id = ?4",
        params![now(), exit_code, tail, id],
    )?;
    Ok(())
}

#[derive(Debug, serde::Serialize)]
pub struct RunRow {
    pub id: i64,
    pub trigger: String,
    pub event_id: String,
    pub transition: String,
    pub started_at: String,
    pub finished_at: Option<String>,
    pub exit_code: Option<i32>,
    pub output_tail: Option<String>,
}

pub fn list_runs(c: &Connection, limit: usize) -> Result<Vec<RunRow>> {
    let mut st = c.prepare(
        "SELECT id, trigger_name, event_id, transition, started_at, finished_at, exit_code, output_tail
         FROM trigger_runs ORDER BY id DESC LIMIT ?1",
    )?;
    let rows = st.query_map([limit as i64], |r| {
        Ok(RunRow {
            id: r.get(0)?,
            trigger: r.get(1)?,
            event_id: r.get(2)?,
            transition: r.get(3)?,
            started_at: r.get(4)?,
            finished_at: r.get(5)?,
            exit_code: r.get(6)?,
            output_tail: r.get(7)?,
        })
    })?;
    Ok(rows.collect::<std::result::Result<_, _>>()?)
}

pub fn notifications_for_event(
    c: &Connection,
    event_id: &str,
    public_only: bool,
) -> Result<Vec<NotificationItem>> {
    let mut st = c.prepare(
        "SELECT json FROM notifications WHERE event_id = ?1 AND (?2 = 0 OR public = 1) ORDER BY seq",
    )?;
    let rows = st.query_map(params![event_id, public_only as i64], |r| {
        r.get::<_, String>(0)
    })?;
    let mut out = Vec::new();
    for j in rows {
        out.push(serde_json::from_str(&j?)?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::RegionInterval;

    fn db() -> Connection {
        open(":memory:").unwrap()
    }

    #[test]
    fn data_ready_needs_every_source_and_is_idempotent() {
        let c = db();
        let sources = vec!["A".to_string(), "B".to_string()];
        assert!(record_data_ready(&c, "A", "2026-10-12", "a12.zip").unwrap());
        assert!(!record_data_ready(&c, "A", "2026-10-12", "a12.zip").unwrap());
        record_data_ready(&c, "A", "2026-10-13", "a13.zip").unwrap();
        assert_eq!(latest_complete_day(&c, &sources).unwrap(), None);
        record_data_ready(&c, "B", "2026-10-12", "b12.zip").unwrap();
        assert_eq!(
            latest_complete_day(&c, &sources).unwrap().as_deref(),
            Some("2026-10-12")
        );
        assert_eq!(latest_complete_day(&c, &[]).unwrap(), None);
    }

    #[test]
    fn webhooks_are_disabled_after_repeated_failures() {
        let c = db();
        let hook = Webhook {
            id: "wh_1".into(),
            url: "https://example.org/h".into(),
            secret: "s".into(),
            regions: Some(vec!["NSW1".into(), "QLD1".into()]),
            min_severity: "high".into(),
            failures: 0,
        };
        insert_webhook(&c, &hook, "hash", "203.0.113.1").unwrap();
        assert_eq!(active_webhooks(&c).unwrap()[0].regions, hook.regions);
        assert_eq!(webhook_result(&c, "wh_1", false, 3).unwrap(), 1);
        assert_eq!(webhook_result(&c, "wh_1", true, 3).unwrap(), 0);
        for _ in 0..3 {
            webhook_result(&c, "wh_1", false, 3).unwrap();
        }
        assert!(active_webhooks(&c).unwrap().is_empty());
        assert_eq!(count_webhooks(&c).unwrap(), 0);
        assert_eq!(
            get_webhook_manage_hash(&c, "wh_1").unwrap(),
            Some(("hash".into(), "disabled".into()))
        );
    }

    #[test]
    fn prune_keeps_recent_intervals_only() {
        let c = db();
        let now = crate::time::now_market();
        for t in [now - chrono::Duration::days(100), now] {
            let mut s = DispatchSnapshot {
                settlement: t,
                ..Default::default()
            };
            s.regions.insert(
                "NSW1".into(),
                RegionInterval {
                    rrp: 1.0,
                    ..Default::default()
                },
            );
            save_snapshot(&c, &s).unwrap();
        }
        prune(&c, 90).unwrap();
        let left: i64 = c
            .query_row("SELECT COUNT(*) FROM region_intervals", [], |r| r.get(0))
            .unwrap();
        assert_eq!(left, 1);
        assert_eq!(
            latest_settlement(&c).unwrap(),
            Some(crate::time::iso_market(now))
        );
    }

    #[test]
    fn notifications_keep_order_and_visibility() {
        let c = db();
        let ev: Event = serde_json::from_value(serde_json::json!({
            "id": "e1", "region": "NSW1", "kind": "high_price", "status": "confirmed",
            "severity": "high", "trading_day": "2026-10-12", "start": "2026-10-12T14:10:00+10:00",
            "last_active": "2026-10-12T14:10:00+10:00", "end": null, "peak_price": 6000.0,
            "peak_interval": "2026-10-12T14:10:00+10:00", "intervals_active": 1,
            "intervals_above_high": 1, "intervals_above_extreme": 0, "mean_price": 6000.0,
            "min_price": 6000.0, "flags": [], "constraints": [], "interconnectors_at_limit": [],
            "demand_start": null, "demand_peak": null, "data_tier": "live", "updated_at": "x"
        }))
        .unwrap();
        for (transition, public) in [("opened", false), ("confirmed", true), ("closed", true)] {
            add_notification(
                &c,
                &NotificationItem {
                    seq: 0,
                    event_id: "e1".into(),
                    transition: transition.into(),
                    title: transition.into(),
                    summary: String::new(),
                    url: String::new(),
                    created_at: "2026-10-12T04:10:00Z".into(),
                    public,
                    event: ev.clone(),
                },
            )
            .unwrap();
        }
        let all: Vec<(i64, String)> = notifications_for_event(&c, "e1", false)
            .unwrap()
            .into_iter()
            .map(|n| (n.seq, n.transition))
            .collect();
        assert_eq!(
            all,
            [
                (1, "opened".to_string()),
                (2, "confirmed".to_string()),
                (3, "closed".to_string())
            ]
        );
        let public = list_notifications(&c, true, 0, 50).unwrap();
        assert_eq!(public.len(), 2);
        assert_eq!(public[0].transition, "closed", "newest first");
        assert_eq!(list_notifications(&c, true, 2, 50).unwrap().len(), 1);
    }
}

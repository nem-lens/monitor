//! Ingest loop: polls NEMWeb for new DispatchIS files and next-day files,
//! runs the detector and publishes transitions. Also drives offline replay,
//! the raw buffer and event bundles.

use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{NaiveDateTime, Utc};
use nem_lens_aemo::{dispatch_snapshot, parse_report};
use tokio::sync::Mutex as AsyncMutex;

use crate::app::{App, StreamMsg};
use crate::bundle::{self, NextDayFile, RawFile, Window};
use crate::detect::Detector;
use crate::model::{DispatchSnapshot, Transition, TransitionKind};
use crate::nemweb;
use crate::notify::publish;
use crate::store;
use crate::time::{iso_market, now_market, parse_market};

pub struct Engine {
    pub detector: Detector,
}

impl Engine {
    pub fn load(app: &App) -> Result<Self> {
        let state = app.with_db(store::load_state)?;
        let detector = match state {
            Some(json) => Detector::restore(app.cfg.detect.clone(), &json).unwrap_or_else(|e| {
                tracing::warn!("detector state unreadable ({e}); starting fresh");
                Detector::new(app.cfg.detect.clone())
            }),
            None => Detector::new(app.cfg.detect.clone()),
        };
        Ok(Self { detector })
    }
}

/// Process one AEMO CSV report (DispatchIS).
pub async fn process_report(
    app: &App,
    engine: &AsyncMutex<Engine>,
    name: &str,
    csv: &[u8],
) -> Result<()> {
    let tables = parse_report(csv).with_context(|| format!("parsing {name}"))?;
    let snap: DispatchSnapshot =
        dispatch_snapshot(&tables).with_context(|| format!("reading {name}"))?;
    app.with_db(|c| store::save_snapshot(c, &snap))?;

    let transitions: Vec<Transition> = {
        let mut eng = engine.lock().await;
        let ts = eng.detector.ingest(&snap);
        let state = eng.detector.state_json();
        app.with_db(|c| {
            for tr in &ts {
                store::upsert_event(c, &tr.event)?;
            }
            // Keep open events current in the store for the API.
            for ev in eng.detector.open_events() {
                store::upsert_event(c, &ev)?;
            }
            store::save_state(c, &state)
        })?;
        ts
    };
    if let Err(e) = queue_bundles(app, &transitions) {
        tracing::warn!("queueing bundle: {e:#}");
    }
    for tr in transitions {
        publish(app, tr).await;
    }
    if let Err(e) = write_due_bundles(app, snap.settlement) {
        tracing::warn!("writing bundle: {e:#}");
    }

    let interval = iso_market(snap.settlement);
    let _ = app.tx.send(StreamMsg {
        kind: "interval",
        data: serde_json::json!({
            "settlement": interval,
            "regions": snap.regions,
        }),
    });
    if let Ok(mut h) = app.health.write() {
        h.last_interval = Some(interval);
        h.last_file = Some(name.to_string());
        h.files_processed += 1;
    }
    Ok(())
}

/// Replay a directory of DispatchIS zips or CSVs in filename order.
pub async fn replay_dir(app: &App, dir: &Path, delay: Duration) -> Result<usize> {
    let engine = AsyncMutex::new(Engine::load(app)?);
    let mut entries: Vec<_> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            let n = p.to_string_lossy().to_ascii_lowercase();
            n.ends_with(".zip") || n.ends_with(".csv")
        })
        .collect();
    entries.sort();
    let mut n = 0;
    for path in entries {
        let name = path
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default();
        let bytes = std::fs::read(&path)?;
        let reports = if name.to_ascii_lowercase().ends_with(".zip") {
            buffer_raw(app, &name, &bytes)?;
            nemweb::unzip_csvs(&bytes)?
        } else {
            vec![(name.clone(), bytes)]
        };
        for (member, csv) in reports {
            process_report(app, &engine, &member, &csv).await?;
            n += 1;
            if !delay.is_zero() {
                tokio::time::sleep(delay).await;
            }
        }
        app.with_db(|c| store::mark_seen(c, &name, "replay"))?;
    }
    // No more data is coming: cut any bundle whose window ran past the end.
    write_due_bundles(app, NaiveDateTime::MAX)?;
    Ok(n)
}

/// Keep a DispatchIS zip in the rolling raw buffer that bundles are cut from.
fn buffer_raw(app: &App, name: &str, bytes: &[u8]) -> Result<()> {
    if nemweb::dispatch_file_interval(name).is_none() {
        return Ok(());
    }
    let dir = Path::new(&app.cfg.store.raw_dir);
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    let path = dir.join(name);
    if !path.exists() {
        std::fs::write(&path, bytes).with_context(|| format!("writing {}", path.display()))?;
    }
    Ok(())
}

/// Queue a bundle for each event that closed (discarded events keep nothing).
fn queue_bundles(app: &App, ts: &[Transition]) -> Result<()> {
    for tr in ts.iter().filter(|t| t.kind == TransitionKind::Closed) {
        let Some(w) = Window::for_event(&tr.event) else {
            continue;
        };
        let row = store::BundleRow {
            event_id: tr.event.id.clone(),
            status: "pending".into(),
            window_start: iso_market(w.start),
            window_end: iso_market(w.end),
            path: bundle::dir_for(&app.cfg.store.bundle_dir, &tr.event.id)
                .to_string_lossy()
                .into(),
        };
        app.with_db(|c| store::queue_bundle(c, &row))?;
    }
    Ok(())
}

fn window_of(b: &store::BundleRow) -> Option<Window> {
    Some(Window {
        start: parse_market(&b.window_start)?,
        end: parse_market(&b.window_end)?,
    })
}

/// Write each pending bundle whose window has fully arrived by `latest`.
fn write_due_bundles(app: &App, latest: NaiveDateTime) -> Result<()> {
    for b in app.with_db(|c| store::bundles_with_status(c, "pending"))? {
        let Some(w) = window_of(&b) else { continue };
        if latest < w.end {
            continue;
        }
        let Some(ev) = app.with_db(|c| store::get_event(c, &b.event_id))? else {
            continue;
        };
        let raw = raw_files_in(app, &w)?;
        let m = bundle::write_dispatch(Path::new(&b.path), &ev, w, &raw)?;
        app.with_db(|c| store::set_bundle_status(c, &b.event_id, "written"))?;
        tracing::info!(
            event = %b.event_id,
            intervals = m.intervals_found,
            expected = m.intervals_expected,
            "bundle written to {}",
            b.path
        );
    }
    Ok(())
}

fn raw_names(app: &App) -> Result<Vec<(NaiveDateTime, String)>> {
    let dir = Path::new(&app.cfg.store.raw_dir);
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut out: Vec<(NaiveDateTime, String)> = std::fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            Some((nemweb::dispatch_file_interval(&name)?, name))
        })
        .collect();
    out.sort();
    Ok(out)
}

fn raw_files_in(app: &App, w: &Window) -> Result<Vec<RawFile>> {
    let base = format!(
        "{}/{}",
        app.cfg.source.base_url.trim_end_matches('/'),
        app.cfg.source.dispatch_dir
    );
    let mut out = Vec::new();
    for (t, name) in raw_names(app)? {
        if w.contains(t) {
            let bytes = std::fs::read(Path::new(&app.cfg.store.raw_dir).join(&name))?;
            out.push(RawFile {
                url: format!("{base}/{name}"),
                name,
                bytes,
            });
        }
    }
    Ok(out)
}

/// Drop buffered outcomes and raw zips older than `store.buffer_hours`,
/// keeping raw zips that a pending bundle still needs. Returns files removed.
pub fn prune_buffer(app: &App) -> Result<usize> {
    let hours = app.cfg.store.buffer_hours;
    app.with_db(|c| store::prune(c, hours))?;
    let cutoff = now_market() - chrono::Duration::hours(hours);
    let pending: Vec<Window> = app
        .with_db(|c| store::bundles_with_status(c, "pending"))?
        .iter()
        .filter_map(window_of)
        .collect();
    let mut removed = 0;
    for (t, name) in raw_names(app)? {
        if t < cutoff && !pending.iter().any(|w| w.contains(t)) {
            std::fs::remove_file(Path::new(&app.cfg.store.raw_dir).join(&name))?;
            removed += 1;
        }
    }
    Ok(removed)
}

/// Slice the next-day files for an event's window into its bundle. The full
/// files are read once and not kept.
pub async fn add_next_day_to_bundle(app: &App, event_id: &str) -> Result<()> {
    let Some(b) = app.with_db(|c| store::get_bundle(c, event_id))? else {
        return Ok(());
    };
    if b.status != "written" {
        return Ok(());
    }
    let Some(w) = window_of(&b) else {
        return Ok(());
    };
    let mut files = Vec::new();
    let mut missing = Vec::new();
    for (day, _, _) in w.trading_periods() {
        let day = day.to_string();
        for source in &app.cfg.source.data_ready_dirs {
            match app.with_db(|c| store::data_ready_file(c, source, &day))? {
                Some(name) if name != "manual" => match load_next_day(app, source, &name).await {
                    Ok(f) => files.push(f),
                    Err(e) => {
                        tracing::warn!("next-day file {name}: {e:#}");
                        missing.push(format!("{source} {day}"));
                    }
                },
                _ => missing.push(format!("{source} {day}")),
            }
        }
    }
    let m = bundle::add_next_day(Path::new(&b.path), &files, &missing)?;
    if !files.is_empty() {
        app.with_db(|c| store::set_bundle_status(c, event_id, "next_day"))?;
    }
    tracing::info!(event = %event_id, status = %m.next_day.status, "next-day data for bundle");
    Ok(())
}

/// A recorded next-day file: a local path (from `mark-data-ready --files`)
/// or a name in the NEMWeb directory.
async fn load_next_day(app: &App, source: &str, name: &str) -> Result<NextDayFile> {
    let local = Path::new(name);
    let file = local
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| name.to_string());
    let url = format!(
        "{}/{source}/{file}",
        app.cfg.source.base_url.trim_end_matches('/')
    );
    let bytes = if local.is_absolute() && local.exists() {
        std::fs::read(local)?
    } else {
        let client = nemweb::client(&app.cfg.source.user_agent)?;
        nemweb::fetch_bytes(&client, &url).await?
    };
    Ok(NextDayFile {
        source: source.to_string(),
        name: file,
        url,
        bytes,
    })
}

/// The always-on loop.
pub async fn run(app: App) {
    let engine = match Engine::load(&app) {
        Ok(e) => AsyncMutex::new(e),
        Err(e) => {
            tracing::error!("cannot start ingest: {e:#}");
            return;
        }
    };
    let client = match nemweb::client(&app.cfg.source.user_agent) {
        Ok(c) => c,
        Err(e) => {
            tracing::error!("http client: {e:#}");
            return;
        }
    };
    let mut poll = tokio::time::interval(Duration::from_secs(app.cfg.source.poll_secs.max(5)));
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut daily = tokio::time::interval(Duration::from_secs(
        app.cfg.source.data_ready_poll_secs.max(60),
    ));
    daily.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut housekeeping = tokio::time::interval(Duration::from_secs(6 * 3600));
    let mut failures = 0u32;
    let mut skip = 0u32;

    loop {
        tokio::select! {
            _ = poll.tick() => {
                if skip > 0 {
                    skip -= 1;
                    continue;
                }
                match poll_dispatch(&app, &engine, &client).await {
                    Ok(_) => {
                        failures = 0;
                        if let Ok(mut h) = app.health.write() {
                            h.last_poll_ok = Some(Utc::now());
                            h.last_error = None;
                        }
                    }
                    Err(e) => {
                        failures += 1;
                        skip = backoff_ticks(failures);
                        tracing::warn!(failures, skip, "dispatch poll failed: {e:#}");
                        if let Ok(mut h) = app.health.write() { h.last_error = Some(format!("{e:#}")); }
                    }
                }
            }
            _ = daily.tick() => {
                if let Err(e) = poll_data_ready(&app, &client).await {
                    tracing::warn!("next-day poll failed: {e:#}");
                }
            }
            _ = housekeeping.tick() => {
                match prune_buffer(&app) {
                    Ok(n) => tracing::debug!(removed = n, "pruned buffer"),
                    Err(e) => tracing::warn!("prune failed: {e:#}"),
                }
            }
        }
    }
}

/// Poll ticks to skip after `failures` consecutive errors: 1, 2, 4, 8, then 15
/// (about five minutes at the default 20 s poll).
pub fn backoff_ticks(failures: u32) -> u32 {
    match failures {
        0 => 0,
        f => (1u32 << (f - 1).min(4)).min(15),
    }
}

async fn poll_dispatch(
    app: &App,
    engine: &AsyncMutex<Engine>,
    client: &reqwest::Client,
) -> Result<usize> {
    let base = format!(
        "{}/{}/",
        app.cfg.source.base_url.trim_end_matches('/'),
        app.cfg.source.dispatch_dir
    );
    let listing = nemweb::fetch_text(client, &base)
        .await
        .context("fetching DispatchIS listing")?;
    let cutoff = now_market() - chrono::Duration::hours(app.cfg.source.backfill_hours);
    let last = engine.lock().await.detector.last_settlement();
    let mut todo = Vec::new();
    for name in nemweb::dispatch_files(&listing) {
        let Some(t) = nemweb::dispatch_file_interval(&name) else {
            continue;
        };
        if t < cutoff || last.is_some_and(|l| t <= l) {
            continue;
        }
        if app.with_db(|c| store::is_seen(c, &name))? {
            continue;
        }
        todo.push(name);
    }
    let n = todo.len();
    for name in todo.into_iter().take(500) {
        let bytes = nemweb::fetch_bytes(client, &format!("{base}{name}"))
            .await
            .with_context(|| format!("downloading {name}"))?;
        if let Err(e) = buffer_raw(app, &name, &bytes) {
            tracing::warn!("buffering {name}: {e:#}");
        }
        for (member, csv) in nemweb::unzip_csvs(&bytes)? {
            if let Err(e) = process_report(app, engine, &member, &csv).await {
                tracing::warn!("skipping {member}: {e:#}");
            }
        }
        app.with_db(|c| store::mark_seen(c, &name, "dispatchis"))?;
    }
    Ok(n)
}

/// Watch next-day directories; when every configured source has a file for
/// trading day D, promote closed events on or before D to the next_day tier.
pub async fn poll_data_ready(app: &App, client: &reqwest::Client) -> Result<()> {
    let base = app.cfg.source.base_url.trim_end_matches('/');
    for dir in &app.cfg.source.data_ready_dirs {
        let listing = nemweb::fetch_text(client, &format!("{base}/{dir}/"))
            .await
            .with_context(|| format!("listing {dir}"))?;
        app.with_db(|c| {
            for (file, day) in nemweb::daily_files(&listing) {
                store::record_data_ready(c, dir, &day.to_string(), &file)?;
            }
            Ok(())
        })?;
    }
    promote_ready_events(app).await
}

pub async fn promote_ready_events(app: &App) -> Result<()> {
    let sources = app.cfg.source.data_ready_dirs.clone();
    let Some(day) = app.with_db(|c| store::latest_complete_day(c, &sources))? else {
        return Ok(());
    };
    let events = app.with_db(|c| store::events_awaiting_next_day(c, &day))?;
    for mut ev in events
        .into_iter()
        .filter(|e| e.status == crate::model::Status::Closed)
    {
        ev.data_tier = "next_day".into();
        ev.updated_at = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        app.with_db(|c| store::upsert_event(c, &ev))?;
        publish(
            app,
            Transition {
                kind: TransitionKind::DataReady {
                    tier: "next_day".into(),
                },
                event: ev.clone(),
            },
        )
        .await;
        if let Err(e) = add_next_day_to_bundle(app, &ev.id).await {
            tracing::warn!(event = %ev.id, "adding next-day data to bundle: {e:#}");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_then_caps() {
        let ticks: Vec<u32> = (0..8).map(backoff_ticks).collect();
        assert_eq!(ticks, [0, 1, 2, 4, 8, 15, 15, 15]);
    }

    /// P0.4 acceptance: replay archived DispatchIS files for 12 October 2026.
    /// Run with `NEM_OCT12_DIR=<dir> cargo test -- --ignored oct12`.
    #[tokio::test]
    #[ignore = "needs archived AEMO files in NEM_OCT12_DIR"]
    async fn oct12_nsw_event_matches_published_figures() {
        let dir = std::env::var("NEM_OCT12_DIR").expect("set NEM_OCT12_DIR");
        let mut cfg = crate::config::Config::default();
        cfg.store.path = ":memory:".into();
        let mut app = App::new(cfg, store::open(":memory:").unwrap(), "replay").unwrap();
        app.notify_external = false;
        app.fire_triggers = false;
        replay_dir(&app, Path::new(&dir), Duration::ZERO)
            .await
            .unwrap();
        let events = app
            .with_db(|c| {
                store::list_events(
                    c,
                    &store::EventFilter {
                        region: Some("NSW1"),
                        status: None,
                        include_discarded: false,
                        limit: 50,
                    },
                )
            })
            .unwrap();
        let ev = events
            .iter()
            .find(|e| e.trading_day == "2026-10-12" && e.peak_price >= 23_000.0)
            .expect("an NSW1 event on 12 October");
        assert_eq!(ev.peak_price, 23_107.0);
        assert_eq!(ev.intervals_above_high, 11);
    }
}

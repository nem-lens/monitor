//! Ingest loop: polls NEMWeb for new DispatchIS files and next-day files,
//! runs the detector and publishes transitions. Also drives offline replay.

use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use tokio::sync::Mutex as AsyncMutex;

use crate::aemo;
use crate::app::{App, StreamMsg};
use crate::detect::Detector;
use crate::model::{DispatchSnapshot, Transition, TransitionKind};
use crate::nemweb;
use crate::notify::publish;
use crate::store;
use crate::time::{iso_market, now_market};

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
pub async fn process_report(app: &App, engine: &AsyncMutex<Engine>, name: &str, csv: &[u8]) -> Result<()> {
    let tables = aemo::parse_report(csv).with_context(|| format!("parsing {name}"))?;
    let snap: DispatchSnapshot = aemo::dispatch_snapshot(&tables).with_context(|| format!("reading {name}"))?;
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
    for tr in transitions {
        publish(app, tr).await;
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
        let name = path.file_name().map(|s| s.to_string_lossy().to_string()).unwrap_or_default();
        let bytes = std::fs::read(&path)?;
        let reports = if name.to_ascii_lowercase().ends_with(".zip") {
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
    Ok(n)
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
    let mut daily = tokio::time::interval(Duration::from_secs(app.cfg.source.data_ready_poll_secs.max(60)));
    daily.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut housekeeping = tokio::time::interval(Duration::from_secs(6 * 3600));
    let mut backoff = 0u32;

    loop {
        tokio::select! {
            _ = poll.tick() => {
                if backoff > 0 {
                    // Skip ticks while backing off after errors (max ~5 minutes).
                    backoff -= 1;
                    continue;
                }
                match poll_dispatch(&app, &engine, &client).await {
                    Ok(_) => {
                        if let Ok(mut h) = app.health.write() {
                            h.last_poll_ok = Some(Utc::now());
                            h.last_error = None;
                        }
                    }
                    Err(e) => {
                        tracing::warn!("dispatch poll failed: {e:#}");
                        if let Ok(mut h) = app.health.write() { h.last_error = Some(format!("{e:#}")); }
                        backoff = (backoff + 1).min(15);
                    }
                }
            }
            _ = daily.tick() => {
                if let Err(e) = poll_data_ready(&app, &client).await {
                    tracing::warn!("next-day poll failed: {e:#}");
                }
            }
            _ = housekeeping.tick() => {
                let days = app.cfg.store.retain_days;
                if let Err(e) = app.with_db(|c| store::prune(c, days)) {
                    tracing::warn!("prune failed: {e:#}");
                }
            }
        }
    }
}

async fn poll_dispatch(app: &App, engine: &AsyncMutex<Engine>, client: &reqwest::Client) -> Result<usize> {
    let base = format!("{}/{}/", app.cfg.source.base_url.trim_end_matches('/'), app.cfg.source.dispatch_dir);
    let listing = nemweb::fetch_text(client, &base).await.context("fetching DispatchIS listing")?;
    let cutoff = now_market() - chrono::Duration::hours(app.cfg.source.backfill_hours);
    let last = engine.lock().await.detector.last_settlement();
    let mut todo = Vec::new();
    for name in nemweb::dispatch_files(&listing) {
        let Some(t) = nemweb::dispatch_file_interval(&name) else { continue };
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
        let bytes = nemweb::fetch_bytes(client, &format!("{base}{name}")).await.with_context(|| format!("downloading {name}"))?;
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
        let listing = nemweb::fetch_text(client, &format!("{base}/{dir}/")).await.with_context(|| format!("listing {dir}"))?;
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
    let Some(day) = app.with_db(|c| store::latest_complete_day(c, &sources))? else { return Ok(()) };
    let events = app.with_db(|c| store::events_awaiting_next_day(c, &day))?;
    for mut ev in events.into_iter().filter(|e| e.status == crate::model::Status::Closed) {
        ev.data_tier = "next_day".into();
        ev.updated_at = Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        app.with_db(|c| store::upsert_event(c, &ev))?;
        publish(app, Transition { kind: TransitionKind::DataReady { tier: "next_day".into() }, event: ev }).await;
    }
    Ok(())
}

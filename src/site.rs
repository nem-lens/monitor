//! Static site for GitHub Pages (handover P2).
//!
//! `nem-monitor once` runs one poll without a persistent database: it loads
//! the previous run's state from the published site (`data/state.json`),
//! rebuilds an in-memory SQLite from it, polls NEMWeb once, and writes the
//! whole site again: the page in static mode, JSON documents mirroring the
//! API, the feeds, and the new state. Deploying the site is the commit, so
//! nothing is pushed to the repository.
//!
//! The state is public, like everything else on the site. It holds public
//! notifications only; internal ones (opened, discarded) are not exported.

use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use chrono::Duration;
use serde::{Deserialize, Serialize};

use crate::app::App;
use crate::feeds;
use crate::model::{Event, NotificationItem};
use crate::store::{self, DataReadyRow, EventFilter, IntervalRow};
use crate::time::{iso_market, parse_market};

pub const STATE_FILE: &str = "data/state.json";
const FORMAT: u32 = 1;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct SiteState {
    pub format: u32,
    pub generated_at: String,
    /// Detector state (`Detector::state_json`), so open events continue.
    pub detector: Option<String>,
    pub events: Vec<Event>,
    /// Public notifications, with their sequence numbers.
    pub notifications: Vec<NotificationItem>,
    /// Dispatch outcomes for the rolling buffer.
    pub intervals: Vec<IntervalRow>,
    pub data_ready: Vec<DataReadyRow>,
}

/// Load the previous state from a site directory or URL. `None` means there
/// is no previous state (first run): a missing file or HTTP 404. Any other
/// failure is an error, because starting fresh by mistake would re-announce
/// recent events.
pub async fn load_state(source: &str, user_agent: &str) -> Result<Option<SiteState>> {
    let text = if source.starts_with("http://") || source.starts_with("https://") {
        let url = format!("{}/{STATE_FILE}", source.trim_end_matches('/'));
        let resp = crate::nemweb::client(user_agent)?
            .get(&url)
            .send()
            .await
            .with_context(|| format!("fetching {url}"))?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        resp.error_for_status()
            .with_context(|| format!("fetching {url}"))?
            .text()
            .await?
    } else {
        let path = Path::new(source).join(STATE_FILE);
        if !path.exists() {
            return Ok(None);
        }
        fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?
    };
    let state: SiteState = serde_json::from_str(&text).context("parsing state.json")?;
    anyhow::ensure!(
        state.format == FORMAT,
        "state.json format {} is not supported (expected {FORMAT})",
        state.format
    );
    Ok(Some(state))
}

pub fn import(app: &App, s: &SiteState) -> Result<()> {
    app.with_db(|c| {
        if let Some(d) = &s.detector {
            store::save_state(c, d)?;
        }
        for e in &s.events {
            store::upsert_event(c, e)?;
        }
        for n in &s.notifications {
            store::restore_notification(c, n)?;
        }
        for r in &s.intervals {
            store::insert_interval(c, r)?;
        }
        for d in &s.data_ready {
            store::set_data_ready_file(c, &d.source, &d.trading_day, &d.filename)?;
        }
        Ok(())
    })
}

/// The state to carry to the next run: intervals within the buffer of the
/// latest one, and everything else that the API serves.
pub fn export(app: &App) -> Result<SiteState> {
    let hours = app.cfg.store.buffer_hours;
    app.with_db(|c| {
        let from = store::latest_settlement(c)?
            .as_deref()
            .and_then(parse_market)
            .map(|t| iso_market(t - Duration::hours(hours)))
            .unwrap_or_default();
        Ok(SiteState {
            format: FORMAT,
            generated_at: chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            detector: store::load_state(c)?,
            events: store::list_events(
                c,
                &EventFilter {
                    region: None,
                    status: None,
                    include_discarded: false,
                    limit: 500,
                },
            )?,
            notifications: store::list_notifications(c, true, 0, 500)?
                .into_iter()
                .rev()
                .collect(),
            intervals: store::intervals_since(c, &from)?,
            data_ready: store::list_data_ready(c)?,
        })
    })
}

fn write_json(path: &Path, v: &impl Serialize) -> Result<()> {
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir)?;
    }
    fs::write(path, serde_json::to_vec(v)?).with_context(|| format!("writing {}", path.display()))
}

/// Event ids are generated internally ("202610121420-NSW1-high"); refuse
/// anything that could escape the output directory.
fn safe_id(id: &str) -> bool {
    !id.is_empty()
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// Write the whole site into `out`.
pub fn write_site(app: &App, out: &Path) -> Result<SiteState> {
    fs::create_dir_all(out.join("data/events"))?;
    fs::write(
        out.join("index.html"),
        crate::web::render_index(&app.cfg, true),
    )?;
    // Serve files as they are: no Jekyll processing on Pages.
    fs::write(out.join(".nojekyll"), b"")?;

    write_json(
        &out.join("data/status.json"),
        &crate::web::status_json(app, true)?,
    )?;
    let (events, recent, feed) = app.with_db(|c| {
        let events = store::list_events(
            c,
            &EventFilter {
                region: None,
                status: None,
                include_discarded: false,
                limit: 100,
            },
        )?;
        let recent = store::list_notifications(c, true, 0, 50)?;
        let feed = store::list_notifications(c, true, 0, app.cfg.public.feed_items)?;
        Ok((events, recent, feed))
    })?;
    write_json(
        &out.join("data/events.json"),
        &serde_json::json!({ "events": events }),
    )?;
    for e in events.iter().filter(|e| safe_id(&e.id)) {
        if let Some(d) = crate::web::event_detail_json(app, &e.id)? {
            write_json(&out.join(format!("data/events/{}.json", e.id)), &d)?;
        }
    }
    write_json(
        &out.join("data/notifications.json"),
        &serde_json::json!({ "notifications": recent }),
    )?;
    fs::write(out.join("feed.atom"), feeds::atom(&feed, &app.cfg))?;
    write_json(&out.join("feed.json"), &feeds::json_feed(&feed, &app.cfg))?;

    let state = export(app)?;
    write_json(&out.join(STATE_FILE), &state)?;
    Ok(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    #[test]
    fn ids_cannot_escape_the_site() {
        assert!(safe_id("202610121420-NSW1-high"));
        assert!(!safe_id("../x"));
        assert!(!safe_id("a/b"));
        assert!(!safe_id(""));
    }

    #[tokio::test]
    async fn missing_state_is_a_first_run() {
        let dir = std::env::temp_dir().join(format!("nem-site-none-{}", std::process::id()));
        assert_eq!(load_state(dir.to_str().unwrap(), "t").await.unwrap(), None);
    }

    #[tokio::test]
    async fn state_round_trips_through_a_written_site() {
        let dir = std::env::temp_dir().join(format!("nem-site-rt-{}", std::process::id()));
        let app = App::new(
            Config::default(),
            store::open(":memory:").unwrap(),
            "static",
        )
        .unwrap();
        let ev: Event = serde_json::from_value(serde_json::json!({
            "id": "202610121420-NSW1-high", "region": "NSW1", "kind": "high_price", "status": "closed",
            "severity": "cap", "trading_day": "2026-10-12", "start": "2026-10-12T14:20:00+10:00",
            "last_active": "2026-10-12T15:35:00+10:00", "end": "2026-10-12T15:35:00+10:00",
            "peak_price": 23107.0, "peak_interval": "2026-10-12T14:20:00+10:00", "intervals_active": 11,
            "intervals_above_high": 11, "intervals_above_extreme": 5, "mean_price": 8966.0, "min_price": 67.0,
            "flags": [], "constraints": [], "interconnectors_at_limit": [], "demand_start": null,
            "demand_peak": null, "data_tier": "live", "updated_at": "x"
        }))
        .unwrap();
        let note = NotificationItem {
            seq: 7,
            event_id: ev.id.clone(),
            transition: "closed".into(),
            title: "NSW price event over: peak $23,107/MWh".into(),
            summary: "s".into(),
            url: "u".into(),
            created_at: "2026-10-12T06:00:00Z".into(),
            public: true,
            event: ev.clone(),
        };
        let original = SiteState {
            format: FORMAT,
            generated_at: String::new(),
            detector: Some("{\"regions\":{},\"last_settlement\":\"2026-10-12T16:00:00\"}".into()),
            events: vec![ev],
            notifications: vec![note],
            intervals: vec![IntervalRow {
                settlement: "2026-10-12T16:00:00+10:00".into(),
                region: "NSW1".into(),
                rrp: 300.0,
                demand: Some(9000.0),
                available_gen: None,
                net_interchange: None,
            }],
            data_ready: vec![DataReadyRow {
                source: "Next_Day_Dispatch".into(),
                trading_day: "2026-10-12".into(),
                filename: "PUBLIC_NEXT_DAY_DISPATCH_20261012_0000000542594175.zip".into(),
            }],
        };
        import(&app, &original).unwrap();
        write_site(&app, &dir).unwrap();
        let mut back = load_state(dir.to_str().unwrap(), "t")
            .await
            .unwrap()
            .unwrap();
        back.generated_at.clear();
        assert_eq!(back, original);

        // The site carries what the page and readers need.
        for f in [
            "index.html",
            ".nojekyll",
            "feed.atom",
            "feed.json",
            "data/status.json",
            "data/events.json",
            "data/notifications.json",
            "data/events/202610121420-NSW1-high.json",
        ] {
            assert!(dir.join(f).exists(), "{f}");
        }
        let status: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(dir.join("data/status.json")).unwrap())
                .unwrap();
        assert_eq!(status["mode"], "static");
        assert!(status["subscribe"]["sse"].is_null() && status["subscribe"]["webhooks"].is_null());
        let page = fs::read_to_string(dir.join("index.html")).unwrap();
        assert!(page.contains("const STATIC = true;"));

        // Sequence numbers continue after a restore.
        let next = app
            .with_db(|c| {
                let mut n = original.notifications[0].clone();
                n.seq = 0;
                store::add_notification(c, &n)
            })
            .unwrap();
        assert_eq!(next, 8);
        fs::remove_dir_all(&dir).ok();
    }
}

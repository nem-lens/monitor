//! Shared application state.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use chrono::{DateTime, Utc};
use rusqlite::Connection;
use serde::Serialize;
use tokio::sync::{broadcast, Semaphore};

use crate::config::Config;

#[derive(Debug, Clone, Default, Serialize)]
pub struct Health {
    pub mode: String,
    pub started_at: Option<DateTime<Utc>>,
    pub last_poll_ok: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub last_file: Option<String>,
    pub last_interval: Option<String>,
    pub files_processed: u64,
}

impl Health {
    /// Live mode is healthy when NEMWeb answered within 10 minutes and a new
    /// interval arrived within 15 (or, before the first one, the service started
    /// within 15). Reaching NEMWeb alone is not enough: files can stop parsing.
    /// Replay and offline modes are always healthy.
    pub fn is_fresh(&self, now: DateTime<Utc>) -> bool {
        if self.mode != "live" {
            return true;
        }
        let within =
            |t: Option<DateTime<Utc>>, mins: i64| t.is_some_and(|t| (now - t).num_minutes() < mins);
        let latest_data = match &self.last_interval {
            Some(s) => crate::time::parse_market(s).map(crate::time::to_utc),
            None => self.started_at,
        };
        within(self.last_poll_ok, 10) && within(latest_data, 15)
    }
}

/// Messages on the public live stream (SSE).
#[derive(Debug, Clone, Serialize)]
pub struct StreamMsg {
    pub kind: &'static str,
    pub data: serde_json::Value,
}

#[derive(Clone)]
pub struct App {
    pub cfg: Arc<Config>,
    pub db: Arc<Mutex<Connection>>,
    pub tx: broadcast::Sender<StreamMsg>,
    pub health: Arc<RwLock<Health>>,
    pub http: reqwest::Client,
    pub trigger_sem: Arc<Semaphore>,
    pub delivery_sem: Arc<Semaphore>,
    pub reg_limiter: Arc<Mutex<HashMap<IpAddr, Vec<Instant>>>>,
    /// Per-IP request counts for the current minute (window start, count).
    pub api_limiter: Arc<Mutex<HashMap<IpAddr, (Instant, u32)>>>,
    /// One permit per open live-stream connection.
    pub sse_slots: Arc<Semaphore>,
    /// Send to ntfy and public webhooks.
    pub notify_external: bool,
    /// Run operator triggers.
    pub fire_triggers: bool,
    /// Print transitions to stdout (replay mode).
    pub echo: bool,
}

impl App {
    pub fn new(cfg: Config, conn: Connection, mode: &str) -> anyhow::Result<Self> {
        let (tx, _) = broadcast::channel(256);
        let http = reqwest::Client::builder()
            .user_agent(cfg.source.user_agent.clone())
            .timeout(std::time::Duration::from_secs(15))
            .build()?;
        let triggers = cfg.max_concurrent_triggers.unwrap_or(2).max(1);
        let sse = cfg.public.max_sse_connections;
        Ok(Self {
            cfg: Arc::new(cfg),
            db: Arc::new(Mutex::new(conn)),
            tx,
            health: Arc::new(RwLock::new(Health {
                mode: mode.into(),
                started_at: Some(Utc::now()),
                ..Default::default()
            })),
            http,
            trigger_sem: Arc::new(Semaphore::new(triggers)),
            delivery_sem: Arc::new(Semaphore::new(16)),
            reg_limiter: Arc::new(Mutex::new(HashMap::new())),
            api_limiter: Arc::new(Mutex::new(HashMap::new())),
            sse_slots: Arc::new(Semaphore::new(sse)),
            notify_external: true,
            fire_triggers: true,
            echo: false,
        })
    }

    /// Run a closure against the database. Never hold the guard across an await.
    pub fn with_db<T>(
        &self,
        f: impl FnOnce(&Connection) -> anyhow::Result<T>,
    ) -> anyhow::Result<T> {
        let guard = self
            .db
            .lock()
            .map_err(|_| anyhow::anyhow!("database lock poisoned"))?;
        f(&guard)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;

    fn live(now: DateTime<Utc>, poll_ago: i64, interval_ago: Option<i64>) -> Health {
        Health {
            mode: "live".into(),
            started_at: Some(now - Duration::minutes(60)),
            last_poll_ok: Some(now - Duration::minutes(poll_ago)),
            last_interval: interval_ago.map(|m| {
                crate::time::iso_market(crate::time::from_utc(now - Duration::minutes(m)))
            }),
            ..Default::default()
        }
    }

    #[test]
    fn health_needs_both_polls_and_new_intervals() {
        let now = Utc::now();
        assert!(live(now, 1, Some(5)).is_fresh(now));
        assert!(!live(now, 11, Some(5)).is_fresh(now), "NEMWeb unreachable");
        assert!(
            !live(now, 1, Some(20)).is_fresh(now),
            "polling but no new data"
        );
        assert!(
            !live(now, 1, None).is_fresh(now),
            "no data an hour after start"
        );
    }

    #[test]
    fn health_allows_a_startup_grace_period() {
        let now = Utc::now();
        let mut h = live(now, 1, None);
        h.started_at = Some(now - Duration::minutes(2));
        assert!(h.is_fresh(now));
        h.last_poll_ok = None;
        assert!(!h.is_fresh(now));
    }

    #[test]
    fn replay_and_offline_are_always_healthy() {
        let h = Health {
            mode: "offline".into(),
            ..Default::default()
        };
        assert!(h.is_fresh(Utc::now()));
    }
}

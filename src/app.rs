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
    pub last_poll_ok: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
    pub last_file: Option<String>,
    pub last_interval: Option<String>,
    pub files_processed: u64,
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
        Ok(Self {
            cfg: Arc::new(cfg),
            db: Arc::new(Mutex::new(conn)),
            tx,
            health: Arc::new(RwLock::new(Health {
                mode: mode.into(),
                ..Default::default()
            })),
            http,
            trigger_sem: Arc::new(Semaphore::new(triggers)),
            delivery_sem: Arc::new(Semaphore::new(16)),
            reg_limiter: Arc::new(Mutex::new(HashMap::new())),
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

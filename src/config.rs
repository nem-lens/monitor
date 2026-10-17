//! Configuration, loaded from a TOML file. Every field has a default so a
//! minimal config only needs `[server] public_url`.

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default)]
pub struct Config {
    pub server: ServerCfg,
    pub source: SourceCfg,
    pub store: StoreCfg,
    pub detect: DetectCfg,
    pub public: PublicCfg,
    #[serde(rename = "triggers")]
    pub triggers: Vec<TriggerCfg>,
    pub max_concurrent_triggers: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct ServerCfg {
    pub bind: String,
    /// Public base URL, used in feeds and notification links.
    pub public_url: String,
    /// Trust X-Forwarded-For for client IPs (only behind your own reverse proxy).
    pub trust_proxy_headers: bool,
}

impl Default for ServerCfg {
    fn default() -> Self {
        Self {
            bind: "0.0.0.0:8080".into(),
            public_url: "http://localhost:8080".into(),
            trust_proxy_headers: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct SourceCfg {
    pub base_url: String,
    pub dispatch_dir: String,
    pub poll_secs: u64,
    /// On a cold start, process files from this many hours back.
    pub backfill_hours: i64,
    pub user_agent: String,
    pub data_ready_poll_secs: u64,
    /// NEMWeb directories that must all show a file for trading day D before
    /// events on day D are marked `next_day` (bids and unit data public).
    pub data_ready_dirs: Vec<String>,
}

impl Default for SourceCfg {
    fn default() -> Self {
        Self {
            base_url: "https://nemweb.com.au/Reports/Current".into(),
            dispatch_dir: "DispatchIS_Reports".into(),
            poll_secs: 20,
            backfill_hours: 6,
            user_agent: concat!("nem-monitor/", env!("CARGO_PKG_VERSION")).into(),
            data_ready_poll_secs: 600,
            data_ready_dirs: vec!["Next_Day_Dispatch".into(), "Next_Day_Offer_Energy".into()],
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct StoreCfg {
    pub path: String,
    pub retain_days: i64,
}

impl Default for StoreCfg {
    fn default() -> Self {
        Self { path: "nem-monitor.db".into(), retain_days: 90 }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct DetectCfg {
    pub regions: Vec<String>,
    /// AEMC market price cap for the current financial year ($23,200/MWh for 2026-27).
    pub market_price_cap: f64,
    pub high_price: f64,
    pub extreme_price: f64,
    /// A price at or above this fraction of the cap is severity `cap`.
    pub cap_fraction: f64,
    /// Intervals at or above `high_price` needed to confirm an event.
    pub confirm_intervals: u32,
    /// A single interval at or above this confirms immediately.
    pub confirm_immediately_price: f64,
    /// Intervals below this count towards closing an event.
    pub close_price: f64,
    /// Consecutive quiet intervals that close an event.
    pub quiet_intervals: u32,
    /// Low-price events open at or below this price.
    pub floor_price: f64,
    pub whiplash_step: f64,
    pub whiplash_reversals: usize,
    pub whiplash_window: usize,
    pub separation_price: f64,
    pub demand_ramp_mw: f64,
    pub demand_ramp_window: usize,
}

impl Default for DetectCfg {
    fn default() -> Self {
        Self {
            regions: ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"].iter().map(|s| s.to_string()).collect(),
            market_price_cap: 23_200.0,
            high_price: 5_000.0,
            extreme_price: 10_000.0,
            cap_fraction: 0.95,
            confirm_intervals: 3,
            confirm_immediately_price: 15_000.0,
            close_price: 1_000.0,
            quiet_intervals: 6,
            floor_price: -900.0,
            whiplash_step: 1_000.0,
            whiplash_reversals: 3,
            whiplash_window: 12,
            separation_price: 5_000.0,
            demand_ramp_mw: 1_500.0,
            demand_ramp_window: 12,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub struct PublicCfg {
    pub site_title: String,
    /// Lowest severity that reaches public channels: "high", "extreme" or "cap".
    pub min_severity: String,
    pub include_low_price: bool,
    /// ntfy server and topic for public push; empty topic disables.
    pub ntfy_server: String,
    pub ntfy_topic: String,
    pub max_webhooks: usize,
    pub allow_http_webhooks: bool,
    pub webhook_registrations_per_ip_per_hour: usize,
    pub webhook_disable_after_failures: i64,
    pub feed_items: usize,
}

impl Default for PublicCfg {
    fn default() -> Self {
        Self {
            site_title: "NEM Event Monitor".into(),
            min_severity: "high".into(),
            include_low_price: true,
            ntfy_server: "https://ntfy.sh".into(),
            ntfy_topic: String::new(),
            max_webhooks: 1000,
            allow_http_webhooks: false,
            webhook_registrations_per_ip_per_hour: 5,
            webhook_disable_after_failures: 10,
            feed_items: 50,
        }
    }
}

/// An operator trigger: runs the rest of the pipeline (replay, report) when an
/// event changes state. Commands run without a shell; placeholders are
/// substituted per argument.
#[derive(Debug, Clone, Deserialize)]
pub struct TriggerCfg {
    pub name: String,
    /// Transitions to fire on: opened, confirmed, escalated, flagged, closed, discarded, data_ready.
    pub on: Vec<String>,
    #[serde(default)]
    pub min_severity: Option<String>,
    #[serde(default)]
    pub regions: Option<Vec<String>>,
    /// argv; placeholders {event_id} {region} {transition} {data_tier} {trading_day} {severity}.
    #[serde(default)]
    pub command: Option<Vec<String>>,
    /// Internal webhook URL (trusted; not subject to the public SSRF guard).
    #[serde(default)]
    pub webhook: Option<String>,
    /// Environment variable holding the HMAC secret for `webhook`.
    #[serde(default)]
    pub secret_env: Option<String>,
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
}

fn default_timeout() -> u64 {
    900
}

impl Config {
    pub fn load(path: Option<&str>) -> anyhow::Result<Self> {
        let cfg: Config = match path {
            Some(p) => {
                let text = std::fs::read_to_string(p)
                    .map_err(|e| anyhow::anyhow!("reading config {p}: {e}"))?;
                toml::from_str(&text)?
            }
            None => Config::default(),
        };
        cfg.validate()?;
        Ok(cfg)
    }

    pub fn validate(&self) -> anyhow::Result<()> {
        let d = &self.detect;
        anyhow::ensure!(d.high_price < d.extreme_price, "high_price must be below extreme_price");
        anyhow::ensure!(d.close_price <= d.high_price, "close_price must not exceed high_price");
        anyhow::ensure!(d.quiet_intervals >= 1, "quiet_intervals must be at least 1");
        anyhow::ensure!(d.whiplash_window >= 3, "whiplash_window must be at least 3");
        anyhow::ensure!(
            crate::model::Severity::parse(&self.public.min_severity).is_some(),
            "public.min_severity must be high, extreme or cap"
        );
        for t in &self.triggers {
            anyhow::ensure!(
                t.command.is_some() || t.webhook.is_some(),
                "trigger {} needs a command or a webhook",
                t.name
            );
            for on in &t.on {
                anyhow::ensure!(
                    crate::model::TRANSITIONS.contains(&on.as_str()),
                    "trigger {}: unknown transition {on}",
                    t.name
                );
            }
        }
        Ok(())
    }
}

//! Configuration, loaded from a TOML file. Every field has a default so a
//! minimal config only needs `[server] public_url`.

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
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
#[serde(default, deny_unknown_fields)]
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
            // Localhost only; expose through a reverse proxy, or set 0.0.0.0 in a container.
            bind: "127.0.0.1:8080".into(),
            public_url: "http://localhost:8080".into(),
            trust_proxy_headers: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
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
            // Next_Day_Offer_Energy is empty on NEMWeb (checked 17 Oct 2026);
            // energy offers are published in the _SPARSE directory.
            data_ready_dirs: vec![
                "Next_Day_Dispatch".into(),
                "Next_Day_Offer_Energy_SPARSE".into(),
            ],
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StoreCfg {
    pub path: String,
    /// Rolling buffer of dispatch outcomes and raw DispatchIS zips, in hours.
    /// Older data is dropped; confirmed events keep theirs in bundles.
    pub buffer_hours: i64,
    /// Raw DispatchIS zips for the buffer, from which bundles are cut.
    pub raw_dir: String,
    /// One directory per confirmed event: Parquet tables and manifest.json.
    pub bundle_dir: String,
}

impl Default for StoreCfg {
    fn default() -> Self {
        Self {
            path: "nem-monitor.db".into(),
            buffer_hours: 48,
            raw_dir: "raw".into(),
            bundle_dir: "bundles".into(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
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
            regions: ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
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
#[serde(default, deny_unknown_fields)]
pub struct PublicCfg {
    pub site_title: String,
    /// Lowest severity that reaches public channels: "high", "extreme" or "cap".
    pub min_severity: String,
    pub include_low_price: bool,
    /// ntfy server and topic for public push; empty topic disables.
    pub ntfy_server: String,
    pub ntfy_topic: String,
    /// Off: the webhook routes return 404 and the page hides the form.
    pub webhooks_enabled: bool,
    pub max_webhooks: usize,
    pub allow_http_webhooks: bool,
    pub webhook_registrations_per_ip_per_hour: usize,
    pub webhook_disable_after_failures: i64,
    pub feed_items: usize,
    /// Concurrent live-stream (SSE) clients; further connections get 503.
    pub max_sse_connections: usize,
    /// Requests per client IP per minute across the API and feeds; 0 disables.
    pub api_requests_per_ip_per_minute: u32,
    /// owner/name of the GitHub repo that hosts the static site, for the page's
    /// subscribe instructions (Watch for Discussions and Releases); empty hides them.
    pub github_repo: String,
}

impl Default for PublicCfg {
    fn default() -> Self {
        Self {
            site_title: "NEM Event Monitor".into(),
            min_severity: "high".into(),
            include_low_price: true,
            ntfy_server: "https://ntfy.sh".into(),
            ntfy_topic: String::new(),
            webhooks_enabled: true,
            max_webhooks: 1000,
            allow_http_webhooks: false,
            webhook_registrations_per_ip_per_hour: 5,
            webhook_disable_after_failures: 10,
            feed_items: 50,
            max_sse_connections: 200,
            api_requests_per_ip_per_minute: 120,
            github_repo: String::new(),
        }
    }
}

/// An operator trigger: runs the rest of the pipeline (replay, report) when an
/// event changes state. Commands run without a shell; placeholders are
/// substituted per argument.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
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
        use crate::model::Severity;
        anyhow::ensure!(
            self.server.bind.parse::<std::net::SocketAddr>().is_ok(),
            "server.bind must be an address and port, e.g. 127.0.0.1:8080"
        );
        anyhow::ensure!(
            url::Url::parse(&self.server.public_url).is_ok(),
            "server.public_url must be an absolute URL"
        );
        anyhow::ensure!(
            self.store.buffer_hours >= 6,
            "store.buffer_hours must be at least 6 (an event window is up to several hours)"
        );
        let d = &self.detect;
        anyhow::ensure!(!d.regions.is_empty(), "detect.regions must not be empty");
        anyhow::ensure!(
            d.confirm_intervals >= 1,
            "confirm_intervals must be at least 1"
        );
        anyhow::ensure!(
            d.cap_fraction > 0.0 && d.cap_fraction <= 1.0,
            "cap_fraction must be in (0, 1]"
        );
        anyhow::ensure!(
            d.high_price < d.extreme_price,
            "high_price must be below extreme_price"
        );
        anyhow::ensure!(
            d.close_price <= d.high_price,
            "close_price must not exceed high_price"
        );
        anyhow::ensure!(d.quiet_intervals >= 1, "quiet_intervals must be at least 1");
        anyhow::ensure!(d.whiplash_window >= 3, "whiplash_window must be at least 3");
        anyhow::ensure!(
            Severity::parse(&self.public.min_severity).is_some(),
            "public.min_severity must be high, extreme or cap"
        );
        for t in &self.triggers {
            anyhow::ensure!(
                t.command.as_ref().is_some_and(|c| !c.is_empty()) || t.webhook.is_some(),
                "trigger {} needs a command or a webhook",
                t.name
            );
            if let Some(s) = &t.min_severity {
                // An unparsable severity would otherwise match every event.
                anyhow::ensure!(
                    Severity::parse(s).is_some(),
                    "trigger {}: min_severity {s} must be floor, high, extreme or cap",
                    t.name
                );
            }
            for r in t.regions.iter().flatten() {
                anyhow::ensure!(
                    d.regions.contains(r),
                    "trigger {}: region {r} is not in detect.regions",
                    t.name
                );
            }
            if let Some(w) = &t.webhook {
                anyhow::ensure!(
                    url::Url::parse(w).is_ok(),
                    "trigger {}: webhook is not a valid URL",
                    t.name
                );
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(text: &str) -> anyhow::Result<Config> {
        let cfg: Config = toml::from_str(text)?;
        cfg.validate()?;
        Ok(cfg)
    }

    #[test]
    fn example_config_parses_and_validates() {
        let cfg = parse(include_str!("../deploy/config.example.toml")).unwrap();
        assert_eq!(cfg.triggers.len(), 2);
        assert_eq!(cfg.server.bind, "127.0.0.1:8080");
    }

    #[test]
    fn defaults_bind_localhost_and_watch_sparse_offers() {
        let cfg = parse("").unwrap();
        assert_eq!(cfg.server.bind, "127.0.0.1:8080");
        assert!(!cfg.server.trust_proxy_headers);
        assert_eq!(
            cfg.source.data_ready_dirs,
            ["Next_Day_Dispatch", "Next_Day_Offer_Energy_SPARSE"]
        );
    }

    #[test]
    fn unknown_keys_are_rejected() {
        let err = parse("[store]\nretain_day = 5\n").unwrap_err();
        assert!(err.to_string().contains("retain_day"), "{err}");
        assert!(
            parse("[[triggers]]\nname = \"t\"\non = [\"closed\"]\ncomand = [\"x\"]\n").is_err()
        );
    }

    #[test]
    fn bad_trigger_settings_are_rejected() {
        let t = |extra: &str| {
            parse(&format!(
                "[[triggers]]\nname = \"t\"\non = [\"closed\"]\ncommand = [\"x\"]\n{extra}"
            ))
        };
        assert!(t("").is_ok());
        assert!(t("min_severity = \"extrem\"").is_err());
        assert!(t("regions = [\"NSW\"]").is_err());
        assert!(
            parse("[[triggers]]\nname = \"t\"\non = [\"closd\"]\ncommand = [\"x\"]\n").is_err()
        );
        assert!(parse("[[triggers]]\nname = \"t\"\non = [\"closed\"]\ncommand = []\n").is_err());
    }

    #[test]
    fn bad_server_and_detect_settings_are_rejected() {
        assert!(parse("[server]\nbind = \"localhost\"\n").is_err());
        assert!(parse("[server]\npublic_url = \"nem.example.org\"\n").is_err());
        assert!(parse("[detect]\nregions = []\n").is_err());
        assert!(parse("[detect]\ncap_fraction = 1.5\n").is_err());
        assert!(parse("[detect]\nconfirm_intervals = 0\n").is_err());
        assert!(parse("[public]\nmin_severity = \"loud\"\n").is_err());
    }
}

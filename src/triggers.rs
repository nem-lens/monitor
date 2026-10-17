//! Operator triggers: run the rest of the pipeline (replay, counterfactuals,
//! report drafting) when an event changes state.
//!
//! Commands run directly, never through a shell. The notification item is
//! written to the command's stdin as JSON, and key fields are also passed as
//! NEM_* environment variables and as {placeholder} substitutions in argv.

use std::process::Stdio;
use std::time::Duration;

use tokio::io::AsyncWriteExt;

use crate::app::App;
use crate::config::TriggerCfg;
use crate::model::NotificationItem;
use crate::store;

pub fn matches(t: &TriggerCfg, item: &NotificationItem) -> bool {
    t.on.iter().any(|o| o == &item.transition)
        && crate::notify::severity_meets(&item.event, t.min_severity.as_deref())
        && t.regions
            .as_ref()
            .is_none_or(|r| r.contains(&item.event.region))
}

/// Fill `{placeholder}`s from internally generated fields only. `{bundle}` is
/// the event's bundle directory (written for closed events; complete at data_ready).
pub fn substitute(arg: &str, item: &NotificationItem, bundle: &str) -> String {
    arg.replace("{event_id}", &item.event_id)
        .replace("{bundle}", bundle)
        .replace("{region}", &item.event.region)
        .replace("{transition}", &item.transition)
        .replace("{data_tier}", &item.event.data_tier)
        .replace("{trading_day}", &item.event.trading_day)
        .replace("{severity}", item.event.severity.as_str())
        .replace("{start}", &item.event.start)
        .replace("{end}", item.event.end.as_deref().unwrap_or(""))
}

pub fn fire(app: &App, item: &NotificationItem) {
    for t in app.cfg.triggers.iter().filter(|t| matches(t, item)) {
        let a = app.clone();
        let t = t.clone();
        let it = item.clone();
        tokio::spawn(async move {
            let _permit = a.trigger_sem.clone().acquire_owned().await;
            run(&a, &t, &it).await;
        });
    }
}

async fn run(app: &App, t: &TriggerCfg, item: &NotificationItem) {
    let run_id = app
        .with_db(|c| store::start_run(c, &t.name, &item.event_id, &item.transition))
        .ok();
    let (code, tail) = if let Some(argv) = &t.command {
        let bundle = crate::bundle::dir_for(&app.cfg.store.bundle_dir, &item.event_id);
        run_command(argv, t.timeout_secs, item, &bundle.to_string_lossy()).await
    } else if let Some(url) = &t.webhook {
        run_webhook(app, url, t.secret_env.as_deref(), item).await
    } else {
        (None, "trigger has no action".into())
    };
    tracing::info!(trigger = %t.name, event = %item.event_id, ?code, "trigger finished");
    if let Some(id) = run_id {
        let _ = app.with_db(|c| store::finish_run(c, id, code, &tail));
    }
}

fn tail(s: &str, n: usize) -> String {
    let chars: Vec<char> = s.chars().collect();
    chars[chars.len().saturating_sub(n)..].iter().collect()
}

pub async fn run_command(
    argv: &[String],
    timeout_secs: u64,
    item: &NotificationItem,
    bundle: &str,
) -> (Option<i32>, String) {
    let Some((prog, args)) = argv.split_first() else {
        return (None, "empty command".into());
    };
    let args: Vec<String> = args.iter().map(|a| substitute(a, item, bundle)).collect();
    let mut cmd = tokio::process::Command::new(prog);
    cmd.args(&args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .env("NEM_EVENT_ID", &item.event_id)
        .env("NEM_REGION", &item.event.region)
        .env("NEM_TRANSITION", &item.transition)
        .env("NEM_DATA_TIER", &item.event.data_tier)
        .env("NEM_TRADING_DAY", &item.event.trading_day)
        .env("NEM_SEVERITY", item.event.severity.as_str())
        .env("NEM_EVENT_START", &item.event.start)
        .env("NEM_EVENT_END", item.event.end.as_deref().unwrap_or(""))
        .env("NEM_BUNDLE", bundle);
    let mut child = match cmd.spawn() {
        Ok(c) => c,
        Err(e) => return (None, format!("spawn failed: {e}")),
    };
    // Write stdin alongside the wait, inside the timeout: a child that never
    // reads stdin would otherwise block the write, and with it the timeout.
    let writer = child.stdin.take().map(|mut stdin| {
        let json = serde_json::to_vec(item).unwrap_or_default();
        tokio::spawn(async move {
            let _ = stdin.write_all(&json).await;
        })
    });
    let result =
        tokio::time::timeout(Duration::from_secs(timeout_secs), child.wait_with_output()).await;
    if let Some(w) = writer {
        // The child has exited or been killed, so the pipe is closed and the write ends.
        w.abort();
    }
    match result {
        Ok(Ok(out)) => {
            let text = format!(
                "{}{}",
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
            (out.status.code(), tail(&text, 4000))
        }
        Ok(Err(e)) => (None, format!("wait failed: {e}")),
        Err(_) => (None, format!("timed out after {timeout_secs}s (killed)")),
    }
}

async fn run_webhook(
    app: &App,
    url: &str,
    secret_env: Option<&str>,
    item: &NotificationItem,
) -> (Option<i32>, String) {
    let body = serde_json::to_string(item).unwrap_or_default();
    let ts = chrono::Utc::now().timestamp();
    let mut req = app
        .http
        .post(url)
        .header("content-type", "application/json")
        .header("x-nem-timestamp", ts.to_string());
    if let Some(secret) = secret_env.and_then(|k| std::env::var(k).ok()) {
        req = req.header(
            "x-nem-signature",
            format!("sha256={}", crate::webhooks::sign(&secret, ts, &body)),
        );
    }
    match req.body(body).send().await {
        Ok(r) => (
            Some(r.status().as_u16() as i32),
            tail(&r.text().await.unwrap_or_default(), 2000),
        ),
        Err(e) => (None, e.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::*;

    fn item() -> NotificationItem {
        let ev: Event = serde_json::from_value(serde_json::json!({
            "id": "202610121410-NSW1-high", "region": "NSW1", "kind": "high_price", "status": "closed",
            "severity": "cap", "trading_day": "2026-10-12", "start": "2026-10-12T14:10:00+10:00",
            "last_active": "2026-10-12T15:20:00+10:00", "end": "2026-10-12T15:20:00+10:00",
            "peak_price": 23107.0, "peak_interval": "2026-10-12T14:20:00+10:00", "intervals_active": 15,
            "intervals_above_high": 11, "intervals_above_extreme": 5, "mean_price": 8000.0, "min_price": 400.0,
            "flags": [], "constraints": [], "interconnectors_at_limit": [], "demand_start": null,
            "demand_peak": null, "data_tier": "live", "updated_at": "x"
        }))
        .unwrap();
        NotificationItem {
            seq: 1,
            event_id: ev.id.clone(),
            transition: "closed".into(),
            title: "t".into(),
            summary: "s".into(),
            url: "u".into(),
            created_at: "c".into(),
            public: true,
            event: ev,
        }
    }

    fn argv(parts: &[&str]) -> Vec<String> {
        parts.iter().map(|s| s.to_string()).collect()
    }

    /// Echoes the {event_id} placeholder and NEM_REGION, then the start of stdin.
    #[cfg(unix)]
    fn echo_cmd() -> Vec<String> {
        argv(&[
            "sh",
            "-c",
            "echo arg=$0 env=$NEM_REGION; head -c 20",
            "{event_id}",
        ])
    }

    #[cfg(windows)]
    fn echo_cmd() -> Vec<String> {
        argv(&[
            "powershell",
            "-NoProfile",
            "-Command",
            r#"Write-Output "arg={event_id} env=$env:NEM_REGION"; $input | Select-Object -First 1"#,
        ])
    }

    /// Runs for about `secs` seconds and never reads stdin.
    #[cfg(unix)]
    fn sleep_cmd(secs: u32) -> Vec<String> {
        argv(&["sleep", &secs.to_string()])
    }

    #[cfg(windows)]
    fn sleep_cmd(secs: u32) -> Vec<String> {
        argv(&["ping", "-n", &(secs + 1).to_string(), "127.0.0.1"])
    }

    #[tokio::test]
    async fn runs_command_with_stdin_env_and_placeholders() {
        let (code, out) = run_command(&echo_cmd(), 30, &item(), "bundles/x").await;
        assert_eq!(code, Some(0), "{out}");
        assert!(out.contains("arg=202610121410-NSW1-high env=NSW1"), "{out}");
        assert!(out.contains("{\"seq\":1"), "{out}");
    }

    #[tokio::test]
    async fn kills_on_timeout() {
        let (code, out) = run_command(&sleep_cmd(5), 1, &item(), "").await;
        assert_eq!(code, None);
        assert!(out.contains("timed out"), "{out}");
    }

    /// A payload larger than the pipe buffer, sent to a command that never
    /// reads stdin, must not stop the timeout from applying.
    #[tokio::test]
    async fn timeout_applies_when_stdin_is_never_read() {
        let mut big = item();
        big.summary = "x".repeat(3 << 20);
        let started = std::time::Instant::now();
        let res = tokio::time::timeout(
            Duration::from_secs(20),
            run_command(&sleep_cmd(30), 2, &big, ""),
        )
        .await;
        let (code, out) = res.expect("run_command hung past its own timeout");
        assert_eq!(code, None);
        assert!(out.contains("timed out"), "{out}");
        assert!(started.elapsed() < Duration::from_secs(10));
    }

    #[test]
    fn placeholders_include_the_bundle() {
        let arg = "--bundle={bundle} --out {bundle}/replay --event {event_id} {trading_day}";
        assert_eq!(
            substitute(arg, &item(), "bundles/202610121410-NSW1-high"),
            "--bundle=bundles/202610121410-NSW1-high --out bundles/202610121410-NSW1-high/replay --event 202610121410-NSW1-high 2026-10-12"
        );
    }

    #[test]
    fn matching() {
        let t = TriggerCfg {
            name: "x".into(),
            on: vec!["closed".into()],
            min_severity: Some("extreme".into()),
            regions: Some(vec!["NSW1".into()]),
            command: Some(vec!["true".into()]),
            webhook: None,
            secret_env: None,
            timeout_secs: 5,
        };
        assert!(matches(&t, &item()));
        let mut i = item();
        i.transition = "confirmed".into();
        assert!(!matches(&t, &i));
    }
}

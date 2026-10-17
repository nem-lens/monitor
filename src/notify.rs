//! Turns transitions into notification items and fans them out: store, live
//! stream, ntfy push, public webhooks and operator triggers.

use chrono::Utc;

use crate::app::{App, StreamMsg};
use crate::detect::money;
use crate::model::*;
use crate::store;
use crate::time::{local_date_label, local_label, region_name};

fn dur_label(ev: &Event) -> String {
    match (ev.start_time(), ev.end_time()) {
        (Some(s), Some(e)) => {
            // Times are interval ends, so the first interval began 5 minutes before `start`.
            let mins = (e - s).num_minutes() + 5;
            if mins >= 60 {
                let (h, m) = (mins / 60, mins % 60);
                if m == 0 {
                    format!("{h}h")
                } else {
                    format!("{h}h {m}m")
                }
            } else {
                format!("{mins} min")
            }
        }
        _ => String::new(),
    }
}

fn t_label(s: &str, region: &str) -> String {
    crate::time::parse_market(s)
        .map(|t| local_label(t, region))
        .unwrap_or_else(|| s.to_string())
}

fn flag_sentence(ev: &Event) -> String {
    let names: Vec<&str> = ev
        .flags
        .iter()
        .map(|f| match f.kind.as_str() {
            "whiplash" => "prices whipsawing",
            "price_separation" => "price separation from neighbouring regions",
            "demand_ramp" => "a fast demand ramp",
            other => other,
        })
        .collect();
    if names.is_empty() {
        String::new()
    } else {
        format!(" Also detected: {}.", names.join(", "))
    }
}

/// Deterministic, plain-English title and summary for a transition.
pub fn compose(kind: &TransitionKind, ev: &Event, cfg: &crate::config::Config) -> (String, String) {
    let r = region_name(&ev.region);
    let peak = money(ev.peak_price);
    let peak_at = t_label(&ev.peak_interval, &ev.region);
    let start = t_label(&ev.start, &ev.region);
    let hi = money(cfg.detect.high_price);
    let low = ev.kind == EventKind::LowPrice;
    match kind {
        TransitionKind::Opened => (
            format!("{r} price watch: ${peak}/MWh"),
            if low {
                format!("{r} spot price fell to ${peak}/MWh at {start}. Watching for a sustained event.")
            } else {
                format!("{r} spot price reached ${peak}/MWh at {start}. Watching for a sustained event.")
            },
        ),
        TransitionKind::Confirmed if low => (
            format!("{r} prices near the floor: ${peak}/MWh"),
            format!(
                "Spot prices in {r} fell to ${}/MWh or below from {start}, with {} intervals at that level. Lowest so far ${peak}/MWh at {peak_at}.{}",
                money(cfg.detect.floor_price),
                ev.intervals_above_high,
                flag_sentence(ev)
            ),
        ),
        TransitionKind::Confirmed => (
            format!("{r} price spike: ${peak}/MWh"),
            format!(
                "Spot prices in {r} rose above ${hi}/MWh from {start}. {} interval{} above ${hi}/MWh so far; peak ${peak}/MWh at {peak_at}.{}",
                ev.intervals_above_high,
                if ev.intervals_above_high == 1 { "" } else { "s" },
                flag_sentence(ev)
            ),
        ),
        TransitionKind::Escalated { to, .. } => (
            format!("{r} price spike escalates: ${peak}/MWh"),
            format!("{r} spot prices are now {}; peak ${peak}/MWh at {peak_at}.", to.label()),
        ),
        TransitionKind::Flagged { flag } => {
            let detail = ev.flags.iter().find(|f| &f.kind == flag).map(|f| f.detail.clone()).unwrap_or_default();
            let title = match flag.as_str() {
                "whiplash" => format!("{r} prices whipsawing"),
                other => format!("{r} event update: {other}"),
            };
            (title, format!("{detail}. Event peak so far ${peak}/MWh at {peak_at}."))
        }
        TransitionKind::Closed | TransitionKind::Discarded => {
            let end = ev.end.as_deref().map(|e| t_label(e, &ev.region)).unwrap_or_default();
            let date = ev.start_time().map(|t| local_date_label(t, &ev.region)).unwrap_or_default();
            let top = ev
                .constraints
                .first()
                .map(|c| format!(" Most often binding constraint: {} ({} intervals).", c.id, c.binding_intervals))
                .unwrap_or_default();
            let what = if low {
                format!("{} intervals at or below ${}/MWh", ev.intervals_above_high, money(cfg.detect.floor_price))
            } else {
                format!("{} intervals above ${hi}/MWh", ev.intervals_above_high)
            };
            (
                format!("{r} price event over: peak ${peak}/MWh"),
                format!(
                    "{what} between {start} and {end} on {date} ({}). Average price ${}/MWh over the event.{}{}",
                    dur_label(ev),
                    money(ev.mean_price),
                    flag_sentence(ev),
                    top
                ),
            )
        }
        TransitionKind::DataReady { .. } => {
            let date = ev.start_time().map(|t| local_date_label(t, &ev.region)).unwrap_or_default();
            (
                format!("Full data published for {r} event on {date}"),
                format!(
                    "AEMO's next-day bid and unit dispatch data for trading day {} are now public, so the {r} event (peak ${peak}/MWh) can be replayed in full.",
                    ev.trading_day
                ),
            )
        }
    }
}

/// Public channels get confirmed, escalated, flagged, closed and data_ready
/// transitions on events at or above the configured severity.
pub fn is_public(kind: &TransitionKind, ev: &Event, cfg: &crate::config::Config) -> bool {
    let kind_ok = matches!(
        kind,
        TransitionKind::Confirmed
            | TransitionKind::Escalated { .. }
            | TransitionKind::Flagged { .. }
            | TransitionKind::Closed
            | TransitionKind::DataReady { .. }
    );
    let sev_ok = match ev.kind {
        EventKind::LowPrice => cfg.public.include_low_price,
        EventKind::HighPrice => {
            ev.severity >= Severity::parse(&cfg.public.min_severity).unwrap_or(Severity::High)
        }
    };
    kind_ok && sev_ok
}

pub fn severity_meets(ev: &Event, min: Option<&str>) -> bool {
    match min.and_then(Severity::parse) {
        None => true,
        Some(m) => ev.severity >= m,
    }
}

pub async fn publish(app: &App, tr: Transition) {
    let (title, summary) = compose(&tr.kind, &tr.event, &app.cfg);
    let public = is_public(&tr.kind, &tr.event, &app.cfg);
    let mut item = NotificationItem {
        seq: 0,
        event_id: tr.event.id.clone(),
        transition: tr.kind.name().to_string(),
        title,
        summary,
        url: format!(
            "{}/?event={}",
            app.cfg.server.public_url.trim_end_matches('/'),
            tr.event.id
        ),
        created_at: Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        public,
        event: tr.event.clone(),
    };
    match app.with_db(|c| store::add_notification(c, &item)) {
        Ok(seq) => item.seq = seq,
        Err(e) => tracing::error!("storing notification: {e:#}"),
    }
    if app.echo {
        println!(
            "[{}] {:<10} {}{}\n             {}",
            tr.event.id,
            item.transition,
            item.title,
            if public { "" } else { "  (internal)" },
            item.summary
        );
    }
    tracing::info!(event = %item.event_id, transition = %item.transition, public, "{}", item.title);

    if public {
        let _ = app.tx.send(StreamMsg {
            kind: "notification",
            data: serde_json::to_value(&item).unwrap_or_default(),
        });
        if app.notify_external {
            let a = app.clone();
            let it = item.clone();
            tokio::spawn(async move { send_ntfy(&a, &it).await });
            crate::webhooks::fanout(app, &item);
        }
    }
    if app.fire_triggers {
        crate::triggers::fire(app, &item);
    }
}

async fn send_ntfy(app: &App, item: &NotificationItem) {
    let topic = app.cfg.public.ntfy_topic.trim();
    if topic.is_empty() {
        return;
    }
    let url = format!(
        "{}/{}",
        app.cfg.public.ntfy_server.trim_end_matches('/'),
        topic
    );
    let priority = match (item.transition.as_str(), item.event.severity) {
        (_, Severity::Cap) => "5",
        ("confirmed" | "escalated", _) => "4",
        _ => "3",
    };
    let res = app
        .http
        .post(&url)
        .header("Title", ascii_header(&item.title))
        .header("Priority", priority)
        .header("Tags", "zap")
        .header("Click", &item.url)
        .body(item.summary.clone())
        .send()
        .await;
    match res {
        Ok(r) if r.status().is_success() => {}
        Ok(r) => tracing::warn!("ntfy returned {}", r.status()),
        Err(e) => tracing::warn!("ntfy failed: {e}"),
    }
}

/// HTTP header values must be visible ASCII; ntfy titles are short anyway.
fn ascii_header(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii() && !c.is_ascii_control() {
                c
            } else {
                ' '
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::detect::Detector;
    use crate::time::parse_market;

    #[test]
    fn closed_summary_reads_well() {
        let cfg = Config::default();
        let mut d = Detector::new(cfg.detect.clone());
        let mut t = parse_market("2026/10/12 14:00:00").unwrap();
        let mut last = None;
        for p in [
            200.0, 6000.0, 23107.0, 7000.0, 300.0, 300.0, 300.0, 300.0, 300.0, 300.0,
        ] {
            t += chrono::Duration::minutes(5);
            let mut s = DispatchSnapshot {
                settlement: t,
                ..Default::default()
            };
            s.regions.insert(
                "NSW1".into(),
                RegionInterval {
                    rrp: p,
                    ..Default::default()
                },
            );
            for tr in d.ingest(&s) {
                last = Some(tr);
            }
        }
        let tr = last.unwrap();
        let (title, summary) = compose(&tr.kind, &tr.event, &cfg);
        assert_eq!(title, "NSW price event over: peak $23,107/MWh");
        assert!(summary.starts_with("3 intervals above $5,000/MWh between 3:10pm AEDT and 3:20pm AEDT on Mon 12 Oct 2026 (15 min)"), "{summary}");
        assert!(is_public(&tr.kind, &tr.event, &cfg));
    }
}

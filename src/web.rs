//! Public HTTP surface: live page, JSON API, feeds, live stream and webhook
//! subscriptions.

use std::convert::Infallible;
use std::net::{IpAddr, SocketAddr};

use axum::extract::{ConnectInfo, Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::{Html, IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Deserialize;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tower_http::cors::{Any, CorsLayer};

use crate::app::App;
use crate::feeds;
use crate::store::{self, EventFilter};
use crate::time::{local_label, parse_market, region_name};
use crate::webhooks::{self, ApiError, RegisterReq};

const INDEX: &str = include_str!("../static/index.html");

pub fn router(app: App) -> Router {
    let cors = CorsLayer::new().allow_origin(Any).allow_methods(Any).allow_headers(Any);
    Router::new()
        .route("/", get(index))
        .route("/healthz", get(healthz))
        .route("/api/status", get(status))
        .route("/api/events", get(events))
        .route("/api/events/{id}", get(event_detail))
        .route("/api/notifications", get(notifications))
        .route("/api/stream", get(stream))
        .route("/api/webhooks", post(register_webhook))
        .route("/api/webhooks/{id}", axum::routing::delete(delete_webhook))
        .route("/api/webhooks/{id}/test", post(test_webhook))
        .route("/api/admin/runs", get(admin_runs))
        .route("/feed.atom", get(atom))
        .route("/feed.json", get(json_feed))
        .layer(cors)
        .with_state(app)
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let code = StatusCode::from_u16(self.0).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        (code, Json(serde_json::json!({ "error": self.1 }))).into_response()
    }
}

fn internal(e: anyhow::Error) -> ApiError {
    tracing::error!("{e:#}");
    ApiError(500, "internal error".into())
}

async fn index(State(app): State<App>) -> Html<String> {
    Html(INDEX.replace("{{SITE_TITLE}}", &feeds::xml_escape(&app.cfg.public.site_title)))
}

async fn healthz(State(app): State<App>) -> Response {
    let h = app.health.read().map(|h| h.clone()).unwrap_or_default();
    let fresh = match h.mode.as_str() {
        "live" => h.last_poll_ok.is_some_and(|t| (chrono::Utc::now() - t).num_minutes() < 10),
        _ => true,
    };
    let code = if fresh { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };
    (code, Json(h)).into_response()
}

async fn status(State(app): State<App>) -> Result<Json<serde_json::Value>, ApiError> {
    let cfg = app.cfg.clone();
    let health = app.health.read().map(|h| h.clone()).unwrap_or_default();
    let (latest, regions, open) = app
        .with_db(|c| {
            let latest = store::latest_settlement(c)?;
            let open = store::list_events(c, &EventFilter { region: None, status: None, include_discarded: false, limit: 50 })?
                .into_iter()
                .filter(|e| e.end.is_none())
                .collect::<Vec<_>>();
            let mut regions = Vec::new();
            for r in &cfg.detect.regions {
                let series = store::recent_series(c, r, 36)?;
                let last = series.last().cloned();
                regions.push(serde_json::json!({
                    "region": r,
                    "name": region_name(r),
                    "rrp": last.as_ref().map(|p| p.rrp),
                    "demand": last.as_ref().and_then(|p| p.demand),
                    "local_time": last.as_ref().and_then(|p| parse_market(&p.t)).map(|t| local_label(t, r)),
                    "series": series.iter().map(|p| p.rrp).collect::<Vec<_>>(),
                    "open_event": open.iter().find(|e| &e.region == r).map(|e| e.id.clone()),
                }));
            }
            Ok((latest, regions, open))
        })
        .map_err(internal)?;
    let base = cfg.server.public_url.trim_end_matches('/');
    let ntfy = if cfg.public.ntfy_topic.trim().is_empty() {
        serde_json::Value::Null
    } else {
        serde_json::json!({
            "server": cfg.public.ntfy_server,
            "topic": cfg.public.ntfy_topic,
            "url": format!("{}/{}", cfg.public.ntfy_server.trim_end_matches('/'), cfg.public.ntfy_topic),
        })
    };
    Ok(Json(serde_json::json!({
        "site_title": cfg.public.site_title,
        "mode": health.mode,
        "latest_interval": latest,
        "health": health,
        "regions": regions,
        "open_events": open,
        "thresholds": {
            "high_price": cfg.detect.high_price,
            "extreme_price": cfg.detect.extreme_price,
            "market_price_cap": cfg.detect.market_price_cap,
            "floor_price": cfg.detect.floor_price,
            "confirm_intervals": cfg.detect.confirm_intervals,
            "public_min_severity": cfg.public.min_severity,
        },
        "subscribe": {
            "atom": format!("{base}/feed.atom"),
            "json_feed": format!("{base}/feed.json"),
            "sse": format!("{base}/api/stream"),
            "ntfy": ntfy,
            "webhooks": format!("{base}/api/webhooks"),
        },
    })))
}

#[derive(Deserialize)]
struct EventsQuery {
    region: Option<String>,
    status: Option<String>,
    limit: Option<usize>,
}

async fn events(State(app): State<App>, Query(q): Query<EventsQuery>) -> Result<Json<serde_json::Value>, ApiError> {
    let list = app
        .with_db(|c| {
            store::list_events(
                c,
                &EventFilter {
                    region: q.region.as_deref(),
                    status: q.status.as_deref(),
                    include_discarded: false,
                    limit: q.limit.unwrap_or(50),
                },
            )
        })
        .map_err(internal)?;
    Ok(Json(serde_json::json!({ "events": list })))
}

async fn event_detail(State(app): State<App>, Path(id): Path<String>) -> Result<Json<serde_json::Value>, ApiError> {
    let out = app
        .with_db(|c| {
            let Some(ev) = store::get_event(c, &id)? else { return Ok(None) };
            if ev.status == crate::model::Status::Discarded {
                return Ok(None);
            }
            let start = ev.start_time().unwrap_or_default() - chrono::Duration::hours(1);
            let end = ev.end_time().or_else(|| ev.last_active_time()).unwrap_or_default() + chrono::Duration::hours(1);
            let series = store::series(c, &ev.region, &crate::time::iso_market(start), &crate::time::iso_market(end))?;
            let notes = store::notifications_for_event(c, &id, true)?;
            Ok(Some(serde_json::json!({ "event": ev, "series": series, "notifications": notes })))
        })
        .map_err(internal)?;
    out.map(Json).ok_or_else(|| ApiError(404, "event not found".into()))
}

#[derive(Deserialize)]
struct NotesQuery {
    since: Option<i64>,
    limit: Option<usize>,
}

async fn notifications(State(app): State<App>, Query(q): Query<NotesQuery>) -> Result<Json<serde_json::Value>, ApiError> {
    let items = app
        .with_db(|c| store::list_notifications(c, true, q.since.unwrap_or(0), q.limit.unwrap_or(50)))
        .map_err(internal)?;
    Ok(Json(serde_json::json!({ "notifications": items })))
}

async fn stream(State(app): State<App>) -> Sse<impl futures_util::Stream<Item = Result<SseEvent, Infallible>>> {
    let rx = app.tx.subscribe();
    let s = BroadcastStream::new(rx).filter_map(|m| {
        m.ok().map(|msg| Ok(SseEvent::default().event(msg.kind).json_data(msg.data).unwrap_or_default()))
    });
    Sse::new(s).keep_alive(KeepAlive::default())
}

async fn atom(State(app): State<App>) -> Result<Response, ApiError> {
    let items = app.with_db(|c| store::list_notifications(c, true, 0, app.cfg.public.feed_items)).map_err(internal)?;
    Ok(([(header::CONTENT_TYPE, "application/atom+xml; charset=utf-8")], feeds::atom(&items, &app.cfg)).into_response())
}

async fn json_feed(State(app): State<App>) -> Result<Response, ApiError> {
    let items = app.with_db(|c| store::list_notifications(c, true, 0, app.cfg.public.feed_items)).map_err(internal)?;
    Ok(([(header::CONTENT_TYPE, "application/feed+json; charset=utf-8")], Json(feeds::json_feed(&items, &app.cfg)))
        .into_response())
}

fn client_ip(app: &App, headers: &HeaderMap, peer: SocketAddr) -> IpAddr {
    if app.cfg.server.trust_proxy_headers {
        if let Some(ip) = headers
            .get("x-forwarded-for")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.split(',').next())
            .and_then(|v| v.trim().parse().ok())
        {
            return ip;
        }
    }
    peer.ip()
}

fn bearer(headers: &HeaderMap) -> Option<String> {
    headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(|s| s.trim().to_string())
}

async fn register_webhook(
    State(app): State<App>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    headers: HeaderMap,
    Json(req): Json<RegisterReq>,
) -> Result<(StatusCode, Json<webhooks::RegisterResp>), ApiError> {
    let ip = client_ip(&app, &headers, peer);
    let resp = webhooks::register(&app, req, ip).await?;
    Ok((StatusCode::CREATED, Json(resp)))
}

async fn delete_webhook(State(app): State<App>, Path(id): Path<String>, headers: HeaderMap) -> Result<StatusCode, ApiError> {
    let token = bearer(&headers).ok_or_else(|| ApiError(401, "missing bearer token".into()))?;
    webhooks::authorise(&app, &id, &token)?;
    app.with_db(|c| store::delete_webhook(c, &id)).map_err(internal)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn test_webhook(State(app): State<App>, Path(id): Path<String>, headers: HeaderMap) -> Result<Json<serde_json::Value>, ApiError> {
    let token = bearer(&headers).ok_or_else(|| ApiError(401, "missing bearer token".into()))?;
    webhooks::authorise(&app, &id, &token)?;
    let hook = app
        .with_db(|c| store::get_webhook(c, &id))
        .map_err(internal)?
        .ok_or_else(|| ApiError(409, "subscription is disabled".into()))?;
    let latest = app.with_db(|c| store::list_notifications(c, true, 0, 1)).map_err(internal)?;
    let item = latest.into_iter().next().ok_or_else(|| ApiError(409, "no notifications yet to send as a test".into()))?;
    match webhooks::deliver(&app, &hook, &item, "test").await {
        Ok(()) => Ok(Json(serde_json::json!({ "delivered": true }))),
        Err(e) => Err(ApiError(502, format!("delivery failed: {e}"))),
    }
}

async fn admin_runs(State(app): State<App>, headers: HeaderMap) -> Result<Json<serde_json::Value>, ApiError> {
    let expected = std::env::var("NEM_ADMIN_TOKEN").ok().filter(|s| s.len() >= 16);
    match (expected, bearer(&headers)) {
        (Some(e), Some(t)) if webhooks::sha256_hex(&e) == webhooks::sha256_hex(&t) => {}
        _ => return Err(ApiError(404, "not found".into())),
    }
    let runs = app.with_db(|c| store::list_runs(c, 100)).map_err(internal)?;
    Ok(Json(serde_json::json!({ "runs": runs })))
}

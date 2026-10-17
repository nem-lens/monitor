//! Public HTTP surface: live page, JSON API, feeds, live stream and webhook
//! subscriptions.

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::{IpAddr, SocketAddr};
use std::time::{Duration, Instant};

use axum::extract::{ConnectInfo, Path, Query, Request, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::middleware::Next;
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
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);
    let mut r = Router::new()
        .route("/", get(index))
        .route("/healthz", get(healthz))
        .route("/api/status", get(status))
        .route("/api/events", get(events))
        .route("/api/events/{id}", get(event_detail))
        .route("/api/notifications", get(notifications))
        .route("/api/stream", get(stream))
        .route("/api/admin/runs", get(admin_runs))
        .route("/feed.atom", get(atom))
        .route("/feed.json", get(json_feed));
    if app.cfg.public.webhooks_enabled {
        r = r
            .route("/api/webhooks", post(register_webhook))
            .route("/api/webhooks/{id}", axum::routing::delete(delete_webhook))
            .route("/api/webhooks/{id}/test", post(test_webhook));
    }
    r.layer(axum::middleware::from_fn_with_state(
        app.clone(),
        rate_limit,
    ))
    .layer(cors)
    .with_state(app)
}

const RATE_WINDOW: Duration = Duration::from_secs(60);
/// Clients tracked at once; when full of current entries, new clients are
/// refused, which bounds memory.
const MAX_TRACKED_CLIENTS: usize = 10_000;

/// Per-IP fixed-window limit over every route except `/healthz`.
async fn rate_limit(
    State(app): State<App>,
    ConnectInfo(peer): ConnectInfo<SocketAddr>,
    req: Request,
    next: Next,
) -> Response {
    let limit = app.cfg.public.api_requests_per_ip_per_minute;
    if limit == 0 || req.uri().path() == "/healthz" {
        return next.run(req).await;
    }
    let ip = client_ip(&app, req.headers(), peer);
    let ok = app
        .api_limiter
        .lock()
        .map(|mut m| allow_request(&mut m, ip, Instant::now(), limit))
        .unwrap_or(false);
    if !ok {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            [(header::RETRY_AFTER, "60")],
            Json(serde_json::json!({ "error": "too many requests; try again in a minute" })),
        )
            .into_response();
    }
    next.run(req).await
}

fn allow_request(
    map: &mut HashMap<IpAddr, (Instant, u32)>,
    ip: IpAddr,
    now: Instant,
    limit: u32,
) -> bool {
    let current = |start: &Instant| now.saturating_duration_since(*start) < RATE_WINDOW;
    if map.len() >= MAX_TRACKED_CLIENTS && !map.contains_key(&ip) {
        map.retain(|_, (start, _)| current(start));
        if map.len() >= MAX_TRACKED_CLIENTS {
            return false;
        }
    }
    let entry = map.entry(ip).or_insert((now, 0));
    if !current(&entry.0) {
        *entry = (now, 0);
    }
    if entry.1 >= limit {
        return false;
    }
    entry.1 += 1;
    true
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
    Html(INDEX.replace(
        "{{SITE_TITLE}}",
        &feeds::xml_escape(&app.cfg.public.site_title),
    ))
}

async fn healthz(State(app): State<App>) -> Response {
    let h = app.health.read().map(|h| h.clone()).unwrap_or_default();
    let code = if h.is_fresh(chrono::Utc::now()) {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
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
        "fresh": health.is_fresh(chrono::Utc::now()),
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
            "webhooks": cfg.public.webhooks_enabled.then(|| format!("{base}/api/webhooks")),
        },
    })))
}

#[derive(Deserialize)]
struct EventsQuery {
    region: Option<String>,
    status: Option<String>,
    limit: Option<usize>,
}

async fn events(
    State(app): State<App>,
    Query(q): Query<EventsQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
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

async fn event_detail(
    State(app): State<App>,
    Path(id): Path<String>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let out = app
        .with_db(|c| {
            let Some(ev) = store::get_event(c, &id)? else {
                return Ok(None);
            };
            if ev.status == crate::model::Status::Discarded {
                return Ok(None);
            }
            let start = ev.start_time().unwrap_or_default() - chrono::Duration::hours(1);
            let end = ev
                .end_time()
                .or_else(|| ev.last_active_time())
                .unwrap_or_default()
                + chrono::Duration::hours(1);
            // A written bundle is the record once the event closes (the buffer
            // is pruned after 48 h); while the event is open, use the buffer.
            let bundle = store::get_bundle(c, &id)?.filter(|b| b.status != "pending");
            let from_bundle = bundle.as_ref().and_then(|b| {
                crate::bundle::read_series(std::path::Path::new(&b.path), &ev.region)
                    .map_err(|e| tracing::warn!(event = %id, "reading bundle: {e:#}"))
                    .ok()
            });
            let (series, source) = match from_bundle {
                Some(s) => (s, "bundle"),
                None => (
                    store::series(
                        c,
                        &ev.region,
                        &crate::time::iso_market(start),
                        &crate::time::iso_market(end),
                    )?,
                    "buffer",
                ),
            };
            let notes = store::notifications_for_event(c, &id, true)?;
            Ok(Some(serde_json::json!({
                "event": ev,
                "series": series,
                "series_source": source,
                "bundle": bundle.map(|b| serde_json::json!({ "status": b.status, "window_start": b.window_start, "window_end": b.window_end })),
                "notifications": notes,
            })))
        })
        .map_err(internal)?;
    out.map(Json)
        .ok_or_else(|| ApiError(404, "event not found".into()))
}

#[derive(Deserialize)]
struct NotesQuery {
    since: Option<i64>,
    limit: Option<usize>,
}

async fn notifications(
    State(app): State<App>,
    Query(q): Query<NotesQuery>,
) -> Result<Json<serde_json::Value>, ApiError> {
    let items = app
        .with_db(|c| {
            store::list_notifications(c, true, q.since.unwrap_or(0), q.limit.unwrap_or(50))
        })
        .map_err(internal)?;
    Ok(Json(serde_json::json!({ "notifications": items })))
}

async fn stream(
    State(app): State<App>,
) -> Result<Sse<impl futures_util::Stream<Item = Result<SseEvent, Infallible>>>, ApiError> {
    // The permit lives as long as the stream, so it is released on disconnect.
    let slot = app.sse_slots.clone().try_acquire_owned().map_err(|_| {
        ApiError(
            503,
            "live stream is at capacity; use the feeds or try again later".into(),
        )
    })?;
    let rx = app.tx.subscribe();
    let s = BroadcastStream::new(rx).filter_map(move |m| {
        let _slot = &slot;
        m.ok().map(|msg| {
            Ok(SseEvent::default()
                .event(msg.kind)
                .json_data(msg.data)
                .unwrap_or_default())
        })
    });
    Ok(Sse::new(s).keep_alive(KeepAlive::default()))
}

async fn atom(State(app): State<App>) -> Result<Response, ApiError> {
    let items = app
        .with_db(|c| store::list_notifications(c, true, 0, app.cfg.public.feed_items))
        .map_err(internal)?;
    Ok((
        [(header::CONTENT_TYPE, "application/atom+xml; charset=utf-8")],
        feeds::atom(&items, &app.cfg),
    )
        .into_response())
}

async fn json_feed(State(app): State<App>) -> Result<Response, ApiError> {
    let items = app
        .with_db(|c| store::list_notifications(c, true, 0, app.cfg.public.feed_items))
        .map_err(internal)?;
    Ok((
        [(header::CONTENT_TYPE, "application/feed+json; charset=utf-8")],
        Json(feeds::json_feed(&items, &app.cfg)),
    )
        .into_response())
}

/// The client address. Behind a trusted proxy, use the LAST X-Forwarded-For
/// entry: that is the one our proxy appended, while earlier entries come from
/// the client and can be forged.
fn client_ip(app: &App, headers: &HeaderMap, peer: SocketAddr) -> IpAddr {
    if app.cfg.server.trust_proxy_headers {
        if let Some(ip) = headers
            .get("x-forwarded-for")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.rsplit(',').next())
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

async fn delete_webhook(
    State(app): State<App>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<StatusCode, ApiError> {
    let token = bearer(&headers).ok_or_else(|| ApiError(401, "missing bearer token".into()))?;
    webhooks::authorise(&app, &id, &token)?;
    app.with_db(|c| store::delete_webhook(c, &id))
        .map_err(internal)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn test_webhook(
    State(app): State<App>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    let token = bearer(&headers).ok_or_else(|| ApiError(401, "missing bearer token".into()))?;
    webhooks::authorise(&app, &id, &token)?;
    let hook = app
        .with_db(|c| store::get_webhook(c, &id))
        .map_err(internal)?
        .ok_or_else(|| ApiError(409, "subscription is disabled".into()))?;
    let latest = app
        .with_db(|c| store::list_notifications(c, true, 0, 1))
        .map_err(internal)?;
    let item = latest
        .into_iter()
        .next()
        .ok_or_else(|| ApiError(409, "no notifications yet to send as a test".into()))?;
    match webhooks::deliver(&app, &hook, &item, "test").await {
        Ok(()) => Ok(Json(serde_json::json!({ "delivered": true }))),
        Err(e) => Err(ApiError(502, format!("delivery failed: {e}"))),
    }
}

async fn admin_runs(
    State(app): State<App>,
    headers: HeaderMap,
) -> Result<Json<serde_json::Value>, ApiError> {
    let expected = std::env::var("NEM_ADMIN_TOKEN")
        .ok()
        .filter(|s| s.len() >= 16);
    match (expected, bearer(&headers)) {
        (Some(e), Some(t)) if webhooks::sha256_hex(&e) == webhooks::sha256_hex(&t) => {}
        _ => return Err(ApiError(404, "not found".into())),
    }
    let runs = app
        .with_db(|c| store::list_runs(c, 100))
        .map_err(internal)?;
    Ok(Json(serde_json::json!({ "runs": runs })))
}

#[cfg(test)]
mod tests {
    use axum::body::Body;
    use axum::extract::connect_info::MockConnectInfo;
    use axum::http::Request;
    use tower::ServiceExt;

    use super::*;
    use crate::config::Config;

    fn app_with(cfg: Config, mode: &str) -> App {
        App::new(cfg, store::open(":memory:").unwrap(), mode).unwrap()
    }

    async fn call(app: &App, req: Request<Body>) -> (StatusCode, HeaderMap, String) {
        let peer: SocketAddr = "203.0.113.9:5000".parse().unwrap();
        let resp = router(app.clone())
            .layer(MockConnectInfo(peer))
            .oneshot(req)
            .await
            .unwrap();
        let (parts, body) = resp.into_parts();
        let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        (
            parts.status,
            parts.headers,
            String::from_utf8_lossy(&bytes).into(),
        )
    }

    fn get(uri: &str) -> Request<Body> {
        Request::get(uri).body(Body::empty()).unwrap()
    }

    #[tokio::test]
    async fn healthz_reflects_mode_and_freshness() {
        let offline = app_with(Config::default(), "offline");
        assert_eq!(call(&offline, get("/healthz")).await.0, StatusCode::OK);
        // Live with no successful poll yet.
        let live = app_with(Config::default(), "live");
        assert_eq!(
            call(&live, get("/healthz")).await.0,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn json_api_shapes_and_errors() {
        let app = app_with(Config::default(), "offline");
        let (st, h, body) = call(&app, get("/api/status")).await;
        assert_eq!(st, StatusCode::OK);
        assert!(h[header::CONTENT_TYPE]
            .to_str()
            .unwrap()
            .starts_with("application/json"));
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(v["regions"].as_array().unwrap().len(), 5);
        assert_eq!(v["fresh"], true);

        let (st, _, body) = call(&app, get("/api/events/nope")).await;
        assert_eq!(st, StatusCode::NOT_FOUND);
        assert!(body.contains("event not found"));
        let (st, _, body) = call(&app, get("/api/events?limit=100000")).await;
        assert_eq!(st, StatusCode::OK);
        assert_eq!(body, r#"{"events":[]}"#);
    }

    #[tokio::test]
    async fn feeds_and_page_have_content_types_and_escaping() {
        let mut cfg = Config::default();
        cfg.public.site_title = "NEM <Watch> & Co".into();
        let app = app_with(cfg, "offline");
        let (st, h, body) = call(&app, get("/feed.atom")).await;
        assert_eq!(st, StatusCode::OK);
        assert!(h[header::CONTENT_TYPE]
            .to_str()
            .unwrap()
            .starts_with("application/atom+xml"));
        assert!(body.contains("<title>NEM &lt;Watch&gt; &amp; Co</title>"));
        let (_, h, _) = call(&app, get("/feed.json")).await;
        assert!(h[header::CONTENT_TYPE]
            .to_str()
            .unwrap()
            .starts_with("application/feed+json"));
        let (_, _, page) = call(&app, get("/")).await;
        assert!(!page.contains("{{SITE_TITLE}}"));
        assert!(page.contains("NEM &lt;Watch&gt; &amp; Co"));
    }

    #[tokio::test]
    async fn cors_is_open_for_reads() {
        let app = app_with(Config::default(), "offline");
        let req = Request::get("/api/events")
            .header("origin", "https://elsewhere.example")
            .body(Body::empty())
            .unwrap();
        let (_, h, _) = call(&app, req).await;
        assert_eq!(h["access-control-allow-origin"], "*");
    }

    #[tokio::test]
    async fn admin_and_webhook_management_need_tokens() {
        let app = app_with(Config::default(), "offline");
        assert_eq!(
            call(&app, get("/api/admin/runs")).await.0,
            StatusCode::NOT_FOUND
        );
        let del = Request::delete("/api/webhooks/wh_x")
            .body(Body::empty())
            .unwrap();
        assert_eq!(call(&app, del).await.0, StatusCode::UNAUTHORIZED);
        let del = Request::delete("/api/webhooks/wh_x")
            .header("authorization", "Bearer mt_wrong")
            .body(Body::empty())
            .unwrap();
        assert_eq!(call(&app, del).await.0, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn webhook_registration_is_refused_when_capacity_is_zero() {
        let mut cfg = Config::default();
        cfg.public.max_webhooks = 0;
        let app = app_with(cfg, "offline");
        let req = Request::post("/api/webhooks")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"url":"https://example.org/hook"}"#))
            .unwrap();
        let (st, _, body) = call(&app, req).await;
        assert_eq!(st, StatusCode::SERVICE_UNAVAILABLE, "{body}");
    }

    #[tokio::test]
    async fn webhooks_can_be_switched_off() {
        let mut cfg = Config::default();
        cfg.public.webhooks_enabled = false;
        let app = app_with(cfg, "offline");
        let req = Request::post("/api/webhooks")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"url":"https://example.org/hook"}"#))
            .unwrap();
        assert_eq!(call(&app, req).await.0, StatusCode::NOT_FOUND);
        let del = Request::delete("/api/webhooks/wh_x")
            .header("authorization", "Bearer mt_x")
            .body(Body::empty())
            .unwrap();
        assert_eq!(call(&app, del).await.0, StatusCode::NOT_FOUND);
        let (_, _, body) = call(&app, get("/api/status")).await;
        let v: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert!(v["subscribe"]["webhooks"].is_null());
        assert!(v["subscribe"]["atom"].is_string());
    }

    #[tokio::test]
    async fn live_stream_connections_are_capped() {
        let mut cfg = Config::default();
        cfg.public.max_sse_connections = 1;
        let app = app_with(cfg, "offline");
        let open = || {
            let peer: SocketAddr = "203.0.113.9:5000".parse().unwrap();
            router(app.clone())
                .layer(MockConnectInfo(peer))
                .oneshot(get("/api/stream"))
        };
        let first = open().await.unwrap();
        assert_eq!(first.status(), StatusCode::OK);
        assert_eq!(
            open().await.unwrap().status(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        drop(first);
        assert_eq!(
            open().await.unwrap().status(),
            StatusCode::OK,
            "slot released on disconnect"
        );
    }

    #[tokio::test]
    async fn api_requests_are_rate_limited_per_ip() {
        let mut cfg = Config::default();
        cfg.public.api_requests_per_ip_per_minute = 3;
        let app = app_with(cfg, "offline");
        for _ in 0..3 {
            assert_eq!(call(&app, get("/api/events")).await.0, StatusCode::OK);
        }
        let (st, h, _) = call(&app, get("/feed.json")).await;
        assert_eq!(st, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(h[header::RETRY_AFTER], "60");
        assert_eq!(
            call(&app, get("/healthz")).await.0,
            StatusCode::OK,
            "health checks exempt"
        );
    }

    #[test]
    fn rate_window_resets_and_memory_is_bounded() {
        let mut map = HashMap::new();
        let ip: IpAddr = "203.0.113.1".parse().unwrap();
        let t0 = Instant::now();
        assert!(allow_request(&mut map, ip, t0, 2));
        assert!(allow_request(&mut map, ip, t0, 2));
        assert!(!allow_request(&mut map, ip, t0, 2));
        assert!(
            allow_request(&mut map, ip, t0 + RATE_WINDOW, 2),
            "new minute"
        );

        let mut full: HashMap<IpAddr, (Instant, u32)> = (0..MAX_TRACKED_CLIENTS as u32)
            .map(|i| (IpAddr::from(std::net::Ipv4Addr::from(i)), (t0, 1)))
            .collect();
        assert!(
            !allow_request(&mut full, ip, t0, 2),
            "full of current clients"
        );
        assert!(
            allow_request(&mut full, ip, t0 + RATE_WINDOW, 2),
            "stale entries pruned"
        );
        assert_eq!(full.len(), 1);
    }

    #[test]
    fn client_ip_uses_the_last_forwarded_hop_only_when_trusted() {
        let peer: SocketAddr = "10.0.0.2:1234".parse().unwrap();
        let mut headers = HeaderMap::new();
        headers.insert("x-forwarded-for", "6.6.6.6, 203.0.113.7".parse().unwrap());
        let untrusted = app_with(Config::default(), "offline");
        assert_eq!(
            client_ip(&untrusted, &headers, peer).to_string(),
            "10.0.0.2"
        );
        let mut cfg = Config::default();
        cfg.server.trust_proxy_headers = true;
        let trusted = app_with(cfg, "offline");
        assert_eq!(
            client_ip(&trusted, &headers, peer).to_string(),
            "203.0.113.7"
        );
    }
}

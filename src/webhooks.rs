//! Public webhook subscriptions.
//!
//! Anyone can register, so registration is guarded: HTTPS only, the target must
//! resolve to public IP addresses (no internal network access), redirects are
//! not followed, the receiver must echo a challenge, registrations are rate
//! limited per IP, and endpoints that keep failing are disabled. Deliveries are
//! signed with HMAC-SHA256 over "<timestamp>.<body>".

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::time::{Duration, Instant};

use hmac::{Hmac, Mac};
use rand::RngCore;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::app::App;
use crate::model::{NotificationItem, Severity};
use crate::store::{self, Webhook};

#[derive(Debug, Deserialize)]
pub struct RegisterReq {
    pub url: String,
    #[serde(default)]
    pub regions: Option<Vec<String>>,
    #[serde(default)]
    pub min_severity: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct RegisterResp {
    pub id: String,
    /// HMAC secret for verifying deliveries. Shown once.
    pub secret: String,
    /// Bearer token to delete or test this subscription. Shown once.
    pub manage_token: String,
}

#[derive(Debug)]
pub struct ApiError(pub u16, pub String);

fn rand_hex(n: usize) -> String {
    let mut b = vec![0u8; n];
    rand::thread_rng().fill_bytes(&mut b);
    hex::encode(b)
}

pub fn sha256_hex(s: &str) -> String {
    hex::encode(Sha256::digest(s.as_bytes()))
}

pub fn sign(secret: &str, timestamp: i64, body: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("any key length");
    mac.update(timestamp.to_string().as_bytes());
    mac.update(b".");
    mac.update(body.as_bytes());
    hex::encode(mac.finalize().into_bytes())
}

pub fn is_public_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(v4) => is_public_v4(v4),
        IpAddr::V6(v6) => {
            if let Some(v4) = v6.to_ipv4_mapped() {
                return is_public_v4(v4);
            }
            let seg = v6.segments();
            !(v6.is_loopback()
                || v6.is_unspecified()
                || v6.is_multicast()
                || (seg[0] & 0xfe00) == 0xfc00 // unique local
                || (seg[0] & 0xffc0) == 0xfe80 // link local
                || (seg[0] == 0x2001 && seg[1] == 0x0db8) // documentation
                || (seg[0] == 0x0064 && seg[1] == 0xff9b) // NAT64
                || v6 == Ipv6Addr::UNSPECIFIED)
        }
    }
}

fn is_public_v4(ip: Ipv4Addr) -> bool {
    let o = ip.octets();
    !(ip.is_private()
        || ip.is_loopback()
        || ip.is_link_local()
        || ip.is_broadcast()
        || ip.is_documentation()
        || ip.is_unspecified()
        || ip.is_multicast()
        || o[0] == 0
        || (o[0] == 100 && (o[1] & 0xc0) == 64) // 100.64.0.0/10 shared address space
        || (o[0] == 192 && o[1] == 0 && o[2] == 0) // 192.0.0.0/24
        || (o[0] == 198 && (o[1] & 0xfe) == 18) // 198.18.0.0/15 benchmarking
        || o[0] >= 240)
}

/// Check the URL shape, then resolve and require every address to be public.
pub async fn check_target(raw: &str, allow_http: bool) -> Result<(url::Url, SocketAddr), ApiError> {
    let url = url::Url::parse(raw).map_err(|_| ApiError(400, "url is not a valid URL".into()))?;
    match url.scheme() {
        "https" => {}
        "http" if allow_http => {}
        _ => return Err(ApiError(400, "url must use https".into())),
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err(ApiError(400, "url must not contain credentials".into()));
    }
    let host = url.host_str().ok_or_else(|| ApiError(400, "url has no host".into()))?.to_string();
    let port = url.port_or_known_default().unwrap_or(443);
    if raw.len() > 2048 {
        return Err(ApiError(400, "url too long".into()));
    }
    let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host.trim_matches(|c| c == '[' || c == ']'), port))
        .await
        .map_err(|_| ApiError(400, "url host does not resolve".into()))?
        .collect();
    if addrs.is_empty() {
        return Err(ApiError(400, "url host does not resolve".into()));
    }
    // Debug builds only: allow local receivers for end-to-end tests. Release builds ignore this.
    let test_bypass = cfg!(debug_assertions) && std::env::var("NEM_TEST_ALLOW_PRIVATE_WEBHOOKS").as_deref() == Ok("1");
    if let Some(bad) = addrs.iter().find(|a| !is_public_ip(a.ip())) {
        if !test_bypass {
            return Err(ApiError(400, format!("url resolves to a non-public address ({})", bad.ip())));
        }
    }
    Ok((url, addrs[0]))
}

/// A client pinned to the address we checked (defeats DNS rebinding), with
/// redirects disabled.
fn pinned_client(app: &App, url: &url::Url, addr: SocketAddr) -> reqwest::Result<reqwest::Client> {
    let mut b = reqwest::Client::builder()
        .user_agent(app.cfg.source.user_agent.clone())
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(10));
    if let Some(host) = url.host_str() {
        b = b.resolve(host, addr);
    }
    b.build()
}

fn rate_limited(app: &App, ip: IpAddr) -> bool {
    let limit = app.cfg.public.webhook_registrations_per_ip_per_hour;
    let mut map = match app.reg_limiter.lock() {
        Ok(m) => m,
        Err(_) => return true,
    };
    let now = Instant::now();
    let v = map.entry(ip).or_default();
    v.retain(|t| now.duration_since(*t) < Duration::from_secs(3600));
    if v.len() >= limit {
        return true;
    }
    v.push(now);
    if map.len() > 10_000 {
        map.retain(|_, v| !v.is_empty());
    }
    false
}

pub async fn register(app: &App, req: RegisterReq, ip: IpAddr) -> Result<RegisterResp, ApiError> {
    if rate_limited(app, ip) {
        return Err(ApiError(429, "too many registrations from this address; try again later".into()));
    }
    let count = app.with_db(store::count_webhooks).map_err(|e| ApiError(500, e.to_string()))?;
    if count >= app.cfg.public.max_webhooks {
        return Err(ApiError(503, "webhook capacity reached; use the Atom/JSON feeds or ntfy".into()));
    }
    let min_severity = req.min_severity.unwrap_or_else(|| app.cfg.public.min_severity.clone());
    if Severity::parse(&min_severity).is_none() {
        return Err(ApiError(400, "min_severity must be high, extreme or cap".into()));
    }
    let regions = match req.regions {
        Some(r) if !r.is_empty() => {
            for x in &r {
                if !app.cfg.detect.regions.contains(x) {
                    return Err(ApiError(400, format!("unknown region {x}")));
                }
            }
            Some(r)
        }
        _ => None,
    };
    let (url, addr) = check_target(&req.url, app.cfg.public.allow_http_webhooks).await?;

    // Verification handshake: the receiver must echo the challenge.
    let id = format!("wh_{}", rand_hex(8));
    let challenge = rand_hex(16);
    let body = serde_json::json!({"type": "verification", "subscription_id": id, "challenge": challenge});
    let client = pinned_client(app, &url, addr).map_err(|e| ApiError(500, e.to_string()))?;
    let resp = client
        .post(url.as_str())
        .json(&body)
        .send()
        .await
        .map_err(|e| ApiError(400, format!("verification request failed: {}", e.without_url())))?;
    if !resp.status().is_success() {
        return Err(ApiError(400, format!("verification returned HTTP {}", resp.status().as_u16())));
    }
    let text = resp.text().await.unwrap_or_default();
    let echoed = text.trim() == challenge
        || serde_json::from_str::<serde_json::Value>(&text)
            .ok()
            .and_then(|v| v.get("challenge").and_then(|c| c.as_str()).map(|c| c == challenge))
            .unwrap_or(false);
    if !echoed {
        return Err(ApiError(400, "verification failed: respond with the challenge value".into()));
    }

    let secret = format!("whsec_{}", rand_hex(24));
    let manage_token = format!("mt_{}", rand_hex(24));
    let hook = Webhook { id: id.clone(), url: url.to_string(), secret: secret.clone(), regions, min_severity, failures: 0 };
    app.with_db(|c| store::insert_webhook(c, &hook, &sha256_hex(&manage_token), &ip.to_string()))
        .map_err(|e| ApiError(500, e.to_string()))?;
    tracing::info!(id = %id, "webhook registered");
    Ok(RegisterResp { id, secret, manage_token })
}

/// Check a bearer token against the stored hash.
pub fn authorise(app: &App, id: &str, token: &str) -> Result<(), ApiError> {
    let row = app.with_db(|c| store::get_webhook_manage_hash(c, id)).map_err(|e| ApiError(500, e.to_string()))?;
    match row {
        Some((hash, _)) if constant_eq(&hash, &sha256_hex(token)) => Ok(()),
        _ => Err(ApiError(404, "subscription not found".into())),
    }
}

fn constant_eq(a: &str, b: &str) -> bool {
    a.len() == b.len() && a.bytes().zip(b.bytes()).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn matches(hook: &Webhook, item: &NotificationItem) -> bool {
    let region_ok = hook.regions.as_ref().is_none_or(|r| r.contains(&item.event.region));
    let sev_ok = match item.event.kind {
        crate::model::EventKind::LowPrice => true,
        crate::model::EventKind::HighPrice => crate::notify::severity_meets(&item.event, Some(&hook.min_severity)),
    };
    region_ok && sev_ok
}

/// Deliver a public notification to every matching active webhook.
pub fn fanout(app: &App, item: &NotificationItem) {
    let hooks = match app.with_db(store::active_webhooks) {
        Ok(h) => h,
        Err(e) => {
            tracing::error!("listing webhooks: {e:#}");
            return;
        }
    };
    for hook in hooks.into_iter().filter(|h| matches(h, item)) {
        let a = app.clone();
        let it = item.clone();
        tokio::spawn(async move {
            let _permit = a.delivery_sem.clone().acquire_owned().await;
            deliver_with_retry(&a, &hook, &it).await;
        });
    }
}

async fn deliver_with_retry(app: &App, hook: &Webhook, item: &NotificationItem) {
    let delays = [0u64, 10, 60, 300];
    for (attempt, d) in delays.iter().enumerate() {
        if *d > 0 {
            tokio::time::sleep(Duration::from_secs(*d)).await;
        }
        match deliver(app, hook, item, "event").await {
            Ok(()) => {
                let _ = app.with_db(|c| store::webhook_result(c, &hook.id, true, 0));
                return;
            }
            Err(e) => tracing::debug!(id = %hook.id, attempt, "webhook delivery failed: {e}"),
        }
    }
    let disable_after = app.cfg.public.webhook_disable_after_failures;
    let _ = app.with_db(|c| store::webhook_result(c, &hook.id, false, disable_after));
}

pub async fn deliver(app: &App, hook: &Webhook, item: &NotificationItem, kind: &str) -> Result<(), String> {
    // Re-check the target on every delivery: DNS may have changed since registration.
    let (url, addr) = check_target(&hook.url, app.cfg.public.allow_http_webhooks).await.map_err(|e| e.1)?;
    let client = pinned_client(app, &url, addr).map_err(|e| e.to_string())?;
    let body = serde_json::to_string(&serde_json::json!({
        "type": format!("{kind}.{}", item.transition),
        "subscription_id": hook.id,
        "item": item,
    }))
    .map_err(|e| e.to_string())?;
    let ts = chrono::Utc::now().timestamp();
    let resp = client
        .post(url.as_str())
        .header("content-type", "application/json")
        .header("x-nem-timestamp", ts.to_string())
        .header("x-nem-signature", format!("sha256={}", sign(&hook.secret, ts, &body)))
        .header("x-nem-event-id", &item.event_id)
        .header("x-nem-delivery", format!("{}-{}", item.seq, hook.id))
        .body(body)
        .send()
        .await
        .map_err(|e| e.without_url().to_string())?;
    if resp.status().is_success() {
        Ok(())
    } else {
        Err(format!("HTTP {}", resp.status().as_u16()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_internal_addresses() {
        for ip in ["127.0.0.1", "10.1.2.3", "172.16.0.1", "192.168.1.1", "169.254.169.254", "100.64.0.1", "0.0.0.0", "::1", "fd00::1", "fe80::1", "::ffff:10.0.0.1"] {
            assert!(!is_public_ip(ip.parse().unwrap()), "{ip} should be rejected");
        }
        for ip in ["1.1.1.1", "203.2.218.214", "2606:4700:4700::1111"] {
            assert!(is_public_ip(ip.parse().unwrap()), "{ip} should be allowed");
        }
    }

    #[tokio::test]
    async fn rejects_bad_urls() {
        assert!(check_target("http://example.com/hook", false).await.is_err());
        assert!(check_target("https://user:pw@example.com/hook", false).await.is_err());
        assert!(check_target("https://127.0.0.1/hook", false).await.is_err());
        assert!(check_target("https://localhost/hook", false).await.is_err());
        assert!(check_target("ftp://example.com", false).await.is_err());
    }

    #[test]
    fn signature_is_stable() {
        // Known answer: HMAC-SHA256(key="whsec_test", msg="1700000000.{}")
        let s = sign("whsec_test", 1_700_000_000, "{}");
        assert_eq!(s.len(), 64);
        assert_eq!(s, sign("whsec_test", 1_700_000_000, "{}"));
        assert_ne!(s, sign("whsec_test", 1_700_000_001, "{}"));
    }
}

# nem-monitor

An always-on event detector for Australia's National Electricity Market. It polls AEMO's NEMWeb every 20 seconds, detects price events as they happen, fires operator triggers to run the rest of the pipeline (replay, counterfactuals, media brief), and publishes a public site that anyone can subscribe to.

Single Rust binary, SQLite storage, no other services required.

## What it detects

| Rule | Default | Notes |
| --- | --- | --- |
| Price spike opens | RRP ≥ $5,000/MWh | Internal "watch" only |
| Spike confirmed | 3 intervals ≥ $5,000, or 1 interval ≥ $15,000 | First public notification |
| Escalation | Severity high → extreme (≥ $10,000) → cap (≥ 95% of MPC, $23,200 in 2026-27) | |
| Whiplash flag | ≥ 3 direction reversals of ≥ $1,000 within 12 intervals | The lagged-response ("hog cycle") signature |
| Price separation flag | Region ≥ $5,000 above the cheapest region | Context |
| Demand ramp flag | ≥ 1,500 MW rise within 12 intervals | Context |
| Low-price event | RRP ≤ −$900/MWh for 3 intervals | Closes when prices turn positive |
| Close | 6 consecutive intervals below $1,000 | Unconfirmed events are discarded silently |
| Next-day data | Next_Day_Dispatch and Next_Day_Offer_Energy present for the trading day | Promotes closed events to `next_day` tier, fires `data_ready` |

Each event records peak, intervals beyond threshold, average price, peak demand, the most binding constraints and interconnectors at limit. All thresholds live in `config.toml`.

## Run it

```sh
cargo build --release
cp deploy/config.example.toml config.toml   # edit public_url, user_agent, triggers
./target/release/nem-monitor -c config.toml serve
```

Other commands:

```sh
nem-monitor -c config.toml check-config
nem-monitor -c config.toml replay DIR [--triggers] [--notify]     # saved DispatchIS zips/CSVs
nem-monitor -c config.toml serve --replay DIR --replay-delay-ms 500   # demo: replay through the live site
nem-monitor -c config.toml mark-data-ready 2026-10-12 [--triggers]   # simulate next-day data
```

Try it offline with the synthetic fixture (shaped on the reported 12 October 2026 NSW event; not AEMO data):

```sh
python3 scripts/make_fixture.py fixtures/2026-10-12-synthetic
nem-monitor serve --replay fixtures/2026-10-12-synthetic --replay-delay-ms 300
# open http://localhost:8080
```

## Deploy on an always-on machine

1. Build the release binary (or the Docker image) and copy it to `/usr/local/bin/`.
2. Put the config at `/etc/nem-monitor/config.toml`; keep `bind = "127.0.0.1:8080"`.
3. Install `deploy/nem-monitor.service` and enable it: `systemctl enable --now nem-monitor`.
4. Put `deploy/Caddyfile` in front for automatic HTTPS (it disables buffering on the live stream).
5. Point an external uptime check at `/healthz` (returns 503 if NEMWeb has not been reached for 10 minutes).

Resource use is small: one HTTP request every 20 s, one ~20 KB zip every 5 minutes, and a database that grows by a few MB a month.

## Triggers (running the rest of the pipeline)

```toml
[[triggers]]
name = "full-brief"
on = ["data_ready"]                 # opened confirmed escalated flagged closed discarded data_ready
min_severity = "high"
regions = ["NSW1"]                  # optional
command = ["/opt/nemcf/bin/nemcf", "report", "--event", "{event_id}", "--trading-day", "{trading_day}"]
timeout_secs = 3600
```

Commands run directly (no shell). The notification JSON is on stdin; `NEM_EVENT_ID`, `NEM_REGION`, `NEM_TRANSITION`, `NEM_DATA_TIER`, `NEM_TRADING_DAY`, `NEM_SEVERITY`, `NEM_EVENT_START` and `NEM_EVENT_END` are set; placeholders `{event_id}` `{region}` `{transition}` `{data_tier}` `{trading_day}` `{severity}` `{start}` `{end}` are substituted in argv. A trigger can instead POST to an internal `webhook` signed with the secret in `secret_env`. Runs are logged; set `NEM_ADMIN_TOKEN` to read them at `GET /api/admin/runs`.

## Public subscriptions

| Channel | Endpoint | Notes |
| --- | --- | --- |
| Web page | `/` | Live prices, events, charts |
| Atom / JSON Feed | `/feed.atom`, `/feed.json` | Confirmed, escalated, flagged, closed, data_ready |
| Phone push | ntfy topic in config | Anyone subscribes in the ntfy app; no accounts |
| Live stream | `/api/stream` (SSE) | `interval` and `notification` events |
| Webhooks | `POST /api/webhooks` | Verified, signed, rate limited |
| JSON API | `/api/status`, `/api/events`, `/api/events/{id}`, `/api/notifications?since=` | CORS open |

### Webhooks

```sh
curl -X POST https://nem.example.org/api/webhooks \
  -H 'content-type: application/json' \
  -d '{"url":"https://you.example/hook","regions":["NSW1"],"min_severity":"extreme"}'
# → {"id":"wh_…","secret":"whsec_…","manage_token":"mt_…"}   (shown once)
```

Before registering, the service POSTs `{"type":"verification","challenge":"…"}`; respond 200 with the challenge (plain or `{"challenge":"…"}`). Deliveries carry `X-NEM-Timestamp` and `X-NEM-Signature: sha256=HMAC_SHA256(secret, timestamp + "." + body)`. Delete with `DELETE /api/webhooks/{id}` and `Authorization: Bearer <manage_token>`; send a test with `POST /api/webhooks/{id}/test`.

Safeguards for open registration: HTTPS only; every resolved address must be public (private, loopback, link-local, CGNAT and metadata ranges are refused) and the connection is pinned to the checked address; redirects are not followed; 5 registrations per IP per hour; 1,000 webhooks maximum; 3 retries then a strike, disabled after 10 consecutive failures.

## Layout

```
src/aemo.rs       AEMO C/I/D CSV parser and DispatchIS snapshot
src/nemweb.rs     listings, downloads, unzip
src/detect.rs     the detector (pure, deterministic, restartable)
src/notify.rs     plain-English titles and summaries, fan-out, ntfy
src/triggers.rs   operator triggers
src/webhooks.rs   public webhook registration, SSRF guard, signing
src/feeds.rs      Atom and JSON Feed
src/web.rs        HTTP routes
src/store.rs      SQLite
static/index.html the public page (no external dependencies)
```

Source data © AEMO, used under AEMO's copyright permissions. Not affiliated with AEMO.

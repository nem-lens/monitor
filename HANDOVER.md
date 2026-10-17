# Handover: nem-monitor (for Claude Code)

Read this file and `README.md` first, then run `cargo test` and `cargo clippy` to confirm a clean baseline (20 tests passing, zero clippy warnings at handover).

## 1. Context

**Project.** A NEM counterfactual engine: replay AEMO dispatch for notable market events (first case, the 12 October 2026 NSW price spike) and re-clear with one input changed, in pure Rust compiling to WebAssembly. The full plan lives in a Claude Doc, "NEM Counterfactual Engine: Plan": https://claude.ai/code/artifact/eec10e78-daf9-435f-b86c-f7f8bc8bf313

**This repo** is one module of that plan: `nem-monitor`, the always-on event detector. It polls NEMWeb, detects price events, fires operator triggers that start the replay engine and the media-brief module (`nem-report`, planned), and publishes public feeds.

**Owner context.**
- Contributor to `microlp`; MILP work in the wider project uses microlp, LP and QP use Clarabel. Pure Rust is a firm preference.
- Works on Shapley-value methods; the brief module uses Shapley attribution over four factors (generator outages, network outages, demand above forecast, behind-the-meter batteries).
- Development machine is a **Windows** laptop on a **work network**. Default to localhost binding and treat inbound exposure as a policy question.
- Writing preferences for documents: open sentences with linking phrases where natural (However, In particular, Moreover, Against this background), avoid starting sentences with a negative, minimise em-dashes.

## 2. Current state

**Built and tested** (`src/`, about 2,300 lines):

| Module | Status |
| --- | --- |
| `aemo.rs` | AEMO C/I/D CSV parser, columns by name; DispatchIS snapshot (PRICE, REGIONSUM, CONSTRAINT, INTERCONNECTORRES, pricing run only) |
| `nemweb.rs` | Listing parse (regex, works on HTML or text), download, unzip with size guard |
| `detect.rs` | Pure, deterministic, serialisable detector: opened, confirmed, escalated, flagged (whiplash), closed, discarded; low-price events; separation and demand-ramp flags; constraint and interconnector aggregation |
| `notify.rs` | Template titles and summaries with regional local time; public filter; ntfy push |
| `triggers.rs` | Commands (no shell, stdin JSON, NEM_* env, argv placeholders, timeout kill) or internal signed webhooks; runs logged |
| `webhooks.rs` | Public registration: HTTPS only, private-range block, address pinning, no redirects, challenge echo, per-IP rate limit, HMAC-SHA256 signing, auto-disable |
| `feeds.rs`, `web.rs` | Page, JSON API, Atom, JSON Feed, SSE, `/healthz`, token-gated `/api/admin/runs` |
| `store.rs` | SQLite (WAL) |
| `ingest.rs`, `main.rs` | Poll loop, next-day data watcher, offline replay, `serve --replay` demo mode, `mark-data-ready` |

**Verified end to end** on a synthetic fixture (`scripts/make_fixture.py`), shaped on reported features of the 12 October event; it is not AEMO data. The replay produced opened, confirmed, flagged and closed transitions; webhook deliveries arrived with valid signatures; triggers fired; feeds and SSE carried the notifications.

**Not yet verified:** any run against live NEMWeb. The cloud workspace that built this could not reach nemweb.com.au.

## 3. Build and run on Windows

Requires Rust (rustup) and the MSVC C++ build tools, because `rusqlite` compiles bundled SQLite.

```powershell
cargo build --release
cargo test
.\target\release\nem-monitor.exe -c trial.toml check-config
.\target\release\nem-monitor.exe -c trial.toml serve      # open http://localhost:8080
```

Minimal `trial.toml` for a work network:

```toml
[server]
bind = "127.0.0.1:8080"
public_url = "http://localhost:8080"
[source]
user_agent = "nem-monitor-trial (owner email)"
[store]
path = "trial.db"
[public]
max_webhooks = 0   # disables webhook registration until the hardening task lands
```

Trigger commands on Windows use `powershell -Command ...` instead of `sh -c ...`. The trigger unit test uses `sh` and `sleep`, so it fails on plain Windows; gate it with `#[cfg(unix)]` or add a Windows variant.

## 4. Key decisions (keep unless the owner changes them)

- **Single binary, SQLite, no other services.** Simplicity for an always-on box.
- **Detector stays pure.** No I/O in `detect.rs`; all state round-trips through `state_json()`.
- **Market time internally.** NEMWeb timestamps are AEST (UTC+10, no DST) and mark interval end; display converts per region (AEDT, ACDT, AEST). The trading day runs 04:00 to 04:00.
- **Public notifications** only for confirmed, escalated, flagged, closed and data_ready; `opened` and `discarded` stay internal.
- **Triggers never use a shell**, and placeholders come from internally generated fields only.
- **MPC is config**, currently $23,200/MWh for 2026-27; update each July.

## 5. Backlog (priority order, with acceptance criteria)

### P0. Repository and live validation
1. Initialise the owner's GitHub repo with this tree (a local git history exists in the tarball). Include `Cargo.lock`.
2. Run live on the Windows laptop with the trial config for at least 48 hours.
   - **Accept when:** `/healthz` stays 200; `region_intervals` has no gaps over 10 minutes while awake; no parse warnings in logs.
3. Confirm next-day filenames. `Next_Day_Dispatch` files look like `PUBLIC_NEXT_DAY_DISPATCH_20261016_0000000543289388.zip` (checked). The `Next_Day_Offer_Energy` pattern is assumed; confirm `nemweb::daily_files` matches it.
   - **Accept when:** `data_ready` table fills for both directories each morning after about 04:15 AEST.
4. Validate detection on real history: fetch archived DispatchIS files for 12 October 2026 (NEMWeb Archive or MMSDM) and replay.
   - **Accept when:** an event confirms in NSW1 with peak $23,107/MWh and 11 intervals above $5,000, matching ABC and AEMO figures.

### P1. Hardening
1. `public.webhooks_enabled = false` switch (route returns 404 when off).
2. Build webhook clients with `.no_proxy()` so address pinning holds behind corporate proxies.
3. Cap concurrent SSE connections (for example 200) and add a simple per-IP rate limit to the JSON API.
4. Gate Unix-only tests; add a Windows CI job.
   - **Accept when:** tests pass on Linux and Windows; a proxy env var does not change which address webhooks connect to.

### P2. Option 1: GitHub Actions + Pages (owner's chosen hosting path)
Design:
- New command `nem-monitor once --state-in <dir|url> --site-out site/`: load state, poll NEMWeb once (all unseen files since the last processed interval), run the detector and data-ready check, write the static site, exit.
- **State persistence.** Avoid committing a growing SQLite file every five minutes. Preferred approach: publish state as JSON inside the Pages site (`site/data/state.json`, `events.json`, `notifications.json`, last 48 hours of intervals) and fetch it from the live Pages URL at the start of each run; rebuild an in-memory SQLite from it. Deploy with `actions/deploy-pages`, which adds no commits.
- **Keepalive.** Scheduled workflows on public repos are disabled after 60 days without repo activity, so add a monthly keepalive.
- **Static page mode.** `static/index.html` falls back to `data/*.json` when `/api/*` is unavailable; hide the live stream and webhook form.
- **Workflow:** `schedule: '*/5 * * * *'` plus `workflow_dispatch`; `concurrency: { group: monitor, cancel-in-progress: false }`; cache the release binary between runs; permissions `contents: read, pages: write, id-token: write, discussions: write, contents: write` (for releases).
- **GitHub Watch notifications:** confirmed creates a Discussion in an announcement-type "Alerts" category (GraphQL `createDiscussion`); later transitions add comments (`addDiscussionComment`); `data_ready` creates a Release with the brief PDF attached once `nem-report` exists. Store Discussion IDs and posted transition sequence numbers in state so reruns never double-post.
- Add a "How to subscribe" section to the page: Watch → Custom → Discussions or Releases, plus feeds and ntfy.
- **Accept when:** a replayed fixture pushed through the workflow (via `workflow_dispatch` with a fixture input) produces one Discussion with update comments, a correct static site, and idempotent reruns.
- First check that GitHub-hosted runners can reach nemweb.com.au.

### P3. Social channels
See `docs/distribution-channels.md` for the comparison.
1. Bluesky (app password, bot self-label) and Telegram channel (bot token) first; X later if budgeted.
2. One thread per event: post on confirmed, reply on closed and brief-ready only. New table `channel_posts(event_id, channel, post_id)`.
3. Render the event chart to PNG in Rust (for example `resvg`) and attach it.
   - **Accept when:** a fixture replay posts exactly three messages per event per channel, threaded, with the chart image.

### P4. Pipeline integration
Wire `closed` and `data_ready` triggers to the replay engine (`nemcf replay`) and `nem-report` once those exist. See the plan doc sections "Event report module", "Full-stack architecture" and "Monitor service".

## 6. Conventions

- `cargo test` and `cargo clippy` stay clean on every change.
- New behaviour in the detector gets a unit test with a short price series.
- No secrets in config files; tokens come from environment variables or repo secrets.
- Keep the page dependency-free (no CDN scripts).
- Label any synthetic data clearly as synthetic.

## 7. References

- Plan doc: https://claude.ai/code/artifact/eec10e78-daf9-435f-b86c-f7f8bc8bf313
- ABC article on the event: https://www.abc.net.au/news/2026-10-17/home-batteries-make-money-in-nsw-power-price-spike/107260880
- AEMC MPC 2026-27: https://www.aemc.gov.au/news-centre/media-releases/aemc-updates-market-price-cap-2026-27
- AEMO MMS Data Model report: https://nemweb.com.au/Reports/Current/MMSDataModelReport/Electricity/
- nempy: https://github.com/UNSW-CEEM/nempy; NEMOSIS: https://github.com/UNSW-CEEM/NEMOSIS; PowerIO: https://github.com/eigenergy/powerio

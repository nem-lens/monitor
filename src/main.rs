//! nem-monitor: an always-on NEM event detector.
//!
//!   nem-monitor -c config.toml serve            poll NEMWeb, detect, publish, serve the site
//!   nem-monitor -c config.toml serve --offline  serve an existing database without polling
//!   nem-monitor -c config.toml replay DIR       run the detector over saved DispatchIS files
//!   nem-monitor -c config.toml mark-data-ready DAY [--files DIR]   record next-day data
//!   nem-monitor -c config.toml prune           trim the rolling buffer
//!   nem-monitor -c config.toml once --state-in URL --site-out site/   static site (GitHub Pages)
//!   nem-monitor -c config.toml check-config

mod app;
mod bundle;
mod config;
mod detect;
mod feeds;
mod ingest;
mod model;
mod nemweb;
mod notify;
mod site;
mod store;
mod time;
mod triggers;
mod web;
mod webhooks;

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    version,
    about = "Always-on NEM event detector with public feeds and operator triggers"
)]
struct Cli {
    /// Path to config.toml (defaults apply when omitted).
    #[arg(short, long, global = true)]
    config: Option<String>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run the monitor and public site.
    Serve {
        /// Do not poll NEMWeb; serve what is already in the database.
        #[arg(long)]
        offline: bool,
        /// Demo mode: feed a directory of saved files through the live pipeline instead of polling.
        #[arg(long, value_name = "DIR")]
        replay: Option<PathBuf>,
        /// Pause between replayed intervals, in milliseconds.
        #[arg(long, default_value_t = 1000)]
        replay_delay_ms: u64,
    },
    /// Replay a directory of DispatchIS .zip or .csv files through the detector.
    Replay {
        dir: PathBuf,
        /// Also send to ntfy and public webhooks.
        #[arg(long)]
        notify: bool,
        /// Also run operator triggers.
        #[arg(long)]
        triggers: bool,
    },
    /// Record next-day data as available for a trading day (YYYY-MM-DD) and promote events.
    MarkDataReady {
        day: String,
        #[arg(long)]
        triggers: bool,
        /// Directory holding the real next-day zips (e.g. from the NEMWeb
        /// Archive); they are sliced into event bundles. Without it the data
        /// is recorded as available but bundles get no bids.
        #[arg(long, value_name = "DIR")]
        files: Option<PathBuf>,
    },
    /// Drop buffered outcomes and raw zips older than store.buffer_hours.
    Prune,
    /// Static-site mode (GitHub Pages): load state, poll once, write the site, exit.
    Once {
        /// Previous site to continue from: a directory or its public URL.
        /// Without it the run starts fresh.
        #[arg(long, value_name = "DIR|URL")]
        state_in: Option<String>,
        /// Where to write the site.
        #[arg(long, value_name = "DIR")]
        site_out: PathBuf,
        /// Feed saved DispatchIS files instead of polling NEMWeb (for testing).
        #[arg(long, value_name = "DIR")]
        replay: Option<PathBuf>,
    },
    /// Validate the configuration and exit.
    CheckConfig,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "nem_monitor=info".into()),
        )
        .init();
    let cli = Cli::parse();
    let cfg = config::Config::load(cli.config.as_deref())?;

    match cli.cmd {
        Cmd::CheckConfig => {
            println!(
                "config OK: {} regions, {} triggers",
                cfg.detect.regions.len(),
                cfg.triggers.len()
            );
            Ok(())
        }
        Cmd::Replay {
            dir,
            notify,
            triggers,
        } => {
            let conn = store::open(&cfg.store.path)?;
            let mut app = app::App::new(cfg, conn, "replay")?;
            app.notify_external = notify;
            app.fire_triggers = triggers;
            app.echo = true;
            let n = ingest::replay_dir(&app, &dir, std::time::Duration::ZERO).await?;
            println!("replayed {n} reports into {}", app.cfg.store.path);
            // Give spawned deliveries and triggers a moment to finish.
            if notify || triggers {
                tokio::time::sleep(std::time::Duration::from_secs(3)).await;
                let _ = app
                    .trigger_sem
                    .acquire_many(app.cfg.max_concurrent_triggers.unwrap_or(2).max(1) as u32)
                    .await;
            }
            Ok(())
        }
        Cmd::MarkDataReady {
            day,
            triggers,
            files,
        } => {
            chrono::NaiveDate::parse_from_str(&day, "%Y-%m-%d")?;
            let conn = store::open(&cfg.store.path)?;
            let mut app = app::App::new(cfg, conn, "replay")?;
            app.notify_external = false;
            app.fire_triggers = triggers;
            app.echo = true;
            let sources = app.cfg.source.data_ready_dirs.clone();
            let found = match &files {
                Some(dir) => local_daily_files(dir, &sources)?,
                None => Vec::new(),
            };
            app.with_db(|c| {
                for (source, d, path) in &found {
                    store::set_data_ready_file(c, source, d, path)?;
                }
                for s in &sources {
                    store::record_data_ready(c, s, &day, "manual")?;
                }
                Ok(())
            })?;
            for (source, d, path) in &found {
                println!("{source} {d}: {path}");
            }
            ingest::promote_ready_events(&app).await?;
            if triggers {
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                let _ = app
                    .trigger_sem
                    .acquire_many(app.cfg.max_concurrent_triggers.unwrap_or(2).max(1) as u32)
                    .await;
            }
            Ok(())
        }
        Cmd::Once {
            state_in,
            site_out,
            replay,
        } => {
            let mut cfg = cfg;
            // No persistent disk between runs: keep the raw buffer and bundles
            // in a scratch directory that is not published.
            let scratch =
                std::env::temp_dir().join(format!("nem-monitor-once-{}", std::process::id()));
            cfg.store.raw_dir = scratch.join("raw").to_string_lossy().into();
            cfg.store.bundle_dir = scratch.join("bundles").to_string_lossy().into();
            let mut app = app::App::new(cfg, store::open(":memory:")?, "static")?;
            // ntfy only (no webhooks are registered in static mode); no local pipeline.
            app.fire_triggers = false;
            app.echo = true;
            let prior = match &state_in {
                Some(src) => site::load_state(src, &app.cfg.source.user_agent).await?,
                None => None,
            };
            match &prior {
                Some(s) => {
                    site::import(&app, s)?;
                    println!(
                        "continuing from state of {} ({} events, {} notifications)",
                        s.generated_at,
                        s.events.len(),
                        s.notifications.len()
                    );
                }
                None => println!("no previous state; starting fresh"),
            }
            let n = match &replay {
                Some(dir) => ingest::replay_dir(&app, dir, std::time::Duration::ZERO).await?,
                None => ingest::poll_once(&app).await?,
            };
            let state = site::write_site(&app, &site_out)?;
            println!(
                "processed {n} new reports; wrote site to {} ({} events, {} intervals buffered)",
                site_out.display(),
                state.events.len(),
                state.intervals.len()
            );
            // Let ntfy pushes finish before exiting.
            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
            std::fs::remove_dir_all(&scratch).ok();
            Ok(())
        }
        Cmd::Prune => {
            let conn = store::open(&cfg.store.path)?;
            let app = app::App::new(cfg, conn, "offline")?;
            let n = ingest::prune_buffer(&app)?;
            println!(
                "pruned to the last {} hours; removed {n} raw files",
                app.cfg.store.buffer_hours
            );
            Ok(())
        }
        Cmd::Serve {
            offline,
            replay,
            replay_delay_ms,
        } => {
            let conn = store::open(&cfg.store.path)?;
            let bind: SocketAddr = cfg.server.bind.parse()?;
            let mode = if replay.is_some() {
                "replay"
            } else if offline {
                "offline"
            } else {
                "live"
            };
            let app = app::App::new(cfg, conn, mode)?;
            if let Some(dir) = replay {
                let a = app.clone();
                tokio::spawn(async move {
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                    match ingest::replay_dir(
                        &a,
                        &dir,
                        std::time::Duration::from_millis(replay_delay_ms),
                    )
                    .await
                    {
                        Ok(n) => tracing::info!("demo replay finished: {n} reports"),
                        Err(e) => tracing::error!("demo replay failed: {e:#}"),
                    }
                });
            } else if !offline {
                tokio::spawn(ingest::run(app.clone()));
            }
            let listener = tokio::net::TcpListener::bind(bind).await?;
            tracing::info!("serving on http://{bind}");
            axum::serve(
                listener,
                web::router(app).into_make_service_with_connect_info::<SocketAddr>(),
            )
            .with_graceful_shutdown(async {
                let _ = tokio::signal::ctrl_c().await;
            })
            .await?;
            Ok(())
        }
    }
}

/// Next-day zips in `dir` for the configured sources, as (source, trading
/// day, absolute path). A source's files are named PUBLIC_<SOURCE>_<YYYYMMDD>_….
fn local_daily_files(
    dir: &std::path::Path,
    sources: &[String],
) -> Result<Vec<(String, String, String)>> {
    let mut out = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let path = std::fs::canonicalize(entry?.path())?;
        let name = path
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default();
        for (file, day) in nemweb::daily_files(&name) {
            for s in sources {
                let prefix = format!("PUBLIC_{}_", s.to_ascii_uppercase());
                // The date must follow the prefix directly, so NEXT_DAY_OFFER_ENERGY
                // does not claim NEXT_DAY_OFFER_ENERGY_SPARSE files.
                let dated = file
                    .strip_prefix(&prefix)
                    .is_some_and(|rest| rest.starts_with(&day.format("%Y%m%d").to_string()));
                if dated {
                    out.push((s.clone(), day.to_string(), path.to_string_lossy().into()));
                }
            }
        }
    }
    out.sort();
    Ok(out)
}

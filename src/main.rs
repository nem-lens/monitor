//! nem-monitor: an always-on NEM event detector.
//!
//!   nem-monitor -c config.toml serve            poll NEMWeb, detect, publish, serve the site
//!   nem-monitor -c config.toml serve --offline  serve an existing database without polling
//!   nem-monitor -c config.toml replay DIR       run the detector over saved DispatchIS files
//!   nem-monitor -c config.toml mark-data-ready DAY   simulate next-day data for testing
//!   nem-monitor -c config.toml check-config

mod app;
mod config;
mod detect;
mod feeds;
mod ingest;
mod model;
mod nemweb;
mod notify;
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
        Cmd::MarkDataReady { day, triggers } => {
            chrono::NaiveDate::parse_from_str(&day, "%Y-%m-%d")?;
            let conn = store::open(&cfg.store.path)?;
            let mut app = app::App::new(cfg, conn, "replay")?;
            app.notify_external = false;
            app.fire_triggers = triggers;
            app.echo = true;
            let sources = app.cfg.source.data_ready_dirs.clone();
            app.with_db(|c| {
                for s in &sources {
                    store::record_data_ready(c, s, &day, "manual")?;
                }
                Ok(())
            })?;
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

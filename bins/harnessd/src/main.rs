use std::{
    env,
    net::{IpAddr, SocketAddr},
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail};
use axum::{
    body::Body,
    extract::OriginalUri,
    http::{HeaderValue, Method, StatusCode, header},
    response::{IntoResponse, Response},
};
use clap::{Parser, Subcommand};
use evaluation::EvaluationService;
use harness_codex::{
    CodexRuntime, CodexRuntimeManager, CodexSettings, EventKind, probe_compatibility,
};
use harness_orchestrator::Orchestrator;
use harness_profile::{HarnessConfig, ResolvedPaths, load_profile};
use harness_store::Store;
use observation::ObservationService;
use rust_embed::RustEmbed;
use serde_json::json;
use tokio::{
    net::TcpListener,
    sync::{mpsc, watch},
    task::JoinHandle,
};
use tower_http::{compression::CompressionLayer, trace::TraceLayer};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "harnessd", version, about = "BILDR local control plane")]
struct Cli {
    /// TOML configuration file. Defaults to the XDG path when present.
    #[arg(long, env = "HARNESS_CONFIG")]
    config: Option<PathBuf>,
    /// Built-in profile id or a profile TOML path.
    #[arg(long, default_value = "general", env = "HARNESS_PROFILE")]
    profile: String,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Serve the localhost API and embedded web application.
    Serve {
        #[arg(long)]
        bind: Option<String>,
        #[arg(long)]
        no_browser: bool,
        /// Start in inspection-only mode without spawning Codex App Server.
        #[arg(long)]
        without_codex: bool,
    },
    /// Validate paths, database, profile, and the pinned Codex protocol tuple.
    Doctor {
        #[arg(long)]
        json: bool,
        #[arg(long)]
        without_codex: bool,
    },
    /// Run the single controller-owned observer snapshot regression evaluation.
    EvaluateObserverSnapshot {
        /// Repository containing the pinned historical commits.
        #[arg(long)]
        repository: PathBuf,
    },
}

#[derive(RustEmbed)]
#[folder = "../../ui/dist/"]
struct UiAssets;

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();
    let cli = Cli::parse();
    let mut config = load_config(cli.config.as_deref())?;
    match cli.command {
        Command::Serve {
            bind,
            no_browser,
            without_codex,
        } => {
            if let Some(bind) = bind {
                config.server.bind = bind;
                config.validate()?;
            }
            serve(config, &cli.profile, no_browser, without_codex).await
        }
        Command::Doctor {
            json,
            without_codex,
        } => doctor(config, &cli.profile, json, without_codex).await,
        Command::EvaluateObserverSnapshot { repository } => {
            let improvement = config.self_improvement_runtime_status();
            if !improvement.observation_enabled {
                bail!(
                    "observer snapshot evaluation requires effective observe_only mode with the frozen safety anchor"
                );
            }
            let paths = config.resolve_paths()?;
            paths.create_securely()?;
            let receipt = EvaluationService::new(
                Store::open(&paths.database, &paths.artifact_root)?,
                paths.worktree_root.clone(),
                paths.cache_dir.join("evaluation-spool"),
            )?
            .run_observer_snapshot_once(repository)
            .await?;
            println!("{receipt}");
            Ok(())
        }
    }
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("harnessd=info,harness_orchestrator=info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_target(false)
        .compact()
        .init();
}

fn load_config(explicit: Option<&Path>) -> Result<HarnessConfig> {
    let selected = explicit
        .map(PathBuf::from)
        .or_else(default_config_path_if_present);
    HarnessConfig::load(selected.as_deref()).map_err(Into::into)
}

fn default_config_path_if_present() -> Option<PathBuf> {
    let base = env::var_os("XDG_CONFIG_HOME")
        .map(PathBuf::from)
        .or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".config")))?;
    let path = base.join("harness-console/config.toml");
    path.exists().then_some(path)
}

async fn serve(
    config: HarnessConfig,
    profile_id: &str,
    no_browser: bool,
    without_codex: bool,
) -> Result<()> {
    let bind = resolve_bind(&config.server.bind).await?;
    let should_open_browser = config.server.open_browser_on_start && !no_browser;
    let paths = config.resolve_paths()?;
    paths.create_securely()?;
    let profile = load_profile(profile_id, &paths.config_dir)?;
    let store = Store::open(&paths.database, &paths.artifact_root)?;
    let (event_tx, mut event_rx) = mpsc::channel(4_096);
    let supervisor_settings = codex_settings(&config, &paths);

    let preferred_account_id = store
        .runtime_metadata("settings.active_codex_account")?
        .and_then(|value| value.as_str().map(ToOwned::to_owned));
    let runtime_manager = if without_codex {
        None
    } else {
        match CodexRuntimeManager::start(
            supervisor_settings.clone(),
            event_tx.clone(),
            preferred_account_id.as_deref(),
        )
        .await
        {
            Ok(manager) => Some(Arc::new(manager)),
            Err(error) => {
                warn!(%error, "Codex App Server unavailable; serving inspection-only UI");
                None
            }
        }
    };
    let runtime = runtime_manager
        .as_ref()
        .map(|runtime| Arc::clone(runtime) as Arc<dyn CodexRuntime>);
    let observation_enabled = config.self_improvement_runtime_status().observation_enabled;
    let orchestrator = Arc::new(
        Orchestrator::new(config, paths, profile, store, runtime)
            .await
            .context("failed to initialize orchestration services")?,
    );
    let shutting_down = Arc::new(AtomicBool::new(false));
    let event_orchestrator = Arc::clone(&orchestrator);
    let event_runtime = runtime_manager.clone();
    let event_shutting_down = Arc::clone(&shutting_down);
    let event_pump: JoinHandle<()> = tokio::spawn(async move {
        while let Some(mut event) = event_rx.recv().await {
            let process_exited = event.kind == EventKind::ProcessExit;
            let exiting_pid = event
                .message
                .get("pid")
                .and_then(serde_json::Value::as_u64)
                .and_then(|pid| u32::try_from(pid).ok());
            let current_pid = match &event_runtime {
                Some(runtime) => runtime.active_pid().await,
                None => None,
            };
            let stale_exit = process_exited
                && exiting_pid.is_some()
                && current_pid.is_some()
                && exiting_pid != current_pid;
            if stale_exit && let Some(message) = event.message.as_object_mut() {
                message.insert("stale".to_owned(), serde_json::Value::Bool(true));
            }
            if let Err(error) = event_orchestrator.ingest_codex_event(event).await {
                error!(%error, "failed to persist/project Codex event");
            }
            if !process_exited
                || stale_exit
                || event_shutting_down.load(Ordering::Acquire)
                || without_codex
            {
                continue;
            }
            let mut restarted = false;
            for ordinal in 1_u32..=3 {
                let backoff_seconds = 1_u64 << (ordinal - 1).min(2);
                warn!(
                    ordinal,
                    backoff_seconds, "restarting Codex App Server after exit"
                );
                tokio::time::sleep(Duration::from_secs(backoff_seconds)).await;
                if event_shutting_down.load(Ordering::Acquire) {
                    break;
                }
                let Some(runtime) = &event_runtime else {
                    break;
                };
                match runtime.restart_active().await {
                    Ok(()) => {
                        info!(ordinal, "Codex App Server restart succeeded");
                        restarted = true;
                        break;
                    }
                    Err(error) => {
                        warn!(ordinal, %error, "Codex App Server restart failed");
                    }
                }
            }
            if !restarted && !event_shutting_down.load(Ordering::Acquire) {
                error!("Codex App Server restart budget exhausted; execution remains disabled");
            }
        }
    });
    let maintenance_orchestrator = Arc::clone(&orchestrator);
    let maintenance_seconds = orchestrator.maintenance_interval_seconds().max(1);
    let maintenance_task: JoinHandle<()> = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(maintenance_seconds));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            ticker.tick().await;
            if let Err(error) = maintenance_orchestrator.maintenance_tick().await {
                warn!(%error, "orchestration maintenance tick failed");
            }
        }
    });
    let observation_task = observation_enabled.then(|| {
        let observer = ObservationService::new(orchestrator.store().clone());
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(15));
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                observer.observe_once();
            }
        })
    });

    let app = harness_api::router(Arc::clone(&orchestrator))
        .fallback(static_asset)
        .layer(CompressionLayer::new())
        .layer(TraceLayer::new_for_http());
    let listener = TcpListener::bind(bind)
        .await
        .with_context(|| format!("failed to bind http://{bind}"))?;
    let local_addr = listener.local_addr()?;
    let url = format!("http://{local_addr}");
    info!(%url, database = %orchestrator.store().database_path().display(), "BILDR ready");
    if should_open_browser {
        open_browser(&url);
    }
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let signal_shutting_down = Arc::clone(&shutting_down);
    let signal_task = tokio::spawn(async move {
        shutdown_signal().await;
        signal_shutting_down.store(true, Ordering::Release);
        let _ = shutdown_tx.send(true);
    });
    let mut graceful_rx = shutdown_rx.clone();
    let server = async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                wait_for_shutdown(&mut graceful_rx).await;
            })
            .await
    };
    tokio::pin!(server);
    let mut deadline_rx = shutdown_rx;
    let result = tokio::select! {
        result = &mut server => result,
        () = async {
            wait_for_shutdown(&mut deadline_rx).await;
            tokio::time::sleep(Duration::from_secs(5)).await;
        } => {
            warn!("graceful HTTP shutdown deadline elapsed; closing remaining connections");
            Ok(())
        }
    };
    info!("BILDR stopping");
    shutting_down.store(true, Ordering::Release);
    if let Some(task) = observation_task {
        task.abort();
    }
    signal_task.abort();
    maintenance_task.abort();
    event_pump.abort();
    if let Some(runtime) = runtime_manager
        && let Err(error) = runtime.shutdown().await
    {
        warn!(%error, "App Server shutdown reported an error");
    }
    result.context("HTTP server failed")
}

async fn doctor(
    config: HarnessConfig,
    profile_id: &str,
    json_output: bool,
    without_codex: bool,
) -> Result<()> {
    let bind = resolve_bind(&config.server.bind).await?;
    let paths = config.resolve_paths()?;
    paths.create_securely()?;
    let profile = load_profile(profile_id, &paths.config_dir)?;
    let store = Store::open(&paths.database, &paths.artifact_root)?;
    let database = store.check()?;
    let compatibility = if without_codex {
        None
    } else {
        Some(probe_compatibility(&codex_settings(&config, &paths)).await?)
    };
    let report = json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "bind": bind,
        "profile": {
            "id": profile.profile.profile_id,
            "source": profile.source,
            "sha256": profile.digest,
        },
        "paths": {
            "database": paths.database,
            "artifacts": paths.artifact_root,
            "worktrees": paths.worktree_root,
        },
        "database": database,
        "codex": compatibility.clone(),
    });
    if json_output {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        println!("BILDR doctor: ok");
        println!(
            "  profile: {}",
            report["profile"]["id"].as_str().unwrap_or("unknown")
        );
        println!(
            "  database: {} ({})",
            database.integrity, database.journal_mode
        );
        if let Some(compatibility) = compatibility {
            println!(
                "  codex: {} · schema {}",
                compatibility.version,
                if compatibility.schema_match {
                    "matched"
                } else {
                    "mismatch"
                }
            );
        } else {
            println!("  codex: skipped");
        }
    }
    Ok(())
}

fn codex_settings(config: &HarnessConfig, paths: &ResolvedPaths) -> CodexSettings {
    CodexSettings {
        binary: PathBuf::from(&config.codex.binary),
        codex_home: env::var_os("CODEX_HOME").map(PathBuf::from),
        managed_account_root: Some(paths.data_dir.join("codex-accounts")),
        required_version: nonempty(&config.codex.required_version),
        required_schema_sha256: nonempty(&config.codex.required_protocol_schema_sha256),
        schema_probe_root: paths.cache_dir.join("codex-schema-probes"),
        service_name: config.codex.service_name.clone(),
        experimental_api: config.codex.experimental_api,
        request_timeout: Duration::from_secs(60),
    }
}

fn nonempty(value: &str) -> Option<String> {
    (!value.trim().is_empty()).then(|| value.to_owned())
}

fn ensure_loopback(ip: IpAddr) -> Result<()> {
    if !ip.is_loopback() {
        bail!("harnessd refuses non-loopback bind address {ip}");
    }
    Ok(())
}

async fn resolve_bind(value: &str) -> Result<SocketAddr> {
    if let Ok(address) = value.parse::<SocketAddr>() {
        ensure_loopback(address.ip())?;
        return Ok(address);
    }
    let addresses = tokio::net::lookup_host(value)
        .await
        .with_context(|| format!("invalid bind address {value}"))?
        .collect::<Vec<_>>();
    if addresses.is_empty() || addresses.iter().any(|address| !address.ip().is_loopback()) {
        bail!("harnessd refuses bind address {value} because it is not exclusively loopback");
    }
    Ok(addresses[0])
}

async fn static_asset(method: Method, OriginalUri(uri): OriginalUri) -> Response {
    if uri.path().starts_with("/api/") {
        return (
            StatusCode::NOT_FOUND,
            axum::Json(json!({"error": {"code": "not_found", "message": "API route not found"}})),
        )
            .into_response();
    }
    if !matches!(method, Method::GET | Method::HEAD) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let requested = uri.path().trim_start_matches('/');
    let (asset_path, asset) = UiAssets::get(requested)
        .map(|asset| (requested, asset))
        .or_else(|| UiAssets::get("index.html").map(|asset| ("index.html", asset)))
        .expect("embedded UI must contain index.html");
    let mime = mime_guess::from_path(asset_path).first_or_octet_stream();
    let mut response = if method == Method::HEAD {
        Response::new(Body::empty())
    } else {
        Response::new(Body::from(asset.data.into_owned()))
    };
    *response.status_mut() = StatusCode::OK;
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_str(mime.as_ref())
            .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
    );
    headers.insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(if asset_path == "index.html" {
            "no-store"
        } else {
            "public, max-age=31536000, immutable"
        }),
    );
    headers.insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    headers.insert("x-frame-options", HeaderValue::from_static("DENY"));
    headers.insert(
        "content-security-policy",
        HeaderValue::from_static(
            "default-src 'self'; connect-src 'self'; img-src 'self' data:; style-src 'self' 'unsafe-inline'; script-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'",
        ),
    );
    response
}

fn open_browser(url: &str) {
    if env::var_os("DISPLAY").is_none() && env::var_os("WAYLAND_DISPLAY").is_none() {
        return;
    }
    match std::process::Command::new("xdg-open")
        .arg(url)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
    {
        Ok(_) => info!(%url, "opened browser"),
        Err(error) => warn!(%error, %url, "could not open browser"),
    }
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();
    tokio::select! {
        () = ctrl_c => {},
        () = terminate => {},
    }
}

async fn wait_for_shutdown(receiver: &mut watch::Receiver<bool>) {
    while !*receiver.borrow_and_update() {
        if receiver.changed().await.is_err() {
            break;
        }
    }
}
mod evaluation;
mod observation;

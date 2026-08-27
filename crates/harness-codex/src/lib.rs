//! Version-pinned Codex App Server supervision over JSONL stdio.

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, VecDeque},
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use harness_domain::CodexRuntimeStatus;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{
    fs,
    io::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufReader},
    process::{Child, Command},
    sync::{Mutex, RwLock, broadcast, mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};
use tracing::{error, info, warn};

const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
/// App Server writes must not allow a stalled durable-event consumer to pin an
/// HTTP mutation forever.  A response that cannot enter the writer queue has
/// not been sent, so retaining its ownership makes an explicit retry safe.
const WRITER_ENQUEUE_TIMEOUT: Duration = Duration::from_secs(10);
const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
const MAX_STDERR_TAIL_BYTES: usize = 64 * 1024;
const MAX_STDERR_LINE_BYTES: usize = 64 * 1024;
const MAX_CONSECUTIVE_PROTOCOL_ERRORS: u8 = 3;
/// Keep passive account telemetry fresh without relaunching an App Server for
/// every account on every dashboard poll. A manual refresh bypasses this cap.
const ACCOUNT_TELEMETRY_REFRESH_INTERVAL_MS: i64 = 60_000;
const SCOPED_READ_RUNTIME_SCHEMA: &str = "harness.scoped-read-runtime.v1";
const SCOPED_READ_BWRAP: &str = "/usr/bin/bwrap";
const SCOPED_READ_WORKSPACE: &str = "/work/investigation";
const SCOPED_READ_CODEX_BINARY: &str = "/opt/harness/codex";
const MAX_SCOPED_READ_FILES: usize = 4_096;
static NEXT_SCHEMA_PROBE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
pub struct CodexSettings {
    pub binary: PathBuf,
    pub codex_home: Option<PathBuf>,
    pub managed_account_root: Option<PathBuf>,
    pub required_version: Option<String>,
    pub required_schema_sha256: Option<String>,
    pub schema_probe_root: PathBuf,
    pub service_name: String,
    pub experimental_api: bool,
    pub request_timeout: Duration,
}

impl Default for CodexSettings {
    fn default() -> Self {
        Self {
            binary: PathBuf::from("codex"),
            codex_home: None,
            managed_account_root: None,
            required_version: None,
            required_schema_sha256: None,
            schema_probe_root: std::env::temp_dir().join("harness-console-schema-probes"),
            service_name: "harness_console".to_owned(),
            experimental_api: false,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
        }
    }
}

/// Controller-derived file admission for one read-only App Server process.
///
/// The App Server protocol currently has no readable-root field or read event
/// stream.  This is therefore deliberately a runtime launch policy, not an
/// advisory `sandboxPolicy` extension: the process receives a new mount and
/// network namespace in which only the admitted regular files exist below the
/// virtual working directory.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScopedReadRuntime {
    pub schema: String,
    pub source_root: PathBuf,
    pub allowed_relative_paths: Vec<PathBuf>,
    pub policy_digest: String,
}

impl ScopedReadRuntime {
    /// Bound the controller materialization before an untrusted repository can
    /// turn a broad but otherwise valid glob into an unbounded launch packet.
    pub const MAX_FILES: usize = MAX_SCOPED_READ_FILES;

    pub fn new(
        source_root: PathBuf,
        allowed_relative_paths: Vec<PathBuf>,
    ) -> Result<Self, CodexError> {
        let mut scope = Self {
            schema: SCOPED_READ_RUNTIME_SCHEMA.to_owned(),
            source_root,
            allowed_relative_paths,
            policy_digest: String::new(),
        };
        scope.canonicalize_paths()?;
        scope.policy_digest = scope.digest();
        Ok(scope)
    }

    #[must_use]
    pub fn virtual_cwd() -> PathBuf {
        PathBuf::from(SCOPED_READ_WORKSPACE)
    }

    fn validate(&self) -> Result<(), CodexError> {
        if self.schema != SCOPED_READ_RUNTIME_SCHEMA {
            return Err(CodexError::ScopedReadRuntime(
                "scoped read runtime schema is not recognized".to_owned(),
            ));
        }
        if !self.source_root.is_absolute() {
            return Err(CodexError::ScopedReadRuntime(
                "scoped read source root must be absolute".to_owned(),
            ));
        }
        if self.allowed_relative_paths.is_empty()
            || self.allowed_relative_paths.len() > Self::MAX_FILES
        {
            return Err(CodexError::ScopedReadRuntime(format!(
                "scoped read runtime must admit between one and {} files",
                Self::MAX_FILES,
            )));
        }
        let mut prior: Option<&PathBuf> = None;
        for path in &self.allowed_relative_paths {
            validate_scoped_relative_path(path)?;
            if prior == Some(path) {
                return Err(CodexError::ScopedReadRuntime(
                    "scoped read runtime contains a duplicate path".to_owned(),
                ));
            }
            prior = Some(path);
        }
        if !is_lower_hex_digest(&self.policy_digest) || self.policy_digest != self.digest() {
            return Err(CodexError::ScopedReadRuntime(
                "scoped read runtime policy digest does not match its canonical admission"
                    .to_owned(),
            ));
        }
        Ok(())
    }

    fn canonicalize_paths(&mut self) -> Result<(), CodexError> {
        self.source_root = std::fs::canonicalize(&self.source_root).map_err(|error| {
            CodexError::ScopedReadRuntime(format!(
                "could not canonicalize scoped read source root {}: {error}",
                self.source_root.display()
            ))
        })?;
        if !self.source_root.is_dir() {
            return Err(CodexError::ScopedReadRuntime(
                "scoped read source root is not a directory".to_owned(),
            ));
        }
        self.allowed_relative_paths.sort();
        self.allowed_relative_paths.dedup();
        if self.allowed_relative_paths.is_empty()
            || self.allowed_relative_paths.len() > Self::MAX_FILES
        {
            return Err(CodexError::ScopedReadRuntime(format!(
                "scoped read runtime must admit between one and {} files",
                Self::MAX_FILES,
            )));
        }
        for path in &self.allowed_relative_paths {
            validate_scoped_relative_path(path)?;
            let source = self.source_root.join(path);
            let metadata = std::fs::symlink_metadata(&source).map_err(|error| {
                CodexError::ScopedReadRuntime(format!(
                    "scoped read source {} is unavailable: {error}",
                    path.display()
                ))
            })?;
            if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
                return Err(CodexError::ScopedReadRuntime(format!(
                    "scoped read source {} must be a regular non-symlink file",
                    path.display()
                )));
            }
            let canonical = std::fs::canonicalize(&source).map_err(|error| {
                CodexError::ScopedReadRuntime(format!(
                    "could not canonicalize scoped read source {}: {error}",
                    path.display()
                ))
            })?;
            if !canonical.starts_with(&self.source_root) {
                return Err(CodexError::ScopedReadRuntime(format!(
                    "scoped read source {} escapes its controller root",
                    path.display()
                )));
            }
        }
        Ok(())
    }

    #[must_use]
    fn digest(&self) -> String {
        let paths = self
            .allowed_relative_paths
            .iter()
            .map(|path| path.to_string_lossy())
            .collect::<Vec<_>>()
            .join("\0");
        hex::encode(Sha256::digest(
            format!(
                "{SCOPED_READ_RUNTIME_SCHEMA}\0{}\0{paths}",
                self.source_root.display()
            )
            .as_bytes(),
        ))
    }
}

fn validate_scoped_relative_path(path: &Path) -> Result<(), CodexError> {
    if path.as_os_str().is_empty()
        || path
            .components()
            .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        return Err(CodexError::ScopedReadRuntime(format!(
            "scoped read path {} is not a normal relative path",
            path.display()
        )));
    }
    Ok(())
}

fn is_lower_hex_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

#[derive(Clone, Debug, Serialize)]
pub struct Compatibility {
    pub version: String,
    pub required_version: Option<String>,
    pub schema_sha256: String,
    pub required_schema_sha256: Option<String>,
    pub version_match: bool,
    pub schema_match: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct CodexEvent {
    pub direction: EventDirection,
    pub kind: EventKind,
    pub method: String,
    pub request_id: Option<Value>,
    pub message: Value,
}

#[derive(Clone, Debug, Serialize)]
pub struct CodexRateLimitWindow {
    pub kind: String,
    pub used_percent: u32,
    pub remaining_percent: u32,
    pub window_duration_mins: Option<u64>,
    pub resets_at: Option<i64>,
}

#[derive(Clone, Debug, Serialize)]
pub struct CodexRateLimit {
    pub limit_id: String,
    pub limit_name: Option<String>,
    pub plan_type: Option<String>,
    pub windows: Vec<CodexRateLimitWindow>,
}

#[derive(Clone, Debug, Serialize)]
pub struct CodexAccountProfile {
    pub id: String,
    pub label: String,
    pub codex_home: PathBuf,
    pub selected: bool,
    pub state: String,
    pub account_type: Option<String>,
    pub email: Option<String>,
    pub plan_type: Option<String>,
    pub rate_limits: Vec<CodexRateLimit>,
    pub observed_at: Option<i64>,
    pub detail: Option<String>,
    pub managed: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct CodexAccountsSnapshot {
    pub selected_account_id: Option<String>,
    pub accounts: Vec<CodexAccountProfile>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EventDirection {
    Inbound,
    Outbound,
    Diagnostic,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EventKind {
    Request,
    Response,
    Notification,
    ServerRequest,
    Stderr,
    ProcessExit,
}

struct PendingRequest {
    method: String,
    response: oneshot::Sender<Result<Value, CodexError>>,
}

#[derive(Clone)]
pub struct CodexSupervisor {
    settings: Arc<CodexSettings>,
    compatibility: Compatibility,
    pid: u32,
    next_id: Arc<AtomicU64>,
    writer: mpsc::Sender<Value>,
    pending: Arc<Mutex<HashMap<String, PendingRequest>>>,
    server_requests: Arc<Mutex<BTreeSet<String>>>,
    status: Arc<RwLock<CodexRuntimeStatus>>,
    stderr_tail: Arc<Mutex<VecDeque<u8>>>,
    events: broadcast::Sender<CodexEvent>,
    child: Arc<Mutex<Option<Child>>>,
    tasks: Arc<Mutex<Vec<JoinHandle<()>>>>,
}

impl CodexSupervisor {
    pub async fn start(
        settings: CodexSettings,
        durable_sink: mpsc::Sender<CodexEvent>,
    ) -> Result<Self, CodexError> {
        Self::start_with_scoped_read(settings, durable_sink, None).await
    }

    async fn start_scoped_read(
        settings: CodexSettings,
        durable_sink: mpsc::Sender<CodexEvent>,
        scope: ScopedReadRuntime,
    ) -> Result<Self, CodexError> {
        Self::start_with_scoped_read(settings, durable_sink, Some(scope)).await
    }

    async fn start_with_scoped_read(
        settings: CodexSettings,
        durable_sink: mpsc::Sender<CodexEvent>,
        scoped_read: Option<ScopedReadRuntime>,
    ) -> Result<Self, CodexError> {
        let compatibility = probe_compatibility(&settings).await?;
        if !compatibility.version_match || !compatibility.schema_match {
            return Err(CodexError::Compatibility(compatibility));
        }

        let mut command = if let Some(scope) = scoped_read.as_ref() {
            scoped_read_command(&settings, scope)?
        } else {
            let mut command = Command::new(&settings.binary);
            command
                .arg("app-server")
                // App Server can materialize the first command sandbox from the
                // thread-start defaults before a turn-level network override is
                // reflected in turn context. Start workspace-write threads with
                // network available, then let Harness narrow non-GitHub turns back
                // to networkAccess=false in turn/start.
                .arg("--config")
                .arg("sandbox_workspace_write.network_access=true")
                .arg("--listen")
                .arg("stdio://")
                // Harness persists App Server stderr diagnostics. Do not inherit a
                // broad operator RUST_LOG filter that can turn span enter/exit
                // telemetry into an unbounded durable event stream.
                .env("RUST_LOG", "warn");
            if let Some(codex_home) = &settings.codex_home {
                command.env("CODEX_HOME", codex_home);
            }
            if let Some(github_config) = host_github_config_dir() {
                // Point every ordinary App Server account at the host's existing
                // gh store. Scoped-read runtimes deliberately never receive it:
                // their network namespace is absent and their only authority is
                // the mounted repository evidence.
                command.env("GH_CONFIG_DIR", github_config);
            }
            command
        };
        command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        command.process_group(0);
        let mut child = command.spawn().map_err(|source| CodexError::Spawn {
            binary: settings.binary.clone(),
            source,
        })?;
        let pid = child.id().ok_or(CodexError::MissingPid)?;
        let stdin = child.stdin.take().ok_or(CodexError::MissingPipe("stdin"))?;
        let stdout = child
            .stdout
            .take()
            .ok_or(CodexError::MissingPipe("stdout"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or(CodexError::MissingPipe("stderr"))?;

        let (writer_tx, writer_rx) = mpsc::channel::<Value>(256);
        let (events, _) = broadcast::channel(2_048);
        let pending = Arc::new(Mutex::new(HashMap::new()));
        let server_requests = Arc::new(Mutex::new(BTreeSet::new()));
        let stderr_tail = Arc::new(Mutex::new(VecDeque::with_capacity(MAX_STDERR_TAIL_BYTES)));
        let child = Arc::new(Mutex::new(Some(child)));
        let status = Arc::new(RwLock::new(CodexRuntimeStatus {
            state: "starting".to_owned(),
            detail: Some("initializing Codex App Server".to_owned()),
            version: Some(compatibility.version.clone()),
            required_version: compatibility.required_version.clone(),
            protocol_schema_sha256: Some(compatibility.schema_sha256.clone()),
            schema_match: compatibility.schema_match,
            native_multi_agent: false,
            native_multi_agent_feature: None,
            pid: Some(pid),
            restart_count: 0,
        }));
        let tasks = Arc::new(Mutex::new(Vec::new()));

        let writer_task = tokio::spawn(writer_loop(
            stdin,
            writer_rx,
            events.clone(),
            durable_sink.clone(),
        ));
        let reader_task = tokio::spawn(reader_loop(
            stdout,
            pending.clone(),
            server_requests.clone(),
            events.clone(),
            durable_sink.clone(),
            status.clone(),
            child.clone(),
        ));
        let stderr_task = tokio::spawn(stderr_loop(
            stderr,
            stderr_tail.clone(),
            events.clone(),
            durable_sink.clone(),
        ));
        let watcher_task = tokio::spawn(child_watcher(
            pid,
            child.clone(),
            pending.clone(),
            status.clone(),
            events.clone(),
            durable_sink,
        ));
        tasks
            .lock()
            .await
            .extend([writer_task, reader_task, stderr_task, watcher_task]);

        let supervisor = Self {
            settings: Arc::new(settings),
            compatibility,
            pid,
            next_id: Arc::new(AtomicU64::new(1)),
            writer: writer_tx,
            pending,
            server_requests,
            status,
            stderr_tail,
            events,
            child,
            tasks,
        };
        if let Err(error) = supervisor.initialize().await {
            let _ = supervisor.shutdown().await;
            return Err(error);
        }
        let multi_agent = supervisor.native_multi_agent_capability().await;
        {
            let mut status = supervisor.status.write().await;
            status.state = "ready".to_owned();
            match multi_agent {
                Ok(Some(feature)) => {
                    status.native_multi_agent = true;
                    status.native_multi_agent_feature = Some(feature.clone());
                    status.detail = Some(format!(
                        "App Server initialized; native multi-agent ready via {feature}"
                    ));
                }
                Ok(None) => {
                    status.detail =
                        Some("App Server initialized; native multi-agent is disabled".to_owned());
                }
                Err(error) => {
                    status.detail = Some(format!(
                        "App Server initialized; native multi-agent capability probe failed: {error}"
                    ));
                }
            }
        }
        info!(pid, version = %supervisor.compatibility.version, "Codex App Server ready");
        Ok(supervisor)
    }

    async fn initialize(&self) -> Result<(), CodexError> {
        self.request(
            "initialize",
            json!({
                "clientInfo": {
                    "name": self.settings.service_name,
                    "title": "BILDR",
                    "version": env!("CARGO_PKG_VERSION")
                },
                "capabilities": {
                    "experimentalApi": self.settings.experimental_api
                }
            }),
        )
        .await?;
        self.notify("initialized", json!({})).await
    }

    async fn native_multi_agent_capability(&self) -> Result<Option<String>, CodexError> {
        let response = self
            .request(
                "experimentalFeature/list",
                json!({"cursor": null, "limit": 100, "threadId": null}),
            )
            .await?;
        select_native_multi_agent_feature(&response)
    }

    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<CodexEvent> {
        self.events.subscribe()
    }

    #[must_use]
    pub fn compatibility(&self) -> &Compatibility {
        &self.compatibility
    }

    #[must_use]
    pub fn pid(&self) -> u32 {
        self.pid
    }

    pub async fn status(&self) -> CodexRuntimeStatus {
        self.status.read().await.clone()
    }

    pub async fn stderr_tail(&self) -> String {
        let bytes = self
            .stderr_tail
            .lock()
            .await
            .iter()
            .copied()
            .collect::<Vec<_>>();
        String::from_utf8_lossy(&bytes).into_owned()
    }

    pub async fn set_restart_count(&self, restart_count: u32) {
        self.status.write().await.restart_count = restart_count;
    }

    pub async fn account_profile(&self, mut profile: CodexAccountProfile) -> CodexAccountProfile {
        let account = self
            .request(
                "account/read",
                json!({
                    "refreshToken": false,
                }),
            )
            .await;
        match account {
            Ok(response) => {
                let account = response.get("account");
                let account_type = account
                    .and_then(|value| value.get("type"))
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned);
                let email = account
                    .and_then(|value| value.get("email"))
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned);
                let plan_type = account
                    .and_then(|value| value.get("planType"))
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned);
                if account_type.is_some() {
                    profile.account_type = account_type;
                }
                if email.is_some() {
                    profile.email = email;
                }
                if plan_type.is_some() {
                    profile.plan_type = plan_type;
                }
                profile.state = if account.is_some_and(|value| !value.is_null()) {
                    "ready".to_owned()
                } else {
                    "signed_out".to_owned()
                };
            }
            Err(error) => return account_read_failed(profile, &error.to_string()),
        }

        match self.request("account/rateLimits/read", Value::Null).await {
            Ok(response) => {
                profile.rate_limits = parse_rate_limits(&response);
                if profile.plan_type.is_none() {
                    profile.plan_type = profile
                        .rate_limits
                        .iter()
                        .find_map(|limit| limit.plan_type.clone());
                }
            }
            Err(error) => return rate_limit_refresh_failed(profile, &error.to_string()),
        }
        profile.observed_at = Some(harness_domain::now_ms());
        profile
    }

    pub async fn request(&self, method: &str, params: Value) -> Result<Value, CodexError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let id_value = Value::from(id);
        let key = id.to_string();
        let message = json!({"method": method, "id": id_value, "params": params});
        let (response_tx, response_rx) = oneshot::channel();
        self.pending.lock().await.insert(
            key.clone(),
            PendingRequest {
                method: method.to_owned(),
                response: response_tx,
            },
        );
        if let Err(error) =
            enqueue_writer_message(&self.writer, method, message, WRITER_ENQUEUE_TIMEOUT).await
        {
            self.pending.lock().await.remove(&key);
            return Err(error);
        }
        match timeout(self.settings.request_timeout, response_rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(CodexError::Disconnected),
            Err(_) => {
                self.pending.lock().await.remove(&key);
                Err(CodexError::Timeout {
                    method: method.to_owned(),
                })
            }
        }
    }

    pub async fn notify(&self, method: &str, params: Value) -> Result<(), CodexError> {
        enqueue_writer_message(
            &self.writer,
            method,
            json!({"method": method, "params": params}),
            WRITER_ENQUEUE_TIMEOUT,
        )
        .await
    }

    pub async fn respond(&self, id: Value, result: Value) -> Result<(), CodexError> {
        enqueue_server_response(
            &self.writer,
            &self.server_requests,
            id.clone(),
            json!({"id": id, "result": result}),
            WRITER_ENQUEUE_TIMEOUT,
        )
        .await
    }

    pub async fn respond_error(
        &self,
        id: Value,
        code: i64,
        message: &str,
    ) -> Result<(), CodexError> {
        enqueue_server_response(
            &self.writer,
            &self.server_requests,
            id.clone(),
            json!({"id": id, "error": {"code": code, "message": message}}),
            WRITER_ENQUEUE_TIMEOUT,
        )
        .await
    }

    pub async fn shutdown(&self) -> Result<(), CodexError> {
        {
            let mut status = self.status.write().await;
            status.state = "disabled".to_owned();
            status.detail = Some("shutting down".to_owned());
        }
        if self.child.lock().await.as_mut().is_some() {
            #[cfg(unix)]
            {
                let _ = Command::new("kill")
                    .args(["-TERM", &format!("-{}", self.pid)])
                    .status()
                    .await;
            }
            #[cfg(not(unix))]
            if let Some(child) = self.child.lock().await.as_mut() {
                let _ = child.start_kill();
            }
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
        if let Some(child) = self.child.lock().await.as_mut() {
            let _ = child.start_kill();
        }
        for task in self.tasks.lock().await.drain(..) {
            task.abort();
        }
        Ok(())
    }

    async fn owns_server_request(&self, id: &Value) -> bool {
        self.server_requests.lock().await.contains(&request_key(id))
    }
}

async fn enqueue_writer_message(
    writer: &mpsc::Sender<Value>,
    method: &str,
    message: Value,
    timeout_duration: Duration,
) -> Result<(), CodexError> {
    match timeout(timeout_duration, writer.send(message)).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err(CodexError::Disconnected),
        Err(_) => Err(CodexError::Timeout {
            method: method.to_owned(),
        }),
    }
}

async fn enqueue_server_response(
    writer: &mpsc::Sender<Value>,
    server_requests: &Mutex<BTreeSet<String>>,
    id: Value,
    message: Value,
    timeout_duration: Duration,
) -> Result<(), CodexError> {
    // Do not forget the source request until its response has entered the
    // protocol writer. If bounded admission fails, the controller can retain
    // its durable decision as pending and retry against the same request.
    enqueue_writer_message(writer, "server response", message, timeout_duration).await?;
    server_requests.lock().await.remove(&request_key(&id));
    Ok(())
}

fn scoped_read_command(
    settings: &CodexSettings,
    scope: &ScopedReadRuntime,
) -> Result<Command, CodexError> {
    if settings.codex_home.is_some() {
        // A direct read-only bind of an authenticated CODEX_HOME would give a
        // model-controlled command process access to its long-lived token
        // material. The scoped runtime needs a separate credential broker;
        // neither prompt instructions nor the generic App Server sandbox are
        // a substitute for that authority boundary.
        return Err(CodexError::ScopedReadRuntime(
            "scoped read runtime requires a credential broker and refuses a direct CODEX_HOME mount"
                .to_owned(),
        ));
    }
    scope.validate()?;
    let canonical_root = std::fs::canonicalize(&scope.source_root).map_err(|error| {
        CodexError::ScopedReadRuntime(format!(
            "could not canonicalize scoped read source root {}: {error}",
            scope.source_root.display()
        ))
    })?;
    if canonical_root != scope.source_root || !canonical_root.is_dir() {
        return Err(CodexError::ScopedReadRuntime(
            "scoped read source root must remain a canonical directory".to_owned(),
        ));
    }
    let binary = resolve_scoped_runtime_binary(&settings.binary)?;
    let mut paths = scope.allowed_relative_paths.clone();
    paths.sort();
    paths.dedup();
    if paths != scope.allowed_relative_paths {
        return Err(CodexError::ScopedReadRuntime(
            "scoped read path admission is not canonical".to_owned(),
        ));
    }
    let mut directory_mounts = BTreeSet::new();
    let mut file_mounts = Vec::with_capacity(paths.len());
    for relative in paths {
        validate_scoped_relative_path(&relative)?;
        let source = canonical_root.join(&relative);
        let metadata = std::fs::symlink_metadata(&source).map_err(|error| {
            CodexError::ScopedReadRuntime(format!(
                "scoped read source {} is unavailable: {error}",
                relative.display()
            ))
        })?;
        if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
            return Err(CodexError::ScopedReadRuntime(format!(
                "scoped read source {} must be a regular non-symlink file",
                relative.display()
            )));
        }
        let canonical_source = std::fs::canonicalize(&source).map_err(|error| {
            CodexError::ScopedReadRuntime(format!(
                "could not canonicalize scoped read source {}: {error}",
                relative.display()
            ))
        })?;
        if !canonical_source.starts_with(&canonical_root) {
            return Err(CodexError::ScopedReadRuntime(format!(
                "scoped read source {} escapes its controller root",
                relative.display()
            )));
        }
        let destination = PathBuf::from(SCOPED_READ_WORKSPACE).join(&relative);
        let mut parent = destination.parent();
        while let Some(directory) = parent {
            directory_mounts.insert(directory.to_path_buf());
            if directory == Path::new(SCOPED_READ_WORKSPACE) {
                break;
            }
            parent = directory.parent();
        }
        file_mounts.push((canonical_source, destination));
    }
    let mut args = scoped_read_bwrap_base_arguments();
    args.extend(runtime_ro_binds());
    args.extend([
        "--dir".to_owned(),
        "/opt".to_owned(),
        "--dir".to_owned(),
        "/opt/harness".to_owned(),
        "--dir".to_owned(),
        "/work".to_owned(),
    ]);
    for directory in directory_mounts {
        args.extend(["--dir".to_owned(), directory.to_string_lossy().into_owned()]);
    }
    for (source, destination) in file_mounts {
        args.extend([
            "--ro-bind".to_owned(),
            source.to_string_lossy().into_owned(),
            destination.to_string_lossy().into_owned(),
        ]);
    }
    args.extend([
        "--ro-bind".to_owned(),
        binary.to_string_lossy().into_owned(),
        SCOPED_READ_CODEX_BINARY.to_owned(),
    ]);
    args.extend([
        "--proc".to_owned(),
        "/proc".to_owned(),
        "--dev".to_owned(),
        "/dev".to_owned(),
        "--chdir".to_owned(),
        SCOPED_READ_WORKSPACE.to_owned(),
        "--".to_owned(),
        SCOPED_READ_CODEX_BINARY.to_owned(),
        "app-server".to_owned(),
        "--config".to_owned(),
        "sandbox_workspace_write.network_access=true".to_owned(),
        "--listen".to_owned(),
        "stdio://".to_owned(),
    ]);
    let mut command = Command::new(SCOPED_READ_BWRAP);
    command.args(args);
    Ok(command)
}

fn scoped_read_bwrap_base_arguments() -> Vec<String> {
    [
        "--die-with-parent",
        "--new-session",
        "--unshare-user",
        "--unshare-pid",
        "--unshare-ipc",
        "--unshare-uts",
        "--unshare-cgroup",
        "--unshare-net",
        "--cap-drop",
        "ALL",
        "--clearenv",
        "--tmpfs",
        "/tmp",
        "--tmpfs",
        "/run",
        "--setenv",
        "HOME",
        "/tmp",
        "--setenv",
        "TMPDIR",
        "/tmp",
        "--setenv",
        "XDG_CACHE_HOME",
        "/tmp",
        "--setenv",
        "XDG_CONFIG_HOME",
        "/tmp",
        "--setenv",
        "XDG_DATA_HOME",
        "/tmp",
        "--setenv",
        "XDG_STATE_HOME",
        "/tmp",
        "--setenv",
        "PATH",
        "/usr/bin:/bin",
        "--setenv",
        "RUST_LOG",
        "warn",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect()
}

fn runtime_ro_binds() -> Vec<String> {
    ["/usr", "/bin", "/lib", "/lib64"]
        .into_iter()
        .filter(|path| Path::new(path).is_dir())
        .flat_map(|path| ["--ro-bind".to_owned(), path.to_owned(), path.to_owned()])
        .collect()
}

fn resolve_scoped_runtime_binary(configured: &Path) -> Result<PathBuf, CodexError> {
    let candidate = if configured.is_absolute() || configured.components().count() > 1 {
        configured.to_path_buf()
    } else {
        let search_paths = std::env::var_os("PATH")
            .map(|paths| std::env::split_paths(&paths).collect::<Vec<_>>())
            .unwrap_or_default();
        search_paths
            .into_iter()
            .map(|directory| directory.join(configured))
            .find(|path| path.is_file())
            .ok_or_else(|| {
                CodexError::ScopedReadRuntime(format!(
                    "configured Codex binary {} is not on PATH",
                    configured.display()
                ))
            })?
    };
    let binary = std::fs::canonicalize(&candidate).map_err(|error| {
        CodexError::ScopedReadRuntime(format!(
            "could not canonicalize configured Codex binary {}: {error}",
            candidate.display()
        ))
    })?;
    if !binary.is_file() {
        return Err(CodexError::ScopedReadRuntime(
            "configured Codex binary is not a regular file".to_owned(),
        ));
    }
    Ok(binary)
}

fn rate_limit_refresh_failed(
    mut profile: CodexAccountProfile,
    detail: &str,
) -> CodexAccountProfile {
    // `account/read` may still prove that the identity is signed in, but a
    // failed limit read must never re-label an older capacity snapshot as new.
    profile.rate_limits.clear();
    profile.detail = Some(format!(
        "account rate-limit telemetry unavailable: {detail}"
    ));
    profile.observed_at = Some(harness_domain::now_ms());
    profile
}

fn account_read_failed(mut profile: CodexAccountProfile, detail: &str) -> CodexAccountProfile {
    // A failed identity read also invalidates any capacity inherited from an
    // older discovery snapshot; it is not a fresh rate-limit observation.
    profile.state = "unavailable".to_owned();
    profile.rate_limits.clear();
    profile.detail = Some(format!("account telemetry unavailable: {detail}"));
    profile.observed_at = Some(harness_domain::now_ms());
    profile
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StartThread {
    pub cwd: PathBuf,
    pub model: String,
    /// Controller-selected provider. This is explicit on every thread so a
    /// process configured with more than one provider cannot silently route a
    /// task to its ambient/default backend.
    pub model_provider: String,
    pub sandbox: String,
    pub approval_policy: String,
    pub developer_instructions: String,
    pub service_name: String,
    #[serde(default)]
    pub ephemeral: bool,
    /// Omitted for ordinary threads. When supplied, the runtime manager must
    /// launch a dedicated OS-confined App Server before forwarding this
    /// request; it is never serialized into the App Server protocol.
    #[serde(skip)]
    pub scoped_read_runtime: Option<ScopedReadRuntime>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StartTurn {
    pub thread_id: String,
    pub input: String,
    pub model: String,
    pub effort: String,
    pub cwd: PathBuf,
    pub sandbox_policy: Value,
    pub approval_policy: String,
    pub output_schema: Option<Value>,
    pub reasoning_summary: String,
}

#[async_trait]
pub trait CodexRuntime: Send + Sync {
    async fn runtime_status(&self) -> CodexRuntimeStatus;
    /// Whether this runtime can create a per-thread, operating-system-enforced
    /// scoped-read process. Ordinary App Server `readOnly` sandbox settings do
    /// not satisfy this requirement because they carry neither a readable-root
    /// restriction nor read-event evidence.
    async fn supports_scoped_read_runtime(&self) -> bool {
        false
    }
    async fn codex_accounts(&self) -> Result<CodexAccountsSnapshot, CodexError> {
        Ok(CodexAccountsSnapshot {
            selected_account_id: None,
            accounts: Vec::new(),
        })
    }
    /// Refresh all account telemetry. Implementations may coalesce ordinary
    /// dashboard refreshes; `force` is reserved for an explicit user refresh.
    async fn refresh_codex_accounts(
        &self,
        force: bool,
    ) -> Result<CodexAccountsSnapshot, CodexError> {
        let _ = force;
        self.codex_accounts().await
    }
    async fn select_codex_account(
        &self,
        _account_id: &str,
    ) -> Result<CodexAccountsSnapshot, CodexError> {
        Err(CodexError::AccountSwitchUnsupported)
    }
    async fn start_thread(&self, request: StartThread) -> Result<Value, CodexError>;
    async fn resume_thread(&self, thread_id: &str) -> Result<Value, CodexError>;
    async fn start_turn(&self, request: StartTurn) -> Result<Value, CodexError>;
    async fn steer_turn(
        &self,
        thread_id: &str,
        turn_id: &str,
        message: &str,
    ) -> Result<Value, CodexError>;
    async fn interrupt_turn(&self, thread_id: &str, turn_id: &str) -> Result<Value, CodexError>;
    /// Delete an unbound native child thread when the controller cannot issue
    /// it a model/provider custody receipt.  This is distinct from interrupting
    /// the parent turn: a child may already be independently live.
    async fn delete_thread(&self, _thread_id: &str) -> Result<Value, CodexError> {
        Err(CodexError::Protocol(
            "runtime does not support thread/delete for native-child containment".to_owned(),
        ))
    }
    async fn set_goal(
        &self,
        thread_id: &str,
        objective: &str,
        token_budget: Option<u64>,
    ) -> Result<Value, CodexError>;
    async fn start_review(
        &self,
        thread_id: &str,
        target: Value,
        detached: bool,
    ) -> Result<Value, CodexError>;
    async fn respond_rpc(&self, id: Value, result: Value) -> Result<(), CodexError>;
    async fn respond_rpc_error(
        &self,
        id: Value,
        code: i64,
        message: &str,
    ) -> Result<(), CodexError>;
}

#[async_trait]
impl CodexRuntime for CodexSupervisor {
    async fn runtime_status(&self) -> CodexRuntimeStatus {
        self.status().await
    }

    async fn start_thread(&self, request: StartThread) -> Result<Value, CodexError> {
        self.request("thread/start", start_thread_params(request))
            .await
    }

    async fn resume_thread(&self, thread_id: &str) -> Result<Value, CodexError> {
        self.request("thread/resume", json!({"threadId": thread_id}))
            .await
    }

    async fn start_turn(&self, request: StartTurn) -> Result<Value, CodexError> {
        let mut params = json!({
            "threadId": request.thread_id,
            "input": [{"type": "text", "text": request.input}],
            "model": request.model,
            "effort": request.effort,
            "cwd": request.cwd,
            "sandboxPolicy": request.sandbox_policy,
            "approvalPolicy": request.approval_policy,
            "approvalsReviewer": "user",
            "summary": request.reasoning_summary,
        });
        if let Some(schema) = request.output_schema {
            params["outputSchema"] = schema;
        }
        self.request("turn/start", params).await
    }

    async fn steer_turn(
        &self,
        thread_id: &str,
        turn_id: &str,
        message: &str,
    ) -> Result<Value, CodexError> {
        self.request(
            "turn/steer",
            json!({
                "threadId": thread_id,
                "expectedTurnId": turn_id,
                "input": [{"type": "text", "text": message}],
            }),
        )
        .await
    }

    async fn interrupt_turn(&self, thread_id: &str, turn_id: &str) -> Result<Value, CodexError> {
        self.request(
            "turn/interrupt",
            json!({"threadId": thread_id, "turnId": turn_id}),
        )
        .await
    }

    async fn delete_thread(&self, thread_id: &str) -> Result<Value, CodexError> {
        self.request("thread/delete", json!({"threadId": thread_id}))
            .await
    }

    async fn set_goal(
        &self,
        thread_id: &str,
        objective: &str,
        token_budget: Option<u64>,
    ) -> Result<Value, CodexError> {
        self.request(
            "thread/goal/set",
            json!({
                "threadId": thread_id,
                "objective": objective,
                "status": "active",
                "tokenBudget": token_budget,
            }),
        )
        .await
    }

    async fn start_review(
        &self,
        thread_id: &str,
        target: Value,
        detached: bool,
    ) -> Result<Value, CodexError> {
        self.request(
            "review/start",
            json!({
                "threadId": thread_id,
                "delivery": if detached { "detached" } else { "inline" },
                "target": target,
            }),
        )
        .await
    }

    async fn respond_rpc(&self, id: Value, result: Value) -> Result<(), CodexError> {
        self.respond(id, result).await
    }

    async fn respond_rpc_error(
        &self,
        id: Value,
        code: i64,
        message: &str,
    ) -> Result<(), CodexError> {
        self.respond_error(id, code, message).await
    }
}

fn start_thread_params(request: StartThread) -> Value {
    json!({
        "cwd": request.cwd,
        "model": request.model,
        "modelProvider": request.model_provider,
        "sandbox": request.sandbox,
        "approvalPolicy": request.approval_policy,
        "approvalsReviewer": "user",
        "developerInstructions": request.developer_instructions,
        "serviceName": request.service_name,
        "ephemeral": request.ephemeral,
    })
}

#[derive(Clone)]
struct ScopedSupervisor {
    supervisor: Arc<CodexSupervisor>,
    virtual_cwd: PathBuf,
}

#[derive(Clone)]
pub struct CodexRuntimeManager {
    settings: Arc<CodexSettings>,
    durable_sink: mpsc::Sender<CodexEvent>,
    profiles: Arc<RwLock<Vec<CodexAccountProfile>>>,
    active_account_id: Arc<RwLock<Option<String>>>,
    active: Arc<RwLock<Option<Arc<CodexSupervisor>>>>,
    scoped: Arc<RwLock<HashMap<String, ScopedSupervisor>>>,
    switch_lock: Arc<Mutex<()>>,
    account_telemetry_refreshing: Arc<AtomicBool>,
    restart_count: Arc<AtomicU32>,
}

impl CodexRuntimeManager {
    pub async fn start(
        settings: CodexSettings,
        durable_sink: mpsc::Sender<CodexEvent>,
        preferred_account_id: Option<&str>,
    ) -> Result<Self, CodexError> {
        let profiles = discover_codex_accounts(
            settings.codex_home.as_deref(),
            settings.managed_account_root.as_deref(),
        )
        .await?;
        let mut selected = preferred_account_id
            .and_then(|id| profiles.iter().find(|profile| profile.id == id))
            .or_else(|| {
                settings.codex_home.as_ref().and_then(|home| {
                    profiles
                        .iter()
                        .find(|profile| profile.codex_home.as_path() == home.as_path())
                })
            })
            .or_else(|| profiles.first())
            .cloned()
            .ok_or(CodexError::NoCodexAccountHomes)?;
        selected.selected = true;
        let mut selected_settings = settings.clone();
        selected_settings.codex_home = Some(selected.codex_home.clone());
        let supervisor =
            Arc::new(CodexSupervisor::start(selected_settings, durable_sink.clone()).await?);
        let selected = supervisor.account_profile(selected).await;
        let selected_id = selected.id.clone();
        let profiles = profiles
            .into_iter()
            .map(|profile| {
                if profile.id == selected_id {
                    selected.clone()
                } else {
                    profile
                }
            })
            .collect();
        Ok(Self {
            settings: Arc::new(settings),
            durable_sink,
            profiles: Arc::new(RwLock::new(profiles)),
            active_account_id: Arc::new(RwLock::new(Some(selected_id))),
            active: Arc::new(RwLock::new(Some(supervisor))),
            scoped: Arc::new(RwLock::new(HashMap::new())),
            switch_lock: Arc::new(Mutex::new(())),
            account_telemetry_refreshing: Arc::new(AtomicBool::new(false)),
            restart_count: Arc::new(AtomicU32::new(0)),
        })
    }

    pub async fn active_pid(&self) -> Option<u32> {
        self.active
            .read()
            .await
            .as_ref()
            .map(|supervisor| supervisor.pid())
    }

    pub async fn restart_active(&self) -> Result<(), CodexError> {
        let _guard = self.switch_lock.lock().await;
        let account_id = self
            .active_account_id
            .read()
            .await
            .clone()
            .ok_or(CodexError::NoCodexAccountHomes)?;
        let profile = self
            .profiles
            .read()
            .await
            .iter()
            .find(|profile| profile.id == account_id)
            .cloned()
            .ok_or_else(|| CodexError::UnknownAccount(account_id.clone()))?;
        let mut settings = self.settings.as_ref().clone();
        settings.codex_home = Some(profile.codex_home.clone());
        let supervisor =
            Arc::new(CodexSupervisor::start(settings, self.durable_sink.clone()).await?);
        let restart_count = self.restart_count.fetch_add(1, Ordering::AcqRel) + 1;
        supervisor.set_restart_count(restart_count).await;
        let telemetry = supervisor.account_profile(profile).await;
        *self.active.write().await = Some(supervisor);
        self.replace_profile(telemetry).await;
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), CodexError> {
        let scoped = std::mem::take(&mut *self.scoped.write().await)
            .into_values()
            .collect::<Vec<_>>();
        for runtime in scoped {
            runtime.supervisor.shutdown().await?;
        }
        if let Some(supervisor) = self.active.write().await.take() {
            supervisor.shutdown().await?;
        }
        Ok(())
    }

    async fn active_supervisor(&self) -> Result<Arc<CodexSupervisor>, CodexError> {
        self.active
            .read()
            .await
            .clone()
            .ok_or(CodexError::Disconnected)
    }

    async fn selected_settings(&self) -> Result<CodexSettings, CodexError> {
        let account_id = self
            .active_account_id
            .read()
            .await
            .clone()
            .ok_or(CodexError::NoCodexAccountHomes)?;
        let profile = self
            .profiles
            .read()
            .await
            .iter()
            .find(|profile| profile.id == account_id)
            .cloned()
            .ok_or(CodexError::UnknownAccount(account_id))?;
        let mut settings = self.settings.as_ref().clone();
        settings.codex_home = Some(profile.codex_home);
        Ok(settings)
    }

    async fn scoped_supervisor(&self, thread_id: &str) -> Option<ScopedSupervisor> {
        self.scoped.read().await.get(thread_id).cloned()
    }

    async fn start_scoped_thread(
        &self,
        mut request: StartThread,
        scope: ScopedReadRuntime,
    ) -> Result<Value, CodexError> {
        if request.sandbox != "read-only" || request.approval_policy != "never" {
            return Err(CodexError::ScopedReadRuntime(
                "scoped read runtime requires read-only sandbox and never approval policy"
                    .to_owned(),
            ));
        }
        let request_cwd = std::fs::canonicalize(&request.cwd).map_err(|error| {
            CodexError::ScopedReadRuntime(format!(
                "could not canonicalize scoped thread cwd {}: {error}",
                request.cwd.display()
            ))
        })?;
        if request_cwd != scope.source_root {
            return Err(CodexError::ScopedReadRuntime(
                "scoped read runtime root does not match the controller thread cwd".to_owned(),
            ));
        }
        let supervisor = Arc::new(
            CodexSupervisor::start_scoped_read(
                self.selected_settings().await?,
                self.durable_sink.clone(),
                scope,
            )
            .await?,
        );
        request.cwd = ScopedReadRuntime::virtual_cwd();
        request.scoped_read_runtime = None;
        let result = supervisor.start_thread(request).await;
        let thread_id = result
            .as_ref()
            .ok()
            .and_then(thread_id_from_start_response)
            .map(ToOwned::to_owned);
        match (result, thread_id) {
            (Ok(result), Some(thread_id)) => {
                self.scoped.write().await.insert(
                    thread_id,
                    ScopedSupervisor {
                        supervisor,
                        virtual_cwd: ScopedReadRuntime::virtual_cwd(),
                    },
                );
                Ok(result)
            }
            (Ok(_), None) => {
                let _ = supervisor.shutdown().await;
                Err(CodexError::Protocol(
                    "scoped App Server thread/start response lacks thread id".to_owned(),
                ))
            }
            (Err(error), _) => {
                let _ = supervisor.shutdown().await;
                Err(error)
            }
        }
    }

    async fn scoped_read_runtime_available(&self) -> bool {
        // The active account home contains long-lived authentication. Until a
        // dedicated broker proves a one-way, scoped credential handoff, do not
        // turn a filesystem boundary into a credential-exfiltration boundary.
        if self
            .selected_settings()
            .await
            .map_or(true, |settings| settings.codex_home.is_some())
        {
            return false;
        }
        if !Path::new(SCOPED_READ_BWRAP).is_file() {
            return false;
        }
        let mut command = Command::new(SCOPED_READ_BWRAP);
        command
            .args(scoped_read_bwrap_base_arguments())
            .args(runtime_ro_binds())
            .args(["--proc", "/proc", "--dev", "/dev", "--", "/usr/bin/true"])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        matches!(
            timeout(Duration::from_secs(5), command.status()).await,
            Ok(Ok(status)) if status.success()
        )
    }

    async fn owner_for_server_request(
        &self,
        id: &Value,
    ) -> Result<Arc<CodexSupervisor>, CodexError> {
        let mut matches = Vec::new();
        if let Ok(active) = self.active_supervisor().await
            && active.owns_server_request(id).await
        {
            matches.push(active);
        }
        let scoped = self
            .scoped
            .read()
            .await
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for runtime in scoped {
            if runtime.supervisor.owns_server_request(id).await {
                matches.push(runtime.supervisor);
            }
        }
        match matches.len() {
            1 => Ok(matches.remove(0)),
            0 => Err(CodexError::Protocol(
                "App Server response id is not owned by an active runtime".to_owned(),
            )),
            _ => Err(CodexError::Protocol(
                "App Server response id is ambiguous across runtimes".to_owned(),
            )),
        }
    }

    async fn refresh_discovery(&self) -> Result<(), CodexError> {
        let active_home = self
            .profiles
            .read()
            .await
            .iter()
            .find(|profile| profile.selected)
            .map(|profile| profile.codex_home.clone())
            .or_else(|| self.settings.codex_home.clone());
        let discovered = discover_codex_accounts(
            active_home.as_deref(),
            self.settings.managed_account_root.as_deref(),
        )
        .await?;
        let existing = self
            .profiles
            .read()
            .await
            .iter()
            .cloned()
            .map(|profile| (profile.id.clone(), profile))
            .collect::<BTreeMap<_, _>>();
        let selected = self.active_account_id.read().await.clone();
        *self.profiles.write().await = discovered
            .into_iter()
            .map(|mut profile| {
                if let Some(prior) = existing.get(&profile.id) {
                    let prior_is_newer = prior.observed_at.unwrap_or_default()
                        >= profile.observed_at.unwrap_or_default();
                    if prior_is_newer {
                        let discovered_label = profile.label;
                        profile = prior.clone();
                        if discovered_label != account_label(&profile.codex_home) {
                            profile.label = discovered_label;
                        }
                    }
                }
                profile.selected = selected.as_deref() == Some(profile.id.as_str());
                profile
            })
            .collect();
        Ok(())
    }

    async fn replace_profile(&self, profile: CodexAccountProfile) {
        let selected_account_id = self.active_account_id.read().await.clone();
        let mut profiles = self.profiles.write().await;
        for current in profiles.iter_mut() {
            if current.id == profile.id {
                let mut replacement = profile.clone();
                replacement.selected =
                    selected_account_id.as_deref() == Some(replacement.id.as_str());
                *current = replacement;
            } else {
                current.selected = selected_account_id.as_deref() == Some(current.id.as_str());
            }
        }
    }

    async fn probe_account_profile(&self, profile: CodexAccountProfile) -> CodexAccountProfile {
        let mut settings = self.settings.as_ref().clone();
        settings.codex_home = Some(profile.codex_home.clone());
        // Its only account-specific requests are account/read (without token
        // refresh) and account/rateLimits/read. It deliberately has no durable
        // event sink, so observing a non-active account cannot alter run
        // history or the active account selection.
        let (sink, mut events) = mpsc::channel(32);
        let drain = tokio::spawn(async move { while events.recv().await.is_some() {} });
        let result = match CodexSupervisor::start(settings, sink).await {
            Ok(supervisor) => {
                let telemetry = supervisor.account_profile(profile).await;
                let _ = supervisor.shutdown().await;
                telemetry
            }
            Err(error) => CodexAccountProfile {
                state: "unavailable".to_owned(),
                rate_limits: Vec::new(),
                observed_at: Some(harness_domain::now_ms()),
                detail: Some(format!("account limit telemetry unavailable: {error}")),
                ..profile
            },
        };
        drain.abort();
        result
    }

    async fn refresh_profiles(&self, force: bool) -> Result<(), CodexError> {
        let selected_id = self.active_account_id.read().await.clone();
        let now = harness_domain::now_ms();
        let profiles = self.profiles.read().await.clone();
        for profile in profiles {
            let due = force
                || profile.observed_at.is_none_or(|observed| {
                    now.saturating_sub(observed) >= ACCOUNT_TELEMETRY_REFRESH_INTERVAL_MS
                });
            if !due {
                continue;
            }
            let telemetry = if Some(profile.id.as_str()) == selected_id.as_deref() {
                self.active_supervisor()
                    .await?
                    .account_profile(profile)
                    .await
            } else {
                self.probe_account_profile(profile).await
            };
            self.replace_profile(telemetry).await;
        }
        Ok(())
    }

    fn queue_passive_profile_refresh(&self) {
        if self
            .account_telemetry_refreshing
            .swap(true, Ordering::AcqRel)
        {
            return;
        }
        let manager = self.clone();
        tokio::spawn(async move {
            // Telemetry probes use a snapshot of profiles and replace only
            // their observation fields. They never alter the active App
            // Server, so holding the switch lock here would let a routine
            // dashboard refresh make an explicit account change appear frozen.
            if let Err(error) = manager.refresh_profiles(false).await {
                warn!(%error, "passive Codex account telemetry refresh failed");
            }
            manager
                .account_telemetry_refreshing
                .store(false, Ordering::Release);
        });
    }

    async fn accounts_snapshot(
        &self,
        refresh: bool,
        force: bool,
    ) -> Result<CodexAccountsSnapshot, CodexError> {
        self.refresh_discovery().await?;
        if refresh && force {
            let _guard = self.switch_lock.lock().await;
            self.refresh_discovery().await?;
            self.refresh_profiles(true).await?;
        } else if refresh {
            self.queue_passive_profile_refresh();
        }
        Ok(CodexAccountsSnapshot {
            selected_account_id: self.active_account_id.read().await.clone(),
            accounts: self.profiles.read().await.clone(),
        })
    }

    async fn switch_account(&self, account_id: &str) -> Result<CodexAccountsSnapshot, CodexError> {
        let _guard = self.switch_lock.lock().await;
        self.refresh_discovery().await?;
        let profile = self
            .profiles
            .read()
            .await
            .iter()
            .find(|profile| profile.id == account_id)
            .cloned()
            .ok_or_else(|| CodexError::UnknownAccount(account_id.to_owned()))?;
        let mut settings = self.settings.as_ref().clone();
        settings.codex_home = Some(profile.codex_home.clone());
        let replacement =
            Arc::new(CodexSupervisor::start(settings, self.durable_sink.clone()).await?);
        let telemetry = replacement.account_profile(profile).await;
        let old = self.active.write().await.replace(replacement);
        *self.active_account_id.write().await = Some(account_id.to_owned());
        self.restart_count.store(0, Ordering::Release);
        self.replace_profile(telemetry).await;
        if let Some(old) = old {
            old.shutdown().await?;
        }
        self.accounts_snapshot(false, false).await
    }
}

#[async_trait]
impl CodexRuntime for CodexRuntimeManager {
    async fn runtime_status(&self) -> CodexRuntimeStatus {
        match self.active_supervisor().await {
            Ok(supervisor) => supervisor.runtime_status().await,
            Err(error) => CodexRuntimeStatus {
                state: "unavailable".to_owned(),
                detail: Some(error.to_string()),
                version: None,
                required_version: self.settings.required_version.clone(),
                protocol_schema_sha256: None,
                schema_match: false,
                native_multi_agent: false,
                native_multi_agent_feature: None,
                pid: None,
                restart_count: self.restart_count.load(Ordering::Acquire),
            },
        }
    }

    async fn supports_scoped_read_runtime(&self) -> bool {
        self.scoped_read_runtime_available().await
    }

    async fn codex_accounts(&self) -> Result<CodexAccountsSnapshot, CodexError> {
        self.accounts_snapshot(true, false).await
    }

    async fn refresh_codex_accounts(
        &self,
        force: bool,
    ) -> Result<CodexAccountsSnapshot, CodexError> {
        self.accounts_snapshot(true, force).await
    }

    async fn select_codex_account(
        &self,
        account_id: &str,
    ) -> Result<CodexAccountsSnapshot, CodexError> {
        self.switch_account(account_id).await
    }

    async fn start_thread(&self, request: StartThread) -> Result<Value, CodexError> {
        if let Some(scope) = request.scoped_read_runtime.clone() {
            return self.start_scoped_thread(request, scope).await;
        }
        self.active_supervisor().await?.start_thread(request).await
    }

    async fn resume_thread(&self, thread_id: &str) -> Result<Value, CodexError> {
        if let Some(runtime) = self.scoped_supervisor(thread_id).await {
            return runtime.supervisor.resume_thread(thread_id).await;
        }
        self.active_supervisor()
            .await?
            .resume_thread(thread_id)
            .await
    }

    async fn start_turn(&self, request: StartTurn) -> Result<Value, CodexError> {
        if let Some(runtime) = self.scoped_supervisor(&request.thread_id).await {
            let mut request = request;
            request.cwd = runtime.virtual_cwd;
            return runtime.supervisor.start_turn(request).await;
        }
        self.active_supervisor().await?.start_turn(request).await
    }

    async fn steer_turn(
        &self,
        thread_id: &str,
        turn_id: &str,
        message: &str,
    ) -> Result<Value, CodexError> {
        if let Some(runtime) = self.scoped_supervisor(thread_id).await {
            return runtime
                .supervisor
                .steer_turn(thread_id, turn_id, message)
                .await;
        }
        self.active_supervisor()
            .await?
            .steer_turn(thread_id, turn_id, message)
            .await
    }

    async fn interrupt_turn(&self, thread_id: &str, turn_id: &str) -> Result<Value, CodexError> {
        if let Some(runtime) = self.scoped_supervisor(thread_id).await {
            return runtime.supervisor.interrupt_turn(thread_id, turn_id).await;
        }
        self.active_supervisor()
            .await?
            .interrupt_turn(thread_id, turn_id)
            .await
    }

    async fn delete_thread(&self, thread_id: &str) -> Result<Value, CodexError> {
        if let Some(runtime) = self.scoped_supervisor(thread_id).await {
            return runtime.supervisor.delete_thread(thread_id).await;
        }
        self.active_supervisor()
            .await?
            .delete_thread(thread_id)
            .await
    }

    async fn set_goal(
        &self,
        thread_id: &str,
        objective: &str,
        token_budget: Option<u64>,
    ) -> Result<Value, CodexError> {
        if let Some(runtime) = self.scoped_supervisor(thread_id).await {
            return runtime
                .supervisor
                .set_goal(thread_id, objective, token_budget)
                .await;
        }
        self.active_supervisor()
            .await?
            .set_goal(thread_id, objective, token_budget)
            .await
    }

    async fn start_review(
        &self,
        thread_id: &str,
        target: Value,
        detached: bool,
    ) -> Result<Value, CodexError> {
        if let Some(runtime) = self.scoped_supervisor(thread_id).await {
            return runtime
                .supervisor
                .start_review(thread_id, target, detached)
                .await;
        }
        self.active_supervisor()
            .await?
            .start_review(thread_id, target, detached)
            .await
    }

    async fn respond_rpc(&self, id: Value, result: Value) -> Result<(), CodexError> {
        self.owner_for_server_request(&id)
            .await?
            .respond_rpc(id, result)
            .await
    }

    async fn respond_rpc_error(
        &self,
        id: Value,
        code: i64,
        message: &str,
    ) -> Result<(), CodexError> {
        self.owner_for_server_request(&id)
            .await?
            .respond_rpc_error(id, code, message)
            .await
    }
}

fn select_native_multi_agent_feature(response: &Value) -> Result<Option<String>, CodexError> {
    let features = response
        .get("data")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            CodexError::Protocol("experimentalFeature/list response lacks data".to_owned())
        })?;
    for name in ["multi_agent_v2", "multi_agent"] {
        if features.iter().any(|feature| {
            feature.get("name").and_then(Value::as_str) == Some(name)
                && feature.get("enabled").and_then(Value::as_bool) == Some(true)
                && !matches!(
                    feature.get("stage").and_then(Value::as_str),
                    Some("deprecated" | "removed")
                )
        }) {
            return Ok(Some(name.to_owned()));
        }
    }
    Ok(None)
}

fn parse_rate_limits(response: &Value) -> Vec<CodexRateLimit> {
    let snapshots = response
        .get("rateLimitsByLimitId")
        .and_then(Value::as_object)
        .filter(|values| !values.is_empty())
        .map(|values| {
            values
                .iter()
                .map(|(id, snapshot)| (id.clone(), snapshot))
                .collect::<Vec<_>>()
        })
        .or_else(|| {
            response
                .get("rateLimits")
                .filter(|value| value.is_object())
                .map(|snapshot| {
                    vec![(
                        snapshot
                            .get("limitId")
                            .and_then(Value::as_str)
                            .unwrap_or("codex")
                            .to_owned(),
                        snapshot,
                    )]
                })
        })
        .unwrap_or_default();
    let mut limits = snapshots
        .into_iter()
        .map(|(fallback_id, snapshot)| {
            let mut windows = Vec::new();
            for (kind, key) in [("primary", "primary"), ("secondary", "secondary")] {
                let Some(window) = snapshot.get(key).filter(|value| value.is_object()) else {
                    continue;
                };
                let used_percent = window
                    .get("usedPercent")
                    .and_then(Value::as_u64)
                    .unwrap_or_default()
                    .min(100) as u32;
                windows.push(CodexRateLimitWindow {
                    kind: kind.to_owned(),
                    used_percent,
                    remaining_percent: 100_u32.saturating_sub(used_percent),
                    window_duration_mins: window.get("windowDurationMins").and_then(Value::as_u64),
                    resets_at: window.get("resetsAt").and_then(Value::as_i64),
                });
            }
            CodexRateLimit {
                limit_id: snapshot
                    .get("limitId")
                    .and_then(Value::as_str)
                    .unwrap_or(&fallback_id)
                    .to_owned(),
                limit_name: snapshot
                    .get("limitName")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                plan_type: snapshot
                    .get("planType")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                windows,
            }
        })
        .collect::<Vec<_>>();
    limits.sort_by(|left, right| left.limit_id.cmp(&right.limit_id));
    limits
}

async fn discover_codex_accounts(
    configured_home: Option<&Path>,
    managed_account_root: Option<&Path>,
) -> Result<Vec<CodexAccountProfile>, CodexError> {
    let mut candidates = BTreeSet::new();
    let headroom = discover_headroom_codex_accounts().await;
    if let Some(path) = configured_home {
        candidates.insert(path.to_path_buf());
    }
    if let Some(path) = std::env::var_os("CODEX_HOME") {
        candidates.insert(PathBuf::from(path));
    }
    if let Some(paths) = std::env::var_os("HARNESS_CODEX_ACCOUNT_HOMES") {
        candidates.extend(std::env::split_paths(&paths));
    }
    if let Some(home) = std::env::var_os("HOME").map(PathBuf::from) {
        candidates.insert(home.join(".codex"));
        discover_named_account_dirs(&home, &mut candidates).await?;
        discover_named_account_dirs(&home.join(".config"), &mut candidates).await?;
    }
    if let Some(root) = managed_account_root {
        discover_managed_account_dirs(root, &mut candidates).await?;
    }
    candidates.extend(headroom.keys().cloned());

    let mut canonical = BTreeSet::new();
    for candidate in candidates {
        if canonical.len() >= 16 {
            break;
        }
        let Ok(metadata) = fs::metadata(&candidate).await else {
            continue;
        };
        if !metadata.is_dir() {
            continue;
        }
        let path = fs::canonicalize(&candidate).await?;
        canonical.insert(path);
    }
    Ok(canonical
        .into_iter()
        .map(|codex_home| {
            let metadata = headroom.get(&codex_home);
            let managed = managed_account_root.is_some_and(|root| codex_home.starts_with(root));
            CodexAccountProfile {
                id: account_id(&codex_home),
                label: metadata
                    .map(|value| value.label.clone())
                    .unwrap_or_else(|| account_label(&codex_home)),
                codex_home,
                selected: false,
                state: metadata
                    .map(|value| value.state.clone())
                    .unwrap_or_else(|| "detected".to_owned()),
                account_type: metadata.map(|_| "headroom".to_owned()),
                email: metadata.and_then(|value| value.email.clone()),
                plan_type: metadata.and_then(|value| value.plan_type.clone()),
                rate_limits: metadata
                    .map(|value| value.rate_limits.clone())
                    .unwrap_or_default(),
                observed_at: metadata.and_then(|value| value.observed_at),
                detail: metadata.map(|value| value.detail.clone()),
                managed,
            }
        })
        .collect())
}

async fn discover_managed_account_dirs(
    root: &Path,
    candidates: &mut BTreeSet<PathBuf>,
) -> Result<(), CodexError> {
    let Ok(mut entries) = fs::read_dir(root).await else {
        return Ok(());
    };
    while let Some(entry) = entries.next_entry().await? {
        if candidates.len() >= 16 {
            break;
        }
        let path = entry.path();
        if fs::metadata(path.join("auth.json")).await.is_ok() {
            candidates.insert(path);
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Default)]
struct HeadroomCodexAccount {
    label: String,
    state: String,
    email: Option<String>,
    plan_type: Option<String>,
    rate_limits: Vec<CodexRateLimit>,
    observed_at: Option<i64>,
    detail: String,
}

async fn discover_headroom_codex_accounts() -> BTreeMap<PathBuf, HeadroomCodexAccount> {
    let Some(home) = std::env::var_os("HOME").map(PathBuf::from) else {
        return BTreeMap::new();
    };
    let root = std::env::var_os("HEADROOM_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| home.join(".headroom"));
    let Ok(config_bytes) = fs::read(root.join("config.json")).await else {
        return BTreeMap::new();
    };
    let Ok(config) = serde_json::from_slice::<Value>(&config_bytes) else {
        return BTreeMap::new();
    };
    let usage = fs::read(root.join("state/usage-private.json"))
        .await
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok());
    let usage_by_name = usage
        .as_ref()
        .and_then(|value| value.get("accounts"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|account| Some((account.get("name")?.as_str()?.to_owned(), account.clone())))
        .collect::<BTreeMap<_, _>>();
    let mut accounts = BTreeMap::new();
    for account in config
        .get("accounts")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if account.get("provider").and_then(Value::as_str) != Some("codex") {
            continue;
        }
        let Some(label) = account.get("name").and_then(Value::as_str) else {
            continue;
        };
        let Some(path) = account.get("home").and_then(Value::as_str) else {
            continue;
        };
        let path = PathBuf::from(path);
        let path = if path.is_absolute() {
            path
        } else {
            home.join(path)
        };
        let Ok(path) = fs::canonicalize(path).await else {
            continue;
        };
        let snapshot = usage_by_name.get(label);
        let routable = snapshot
            .and_then(|value| value.get("routable"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let ok = snapshot
            .and_then(|value| value.get("ok"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let stale = snapshot
            .and_then(|value| value.get("stale"))
            .and_then(Value::as_bool)
            .unwrap_or(true);
        let windows = snapshot
            .and_then(|value| value.get("windows"))
            .and_then(Value::as_object);
        let mut rate_limits = windows
            .into_iter()
            .flat_map(|values| values.iter())
            .filter_map(|(id, window)| {
                let used_percent = window.get("used_percent")?.as_f64()?.clamp(0.0, 100.0);
                Some(CodexRateLimit {
                    limit_id: id.clone(),
                    limit_name: Some(if id == "7d" {
                        "Weekly".to_owned()
                    } else {
                        id.trim_start_matches("scoped:").to_owned()
                    }),
                    plan_type: snapshot
                        .and_then(|value| value.get("plan"))
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned),
                    windows: vec![CodexRateLimitWindow {
                        kind: "primary".to_owned(),
                        used_percent: used_percent.round() as u32,
                        remaining_percent: (100.0 - used_percent).round() as u32,
                        window_duration_mins: window.get("window_minutes").and_then(Value::as_u64),
                        resets_at: window.get("resets_at").and_then(Value::as_i64),
                    }],
                })
            })
            .collect::<Vec<_>>();
        rate_limits.sort_by(|left, right| left.limit_id.cmp(&right.limit_id));
        let observed_at = windows
            .into_iter()
            .flat_map(|values| values.values())
            .filter_map(|window| window.get("observed_at").and_then(Value::as_i64))
            .max()
            .map(|seconds| seconds.saturating_mul(1_000));
        accounts.insert(
            path,
            HeadroomCodexAccount {
                label: label.to_owned(),
                state: if ok && routable && !stale {
                    "ready".to_owned()
                } else if ok {
                    "detected".to_owned()
                } else {
                    "unavailable".to_owned()
                },
                email: snapshot
                    .and_then(|value| value.get("email"))
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                plan_type: snapshot
                    .and_then(|value| value.get("plan"))
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                rate_limits,
                observed_at,
                detail: if stale {
                    "Headroom account detected; limit snapshot is stale".to_owned()
                } else if routable {
                    "Headroom account ready for the next bounded attempt".to_owned()
                } else {
                    "Headroom account is currently held or out of capacity".to_owned()
                },
            },
        );
    }
    accounts
}

async fn discover_named_account_dirs(
    parent: &Path,
    candidates: &mut BTreeSet<PathBuf>,
) -> Result<(), CodexError> {
    let Ok(mut entries) = fs::read_dir(parent).await else {
        return Ok(());
    };
    while let Some(entry) = entries.next_entry().await? {
        if candidates.len() >= 16 {
            break;
        }
        let name = entry.file_name().to_string_lossy().to_ascii_lowercase();
        if name == ".codex"
            || name.starts_with(".codex-")
            || name.starts_with(".codex_")
            || name.starts_with("codex-")
            || name.starts_with("codex_")
        {
            let path = entry.path();
            if fs::metadata(path.join("auth.json")).await.is_ok() {
                candidates.insert(path);
            }
        }
    }
    Ok(())
}

fn account_id(path: &Path) -> String {
    let digest = Sha256::digest(path.as_os_str().as_encoded_bytes());
    format!("codex-{}", &hex::encode(digest)[..12])
}

fn account_label(path: &Path) -> String {
    match path.file_name().and_then(|name| name.to_str()) {
        Some(".codex") => "codex-main".to_owned(),
        Some(name) => name.trim_start_matches('.').to_owned(),
        None => "codex-account".to_owned(),
    }
}

/// Probe the installed Codex CLI without starting a persistent App Server.
///
/// Both the semantic version and the generated root protocol schema are checked
/// because either can change the safety boundary of a controller integration.
pub async fn probe_compatibility(settings: &CodexSettings) -> Result<Compatibility, CodexError> {
    let mut version_command = Command::new(&settings.binary);
    version_command
        .arg("--version")
        .stdin(Stdio::null())
        .kill_on_drop(true);
    if let Some(codex_home) = &settings.codex_home {
        version_command.env("CODEX_HOME", codex_home);
    }
    let version_output = timeout(settings.request_timeout, version_command.output())
        .await
        .map_err(|_| CodexError::Timeout {
            method: "codex --version".to_owned(),
        })?
        .map_err(|source| CodexError::Spawn {
            binary: settings.binary.clone(),
            source,
        })?;
    if !version_output.status.success() {
        return Err(CodexError::VersionProbe(
            String::from_utf8_lossy(&version_output.stderr).into_owned(),
        ));
    }
    let version_line = String::from_utf8_lossy(&version_output.stdout)
        .trim()
        .to_owned();
    let version = version_line
        .split_whitespace()
        .last()
        .unwrap_or(&version_line)
        .to_owned();
    let version_match = settings
        .required_version
        .as_deref()
        .is_none_or(|required| required == version || version_line == required);

    fs::create_dir_all(&settings.schema_probe_root).await?;
    let probe_dir = settings
        .schema_probe_root
        .join(format!("probe-{}", ulid_like()));
    fs::create_dir(&probe_dir).await?;
    let mut schema_command = Command::new(&settings.binary);
    schema_command
        .args(["app-server", "generate-json-schema", "--out"])
        .arg(&probe_dir)
        .stdin(Stdio::null())
        .kill_on_drop(true);
    if let Some(codex_home) = &settings.codex_home {
        schema_command.env("CODEX_HOME", codex_home);
    }
    let result = timeout(settings.request_timeout, schema_command.output())
        .await
        .map_err(|_| CodexError::Timeout {
            method: "app-server generate-json-schema".to_owned(),
        });
    let schema_result = match result {
        Ok(Ok(output)) if output.status.success() => {
            let schema_path = probe_dir.join("codex_app_server_protocol.v2.schemas.json");
            let result = canonical_json_sha256_file(&schema_path).await;
            let _ = fs::remove_dir_all(&probe_dir).await;
            result
        }
        Ok(Ok(output)) => {
            let _ = fs::remove_dir_all(&probe_dir).await;
            Err(CodexError::SchemaProbe(
                String::from_utf8_lossy(&output.stderr).into_owned(),
            ))
        }
        Ok(Err(source)) => {
            let _ = fs::remove_dir_all(&probe_dir).await;
            Err(CodexError::Spawn {
                binary: settings.binary.clone(),
                source,
            })
        }
        Err(error) => {
            let _ = fs::remove_dir_all(&probe_dir).await;
            Err(error)
        }
    }?;
    let schema_match = settings
        .required_schema_sha256
        .as_deref()
        .is_none_or(|required| required == schema_result);
    Ok(Compatibility {
        version,
        required_version: settings.required_version.clone(),
        schema_sha256: schema_result,
        required_schema_sha256: settings.required_schema_sha256.clone(),
        version_match,
        schema_match,
    })
}

async fn canonical_json_sha256_file(path: &Path) -> Result<String, CodexError> {
    let bytes = fs::read(path).await?;
    canonical_json_sha256(&bytes)
}

fn canonical_json_sha256(bytes: &[u8]) -> Result<String, CodexError> {
    let value = normalize_json(serde_json::from_slice(bytes)?);
    let canonical = serde_json::to_vec(&value)?;
    Ok(hex::encode(Sha256::digest(canonical)))
}

fn normalize_json(value: Value) -> Value {
    match value {
        Value::Object(map) => {
            let mut fields = map.into_iter().collect::<Vec<_>>();
            fields.sort_unstable_by(|left, right| left.0.cmp(&right.0));
            Value::Object(
                fields
                    .into_iter()
                    .map(|(key, value)| (key, normalize_json(value)))
                    .collect(),
            )
        }
        Value::Array(values) => Value::Array(values.into_iter().map(normalize_json).collect()),
        value => value,
    }
}

fn ulid_like() -> String {
    format!(
        "{}-{}-{}",
        std::process::id(),
        harness_domain::now_ms(),
        NEXT_SCHEMA_PROBE.fetch_add(1, Ordering::Relaxed)
    )
}

async fn writer_loop(
    mut stdin: tokio::process::ChildStdin,
    mut receiver: mpsc::Receiver<Value>,
    events: broadcast::Sender<CodexEvent>,
    durable_sink: mpsc::Sender<CodexEvent>,
) {
    while let Some(message) = receiver.recv().await {
        let method = message
            .get("method")
            .and_then(Value::as_str)
            .unwrap_or("response")
            .to_owned();
        let id = message.get("id").cloned();
        let event = CodexEvent {
            direction: EventDirection::Outbound,
            kind: if method == "response" {
                EventKind::Response
            } else if id.is_some() {
                EventKind::Request
            } else {
                EventKind::Notification
            },
            method,
            request_id: id,
            message: message.clone(),
        };
        let _ = events.send(event.clone());
        let _ = durable_sink.send(event).await;
        let Ok(mut line) = serde_json::to_vec(&message) else {
            error!("failed to serialize App Server request");
            continue;
        };
        line.push(b'\n');
        if stdin.write_all(&line).await.is_err() || stdin.flush().await.is_err() {
            break;
        }
    }
}

async fn reader_loop(
    stdout: tokio::process::ChildStdout,
    pending: Arc<Mutex<HashMap<String, PendingRequest>>>,
    server_requests: Arc<Mutex<BTreeSet<String>>>,
    events: broadcast::Sender<CodexEvent>,
    durable_sink: mpsc::Sender<CodexEvent>,
    status: Arc<RwLock<CodexRuntimeStatus>>,
    child: Arc<Mutex<Option<Child>>>,
) {
    let mut reader = BufReader::new(stdout);
    let mut consecutive_protocol_errors = 0_u8;
    loop {
        let line = match read_bounded_line(&mut reader, MAX_FRAME_BYTES).await {
            Ok(Some(BoundedLine::Data(line))) => line,
            Ok(Some(BoundedLine::Oversized)) => {
                consecutive_protocol_errors = consecutive_protocol_errors.saturating_add(1);
                warn!(
                    bytes = MAX_FRAME_BYTES,
                    "discarding oversized App Server frame"
                );
                if consecutive_protocol_errors >= MAX_CONSECUTIVE_PROTOCOL_ERRORS {
                    break;
                }
                continue;
            }
            Ok(None) => break,
            Err(error) => {
                warn!(%error, "App Server stdout read failed");
                break;
            }
        };
        let message: Value = match serde_json::from_slice(&line) {
            Ok(message) => {
                consecutive_protocol_errors = 0;
                message
            }
            Err(error) => {
                consecutive_protocol_errors = consecutive_protocol_errors.saturating_add(1);
                warn!(%error, "discarding malformed App Server JSON frame");
                if consecutive_protocol_errors >= MAX_CONSECUTIVE_PROTOCOL_ERRORS {
                    break;
                }
                continue;
            }
        };
        let id = message.get("id").cloned();
        let method = message
            .get("method")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        let (kind, event_method) = if let Some(method) = method {
            (
                if id.is_some() {
                    EventKind::ServerRequest
                } else {
                    EventKind::Notification
                },
                method,
            )
        } else if let Some(id) = &id {
            let key = request_key(id);
            let method = pending
                .lock()
                .await
                .get(&key)
                .map_or_else(|| "unknown".to_owned(), |pending| pending.method.clone());
            (EventKind::Response, format!("response:{method}"))
        } else {
            (EventKind::Notification, "unknown".to_owned())
        };
        let event = CodexEvent {
            direction: EventDirection::Inbound,
            kind,
            method: event_method,
            request_id: id.clone(),
            message: message.clone(),
        };
        if kind == EventKind::ServerRequest
            && let Some(id) = id.as_ref()
        {
            server_requests.lock().await.insert(request_key(id));
        }
        let _ = events.send(event.clone());
        let _ = durable_sink.send(event).await;

        if kind == EventKind::Response
            && let Some(id) = id
            && let Some(pending_request) = pending.lock().await.remove(&request_key(&id))
        {
            let response = if let Some(error) = message.get("error") {
                Err(CodexError::Rpc {
                    method: pending_request.method,
                    code: error.get("code").and_then(Value::as_i64).unwrap_or(-1),
                    message: error
                        .get("message")
                        .and_then(Value::as_str)
                        .unwrap_or("unknown App Server error")
                        .to_owned(),
                    data: error.get("data").cloned(),
                })
            } else {
                Ok(message.get("result").cloned().unwrap_or(Value::Null))
            };
            let _ = pending_request.response.send(response);
        }
    }
    let should_terminate = {
        let mut runtime = status.write().await;
        if runtime.state == "disabled" {
            false
        } else {
            runtime.state = "unavailable".to_owned();
            runtime.detail = Some(
                if consecutive_protocol_errors >= MAX_CONSECUTIVE_PROTOCOL_ERRORS {
                    "App Server exceeded the malformed-frame budget".to_owned()
                } else {
                    "App Server stream closed".to_owned()
                },
            );
            true
        }
    };
    if should_terminate && let Some(process) = child.lock().await.as_mut() {
        let _ = process.start_kill();
    }
}

async fn stderr_loop(
    stderr: tokio::process::ChildStderr,
    tail: Arc<Mutex<VecDeque<u8>>>,
    events: broadcast::Sender<CodexEvent>,
    durable_sink: mpsc::Sender<CodexEvent>,
) {
    let mut reader = BufReader::new(stderr);
    while let Ok(Some(line)) = read_bounded_line(&mut reader, MAX_STDERR_LINE_BYTES).await {
        let line = match line {
            BoundedLine::Data(line) => redact_diagnostic_line(&String::from_utf8_lossy(&line)),
            BoundedLine::Oversized => "[oversized App Server stderr line dropped]".to_owned(),
        };
        {
            let mut tail = tail.lock().await;
            for byte in line.bytes().chain(std::iter::once(b'\n')) {
                if tail.len() == MAX_STDERR_TAIL_BYTES {
                    tail.pop_front();
                }
                tail.push_back(byte);
            }
        }
        let event = CodexEvent {
            direction: EventDirection::Diagnostic,
            kind: EventKind::Stderr,
            method: "runtime/stderr".to_owned(),
            request_id: None,
            message: json!({"line": line}),
        };
        let _ = events.send(event.clone());
        let _ = durable_sink.send(event).await;
    }
}

async fn child_watcher(
    pid: u32,
    child: Arc<Mutex<Option<Child>>>,
    pending: Arc<Mutex<HashMap<String, PendingRequest>>>,
    status: Arc<RwLock<CodexRuntimeStatus>>,
    events: broadcast::Sender<CodexEvent>,
    durable_sink: mpsc::Sender<CodexEvent>,
) {
    let exit = loop {
        let observed = {
            let mut guard = child.lock().await;
            match guard.as_mut() {
                Some(process) => process.try_wait(),
                None => return,
            }
        };
        match observed {
            Ok(Some(exit)) => {
                child.lock().await.take();
                break Ok(exit);
            }
            Ok(None) => tokio::time::sleep(Duration::from_millis(100)).await,
            Err(error) => break Err(error),
        }
    };
    let description = match exit {
        Ok(status) => format!("App Server exited with {status}"),
        Err(error) => format!("App Server wait failed: {error}"),
    };
    warn!(%description);
    {
        let mut runtime = status.write().await;
        if runtime.state != "disabled" {
            runtime.state = "unavailable".to_owned();
            runtime.detail = Some(description.clone());
            runtime.pid = None;
        }
    }
    for (_, request) in pending.lock().await.drain() {
        let _ = request.response.send(Err(CodexError::Disconnected));
    }
    let event = CodexEvent {
        direction: EventDirection::Diagnostic,
        kind: EventKind::ProcessExit,
        method: "runtime/exited".to_owned(),
        request_id: None,
        message: json!({"detail": description, "pid": pid}),
    };
    let _ = events.send(event.clone());
    let _ = durable_sink.send(event).await;
}

enum BoundedLine {
    Data(Vec<u8>),
    Oversized,
}

async fn read_bounded_line<R: AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
    max_bytes: usize,
) -> Result<Option<BoundedLine>, std::io::Error> {
    let mut bytes = Vec::with_capacity(max_bytes.min(8 * 1024));
    let mut oversized = false;
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            if bytes.is_empty() && !oversized {
                return Ok(None);
            }
            break;
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let take = newline.unwrap_or(available.len());
        if !oversized {
            if bytes.len().saturating_add(take) > max_bytes {
                bytes.clear();
                oversized = true;
            } else {
                bytes.extend_from_slice(&available[..take]);
            }
        }
        let consumed = take + usize::from(newline.is_some());
        reader.consume(consumed);
        if newline.is_some() {
            break;
        }
    }
    if oversized {
        Ok(Some(BoundedLine::Oversized))
    } else {
        if bytes.last() == Some(&b'\r') {
            bytes.pop();
        }
        Ok(Some(BoundedLine::Data(bytes)))
    }
}

fn redact_diagnostic_line(line: &str) -> String {
    let lower = line.to_ascii_lowercase();
    if [
        "token",
        "secret",
        "password",
        "passwd",
        "api_key",
        "apikey",
        "authorization",
        "cookie",
        "credential",
    ]
    .iter()
    .any(|key| lower.contains(key))
    {
        "[probable secret-bearing diagnostic redacted]".to_owned()
    } else {
        line.to_owned()
    }
}

fn request_key(id: &Value) -> String {
    match id {
        Value::String(value) => value.clone(),
        _ => id.to_string(),
    }
}

fn thread_id_from_start_response(response: &Value) -> Option<&str> {
    response
        .pointer("/thread/id")
        .or_else(|| response.get("threadId"))
        .or_else(|| response.get("id"))
        .and_then(Value::as_str)
}

fn host_github_config_dir() -> Option<PathBuf> {
    std::env::var_os("GH_CONFIG_DIR")
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("XDG_CONFIG_HOME")
                .map(PathBuf::from)
                .map(|path| path.join("gh"))
        })
        .or_else(|| {
            std::env::var_os("HOME")
                .map(PathBuf::from)
                .map(|path| path.join(".config/gh"))
        })
        .filter(|path| path.is_dir())
}

#[derive(Debug, Error)]
pub enum CodexError {
    #[error("failed to spawn {binary}: {source}")]
    Spawn {
        binary: PathBuf,
        source: std::io::Error,
    },
    #[error("Codex version probe failed: {0}")]
    VersionProbe(String),
    #[error("Codex schema probe failed: {0}")]
    SchemaProbe(String),
    #[error("Codex compatibility check failed: {0:?}")]
    Compatibility(Compatibility),
    #[error("App Server child did not expose a PID")]
    MissingPid,
    #[error("App Server child did not expose {0}")]
    MissingPipe(&'static str),
    #[error("App Server disconnected")]
    Disconnected,
    #[error("no local Codex account homes were detected")]
    NoCodexAccountHomes,
    #[error("unknown Codex account profile: {0}")]
    UnknownAccount(String),
    #[error("this Codex runtime does not support account switching")]
    AccountSwitchUnsupported,
    #[error("App Server request {method} timed out")]
    Timeout { method: String },
    #[error("App Server {method} failed with {code}: {message}")]
    Rpc {
        method: String,
        code: i64,
        message: String,
        data: Option<Value>,
    },
    #[error("App Server protocol error: {0}")]
    Protocol(String),
    #[error("scoped read runtime rejected its controller admission: {0}")]
    ScopedReadRuntime(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn account_profile(id: &str, selected: bool) -> CodexAccountProfile {
        CodexAccountProfile {
            id: id.to_owned(),
            label: id.to_owned(),
            codex_home: PathBuf::from(format!("/tmp/{id}")),
            selected,
            state: "ready".to_owned(),
            account_type: None,
            email: None,
            plan_type: None,
            rate_limits: Vec::new(),
            observed_at: None,
            detail: None,
            managed: false,
        }
    }

    #[test]
    fn request_keys_preserve_string_ids() {
        assert_eq!(request_key(&json!(7)), "7");
        assert_eq!(request_key(&json!("rpc-7")), "rpc-7");
    }

    #[tokio::test]
    async fn stalled_writer_keeps_server_request_owned_for_retry() {
        let (writer, mut receiver) = mpsc::channel(1);
        writer.send(json!({"method": "occupied"})).await.unwrap();
        let requests = Mutex::new(BTreeSet::from([request_key(&json!("approval-1"))]));

        let error = enqueue_server_response(
            &writer,
            &requests,
            json!("approval-1"),
            json!({"id": "approval-1", "result": {"decision": "accept"}}),
            Duration::from_millis(1),
        )
        .await
        .expect_err("a full writer must time out rather than pin the caller");
        assert!(matches!(error, CodexError::Timeout { method } if method == "server response"));
        assert!(
            requests
                .lock()
                .await
                .contains(&request_key(&json!("approval-1")))
        );

        assert!(receiver.recv().await.is_some());
        enqueue_server_response(
            &writer,
            &requests,
            json!("approval-1"),
            json!({"id": "approval-1", "result": {"decision": "accept"}}),
            Duration::from_millis(50),
        )
        .await
        .expect("a writable queue accepts the response");
        assert!(
            !requests
                .lock()
                .await
                .contains(&request_key(&json!("approval-1")))
        );
    }

    #[test]
    fn thread_start_binds_the_controller_selected_provider() {
        let params = start_thread_params(StartThread {
            cwd: PathBuf::from("/worktree"),
            model: "ornith-1.5-35b-a3b-nvfp4".to_owned(),
            model_provider: "qwen-local-switcher".to_owned(),
            sandbox: "workspace-write".to_owned(),
            approval_policy: "never".to_owned(),
            developer_instructions: "controller-owned".to_owned(),
            service_name: "test".to_owned(),
            ephemeral: false,
            scoped_read_runtime: None,
        });
        assert_eq!(
            params.get("modelProvider").and_then(Value::as_str),
            Some("qwen-local-switcher")
        );
        assert_eq!(
            params.get("model").and_then(Value::as_str),
            Some("ornith-1.5-35b-a3b-nvfp4")
        );
    }

    #[test]
    fn scoped_read_runtime_mounts_only_controller_admitted_regular_files() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("investigation");
        std::fs::create_dir_all(root.join("allowed/nested")).unwrap();
        std::fs::write(root.join("allowed/nested/evidence.rs"), "visible").unwrap();
        std::fs::write(root.join("secret.txt"), "must not be mounted").unwrap();

        let scope = ScopedReadRuntime::new(
            root.clone(),
            vec![PathBuf::from("allowed/nested/evidence.rs")],
        )
        .unwrap();
        let command = scoped_read_command(
            &CodexSettings {
                binary: PathBuf::from("/usr/bin/true"),
                ..CodexSettings::default()
            },
            &scope,
        )
        .unwrap();
        let args = command
            .as_std()
            .get_args()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>();
        let canonical_root = std::fs::canonicalize(root).unwrap();
        let admitted = canonical_root.join("allowed/nested/evidence.rs");
        let denied = canonical_root.join("secret.txt");
        let admitted = admitted.to_string_lossy().into_owned();
        let denied = denied.to_string_lossy().into_owned();

        assert!(args.windows(3).any(|arguments| {
            arguments[0] == "--ro-bind"
                && arguments[1] == admitted
                && arguments[2] == "/work/investigation/allowed/nested/evidence.rs"
        }));
        assert!(!args.iter().any(|argument| argument == &denied));
        assert!(
            args.windows(2).any(|arguments| {
                arguments[0] == "--unshare-net" && arguments[1] == "--cap-drop"
            })
        );
        assert!(args.windows(3).any(|arguments| {
            arguments[0] == "--ro-bind"
                && arguments[1] == "/usr/bin/true"
                && arguments[2] == SCOPED_READ_CODEX_BINARY
        }));
    }

    #[cfg(unix)]
    #[test]
    fn scoped_read_runtime_rejects_admitted_symbolic_links() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("investigation");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("outside.txt"), "outside").unwrap();
        std::os::unix::fs::symlink("outside.txt", root.join("linked.txt")).unwrap();

        let error = ScopedReadRuntime::new(root, vec![PathBuf::from("linked.txt")])
            .expect_err("indirect source must not be admitted");
        assert!(error.to_string().contains("regular non-symlink"));
    }

    #[test]
    fn scoped_read_runtime_refuses_to_mount_an_authenticated_codex_home() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("investigation");
        let codex_home = temp.path().join("codex-home");
        std::fs::create_dir_all(&root).unwrap();
        std::fs::create_dir_all(&codex_home).unwrap();
        std::fs::write(root.join("evidence.txt"), "visible").unwrap();
        let scope = ScopedReadRuntime::new(root, vec![PathBuf::from("evidence.txt")]).unwrap();

        let error = scoped_read_command(
            &CodexSettings {
                binary: PathBuf::from("/usr/bin/true"),
                codex_home: Some(codex_home),
                ..CodexSettings::default()
            },
            &scope,
        )
        .expect_err("long-lived credentials must remain outside the agent namespace");
        assert!(error.to_string().contains("credential broker"));
    }

    #[test]
    fn native_multi_agent_feature_prefers_enabled_v2_and_rejects_removed_flags() {
        let selected = select_native_multi_agent_feature(&json!({
            "data": [
                {"name": "multi_agent", "enabled": true, "stage": "stable"},
                {"name": "multi_agent_v2", "enabled": true, "stage": "beta"}
            ]
        }))
        .unwrap();
        assert_eq!(selected.as_deref(), Some("multi_agent_v2"));

        let unavailable = select_native_multi_agent_feature(&json!({
            "data": [
                {"name": "multi_agent", "enabled": true, "stage": "removed"},
                {"name": "multi_agent_v2", "enabled": false, "stage": "beta"}
            ]
        }))
        .unwrap();
        assert_eq!(unavailable, None);
    }

    #[tokio::test]
    async fn oversized_protocol_lines_are_discarded_without_buffering_them() {
        let (mut writer, reader) = tokio::io::duplex(128);
        let task = tokio::spawn(async move {
            writer.write_all(b"123456789\nsmall\n").await.unwrap();
        });
        let mut reader = BufReader::new(reader);
        assert!(matches!(
            read_bounded_line(&mut reader, 4).await.unwrap(),
            Some(BoundedLine::Oversized)
        ));
        match read_bounded_line(&mut reader, 8).await.unwrap() {
            Some(BoundedLine::Data(value)) => assert_eq!(value, b"small"),
            _ => panic!("expected the next bounded line"),
        }
        task.await.unwrap();
    }

    #[test]
    fn probable_secret_diagnostics_are_redacted() {
        assert_eq!(
            redact_diagnostic_line("Authorization: bearer value"),
            "[probable secret-bearing diagnostic redacted]"
        );
        assert_eq!(redact_diagnostic_line("runtime ready"), "runtime ready");
    }

    #[test]
    fn schema_digest_ignores_json_object_order() {
        let first = br#"{"version":1,"schema":{"type":"object","required":["id"]}}"#;
        let reordered = br#"{"schema":{"required":["id"],"type":"object"},"version":1}"#;

        assert_eq!(
            canonical_json_sha256(first).unwrap(),
            canonical_json_sha256(reordered).unwrap()
        );
    }

    #[test]
    fn rate_limit_parser_preserves_backend_windows_and_remaining_capacity() {
        let parsed = parse_rate_limits(&json!({
            "rateLimitsByLimitId": {
                "codex": {
                    "limitId": "codex",
                    "planType": "pro",
                    "primary": {
                        "usedPercent": 4,
                        "windowDurationMins": 10080,
                        "resetsAt": 1786630416
                    }
                }
            }
        }));

        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].limit_id, "codex");
        assert_eq!(parsed[0].windows[0].remaining_percent, 96);
        assert_eq!(parsed[0].windows[0].window_duration_mins, Some(10_080));
    }

    #[test]
    fn failed_rate_limit_refresh_clears_retained_capacity_before_marking_it_checked() {
        let mut profile = account_profile("ready", true);
        profile.rate_limits = vec![CodexRateLimit {
            limit_id: "codex".to_owned(),
            limit_name: None,
            plan_type: Some("pro".to_owned()),
            windows: vec![CodexRateLimitWindow {
                kind: "primary".to_owned(),
                used_percent: 20,
                remaining_percent: 80,
                window_duration_mins: None,
                resets_at: None,
            }],
        }];

        let refreshed = rate_limit_refresh_failed(profile, "rate limit RPC timed out");

        assert_eq!(refreshed.state, "ready");
        assert!(refreshed.rate_limits.is_empty());
        assert!(refreshed.observed_at.is_some());
        assert!(
            refreshed
                .detail
                .as_deref()
                .is_some_and(|detail| detail.contains("rate limit RPC timed out"))
        );
    }

    #[test]
    fn failed_account_read_clears_retained_capacity_before_marking_it_checked() {
        let mut profile = account_profile("unavailable", true);
        profile.rate_limits = vec![CodexRateLimit {
            limit_id: "codex".to_owned(),
            limit_name: None,
            plan_type: Some("pro".to_owned()),
            windows: vec![CodexRateLimitWindow {
                kind: "primary".to_owned(),
                used_percent: 20,
                remaining_percent: 80,
                window_duration_mins: None,
                resets_at: None,
            }],
        }];

        let refreshed = account_read_failed(profile, "account RPC timed out");

        assert_eq!(refreshed.state, "unavailable");
        assert!(refreshed.rate_limits.is_empty());
        assert!(refreshed.observed_at.is_some());
        assert!(
            refreshed
                .detail
                .as_deref()
                .is_some_and(|detail| detail.contains("account RPC timed out"))
        );
    }

    #[tokio::test]
    async fn refreshing_a_non_active_profile_cannot_change_the_selected_account() {
        let (durable_sink, _events) = mpsc::channel(1);
        let manager = CodexRuntimeManager {
            settings: Arc::new(CodexSettings::default()),
            durable_sink,
            profiles: Arc::new(RwLock::new(vec![
                account_profile("active", true),
                account_profile("other", false),
            ])),
            active_account_id: Arc::new(RwLock::new(Some("active".to_owned()))),
            active: Arc::new(RwLock::new(None)),
            scoped: Arc::new(RwLock::new(HashMap::new())),
            switch_lock: Arc::new(Mutex::new(())),
            account_telemetry_refreshing: Arc::new(AtomicBool::new(false)),
            restart_count: Arc::new(AtomicU32::new(0)),
        };
        let mut refreshed = account_profile("other", true);
        let observed_at = harness_domain::now_ms();
        refreshed.observed_at = Some(observed_at);
        refreshed.rate_limits = vec![CodexRateLimit {
            limit_id: "codex".to_owned(),
            limit_name: None,
            plan_type: Some("pro".to_owned()),
            windows: Vec::new(),
        }];

        manager.replace_profile(refreshed).await;

        let profiles = manager.profiles.read().await;
        assert!(
            profiles
                .iter()
                .find(|profile| profile.id == "active")
                .unwrap()
                .selected
        );
        assert!(
            !profiles
                .iter()
                .find(|profile| profile.id == "other")
                .unwrap()
                .selected
        );
        assert_eq!(
            profiles
                .iter()
                .find(|profile| profile.id == "other")
                .unwrap()
                .observed_at,
            Some(observed_at)
        );
    }
}

//! Version-pinned Codex App Server supervision over JSONL stdio.

use std::{
    collections::{HashMap, VecDeque},
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
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
const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
const MAX_STDERR_TAIL_BYTES: usize = 64 * 1024;
const MAX_STDERR_LINE_BYTES: usize = 64 * 1024;
const MAX_CONSECUTIVE_PROTOCOL_ERRORS: u8 = 3;
static NEXT_SCHEMA_PROBE: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
pub struct CodexSettings {
    pub binary: PathBuf,
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
            required_version: None,
            required_schema_sha256: None,
            schema_probe_root: std::env::temp_dir().join("harness-console-schema-probes"),
            service_name: "harness_console".to_owned(),
            experimental_api: false,
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
        }
    }
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
        let compatibility = probe_compatibility(&settings).await?;
        if !compatibility.version_match || !compatibility.schema_match {
            return Err(CodexError::Compatibility(compatibility));
        }

        let mut command = Command::new(&settings.binary);
        command
            .arg("app-server")
            .arg("--listen")
            .arg("stdio://")
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
        let stderr_tail = Arc::new(Mutex::new(VecDeque::with_capacity(MAX_STDERR_TAIL_BYTES)));
        let child = Arc::new(Mutex::new(Some(child)));
        let status = Arc::new(RwLock::new(CodexRuntimeStatus {
            state: "starting".to_owned(),
            detail: Some("initializing Codex App Server".to_owned()),
            version: Some(compatibility.version.clone()),
            required_version: compatibility.required_version.clone(),
            protocol_schema_sha256: Some(compatibility.schema_sha256.clone()),
            schema_match: compatibility.schema_match,
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
        {
            let mut status = supervisor.status.write().await;
            status.state = "ready".to_owned();
            status.detail = Some("App Server initialized; version and schema matched".to_owned());
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
                    "title": "Harness Console",
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
        if self.writer.send(message).await.is_err() {
            self.pending.lock().await.remove(&key);
            return Err(CodexError::Disconnected);
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
        self.writer
            .send(json!({"method": method, "params": params}))
            .await
            .map_err(|_| CodexError::Disconnected)
    }

    pub async fn respond(&self, id: Value, result: Value) -> Result<(), CodexError> {
        self.writer
            .send(json!({"id": id, "result": result}))
            .await
            .map_err(|_| CodexError::Disconnected)
    }

    pub async fn respond_error(
        &self,
        id: Value,
        code: i64,
        message: &str,
    ) -> Result<(), CodexError> {
        self.writer
            .send(json!({"id": id, "error": {"code": code, "message": message}}))
            .await
            .map_err(|_| CodexError::Disconnected)
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
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StartThread {
    pub cwd: PathBuf,
    pub model: String,
    pub sandbox: String,
    pub approval_policy: String,
    pub developer_instructions: String,
    pub service_name: String,
    #[serde(default)]
    pub ephemeral: bool,
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
        self.request(
            "thread/start",
            json!({
                "cwd": request.cwd,
                "model": request.model,
                "sandbox": request.sandbox,
                "approvalPolicy": request.approval_policy,
                "approvalsReviewer": "user",
                "developerInstructions": request.developer_instructions,
                "serviceName": request.service_name,
                "ephemeral": request.ephemeral,
            }),
        )
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
    let result = timeout(settings.request_timeout, schema_command.output())
        .await
        .map_err(|_| CodexError::Timeout {
            method: "app-server generate-json-schema".to_owned(),
        });
    let schema_result = match result {
        Ok(Ok(output)) if output.status.success() => {
            let schema_path = probe_dir.join("codex_app_server_protocol.v2.schemas.json");
            let result = sha256_file(&schema_path).await;
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

async fn sha256_file(path: &Path) -> Result<String, CodexError> {
    let bytes = fs::read(path).await?;
    Ok(hex::encode(Sha256::digest(bytes)))
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
    #[error("App Server request {method} timed out")]
    Timeout { method: String },
    #[error("App Server {method} failed with {code}: {message}")]
    Rpc {
        method: String,
        code: i64,
        message: String,
        data: Option<Value>,
    },
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_keys_preserve_string_ids() {
        assert_eq!(request_key(&json!(7)), "7");
        assert_eq!(request_key(&json!("rpc-7")), "rpc-7");
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
}

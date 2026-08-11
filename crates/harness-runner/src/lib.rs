//! Controller-owned, resource-aware command execution.

use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    process::Stdio,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use harness_domain::{ResourceClass, ResultClass, now_ms};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{
    fs::{self, File},
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    process::{Child, Command},
    sync::{OwnedSemaphorePermit, Semaphore},
    task::JoinHandle,
    time::timeout,
};
use tracing::{debug, warn};
use ulid::Ulid;

const DEFAULT_PREVIEW_BYTES: usize = 64 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommandSpec {
    pub program: String,
    #[serde(default)]
    pub args: Vec<String>,
    pub cwd: PathBuf,
    pub resource_class: ResourceClass,
    pub timeout_ms: u64,
    #[serde(default)]
    pub inherited_environment: Vec<String>,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    #[serde(default)]
    pub stdin: Option<Vec<u8>>,
}

impl CommandSpec {
    pub fn validate(&self) -> Result<(), RunnerError> {
        if self.program.trim().is_empty() {
            return Err(RunnerError::Invalid("program must not be empty".to_owned()));
        }
        if !self.cwd.is_dir() {
            return Err(RunnerError::Invalid(format!(
                "command cwd is not a directory: {}",
                self.cwd.display()
            )));
        }
        if self.timeout_ms == 0 {
            return Err(RunnerError::Invalid(
                "timeout must be greater than zero".to_owned(),
            ));
        }
        for key in self
            .environment
            .keys()
            .chain(self.inherited_environment.iter())
        {
            if key.is_empty()
                || !key
                    .bytes()
                    .all(|byte| byte == b'_' || byte.is_ascii_alphanumeric())
            {
                return Err(RunnerError::Invalid(format!(
                    "invalid environment variable name: {key}"
                )));
            }
        }
        Ok(())
    }

    #[must_use]
    pub fn display_command(&self) -> String {
        std::iter::once(self.program.as_str())
            .chain(self.args.iter().map(String::as_str))
            .map(shell_escape_for_display)
            .collect::<Vec<_>>()
            .join(" ")
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamCapture {
    pub path: PathBuf,
    pub preview: String,
    pub bytes: u64,
    pub sha256: String,
    pub preview_truncated: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CommandOutcome {
    pub command_id: String,
    pub display_command: String,
    pub cwd: PathBuf,
    pub resource_class: ResourceClass,
    pub started_at_ms: i64,
    pub duration_ms: u64,
    pub exit_code: Option<i32>,
    pub signal: Option<i32>,
    pub timed_out: bool,
    pub result_class: ResultClass,
    pub stdout: StreamCapture,
    pub stderr: StreamCapture,
}

impl CommandOutcome {
    #[must_use]
    pub fn succeeded(&self) -> bool {
        !self.timed_out && self.exit_code == Some(0)
    }
}

#[derive(Clone)]
pub struct ResourceManager {
    control: Arc<Semaphore>,
    medium: Arc<Semaphore>,
    heavy: Arc<Semaphore>,
    hardware: Arc<Mutex<HashMap<String, Arc<Semaphore>>>>,
}

impl ResourceManager {
    #[must_use]
    pub fn new(control_slots: usize, medium_slots: usize, heavy_slots: usize) -> Self {
        Self {
            control: Arc::new(Semaphore::new(control_slots.max(1))),
            medium: Arc::new(Semaphore::new(medium_slots.max(1))),
            heavy: Arc::new(Semaphore::new(heavy_slots.max(1))),
            hardware: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn acquire(&self, class: &ResourceClass) -> Result<OwnedSemaphorePermit, RunnerError> {
        let semaphore = match class {
            ResourceClass::Control => Arc::clone(&self.control),
            ResourceClass::Medium => Arc::clone(&self.medium),
            ResourceClass::Heavy => Arc::clone(&self.heavy),
            ResourceClass::Hardware(name) => {
                let mut hardware = self.hardware.lock().map_err(|_| RunnerError::Poisoned)?;
                Arc::clone(
                    hardware
                        .entry(name.clone())
                        .or_insert_with(|| Arc::new(Semaphore::new(1))),
                )
            }
        };
        semaphore
            .acquire_owned()
            .await
            .map_err(|_| RunnerError::ResourceClosed)
    }
}

impl Default for ResourceManager {
    fn default() -> Self {
        let parallel = std::thread::available_parallelism().map_or(2, usize::from);
        Self::new(parallel.clamp(2, 8), (parallel / 2).clamp(1, 4), 1)
    }
}

#[derive(Clone)]
pub struct CommandRunner {
    spool_root: Arc<PathBuf>,
    resources: ResourceManager,
    preview_bytes: usize,
}

impl CommandRunner {
    pub async fn new(
        spool_root: impl AsRef<Path>,
        resources: ResourceManager,
    ) -> Result<Self, RunnerError> {
        fs::create_dir_all(spool_root.as_ref()).await?;
        Ok(Self {
            spool_root: Arc::new(spool_root.as_ref().to_path_buf()),
            resources,
            preview_bytes: DEFAULT_PREVIEW_BYTES,
        })
    }

    #[must_use]
    pub fn with_preview_bytes(mut self, bytes: usize) -> Self {
        self.preview_bytes = bytes.max(256);
        self
    }

    pub async fn run(&self, spec: CommandSpec) -> Result<CommandOutcome, RunnerError> {
        spec.validate()?;
        let _permit = self.resources.acquire(&spec.resource_class).await?;
        let command_id = Ulid::generate().to_string();
        let command_dir = self.spool_root.join(&command_id);
        fs::create_dir_all(&command_dir).await?;
        let temporary_dir = command_dir.join("tmp");
        let cache_dir = command_dir.join("cache");
        let home_dir = command_dir.join("home");
        let config_dir = command_dir.join("config");
        let data_dir = command_dir.join("data");
        let state_dir = command_dir.join("state");
        fs::create_dir(&temporary_dir).await?;
        fs::create_dir(&cache_dir).await?;
        fs::create_dir(&home_dir).await?;
        fs::create_dir(&config_dir).await?;
        fs::create_dir(&data_dir).await?;
        fs::create_dir(&state_dir).await?;
        let stdout_path = command_dir.join("stdout.log");
        let stderr_path = command_dir.join("stderr.log");

        let mut command = Command::new(&spec.program);
        command
            .args(&spec.args)
            .current_dir(&spec.cwd)
            .env_clear()
            .stdin(if spec.stdin.is_some() {
                Stdio::piped()
            } else {
                Stdio::null()
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(unix)]
        command.process_group(0);
        for key in &spec.inherited_environment {
            if let Some(value) = std::env::var_os(key) {
                command.env(key, value);
            }
        }
        for (key, value) in &spec.environment {
            command.env(key, value);
        }
        // Controller commands get command-scoped disposable roots regardless
        // of the host environment. This contains generic temporary files and
        // standards-compliant tool caches without assuming a build system.
        command
            .env("TMPDIR", &temporary_dir)
            .env("TMP", &temporary_dir)
            .env("TEMP", &temporary_dir)
            .env("XDG_CACHE_HOME", &cache_dir);
        let host_home_allowed = spec.environment.contains_key("HOME")
            || spec.inherited_environment.iter().any(|item| item == "HOME");
        if !host_home_allowed {
            command.env("HOME", &home_dir);
        }
        for (key, value) in [
            ("XDG_CONFIG_HOME", &config_dir),
            ("XDG_DATA_HOME", &data_dir),
            ("XDG_STATE_HOME", &state_dir),
        ] {
            if !host_home_allowed
                && !spec.environment.contains_key(key)
                && !spec.inherited_environment.iter().any(|item| item == key)
            {
                command.env(key, value);
            }
        }

        let started_at_ms = now_ms();
        let started = Instant::now();
        debug!(command_id, command = %spec.display_command(), "starting controlled command");
        let mut child = command.spawn()?;
        let pid = child.id();
        if let Some(input) = spec.stdin.as_ref()
            && let Some(mut stdin) = child.stdin.take()
        {
            stdin.write_all(input).await?;
            stdin.shutdown().await?;
        }
        let stdout = child
            .stdout
            .take()
            .ok_or(RunnerError::MissingPipe("stdout"))?;
        let stderr = child
            .stderr
            .take()
            .ok_or(RunnerError::MissingPipe("stderr"))?;
        let stdout_task = tokio::spawn(capture_stream(stdout, stdout_path, self.preview_bytes));
        let stderr_task = tokio::spawn(capture_stream(stderr, stderr_path, self.preview_bytes));

        let wait_result = timeout(Duration::from_millis(spec.timeout_ms), child.wait()).await;
        let (status, timed_out) = match wait_result {
            Ok(status) => (status?, false),
            Err(_) => {
                warn!(command_id, pid, "controlled command timed out");
                terminate_managed_process(&mut child, pid);
                let status = match timeout(Duration::from_secs(5), child.wait()).await {
                    Ok(status) => status?,
                    Err(_) => {
                        kill_managed_process(&mut child, pid);
                        child.wait().await?
                    }
                };
                (status, true)
            }
        };
        let stdout = join_capture(stdout_task).await?;
        let stderr = join_capture(stderr_task).await?;
        #[cfg(unix)]
        let signal = std::os::unix::process::ExitStatusExt::signal(&status);
        #[cfg(not(unix))]
        let signal = None;
        let result_class = if timed_out {
            ResultClass::InfrastructureUnavailable
        } else if status.success() {
            ResultClass::Success
        } else {
            ResultClass::SourceFailure
        };

        Ok(CommandOutcome {
            command_id,
            display_command: spec.display_command(),
            cwd: spec.cwd,
            resource_class: spec.resource_class,
            started_at_ms,
            duration_ms: started.elapsed().as_millis().try_into().unwrap_or(u64::MAX),
            exit_code: status.code(),
            signal,
            timed_out,
            result_class,
            stdout,
            stderr,
        })
    }

    /// Remove a completed command's disposable spool after callers have
    /// copied any durable evidence into the artifact store.
    pub async fn discard(&self, outcome: &CommandOutcome) -> Result<(), RunnerError> {
        if outcome.command_id.parse::<Ulid>().is_err() {
            return Err(RunnerError::Invalid(
                "command outcome has an invalid managed identifier".to_owned(),
            ));
        }
        let command_dir = self.spool_root.join(&outcome.command_id);
        if outcome.stdout.path.parent() != Some(command_dir.as_path())
            || outcome.stderr.path.parent() != Some(command_dir.as_path())
        {
            return Err(RunnerError::Invalid(
                "command outcome is outside the managed spool".to_owned(),
            ));
        }
        if fs::try_exists(&command_dir).await? {
            fs::remove_dir_all(command_dir).await?;
        }
        Ok(())
    }
}

async fn capture_stream<R: AsyncRead + Unpin>(
    mut stream: R,
    path: PathBuf,
    preview_limit: usize,
) -> Result<StreamCapture, RunnerError> {
    let mut output = File::create(&path).await?;
    let mut preview = Vec::with_capacity(preview_limit);
    let mut digest = Sha256::new();
    let mut total = 0_u64;
    let mut buffer = vec![0_u8; 32 * 1024];
    loop {
        let count = stream.read(&mut buffer).await?;
        if count == 0 {
            break;
        }
        output.write_all(&buffer[..count]).await?;
        digest.update(&buffer[..count]);
        total = total.saturating_add(count as u64);
        let available = preview_limit.saturating_sub(preview.len());
        preview.extend_from_slice(&buffer[..count.min(available)]);
    }
    output.flush().await?;
    output.sync_all().await?;
    Ok(StreamCapture {
        path,
        preview: String::from_utf8_lossy(&preview).into_owned(),
        bytes: total,
        sha256: hex::encode(digest.finalize()),
        preview_truncated: total > preview.len() as u64,
    })
}

async fn join_capture(
    task: JoinHandle<Result<StreamCapture, RunnerError>>,
) -> Result<StreamCapture, RunnerError> {
    task.await.map_err(RunnerError::Join)?
}

fn terminate_managed_process(child: &mut Child, pid: Option<u32>) {
    signal_managed_process(child, pid, ManagedSignal::Terminate);
}

fn kill_managed_process(child: &mut Child, pid: Option<u32>) {
    signal_managed_process(child, pid, ManagedSignal::Kill);
}

#[derive(Clone, Copy)]
enum ManagedSignal {
    Terminate,
    Kill,
}

fn signal_managed_process(child: &mut Child, pid: Option<u32>, signal: ManagedSignal) {
    #[cfg(unix)]
    if signal_isolated_process_group(pid, signal) {
        return;
    }

    // If the platform cannot prove that the child owns an isolated process
    // group, target only the owned child. A broad group signal must never be
    // allowed to reach the controller or its service runner.
    if let Err(error) = child.start_kill() {
        warn!(?pid, %error, "failed to terminate managed child process");
    }
}

#[cfg(unix)]
fn signal_isolated_process_group(pid: Option<u32>, signal: ManagedSignal) -> bool {
    use rustix::io::Errno;
    use rustix::process::{Pid, Signal, getpgid, getpgrp, kill_process_group};

    let Some(raw_pid) = pid.and_then(|pid| i32::try_from(pid).ok()) else {
        return false;
    };
    let Some(pid) = Pid::from_raw(raw_pid) else {
        return false;
    };
    let process_group = match getpgid(Some(pid)) {
        Ok(process_group) => process_group,
        Err(Errno::SRCH) => return true,
        Err(error) => {
            warn!(%pid, %error, "could not verify managed child process group");
            return false;
        }
    };
    let controller_group = getpgrp();
    if process_group != pid || process_group == controller_group {
        warn!(
            %pid,
            %process_group,
            %controller_group,
            "refusing to signal a process group not isolated for the managed child"
        );
        return false;
    }

    let signal = match signal {
        ManagedSignal::Terminate => Signal::TERM,
        ManagedSignal::Kill => Signal::KILL,
    };
    match kill_process_group(process_group, signal) {
        Ok(()) | Err(Errno::SRCH) => true,
        Err(error) => {
            warn!(%pid, %process_group, %error, "failed to signal managed child process group");
            false
        }
    }
}

fn shell_escape_for_display(value: &str) -> String {
    if value.bytes().all(|byte| {
        byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b'/' | b':' | b'=')
    }) {
        value.to_owned()
    } else {
        format!("'{}'", value.replace('\'', "'\\''"))
    }
}

#[derive(Debug, Error)]
pub enum RunnerError {
    #[error("invalid command specification: {0}")]
    Invalid(String),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("command resource semaphore was closed")]
    ResourceClosed,
    #[error("resource map mutex was poisoned")]
    Poisoned,
    #[error("spawned command did not expose its {0} pipe")]
    MissingPipe(&'static str),
    #[error("output capture task failed: {0}")]
    Join(#[from] tokio::task::JoinError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn captures_full_output_and_bounded_preview() {
        let temp = TempDir::new().unwrap();
        let runner = CommandRunner::new(temp.path().join("spool"), ResourceManager::default())
            .await
            .unwrap()
            .with_preview_bytes(256);
        let result = runner
            .run(CommandSpec {
                program: "/usr/bin/printf".to_owned(),
                args: vec!["%0300d".to_owned(), "0".to_owned()],
                cwd: temp.path().to_path_buf(),
                resource_class: ResourceClass::Control,
                timeout_ms: 5_000,
                inherited_environment: vec![],
                environment: BTreeMap::new(),
                stdin: None,
            })
            .await
            .unwrap();
        assert!(result.succeeded());
        assert_eq!(result.stdout.bytes, 300);
        assert!(result.stdout.preview_truncated);
        assert_eq!(fs::read(&result.stdout.path).await.unwrap().len(), 300);
        let command_dir = result.stdout.path.parent().unwrap().to_path_buf();
        runner.discard(&result).await.unwrap();
        assert!(!command_dir.exists());
    }

    #[tokio::test]
    async fn command_temporary_and_cache_paths_are_disposable_and_scoped() {
        let temp = TempDir::new().unwrap();
        let runner = CommandRunner::new(temp.path().join("spool"), ResourceManager::default())
            .await
            .unwrap();
        let result = runner
            .run(CommandSpec {
                program: "/bin/sh".to_owned(),
                args: vec![
                    "-c".to_owned(),
                    "test -d \"$TMPDIR\" && test -d \"$XDG_CACHE_HOME\" && test -d \"$HOME\" && printf '%s\\n%s\\n%s' \"$TMPDIR\" \"$XDG_CACHE_HOME\" \"$HOME\""
                        .to_owned(),
                ],
                cwd: temp.path().to_path_buf(),
                resource_class: ResourceClass::Control,
                timeout_ms: 5_000,
                inherited_environment: vec![],
                environment: BTreeMap::new(),
                stdin: None,
            })
            .await
            .unwrap();
        assert!(result.succeeded());
        let command_dir = result.stdout.path.parent().unwrap().to_path_buf();
        for path in result.stdout.preview.lines() {
            assert!(Path::new(path).starts_with(&command_dir));
        }
        runner.discard(&result).await.unwrap();
        assert!(!command_dir.exists());

        let allowed_home = temp.path().join("allowed-home");
        fs::create_dir(&allowed_home).await.unwrap();
        let result = runner
            .run(CommandSpec {
                program: "/bin/sh".to_owned(),
                args: vec![
                    "-c".to_owned(),
                    "printf '%s\\n%s' \"$HOME\" \"${XDG_CONFIG_HOME-unset}\"".to_owned(),
                ],
                cwd: temp.path().to_path_buf(),
                resource_class: ResourceClass::Control,
                timeout_ms: 5_000,
                inherited_environment: vec![],
                environment: BTreeMap::from([(
                    "HOME".to_owned(),
                    allowed_home.to_string_lossy().into_owned(),
                )]),
                stdin: None,
            })
            .await
            .unwrap();
        assert_eq!(
            result.stdout.preview,
            format!("{}\nunset", allowed_home.to_string_lossy())
        );
        runner.discard(&result).await.unwrap();
    }

    #[tokio::test]
    async fn timeout_terminates_descendants_without_source_failure() {
        let temp = TempDir::new().unwrap();
        let runner = CommandRunner::new(temp.path().join("spool"), ResourceManager::default())
            .await
            .unwrap();
        let result = runner
            .run(CommandSpec {
                program: "/bin/sh".to_owned(),
                args: vec!["-c".to_owned(), "sleep 30 & wait".to_owned()],
                cwd: temp.path().to_path_buf(),
                resource_class: ResourceClass::Control,
                timeout_ms: 50,
                inherited_environment: vec![],
                environment: BTreeMap::new(),
                stdin: None,
            })
            .await
            .unwrap();
        assert!(result.timed_out);
        assert_eq!(result.result_class, ResultClass::InfrastructureUnavailable);
    }
}

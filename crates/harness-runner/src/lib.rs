//! Controller-owned, resource-aware command execution.

use std::{
    collections::{BTreeMap, HashMap},
    os::unix::fs::PermissionsExt,
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

/// Controller-owned Bubblewrap isolation for governed evaluation commands.
///
/// This is deliberately separate from [`CommandRunner`]: generic commands do
/// not acquire sandbox semantics merely by using the runner.  A caller must
/// explicitly select this backend and gets an infrastructure-unavailable
/// result when the host cannot prove the required namespace boundary.
#[derive(Clone)]
pub struct EvaluationIsolationRunner {
    commands: CommandRunner,
    bwrap: PathBuf,
    trusted_worktree_root: Arc<PathBuf>,
    trusted_grader_root: Arc<PathBuf>,
    trusted_holdout_root: Arc<PathBuf>,
    trusted_artifact_root: Arc<PathBuf>,
    staging_root: Arc<PathBuf>,
    cargo_build_cache: Option<Arc<AdmittedCargoBuildCache>>,
}

/// Controller-approved offline Cargo inputs.  The registry and git directories
/// are immutable cache snapshots; the target directory is a controller-owned
/// disposable build output root.  They are deliberately configured on the
/// isolation runner rather than supplied by an evaluated command.
#[derive(Clone, Debug)]
pub struct CargoBuildCacheAdmission {
    pub trusted_registry_root: PathBuf,
    pub registry_cache: PathBuf,
    pub registry_receipt_digest: String,
    pub trusted_git_root: PathBuf,
    pub git_cache: PathBuf,
    pub git_receipt_digest: String,
    pub trusted_target_root: PathBuf,
    pub target_dir: PathBuf,
    pub target_receipt_digest: String,
    pub trusted_toolchain_root: PathBuf,
    pub toolchain_dir: PathBuf,
    pub toolchain_receipt_digest: String,
}

#[derive(Clone, Debug)]
struct AdmittedCargoBuildCache {
    registry_cache: PathBuf,
    registry_receipt_digest: String,
    git_cache: PathBuf,
    git_receipt_digest: String,
    target_dir: PathBuf,
    target_receipt_digest: String,
    toolchain_dir: PathBuf,
    toolchain_receipt_digest: String,
}

impl CargoBuildCacheAdmission {
    fn admit(self) -> Result<AdmittedCargoBuildCache, RunnerError> {
        let registry_root = canonical_directory(
            &self.trusted_registry_root,
            "trusted Cargo registry cache root",
        )?;
        let registry_cache =
            strict_trusted_directory(&self.registry_cache, &registry_root, "Cargo registry cache")?;
        let git_root = canonical_directory(&self.trusted_git_root, "trusted Cargo git cache root")?;
        let git_cache = strict_trusted_directory(&self.git_cache, &git_root, "Cargo git cache")?;
        let target_root =
            canonical_directory(&self.trusted_target_root, "trusted Cargo target root")?;
        let target_dir =
            strict_trusted_directory(&self.target_dir, &target_root, "Cargo target directory")?;
        let toolchain_root =
            canonical_directory(&self.trusted_toolchain_root, "trusted Rust toolchain root")?;
        let toolchain_dir = strict_trusted_directory(
            &self.toolchain_dir,
            &toolchain_root,
            "Rust toolchain directory",
        )?;
        validate_cargo_cache_layout(&registry_cache, &["index", "cache", "src"], "registry")?;
        validate_cargo_cache_layout(&git_cache, &["db", "checkouts"], "git")?;
        reject_cargo_credentials_or_config(&registry_cache)?;
        reject_cargo_credentials_or_config(&git_cache)?;
        validate_rust_toolchain_layout(&toolchain_dir)?;
        reject_cargo_credentials_or_config(&toolchain_dir)?;
        for (digest, label) in [
            (
                &self.registry_receipt_digest,
                "Cargo registry receipt digest",
            ),
            (&self.git_receipt_digest, "Cargo git receipt digest"),
            (&self.target_receipt_digest, "Cargo target receipt digest"),
            (
                &self.toolchain_receipt_digest,
                "Rust toolchain receipt digest",
            ),
        ] {
            validate_sha256(digest, label)?;
        }
        Ok(AdmittedCargoBuildCache {
            registry_cache,
            registry_receipt_digest: self.registry_receipt_digest,
            git_cache,
            git_receipt_digest: self.git_receipt_digest,
            target_dir,
            target_receipt_digest: self.target_receipt_digest,
            toolchain_dir,
            toolchain_receipt_digest: self.toolchain_receipt_digest,
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvaluationIsolationReceipt {
    pub backend: String,
    pub backend_version: String,
    pub namespaces: Vec<String>,
    pub candidate_access: String,
    pub grader_access: String,
    pub artifact_access: String,
    pub available: bool,
    pub policy_digest: String,
    pub digest: String,
}

/// Canonical digest for the closed isolation receipt fields.  Policy binding
/// may change while an evaluation is assembled, so callers must recompute the
/// digest after setting `policy_digest`; the digest never chains through a
/// caller-supplied prior digest.
#[must_use]
pub fn evaluation_isolation_receipt_digest(receipt: &EvaluationIsolationReceipt) -> String {
    let serialized = format!(
        "harness.eval.isolation.receipt.v2\0{}\0{}\0{}\0{}\0{}\0{}\0{}\0{}",
        receipt.backend,
        receipt.backend_version,
        receipt.namespaces.join(","),
        receipt.candidate_access,
        receipt.grader_access,
        receipt.artifact_access,
        receipt.available,
        receipt.policy_digest,
    );
    hex::encode(Sha256::digest(serialized.as_bytes()))
}

#[must_use]
pub fn verify_evaluation_isolation_receipt(receipt: &EvaluationIsolationReceipt) -> bool {
    validate_sha256(&receipt.policy_digest, "evaluation isolation policy digest").is_ok()
        && validate_sha256(&receipt.digest, "evaluation isolation receipt digest").is_ok()
        && evaluation_isolation_receipt_digest(receipt) == receipt.digest
}

#[derive(Clone, Debug)]
pub struct CandidateIsolationSpec {
    pub command: CommandSpec,
    pub grader_paths: Vec<PathBuf>,
    pub ground_truth_paths: Vec<PathBuf>,
}

#[derive(Clone, Debug)]
pub struct GraderIsolationSpec {
    pub command: CommandSpec,
    pub grader_root: PathBuf,
    pub ground_truth_paths: Vec<PathBuf>,
    pub artifact_path: PathBuf,
    pub artifact_sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvaluationIsolationOutcome {
    pub result_class: ResultClass,
    pub command: Option<CommandOutcome>,
    pub receipt: EvaluationIsolationReceipt,
    pub unavailable_reason: Option<String>,
    #[serde(skip)]
    staged_artifact: Option<PathBuf>,
}

impl EvaluationIsolationRunner {
    pub fn new(
        commands: CommandRunner,
        trusted_worktree_root: impl AsRef<Path>,
        trusted_grader_root: impl AsRef<Path>,
        trusted_holdout_root: impl AsRef<Path>,
        trusted_artifact_root: impl AsRef<Path>,
        staging_root: impl AsRef<Path>,
    ) -> Result<Self, RunnerError> {
        Self::with_bwrap_path(
            commands,
            PathBuf::from("/usr/bin/bwrap"),
            trusted_worktree_root,
            trusted_grader_root,
            trusted_holdout_root,
            trusted_artifact_root,
            staging_root,
        )
    }

    pub fn with_bwrap_path(
        commands: CommandRunner,
        bwrap: PathBuf,
        trusted_worktree_root: impl AsRef<Path>,
        trusted_grader_root: impl AsRef<Path>,
        trusted_holdout_root: impl AsRef<Path>,
        trusted_artifact_root: impl AsRef<Path>,
        staging_root: impl AsRef<Path>,
    ) -> Result<Self, RunnerError> {
        let trusted_worktree_root = canonical_directory(
            trusted_worktree_root.as_ref(),
            "trusted materialized-worktree root",
        )?;
        let trusted_grader_root =
            canonical_directory(trusted_grader_root.as_ref(), "trusted grader source root")?;
        let trusted_holdout_root =
            canonical_directory(trusted_holdout_root.as_ref(), "trusted holdout source root")?;
        let trusted_artifact_root = canonical_directory(
            trusted_artifact_root.as_ref(),
            "trusted candidate artifact source root",
        )?;
        std::fs::create_dir_all(staging_root.as_ref())?;
        std::fs::set_permissions(
            staging_root.as_ref(),
            std::fs::Permissions::from_mode(0o700),
        )?;
        let staging_root = canonical_directory(staging_root.as_ref(), "isolation staging root")?;
        Ok(Self {
            commands,
            bwrap,
            trusted_worktree_root: Arc::new(trusted_worktree_root),
            trusted_grader_root: Arc::new(trusted_grader_root),
            trusted_holdout_root: Arc::new(trusted_holdout_root),
            trusted_artifact_root: Arc::new(trusted_artifact_root),
            staging_root: Arc::new(staging_root),
            cargo_build_cache: None,
        })
    }

    /// Admit only controller-configured offline Cargo cache snapshots.  This
    /// does not alter generic command execution or enable an unisolated path.
    pub fn with_cargo_build_cache(
        mut self,
        admission: CargoBuildCacheAdmission,
    ) -> Result<Self, RunnerError> {
        let cache = admission.admit()?;
        if paths_overlap(&cache.registry_cache, &cache.git_cache)
            || paths_overlap(&cache.registry_cache, &cache.target_dir)
            || paths_overlap(&cache.git_cache, &cache.target_dir)
        {
            return Err(RunnerError::Invalid(
                "Cargo registry, git, and target custody must not overlap".to_owned(),
            ));
        }
        if paths_overlap(&cache.toolchain_dir, &cache.registry_cache)
            || paths_overlap(&cache.toolchain_dir, &cache.git_cache)
            || paths_overlap(&cache.toolchain_dir, &cache.target_dir)
        {
            return Err(RunnerError::Invalid(
                "Rust toolchain custody must not overlap Cargo cache or target custody".to_owned(),
            ));
        }
        for path in [
            &cache.registry_cache,
            &cache.git_cache,
            &cache.target_dir,
            &cache.toolchain_dir,
        ] {
            if path.starts_with(self.trusted_worktree_root.as_ref())
                || path.starts_with(self.trusted_grader_root.as_ref())
                || path.starts_with(self.trusted_holdout_root.as_ref())
                || path.starts_with(self.trusted_artifact_root.as_ref())
                || path.starts_with(self.staging_root.as_ref())
            {
                return Err(RunnerError::Invalid(
                    "Cargo cache custody must not overlap evaluation custody roots".to_owned(),
                ));
            }
        }
        self.cargo_build_cache = Some(Arc::new(cache));
        Ok(self)
    }

    /// Discard an isolated command spool only after the caller has retained
    /// any output artifacts needed as evaluation evidence.
    pub async fn discard(&self, outcome: &EvaluationIsolationOutcome) -> Result<(), RunnerError> {
        if let Some(command) = &outcome.command {
            self.commands.discard(command).await?;
        }
        if let Some(staged) = &outcome.staged_artifact {
            remove_staged_artifact(&self.staging_root, staged)?;
        }
        Ok(())
    }

    /// Probe the actual host boundary.  A missing executable, unsupported
    /// Bubblewrap, or denied namespace creation is unavailable—not a reason
    /// to run an evaluation command without isolation.
    pub async fn probe(&self, cwd: &Path) -> EvaluationIsolationReceipt {
        let version = match self.bwrap_version(cwd).await {
            Ok(version) => version,
            Err(_) => return isolation_receipt("unavailable", false, "none", "none", "none"),
        };
        if !supported_bubblewrap_version(&version) {
            return isolation_receipt(&version, false, "none", "none", "none");
        }
        let probe = CommandSpec {
            program: self.bwrap.to_string_lossy().into_owned(),
            args: bwrap_base_arguments()
                .into_iter()
                .chain(runtime_ro_binds())
                .chain([
                    "--proc".to_owned(),
                    "/proc".to_owned(),
                    "--dev".to_owned(),
                    "/dev".to_owned(),
                    "--".to_owned(),
                    "/usr/bin/true".to_owned(),
                ])
                .collect(),
            cwd: cwd.to_path_buf(),
            resource_class: ResourceClass::Control,
            timeout_ms: 5_000,
            inherited_environment: Vec::new(),
            environment: BTreeMap::new(),
            stdin: None,
        };
        match self.commands.run(probe).await {
            Ok(outcome) => {
                let available = outcome.succeeded();
                let _ = self.commands.discard(&outcome).await;
                if available {
                    isolation_receipt(&version, true, "none", "none", "none")
                } else {
                    isolation_receipt(&version, false, "none", "none", "none")
                }
            }
            _ => isolation_receipt(&version, false, "none", "none", "none"),
        }
    }

    pub async fn run_candidate(
        &self,
        spec: CandidateIsolationSpec,
    ) -> Result<EvaluationIsolationOutcome, RunnerError> {
        let candidate = self.trusted_candidate_cwd(&spec.command.cwd)?;
        validate_candidate_isolated_command(&spec.command, self.cargo_build_cache.as_deref())?;
        let hidden = canonical_paths(
            spec.grader_paths
                .iter()
                .chain(spec.ground_truth_paths.iter()),
            "hidden custody path",
        )?;
        for path in &hidden {
            if path.starts_with(&candidate) || candidate.starts_with(path) {
                return Err(RunnerError::Invalid(
                    "candidate cwd must not overlap grader or ground-truth custody".to_owned(),
                ));
            }
        }
        let mut receipt = self.probe(&candidate).await;
        receipt.candidate_access = "read_write".to_owned();
        receipt.grader_access = "not_exposed".to_owned();
        receipt.artifact_access = "not_exposed".to_owned();
        let receipt = bind_isolation_policy(
            receipt,
            "candidate",
            std::iter::once(candidate.to_string_lossy().into_owned()).chain(
                hidden
                    .iter()
                    .map(|path| path.to_string_lossy().into_owned()),
            ),
        );
        let receipt = bind_cargo_cache_policy(receipt, self.cargo_build_cache.as_deref());
        if !receipt.available {
            return Ok(unavailable_outcome(receipt));
        }
        let args =
            candidate_arguments(&candidate, &spec.command, self.cargo_build_cache.as_deref());
        let command = self
            .commands
            .run(wrapped_command(
                &self.bwrap,
                &candidate,
                &spec.command,
                args,
            ))
            .await?;
        Ok(EvaluationIsolationOutcome {
            result_class: command.result_class,
            command: Some(command),
            receipt,
            unavailable_reason: None,
            staged_artifact: None,
        })
    }

    pub async fn run_grader(
        &self,
        spec: GraderIsolationSpec,
    ) -> Result<EvaluationIsolationOutcome, RunnerError> {
        validate_isolated_command(&spec.command)?;
        let grader =
            strict_trusted_directory(&spec.grader_root, &self.trusted_grader_root, "grader root")?;
        if canonical_directory(&spec.command.cwd, "grader command cwd")? != grader {
            return Err(RunnerError::Invalid(
                "grader command cwd must be the controller-owned grader root".to_owned(),
            ));
        }
        let artifact = strict_trusted_file(
            &spec.artifact_path,
            &self.trusted_artifact_root,
            "grader artifact",
        )?;
        let ground_truth = spec
            .ground_truth_paths
            .iter()
            .map(|path| {
                strict_trusted_path(
                    path,
                    &self.trusted_holdout_root,
                    "ground-truth custody path",
                )
            })
            .collect::<Result<Vec<_>, _>>()?;
        for path in &ground_truth {
            if path.starts_with(&grader) || grader.starts_with(path) || path == &artifact {
                return Err(RunnerError::Invalid(
                    "grader, artifact, and ground-truth custody must not overlap".to_owned(),
                ));
            }
        }
        let mut receipt = self.probe(&grader).await;
        receipt.candidate_access = "not_exposed".to_owned();
        receipt.grader_access = "read_only".to_owned();
        receipt.artifact_access = "read_only".to_owned();
        if !receipt.available {
            return Ok(unavailable_outcome(receipt));
        }
        let staged_artifact = self.stage_artifact(&artifact, &spec.artifact_sha256)?;
        let receipt = bind_isolation_policy(
            receipt,
            "grader",
            std::iter::once(grader.to_string_lossy().into_owned())
                .chain(std::iter::once(format!(
                    "{}:{}",
                    staged_artifact.display(),
                    spec.artifact_sha256
                )))
                .chain(
                    ground_truth
                        .iter()
                        .map(|path| path.to_string_lossy().into_owned()),
                ),
        );
        let args = grader_arguments(&grader, &ground_truth, &staged_artifact, &spec.command);
        let command = match self
            .commands
            .run(wrapped_command(&self.bwrap, &grader, &spec.command, args))
            .await
        {
            Ok(command) => command,
            Err(error) => {
                let _ = remove_staged_artifact(&self.staging_root, &staged_artifact);
                return Err(error);
            }
        };
        Ok(EvaluationIsolationOutcome {
            result_class: command.result_class,
            command: Some(command),
            receipt,
            unavailable_reason: None,
            staged_artifact: Some(staged_artifact),
        })
    }

    async fn bwrap_version(&self, cwd: &Path) -> Result<String, RunnerError> {
        if !self.bwrap.is_file() {
            return Err(RunnerError::Invalid(
                "Bubblewrap executable is unavailable".to_owned(),
            ));
        }
        let outcome = self
            .commands
            .run(CommandSpec {
                program: self.bwrap.to_string_lossy().into_owned(),
                args: vec!["--version".to_owned()],
                cwd: cwd.to_path_buf(),
                resource_class: ResourceClass::Control,
                timeout_ms: 5_000,
                inherited_environment: Vec::new(),
                environment: BTreeMap::new(),
                stdin: None,
            })
            .await?;
        let succeeded = outcome.succeeded();
        let version = outcome.stdout.preview.trim().to_owned();
        let _ = self.commands.discard(&outcome).await;
        if succeeded {
            Ok(version)
        } else {
            Err(RunnerError::Invalid(
                "Bubblewrap version probe failed".to_owned(),
            ))
        }
    }

    fn trusted_candidate_cwd(&self, cwd: &Path) -> Result<PathBuf, RunnerError> {
        let cwd = canonical_directory(cwd, "candidate cwd")?;
        if cwd == *self.trusted_worktree_root
            || !cwd.starts_with(self.trusted_worktree_root.as_ref())
        {
            return Err(RunnerError::Invalid(
                "candidate cwd must be a leased child of the trusted materialized-worktree root"
                    .to_owned(),
            ));
        }
        Ok(cwd)
    }

    fn stage_artifact(&self, source: &Path, expected: &str) -> Result<PathBuf, RunnerError> {
        validate_sha256(expected, "grader artifact digest")?;
        let bytes = std::fs::read(source)?;
        if hex::encode(Sha256::digest(&bytes)) != expected {
            return Err(RunnerError::Invalid(
                "grader artifact digest does not match the supplied artifact".to_owned(),
            ));
        }
        let directory = self
            .staging_root
            .join(format!("artifact-{}", Ulid::generate()));
        std::fs::create_dir(&directory)?;
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))?;
        let staged = directory.join("artifact");
        let mut output = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&staged)?;
        use std::io::Write;
        output.write_all(&bytes)?;
        output.sync_all()?;
        std::fs::set_permissions(&staged, std::fs::Permissions::from_mode(0o400))?;
        if verify_file_sha256(&staged, expected).is_err() {
            let _ = std::fs::remove_dir_all(&directory);
            return Err(RunnerError::Invalid(
                "controller-staged grader artifact failed attestation".to_owned(),
            ));
        }
        Ok(staged)
    }
}

fn supported_bubblewrap_version(version: &str) -> bool {
    version == "bubblewrap 0.11.0"
}

fn unavailable_outcome(receipt: EvaluationIsolationReceipt) -> EvaluationIsolationOutcome {
    EvaluationIsolationOutcome {
        result_class: ResultClass::InfrastructureUnavailable,
        command: None,
        receipt,
        unavailable_reason: Some(
            "required Bubblewrap isolation capability is unavailable".to_owned(),
        ),
        staged_artifact: None,
    }
}

fn isolation_receipt(
    version: &str,
    available: bool,
    candidate_access: &str,
    grader_access: &str,
    artifact_access: &str,
) -> EvaluationIsolationReceipt {
    let namespaces = vec![
        "network".to_owned(),
        "user".to_owned(),
        "pid".to_owned(),
        "ipc".to_owned(),
        "uts".to_owned(),
        "cgroup".to_owned(),
    ];
    let mut receipt = EvaluationIsolationReceipt {
        backend: "bubblewrap".to_owned(),
        backend_version: version.to_owned(),
        namespaces,
        candidate_access: candidate_access.to_owned(),
        grader_access: grader_access.to_owned(),
        artifact_access: artifact_access.to_owned(),
        available,
        policy_digest: hex::encode(Sha256::digest(b"harness.eval.isolation.policy.v1\0")),
        digest: String::new(),
    };
    receipt.digest = evaluation_isolation_receipt_digest(&receipt);
    receipt
}

fn bind_isolation_policy(
    mut receipt: EvaluationIsolationReceipt,
    role: &str,
    bindings: impl IntoIterator<Item = String>,
) -> EvaluationIsolationReceipt {
    let mut bindings = bindings.into_iter().collect::<Vec<_>>();
    bindings.sort();
    bindings.dedup();
    let policy = format!(
        "harness.eval.isolation.policy.v1\0{role}\0{}",
        bindings.join("\0")
    );
    receipt.policy_digest = hex::encode(Sha256::digest(policy.as_bytes()));
    receipt.digest = evaluation_isolation_receipt_digest(&receipt);
    receipt
}

fn bind_cargo_cache_policy(
    receipt: EvaluationIsolationReceipt,
    cache: Option<&AdmittedCargoBuildCache>,
) -> EvaluationIsolationReceipt {
    let Some(cache) = cache else {
        return receipt;
    };
    bind_isolation_policy(
        receipt,
        "candidate-offline-cargo-cache",
        [
            format!(
                "registry:{}:{}",
                cache.registry_cache.display(),
                cache.registry_receipt_digest
            ),
            format!(
                "git:{}:{}",
                cache.git_cache.display(),
                cache.git_receipt_digest
            ),
            format!(
                "target:{}:{}",
                cache.target_dir.display(),
                cache.target_receipt_digest
            ),
            format!(
                "toolchain:{}:{}",
                cache.toolchain_dir.display(),
                cache.toolchain_receipt_digest
            ),
        ],
    )
}

fn bwrap_base_arguments() -> Vec<String> {
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

fn candidate_arguments(
    candidate: &Path,
    command: &CommandSpec,
    cargo_cache: Option<&AdmittedCargoBuildCache>,
) -> Vec<String> {
    let mut args = bwrap_base_arguments();
    args.extend(runtime_ro_binds());
    args.extend([
        "--dir".to_owned(),
        "/work".to_owned(),
        "--bind".to_owned(),
        candidate.to_string_lossy().into_owned(),
        "/work/candidate".to_owned(),
        "--proc".to_owned(),
        "/proc".to_owned(),
        "--dev".to_owned(),
        "/dev".to_owned(),
        "--chdir".to_owned(),
        "/work/candidate".to_owned(),
    ]);
    if let Some(cache) = cargo_cache {
        // Cargo's mutable metadata and locks live only in the sandbox tmpfs;
        // controller-admitted registry/git snapshots remain read-only.
        args.extend([
            "--tmpfs".to_owned(),
            "/cargo-home".to_owned(),
            "--dir".to_owned(),
            "/cargo-home/registry".to_owned(),
            "--dir".to_owned(),
            "/cargo-home/git".to_owned(),
            "--ro-bind".to_owned(),
            cache.registry_cache.to_string_lossy().into_owned(),
            "/cargo-home/registry".to_owned(),
            "--ro-bind".to_owned(),
            cache.git_cache.to_string_lossy().into_owned(),
            "/cargo-home/git".to_owned(),
            "--bind".to_owned(),
            cache.target_dir.to_string_lossy().into_owned(),
            "/work/cargo-target".to_owned(),
            "--ro-bind".to_owned(),
            cache.toolchain_dir.to_string_lossy().into_owned(),
            "/cargo-toolchain".to_owned(),
            "--setenv".to_owned(),
            "CARGO_HOME".to_owned(),
            "/cargo-home".to_owned(),
            "--setenv".to_owned(),
            "CARGO_TARGET_DIR".to_owned(),
            "/work/cargo-target".to_owned(),
            "--setenv".to_owned(),
            "CARGO_NET_OFFLINE".to_owned(),
            "true".to_owned(),
            "--setenv".to_owned(),
            "RUSTC".to_owned(),
            "/cargo-toolchain/bin/rustc".to_owned(),
        ]);
    }
    append_environment(&mut args, &command.environment);
    args.push("--".to_owned());
    args.push(command.program.clone());
    args.extend(command.args.clone());
    args
}

fn grader_arguments(
    grader: &Path,
    ground_truth: &[PathBuf],
    artifact: &Path,
    command: &CommandSpec,
) -> Vec<String> {
    let mut args = bwrap_base_arguments();
    args.extend(runtime_ro_binds());
    args.extend([
        "--dir".to_owned(),
        "/work".to_owned(),
        "--dir".to_owned(),
        "/work/ground-truth".to_owned(),
        "--ro-bind".to_owned(),
        grader.to_string_lossy().into_owned(),
        "/work/grader".to_owned(),
        "--ro-bind".to_owned(),
        artifact.to_string_lossy().into_owned(),
        "/work/artifact".to_owned(),
    ]);
    for (index, path) in ground_truth.iter().enumerate() {
        args.extend([
            "--ro-bind".to_owned(),
            path.to_string_lossy().into_owned(),
            format!("/work/ground-truth/{index}"),
        ]);
    }
    args.extend([
        "--proc".to_owned(),
        "/proc".to_owned(),
        "--dev".to_owned(),
        "/dev".to_owned(),
        "--chdir".to_owned(),
        "/work/grader".to_owned(),
    ]);
    append_environment(&mut args, &command.environment);
    args.push("--".to_owned());
    args.push(command.program.clone());
    args.extend(command.args.clone());
    args
}

fn wrapped_command(
    bwrap: &Path,
    cwd: &Path,
    original: &CommandSpec,
    args: Vec<String>,
) -> CommandSpec {
    CommandSpec {
        program: bwrap.to_string_lossy().into_owned(),
        args,
        cwd: cwd.to_path_buf(),
        resource_class: original.resource_class.clone(),
        timeout_ms: original.timeout_ms,
        inherited_environment: Vec::new(),
        environment: BTreeMap::new(),
        stdin: original.stdin.clone(),
    }
}

fn append_environment(args: &mut Vec<String>, environment: &BTreeMap<String, String>) {
    for (key, value) in environment {
        args.extend(["--setenv".to_owned(), key.clone(), value.clone()]);
    }
}

fn validate_candidate_isolated_command(
    command: &CommandSpec,
    cargo_cache: Option<&AdmittedCargoBuildCache>,
) -> Result<(), RunnerError> {
    validate_isolated_command_common(command)?;
    if command.program == "/cargo-toolchain/bin/cargo" && cargo_cache.is_some() {
        return Ok(());
    }
    validate_runtime_executable(&command.program)
}

fn validate_isolated_command(command: &CommandSpec) -> Result<(), RunnerError> {
    validate_isolated_command_common(command)?;
    validate_runtime_executable(&command.program)
}

fn validate_isolated_command_common(command: &CommandSpec) -> Result<(), RunnerError> {
    command.validate()?;
    if !command.inherited_environment.is_empty() {
        return Err(RunnerError::Invalid(
            "governed isolation forbids inherited environment variables".to_owned(),
        ));
    }
    if !command.environment.is_empty() {
        return Err(RunnerError::Invalid(
            "governed isolation forbids caller-supplied environment variables".to_owned(),
        ));
    }
    if command.stdin.is_some() {
        return Err(RunnerError::Invalid(
            "governed isolation forbids caller-supplied stdin".to_owned(),
        ));
    }
    if !Path::new(&command.program).is_absolute() {
        return Err(RunnerError::Invalid(
            "governed isolation requires an absolute executable path".to_owned(),
        ));
    }
    Ok(())
}

fn validate_runtime_executable(program: &str) -> Result<(), RunnerError> {
    if !Path::new(program).is_file() {
        return Err(RunnerError::Invalid(
            "governed isolation executable does not exist".to_owned(),
        ));
    }
    if !["/usr/", "/bin/"]
        .iter()
        .any(|prefix| program.starts_with(prefix))
    {
        return Err(RunnerError::Invalid(
            "governed isolation executable must be under the read-only runtime".to_owned(),
        ));
    }
    Ok(())
}

fn canonical_directory(path: &Path, label: &str) -> Result<PathBuf, RunnerError> {
    let path = std::fs::canonicalize(path)?;
    if path.is_dir() {
        Ok(path)
    } else {
        Err(RunnerError::Invalid(format!("{label} is not a directory")))
    }
}

fn canonical_file(path: &Path, label: &str) -> Result<PathBuf, RunnerError> {
    let path = std::fs::canonicalize(path)?;
    if path.is_file() {
        Ok(path)
    } else {
        Err(RunnerError::Invalid(format!("{label} is not a file")))
    }
}

fn validate_cargo_cache_layout(
    cache: &Path,
    required_children: &[&str],
    label: &str,
) -> Result<(), RunnerError> {
    if required_children
        .iter()
        .any(|child| !cache.join(child).is_dir())
    {
        return Err(RunnerError::Invalid(format!(
            "Cargo {label} cache must contain its standard {} layout",
            required_children.join(", ")
        )));
    }
    Ok(())
}

fn validate_rust_toolchain_layout(toolchain: &Path) -> Result<(), RunnerError> {
    for path in [
        toolchain.join("bin/cargo"),
        toolchain.join("bin/rustc"),
        toolchain.join("lib/rustlib"),
    ] {
        if !(path.is_file() || path.is_dir()) {
            return Err(RunnerError::Invalid(
                "Rust toolchain must contain bin/cargo, bin/rustc, and lib/rustlib".to_owned(),
            ));
        }
    }
    if !toolchain.join("bin/cargo").is_file()
        || !toolchain.join("bin/rustc").is_file()
        || !toolchain.join("lib/rustlib").is_dir()
    {
        return Err(RunnerError::Invalid(
            "Rust toolchain must contain bin/cargo, bin/rustc, and lib/rustlib".to_owned(),
        ));
    }
    Ok(())
}

fn reject_cargo_credentials_or_config(cache: &Path) -> Result<(), RunnerError> {
    for name in ["credentials", "credentials.toml", "config", "config.toml"] {
        if cache.join(name).exists() {
            return Err(RunnerError::Invalid(
                "Cargo cache admission must not expose credentials or Cargo configuration"
                    .to_owned(),
            ));
        }
    }
    Ok(())
}

fn strict_trusted_directory(path: &Path, root: &Path, label: &str) -> Result<PathBuf, RunnerError> {
    let path = canonical_directory(path, label)?;
    ensure_strict_descendant(&path, root, label)?;
    Ok(path)
}

fn strict_trusted_file(path: &Path, root: &Path, label: &str) -> Result<PathBuf, RunnerError> {
    let path = canonical_file(path, label)?;
    ensure_strict_descendant(&path, root, label)?;
    Ok(path)
}

fn strict_trusted_path(path: &Path, root: &Path, label: &str) -> Result<PathBuf, RunnerError> {
    let path = std::fs::canonicalize(path)?;
    ensure_strict_descendant(&path, root, label)?;
    Ok(path)
}

fn ensure_strict_descendant(path: &Path, root: &Path, label: &str) -> Result<(), RunnerError> {
    if path != root && path.starts_with(root) {
        Ok(())
    } else {
        Err(RunnerError::Invalid(format!(
            "{label} must be a strict child of its controller-trusted root"
        )))
    }
}

fn paths_overlap(left: &Path, right: &Path) -> bool {
    left.starts_with(right) || right.starts_with(left)
}

fn canonical_paths<'a>(
    paths: impl IntoIterator<Item = &'a PathBuf>,
    label: &str,
) -> Result<Vec<PathBuf>, RunnerError> {
    paths
        .into_iter()
        .map(|path| {
            let path = std::fs::canonicalize(path)?;
            if path.exists() {
                Ok(path)
            } else {
                Err(RunnerError::Invalid(format!("{label} does not exist")))
            }
        })
        .collect()
}

fn verify_file_sha256(path: &Path, expected: &str) -> Result<(), RunnerError> {
    validate_sha256(expected, "grader artifact digest")?;
    let actual = hex::encode(Sha256::digest(std::fs::read(path)?));
    if actual == expected {
        Ok(())
    } else {
        Err(RunnerError::Invalid(
            "grader artifact digest does not match the supplied artifact".to_owned(),
        ))
    }
}

fn validate_sha256(value: &str, label: &str) -> Result<(), RunnerError> {
    if value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        Ok(())
    } else {
        Err(RunnerError::Invalid(format!(
            "{label} must be 64 lowercase hexadecimal characters"
        )))
    }
}

fn remove_staged_artifact(staging_root: &Path, staged: &Path) -> Result<(), RunnerError> {
    let parent = staged.parent().ok_or_else(|| {
        RunnerError::Invalid("staged artifact has no controller-owned parent".to_owned())
    })?;
    if !parent.starts_with(staging_root) {
        return Err(RunnerError::Invalid(
            "refusing to discard artifact outside isolation staging root".to_owned(),
        ));
    }
    std::fs::remove_dir_all(parent)?;
    Ok(())
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

    fn isolated_spec(cwd: &Path) -> CommandSpec {
        CommandSpec {
            program: "/bin/sh".to_owned(),
            args: vec!["-c".to_owned(), "true".to_owned()],
            cwd: cwd.to_path_buf(),
            resource_class: ResourceClass::Control,
            timeout_ms: 5_000,
            inherited_environment: Vec::new(),
            environment: BTreeMap::new(),
            stdin: None,
        }
    }

    #[test]
    fn candidate_bubblewrap_policy_unshares_network_and_hides_grader_custody() {
        let temp = TempDir::new().unwrap();
        let candidate = temp.path().join("candidate");
        let grader = temp.path().join("grader");
        let ground_truth = temp.path().join("ground-truth");
        std::fs::create_dir_all(&candidate).unwrap();
        std::fs::create_dir_all(&grader).unwrap();
        std::fs::create_dir_all(&ground_truth).unwrap();
        let command = isolated_spec(&candidate);
        let candidate = std::fs::canonicalize(&candidate).unwrap();
        let candidate_text = candidate.to_string_lossy().into_owned();
        let args = candidate_arguments(&candidate, &command, None);

        for flag in [
            "--unshare-net",
            "--unshare-user",
            "--unshare-pid",
            "--unshare-ipc",
            "--unshare-uts",
            "--unshare-cgroup",
            "--clearenv",
        ] {
            assert!(args.iter().any(|value| value == flag), "missing {flag}");
        }
        assert!(args.windows(3).any(|values| {
            values[0] == "--bind" && values[1] == candidate_text && values[2] == "/work/candidate"
        }));
        let rendered = args.join("\0");
        assert!(!rendered.contains(grader.to_string_lossy().as_ref()));
        assert!(!rendered.contains(ground_truth.to_string_lossy().as_ref()));
        assert!(!rendered.contains("CARGO_HOME"));
        assert!(!rendered.contains("CARGO_TARGET_DIR"));
        assert!(
            !args
                .windows(2)
                .any(|values| values[0] == "--ro-bind" && values[1] == "/work/candidate")
        );
    }

    #[test]
    fn cargo_cache_admission_is_canonical_receipt_bound_and_offline_only() {
        let temp = TempDir::new().unwrap();
        let registry_root = temp.path().join("registry-root");
        let git_root = temp.path().join("git-root");
        let target_root = temp.path().join("target-root");
        let toolchain_root = temp.path().join("toolchain-root");
        let registry = registry_root.join("snapshot");
        let git = git_root.join("snapshot");
        let target = target_root.join("lease-target");
        let toolchain = toolchain_root.join("rust-1.97");
        for path in [
            &registry.join("index"),
            &registry.join("cache"),
            &registry.join("src"),
            &git.join("db"),
            &git.join("checkouts"),
            &target,
            &toolchain.join("lib/rustlib"),
        ] {
            std::fs::create_dir_all(path).unwrap();
        }
        std::fs::create_dir_all(toolchain.join("bin")).unwrap();
        std::fs::write(toolchain.join("bin/cargo"), "toolchain cargo").unwrap();
        std::fs::write(toolchain.join("bin/rustc"), "toolchain rustc").unwrap();
        let cache = CargoBuildCacheAdmission {
            trusted_registry_root: registry_root,
            registry_cache: registry.clone(),
            registry_receipt_digest: "a".repeat(64),
            trusted_git_root: git_root,
            git_cache: git.clone(),
            git_receipt_digest: "b".repeat(64),
            trusted_target_root: target_root,
            target_dir: target.clone(),
            target_receipt_digest: "c".repeat(64),
            trusted_toolchain_root: toolchain_root,
            toolchain_dir: toolchain.clone(),
            toolchain_receipt_digest: "d".repeat(64),
        }
        .admit()
        .unwrap();
        let candidate = temp.path().join("candidate");
        std::fs::create_dir_all(&candidate).unwrap();
        let args = candidate_arguments(&candidate, &isolated_spec(&candidate), Some(&cache));
        let registry_dir = args
            .windows(2)
            .position(|values| values[0] == "--dir" && values[1] == "/cargo-home/registry")
            .unwrap();
        let registry_bind = args
            .windows(3)
            .position(|values| values[0] == "--ro-bind" && values[2] == "/cargo-home/registry")
            .unwrap();
        let git_dir = args
            .windows(2)
            .position(|values| values[0] == "--dir" && values[1] == "/cargo-home/git")
            .unwrap();
        let git_bind = args
            .windows(3)
            .position(|values| values[0] == "--ro-bind" && values[2] == "/cargo-home/git")
            .unwrap();
        assert!(registry_dir < registry_bind);
        assert!(git_dir < git_bind);
        assert!(args.windows(3).any(|values| {
            values[0] == "--ro-bind"
                && values[1] == registry.to_string_lossy().as_ref()
                && values[2] == "/cargo-home/registry"
        }));
        assert!(args.windows(3).any(|values| {
            values[0] == "--ro-bind"
                && values[1] == toolchain.to_string_lossy().as_ref()
                && values[2] == "/cargo-toolchain"
        }));
        assert!(args.windows(3).any(|values| {
            values[0] == "--ro-bind"
                && values[1] == git.to_string_lossy().as_ref()
                && values[2] == "/cargo-home/git"
        }));
        assert!(args.windows(3).any(|values| {
            values[0] == "--bind"
                && values[1] == target.to_string_lossy().as_ref()
                && values[2] == "/work/cargo-target"
        }));
        assert!(args.windows(3).any(|values| {
            values[0] == "--setenv" && values[1] == "CARGO_HOME" && values[2] == "/cargo-home"
        }));
        assert!(args.windows(3).any(|values| {
            values[0] == "--setenv"
                && values[1] == "CARGO_TARGET_DIR"
                && values[2] == "/work/cargo-target"
        }));
        assert!(args.windows(3).any(|values| {
            values[0] == "--setenv" && values[1] == "CARGO_NET_OFFLINE" && values[2] == "true"
        }));
        assert!(args.windows(3).any(|values| {
            values[0] == "--setenv"
                && values[1] == "RUSTC"
                && values[2] == "/cargo-toolchain/bin/rustc"
        }));
        let virtual_cargo = CommandSpec {
            program: "/cargo-toolchain/bin/cargo".into(),
            ..isolated_spec(&candidate)
        };
        assert!(validate_candidate_isolated_command(&virtual_cargo, Some(&cache)).is_ok());
        assert!(validate_candidate_isolated_command(&virtual_cargo, None).is_err());
        let receipt = bind_cargo_cache_policy(
            isolation_receipt("bubblewrap 0.11.0", true, "read_write", "none", "none"),
            Some(&cache),
        );
        assert!(verify_evaluation_isolation_receipt(&receipt));
        let mut forged = receipt.clone();
        forged.digest = "e".repeat(64);
        assert!(!verify_evaluation_isolation_receipt(&forged));
        assert_ne!(
            receipt.policy_digest,
            isolation_receipt("bubblewrap 0.11.0", true, "read_write", "none", "none")
                .policy_digest
        );
    }

    #[test]
    fn cargo_cache_admission_rejects_outside_roots_and_invalid_digests() {
        let temp = TempDir::new().unwrap();
        let registry_root = temp.path().join("registry-root");
        let git_root = temp.path().join("git-root");
        let target_root = temp.path().join("target-root");
        let toolchain_root = temp.path().join("toolchain-root");
        let outside = temp.path().join("outside");
        for path in [&registry_root, &git_root, &target_root, &outside] {
            std::fs::create_dir_all(path).unwrap();
        }
        let admission = CargoBuildCacheAdmission {
            trusted_registry_root: registry_root.clone(),
            registry_cache: outside.clone(),
            registry_receipt_digest: "a".repeat(64),
            trusted_git_root: git_root.clone(),
            git_cache: outside.clone(),
            git_receipt_digest: "b".repeat(64),
            trusted_target_root: target_root.clone(),
            target_dir: outside,
            target_receipt_digest: "c".repeat(64),
            trusted_toolchain_root: toolchain_root.clone(),
            toolchain_dir: toolchain_root.join("outside"),
            toolchain_receipt_digest: "d".repeat(64),
        };
        assert!(admission.admit().is_err());

        let registry = registry_root.join("registry");
        let git = git_root.join("git");
        let target = target_root.join("target");
        let toolchain = toolchain_root.join("rust-1.97");
        for path in [
            &registry.join("index"),
            &registry.join("cache"),
            &registry.join("src"),
            &git.join("db"),
            &git.join("checkouts"),
            &target,
            &toolchain.join("lib/rustlib"),
        ] {
            std::fs::create_dir_all(path).unwrap();
        }
        std::fs::create_dir_all(toolchain.join("bin")).unwrap();
        std::fs::write(toolchain.join("bin/cargo"), "toolchain cargo").unwrap();
        std::fs::write(toolchain.join("bin/rustc"), "toolchain rustc").unwrap();
        let invalid_digest = CargoBuildCacheAdmission {
            trusted_registry_root: registry_root,
            registry_cache: registry.clone(),
            registry_receipt_digest: "not-a-digest".into(),
            trusted_git_root: git_root,
            git_cache: git.clone(),
            git_receipt_digest: "b".repeat(64),
            trusted_target_root: target_root,
            target_dir: target.clone(),
            target_receipt_digest: "c".repeat(64),
            trusted_toolchain_root: toolchain_root.clone(),
            toolchain_dir: toolchain.clone(),
            toolchain_receipt_digest: "d".repeat(64),
        };
        assert!(invalid_digest.admit().is_err());

        std::fs::write(registry.join("credentials.toml"), "token = 'secret'").unwrap();
        let credentials = CargoBuildCacheAdmission {
            trusted_registry_root: temp.path().join("registry-root"),
            registry_cache: registry,
            registry_receipt_digest: "a".repeat(64),
            trusted_git_root: temp.path().join("git-root"),
            git_cache: git,
            git_receipt_digest: "b".repeat(64),
            trusted_target_root: temp.path().join("target-root"),
            target_dir: target,
            target_receipt_digest: "c".repeat(64),
            trusted_toolchain_root: toolchain_root,
            toolchain_dir: toolchain,
            toolchain_receipt_digest: "d".repeat(64),
        };
        assert!(credentials.admit().is_err());
    }

    #[test]
    fn grader_policy_is_read_only_and_consumes_the_attested_artifact_only() {
        let temp = TempDir::new().unwrap();
        let grader = temp.path().join("grader");
        let ground_truth = temp.path().join("ground-truth");
        let artifact = temp.path().join("artifact.json");
        std::fs::create_dir_all(&grader).unwrap();
        std::fs::create_dir_all(&ground_truth).unwrap();
        std::fs::write(&artifact, "attested output").unwrap();
        let command = isolated_spec(&grader);
        let args = grader_arguments(
            &std::fs::canonicalize(&grader).unwrap(),
            &[std::fs::canonicalize(&ground_truth).unwrap()],
            &std::fs::canonicalize(&artifact).unwrap(),
            &command,
        );

        assert!(
            args.windows(3)
                .any(|values| values[0] == "--ro-bind" && values[2] == "/work/grader")
        );
        assert!(
            args.windows(3)
                .any(|values| values[0] == "--ro-bind" && values[2] == "/work/artifact")
        );
        assert!(
            !args
                .windows(3)
                .any(|values| values[0] == "--bind" && values[2].starts_with("/work/"))
        );
        let digest = hex::encode(Sha256::digest(b"attested output"));
        verify_file_sha256(&artifact, &digest).unwrap();
        assert!(verify_file_sha256(&artifact, &"0".repeat(64)).is_err());
    }

    #[tokio::test]
    async fn unavailable_bubblewrap_fails_closed_as_infrastructure_unavailable() {
        let temp = TempDir::new().unwrap();
        let commands = CommandRunner::new(temp.path().join("spool"), ResourceManager::default())
            .await
            .unwrap();
        let worktrees = temp.path().join("worktrees");
        let candidate = worktrees.join("candidate");
        std::fs::create_dir_all(&candidate).unwrap();
        let isolated = EvaluationIsolationRunner::with_bwrap_path(
            commands,
            temp.path().join("missing-bwrap"),
            &worktrees,
            temp.path(),
            temp.path(),
            temp.path(),
            temp.path().join("staging"),
        )
        .unwrap();
        let outcome = isolated
            .run_candidate(CandidateIsolationSpec {
                command: isolated_spec(&candidate),
                grader_paths: Vec::new(),
                ground_truth_paths: Vec::new(),
            })
            .await
            .unwrap();
        assert_eq!(outcome.result_class, ResultClass::InfrastructureUnavailable);
        assert!(outcome.command.is_none());
        assert!(!outcome.receipt.available);
    }

    #[tokio::test]
    async fn isolation_admission_rejects_unleased_cwd_secret_environment_and_stdin() {
        let temp = TempDir::new().unwrap();
        let worktrees = temp.path().join("worktrees");
        let leased = worktrees.join("lease-a");
        let outside = temp.path().join("outside");
        std::fs::create_dir_all(&leased).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        let commands = CommandRunner::new(temp.path().join("spool"), ResourceManager::default())
            .await
            .unwrap();
        let isolated = EvaluationIsolationRunner::with_bwrap_path(
            commands,
            temp.path().join("missing-bwrap"),
            &worktrees,
            temp.path(),
            temp.path(),
            temp.path(),
            temp.path().join("staging"),
        )
        .unwrap();

        assert!(
            isolated
                .run_candidate(CandidateIsolationSpec {
                    command: isolated_spec(&outside),
                    grader_paths: Vec::new(),
                    ground_truth_paths: Vec::new(),
                })
                .await
                .is_err()
        );
        assert!(
            isolated
                .run_candidate(CandidateIsolationSpec {
                    command: CommandSpec {
                        environment: BTreeMap::from([(
                            "API_TOKEN".to_owned(),
                            "secret".to_owned()
                        )]),
                        ..isolated_spec(&leased)
                    },
                    grader_paths: Vec::new(),
                    ground_truth_paths: Vec::new(),
                })
                .await
                .is_err()
        );
        assert!(
            isolated
                .run_candidate(CandidateIsolationSpec {
                    command: CommandSpec {
                        stdin: Some(b"secret input".to_vec()),
                        ..isolated_spec(&leased)
                    },
                    grader_paths: Vec::new(),
                    ground_truth_paths: Vec::new(),
                })
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn staged_artifact_remains_attested_after_source_replacement() {
        let temp = TempDir::new().unwrap();
        let worktrees = temp.path().join("worktrees");
        std::fs::create_dir_all(worktrees.join("lease-a")).unwrap();
        let source = temp.path().join("artifact");
        std::fs::write(&source, "original bytes").unwrap();
        let digest = hex::encode(Sha256::digest(b"original bytes"));
        let commands = CommandRunner::new(temp.path().join("spool"), ResourceManager::default())
            .await
            .unwrap();
        let isolated = EvaluationIsolationRunner::with_bwrap_path(
            commands,
            temp.path().join("missing-bwrap"),
            &worktrees,
            temp.path(),
            temp.path(),
            temp.path(),
            temp.path().join("staging"),
        )
        .unwrap();

        let staged = isolated.stage_artifact(&source, &digest).unwrap();
        std::fs::write(&source, "replacement bytes").unwrap();
        assert_eq!(std::fs::read(&staged).unwrap(), b"original bytes");
        verify_file_sha256(&staged, &digest).unwrap();
        remove_staged_artifact(&isolated.staging_root, &staged).unwrap();
        assert!(!staged.exists());
    }

    #[tokio::test]
    async fn grader_holdout_and_artifact_sources_must_be_under_their_own_trusted_roots() {
        let temp = TempDir::new().unwrap();
        let worktrees = temp.path().join("worktrees");
        let grader_sources = temp.path().join("grader-sources");
        let holdouts = temp.path().join("holdouts");
        let artifacts = temp.path().join("artifacts");
        let outside = temp.path().join("outside");
        let lease = worktrees.join("lease");
        let grader = grader_sources.join("grader");
        let holdout = holdouts.join("answer");
        let artifact = artifacts.join("result");
        for path in [&lease, &grader, &holdout, &artifacts, &outside] {
            std::fs::create_dir_all(path).unwrap();
        }
        std::fs::write(&artifact, "artifact").unwrap();
        let artifact_sha256 = hex::encode(Sha256::digest(b"artifact"));
        let commands = CommandRunner::new(temp.path().join("spool"), ResourceManager::default())
            .await
            .unwrap();
        let isolated = EvaluationIsolationRunner::with_bwrap_path(
            commands,
            temp.path().join("missing-bwrap"),
            &worktrees,
            &grader_sources,
            &holdouts,
            &artifacts,
            temp.path().join("staging"),
        )
        .unwrap();
        let valid = || GraderIsolationSpec {
            command: isolated_spec(&grader),
            grader_root: grader.clone(),
            ground_truth_paths: vec![holdout.clone()],
            artifact_path: artifact.clone(),
            artifact_sha256: artifact_sha256.clone(),
        };
        let mut outside_grader = valid();
        outside_grader.grader_root = outside.clone();
        outside_grader.command.cwd = outside.clone();
        assert!(isolated.run_grader(outside_grader).await.is_err());
        let mut outside_holdout = valid();
        outside_holdout.ground_truth_paths = vec![outside.clone()];
        assert!(isolated.run_grader(outside_holdout).await.is_err());
        let mut outside_artifact = valid();
        let outside_file = outside.join("artifact");
        std::fs::write(&outside_file, "artifact").unwrap();
        outside_artifact.artifact_path = outside_file;
        assert!(isolated.run_grader(outside_artifact).await.is_err());
    }

    #[test]
    fn staged_controller_path_is_not_serialized_in_outcome() {
        let outcome = EvaluationIsolationOutcome {
            result_class: ResultClass::Success,
            command: None,
            receipt: isolation_receipt("bubblewrap 0.11.0", true, "read_write", "none", "none"),
            unavailable_reason: None,
            staged_artifact: Some(PathBuf::from("/controller/private/staging/artifact")),
        };
        let value = serde_json::to_value(outcome).unwrap();
        assert!(value.get("staged_artifact").is_none());
    }

    #[test]
    fn isolation_receipt_is_deterministic_for_the_same_policy() {
        assert_eq!(
            isolation_receipt(
                "bubblewrap 0.11.0",
                true,
                "read_write",
                "read_only",
                "read_only"
            )
            .digest,
            isolation_receipt(
                "bubblewrap 0.11.0",
                true,
                "read_write",
                "read_only",
                "read_only"
            )
            .digest,
        );
    }

    #[test]
    fn observer_snapshot_accepts_only_the_receipt_pinned_bubblewrap_version() {
        assert!(supported_bubblewrap_version("bubblewrap 0.11.0"));
        assert!(!supported_bubblewrap_version("bubblewrap 0.11.1"));
        assert!(!supported_bubblewrap_version("bubblewrap 0.10.0"));
        assert!(!supported_bubblewrap_version("bubblewrap 0.11.0-dev"));
    }

    #[tokio::test]
    async fn bubblewrap_executes_custody_boundary_when_available() {
        let temp = TempDir::new().unwrap();
        let worktrees = temp.path().join("worktrees");
        let candidate = worktrees.join("candidate");
        let grader = temp.path().join("grader");
        let hidden = temp.path().join("hidden");
        let artifact = temp.path().join("artifact.txt");
        std::fs::create_dir_all(&candidate).unwrap();
        std::fs::create_dir_all(&grader).unwrap();
        std::fs::create_dir_all(&hidden).unwrap();
        std::fs::write(hidden.join("answer"), "secret").unwrap();
        std::fs::write(&artifact, "attested output").unwrap();
        let commands = CommandRunner::new(temp.path().join("spool"), ResourceManager::default())
            .await
            .unwrap();
        let isolated = EvaluationIsolationRunner::new(
            commands,
            &worktrees,
            temp.path(),
            temp.path(),
            temp.path(),
            temp.path().join("staging"),
        )
        .unwrap();
        assert!(
            isolated.probe(&candidate).await.available,
            "the supported BILDR host must provide Bubblewrap 0.11.0 with namespace isolation"
        );
        let host_network_namespace = std::fs::read_link("/proc/self/ns/net")
            .unwrap()
            .to_string_lossy()
            .into_owned();

        let candidate_outcome = isolated
            .run_candidate(CandidateIsolationSpec {
                command: CommandSpec {
                    args: vec![
                        "-c".to_owned(),
                        format!(
                            "printf candidate >/work/candidate/result && test ! -e {} && test \"$(readlink /proc/self/ns/net)\" != \"{}\"",
                            hidden.display(), host_network_namespace
                        ),
                    ],
                    ..isolated_spec(&candidate)
                },
                grader_paths: vec![grader.clone()],
                ground_truth_paths: vec![hidden.clone()],
            })
            .await
            .unwrap();
        assert_eq!(candidate_outcome.result_class, ResultClass::Success);
        assert_eq!(candidate_outcome.receipt.candidate_access, "read_write");
        assert_eq!(candidate_outcome.receipt.grader_access, "not_exposed");
        assert!(candidate.join("result").is_file());
        isolated.discard(&candidate_outcome).await.unwrap();

        let artifact_sha = hex::encode(Sha256::digest(b"attested output"));
        let grader_outcome = isolated
            .run_grader(GraderIsolationSpec {
                command: CommandSpec {
                    args: vec![
                        "-c".to_owned(),
                        format!(
                            "test \"$(cat /work/artifact)\" = 'attested output' && ! touch /work/grader/nope && ! touch /work/artifact && test ! -e {}",
                            candidate.display()
                        ),
                    ],
                    ..isolated_spec(&grader)
                },
                grader_root: grader,
                ground_truth_paths: vec![hidden],
                artifact_path: artifact,
                artifact_sha256: artifact_sha,
            })
            .await
            .unwrap();
        assert_eq!(grader_outcome.result_class, ResultClass::Success);
        assert_eq!(grader_outcome.receipt.candidate_access, "not_exposed");
        assert_eq!(grader_outcome.receipt.grader_access, "read_only");
        assert_eq!(grader_outcome.receipt.artifact_access, "read_only");
        isolated.discard(&grader_outcome).await.unwrap();
    }

    #[tokio::test]
    async fn bubblewrap_timeout_remains_infrastructure_unavailable_when_available() {
        let temp = TempDir::new().unwrap();
        let worktrees = temp.path().join("worktrees");
        let candidate = worktrees.join("candidate");
        std::fs::create_dir_all(&candidate).unwrap();
        let commands = CommandRunner::new(temp.path().join("spool"), ResourceManager::default())
            .await
            .unwrap();
        let isolated = EvaluationIsolationRunner::new(
            commands,
            &worktrees,
            temp.path(),
            temp.path(),
            temp.path(),
            temp.path().join("staging"),
        )
        .unwrap();
        if !isolated.probe(&candidate).await.available {
            return;
        }
        let outcome = isolated
            .run_candidate(CandidateIsolationSpec {
                command: CommandSpec {
                    args: vec!["-c".to_owned(), "sleep 30 & wait".to_owned()],
                    timeout_ms: 50,
                    ..isolated_spec(&candidate)
                },
                grader_paths: Vec::new(),
                ground_truth_paths: Vec::new(),
            })
            .await
            .unwrap();
        assert_eq!(outcome.result_class, ResultClass::InfrastructureUnavailable);
        assert!(
            outcome
                .command
                .as_ref()
                .is_some_and(|command| command.timed_out)
        );
        isolated.discard(&outcome).await.unwrap();
    }
}

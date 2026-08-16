use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::mpsc::{self, Receiver},
    thread,
    time::{Duration, Instant},
};

#[cfg(unix)]
use std::os::unix::process::CommandExt;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use harness_domain::InvestigationArtifact;
use harness_eval::{
    FaultOutcome, OPERATOR_CONTROL_FAULT_CASES, OperatorControlFaultCase,
    OperatorControlFaultMatrixRunV1, OperatorControlFaultResultV1,
    OperatorControlFaultSourceIdentityV1, SourceTreeStateV1,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use walkdir::WalkDir;

const SCHEMA_DIGEST_ENCODING: &str = "normalized-compact-json";
const JSON_SCHEMA_2020_12: &str = "https://json-schema.org/draft/2020-12/schema";
const IMPROVEMENT_CRATES: &[&str] = &[
    "harness-trace",
    "harness-eval",
    "harness-learning",
    "harness-promotion",
];
// This applies only to the new improvement surfaces.  The legacy orchestrator
// and App are reviewed exceptions, rather than files subject to a growth cap.
const IMPROVEMENT_RUST_FILE_LINE_BUDGET: usize = 1_200;
const IMPROVEMENT_UI_FILE_LINE_BUDGET: usize = 1_200;
const OPERATOR_CONTROL_FAULT_TIMEOUT: Duration = Duration::from_secs(300);
const OPERATOR_CONTROL_PIPE_DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
const OPERATOR_CONTROL_FAULT_REAP_TIMEOUT: Duration = Duration::from_secs(5);
const MAX_OPERATOR_CONTROL_PIPE_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug)]
struct SchemaDocument {
    path: PathBuf,
    value: Value,
}

#[derive(Parser)]
#[command(name = "cargo xtask", version, about = "BILDR build tasks")]
struct Cli {
    #[command(subcommand)]
    command: Task,
}

#[derive(Subcommand)]
enum Task {
    UiInstall {
        #[arg(long)]
        locked: bool,
    },
    UiBuild,
    Check,
    ArchitecturePolicyCheck,
    OpenapiCheck,
    SchemaCheck,
    OperatorControlFaultMatrix {
        #[arg(long)]
        output: PathBuf,
        #[arg(long)]
        evidence_dir: PathBuf,
        #[arg(long)]
        expected_sha: String,
    },
    OperatorControlFaultMatrixVerify {
        #[arg(long)]
        receipt: PathBuf,
        #[arg(long)]
        evidence_dir: PathBuf,
        #[arg(long)]
        expected_sha: String,
    },
    AppServerBindingsCheck,
    CodexSchema {
        #[arg(long, default_value = "codex")]
        codex: PathBuf,
    },
    Dist {
        #[arg(long)]
        check: bool,
    },
}

fn main() -> Result<()> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .context("xtask has no workspace parent")?
        .to_path_buf();
    match Cli::parse().command {
        Task::UiInstall { locked } => ui_install(&root, locked),
        Task::UiBuild => ui_build(&root),
        Task::Check => check(&root),
        Task::ArchitecturePolicyCheck => architecture_policy_check(&root),
        Task::OpenapiCheck => openapi_check(&root),
        Task::SchemaCheck => schema_check(&root),
        Task::OperatorControlFaultMatrix {
            output,
            evidence_dir,
            expected_sha,
        } => operator_control_fault_matrix(&root, &output, &evidence_dir, &expected_sha),
        Task::OperatorControlFaultMatrixVerify {
            receipt,
            evidence_dir,
            expected_sha,
        } => verify_operator_control_fault_matrix(&receipt, &evidence_dir, &expected_sha),
        Task::AppServerBindingsCheck => app_server_bindings_check(&root),
        Task::CodexSchema { codex } => codex_schema(&root, &codex),
        Task::Dist { check } => dist(&root, check),
    }
}

fn operator_control_fault_matrix(
    root: &Path,
    output: &Path,
    evidence_dir: &Path,
    expected_sha: &str,
) -> Result<()> {
    let root = fs::canonicalize(root).context("canonicalize workspace root")?;
    let preflight = source_tree_state(&root)?;
    require_exact_clean_source(&preflight, expected_sha, "preflight")?;
    let output = external_output_path(&root, output, "fault-matrix output")?;
    let evidence_dir =
        external_output_path(&root, evidence_dir, "fault-matrix evidence directory")?;
    if output.exists() {
        bail!("fault-matrix output already exists: {}", output.display())
    }
    if evidence_dir.exists() {
        bail!(
            "fault-matrix evidence directory already exists: {}",
            evidence_dir.display()
        )
    }
    fs::create_dir(&evidence_dir).with_context(|| {
        format!(
            "create explicit fault-matrix evidence directory: {}",
            evidence_dir.display()
        )
    })?;

    let mut results = Vec::with_capacity(OPERATOR_CONTROL_FAULT_CASES.len());
    for case in OPERATOR_CONTROL_FAULT_CASES {
        let (package, test_name, library_test) = fault_test_command(case);
        let mut command = Command::new("cargo");
        command.args(["test", "-q", "-p", package]);
        if library_test {
            command.arg("--lib");
        }
        let execution = run_fault_command(
            command
                .args([test_name, "--", "--exact", "--test-threads=1"])
                .current_dir(&root),
        );
        let transcript = fault_transcript(case.test_selector, &execution);
        let evidence_file = format!("{}.log", case.invariant.case_id());
        write_new(&evidence_dir.join(&evidence_file), transcript.as_bytes())?;
        results.push(OperatorControlFaultResultV1 {
            case_id: case.invariant.case_id().to_owned(),
            invariant: case.invariant,
            injection: case.injection,
            test_selector: case.test_selector.to_owned(),
            outcome: fault_outcome(&execution, &transcript),
            evidence_file,
            evidence_digest: sha256(transcript.as_bytes()),
        });
    }
    let postflight = source_tree_state(&root)?;
    let identity_bytes = source_identity_transcript(expected_sha, &preflight, &postflight)?;
    write_new(&evidence_dir.join("source-identity.log"), &identity_bytes)?;
    results.sort_by(|left, right| left.case_id.cmp(&right.case_id));
    let mut matrix = OperatorControlFaultMatrixRunV1 {
        schema: "harness.operator-control-fault-matrix.v1".to_owned(),
        implementation_sha: expected_sha.to_owned(),
        source_identity: OperatorControlFaultSourceIdentityV1 {
            expected_release_sha: expected_sha.to_owned(),
            preflight,
            postflight,
            evidence_file: "source-identity.log".to_owned(),
            evidence_digest: sha256(&identity_bytes),
        },
        results,
        sha256: String::new(),
    };
    matrix.sha256 = matrix.digest().context("digest fault matrix")?;
    matrix.validate().context("validate fault matrix")?;
    write_new(
        &output,
        &serde_json::to_vec_pretty(&matrix).context("serialize fault matrix")?,
    )?;
    verify_fault_matrix_evidence(&matrix, &evidence_dir)?;
    matrix
        .promotion_gate(expected_sha)
        .map_err(anyhow::Error::from)?;
    println!(
        "operator-control-fault-matrix: 12 invariant cases held for {}\\nreceipt: {}\\nevidence: {}",
        matrix.implementation_sha,
        output.display(),
        evidence_dir.display()
    );
    Ok(())
}

fn verify_operator_control_fault_matrix(
    receipt: &Path,
    evidence_dir: &Path,
    expected_sha: &str,
) -> Result<()> {
    let receipt = fs::canonicalize(receipt)
        .with_context(|| format!("fault-matrix receipt does not exist: {}", receipt.display()))?;
    let evidence_dir = fs::canonicalize(evidence_dir).with_context(|| {
        format!(
            "fault-matrix evidence directory does not exist: {}",
            evidence_dir.display()
        )
    })?;
    let matrix: OperatorControlFaultMatrixRunV1 =
        serde_json::from_slice(&fs::read(&receipt).context("read fault-matrix receipt")?)
            .context("parse fault-matrix receipt")?;
    verify_fault_matrix_evidence(&matrix, &evidence_dir)?;
    matrix
        .promotion_gate(expected_sha)
        .map_err(anyhow::Error::from)?;
    println!(
        "operator-control-fault-matrix verified for {}\\nreceipt: {}\\nevidence: {}",
        expected_sha,
        receipt.display(),
        evidence_dir.display()
    );
    Ok(())
}

fn source_tree_state(root: &Path) -> Result<SourceTreeStateV1> {
    let status = Command::new("git")
        .args(["status", "--porcelain"])
        .current_dir(root)
        .output()
        .context("read Git worktree status")?;
    if !status.status.success() {
        bail!("could not read Git worktree status")
    }
    let head = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(root)
        .output()
        .context("read implementation Git HEAD")?;
    if !head.status.success() {
        bail!("could not read implementation Git HEAD")
    }
    let head_sha = String::from_utf8(head.stdout)
        .context("Git HEAD was not UTF-8")?
        .trim()
        .to_owned();
    if !sha40(&head_sha) {
        bail!("implementation Git HEAD is not a full lowercase SHA-1")
    }
    Ok(SourceTreeStateV1 {
        head_sha,
        clean: status.stdout.is_empty(),
    })
}

fn require_exact_clean_source(
    state: &SourceTreeStateV1,
    expected_sha: &str,
    phase: &str,
) -> Result<()> {
    if !sha40(expected_sha) {
        bail!("fault-matrix expected SHA is not a full lowercase SHA-1")
    }
    if !state.clean || state.head_sha != expected_sha {
        bail!("fault-matrix {phase} requires clean exact source at {expected_sha}")
    }
    Ok(())
}

fn external_output_path(root: &Path, path: &Path, label: &str) -> Result<PathBuf> {
    if !path.is_absolute() {
        bail!("{label} must be an explicit absolute path outside the repository")
    }
    let parent = path
        .parent()
        .with_context(|| format!("{label} has no parent directory"))?;
    let parent = fs::canonicalize(parent)
        .with_context(|| format!("{label} parent does not exist: {}", parent.display()))?;
    if parent.starts_with(root) {
        bail!("{label} must be outside the repository")
    }
    Ok(parent.join(
        path.file_name()
            .with_context(|| format!("{label} has no filename"))?,
    ))
}

fn write_new(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| format!("create evidence artifact: {}", path.display()))?;
    file.write_all(bytes)
        .with_context(|| format!("write evidence artifact: {}", path.display()))
}

#[derive(Debug)]
enum FaultExecution {
    Completed {
        status: ExitStatus,
        stdout: FaultPipe,
        stderr: FaultPipe,
        completion_issue: Option<String>,
    },
    TimedOut {
        stdout: FaultPipe,
        stderr: FaultPipe,
        containment_issue: Option<String>,
    },
    SpawnFailed(String),
    WaitFailed {
        error: String,
        stdout: FaultPipe,
        stderr: FaultPipe,
    },
}

#[derive(Debug, Default)]
struct FaultPipe {
    bytes: Vec<u8>,
    issue: Option<String>,
}

fn run_fault_command(command: &mut Command) -> FaultExecution {
    // Each fault test owns a process group so a timeout can terminate Cargo and
    // every test process it started, without ever signaling the controller.
    #[cfg(unix)]
    command.process_group(0);
    let mut child = match command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(child) => child,
        Err(error) => return FaultExecution::SpawnFailed(error.to_string()),
    };
    let stdout = child.stdout.take().map(read_child_pipe);
    let stderr = child.stderr.take().map(read_child_pipe);
    let started = Instant::now();
    let mut terminal = None;
    let mut wait_error = None;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                terminal = Some(status);
                break;
            }
            Ok(None) if started.elapsed() < OPERATOR_CONTROL_FAULT_TIMEOUT => {
                thread::sleep(Duration::from_millis(25));
            }
            Ok(None) => {
                let containment_issue = terminate_fault_command(&mut child);
                let reap_issue = reap_fault_child(&mut child).err();
                let stdout = collect_child_pipe(stdout);
                let stderr = collect_child_pipe(stderr);
                return FaultExecution::TimedOut {
                    stdout,
                    stderr,
                    containment_issue: combine_fault_issues(containment_issue, reap_issue),
                };
            }
            Err(error) => {
                let containment_issue = terminate_fault_command(&mut child);
                let reap_issue = reap_fault_child(&mut child).err();
                wait_error = Some(
                    combine_fault_issues(Some(error.to_string()), containment_issue)
                        .map(|issue| {
                            combine_fault_issues(Some(issue), reap_issue)
                                .expect("combined wait issue remains present")
                        })
                        .expect("wait failure remains present"),
                );
                break;
            }
        }
    }
    let completion_issue =
        terminal
            .as_ref()
            .and_then(|_| match terminate_fault_process_group(child.id()) {
                Ok(issue) => issue,
                Err(error) => Some(error),
            });
    let stdout = collect_child_pipe(stdout);
    let stderr = collect_child_pipe(stderr);
    if let Some(error) = wait_error {
        FaultExecution::WaitFailed {
            error,
            stdout,
            stderr,
        }
    } else if let Some(status) = terminal {
        FaultExecution::Completed {
            status,
            stdout,
            stderr,
            completion_issue,
        }
    } else {
        FaultExecution::TimedOut {
            stdout,
            stderr,
            containment_issue: Some(
                "fault command ended without a terminal process status".to_owned(),
            ),
        }
    }
}

fn combine_fault_issues(first: Option<String>, second: Option<String>) -> Option<String> {
    match (first, second) {
        (Some(first), Some(second)) => Some(format!("{first}; {second}")),
        (Some(issue), None) | (None, Some(issue)) => Some(issue),
        (None, None) => None,
    }
}

/// Contain the process group and then the direct child if group containment
/// could not prove closure. The caller must still use [`reap_fault_child`] so
/// an unkillable process cannot block the promotion runner forever.
fn terminate_fault_command(child: &mut Child) -> Option<String> {
    match terminate_fault_process_group(child.id()) {
        Ok(Some(issue)) => Some(issue),
        Ok(None) => match child.kill() {
            Ok(()) => {
                Some("fault process group was absent; direct child was terminated".to_owned())
            }
            Err(error) => Some(format!(
                "fault process group was absent and direct child termination failed: {error}"
            )),
        },
        Err(group_error) => match child.kill() {
            Ok(()) => Some(format!(
                "{group_error}; direct child termination was required"
            )),
            Err(child_error) => Some(format!(
                "{group_error}; direct child termination failed: {child_error}"
            )),
        },
    }
}

fn reap_fault_child(child: &mut Child) -> Result<ExitStatus, String> {
    let deadline = Instant::now() + OPERATOR_CONTROL_FAULT_REAP_TIMEOUT;
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Ok(status),
            Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(25)),
            Ok(None) => {
                return Err(format!(
                    "fault child did not exit within {} seconds after containment",
                    OPERATOR_CONTROL_FAULT_REAP_TIMEOUT.as_secs()
                ));
            }
            Err(error) => return Err(format!("fault child reaping failed: {error}")),
        }
    }
}

/// A normal completed test has no group members after Cargo exits. Any member
/// left here is forcibly contained and marks the evidence unavailable; an OS
/// error is distinct because it leaves closure unproven.
fn terminate_fault_process_group(pid: u32) -> Result<Option<String>, String> {
    #[cfg(unix)]
    {
        use rustix::{
            io::Errno,
            process::{Pid, Signal, kill_process_group},
        };

        if let Some(group) = pid.try_into().ok().and_then(Pid::from_raw) {
            return match kill_process_group(group, Signal::KILL) {
                Ok(()) => Ok(Some(
                    "fault process group retained descendants after Cargo exited".to_owned(),
                )),
                Err(Errno::SRCH) => Ok(None),
                Err(error) => Err(format!(
                    "could not prove fault process-group closure: {error}"
                )),
            };
        }
    }
    Ok(None)
}

fn read_child_pipe<R>(mut pipe: R) -> Receiver<Result<FaultPipe, std::io::Error>>
where
    R: Read + Send + 'static,
{
    let (sender, receiver) = mpsc::sync_channel(1);
    thread::spawn(move || {
        let mut capture = FaultPipe::default();
        let mut buffer = [0_u8; 8192];
        loop {
            let read = pipe.read(&mut buffer)?;
            if read == 0 {
                break;
            }
            let remaining = MAX_OPERATOR_CONTROL_PIPE_BYTES.saturating_sub(capture.bytes.len());
            let retained = remaining.min(read);
            capture.bytes.extend_from_slice(&buffer[..retained]);
            if retained < read && capture.issue.is_none() {
                capture.issue = Some(format!(
                    "pipe capture exceeded {MAX_OPERATOR_CONTROL_PIPE_BYTES} bytes"
                ));
            }
        }
        let _ = sender.send(Ok(capture));
        Ok::<(), std::io::Error>(())
    });
    receiver
}

fn collect_child_pipe(pipe: Option<Receiver<Result<FaultPipe, std::io::Error>>>) -> FaultPipe {
    let Some(pipe) = pipe else {
        return FaultPipe::default();
    };
    match pipe.recv_timeout(OPERATOR_CONTROL_PIPE_DRAIN_TIMEOUT) {
        Ok(Ok(capture)) => capture,
        Ok(Err(error)) => FaultPipe {
            bytes: Vec::new(),
            issue: Some(format!("pipe reader failed: {error}")),
        },
        Err(mpsc::RecvTimeoutError::Timeout) => FaultPipe {
            bytes: Vec::new(),
            issue: Some(format!(
                "pipe reader did not close within {} seconds",
                OPERATOR_CONTROL_PIPE_DRAIN_TIMEOUT.as_secs()
            )),
        },
        Err(mpsc::RecvTimeoutError::Disconnected) => FaultPipe {
            bytes: Vec::new(),
            issue: Some("pipe reader disconnected before completing capture".to_owned()),
        },
    }
}

fn fault_transcript(selector: &str, execution: &FaultExecution) -> String {
    let (state, status, stdout, stderr, detail) = match execution {
        FaultExecution::Completed {
            status,
            stdout,
            stderr,
            completion_issue,
        } => (
            "completed",
            status.to_string(),
            stdout.bytes.as_slice(),
            stderr.bytes.as_slice(),
            completion_issue
                .clone()
                .or_else(|| stdout.issue.clone())
                .or_else(|| stderr.issue.clone())
                .map_or_else(
                    || Some("capture: complete".to_owned()),
                    |issue| Some(format!("capture: incomplete\n{issue}")),
                ),
        ),
        FaultExecution::TimedOut {
            stdout,
            stderr,
            containment_issue,
        } => (
            "timed_out",
            "unavailable".to_owned(),
            stdout.bytes.as_slice(),
            stderr.bytes.as_slice(),
            Some(match containment_issue {
                Some(issue) => format!(
                    "timeout_seconds: {}\ncontainment: {issue}",
                    OPERATOR_CONTROL_FAULT_TIMEOUT.as_secs()
                ),
                None => format!(
                    "timeout_seconds: {}\ncontainment: complete",
                    OPERATOR_CONTROL_FAULT_TIMEOUT.as_secs()
                ),
            }),
        ),
        FaultExecution::SpawnFailed(error) => (
            "spawn_failed",
            "unavailable".to_owned(),
            &[] as &[u8],
            &[] as &[u8],
            Some(format!("spawn_error: {error}")),
        ),
        FaultExecution::WaitFailed {
            error,
            stdout,
            stderr,
        } => (
            "wait_failed",
            "unavailable".to_owned(),
            stdout.bytes.as_slice(),
            stderr.bytes.as_slice(),
            Some(format!("wait_error: {error}")),
        ),
    };
    format!(
        "$ {selector}\nexecution: {state}\nexit_status: {status}\n{}--- stdout ---\n{}\n--- stderr ---\n{}\n",
        detail.map_or_else(String::new, |detail| format!("{detail}\n")),
        String::from_utf8_lossy(stdout),
        String::from_utf8_lossy(stderr),
    )
}

fn source_identity_transcript(
    expected_sha: &str,
    preflight: &SourceTreeStateV1,
    postflight: &SourceTreeStateV1,
) -> Result<Vec<u8>> {
    Ok(serde_json::to_vec_pretty(&json!({
        "expected_release_sha": expected_sha,
        "preflight": preflight,
        "postflight": postflight,
    }))?)
}

fn verify_fault_matrix_evidence(
    matrix: &OperatorControlFaultMatrixRunV1,
    evidence_dir: &Path,
) -> Result<()> {
    matrix.validate().map_err(anyhow::Error::from)?;
    let identity = fs::read(evidence_dir.join(&matrix.source_identity.evidence_file))
        .context("read source-identity evidence")?;
    if sha256(&identity) != matrix.source_identity.evidence_digest
        || identity
            != source_identity_transcript(
                &matrix.source_identity.expected_release_sha,
                &matrix.source_identity.preflight,
                &matrix.source_identity.postflight,
            )?
    {
        bail!("source-identity evidence does not match the signed receipt")
    }
    for result in &matrix.results {
        let transcript = fs::read(evidence_dir.join(&result.evidence_file))
            .with_context(|| format!("read evidence for {}", result.case_id))?;
        if sha256(&transcript) != result.evidence_digest {
            bail!("evidence digest mismatch for {}", result.case_id)
        }
        let transcript = String::from_utf8(transcript)
            .with_context(|| format!("evidence for {} is not UTF-8", result.case_id))?;
        if !transcript.starts_with(&format!("$ {}\n", result.test_selector)) {
            bail!("evidence command mismatch for {}", result.case_id)
        }
        let recorded_outcome = fault_outcome_from_transcript(&transcript);
        if recorded_outcome != result.outcome {
            bail!("evidence outcome mismatch for {}", result.case_id)
        }
    }
    Ok(())
}

fn fault_outcome_from_transcript(transcript: &str) -> FaultOutcome {
    if transcript.starts_with("$ ")
        && transcript.contains("execution: completed\nexit_status: exit status: 0\n")
        && transcript.contains("capture: complete\n")
        && transcript.contains("test result: ok. 1 passed;")
    {
        FaultOutcome::Held
    } else if transcript.contains("test result: FAILED") {
        FaultOutcome::Violated
    } else {
        FaultOutcome::InfrastructureUnavailable
    }
}

fn sha40(value: &str) -> bool {
    value.len() == 40
        && value.bytes().all(|byte| {
            byte.is_ascii_digit() || (byte.is_ascii_lowercase() && byte.is_ascii_hexdigit())
        })
}

fn fault_test_command(case: OperatorControlFaultCase) -> (&'static str, &'static str, bool) {
    match case.invariant.case_id() {
        "one_mutable_owner" => (
            "harness-store",
            "operator_control::reconciliation::tests::proof_consumption_authorizes_exactly_one_replacement_and_scheduler_lease",
            true,
        ),
        "unknown_cannot_authorize_replacement" => (
            "harness-store",
            "operator_control::reconciliation::tests::proof_consumption_refuses_any_unreconciled_command_history",
            true,
        ),
        "source_only_attention_closure" => (
            "harness-domain",
            "operator_control::tests::attention_transitions_are_source_owned_and_terminal_receipts_idempotent",
            true,
        ),
        "completion_cannot_hide_blocking_attention" => (
            "harness-orchestrator",
            "tests::investigation_completion_preserves_open_blocking_attention",
            true,
        ),
        "presentation_cannot_resolve" => (
            "harness-store",
            "operator_control::notifications::tests::notification_presentation_is_exact_session_scoped_idempotent_and_authority_neutral",
            true,
        ),
        "investigation_cannot_mutate_or_create_candidate" => (
            "harness-orchestrator",
            "tests::investigation_launch_and_artifact_completion_are_read_only_and_bound",
            true,
        ),
        "unknown_external_effect_never_auto_retried" => (
            "harness-orchestrator",
            "tests::automatic_fresh_attempt_routes_remain_unavailable",
            true,
        ),
        "projection_never_authorizes" => (
            "harness-store",
            "operator_control::snapshots::tests::return_view_acknowledgement_cannot_authorize_or_mutate_open_attention",
            true,
        ),
        "replay_deterministic" => (
            "harness-store",
            "operator_control::snapshots::tests::reordered_return_view_replay_cannot_regress_or_skip_current_events",
            true,
        ),
        "stale_version_or_digest_rejected" => (
            "harness-store",
            "operator_control::liveness::tests::wait_intervention_is_idempotent_and_cannot_apply_to_a_stale_episode",
            true,
        ),
        "critical_notification_not_omitted" => (
            "harness-store",
            "operator_control::notifications::tests::shadow_batch_is_exact_idempotent_and_keeps_critical_attention_immediate",
            true,
        ),
        "remote_runtime_absent" => (
            "harnessd",
            "tests::remote_bind_request_is_rejected_by_the_local_control_plane",
            false,
        ),
        _ => unreachable!("closed operator-control fault cases are exhaustive"),
    }
}

fn fault_outcome(execution: &FaultExecution, transcript: &str) -> FaultOutcome {
    if matches!(execution, FaultExecution::Completed { status, completion_issue: None, stdout, stderr } if status.success() && stdout.issue.is_none() && stderr.issue.is_none())
        && transcript.contains("test result: ok. 1 passed;")
    {
        FaultOutcome::Held
    } else if transcript.contains("test result: FAILED") {
        FaultOutcome::Violated
    } else {
        FaultOutcome::InfrastructureUnavailable
    }
}

fn ui_install(root: &Path, locked: bool) -> Result<()> {
    let ui = root.join("ui");
    let command = if locked {
        if !ui.join("package-lock.json").exists() {
            bail!("ui/package-lock.json is required for --locked")
        }
        "ci"
    } else {
        "install"
    };
    run(
        Command::new("npm").arg(command).current_dir(ui),
        "npm install",
    )
}

fn ui_build(root: &Path) -> Result<()> {
    if !root.join("ui/node_modules").exists() {
        ui_install(root, root.join("ui/package-lock.json").exists())?;
    }
    run(
        Command::new("npm")
            .args(["run", "build"])
            .current_dir(root.join("ui")),
        "UI build",
    )?;
    require_file(&root.join("ui/dist/index.html"))
}

fn check(root: &Path) -> Result<()> {
    schema_check(root)?;
    openapi_check(root)?;
    app_server_bindings_check(root)?;
    architecture_policy_check(root)?;
    ui_build(root)?;
    run(
        Command::new("cargo")
            .args(["fmt", "--all", "--", "--check"])
            .current_dir(root),
        "rustfmt",
    )?;
    run(
        Command::new("cargo")
            .args(["test", "--workspace", "--all-targets"])
            .current_dir(root),
        "workspace tests",
    )
}

fn architecture_policy_check(root: &Path) -> Result<()> {
    let mut violations = Vec::new();
    let crates_root = root.join("crates");
    for crate_name in IMPROVEMENT_CRATES {
        let crate_root = crates_root.join(crate_name);
        let manifest = crate_root.join("Cargo.toml");
        if !manifest.exists() {
            continue;
        }
        let value: toml::Value = toml::from_str(&fs::read_to_string(&manifest)?)
            .with_context(|| format!("invalid manifest: {}", manifest.display()))?;
        if manifest_depends_on_orchestrator(&value) {
            violations.push(format!(
                "{} must not depend on harness-orchestrator",
                manifest.display()
            ));
        }
        violations.extend(source_line_budget_violations(
            &crate_root,
            &["rs"],
            IMPROVEMENT_RUST_FILE_LINE_BUDGET,
        )?);
    }
    violations.extend(source_line_budget_violations(
        &root.join("ui/src/improvement"),
        &["ts", "tsx", "css"],
        IMPROVEMENT_UI_FILE_LINE_BUDGET,
    )?);
    if !violations.is_empty() {
        bail!(
            "architecture policy violations:\n- {}",
            violations.join("\n- ")
        )
    }
    println!(
        "architecture-policy-check: present improvement crates avoid harness-orchestrator; new improvement source files are within the {IMPROVEMENT_RUST_FILE_LINE_BUDGET}-line budget"
    );
    Ok(())
}

fn manifest_depends_on_orchestrator(manifest: &toml::Value) -> bool {
    dependency_tables(manifest).any(|dependencies| {
        dependencies.iter().any(|(name, specification)| {
            name == "harness-orchestrator"
                || specification
                    .as_table()
                    .and_then(|table| table.get("package"))
                    .and_then(toml::Value::as_str)
                    == Some("harness-orchestrator")
        })
    })
}

fn dependency_tables(
    manifest: &toml::Value,
) -> impl Iterator<Item = &toml::map::Map<String, toml::Value>> {
    let root = manifest.as_table();
    let direct = root.into_iter().flat_map(|table| {
        ["dependencies", "dev-dependencies", "build-dependencies"]
            .into_iter()
            .filter_map(|name| table.get(name).and_then(toml::Value::as_table))
    });
    let target = root
        .and_then(|table| table.get("target"))
        .and_then(toml::Value::as_table)
        .into_iter()
        .flat_map(|targets| targets.values())
        .filter_map(toml::Value::as_table)
        .flat_map(|table| {
            ["dependencies", "dev-dependencies", "build-dependencies"]
                .into_iter()
                .filter_map(|name| table.get(name).and_then(toml::Value::as_table))
        });
    direct.chain(target)
}

fn source_line_budget_violations(
    directory: &Path,
    extensions: &[&str],
    maximum_lines: usize,
) -> Result<Vec<String>> {
    if !directory.exists() {
        return Ok(Vec::new());
    }
    let entries = WalkDir::new(directory)
        .into_iter()
        .collect::<std::result::Result<Vec<_>, _>>()
        .with_context(|| format!("cannot walk source directory: {}", directory.display()))?;
    let mut paths = entries
        .into_iter()
        .filter(|entry| {
            entry.file_type().is_file()
                && entry
                    .path()
                    .extension()
                    .and_then(|extension| extension.to_str())
                    .is_some_and(|extension| extensions.contains(&extension))
        })
        .map(|entry| entry.into_path())
        .collect::<Vec<_>>();
    paths.sort();
    let mut violations = Vec::new();
    for path in paths {
        let lines = fs::read_to_string(&path)
            .with_context(|| format!("cannot read source file: {}", path.display()))?
            .lines()
            .count();
        if lines > maximum_lines {
            violations.push(format!(
                "{} has {lines} lines; the new improvement-source budget is {maximum_lines}",
                path.display()
            ));
        }
    }
    Ok(violations)
}

fn schema_check(root: &Path) -> Result<()> {
    let schemas = load_schema_catalog(&root.join("schemas"))?;
    let registry = schema_registry(&schemas)?;
    for schema in schemas.values() {
        compile_schema(&schema.path, &schema.value, &registry)?;
    }
    let examples = validate_schema_examples(&root.join("examples"), &schemas, &registry)?;
    let investigation_schema = schemas
        .get("harness.investigation-artifact.v1")
        .context("investigation artifact schema is missing")?;
    let investigation_validator = compile_schema(
        &investigation_schema.path,
        &investigation_schema.value,
        &registry,
    )?;
    let investigation_fixture =
        read_json(&root.join("examples/investigation-artifact.example.json"))?;
    validate_runtime_investigation_artifact(&investigation_fixture, "Investigation artifact")?;
    validate_investigation_artifact_cases(
        &investigation_validator,
        &investigation_fixture,
        "Investigation artifact JSON Schema",
    )?;
    let _: toml::Value = toml::from_str(&fs::read_to_string(
        root.join("config/harness.example.toml"),
    )?)?;
    let _: toml::Value = toml::from_str(&fs::read_to_string(
        root.join("profiles/bildr/profile.toml"),
    )?)?;
    let _: toml::Value = toml::from_str(&fs::read_to_string(
        root.join("profiles/general/profile.toml"),
    )?)?;
    println!(
        "schema-check: {} Draft 2020-12 schemas and {examples} examples conform; config and profiles parsed",
        schemas.len()
    );
    Ok(())
}

fn load_schema_catalog(directory: &Path) -> Result<BTreeMap<String, SchemaDocument>> {
    let mut documents = BTreeMap::new();
    let mut ids = BTreeMap::<String, PathBuf>::new();
    for path in json_paths(directory)? {
        let value = read_json(&path)?;
        if value.get("$schema").and_then(Value::as_str) != Some(JSON_SCHEMA_2020_12) {
            bail!(
                "schema {} must declare {JSON_SCHEMA_2020_12}",
                path.display()
            )
        }
        let id = required_string(&value, "$id", &path)?;
        if let Some(first) = ids.insert(id.to_owned(), path.clone()) {
            bail!(
                "duplicate schema $id {id} in {} and {}",
                first.display(),
                path.display()
            )
        }
        let discriminator = value
            .pointer("/properties/schema/const")
            .and_then(Value::as_str)
            .with_context(|| {
                format!(
                    "schema {} has no string properties.schema.const discriminator",
                    path.display()
                )
            })?;
        let discriminator = discriminator.to_owned();
        if let Some(first) = documents.insert(
            discriminator.clone(),
            SchemaDocument {
                path: path.clone(),
                value,
            },
        ) {
            bail!(
                "duplicate schema discriminator {discriminator} in {} and {}",
                first.path.display(),
                path.display()
            )
        }
    }
    if documents.is_empty() {
        bail!("no JSON schemas found under {}", directory.display())
    }
    Ok(documents)
}

fn schema_registry(schemas: &BTreeMap<String, SchemaDocument>) -> Result<jsonschema::Registry<'_>> {
    let mut registry = jsonschema::Registry::new();
    for schema in schemas.values() {
        let id = required_string(&schema.value, "$id", &schema.path)?;
        registry = registry
            .add(id, &schema.value)
            .with_context(|| format!("invalid schema $id {id} in {}", schema.path.display()))?;
    }
    registry
        .prepare()
        .context("failed to prepare local JSON Schema registry")
}

fn validate_schema_examples(
    directory: &Path,
    schemas: &BTreeMap<String, SchemaDocument>,
    registry: &jsonschema::Registry<'_>,
) -> Result<usize> {
    let openapi_examples = directory.join("openapi");
    let paths = json_paths(directory)?
        .into_iter()
        .filter(|path| !path.starts_with(&openapi_examples))
        .collect::<Vec<_>>();
    if paths.is_empty() {
        bail!("no JSON examples found under {}", directory.display())
    }
    for path in &paths {
        let value = read_json(path)?;
        validate_schema_example(path, &value, schemas, registry)?;
    }
    Ok(paths.len())
}

fn validate_schema_example(
    path: &Path,
    value: &Value,
    schemas: &BTreeMap<String, SchemaDocument>,
    registry: &jsonschema::Registry<'_>,
) -> Result<()> {
    let discriminator = required_string(value, "schema", path)?;
    let schema = schemas.get(discriminator).with_context(|| {
        format!(
            "example {} names undocumented schema {discriminator}",
            path.display()
        )
    })?;
    let validator = compile_schema(&schema.path, &schema.value, registry)?;
    if let Err(error) = validator.validate(value) {
        bail!(
            "example {} does not conform to {}: {error}",
            path.display(),
            schema.path.display()
        )
    }
    Ok(())
}

fn compile_schema(
    path: &Path,
    value: &Value,
    registry: &jsonschema::Registry<'_>,
) -> Result<jsonschema::Validator> {
    jsonschema::options()
        .with_draft(jsonschema::Draft::Draft202012)
        .with_registry(registry)
        .should_validate_formats(true)
        .build(value)
        .with_context(|| format!("invalid Draft 2020-12 schema: {}", path.display()))
}

fn required_string<'a>(value: &'a Value, key: &str, path: &Path) -> Result<&'a str> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .with_context(|| format!("{} has no non-empty {key}", path.display()))
}

fn read_json(path: &Path) -> Result<Value> {
    serde_json::from_slice(&fs::read(path)?)
        .with_context(|| format!("invalid JSON: {}", path.display()))
}

fn json_paths(directory: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in WalkDir::new(directory) {
        let entry = entry.with_context(|| format!("cannot walk {}", directory.display()))?;
        if entry.file_type().is_file()
            && entry.path().extension().and_then(|value| value.to_str()) == Some("json")
        {
            paths.push(entry.into_path());
        }
    }
    paths.sort();
    Ok(paths)
}

fn openapi_check(root: &Path) -> Result<()> {
    let path = root.join("openapi/harness-api.yaml");
    let value: serde_yaml::Value = serde_yaml::from_slice(&fs::read(&path)?)?;
    let mapping = value
        .as_mapping()
        .context("OpenAPI document must be a mapping")?;
    if mapping.get("openapi").is_none() || mapping.get("paths").is_none() {
        bail!("OpenAPI document has no openapi or paths field")
    }
    let pointers = collect_refs(&value);
    for pointer in &pointers {
        let pointer = pointer
            .strip_prefix('#')
            .context("only local OpenAPI references are allowed")?;
        if yaml_pointer(&value, pointer).is_none() {
            bail!("unresolved OpenAPI reference #{pointer}")
        }
    }
    let runtime_status_schema = runtime_status_schema(&value)?;
    let runtime_status_fixture =
        read_json(&root.join("examples/openapi/runtime-status.example.json"))?;
    let notification_delivery_health_fixture =
        read_json(&root.join("examples/openapi/notification-delivery-health.example.json"))?;
    let liveness_knowledge_candidate_fixture =
        read_json(&root.join("examples/openapi/liveness-knowledge-candidate.example.json"))?;
    let investigation_artifact_fixture =
        read_json(&root.join("examples/openapi/investigation-artifact.example.json"))?;
    let registry = jsonschema::Registry::new()
        .prepare()
        .context("failed to prepare OpenAPI JSON Schema registry")?;
    let runtime_status_validator = compile_schema(&path, &runtime_status_schema, &registry)?;
    if let Err(error) = runtime_status_validator.validate(&runtime_status_fixture) {
        bail!("RuntimeStatus fixture does not conform to OpenAPI: {error}")
    }
    let notification_delivery_health_schema =
        openapi_component_schema(&value, "NotificationDeliveryHealth")?;
    let notification_delivery_health_validator =
        compile_schema(&path, &notification_delivery_health_schema, &registry)?;
    if let Err(error) =
        notification_delivery_health_validator.validate(&notification_delivery_health_fixture)
    {
        bail!("NotificationDeliveryHealth fixture does not conform to OpenAPI: {error}")
    }
    let liveness_knowledge_candidate_schema =
        openapi_component_schema(&value, "LivenessKnowledgeCandidate")?;
    let liveness_knowledge_candidate_validator =
        compile_schema(&path, &liveness_knowledge_candidate_schema, &registry)?;
    if let Err(error) =
        liveness_knowledge_candidate_validator.validate(&liveness_knowledge_candidate_fixture)
    {
        bail!("LivenessKnowledgeCandidate fixture does not conform to OpenAPI: {error}")
    }
    let investigation_artifact_schema = openapi_component_schema(&value, "InvestigationArtifact")?;
    let investigation_artifact_validator =
        compile_schema(&path, &investigation_artifact_schema, &registry)?;
    if let Err(error) = investigation_artifact_validator.validate(&investigation_artifact_fixture) {
        bail!("InvestigationArtifact fixture does not conform to OpenAPI: {error}")
    }
    validate_runtime_investigation_artifact(
        &investigation_artifact_fixture,
        "OpenAPI InvestigationArtifact",
    )?;
    validate_investigation_artifact_cases(
        &investigation_artifact_validator,
        &investigation_artifact_fixture,
        "InvestigationArtifact OpenAPI contract",
    )?;
    validate_run_detail_supervision_contract(&value)?;
    validate_operator_settings_supervision_contract(&value)?;
    let documented_routes = mapping
        .get("paths")
        .and_then(serde_yaml::Value::as_mapping)
        .context("OpenAPI paths must be a mapping")?
        .keys()
        .map(|path| {
            path.as_str()
                .map(|path| format!("/api/v1{}", normalize_path_parameters(path)))
                .context("OpenAPI path keys must be strings")
        })
        .collect::<Result<BTreeSet<_>>>()?;
    let implemented_routes = api_router_paths(&root.join("crates/harness-api/src"))?;
    let missing = documented_routes
        .difference(&implemented_routes)
        .cloned()
        .collect::<Vec<_>>();
    let undocumented = implemented_routes
        .difference(&documented_routes)
        .cloned()
        .collect::<Vec<_>>();
    if !missing.is_empty() || !undocumented.is_empty() {
        bail!(
            "OpenAPI/router path drift; missing implementations: {missing:?}; undocumented routes: {undocumented:?}"
        )
    }
    println!(
        "openapi-check: {} local references resolved; RuntimeStatus/NotificationDeliveryHealth/LivenessKnowledgeCandidate/InvestigationArtifact fixtures and supervisory RunDetail/operator settings contracts conform; {} router paths match",
        pointers.len(),
        documented_routes.len()
    );
    Ok(())
}

fn validate_runtime_investigation_artifact(value: &Value, label: &str) -> Result<()> {
    let artifact: InvestigationArtifact = serde_json::from_value(value.clone())
        .with_context(|| format!("{label} cannot deserialize into the runtime contract"))?;
    artifact
        .validate()
        .with_context(|| format!("{label} does not satisfy the runtime contract"))
}

fn validate_investigation_artifact_cases(
    validator: &jsonschema::Validator,
    fixture: &Value,
    contract: &str,
) -> Result<()> {
    for (label, invalid, static_rejection_required) in
        investigation_artifact_invalid_cases(fixture)?
    {
        if static_rejection_required && validator.validate(&invalid).is_ok() {
            bail!("{contract} accepted {label}")
        }
        if validate_runtime_investigation_artifact(&invalid, label).is_ok() {
            bail!("runtime InvestigationArtifact contract accepted {label}")
        }
    }
    Ok(())
}

fn investigation_artifact_invalid_cases(
    fixture: &Value,
) -> Result<Vec<(&'static str, Value, bool)>> {
    let mut empty_conclusions = fixture.clone();
    empty_conclusions["findings"] = json!([]);
    empty_conclusions["recommendations"] = json!([]);
    empty_conclusions["decision_inventory"] = json!([]);

    let mut external_evidence = fixture.clone();
    external_evidence["findings"][0]["evidence_refs"] = json!(["artifact:external"]);

    let mut external_artifact = fixture.clone();
    external_artifact["artifact_refs"] = json!(["artifact:external"]);

    let mut no_context_source = fixture.clone();
    no_context_source["sources"] = json!([]);

    let mut multiple_context_sources = fixture.clone();
    multiple_context_sources["sources"] =
        json!([fixture["sources"][0].clone(), fixture["sources"][0].clone()]);

    let mut source_digest_mismatch = fixture.clone();
    source_digest_mismatch["sources"] = json!([format!("context:{}", "e".repeat(64))]);
    source_digest_mismatch["findings"][0]["evidence_refs"] =
        json!([format!("context:{}", "e".repeat(64))]);

    let mut cases = vec![
        ("empty conclusions", empty_conclusions, true),
        ("external conclusion evidence", external_evidence, true),
        ("external artifact reference", external_artifact, true),
        ("zero context sources", no_context_source, true),
        ("multiple context sources", multiple_context_sources, true),
        (
            "context source does not match repository state digest",
            source_digest_mismatch,
            false,
        ),
    ];
    for (_, invalid, _) in &mut cases {
        let mut artifact: InvestigationArtifact = serde_json::from_value(invalid.clone())?;
        artifact.sha256 = artifact.digest()?;
        *invalid = serde_json::to_value(artifact)?;
    }
    Ok(cases)
}

fn validate_run_detail_supervision_contract(openapi: &serde_yaml::Value) -> Result<()> {
    let properties = yaml_pointer(openapi, "/components/schemas/RunDetail/properties")
        .and_then(serde_yaml::Value::as_mapping)
        .context("RunDetail properties are missing from OpenAPI")?;
    let required = yaml_pointer(openapi, "/components/schemas/RunDetail/required")
        .and_then(serde_yaml::Value::as_sequence)
        .context("RunDetail required fields are missing from OpenAPI")?;
    for field in [
        "supervision_mode",
        "supervisor_snapshot",
        "supervisor_review",
        "supervisor_decision",
    ] {
        let field_key = serde_yaml::Value::String(field.to_owned());
        if !properties.contains_key(&field_key)
            || !required.iter().any(|value| value.as_str() == Some(field))
        {
            bail!("RunDetail must declare required {field} output")
        }
    }
    for (field, schema) in [
        ("supervisor_snapshot", "SupervisorSnapshot"),
        ("supervisor_review", "SupervisorReview"),
        ("supervisor_decision", "SupervisorDecision"),
    ] {
        let value = properties
            .get(serde_yaml::Value::String(field.to_owned()))
            .with_context(|| format!("RunDetail {field} schema is missing"))?;
        let expected = format!("#/components/schemas/{schema}");
        let has_ref = value
            .get("anyOf")
            .and_then(serde_yaml::Value::as_sequence)
            .is_some_and(|variants| {
                variants.iter().any(|variant| {
                    variant.get("$ref").and_then(serde_yaml::Value::as_str)
                        == Some(expected.as_str())
                })
            });
        if !has_ref {
            bail!("RunDetail {field} must reference {schema}")
        }
    }
    Ok(())
}

fn validate_operator_settings_supervision_contract(openapi: &serde_yaml::Value) -> Result<()> {
    for pointer in [
        "/components/schemas/OperatorSettings/properties",
        "/components/schemas/UpdateOperatorSettingsRequest/properties",
    ] {
        let properties = yaml_pointer(openapi, pointer)
            .and_then(serde_yaml::Value::as_mapping)
            .with_context(|| format!("operator settings properties are missing at {pointer}"))?;
        let key = serde_yaml::Value::String("supervision_enabled".to_owned());
        if !properties.contains_key(&key) {
            bail!("operator settings must declare supervision_enabled at {pointer}")
        }
    }
    let required = yaml_pointer(openapi, "/components/schemas/OperatorSettings/required")
        .and_then(serde_yaml::Value::as_sequence)
        .context("OperatorSettings required fields are missing from OpenAPI")?;
    if !required
        .iter()
        .any(|value| value.as_str() == Some("supervision_enabled"))
    {
        bail!("OperatorSettings must require supervision_enabled")
    }
    Ok(())
}

fn runtime_status_schema(openapi: &serde_yaml::Value) -> Result<Value> {
    openapi_component_schema(openapi, "RuntimeStatus")
}

fn openapi_component_schema(openapi: &serde_yaml::Value, component: &str) -> Result<Value> {
    let mut schema = serde_json::to_value(openapi)
        .context("OpenAPI document cannot be represented as JSON Schema input")?;
    let object = schema
        .as_object_mut()
        .context("OpenAPI JSON Schema input must be an object")?;
    object.insert(
        "$schema".to_owned(),
        Value::String(JSON_SCHEMA_2020_12.to_owned()),
    );
    object.insert(
        "$ref".to_owned(),
        Value::String(format!("#/components/schemas/{component}")),
    );
    Ok(schema)
}

fn rust_router_paths(source: &str) -> BTreeSet<String> {
    source
        .match_indices(".route(")
        .filter_map(|(offset, marker)| {
            let tail = source.get(offset + marker.len()..)?;
            let quote = tail.find('"')?;
            let value = tail.get(quote + 1..)?;
            let end = value.find('"')?;
            value.get(..end).map(ToOwned::to_owned)
        })
        .filter(|path| path.starts_with("/api/v1/"))
        .collect()
}

fn api_router_paths(directory: &Path) -> Result<BTreeSet<String>> {
    let mut paths = BTreeSet::new();
    for entry in WalkDir::new(directory) {
        let entry = entry?;
        if entry.file_type().is_file()
            && entry
                .path()
                .extension()
                .and_then(|extension| extension.to_str())
                == Some("rs")
        {
            paths.extend(rust_router_paths(&fs::read_to_string(entry.path())?));
        }
    }
    Ok(paths)
}

fn normalize_path_parameters(path: &str) -> String {
    let mut result = String::with_capacity(path.len());
    let mut parameter = false;
    for character in path.chars() {
        match character {
            '{' => {
                parameter = true;
                result.push(character);
            }
            '}' => {
                parameter = false;
                result.push(character);
            }
            uppercase if parameter && uppercase.is_ascii_uppercase() => {
                result.push('_');
                result.push(uppercase.to_ascii_lowercase());
            }
            other => result.push(other),
        }
    }
    result
}

fn collect_refs(value: &serde_yaml::Value) -> BTreeSet<String> {
    let mut refs = BTreeSet::new();
    match value {
        serde_yaml::Value::Mapping(mapping) => {
            for (key, value) in mapping {
                if key.as_str() == Some("$ref")
                    && let Some(reference) = value.as_str()
                {
                    refs.insert(reference.to_owned());
                }
                refs.extend(collect_refs(value));
            }
        }
        serde_yaml::Value::Sequence(sequence) => {
            for value in sequence {
                refs.extend(collect_refs(value));
            }
        }
        _ => {}
    }
    refs
}

fn yaml_pointer<'a>(
    mut value: &'a serde_yaml::Value,
    pointer: &str,
) -> Option<&'a serde_yaml::Value> {
    if pointer.is_empty() {
        return Some(value);
    }
    for segment in pointer.trim_start_matches('/').split('/') {
        let segment = segment.replace("~1", "/").replace("~0", "~");
        value = value
            .as_mapping()?
            .get(serde_yaml::Value::String(segment))?;
    }
    Some(value)
}

fn app_server_bindings_check(root: &Path) -> Result<()> {
    let schema =
        root.join("generated/codex-app-server-schema/codex_app_server_protocol.v2.schemas.json");
    require_file(&schema)?;
    let schema_bytes = fs::read(&schema)?;
    let _: Value = serde_json::from_slice(&schema_bytes)?;
    let digest = canonical_json_sha256(&schema_bytes)?;
    let config: toml::Value = toml::from_str(&fs::read_to_string(
        root.join("config/harness.example.toml"),
    )?)?;
    let configured = config
        .get("codex")
        .and_then(|value| value.get("required_protocol_schema_sha256"))
        .and_then(toml::Value::as_str)
        .context("config has no Codex schema digest")?;
    if configured != digest {
        bail!("generated App Server schema digest is {digest}, config pins {configured}")
    }
    let compatibility: Value =
        serde_json::from_slice(&fs::read(root.join("generated/CODEX_COMPATIBILITY.json"))?)?;
    if compatibility
        .get("root_schema_sha256_encoding")
        .and_then(Value::as_str)
        != Some(SCHEMA_DIGEST_ENCODING)
    {
        bail!("generated/CODEX_COMPATIBILITY.json has the wrong schema digest encoding")
    }
    if compatibility
        .get("root_schema_sha256")
        .and_then(Value::as_str)
        != Some(digest.as_str())
    {
        bail!("generated/CODEX_COMPATIBILITY.json does not match generated schema")
    }
    let configured_version = config
        .get("codex")
        .and_then(|value| value.get("required_version"))
        .and_then(toml::Value::as_str)
        .context("config has no required Codex version")?;
    if compatibility
        .get("codex_cli_version")
        .and_then(Value::as_str)
        != Some(configured_version)
    {
        bail!("generated/CODEX_COMPATIBILITY.json does not match configured Codex version")
    }
    println!("app-server-bindings-check: {digest}");
    Ok(())
}

fn codex_schema(root: &Path, codex: &Path) -> Result<()> {
    let temporary = tempfile::tempdir()?;
    run(
        Command::new(codex)
            .args(["app-server", "generate-json-schema", "--out"])
            .arg(temporary.path()),
        "Codex schema generation",
    )?;
    let source = temporary
        .path()
        .join("codex_app_server_protocol.v2.schemas.json");
    require_file(&source)?;
    let digest = canonical_json_sha256(&fs::read(&source)?)?;
    let destination = root.join("generated/codex-app-server-schema");
    if destination.exists() {
        fs::remove_dir_all(&destination)?;
    }
    fs::create_dir_all(&destination)?;
    for entry in WalkDir::new(temporary.path())
        .into_iter()
        .collect::<Result<Vec<_>, _>>()?
    {
        let relative = entry.path().strip_prefix(temporary.path())?;
        if relative.as_os_str().is_empty() {
            continue;
        }
        let target = destination.join(relative);
        if entry.file_type().is_dir() {
            fs::create_dir_all(&target)?;
        } else if entry.file_type().is_file() {
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(entry.path(), target)?;
        }
    }
    let version = output_text(Command::new(codex).arg("--version"), "Codex version probe")?;
    fs::write(
        root.join("generated/CODEX_COMPATIBILITY.json"),
        serde_json::to_vec_pretty(&json!({
            "schema": "harness-codex-compatibility/v1",
            "codex_cli_version": version.split_whitespace().last().unwrap_or(&version),
            "transport": "stdio-jsonl",
            "generated_schema_root": "generated/codex-app-server-schema",
            "root_schema": "codex_app_server_protocol.v2.schemas.json",
            "root_schema_sha256": digest,
            "root_schema_sha256_encoding": SCHEMA_DIGEST_ENCODING,
            "generated_at": "update-with-release-metadata"
        }))?,
    )?;
    println!("generated schema {digest}; update the intentional pins in config after review");
    Ok(())
}

fn dist(root: &Path, check_only: bool) -> Result<()> {
    for path in [
        "ui/dist/index.html",
        "config/harness.example.toml",
        "profiles/general/profile.toml",
        "profiles/bildr/profile.toml",
        "packaging/systemd/harnessd.service",
        "generated/CODEX_COMPATIBILITY.json",
        "LICENSE",
        "README.md",
        "VERSION",
    ] {
        require_file(&root.join(path))?;
    }
    if check_only {
        println!("dist --check: release inputs present");
        return Ok(());
    }
    ui_build(root)?;
    run(
        Command::new("cargo")
            .args([
                "build",
                "--release",
                "--package",
                "harnessd",
                "--package",
                "harnessctl",
                "--package",
                "harness-probe",
            ])
            .current_dir(root),
        "release build",
    )?;
    let version = fs::read_to_string(root.join("VERSION"))?.trim().to_owned();
    let dist = root.join("dist");
    let stage = dist.join(format!("bildr-{version}-linux-x86_64"));
    if stage.exists() {
        fs::remove_dir_all(&stage)?;
    }
    fs::create_dir_all(stage.join("bin"))?;
    fs::create_dir_all(stage.join("share/harness-console"))?;
    let target_dir = cargo_target_dir(root);
    for binary in ["harnessd", "harnessctl", "harness-probe"] {
        fs::copy(
            target_dir.join("release").join(binary),
            stage.join("bin").join(binary),
        )?;
    }
    for path in [
        "LICENSE",
        "README.md",
        "VERSION",
        "generated/CODEX_COMPATIBILITY.json",
        "openapi/harness-api.yaml",
        "packaging/systemd/harnessd.service",
    ] {
        let destination = stage.join("share/harness-console").join(path);
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(root.join(path), destination)?;
    }
    for path in ["codex", "config", "profiles", "schemas"] {
        copy_tree(
            &root.join(path),
            &stage.join("share/harness-console").join(path),
        )?;
    }
    let archive = dist.join(format!("bildr-{version}-linux-x86_64.tar.gz"));
    run(
        Command::new("tar")
            .arg("-C")
            .arg(&dist)
            .args(["-czf"])
            .arg(&archive)
            .arg(stage.file_name().unwrap()),
        "distribution archive",
    )?;
    fs::write(
        PathBuf::from(format!("{}.sha256", archive.display())),
        format!(
            "{}  {}\n",
            sha256(&fs::read(&archive)?),
            archive.file_name().unwrap().to_string_lossy()
        ),
    )?;
    println!("dist: {}", archive.display());
    Ok(())
}

fn cargo_target_dir(root: &Path) -> PathBuf {
    match env::var_os("CARGO_TARGET_DIR") {
        Some(value) => {
            let path = PathBuf::from(value);
            if path.is_absolute() {
                path
            } else {
                root.join(path)
            }
        }
        None => root.join("target"),
    }
}

fn require_file(path: &Path) -> Result<()> {
    let metadata = fs::metadata(path)
        .with_context(|| format!("required file {} is missing", path.display()))?;
    if !metadata.is_file() || metadata.len() == 0 {
        bail!("required file {} is empty or not a file", path.display())
    }
    Ok(())
}

fn copy_tree(source: &Path, destination: &Path) -> Result<()> {
    for entry in WalkDir::new(source) {
        let entry = entry?;
        let relative = entry.path().strip_prefix(source)?;
        let target = destination.join(relative);
        if entry.file_type().is_dir() {
            fs::create_dir_all(&target)?;
        } else if entry.file_type().is_file() {
            if let Some(parent) = target.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

fn run(command: &mut Command, label: &str) -> Result<()> {
    command.stdin(Stdio::null());
    let status = command
        .status()
        .with_context(|| format!("failed to start {label}"))?;
    if !status.success() {
        bail!("{label} failed with {status}")
    }
    Ok(())
}

fn output_text(command: &mut Command, label: &str) -> Result<String> {
    let output = command
        .output()
        .with_context(|| format!("failed to start {label}"))?;
    if !output.status.success() {
        bail!(
            "{label} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        )
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn canonical_json_sha256(bytes: &[u8]) -> Result<String> {
    let value = normalize_json(serde_json::from_slice(bytes)?);
    Ok(sha256(&serde_json::to_vec(&value)?))
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn fault_matrix_runner_has_one_exact_command_per_closed_case() {
        for case in OPERATOR_CONTROL_FAULT_CASES {
            let (package, test_name, library_test) = fault_test_command(case);
            let selector = if library_test {
                format!("cargo test -p {package} --lib {test_name} -- --exact --test-threads=1")
            } else {
                format!("cargo test -p {package} {test_name} -- --exact --test-threads=1")
            };
            assert_eq!(case.test_selector, selector);
        }
    }
    #[test]
    fn fault_matrix_evidence_verifier_rejects_tampered_transcript() {
        let evidence_dir = tempfile::tempdir().unwrap();
        let sha = "a".repeat(40);
        let source = SourceTreeStateV1 {
            head_sha: sha.clone(),
            clean: true,
        };
        let identity = source_identity_transcript(&sha, &source, &source).unwrap();
        fs::write(evidence_dir.path().join("source-identity.log"), &identity).unwrap();
        let mut results: Vec<_> = OPERATOR_CONTROL_FAULT_CASES
            .iter()
            .map(|case| {
                let evidence_file = format!("{}.log", case.invariant.case_id());
                let transcript = format!(
                    "$ {}\nexecution: completed\nexit_status: exit status: 0\ncapture: complete\n--- stdout ---\ntest result: ok. 1 passed;\n--- stderr ---\n",
                    case.test_selector
                );
                fs::write(evidence_dir.path().join(&evidence_file), &transcript).unwrap();
                OperatorControlFaultResultV1 {
                    case_id: case.invariant.case_id().to_owned(),
                    invariant: case.invariant,
                    injection: case.injection,
                    test_selector: case.test_selector.to_owned(),
                    outcome: FaultOutcome::Held,
                    evidence_file,
                    evidence_digest: sha256(transcript.as_bytes()),
                }
            })
            .collect();
        results.sort_by(|left, right| left.case_id.cmp(&right.case_id));
        let mut matrix = OperatorControlFaultMatrixRunV1 {
            schema: "harness.operator-control-fault-matrix.v1".to_owned(),
            implementation_sha: sha.clone(),
            source_identity: OperatorControlFaultSourceIdentityV1 {
                expected_release_sha: sha,
                preflight: source.clone(),
                postflight: source,
                evidence_file: "source-identity.log".to_owned(),
                evidence_digest: sha256(&identity),
            },
            results,
            sha256: String::new(),
        };
        matrix.sha256 = matrix.digest().unwrap();
        verify_fault_matrix_evidence(&matrix, evidence_dir.path()).unwrap();

        fs::write(
            evidence_dir.path().join("one_mutable_owner.log"),
            "tampered",
        )
        .unwrap();
        assert!(verify_fault_matrix_evidence(&matrix, evidence_dir.path()).is_err());
    }

    #[test]
    fn fault_runner_records_spawn_failure_instead_of_panicking() {
        let mut command = Command::new("/definitely-not-a-bildr-command");
        assert!(matches!(
            run_fault_command(&mut command),
            FaultExecution::SpawnFailed(_)
        ));
    }

    #[test]
    fn fault_pipe_capture_is_bounded_and_marks_truncation() {
        let capture = collect_child_pipe(Some(read_child_pipe(std::io::Cursor::new(vec![
            b'x';
            MAX_OPERATOR_CONTROL_PIPE_BYTES
                + 1
        ]))));
        assert_eq!(capture.bytes.len(), MAX_OPERATOR_CONTROL_PIPE_BYTES);
        assert!(capture.issue.is_some());
    }

    #[test]
    fn canonical_json_digest_ignores_object_order() {
        let first = br#"{"version":1,"schema":{"type":"object","required":["id"]}}"#;
        let reordered = br#"{"schema":{"required":["id"],"type":"object"},"version":1}"#;

        assert_eq!(
            canonical_json_sha256(first).unwrap(),
            canonical_json_sha256(reordered).unwrap()
        );
    }

    #[test]
    fn schema_compilation_rejects_malformed_keywords_and_references() {
        let registry = jsonschema::Registry::new().prepare().unwrap();
        let malformed = json!({
            "$schema": JSON_SCHEMA_2020_12,
            "$id": "urn:harness:malformed",
            "type": 42
        });
        assert!(compile_schema(Path::new("malformed.json"), &malformed, &registry).is_err());

        let unresolved = json!({
            "$schema": JSON_SCHEMA_2020_12,
            "$id": "urn:harness:unresolved",
            "$ref": "#/$defs/missing"
        });
        assert!(compile_schema(Path::new("unresolved.json"), &unresolved, &registry).is_err());
    }

    #[test]
    fn schema_catalog_rejects_missing_or_duplicate_identity() {
        let missing_id = tempfile::tempdir().unwrap();
        fs::write(
            missing_id.path().join("schema.json"),
            json!({
                "$schema": JSON_SCHEMA_2020_12,
                "properties": {"schema": {"const": "harness.example.v1"}}
            })
            .to_string(),
        )
        .unwrap();
        assert!(load_schema_catalog(missing_id.path()).is_err());

        let missing_discriminator = tempfile::tempdir().unwrap();
        fs::write(
            missing_discriminator.path().join("schema.json"),
            json!({
                "$schema": JSON_SCHEMA_2020_12,
                "$id": "urn:harness:missing-discriminator"
            })
            .to_string(),
        )
        .unwrap();
        assert!(load_schema_catalog(missing_discriminator.path()).is_err());

        let duplicate_id = tempfile::tempdir().unwrap();
        for (name, discriminator) in [
            ("first", "harness.first.v1"),
            ("second", "harness.second.v1"),
        ] {
            fs::write(
                duplicate_id.path().join(format!("{name}.json")),
                json!({
                    "$schema": JSON_SCHEMA_2020_12,
                    "$id": "urn:harness:duplicate",
                    "properties": {"schema": {"const": discriminator}}
                })
                .to_string(),
            )
            .unwrap();
        }
        assert!(load_schema_catalog(duplicate_id.path()).is_err());

        let duplicate_discriminator = tempfile::tempdir().unwrap();
        for name in ["first", "second"] {
            fs::write(
                duplicate_discriminator.path().join(format!("{name}.json")),
                json!({
                    "$schema": JSON_SCHEMA_2020_12,
                    "$id": format!("urn:harness:{name}"),
                    "properties": {"schema": {"const": "harness.example.v1"}}
                })
                .to_string(),
            )
            .unwrap();
        }
        assert!(load_schema_catalog(duplicate_discriminator.path()).is_err());
    }

    #[test]
    fn schema_compilation_resolves_catalog_references() {
        let root = json!({
            "$schema": JSON_SCHEMA_2020_12,
            "$id": "urn:harness:root",
            "type": "object",
            "properties": {"value": {"$ref": "urn:harness:target"}}
        });
        let target =
            json!({"$schema": JSON_SCHEMA_2020_12, "$id": "urn:harness:target", "type": "string"});
        let catalog = BTreeMap::from([
            (
                "harness.root.v1".to_owned(),
                SchemaDocument {
                    path: PathBuf::from("root.json"),
                    value: root,
                },
            ),
            (
                "harness.target.v1".to_owned(),
                SchemaDocument {
                    path: PathBuf::from("target.json"),
                    value: target,
                },
            ),
        ]);
        let registry = schema_registry(&catalog).unwrap();
        let validator = compile_schema(
            &catalog["harness.root.v1"].path,
            &catalog["harness.root.v1"].value,
            &registry,
        )
        .unwrap();
        assert!(validator.is_valid(&json!({"value": "resolved"})));
        assert!(!validator.is_valid(&json!({"value": 42})));
    }

    #[test]
    fn candidate_schema_enforces_component_risk_pairings() {
        let registry = jsonschema::Registry::new().prepare().unwrap();
        let schema: Value = serde_json::from_str(include_str!(
            "../../schemas/harness.improvement-candidate.v1.schema.json"
        ))
        .unwrap();
        let validator =
            compile_schema(Path::new("candidate.schema.json"), &schema, &registry).unwrap();
        let example: Value = serde_json::from_str(include_str!(
            "../../examples/self-improvement/candidate.example.json"
        ))
        .unwrap();
        assert!(validator.is_valid(&example));

        let mut underclassified = example.clone();
        underclassified["edit"]["dimension"] = json!("validator_selection");
        underclassified["edit"]["risk_class"] = json!("green");
        assert!(!validator.is_valid(&underclassified));

        let mut valid_amber = example.clone();
        valid_amber["edit"]["dimension"] = json!("validator_selection");
        valid_amber["edit"]["risk_class"] = json!("amber");
        assert!(validator.is_valid(&valid_amber));

        let mut duplicate_prediction = example.clone();
        let first_prediction = duplicate_prediction["predictions"][0].clone();
        duplicate_prediction["predictions"]
            .as_array_mut()
            .unwrap()
            .push(first_prediction);
        assert!(!validator.is_valid(&duplicate_prediction));

        for component_id in ["frozen_safety_anchor", "unknown_component"] {
            let mut forbidden = example.clone();
            forbidden["edit"]["dimension"] = json!(component_id);
            assert!(!validator.is_valid(&forbidden));
        }
    }

    #[test]
    fn experiment_schema_requires_evidence_for_passed_stages() {
        let registry = jsonschema::Registry::new().prepare().unwrap();
        let schema: Value = serde_json::from_str(include_str!(
            "../../schemas/harness.experiment.v1.schema.json"
        ))
        .unwrap();
        let validator =
            compile_schema(Path::new("experiment.schema.json"), &schema, &registry).unwrap();
        let example: Value = serde_json::from_str(include_str!(
            "../../examples/self-improvement/experiment.example.json"
        ))
        .unwrap();
        assert!(validator.is_valid(&example));

        let mut missing_passed_evidence = example;
        let passed_stage = missing_passed_evidence["stages"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .find(|stage| stage["state"] == "passed")
            .expect("example contains a passed stage");
        passed_stage["evidence"] = Value::Null;
        assert!(!validator.is_valid(&missing_passed_evidence));
    }

    #[test]
    fn trace_v2_schema_preserves_projection_branch_bounds() {
        let registry = jsonschema::Registry::new().prepare().unwrap();
        let schema: Value =
            serde_json::from_str(include_str!("../../schemas/harness.trace.v2.schema.json"))
                .unwrap();
        let validator =
            compile_schema(Path::new("trace-v2.schema.json"), &schema, &registry).unwrap();
        let example: Value = serde_json::from_str(include_str!(
            "../../examples/self-improvement/trace.v2.example.json"
        ))
        .unwrap();
        assert!(validator.is_valid(&example));

        let mut wrong_bound = example;
        wrong_bound["branches"][0]["metadata"]["path_bound"] = json!(1);
        assert!(!validator.is_valid(&wrong_bound));
    }

    #[test]
    fn knowledge_schema_requires_active_human_review_and_safe_optional_scope_ids() {
        let registry = jsonschema::Registry::new().prepare().unwrap();
        let schema: Value = serde_json::from_str(include_str!(
            "../../schemas/harness.knowledge-item.v1.schema.json"
        ))
        .unwrap();
        let validator =
            compile_schema(Path::new("knowledge.schema.json"), &schema, &registry).unwrap();
        let example: Value = serde_json::from_str(include_str!(
            "../../examples/self-improvement/knowledge-item.example.json"
        ))
        .unwrap();
        assert!(validator.is_valid(&example));

        let mut unreviewed_active = example.clone();
        unreviewed_active["review"]["state"] = json!("unreviewed");
        unreviewed_active["review"]["reviewer_id"] = Value::Null;
        unreviewed_active["review"]["reviewed_at"] = Value::Null;
        unreviewed_active["review"]["receipt"] = Value::Null;
        assert!(!validator.is_valid(&unreviewed_active));

        let mut free_text_scope = example.clone();
        free_text_scope["scope"]["model_family"] = json!("model family with spaces");
        assert!(!validator.is_valid(&free_text_scope));
        let mut free_text_reviewer = example;
        free_text_reviewer["review"]["reviewer_id"] = json!("operator name");
        assert!(!validator.is_valid(&free_text_reviewer));
    }

    #[test]
    fn outcome_schema_enforces_closed_manual_label_pairs() {
        let registry = jsonschema::Registry::new().prepare().unwrap();
        let schema: Value =
            serde_json::from_str(include_str!("../../schemas/harness.outcome.v1.schema.json"))
                .unwrap();
        let validator =
            compile_schema(Path::new("outcome.schema.json"), &schema, &registry).unwrap();
        let example: Value = serde_json::from_str(include_str!(
            "../../examples/self-improvement/outcome.example.json"
        ))
        .unwrap();
        assert!(validator.is_valid(&example));

        let mut acceptance_wrong_pair = example.clone();
        acceptance_wrong_pair["classification"] = json!("negative");
        acceptance_wrong_pair["code"] = json!("accepted_after_correction");
        assert!(!validator.is_valid(&acceptance_wrong_pair));

        let mut review_wrong_code = example.clone();
        review_wrong_code["dimension"] = json!("review_regression");
        review_wrong_code["classification"] = json!("negative");
        review_wrong_code["code"] = json!("arbitrary");
        assert!(!validator.is_valid(&review_wrong_code));

        let mut rollback_wrong_pair = example;
        rollback_wrong_pair["dimension"] = json!("rollback");
        rollback_wrong_pair["classification"] = json!("positive");
        rollback_wrong_pair["code"] = json!("rollback_recorded");
        assert!(!validator.is_valid(&rollback_wrong_pair));

        let mut automated_wrong_pair = rollback_wrong_pair;
        automated_wrong_pair["dimension"] = json!("ci_required_checks");
        automated_wrong_pair["classification"] = json!("positive");
        automated_wrong_pair["code"] = json!("failed");
        automated_wrong_pair["confidence"] = json!("authoritative");
        automated_wrong_pair["source"]["kind"] = json!("validation");
        assert!(!validator.is_valid(&automated_wrong_pair));
    }

    #[test]
    fn taskset_schema_rejects_open_split_and_extra_case_fields() {
        let registry = jsonschema::Registry::new().prepare().unwrap();
        let schema: Value =
            serde_json::from_str(include_str!("../../schemas/harness.taskset.v1.schema.json"))
                .unwrap();
        let validator =
            compile_schema(Path::new("taskset.schema.json"), &schema, &registry).unwrap();
        let example: Value = serde_json::from_str(include_str!(
            "../../examples/self-improvement/taskset.example.json"
        ))
        .unwrap();
        assert!(validator.is_valid(&example));
        let mut split = example.clone();
        split["cases"][0]["split"] = json!("unreviewed");
        assert!(!validator.is_valid(&split));
        let mut open = example;
        open["cases"][0]["answer"] = json!("secret");
        assert!(!validator.is_valid(&open));
    }

    #[test]
    fn example_validation_rejects_unknown_discriminators_and_extra_fields() {
        let schema_path = PathBuf::from("shape.schema.json");
        let schema = json!({
            "$schema": JSON_SCHEMA_2020_12,
            "$id": "urn:harness:shape.v1",
            "type": "object",
            "additionalProperties": false,
            "required": ["schema", "value"],
            "properties": {
                "schema": {"const": "harness.shape.v1"},
                "value": {"type": "string"}
            }
        });
        let catalog = BTreeMap::from([(
            "harness.shape.v1".to_owned(),
            SchemaDocument {
                path: schema_path,
                value: schema,
            },
        )]);
        let registry = schema_registry(&catalog).unwrap();

        assert!(
            validate_schema_example(
                Path::new("unknown.json"),
                &json!({"schema": "harness.shape.v2", "value": "ok"}),
                &catalog,
                &registry,
            )
            .is_err()
        );
        assert!(
            validate_schema_example(
                Path::new("extra.json"),
                &json!({"schema": "harness.shape.v1", "value": "ok", "extra": true}),
                &catalog,
                &registry,
            )
            .is_err()
        );
    }

    #[test]
    fn architecture_policy_detects_direct_renamed_and_target_orchestrator_dependencies() {
        let allowed: toml::Value =
            toml::from_str("[dependencies]\nharness-domain = { path = \"../harness-domain\" }")
                .unwrap();
        assert!(!manifest_depends_on_orchestrator(&allowed));

        for manifest in [
            "[dependencies]\nharness-orchestrator = { path = \"../harness-orchestrator\" }",
            "[dependencies]\ncontroller = { package = \"harness-orchestrator\", path = \"../harness-orchestrator\" }",
            "[target.'cfg(unix)'.dev-dependencies]\nharness-orchestrator = { path = \"../harness-orchestrator\" }",
        ] {
            let value: toml::Value = toml::from_str(manifest).unwrap();
            assert!(manifest_depends_on_orchestrator(&value), "{manifest}");
        }
    }

    #[test]
    fn architecture_policy_enforces_only_the_new_source_roots_line_budget() {
        let root = tempfile::tempdir().unwrap();
        let trace = root.path().join("crates/harness-trace/src");
        let improvement = root.path().join("ui/src/improvement");
        let legacy = root.path().join("crates/harness-orchestrator/src");
        fs::create_dir_all(&trace).unwrap();
        fs::create_dir_all(&improvement).unwrap();
        fs::create_dir_all(&legacy).unwrap();
        fs::write(trace.join("within.rs"), "one\ntwo\n").unwrap();
        fs::write(trace.join("over.rs"), "one\ntwo\nthree\n").unwrap();
        fs::write(improvement.join("over.tsx"), "one\ntwo\nthree\n").unwrap();
        fs::write(legacy.join("legacy.rs"), "one\ntwo\nthree\nfour\n").unwrap();

        let rust =
            source_line_budget_violations(&root.path().join("crates/harness-trace"), &["rs"], 2)
                .unwrap();
        let ui = source_line_budget_violations(
            &root.path().join("ui/src/improvement"),
            &["ts", "tsx", "css"],
            2,
        )
        .unwrap();
        assert_eq!(rust.len(), 1);
        assert!(rust[0].contains("over.rs"));
        assert_eq!(ui.len(), 1);
        assert!(ui[0].contains("over.tsx"));
    }
}

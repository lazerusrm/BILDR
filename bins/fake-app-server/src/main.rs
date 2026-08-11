use std::{
    collections::HashMap,
    env, fs,
    io::Write,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use ulid::Ulid;

const VERSION: &str = "0.147.0";
const ROOT_SCHEMA: &str = include_str!(
    "../../../generated/codex-app-server-schema/codex_app_server_protocol.v2.schemas.json"
);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Scenario {
    Normal,
    Approval,
    Malformed,
    Crash,
}

impl Scenario {
    fn from_environment() -> Self {
        match env::var("HARNESS_FAKE_SCENARIO")
            .unwrap_or_default()
            .as_str()
        {
            "approval" => Self::Approval,
            "malformed" => Self::Malformed,
            "crash" => Self::Crash,
            _ => Self::Normal,
        }
    }
}

struct PendingTurn {
    thread_id: String,
    turn_id: String,
    response_text: String,
    worker_mutation: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    if matches!(args.as_slice(), [arg] if arg == "--version") {
        println!("codex-cli {VERSION}");
        return Ok(());
    }
    if args.first().is_some_and(|arg| arg == "app-server")
        && args.get(1).is_some_and(|arg| arg == "generate-json-schema")
    {
        let out_index = args
            .iter()
            .position(|arg| arg == "--out")
            .context("generate-json-schema requires --out")?;
        let output = PathBuf::from(
            args.get(out_index + 1)
                .context("generate-json-schema --out requires a directory")?,
        );
        fs::create_dir_all(&output)?;
        fs::write(
            output.join("codex_app_server_protocol.v2.schemas.json"),
            ROOT_SCHEMA,
        )?;
        return Ok(());
    }
    if args.first().is_some_and(|arg| arg == "app-server") {
        return serve(Scenario::from_environment()).await;
    }
    bail!(
        "usage: fake-app-server --version | app-server generate-json-schema --out DIR | app-server --listen stdio://"
    )
}

async fn serve(scenario: Scenario) -> Result<()> {
    let stdin = tokio::io::stdin();
    let mut lines = BufReader::new(stdin).lines();
    let mut stdout = tokio::io::stdout();
    let mut thread_goals = HashMap::<String, String>::new();
    let mut pending_approval = HashMap::<String, PendingTurn>::new();

    while let Some(line) = lines.next_line().await? {
        if line.len() > 8 * 1024 * 1024 {
            continue;
        }
        let message: Value = match serde_json::from_str(&line) {
            Ok(message) => message,
            Err(_) => continue,
        };
        let Some(method) = message.get("method").and_then(Value::as_str) else {
            if let Some(key) = message.get("id").map(request_key)
                && let Some(turn) = pending_approval.remove(&key)
            {
                let accepted = message
                    .pointer("/result/decision")
                    .and_then(Value::as_str)
                    .is_some_and(|decision| matches!(decision, "accept" | "approved" | "approve"));
                if accepted && let Some(readme) = turn.worker_mutation.as_deref() {
                    apply_worker_mutation(readme)?;
                }
                finish_turn(
                    &mut stdout,
                    &turn.thread_id,
                    &turn.turn_id,
                    &turn.response_text,
                    if accepted { "completed" } else { "failed" },
                )
                .await?;
            }
            continue;
        };
        let Some(id) = message.get("id").cloned() else {
            continue;
        };
        let params = message.get("params").cloned().unwrap_or_else(|| json!({}));
        match method {
            "initialize" => {
                send(
                    &mut stdout,
                    json!({"id": id, "result": {"serverInfo": {"name": "fake-app-server", "version": VERSION}}}),
                )
                .await?;
            }
            "experimentalFeature/list" => {
                send(
                    &mut stdout,
                    json!({
                        "id": id,
                        "result": {
                            "data": [
                                {
                                    "name": "multi_agent",
                                    "enabled": true,
                                    "defaultEnabled": true,
                                    "stage": "stable"
                                },
                                {
                                    "name": "multi_agent_v2",
                                    "enabled": false,
                                    "defaultEnabled": false,
                                    "stage": "stable"
                                }
                            ],
                            "nextCursor": null
                        }
                    }),
                )
                .await?;
            }
            "thread/start" => {
                let thread_id = format!("fake-thread-{}", Ulid::generate());
                let cwd = params.get("cwd").cloned().unwrap_or_else(|| json!("/tmp"));
                let model = params
                    .get("model")
                    .cloned()
                    .unwrap_or_else(|| json!("gpt-5.6-sol"));
                let thread = thread_value(&thread_id, cwd.clone());
                send(
                    &mut stdout,
                    json!({
                        "id": id,
                        "result": {
                            "thread": thread,
                            "model": model,
                            "modelProvider": "fake",
                            "cwd": cwd,
                            "approvalPolicy": params.get("approvalPolicy").cloned().unwrap_or_else(|| json!("untrusted")),
                            "approvalsReviewer": "user",
                            "sandbox": params.get("sandbox").cloned().unwrap_or_else(|| json!("read-only")),
                            "reasoningEffort": null,
                            "serviceTier": null,
                            "instructionSources": []
                        }
                    }),
                )
                .await?;
                send(
                    &mut stdout,
                    json!({"method": "thread/started", "params": {"thread": thread_value(&thread_id, cwd)}}),
                )
                .await?;
            }
            "thread/resume" => {
                let thread_id = params
                    .get("threadId")
                    .and_then(Value::as_str)
                    .unwrap_or("fake-thread")
                    .to_owned();
                send(
                    &mut stdout,
                    json!({"id": id, "result": {"thread": thread_value(&thread_id, json!("/tmp"))}}),
                )
                .await?;
            }
            "thread/goal/set" => {
                if let (Some(thread_id), Some(objective)) = (
                    params.get("threadId").and_then(Value::as_str),
                    params.get("objective").and_then(Value::as_str),
                ) {
                    thread_goals.insert(thread_id.to_owned(), objective.to_owned());
                }
                send(
                    &mut stdout,
                    json!({"id": id, "result": {"goal": {"status": "active"}}}),
                )
                .await?;
            }
            "turn/start" | "review/start" => {
                let thread_id = params
                    .get("threadId")
                    .and_then(Value::as_str)
                    .unwrap_or("fake-thread")
                    .to_owned();
                let turn_id = format!("fake-turn-{}", Ulid::generate());
                let input = input_text(&params);
                let response_text =
                    scripted_response(&input, thread_goals.get(&thread_id).map(String::as_str));
                let worker_mutation = worker_mutation_path(&input, &params);
                send(
                    &mut stdout,
                    json!({"id": id, "result": {"turn": turn_value(&turn_id, "inProgress")}}),
                )
                .await?;
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                send(
                    &mut stdout,
                    json!({"method": "turn/started", "params": {"threadId": thread_id, "turn": turn_value(&turn_id, "inProgress")}}),
                )
                .await?;
                if scenario == Scenario::Crash {
                    std::process::exit(23);
                }
                if scenario == Scenario::Malformed {
                    stdout
                        .write_all(b"{this is deliberately malformed json\n")
                        .await?;
                    stdout.flush().await?;
                }
                if scenario == Scenario::Approval {
                    let approval_id = format!("fake-approval-{}", Ulid::generate());
                    send(
                        &mut stdout,
                        json!({
                            "id": approval_id,
                            "method": "item/commandExecution/requestApproval",
                            "params": {
                                "threadId": thread_id,
                                "turnId": turn_id,
                                "itemId": format!("fake-item-{}", Ulid::generate()),
                                "command": "printf fake-app-server",
                                "cwd": "/tmp",
                                "reason": "deterministic approval scenario"
                            }
                        }),
                    )
                    .await?;
                    pending_approval.insert(
                        approval_id,
                        PendingTurn {
                            thread_id,
                            turn_id,
                            response_text,
                            worker_mutation,
                        },
                    );
                } else {
                    if let Some(readme) = worker_mutation.as_deref() {
                        apply_worker_mutation(readme)?;
                    }
                    finish_turn(
                        &mut stdout,
                        &thread_id,
                        &turn_id,
                        &response_text,
                        "completed",
                    )
                    .await?;
                }
            }
            "turn/steer" => {
                send(
                    &mut stdout,
                    json!({"id": id, "result": {"turnId": params.get("expectedTurnId")}}),
                )
                .await?;
            }
            "turn/interrupt" => {
                send(&mut stdout, json!({"id": id, "result": {}})).await?;
                if let (Some(thread_id), Some(turn_id)) = (
                    params.get("threadId").and_then(Value::as_str),
                    params.get("turnId").and_then(Value::as_str),
                ) {
                    send(
                        &mut stdout,
                        json!({"method": "turn/completed", "params": {"threadId": thread_id, "turn": turn_value(turn_id, "interrupted")}}),
                    )
                    .await?;
                }
            }
            _ => {
                send(
                    &mut stdout,
                    json!({"id": id, "error": {"code": -32601, "message": format!("unsupported fake method {method}")}}),
                )
                .await?;
            }
        }
    }
    Ok(())
}

async fn finish_turn(
    stdout: &mut tokio::io::Stdout,
    thread_id: &str,
    turn_id: &str,
    response_text: &str,
    status: &str,
) -> Result<()> {
    let item_id = format!("fake-item-{}", Ulid::generate());
    send(
        stdout,
        json!({
            "method": "item/started",
            "params": {"threadId": thread_id, "turnId": turn_id, "item": {"id": item_id, "type": "agentMessage", "text": ""}}
        }),
    )
    .await?;
    send(
        stdout,
        json!({
            "method": "item/completed",
            "params": {"threadId": thread_id, "turnId": turn_id, "item": {"id": item_id, "type": "agentMessage", "text": response_text}}
        }),
    )
    .await?;
    let usage = json!({
        "inputTokens": 1800,
        "cachedInputTokens": 400,
        "cacheWriteInputTokens": 0,
        "outputTokens": 320,
        "reasoningOutputTokens": 120,
        "totalTokens": 2120
    });
    send(
        stdout,
        json!({
            "method": "thread/tokenUsage/updated",
            "params": {"threadId": thread_id, "turnId": turn_id, "tokenUsage": {"last": usage, "total": usage, "modelContextWindow": 400000}}
        }),
    )
    .await?;
    send(
        stdout,
        json!({"method": "turn/completed", "params": {"threadId": thread_id, "turn": turn_value(turn_id, status)}}),
    )
    .await
}

async fn send(stdout: &mut tokio::io::Stdout, value: Value) -> Result<()> {
    let mut bytes = serde_json::to_vec(&value)?;
    bytes.push(b'\n');
    stdout.write_all(&bytes).await?;
    stdout.flush().await?;
    Ok(())
}

fn thread_value(id: &str, cwd: Value) -> Value {
    json!({
        "id": id,
        "preview": "BILDR deterministic fake thread",
        "cwd": cwd,
        "createdAt": 1_786_000_000,
        "updatedAt": 1_786_000_000,
        "status": {"type": "idle"},
        "turns": []
    })
}

fn turn_value(id: &str, status: &str) -> Value {
    json!({"id": id, "status": status, "items": [], "error": null})
}

fn input_text(params: &Value) -> String {
    params
        .get("input")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|item| item.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("\n")
}

fn scripted_response(input: &str, objective: Option<&str>) -> String {
    if input.contains("Intent interview for this run.") {
        return json!({
            "schema": "harness.intent-interview-turn.v1",
            "status": "question",
            "question": "What observable final behavior matters most when this task is complete?",
            "why_it_matters": "This determines the acceptance example without prescribing an implementation.",
            "recommended_answer": null,
            "brief": null
        })
        .to_string();
    }
    if input.contains("Human response:") {
        return json!({
            "schema": "harness.intent-interview-turn.v1",
            "status": "ready",
            "question": null,
            "why_it_matters": null,
            "recommended_answer": null,
            "brief": {
                "refined_objective": objective.unwrap_or("Complete the requested repository change"),
                "intended_final_shape": ["The requested behavior is observable on the authoritative path"],
                "hard_constraints": [],
                "preferences": [],
                "non_goals": ["Unrelated repository redesign"],
                "acceptance_examples": ["The human's stated outcome works in the authoritative pipeline"],
                "planner_may_decide": ["Implementation details not specified by the human"],
                "assumptions_to_validate": ["The authoritative pipeline is available in the repository"]
            }
        })
        .to_string();
    }
    if input.contains("Planning posture:") && input.contains("Plan the shortest credible path") {
        let base_sha = extract_after(input, "Every task must use base SHA ", 40)
            .or_else(|| extract_after(input, "Base SHA: ", 40))
            .filter(|sha| sha.len() == 40 && sha.bytes().all(|byte| byte.is_ascii_hexdigit()))
            .unwrap_or_else(|| "0000000000000000000000000000000000000000".to_owned())
            .to_ascii_lowercase();
        return json!({
            "schema": "harness.orchestration.plan.v1",
            "summary": "A bounded task generated by the deterministic fake App Server.",
            "tasks": [{
                "schema": "harness.orchestration.task.v1",
                "program_id": "fake-run",
                "task_id": "FAKE-001",
                "title": "Exercise the Harness execution path",
                "state": "proposed",
                "priority": "P1",
                "execution_mode": "agent",
                "owner_profile": "governor",
                "reviewer_profile": "verifier",
                "checklist_rows": ["Run deterministic adapter smoke"],
                "authority_refs": ["README.md"],
                "base_sha": base_sha,
                "dependency_shas": {},
                "depends_on": [],
                "owned_paths": ["README.md"],
                "forbidden_paths": [".harness-runtime/**"],
                "reserved_serial_paths": [],
                "objective": objective.unwrap_or("Exercise the Harness execution path"),
                "milestones": [
                    {
                        "id": "slice",
                        "title": "Create the bounded behavior slice",
                        "objective": "Make the requested behavior observable with the smallest credible change.",
                        "success_criteria": ["A candidate exists in the leased worktree"]
                    },
                    {
                        "id": "exercise",
                        "title": "Exercise the authoritative path",
                        "objective": "Run the smallest behavior check that can falsify the candidate.",
                        "success_criteria": ["The authoritative check records its result"]
                    },
                    {
                        "id": "signoff",
                        "title": "Add focused proof and prepare signoff",
                        "objective": "Protect the accepted behavior without freezing provisional internals.",
                        "success_criteria": ["Controller evidence covers the accepted behavior"]
                    }
                ],
                "non_goals": ["Do not make external writes"],
                "success_criteria": ["The bounded work is independently reviewable"],
                "required_positive_tests": ["git diff --check"],
                "required_negative_tests": [],
                "required_metrics": [],
                "required_evidence": ["controller-custodied diff"],
                "proof_limits": ["Fake App Server does not implement repository edits"],
                "diff_budget": {"files": 3, "lines": 120},
                "token_budget": 12000,
                "tool_budget": 20,
                "lease_expires_at": "controller-managed",
                "stop_conditions": ["Owned-path ambiguity"],
                "handoff_path": "controller://fake-handoff",
                "risk_flags": []
            }]
        })
        .to_string();
    }
    if input.contains("Try to falsify whether this plan can deliver the objective") {
        return json!({
            "verdict": "accept",
            "summary": "The deterministic plan has one bounded governor-owned critical path and an early behavior check.",
            "findings": [],
            "evidence": {
                "inspected_files": ["README.md"],
                "critical_path": [{
                    "task_id": "FAKE-001",
                    "why_critical": "It owns the complete bounded behavior slice.",
                    "behavioral_proof": "The authoritative check exercises the candidate before focused hardening."
                }],
                "failure_modes": [{
                    "failure_mode": "The candidate could fail the authoritative behavior check.",
                    "mitigation": "Revise the bounded slice before adding regression coverage."
                }]
            }
        })
        .to_string();
    }
    if input.contains("Independently verify") {
        return json!({
            "verdict": "accept",
            "summary": "Deterministic fake verifier accepted the supplied commit.",
            "findings": []
        })
        .to_string();
    }
    if input.contains("Audit exact integrated head")
        || objective.is_some_and(|goal| goal.contains("Independently audit integrated head"))
    {
        return json!({
            "verdict": "accept",
            "summary": "Deterministic fake final auditor accepted the exact integrated head.",
            "findings": []
        })
        .to_string();
    }
    if input.contains("Implement only this task") {
        return "Updated README.md with the deterministic smoke marker; controller custody and independent review remain authoritative.".to_owned();
    }
    "Fake App Server completed the bounded turn. No repository files were changed; this simulator is intended for protocol and UI smoke testing.".to_owned()
}

fn worker_mutation_path(input: &str, params: &Value) -> Option<PathBuf> {
    if !input.contains("Implement only this task") {
        return None;
    }
    let cwd = PathBuf::from(params.get("cwd")?.as_str()?);
    let readme = cwd.join("README.md");
    readme.is_file().then_some(readme)
}

fn apply_worker_mutation(readme: &Path) -> Result<()> {
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(readme)
        .with_context(|| format!("failed to open fake worker target {}", readme.display()))?;
    writeln!(
        file,
        "\n<!-- deterministic Harness fake-App-Server smoke change -->"
    )?;
    file.sync_all()?;
    Ok(())
}

fn extract_after(input: &str, marker: &str, length: usize) -> Option<String> {
    let start = input.find(marker)? + marker.len();
    input
        .get(start..start.saturating_add(length))
        .map(ToOwned::to_owned)
}

fn request_key(id: &Value) -> String {
    id.as_str()
        .map_or_else(|| id.to_string(), ToOwned::to_owned)
}

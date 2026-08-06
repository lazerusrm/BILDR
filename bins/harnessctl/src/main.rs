use std::{fs, net::IpAddr, path::PathBuf};

use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand};
use reqwest::{Client, Method, header};
use serde_json::{Value, json};
use url::Url;

#[derive(Parser)]
#[command(
    name = "harnessctl",
    version,
    about = "Operator CLI for Harness Console"
)]
struct Cli {
    #[arg(long, env = "HARNESS_URL", default_value = "http://127.0.0.1:7310")]
    url: String,
    #[arg(long, global = true)]
    compact: bool,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Show daemon, Codex, database, and scheduler status.
    Status,
    /// Check health and all runtime components.
    Doctor,
    /// Inspect runtime status (optionally just one component).
    Runtime {
        #[arg(value_parser = ["daemon", "codex", "database", "scheduler"])]
        component: Option<String>,
    },
    Repo {
        #[command(subcommand)]
        command: RepoCommand,
    },
    Run {
        #[command(subcommand)]
        command: RunCommand,
    },
    Approvals {
        #[command(subcommand)]
        command: ApprovalCommand,
    },
    Worktree {
        #[command(subcommand)]
        command: WorktreeCommand,
    },
    Agent {
        #[command(subcommand)]
        command: AgentCommand,
    },
}

#[derive(Subcommand)]
enum RepoCommand {
    List,
    Add {
        #[arg(long, default_value = "neuralmatrix")]
        profile: String,
        #[arg(long)]
        path: PathBuf,
        #[arg(long)]
        expected_origin: Option<String>,
    },
    Show {
        repository_id: String,
    },
    Inspect {
        repository_id: String,
    },
}

#[derive(Subcommand)]
enum RunCommand {
    List {
        #[arg(long)]
        repo: Option<String>,
    },
    Show {
        run_id: String,
    },
    Create(CreateRun),
    StartArchitecture {
        run_id: String,
    },
    ApprovePlan {
        run_id: String,
        #[arg(long)]
        digest: Option<String>,
    },
    ApproveIntegration {
        run_id: String,
        #[arg(long)]
        expected_head: Option<String>,
    },
    PublishDraftPr {
        run_id: String,
        #[arg(long)]
        expected_head: Option<String>,
        #[arg(long)]
        title: String,
        #[arg(long)]
        body_appendix: Option<String>,
    },
    Pause {
        run_id: String,
    },
    Resume {
        run_id: String,
    },
    Stop {
        run_id: String,
        #[arg(long)]
        interrupt: bool,
    },
    Export {
        run_id: String,
    },
    Usage {
        run_id: String,
    },
    Evidence {
        run_id: String,
    },
    RetryTask {
        task_id: String,
        #[arg(long)]
        reason: String,
        #[arg(long)]
        revised_objective: Option<String>,
        #[arg(long, default_value = "same", value_parser = ["same", "escalate_terra"])]
        model_route: String,
        #[arg(long, default_value_t = 0)]
        additional_token_budget: u64,
    },
    RequestReview {
        task_id: String,
    },
}

#[derive(Args)]
struct CreateRun {
    #[arg(long)]
    repo: String,
    #[arg(long, conflicts_with = "objective_file")]
    objective: Option<String>,
    #[arg(long, conflicts_with = "objective")]
    objective_file: Option<PathBuf>,
    #[arg(long, default_value = "plan_and_implement")]
    mode: String,
    #[arg(long, default_value = "local_only")]
    publication: String,
    #[arg(long)]
    base: Option<String>,
    #[arg(long)]
    title: Option<String>,
    #[arg(long)]
    token_budget: Option<u64>,
}

#[derive(Subcommand)]
enum ApprovalCommand {
    List {
        #[arg(long)]
        run: Option<String>,
    },
    Decide {
        approval_id: String,
        #[arg(value_parser = ["accept", "decline", "cancel"])]
        decision: String,
        #[arg(long)]
        note: Option<String>,
        #[arg(long)]
        expected_version: Option<u64>,
    },
}

#[derive(Subcommand)]
enum WorktreeCommand {
    List {
        #[arg(long)]
        run: Option<String>,
    },
    Preserve {
        worktree_id: String,
        #[arg(long)]
        reason: Option<String>,
    },
}

#[derive(Subcommand)]
enum AgentCommand {
    Show { agent_id: String },
    Steer { agent_id: String, message: String },
    Interrupt { agent_id: String },
}

struct ApiClient {
    http: Client,
    base: Url,
    origin: String,
    cookie: String,
    csrf: String,
}

impl ApiClient {
    async fn connect(base: &str) -> Result<Self> {
        let base = Url::parse(base).context("HARNESS_URL is not a valid URL")?;
        validate_local_url(&base)?;
        let origin = base.origin().ascii_serialization();
        let http = Client::builder()
            .timeout(std::time::Duration::from_secs(120))
            .build()?;
        let endpoint = base.join("/api/v1/session")?;
        let response = http
            .post(endpoint)
            .header(header::ORIGIN, &origin)
            .send()
            .await
            .context("could not connect to harnessd")?;
        let status = response.status();
        let cookie = response
            .headers()
            .get(header::SET_COOKIE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .context("harnessd did not return a local session cookie")?
            .to_owned();
        let body: Value = response.json().await?;
        if !status.is_success() {
            bail!("session request failed ({status}): {body}");
        }
        let csrf = body
            .get("csrf_token")
            .and_then(Value::as_str)
            .context("harnessd session response has no CSRF token")?
            .to_owned();
        Ok(Self {
            http,
            base,
            origin,
            cookie,
            csrf,
        })
    }

    async fn get(&self, path: &str) -> Result<Value> {
        self.call(Method::GET, path, None).await
    }

    async fn post(&self, path: &str, body: Value) -> Result<Value> {
        self.call(Method::POST, path, Some(body)).await
    }

    async fn call(&self, method: Method, path: &str, body: Option<Value>) -> Result<Value> {
        let url = self.base.join(path)?;
        let mutation = method != Method::GET && method != Method::HEAD;
        let mut request = self
            .http
            .request(method, url)
            .header(header::COOKIE, &self.cookie);
        if mutation {
            request = request
                .header(header::ORIGIN, &self.origin)
                .header("x-harness-csrf", &self.csrf);
        }
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await?;
        let status = response.status();
        let text = response.text().await?;
        let value = if text.trim().is_empty() {
            Value::Null
        } else {
            serde_json::from_str(&text).unwrap_or_else(|_| json!({"body": text}))
        };
        if !status.is_success() {
            let message = value
                .pointer("/error/message")
                .and_then(Value::as_str)
                .unwrap_or("request failed");
            bail!("{message} ({status}): {value}");
        }
        Ok(value)
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let api = ApiClient::connect(&cli.url).await?;
    let value = execute(&api, cli.command).await?;
    if cli.compact {
        println!("{}", serde_json::to_string(&value)?);
    } else {
        println!("{}", serde_json::to_string_pretty(&value)?);
    }
    Ok(())
}

async fn execute(api: &ApiClient, command: Command) -> Result<Value> {
    match command {
        Command::Status => api.get("/api/v1/runtime").await,
        Command::Doctor => {
            let health = api.get("/api/v1/health").await?;
            let runtime = api.get("/api/v1/runtime").await?;
            let ok = health.get("status") == Some(&json!("ok"))
                && runtime.pointer("/daemon/state") == Some(&json!("ready"))
                && runtime.pointer("/database/state") == Some(&json!("ready"));
            if !ok {
                bail!("daemon health check is degraded: {runtime}");
            }
            Ok(json!({"status": "ok", "health": health, "runtime": runtime}))
        }
        Command::Runtime { component } => {
            let value = api.get("/api/v1/runtime").await?;
            Ok(component
                .as_deref()
                .and_then(|name| value.get(name).cloned())
                .unwrap_or(value))
        }
        Command::Repo { command } => match command {
            RepoCommand::List => api.get("/api/v1/repositories").await,
            RepoCommand::Add {
                profile,
                path,
                expected_origin,
            } => {
                let path = path.canonicalize().with_context(|| {
                    format!("repository path {} is not accessible", path.display())
                })?;
                api.post(
                    "/api/v1/repositories",
                    json!({"profile_id": profile, "root_path": path, "expected_origin": expected_origin}),
                )
                .await
            }
            RepoCommand::Show { repository_id } => {
                api.get(&format!("/api/v1/repositories/{repository_id}"))
                    .await
            }
            RepoCommand::Inspect { repository_id } => {
                api.post(
                    &format!("/api/v1/repositories/{repository_id}/inspect"),
                    json!({}),
                )
                .await
            }
        },
        Command::Run { command } => match command {
            RunCommand::List { repo } => {
                let path = repo.map_or_else(
                    || "/api/v1/runs".to_owned(),
                    |repo| format!("/api/v1/runs?repository_id={repo}"),
                );
                api.get(&path).await
            }
            RunCommand::Show { run_id } => api.get(&format!("/api/v1/runs/{run_id}")).await,
            RunCommand::Create(args) => {
                let objective = match (args.objective, args.objective_file) {
                    (Some(value), None) if !value.trim().is_empty() => value,
                    (None, Some(path)) => fs::read_to_string(&path)
                        .with_context(|| format!("failed to read {}", path.display()))?,
                    _ => bail!("provide either --objective or --objective-file"),
                };
                api.post(
                    "/api/v1/runs",
                    json!({
                        "repository_id": args.repo,
                        "objective": objective,
                        "mode": args.mode,
                        "publication": args.publication,
                        "base_ref": args.base,
                        "title": args.title,
                        "run_token_budget": args.token_budget,
                    }),
                )
                .await
            }
            RunCommand::StartArchitecture { run_id } => {
                api.post(
                    &format!("/api/v1/runs/{run_id}/start-architecture"),
                    json!({}),
                )
                .await
            }
            RunCommand::ApprovePlan { run_id, digest } => {
                let digest = match digest {
                    Some(digest) => digest,
                    None => api
                        .get(&format!("/api/v1/runs/{run_id}"))
                        .await?
                        .get("plan_digest")
                        .and_then(Value::as_str)
                        .context("run has no proposed plan digest")?
                        .to_owned(),
                };
                api.post(
                    &format!("/api/v1/runs/{run_id}/plan/approve"),
                    json!({"task_graph_digest": digest}),
                )
                .await
            }
            RunCommand::ApproveIntegration {
                run_id,
                expected_head,
            } => {
                let expected_head = integration_head(api, &run_id, expected_head).await?;
                api.post(
                    &format!("/api/v1/runs/{run_id}/approve-integration"),
                    json!({"expected_head_sha": expected_head, "note": "approved with harnessctl"}),
                )
                .await
            }
            RunCommand::PublishDraftPr {
                run_id,
                expected_head,
                title,
                body_appendix,
            } => {
                let expected_head = integration_head(api, &run_id, expected_head).await?;
                api.post(
                    &format!("/api/v1/runs/{run_id}/publish-draft-pr"),
                    json!({"expected_head_sha": expected_head, "title": title, "body_appendix": body_appendix}),
                )
                .await
            }
            RunCommand::Pause { run_id } => {
                api.post(
                    &format!("/api/v1/runs/{run_id}/scheduler/pause"),
                    json!({}),
                )
                .await
            }
            RunCommand::Resume { run_id } => {
                api.post(
                    &format!("/api/v1/runs/{run_id}/scheduler/resume"),
                    json!({}),
                )
                .await
            }
            RunCommand::Stop { run_id, interrupt } => {
                api.post(
                    &format!("/api/v1/runs/{run_id}/stop"),
                    json!({"mode": if interrupt { "interrupt_turns" } else { "after_current_commands" }, "preserve_all_worktrees": true}),
                )
                .await
            }
            RunCommand::Export { run_id } => {
                api.post(
                    &format!("/api/v1/runs/{run_id}/evidence/export"),
                    json!({}),
                )
                .await
            }
            RunCommand::Usage { run_id } => {
                api.get(&format!("/api/v1/runs/{run_id}/usage")).await
            }
            RunCommand::Evidence { run_id } => {
                api.get(&format!("/api/v1/runs/{run_id}/evidence"))
                    .await
            }
            RunCommand::RetryTask {
                task_id,
                reason,
                revised_objective,
                model_route,
                additional_token_budget,
            } => {
                api.post(
                    &format!("/api/v1/tasks/{task_id}/retry"),
                    json!({
                        "reason": reason,
                        "revised_objective": revised_objective,
                        "model_route": model_route,
                        "additional_token_budget": additional_token_budget,
                    }),
                )
                .await
            }
            RunCommand::RequestReview { task_id } => {
                api.post(
                    &format!("/api/v1/tasks/{task_id}/request-review"),
                    json!({}),
                )
                .await
            }
        },
        Command::Approvals { command } => match command {
            ApprovalCommand::List { run } => {
                let path = run.map_or_else(
                    || "/api/v1/approvals".to_owned(),
                    |run| format!("/api/v1/approvals?run_id={run}"),
                );
                api.get(&path).await
            }
            ApprovalCommand::Decide {
                approval_id,
                decision,
                note,
                expected_version,
            } => {
                api.post(
                    &format!("/api/v1/approvals/{approval_id}/decision"),
                    json!({"decision": decision, "note": note, "expected_version": expected_version}),
                )
                .await
            }
        },
        Command::Worktree { command } => match command {
            WorktreeCommand::List { run } => {
                let path = run.map_or_else(
                    || "/api/v1/worktrees".to_owned(),
                    |run| format!("/api/v1/worktrees?run_id={run}"),
                );
                api.get(&path).await
            }
            WorktreeCommand::Preserve {
                worktree_id,
                reason,
            } => {
                api.post(
                    &format!("/api/v1/worktrees/{worktree_id}/preserve"),
                    json!({"reason": reason}),
                )
                .await
            }
        },
        Command::Agent { command } => match command {
            AgentCommand::Show { agent_id } => {
                api.get(&format!("/api/v1/agents/{agent_id}")).await
            }
            AgentCommand::Steer { agent_id, message } => {
                api.post(
                    &format!("/api/v1/agents/{agent_id}/steer"),
                    json!({"message": message}),
                )
                .await
            }
            AgentCommand::Interrupt { agent_id } => {
                api.post(
                    &format!("/api/v1/agents/{agent_id}/interrupt"),
                    json!({}),
                )
                .await
            }
        },
    }
}

fn validate_local_url(url: &Url) -> Result<()> {
    if url.scheme() != "http" {
        bail!("harnessctl requires a local http:// endpoint");
    }
    let host = url.host_str().context("HARNESS_URL has no host")?;
    let local = host == "localhost"
        || host
            .parse::<IpAddr>()
            .is_ok_and(|address| address.is_loopback());
    if !local {
        bail!("harnessctl refuses non-loopback endpoint {host}");
    }
    Ok(())
}

async fn integration_head(
    api: &ApiClient,
    run_id: &str,
    supplied: Option<String>,
) -> Result<String> {
    match supplied {
        Some(head) => Ok(head),
        None => api
            .get(&format!("/api/v1/runs/{run_id}"))
            .await?
            .pointer("/run/integration_sha")
            .and_then(Value::as_str)
            .context("run has no prepared integration head")
            .map(ToOwned::to_owned),
    }
}

use std::{fs, path::PathBuf};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand};
use harness_context::{Probe, ProbeOutput};
use serde_json::{Value, json};

#[derive(Parser)]
#[command(
    name = "harness-probe",
    version,
    about = "Bounded, read-only repository probes for BILDR roles"
)]
struct Cli {
    /// Repository root. Defaults to the current directory.
    #[arg(long, global = true, default_value = ".")]
    repository: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Search tracked and untracked source text through ripgrep.
    Search {
        #[arg(long)]
        query: String,
        #[arg(long = "paths")]
        globs: Vec<String>,
        #[arg(long, default_value_t = 200)]
        max_results: usize,
        #[arg(long, default_value_t = 262_144)]
        max_total_bytes: usize,
    },
    /// Read several safe relative paths in one bounded response.
    ReadMany {
        /// JSON file containing an array of paths or {"paths": [...]}.
        #[arg(long)]
        manifest: PathBuf,
        #[arg(long, default_value_t = 65_536)]
        max_per_file_bytes: usize,
        #[arg(long, default_value_t = 262_144)]
        max_total_bytes: usize,
    },
    /// Return Cargo workspace metadata.
    CargoMap {
        /// Optional affected paths, echoed as selection hints.
        #[arg(long = "affected")]
        affected: Vec<String>,
        #[arg(long, default_value_t = 524_288)]
        max_total_bytes: usize,
    },
    /// Locate likely tests for a term or task packet.
    TestSelect {
        #[arg(long, conflicts_with = "task_packet")]
        term: Option<String>,
        #[arg(long, conflicts_with = "term")]
        task_packet: Option<PathBuf>,
        #[arg(long, default_value_t = 262_144)]
        max_total_bytes: usize,
    },
    /// Extract failures, warnings, and a bounded log tail.
    SummarizeLog {
        #[arg(long, alias = "artifact")]
        path: PathBuf,
        #[arg(long)]
        focus: Option<String>,
        #[arg(long, default_value_t = 262_144)]
        max_total_bytes: usize,
    },
    /// Render an existing JSON context/task packet in normalized form.
    ContextShow {
        #[arg(long, alias = "task")]
        packet: PathBuf,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let repository = cli
        .repository
        .canonicalize()
        .with_context(|| format!("repository {} is not accessible", cli.repository.display()))?;
    let envelope = match cli.command {
        Command::Search {
            query,
            globs,
            max_results,
            max_total_bytes,
        } => result_envelope(
            "search",
            Probe::search(
                &repository,
                &query,
                &globs,
                max_results,
                Some(max_total_bytes),
            )?,
            json!({"query": query, "globs": globs, "max_results": max_results}),
        ),
        Command::ReadMany {
            manifest,
            max_per_file_bytes,
            max_total_bytes,
        } => {
            let value: Value = serde_json::from_slice(
                &fs::read(&manifest)
                    .with_context(|| format!("failed to read {}", manifest.display()))?,
            )?;
            let values = value
                .as_array()
                .or_else(|| value.get("paths").and_then(Value::as_array))
                .context("manifest must be a JSON array or an object with a paths array")?;
            let paths = values
                .iter()
                .map(|value| {
                    value
                        .as_str()
                        .map(ToOwned::to_owned)
                        .context("every manifest path must be a string")
                })
                .collect::<Result<Vec<_>>>()?;
            result_envelope(
                "read-many",
                Probe::read_many(
                    &repository,
                    &paths,
                    max_per_file_bytes,
                    Some(max_total_bytes),
                )?,
                json!({"paths": paths, "max_per_file_bytes": max_per_file_bytes}),
            )
        }
        Command::CargoMap {
            affected,
            max_total_bytes,
        } => result_envelope(
            "cargo-map",
            Probe::cargo_map(&repository, Some(max_total_bytes))?,
            json!({"affected": affected}),
        ),
        Command::TestSelect {
            term,
            task_packet,
            max_total_bytes,
        } => {
            let term = match (term, task_packet) {
                (Some(term), None) if !term.trim().is_empty() => term,
                (None, Some(path)) => {
                    let value: Value = serde_json::from_slice(
                        &fs::read(&path)
                            .with_context(|| format!("failed to read {}", path.display()))?,
                    )?;
                    value
                        .get("task_id")
                        .or_else(|| value.get("title"))
                        .and_then(Value::as_str)
                        .context("task packet has no task_id or title")?
                        .to_owned()
                }
                _ => bail!("provide either --term or --task-packet"),
            };
            result_envelope(
                "test-select",
                Probe::test_select(&repository, &term, Some(max_total_bytes))?,
                json!({"term": term}),
            )
        }
        Command::SummarizeLog {
            path,
            focus,
            max_total_bytes,
        } => result_envelope(
            "summarize-log",
            Probe::summarize_log(&path, Some(max_total_bytes))?,
            json!({"path": path, "focus": focus}),
        ),
        Command::ContextShow { packet } => {
            let value: Value = serde_json::from_slice(
                &fs::read(&packet)
                    .with_context(|| format!("failed to read {}", packet.display()))?,
            )?;
            json!({
                "schema": "harness-probe/v1",
                "operation": "context-show",
                "repository": repository,
                "packet": value,
            })
        }
    };
    println!("{}", serde_json::to_string_pretty(&envelope)?);
    Ok(())
}

fn result_envelope(operation: &str, result: ProbeOutput, request: Value) -> Value {
    json!({
        "schema": "harness-probe/v1",
        "operation": operation,
        "request": request,
        "output": result.output,
        "total_bytes": result.total_bytes,
        "sha256": result.sha256,
        "truncated": result.truncated,
    })
}

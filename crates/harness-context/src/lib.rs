//! Authority-first repository context compiler and bounded probe helpers.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs,
    io::{Read, Seek},
    path::{Component, Path},
    process::{Command, Stdio},
};

use globset::{Glob, GlobSet, GlobSetBuilder};
use harness_domain::TaskPacket;
use harness_profile::RepositoryProfile;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

const MAX_SOURCE_BYTES: u64 = 512 * 1024;
const MAX_INLINE_SOURCE_BYTES: usize = 64 * 1024;
const MAX_CONTEXT_BYTES: usize = 2 * 1024 * 1024;
const DEFAULT_PROBE_BYTES: usize = 256 * 1024;
const MAX_PROBE_BYTES: usize = 4 * 1024 * 1024;
const MAX_READ_MANY_PATHS: usize = 64;
const MAX_READ_MANY_FILE_BYTES: usize = 128 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContextSource {
    pub path: String,
    pub kind: String,
    pub sha256: Option<String>,
    pub bytes: u64,
    pub included: bool,
    #[serde(default)]
    pub receipt_only: bool,
    pub reason: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RepositoryMap {
    pub tracked_files: u64,
    pub text_files: u64,
    pub binary_files: u64,
    pub oversized_files: u64,
    pub excluded_files: u64,
    pub bytes: u64,
    pub top_level: BTreeMap<String, u64>,
    pub extensions: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ContextPacket {
    pub schema: String,
    pub base_sha: String,
    pub task_id: String,
    pub profile_id: String,
    pub profile_digest: String,
    pub instruction_digest: String,
    pub sources: Vec<ContextSource>,
    pub repository_map: RepositoryMap,
    pub protected_semantics: Vec<String>,
    pub context_bytes: u64,
    pub estimated_tokens: u64,
    pub digest: String,
}

impl ContextPacket {
    #[must_use]
    pub fn prompt_prefix(&self) -> String {
        let mut output = "Controller-protected semantics:\n".to_owned();
        for rule in &self.protected_semantics {
            output.push_str("- ");
            output.push_str(rule);
            output.push('\n');
        }
        output.push_str("\n<repository_evidence>\n");
        for source in self
            .sources
            .iter()
            .filter(|source| source.included && !source.receipt_only)
        {
            output.push_str("\n<source>\nPath: ");
            output.push_str(&source.path);
            output.push_str("\nKind: ");
            output.push_str(&source.kind);
            output.push_str("\nSHA-256: ");
            output.push_str(source.sha256.as_deref().unwrap_or("unavailable"));
            output.push_str("\nContent:\n");
            if let Some(content) = &source.content {
                output.push_str(content);
                if !content.ends_with('\n') {
                    output.push('\n');
                }
            }
            output.push_str("</source>\n");
        }
        output.push_str("</repository_evidence>\n");
        for source in self
            .sources
            .iter()
            .filter(|source| source.included && source.receipt_only)
        {
            let mandatory_authority = matches!(
                source.kind.as_str(),
                "instruction" | "global_authority" | "task_authority" | "domain_authority"
            );
            output.push_str(if mandatory_authority {
                "\n<mandatory_authority_receipt>\nPath: "
            } else {
                "\n<source_receipt>\nPath: "
            });
            output.push_str(&source.path);
            output.push_str("\nSHA-256: ");
            output.push_str(source.sha256.as_deref().unwrap_or("unavailable"));
            output.push_str("\nBytes: ");
            output.push_str(&source.bytes.to_string());
            if mandatory_authority {
                output.push_str("\nThis mandatory authority is exact-head-bound and available in the current leased worktree. Before a decision governed by it, use targeted rg and bounded line reads.\n</mandatory_authority_receipt>\n");
            } else {
                output.push_str("\nThis exact-head-bound source is available in the current leased worktree. Use targeted rg and bounded line reads before changing behavior it governs.\n</source_receipt>\n");
            }
        }
        output.push_str("\nContext receipt:\nPacket: ");
        output.push_str(&self.digest);
        output.push_str("\nBase SHA: ");
        output.push_str(&self.base_sha);
        output.push_str("\nTask: ");
        output.push_str(&self.task_id);
        output.push('\n');
        output
    }
}

pub struct ContextCompiler {
    max_source_bytes: u64,
    max_context_bytes: usize,
}

impl Default for ContextCompiler {
    fn default() -> Self {
        Self {
            max_source_bytes: MAX_SOURCE_BYTES,
            max_context_bytes: MAX_CONTEXT_BYTES,
        }
    }
}

impl ContextCompiler {
    #[must_use]
    pub fn with_limits(max_source_bytes: u64, max_context_bytes: usize) -> Self {
        Self {
            max_source_bytes,
            max_context_bytes,
        }
    }

    pub fn compile(
        &self,
        repository: &Path,
        expected_sha: &str,
        task: &TaskPacket,
        profile: &RepositoryProfile,
        profile_digest: &str,
    ) -> Result<ContextPacket, ContextError> {
        let repository = repository.canonicalize()?;
        let actual_sha = git_text(&repository, ["rev-parse", "HEAD"])?;
        if actual_sha != expected_sha {
            return Err(ContextError::WrongHead {
                expected: expected_sha.to_owned(),
                actual: actual_sha,
            });
        }
        let tracked = git_files(&repository)?;
        let tracked_set: BTreeSet<&str> = tracked.iter().map(String::as_str).collect();
        let repository_map = build_repository_map(&repository, &tracked, self.max_source_bytes)?;
        let selected = select_sources(task, profile)?;
        let mut sources = Vec::new();
        let mut included_bytes = 0_usize;
        let mut instruction_hasher = Sha256::new();

        for (path, kind, explicitly_promoted) in selected {
            let normalized = normalize_relative(&path)?;
            let profile_receipt_only = profile
                .receipt_only_authorities
                .iter()
                .any(|authority| authority == &normalized);
            if !tracked_set.contains(normalized.as_str()) {
                sources.push(excluded_source(
                    normalized,
                    kind,
                    "not tracked at pinned SHA",
                ));
                continue;
            }
            if is_secret_path(&normalized) {
                sources.push(excluded_source(normalized, kind, "secret-like path"));
                continue;
            }
            if is_archive_path(&normalized) && !explicitly_promoted {
                sources.push(excluded_source(
                    normalized,
                    kind,
                    "archived authority not promoted",
                ));
                continue;
            }
            if is_bulk_excluded(&normalized) && !explicitly_promoted {
                sources.push(excluded_source(
                    normalized,
                    kind,
                    "vendor/generated/build output",
                ));
                continue;
            }
            let absolute = repository.join(&normalized);
            let metadata = fs::symlink_metadata(&absolute)?;
            if metadata.file_type().is_symlink() {
                sources.push(excluded_source(normalized, kind, "symlink source rejected"));
                continue;
            }
            if metadata.len() > self.max_source_bytes {
                sources.push(ContextSource {
                    path: normalized,
                    kind,
                    sha256: None,
                    bytes: metadata.len(),
                    included: false,
                    receipt_only: false,
                    reason: "source exceeds per-file context limit".to_owned(),
                    content: None,
                });
                continue;
            }
            let bytes = fs::read(&absolute)?;
            if looks_binary(&bytes) {
                sources.push(ContextSource {
                    path: normalized,
                    kind,
                    sha256: Some(digest(&bytes)),
                    bytes: bytes.len() as u64,
                    included: false,
                    receipt_only: false,
                    reason: "binary source".to_owned(),
                    content: None,
                });
                continue;
            }
            let sha256 = digest(&bytes);
            if kind == "instruction" {
                instruction_hasher.update(normalized.as_bytes());
                instruction_hasher.update([0]);
                instruction_hasher.update(sha256.as_bytes());
                instruction_hasher.update([0]);
            }
            let receipt_only = profile_receipt_only || bytes.len() > MAX_INLINE_SOURCE_BYTES;
            if receipt_only {
                sources.push(ContextSource {
                    path: normalized,
                    kind,
                    sha256: Some(sha256),
                    bytes: bytes.len() as u64,
                    included: true,
                    receipt_only: true,
                    reason: if profile_receipt_only {
                        "selected by repository profile; body omitted by receipt-only policy".to_owned()
                    } else {
                        "source exceeds inline prompt threshold; body omitted by receipt-only policy"
                            .to_owned()
                    },
                    content: None,
                });
                continue;
            }
            if included_bytes.saturating_add(bytes.len()) > self.max_context_bytes {
                sources.push(ContextSource {
                    path: normalized,
                    kind,
                    sha256: Some(sha256),
                    bytes: bytes.len() as u64,
                    included: false,
                    receipt_only: false,
                    reason: "context packet byte budget exhausted".to_owned(),
                    content: None,
                });
                continue;
            }
            included_bytes += bytes.len();
            sources.push(ContextSource {
                path: normalized,
                kind,
                sha256: Some(sha256),
                bytes: bytes.len() as u64,
                included: true,
                receipt_only: false,
                reason: if explicitly_promoted {
                    "explicit task authority".to_owned()
                } else {
                    "selected by repository profile".to_owned()
                },
                content: Some(String::from_utf8_lossy(&bytes).into_owned()),
            });
        }

        let mut packet = ContextPacket {
            schema: "harness-context/v1".to_owned(),
            base_sha: expected_sha.to_owned(),
            task_id: task.task_id.clone(),
            profile_id: profile.profile_id.clone(),
            profile_digest: profile_digest.to_owned(),
            instruction_digest: hex::encode(instruction_hasher.finalize()),
            sources,
            repository_map,
            protected_semantics: profile.protected_semantics.clone(),
            context_bytes: included_bytes as u64,
            estimated_tokens: (included_bytes as u64).div_ceil(4),
            digest: String::new(),
        };
        packet.digest = digest(&serde_json::to_vec(&packet)?);
        Ok(packet)
    }
}

fn select_sources(
    task: &TaskPacket,
    profile: &RepositoryProfile,
) -> Result<Vec<(String, String, bool)>, ContextError> {
    let mut selected = Vec::new();
    let mut seen = BTreeSet::new();
    let push = |path: &str,
                kind: &str,
                promoted: bool,
                selected: &mut Vec<(String, String, bool)>,
                seen: &mut BTreeSet<String>| {
        if seen.insert(path.to_owned()) {
            selected.push((path.to_owned(), kind.to_owned(), promoted));
        }
    };
    for path in &profile.instruction_sources {
        push(path, "instruction", false, &mut selected, &mut seen);
    }
    for path in &profile.required_global_authorities {
        push(path, "global_authority", false, &mut selected, &mut seen);
    }
    for authority in &task.authority_refs {
        push(authority, "task_authority", true, &mut selected, &mut seen);
    }
    for domain in &profile.domains {
        let matcher = compile_globs(&domain.globs)?;
        if task.owned_paths.iter().any(|path| matcher.is_match(path)) {
            for authority in &domain.authority_hints {
                push(
                    authority,
                    "domain_authority",
                    false,
                    &mut selected,
                    &mut seen,
                );
            }
        }
    }
    for path in &task.owned_paths {
        if !contains_glob(path) {
            push(path, "task_file", true, &mut selected, &mut seen);
        }
    }
    Ok(selected)
}

fn build_repository_map(
    repository: &Path,
    tracked: &[String],
    max_source_bytes: u64,
) -> Result<RepositoryMap, ContextError> {
    let mut map = RepositoryMap::default();
    for path in tracked {
        map.tracked_files += 1;
        let first = path.split('/').next().unwrap_or(".").to_owned();
        *map.top_level.entry(first).or_default() += 1;
        let extension = Path::new(path)
            .extension()
            .and_then(|value| value.to_str())
            .unwrap_or("<none>")
            .to_ascii_lowercase();
        *map.extensions.entry(extension).or_default() += 1;
        if is_bulk_excluded(path) || is_secret_path(path) {
            map.excluded_files += 1;
            continue;
        }
        let metadata = match fs::symlink_metadata(repository.join(path)) {
            Ok(metadata) => metadata,
            Err(_) => {
                map.excluded_files += 1;
                continue;
            }
        };
        map.bytes = map.bytes.saturating_add(metadata.len());
        if metadata.len() > max_source_bytes {
            map.oversized_files += 1;
            continue;
        }
        let bytes = fs::read(repository.join(path))?;
        if looks_binary(&bytes) {
            map.binary_files += 1;
        } else {
            map.text_files += 1;
        }
    }
    Ok(map)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProbeOutput {
    pub output: String,
    pub total_bytes: u64,
    pub sha256: String,
    pub truncated: bool,
}

pub struct Probe;

impl Probe {
    pub fn search(
        repository: &Path,
        query: &str,
        globs: &[String],
        max_matches: usize,
        max_bytes: Option<usize>,
    ) -> Result<ProbeOutput, ContextError> {
        if query.is_empty() {
            return Err(ContextError::Invalid(
                "search query must not be empty".to_owned(),
            ));
        }
        let mut command = Command::new("rg");
        command.current_dir(repository).args([
            "--line-number",
            "--column",
            "--no-heading",
            "--color",
            "never",
        ]);
        for glob in globs {
            command.args(["--glob", glob]);
        }
        command.arg("--").arg(query).arg(".");
        bounded_command(
            command,
            max_bytes.unwrap_or(DEFAULT_PROBE_BYTES),
            Some(max_matches.clamp(1, 10_000)),
            true,
        )
    }

    pub fn read_many(
        repository: &Path,
        paths: &[String],
        max_per_file: usize,
        max_bytes: Option<usize>,
    ) -> Result<ProbeOutput, ContextError> {
        let repository = repository.canonicalize()?;
        let mut output = Vec::new();
        let max_per_file = max_per_file.clamp(1, MAX_READ_MANY_FILE_BYTES);
        let mut request_truncated = paths.len() > MAX_READ_MANY_PATHS;
        for path in paths.iter().take(MAX_READ_MANY_PATHS) {
            let normalized = normalize_relative(path)?;
            if is_secret_path(&normalized) || is_bulk_excluded(&normalized) {
                continue;
            }
            let absolute = repository.join(&normalized);
            let metadata = match fs::symlink_metadata(&absolute) {
                Ok(metadata) if metadata.is_file() && !metadata.file_type().is_symlink() => {
                    metadata
                }
                _ => continue,
            };
            let canonical = absolute.canonicalize()?;
            if !canonical.starts_with(&repository) {
                continue;
            }
            let mut bytes = Vec::with_capacity(
                max_per_file.min(usize::try_from(metadata.len()).unwrap_or(usize::MAX)),
            );
            fs::File::open(canonical)?
                .take(max_per_file as u64)
                .read_to_end(&mut bytes)?;
            if looks_binary(&bytes) {
                continue;
            }
            output.extend_from_slice(format!("--- {normalized} ---\n").as_bytes());
            output.extend_from_slice(&bytes);
            if metadata.len() > bytes.len() as u64 {
                request_truncated = true;
                output.extend_from_slice(b"\n[per-file limit reached]");
            }
            output.push(b'\n');
        }
        let mut result = bound_output(output, max_bytes.unwrap_or(DEFAULT_PROBE_BYTES), None);
        result.truncated |= request_truncated;
        Ok(result)
    }

    pub fn cargo_map(
        repository: &Path,
        max_bytes: Option<usize>,
    ) -> Result<ProbeOutput, ContextError> {
        let mut command = Command::new("cargo");
        command
            .current_dir(repository)
            .args(["metadata", "--format-version", "1", "--no-deps"]);
        bounded_command(
            command,
            max_bytes.unwrap_or(DEFAULT_PROBE_BYTES),
            None,
            false,
        )
    }

    pub fn test_select(
        repository: &Path,
        term: &str,
        max_bytes: Option<usize>,
    ) -> Result<ProbeOutput, ContextError> {
        Self::search(
            repository,
            &format!(
                "(test|spec|scenario).{{0,80}}{}|{}.{{0,80}}(test|spec|scenario)",
                escape_regex(term),
                escape_regex(term)
            ),
            &["!target/**".to_owned(), "!node_modules/**".to_owned()],
            200,
            max_bytes,
        )
    }

    pub fn summarize_log(
        path: &Path,
        max_bytes: Option<usize>,
    ) -> Result<ProbeOutput, ContextError> {
        let bytes = fs::read(path)?;
        let text = String::from_utf8_lossy(&bytes);
        let lines: Vec<&str> = text.lines().collect();
        let mut selected = Vec::new();
        for (index, line) in lines.iter().enumerate() {
            let lower = line.to_ascii_lowercase();
            if ["error", "failed", "panic", "warning", "timeout", "denied"]
                .iter()
                .any(|needle| lower.contains(needle))
            {
                selected.push(format!("{}: {}", index + 1, line));
            }
        }
        selected.extend(
            lines
                .iter()
                .enumerate()
                .skip(lines.len().saturating_sub(80))
                .map(|(index, line)| format!("{}: {}", index + 1, line)),
        );
        selected.sort();
        selected.dedup();
        Ok(bound_output(
            selected.join("\n").into_bytes(),
            max_bytes.unwrap_or(DEFAULT_PROBE_BYTES),
            None,
        ))
    }
}

fn bounded_command(
    mut command: Command,
    max_bytes: usize,
    max_lines: Option<usize>,
    allow_exit_one: bool,
) -> Result<ProbeOutput, ContextError> {
    let stdout = tempfile::tempfile()?;
    let stderr = tempfile::tempfile()?;
    let mut stdout_reader = stdout.try_clone()?;
    let mut stderr_reader = stderr.try_clone()?;
    command
        .stdin(Stdio::null())
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr));
    let status = command.status()?;
    if !status.success() && !(allow_exit_one && status.code() == Some(1)) {
        stderr_reader.rewind()?;
        let mut diagnostic = Vec::new();
        stderr_reader
            .take(DEFAULT_PROBE_BYTES as u64)
            .read_to_end(&mut diagnostic)?;
        return Err(ContextError::Command(
            String::from_utf8_lossy(&diagnostic).trim().to_owned(),
        ));
    }
    stdout_reader.rewind()?;
    bound_reader(
        &mut stdout_reader,
        max_bytes.clamp(1, MAX_PROBE_BYTES),
        max_lines,
    )
}

fn bound_reader(
    reader: &mut impl Read,
    max_bytes: usize,
    max_lines: Option<usize>,
) -> Result<ProbeOutput, ContextError> {
    let mut digest = Sha256::new();
    let mut total_bytes = 0_u64;
    let mut retained = Vec::with_capacity(max_bytes);
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = reader.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
        total_bytes = total_bytes.saturating_add(count as u64);
        let available = max_bytes.saturating_sub(retained.len());
        retained.extend_from_slice(&buffer[..count.min(available)]);
    }
    let mut output = String::from_utf8_lossy(&retained).into_owned();
    let mut truncated = total_bytes > retained.len() as u64;
    if let Some(max_lines) = max_lines {
        let all_lines = output.lines().count();
        output = output
            .lines()
            .take(max_lines)
            .collect::<Vec<_>>()
            .join("\n");
        truncated |= all_lines > max_lines;
    }
    Ok(ProbeOutput {
        output,
        total_bytes,
        sha256: hex::encode(digest.finalize()),
        truncated,
    })
}

fn bound_output(bytes: Vec<u8>, max_bytes: usize, max_lines: Option<usize>) -> ProbeOutput {
    let max_bytes = max_bytes.clamp(1, MAX_PROBE_BYTES);
    let total_bytes = bytes.len() as u64;
    let sha256 = digest(&bytes);
    let byte_end = bytes.len().min(max_bytes);
    let mut output = String::from_utf8_lossy(&bytes[..byte_end]).into_owned();
    let mut truncated = byte_end < bytes.len();
    if let Some(max_lines) = max_lines {
        let lines: Vec<&str> = output.lines().take(max_lines).collect();
        if lines.len() < output.lines().count() {
            truncated = true;
        }
        output = lines.join("\n");
    }
    ProbeOutput {
        output,
        total_bytes,
        sha256,
        truncated,
    }
}

fn escape_regex(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        if matches!(
            character,
            '.' | '+' | '*' | '?' | '(' | ')' | '[' | ']' | '{' | '}' | '^' | '$' | '|' | '\\'
        ) {
            escaped.push('\\');
        }
        escaped.push(character);
    }
    escaped
}

fn git_files(repository: &Path) -> Result<Vec<String>, ContextError> {
    let output = Command::new("git")
        .current_dir(repository)
        .args(["ls-files", "-z"])
        .output()?;
    if !output.status.success() {
        return Err(ContextError::Command(
            String::from_utf8_lossy(&output.stderr).into_owned(),
        ));
    }
    let mut files: Vec<String> = output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
        .map(|path| String::from_utf8_lossy(path).into_owned())
        .collect();
    files.sort();
    Ok(files)
}

fn git_text<'a>(
    repository: &Path,
    args: impl IntoIterator<Item = &'a str>,
) -> Result<String, ContextError> {
    let output = Command::new("git")
        .current_dir(repository)
        .args(args)
        .output()?;
    if !output.status.success() {
        return Err(ContextError::Command(
            String::from_utf8_lossy(&output.stderr).into_owned(),
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn compile_globs(patterns: &[String]) -> Result<GlobSet, ContextError> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        builder.add(Glob::new(pattern)?);
    }
    Ok(builder.build()?)
}

fn normalize_relative(path: &str) -> Result<String, ContextError> {
    let path = Path::new(path);
    if path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(ContextError::Invalid(format!(
            "unsafe repository path: {}",
            path.display()
        )));
    }
    let normalized = path
        .components()
        .filter_map(|component| match component {
            Component::Normal(value) => Some(value.to_string_lossy()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/");
    if normalized.is_empty() {
        return Err(ContextError::Invalid("empty repository path".to_owned()));
    }
    Ok(normalized)
}

fn excluded_source(path: String, kind: String, reason: &str) -> ContextSource {
    ContextSource {
        path,
        kind,
        sha256: None,
        bytes: 0,
        included: false,
        receipt_only: false,
        reason: reason.to_owned(),
        content: None,
    }
}

fn contains_glob(path: &str) -> bool {
    path.bytes()
        .any(|byte| matches!(byte, b'*' | b'?' | b'[' | b']' | b'{' | b'}'))
}

fn is_archive_path(path: &str) -> bool {
    path.split('/').any(|part| {
        matches!(
            part.to_ascii_lowercase().as_str(),
            "archive" | "archives" | "archived" | "attic"
        )
    })
}

fn is_bulk_excluded(path: &str) -> bool {
    path.split('/').any(|part| {
        matches!(
            part.to_ascii_lowercase().as_str(),
            ".git" | "target" | "node_modules" | "vendor" | ".cache" | "dist"
        )
    }) || path.contains("/generated/")
        || path.starts_with("generated/")
}

fn is_secret_path(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    lower == ".env"
        || lower.contains("/.env")
        || lower.ends_with(".pem")
        || lower.ends_with(".key")
        || lower.ends_with(".p12")
        || lower.ends_with("credentials.json")
        || lower.contains("/secrets/")
}

fn looks_binary(bytes: &[u8]) -> bool {
    bytes.iter().take(8 * 1024).any(|byte| *byte == 0)
}

fn digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

#[derive(Debug, Error)]
pub enum ContextError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("glob error: {0}")]
    Glob(#[from] globset::Error),
    #[error("repository is at {actual}, expected pinned SHA {expected}")]
    WrongHead { expected: String, actual: String },
    #[error("context command failed: {0}")]
    Command(String),
    #[error("invalid context request: {0}")]
    Invalid(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task_with_file(path: &str) -> TaskPacket {
        TaskPacket {
            schema: "harness.orchestration.task.v1".to_owned(),
            program_id: "test".to_owned(),
            task_id: "task".to_owned(),
            title: "test".to_owned(),
            state: "ready".to_owned(),
            priority: "P1".to_owned(),
            execution_mode: "controller".to_owned(),
            execution_kind: harness_domain::TaskExecutionKind::Implementation,
            owner_profile: "worker".to_owned(),
            reviewer_profile: "verifier".to_owned(),
            checklist_rows: vec![],
            authority_refs: vec![],
            base_sha: String::new(),
            dependency_shas: BTreeMap::new(),
            depends_on: vec![],
            owned_paths: vec![path.to_owned()],
            forbidden_paths: vec![],
            reserved_serial_paths: vec![],
            objective: "test context".to_owned(),
            milestones: vec![],
            non_goals: vec![],
            success_criteria: vec![],
            required_positive_tests: vec![],
            required_negative_tests: vec![],
            required_metrics: vec![],
            required_evidence: vec![],
            proof_limits: vec![],
            diff_budget: harness_domain::DiffBudget { files: 0, lines: 0 },
            token_budget: 1,
            tool_budget: None,
            lease_expires_at: "test".to_owned(),
            stop_conditions: vec![],
            handoff_path: "controller://test".to_owned(),
            risk_flags: vec![],
        }
    }

    fn git(repository: &Path, args: &[&str]) {
        let output = Command::new("git")
            .current_dir(repository)
            .args(args)
            .output()
            .expect("git starts");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn archive_is_not_implicitly_active() {
        assert!(is_archive_path("docs/archive/old-contract.md"));
        assert!(!is_archive_path("docs/architecture/current.md"));
    }

    #[test]
    fn secret_and_bulk_paths_are_rejected() {
        assert!(is_secret_path("config/.env.production"));
        assert!(is_secret_path("certs/server.key"));
        assert!(is_bulk_excluded("dashboard/node_modules/pkg/index.js"));
        assert!(is_bulk_excluded("shared/generated/api.rs"));
    }

    #[test]
    fn bounded_output_retains_full_digest() {
        let data = b"one\ntwo\nthree\n".to_vec();
        let output = bound_output(data.clone(), 7, None);
        assert!(output.truncated);
        assert_eq!(output.total_bytes, data.len() as u64);
        assert_eq!(output.sha256, digest(&data));
    }

    #[test]
    fn relative_path_normalization_is_fail_closed() {
        assert!(normalize_relative("../secret").is_err());
        assert!(normalize_relative("/tmp/file").is_err());
        assert_eq!(
            normalize_relative("docs/./README.md").unwrap(),
            "docs/README.md"
        );
    }

    #[test]
    fn prompt_prefix_keeps_reusable_evidence_before_volatile_receipt() {
        let packet = ContextPacket {
            schema: "harness-context/v1".to_owned(),
            base_sha: "base-sha".to_owned(),
            task_id: "task-7".to_owned(),
            profile_id: "general".to_owned(),
            profile_digest: "profile-digest".to_owned(),
            instruction_digest: "instruction-digest".to_owned(),
            sources: vec![ContextSource {
                path: "CONTRIBUTING.md".to_owned(),
                kind: "instruction".to_owned(),
                sha256: Some("source-digest".to_owned()),
                bytes: 18,
                included: true,
                receipt_only: false,
                reason: "selected".to_owned(),
                content: Some("stable source text".to_owned()),
            }],
            repository_map: RepositoryMap::default(),
            protected_semantics: vec!["Preserve user changes".to_owned()],
            context_bytes: 18,
            estimated_tokens: 5,
            digest: "packet-digest".to_owned(),
        };

        let prompt = packet.prompt_prefix();
        let source = prompt.find("stable source text").unwrap();
        let receipt = prompt.find("Context receipt:").unwrap();
        let volatile_digest = prompt.find("packet-digest").unwrap();

        assert!(source < receipt);
        assert!(receipt < volatile_digest);
        assert!(prompt.contains("<repository_evidence>"));
        assert!(prompt.contains("Controller-protected semantics:"));
    }

    #[test]
    fn receipt_only_authority_keeps_its_digest_without_inlining_the_body() {
        let packet = ContextPacket {
            schema: "harness-context/v1".to_owned(),
            base_sha: "base-sha".to_owned(),
            task_id: "ARCHITECTURE".to_owned(),
            profile_id: "bildr".to_owned(),
            profile_digest: "profile-digest".to_owned(),
            instruction_digest: "instruction-digest".to_owned(),
            sources: vec![ContextSource {
                path: "ARCHITECTURE_AND_IMPLEMENTATION_PLAN.md".to_owned(),
                kind: "instruction".to_owned(),
                sha256: Some("source-digest".to_owned()),
                bytes: 109_444,
                included: true,
                receipt_only: true,
                reason: "receipt-only policy".to_owned(),
                content: Some("receipt-only body must not appear".to_owned()),
            }],
            repository_map: RepositoryMap::default(),
            protected_semantics: vec![],
            context_bytes: 0,
            estimated_tokens: 0,
            digest: "packet-digest".to_owned(),
        };

        let prompt = packet.prompt_prefix();
        assert!(prompt.contains("<mandatory_authority_receipt>"));
        assert!(prompt.contains("ARCHITECTURE_AND_IMPLEMENTATION_PLAN.md"));
        assert!(prompt.contains("source-digest"));
        assert!(prompt.contains("mandatory authority is exact-head-bound"));
        assert!(prompt.contains("targeted rg and bounded line reads"));
        assert!(!prompt.contains("receipt-only body must not appear"));
    }

    #[test]
    fn large_task_sources_are_receipted_while_short_task_sources_stay_inline() {
        let repository = tempfile::tempdir().expect("temporary repository");
        git(repository.path(), &["init", "-q"]);
        let large = "large-body\n".repeat((MAX_INLINE_SOURCE_BYTES / "large-body\n".len()) + 1);
        fs::write(repository.path().join("large.txt"), &large).expect("large source");
        fs::write(repository.path().join("short.txt"), "short-body\n").expect("short source");
        git(repository.path(), &["add", "."]);
        git(
            repository.path(),
            &[
                "-c",
                "user.name=test",
                "-c",
                "user.email=test@example.invalid",
                "commit",
                "-qm",
                "context fixture",
            ],
        );
        let base_sha = git_text(repository.path(), ["rev-parse", "HEAD"]).expect("base SHA");
        let profile = harness_profile::load_profile("general", repository.path())
            .expect("general profile")
            .profile;
        let compiler = ContextCompiler::default();

        let large_packet = compiler
            .compile(
                repository.path(),
                &base_sha,
                &task_with_file("large.txt"),
                &profile,
                "profile-digest",
            )
            .expect("large context compiles");
        let large_source = large_packet.sources.first().expect("large source receipt");
        assert!(large_source.included && large_source.receipt_only);
        assert!(large_source.sha256.is_some());
        assert!(large_source.content.is_none());
        assert_eq!(large_packet.context_bytes, 0);
        let large_prompt = large_packet.prompt_prefix();
        assert!(large_prompt.contains("<source_receipt>"));
        assert!(!large_prompt.contains("<mandatory_authority_receipt>"));
        assert!(!large_prompt.contains("large-body"));

        let short_packet = compiler
            .compile(
                repository.path(),
                &base_sha,
                &task_with_file("short.txt"),
                &profile,
                "profile-digest",
            )
            .expect("short context compiles");
        let short_source = short_packet.sources.first().expect("short source");
        assert!(short_source.included && !short_source.receipt_only);
        assert_eq!(short_source.content.as_deref(), Some("short-body\n"));
        assert_eq!(short_packet.context_bytes, 11);
    }
}

//! Exact-base Git coordination, worktree custody, path leases, and diff checks.

use std::{
    collections::{BTreeSet, HashMap, VecDeque},
    fs::{self, File, OpenOptions},
    io::Read,
    os::unix::{
        ffi::OsStrExt,
        fs::{OpenOptionsExt, PermissionsExt},
    },
    path::{Component, Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};

use fs2::FileExt;
use globset::{Glob, GlobSet, GlobSetBuilder};
use harness_domain::now_ms;
use harness_profile::{RepositoryProfile, redact_diagnostic};
use serde::Serialize;
use sha2::{Digest, Sha256};
use thiserror::Error;
use tokio::{io::AsyncWriteExt, process::Command, sync::Mutex, time::timeout};
use tracing::debug;
use url::Url;

#[derive(Clone)]
pub struct GitManager {
    worktree_root: Arc<PathBuf>,
    locks: Arc<Mutex<HashMap<PathBuf, Arc<Mutex<()>>>>>,
}

#[derive(Clone, Debug, Serialize)]
pub struct RepositoryInspection {
    pub root: PathBuf,
    pub origin_url: Option<String>,
    pub current_branch: Option<String>,
    pub head_sha: String,
    pub clean: bool,
    pub git_identity_name_present: bool,
    pub git_identity_email_present: bool,
    pub blockers: Vec<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct DiscoveredRepository {
    pub root_path: PathBuf,
    pub display_name: String,
    pub origin_url: Option<String>,
    pub is_github: bool,
}

#[derive(Clone, Debug)]
pub struct WorktreeSpec {
    pub repository_root: PathBuf,
    pub relative_path: PathBuf,
    pub base_sha: String,
    pub branch: Option<String>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ManagedWorktree {
    pub path: PathBuf,
    pub branch: Option<String>,
    pub base_sha: String,
    pub head_sha: String,
}

#[derive(Clone, Debug)]
pub struct DiffPolicy {
    pub owned_paths: Vec<String>,
    pub forbidden_paths: Vec<String>,
    pub serial_paths: Vec<String>,
    pub reserved_serial_paths: Vec<String>,
    pub max_files: u32,
    pub max_lines: u32,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct VerifiedDiff {
    pub head_sha: String,
    pub changed_paths: Vec<String>,
    pub additions: u64,
    pub deletions: u64,
    pub binary_files: Vec<String>,
    pub unexpected_paths: Vec<String>,
    pub forbidden_paths: Vec<String>,
    pub serial_paths: Vec<String>,
    pub diff_check: String,
    pub status_porcelain_v2: String,
    pub patch: String,
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct DiffSummary {
    pub head_sha: String,
    pub dirty: bool,
    pub changed_paths: Vec<String>,
    pub additions: u64,
    pub deletions: u64,
}

impl VerifiedDiff {
    #[must_use]
    pub fn acceptable(&self) -> bool {
        self.diff_check.trim().is_empty()
            && self.unexpected_paths.is_empty()
            && self.forbidden_paths.is_empty()
            && self.serial_paths.is_empty()
    }

    #[must_use]
    pub fn files_changed(&self) -> u32 {
        self.changed_paths.len().try_into().unwrap_or(u32::MAX)
    }
}

impl GitManager {
    pub fn new(worktree_root: &Path) -> Result<Self, GitError> {
        fs::create_dir_all(worktree_root)?;
        fs::set_permissions(worktree_root, fs::Permissions::from_mode(0o700))?;
        Ok(Self {
            worktree_root: Arc::new(worktree_root.to_path_buf()),
            locks: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    #[must_use]
    pub fn worktree_root(&self) -> &Path {
        self.worktree_root.as_ref()
    }

    pub async fn inspect(
        &self,
        repository: &Path,
        profile: &RepositoryProfile,
    ) -> Result<RepositoryInspection, GitError> {
        let root = canonical_repo_root(repository).await?;
        let current_branch = optional_git_text(&root, ["branch", "--show-current"]).await?;
        let head_sha = git_text(&root, ["rev-parse", "HEAD"]).await?;
        ensure_sha(&head_sha)?;
        let status = git_bytes(&root, ["status", "--porcelain=v2", "-z"]).await?;
        let clean = status.is_empty();
        let origin_url = optional_git_text(&root, ["remote", "get-url", "origin"])
            .await?
            .map(|origin| sanitize_remote_url(&origin));
        let identity_name = optional_git_text(&root, ["config", "user.name"]).await?;
        let identity_email = optional_git_text(&root, ["config", "user.email"]).await?;
        let mut blockers = Vec::new();
        if !clean {
            blockers.push("primary checkout is dirty".to_owned());
        }
        if profile.default_branch != "auto"
            && current_branch.as_deref() != Some(profile.default_branch.as_str())
        {
            blockers.push(format!(
                "primary checkout must be on {}, observed {}",
                profile.default_branch,
                current_branch.as_deref().unwrap_or("detached HEAD")
            ));
        }
        if identity_name.is_none() || identity_email.is_none() {
            blockers.push("Git user.name and user.email must both be configured".to_owned());
        }
        if origin_url.is_none() {
            blockers.push("origin remote is missing".to_owned());
        }
        for authority in profile
            .instruction_sources
            .iter()
            .chain(profile.required_global_authorities.iter())
        {
            if !root.join(authority).is_file() {
                blockers.push(format!("required authority is missing: {authority}"));
            }
        }
        Ok(RepositoryInspection {
            root,
            origin_url,
            current_branch,
            head_sha,
            clean,
            git_identity_name_present: identity_name.is_some(),
            git_identity_email_present: identity_email.is_some(),
            blockers,
        })
    }

    pub async fn discover_repositories(
        &self,
        search_roots: Vec<PathBuf>,
    ) -> Result<Vec<DiscoveredRepository>, GitError> {
        let roots = tokio::task::spawn_blocking(move || find_repository_roots(&search_roots))
            .await
            .map_err(|error| {
                GitError::Protocol(format!("repository discovery task failed: {error}"))
            })??;
        let mut repositories = Vec::new();
        for path in roots {
            let Ok(root) = canonical_repo_root(&path).await else {
                continue;
            };
            if root != path {
                continue;
            }
            let origin_url = optional_git_text(&root, ["remote", "get-url", "origin"])
                .await?
                .map(|origin| sanitize_remote_url(&origin));
            let display_name = root
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
                .unwrap_or_else(|| root.display().to_string());
            let is_github = origin_url
                .as_deref()
                .is_some_and(|origin| origin.to_ascii_lowercase().contains("github.com"));
            repositories.push(DiscoveredRepository {
                root_path: root,
                display_name,
                origin_url,
                is_github,
            });
        }
        repositories.sort_by(|left, right| {
            right
                .is_github
                .cmp(&left.is_github)
                .then_with(|| left.display_name.cmp(&right.display_name))
                .then_with(|| left.root_path.cmp(&right.root_path))
        });
        Ok(repositories)
    }

    /// Create a clean, branch-attached coordination clone without changing the
    /// selected source checkout. Existing source objects are used as a local
    /// reference so the onboarding clone does not duplicate the full object DB.
    pub async fn create_coordination_clone(
        &self,
        source: &Path,
        destination: &Path,
        profile: &RepositoryProfile,
    ) -> Result<RepositoryInspection, GitError> {
        let source = canonical_repo_root(source).await?;
        if !destination.is_absolute()
            || destination
                .components()
                .any(|component| matches!(component, Component::ParentDir | Component::Prefix(_)))
        {
            return Err(GitError::Policy(
                "coordination checkout destination must be an absolute normalized path".to_owned(),
            ));
        }
        if destination.starts_with(&source) {
            return Err(GitError::Policy(
                "coordination checkout cannot be created inside the source repository".to_owned(),
            ));
        }
        if fs::symlink_metadata(destination).is_ok() {
            return Err(GitError::Policy(format!(
                "coordination checkout destination already exists: {}",
                destination.display()
            )));
        }
        let parent = destination.parent().ok_or_else(|| {
            GitError::Policy("coordination checkout destination has no parent".to_owned())
        })?;
        fs::create_dir_all(parent)?;
        let parent = fs::canonicalize(parent)?;
        let destination_name = destination.file_name().ok_or_else(|| {
            GitError::Policy("coordination checkout destination has no directory name".to_owned())
        })?;
        let destination = parent.join(destination_name);
        if destination.starts_with(&source) {
            return Err(GitError::Policy(
                "coordination checkout cannot be created inside the source repository".to_owned(),
            ));
        }
        if fs::symlink_metadata(&destination).is_ok() {
            return Err(GitError::Policy(format!(
                "coordination checkout destination already exists: {}",
                destination.display()
            )));
        }
        let origin_url = optional_git_text(&source, ["remote", "get-url", "origin"])
            .await?
            .map(|origin| sanitize_remote_url(&origin))
            .ok_or_else(|| GitError::Policy("source repository has no origin remote".to_owned()))?;
        if origin_url.starts_with('-') || origin_url.contains(['\0', '\n', '\r']) {
            return Err(GitError::Policy(
                "source repository origin is unsafe for cloning".to_owned(),
            ));
        }
        let staging = parent.join(format!(
            ".harness-clone-{}-{}",
            std::process::id(),
            now_ms()
        ));
        let branch = if profile.default_branch == "auto" {
            optional_git_text(&source, ["branch", "--show-current"])
                .await?
                .ok_or_else(|| {
                    GitError::Policy(
                        "source repository must be on a named branch for coordination cloning"
                            .to_owned(),
                    )
                })?
        } else {
            profile.default_branch.clone()
        };
        let arguments = vec![
            "clone".to_owned(),
            "--single-branch".to_owned(),
            "--branch".to_owned(),
            branch.clone(),
            "--reference-if-able".to_owned(),
            source.to_string_lossy().into_owned(),
            "--".to_owned(),
            origin_url,
            staging.to_string_lossy().into_owned(),
        ];
        let clone = git_status(&parent, &arguments).await;
        match clone {
            Ok(output) if output.status.success() => {}
            Ok(output) => {
                let _ = fs::remove_dir_all(&staging);
                return Err(GitError::Command {
                    cwd: parent,
                    stderr: safe_stderr(&output.stderr),
                });
            }
            Err(error) => {
                let _ = fs::remove_dir_all(&staging);
                return Err(error);
            }
        }
        let mut effective_profile = profile.clone();
        effective_profile.default_branch = branch;
        let inspection = match self.inspect(&staging, &effective_profile).await {
            Ok(inspection) => inspection,
            Err(error) => {
                let _ = fs::remove_dir_all(&staging);
                return Err(error);
            }
        };
        if !inspection.blockers.is_empty() {
            let blockers = inspection.blockers.join("; ");
            let _ = fs::remove_dir_all(&staging);
            return Err(GitError::Policy(format!(
                "new coordination checkout failed inspection: {blockers}"
            )));
        }
        if fs::symlink_metadata(&destination).is_ok() {
            let _ = fs::remove_dir_all(&staging);
            return Err(GitError::Policy(format!(
                "coordination checkout destination appeared during clone: {}",
                destination.display()
            )));
        }
        if let Err(error) = fs::rename(&staging, &destination) {
            let _ = fs::remove_dir_all(&staging);
            return Err(error.into());
        }
        self.inspect(&destination, &effective_profile).await
    }

    pub async fn fetch_and_pin(
        &self,
        repository: &Path,
        reference: &str,
        fetch: bool,
    ) -> Result<String, GitError> {
        validate_reference(reference)?;
        let root = canonical_repo_root(repository).await?;
        let lock = self.process_lock(&root).await;
        let _guard = lock.lock().await;
        let _file_lock = RepoFileLock::acquire(&root)?;
        if fetch {
            git_ok(&root, ["fetch", "--prune", "origin"]).await?;
        }
        let expression = format!("{reference}^{{commit}}");
        let sha = git_text(&root, ["rev-parse", "--verify", expression.as_str()]).await?;
        ensure_sha(&sha)?;
        Ok(sha)
    }

    pub async fn create_worktree(&self, spec: &WorktreeSpec) -> Result<ManagedWorktree, GitError> {
        ensure_sha(&spec.base_sha)?;
        let root = canonical_repo_root(&spec.repository_root).await?;
        let lock = self.process_lock(&root).await;
        let _guard = lock.lock().await;
        let _file_lock = RepoFileLock::acquire(&root)?;
        let path = self.safe_worktree_path(&spec.relative_path)?;
        if path.exists() {
            return Err(GitError::Policy(format!(
                "managed worktree path already exists: {}",
                path.display()
            )));
        }
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
            fs::set_permissions(parent, fs::Permissions::from_mode(0o700))?;
        }
        let mut args = vec!["worktree".to_owned(), "add".to_owned()];
        if let Some(branch) = &spec.branch {
            validate_branch(branch)?;
            let exists = git_status(
                &root,
                [
                    "show-ref",
                    "--verify",
                    "--quiet",
                    &format!("refs/heads/{branch}"),
                ],
            )
            .await?;
            if exists.status.success() {
                return Err(GitError::Policy(format!("branch already exists: {branch}")));
            }
            args.extend(["-b".to_owned(), branch.clone()]);
        } else {
            args.push("--detach".to_owned());
        }
        args.push(path.to_string_lossy().into_owned());
        args.push(spec.base_sha.clone());
        git_ok_owned(&root, &args).await?;
        let head_sha = git_text(&path, ["rev-parse", "HEAD"]).await?;
        Ok(ManagedWorktree {
            path,
            branch: spec.branch.clone(),
            base_sha: spec.base_sha.clone(),
            head_sha,
        })
    }

    pub async fn verify_diff(
        &self,
        worktree: &Path,
        base_sha: &str,
        policy: &DiffPolicy,
    ) -> Result<VerifiedDiff, GitError> {
        ensure_sha(base_sha)?;
        let root = canonical_repo_root(worktree).await?;
        if root != fs::canonicalize(worktree)? {
            return Err(GitError::Policy(
                "worktree root did not canonicalize exactly".to_owned(),
            ));
        }
        let head_sha = git_text(&root, ["rev-parse", "HEAD"]).await?;
        let status_bytes = git_bytes(&root, ["status", "--porcelain=v2", "-z"]).await?;
        let status_porcelain_v2 = String::from_utf8_lossy(&status_bytes).replace('\0', "\n");
        let mut changed = split_nul(
            &git_bytes(
                &root,
                [
                    "diff",
                    "--name-only",
                    "-z",
                    "--find-renames",
                    base_sha,
                    "--",
                ],
            )
            .await?,
        )?;
        let untracked = split_nul(
            &git_bytes(&root, ["ls-files", "--others", "--exclude-standard", "-z"]).await?,
        )?;
        changed.extend(untracked.iter().cloned());
        changed.sort();
        changed.dedup();

        let owned = compile_globs(&policy.owned_paths)?;
        let forbidden = compile_globs(&policy.forbidden_paths)?;
        let serial = compile_globs(&policy.serial_paths)?;
        let reserved_serial = compile_globs(&policy.reserved_serial_paths)?;
        let mut unexpected_paths = Vec::new();
        let mut forbidden_paths = Vec::new();
        let mut serial_paths = Vec::new();
        for path in &changed {
            validate_relative_repo_path(path)?;
            ensure_path_stays_in(&root, path)?;
            if !owned.is_match(path) {
                unexpected_paths.push(path.clone());
            }
            if forbidden.is_match(path) {
                forbidden_paths.push(path.clone());
            }
            if serial.is_match(path) && !reserved_serial.is_match(path) {
                serial_paths.push(path.clone());
            }
        }

        let numstat = git_text_allow_empty(
            &root,
            ["diff", "--numstat", "--find-renames", base_sha, "--"],
        )
        .await?;
        let (mut additions, deletions, mut binary_files) = parse_numstat(&numstat)?;
        let tracked = changed.iter().collect::<BTreeSet<_>>();
        for path in &untracked {
            if tracked.contains(path) {
                match count_text_lines(&root.join(path))? {
                    Some(lines) => additions = additions.saturating_add(lines),
                    None => binary_files.push(path.clone()),
                }
            }
        }
        binary_files.sort();
        binary_files.dedup();
        if changed.len() > policy.max_files as usize {
            return Err(GitError::Policy(format!(
                "diff contains {} files; budget is {}",
                changed.len(),
                policy.max_files
            )));
        }
        if additions.saturating_add(deletions) > u64::from(policy.max_lines) {
            return Err(GitError::Policy(format!(
                "diff contains {} changed lines; budget is {}",
                additions.saturating_add(deletions),
                policy.max_lines
            )));
        }
        let diff_check = git_text_allow_failure(&root, ["diff", "--check", base_sha, "--"]).await?;
        let mut patch = git_text_allow_empty(
            &root,
            ["diff", "--binary", "--find-renames", base_sha, "--"],
        )
        .await?;
        for path in &untracked {
            let output = git_status(
                &root,
                ["diff", "--binary", "--no-index", "--", "/dev/null", path],
            )
            .await?;
            if !output.status.success() && output.status.code() != Some(1) {
                return Err(GitError::Command {
                    cwd: root.clone(),
                    stderr: safe_stderr(&output.stderr),
                });
            }
            patch.push_str(&String::from_utf8_lossy(&output.stdout));
        }
        Ok(VerifiedDiff {
            head_sha,
            changed_paths: changed,
            additions,
            deletions,
            binary_files,
            unexpected_paths,
            forbidden_paths,
            serial_paths,
            diff_check,
            status_porcelain_v2,
            patch,
        })
    }

    /// Materialize an attested Git tree into a clean controller-owned
    /// worktree. The candidate may live in an alternate object directory, but
    /// the resulting diff is still subject to the normal owned-path and budget
    /// verification before Harness can commit it.
    pub async fn materialize_candidate_tree(
        &self,
        worktree: &Path,
        expected_base: &str,
        alternate_object_directory: &Path,
        tree_sha: &str,
    ) -> Result<(), GitError> {
        ensure_sha(expected_base)?;
        ensure_sha(tree_sha)?;
        let root = canonical_repo_root(worktree).await?;
        if root != fs::canonicalize(worktree)? {
            return Err(GitError::Policy(
                "candidate target is not the exact managed worktree root".to_owned(),
            ));
        }
        let head = git_text(&root, ["rev-parse", "HEAD"]).await?;
        if head != expected_base {
            return Err(GitError::Conflict(format!(
                "candidate base {expected_base} differs from worktree head {head}"
            )));
        }
        if !git_bytes(&root, ["status", "--porcelain=v2", "-z"])
            .await?
            .is_empty()
        {
            return Err(GitError::Conflict(
                "candidate recovery requires a clean managed worktree".to_owned(),
            ));
        }
        let object_directory = fs::canonicalize(alternate_object_directory)?;
        if !object_directory.is_dir() {
            return Err(GitError::Policy(
                "candidate alternate object directory is not a directory".to_owned(),
            ));
        }
        let alternate = object_directory.as_os_str();
        let tree_type = Command::new("git")
            .current_dir(&root)
            .env("GIT_ALTERNATE_OBJECT_DIRECTORIES", alternate)
            .args(["cat-file", "-t", tree_sha])
            .output()
            .await?;
        if !tree_type.status.success() || tree_type.stdout != b"tree\n" {
            return Err(GitError::Policy(format!(
                "candidate object {tree_sha} is not an accessible Git tree: {}",
                safe_stderr(&tree_type.stderr)
            )));
        }
        let patch = Command::new("git")
            .current_dir(&root)
            .env("GIT_ALTERNATE_OBJECT_DIRECTORIES", alternate)
            .args(["diff", "--binary", expected_base, tree_sha, "--"])
            .output()
            .await?;
        if !patch.status.success() {
            return Err(GitError::Command {
                cwd: root.clone(),
                stderr: safe_stderr(&patch.stderr),
            });
        }
        if patch.stdout.is_empty() {
            return Err(GitError::Policy(
                "candidate tree is identical to the managed base".to_owned(),
            ));
        }
        if patch.stdout.len() > 128 * 1024 * 1024 {
            return Err(GitError::Policy(
                "candidate patch exceeds the 128 MiB recovery boundary".to_owned(),
            ));
        }
        let mut apply = Command::new("git")
            .current_dir(&root)
            .args(["apply", "--index", "--binary", "--whitespace=nowarn", "-"])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()?;
        let Some(mut stdin) = apply.stdin.take() else {
            return Err(GitError::Protocol(
                "git apply did not expose its input pipe".to_owned(),
            ));
        };
        stdin.write_all(&patch.stdout).await?;
        drop(stdin);
        let result = apply.wait_with_output().await?;
        if !result.status.success() {
            return Err(GitError::Command {
                cwd: root,
                stderr: safe_stderr(&result.stderr),
            });
        }
        Ok(())
    }

    pub async fn diff_summary(
        &self,
        worktree: &Path,
        base_sha: &str,
    ) -> Result<DiffSummary, GitError> {
        ensure_sha(base_sha)?;
        let root = canonical_repo_root(worktree).await?;
        if root != fs::canonicalize(worktree)? {
            return Err(GitError::Policy(
                "worktree root did not canonicalize exactly".to_owned(),
            ));
        }
        let head_sha = git_text(&root, ["rev-parse", "HEAD"]).await?;
        let status = git_bytes(&root, ["status", "--porcelain=v2", "-z"]).await?;
        let mut changed_paths = split_nul(
            &git_bytes(
                &root,
                [
                    "diff",
                    "--name-only",
                    "-z",
                    "--find-renames",
                    base_sha,
                    "--",
                ],
            )
            .await?,
        )?;
        let untracked = split_nul(
            &git_bytes(&root, ["ls-files", "--others", "--exclude-standard", "-z"]).await?,
        )?;
        changed_paths.extend(untracked.iter().cloned());
        changed_paths.sort();
        changed_paths.dedup();
        for path in &changed_paths {
            validate_relative_repo_path(path)?;
            ensure_path_stays_in(&root, path)?;
        }

        let numstat = git_text_allow_empty(
            &root,
            ["diff", "--numstat", "--find-renames", base_sha, "--"],
        )
        .await?;
        let (mut additions, deletions, _) = parse_numstat(&numstat)?;
        for path in &untracked {
            if let Some(lines) = count_text_lines(&root.join(path))? {
                additions = additions.saturating_add(lines);
            }
        }
        Ok(DiffSummary {
            head_sha,
            dirty: !status.is_empty(),
            changed_paths,
            additions,
            deletions,
        })
    }

    /// Returns a stable digest of HEAD plus every staged, unstaged, and
    /// untracked worktree change. Two consecutive snapshots must agree so an
    /// approval cannot be minted from an internally inconsistent read.
    pub async fn worktree_fingerprint(&self, worktree: &Path) -> Result<String, GitError> {
        let root = canonical_repo_root(worktree).await?;
        if root != fs::canonicalize(worktree)? {
            return Err(GitError::Policy(
                "worktree root did not canonicalize exactly".to_owned(),
            ));
        }
        let first = worktree_snapshot(&root).await?;
        let second = worktree_snapshot(&root).await?;
        if first != second {
            return Err(GitError::Conflict(
                "worktree changed while its approval fingerprint was being computed".to_owned(),
            ));
        }
        Ok(first)
    }

    pub async fn head_sha(&self, worktree: &Path) -> Result<String, GitError> {
        let root = canonical_repo_root(worktree).await?;
        let head = git_text(&root, ["rev-parse", "HEAD"]).await?;
        ensure_sha(&head)?;
        Ok(head)
    }

    pub async fn commit(
        &self,
        worktree: &Path,
        message: &str,
        verified: &VerifiedDiff,
    ) -> Result<String, GitError> {
        if !verified.acceptable() {
            return Err(GitError::Policy(
                "cannot commit a diff that failed custody checks".to_owned(),
            ));
        }
        validate_public_change_metadata(message)?;
        let root = canonical_repo_root(worktree).await?;
        let name = optional_git_text(&root, ["config", "user.name"]).await?;
        let email = optional_git_text(&root, ["config", "user.email"]).await?;
        if name.is_none() || email.is_none() {
            return Err(GitError::Policy(
                "configured user Git identity is required for controller commits".to_owned(),
            ));
        }
        git_ok(&root, ["add", "--all", "--"]).await?;
        git_ok(&root, ["commit", "--no-gpg-sign", "-m", message]).await?;
        let sha = git_text(&root, ["rev-parse", "HEAD"]).await?;
        ensure_sha(&sha)?;
        let body = git_text(&root, ["log", "-1", "--pretty=%B"]).await?;
        validate_public_change_metadata(&body)?;
        Ok(sha)
    }

    pub async fn cherry_pick(
        &self,
        integration_worktree: &Path,
        commits: &[String],
    ) -> Result<String, GitError> {
        let root = canonical_repo_root(integration_worktree).await?;
        for commit in commits {
            ensure_sha(commit)?;
            let result = git_status(&root, ["cherry-pick", commit]).await?;
            if !result.status.success() {
                return Err(GitError::Conflict(format!(
                    "cherry-pick {commit} stopped for semantic review: {}",
                    String::from_utf8_lossy(&result.stderr)
                )));
            }
        }
        git_text(&root, ["rev-parse", "HEAD"]).await
    }

    pub async fn push_exact(
        &self,
        worktree: &Path,
        remote: &str,
        branch: &str,
        expected_head: &str,
    ) -> Result<(), GitError> {
        ensure_sha(expected_head)?;
        validate_branch(branch)?;
        let root = canonical_repo_root(worktree).await?;
        let actual = git_text(&root, ["rev-parse", "HEAD"]).await?;
        if actual != expected_head {
            return Err(GitError::Conflict(format!(
                "reviewed head {expected_head} changed to {actual} before push"
            )));
        }
        let refspec = format!("HEAD:refs/heads/{branch}");
        git_ok(&root, ["push", remote, &refspec]).await
    }

    pub async fn remove_worktree(
        &self,
        repository: &Path,
        worktree: &Path,
        force: bool,
    ) -> Result<(), GitError> {
        let root = canonical_repo_root(repository).await?;
        let lock = self.process_lock(&root).await;
        let _guard = lock.lock().await;
        let _file_lock = RepoFileLock::acquire(&root)?;
        let managed = fs::canonicalize(worktree)?;
        if !managed.starts_with(self.worktree_root.as_ref()) {
            return Err(GitError::Policy(
                "refusing to remove a worktree outside the managed root".to_owned(),
            ));
        }
        let status = git_bytes(&managed, ["status", "--porcelain=v2", "-z"]).await?;
        if !force && !status.is_empty() {
            return Err(GitError::Policy(
                "worktree is dirty; preserve or explicitly dispose it".to_owned(),
            ));
        }
        let mut args = vec!["worktree", "remove"];
        if force {
            args.push("--force");
        }
        args.push(
            managed
                .to_str()
                .ok_or_else(|| GitError::Policy("non-UTF-8 path".to_owned()))?,
        );
        git_ok(&root, args).await?;
        git_ok(&root, ["worktree", "prune"]).await
    }

    pub async fn prune_worktrees(&self, repository: &Path) -> Result<(), GitError> {
        let root = canonical_repo_root(repository).await?;
        let lock = self.process_lock(&root).await;
        let _guard = lock.lock().await;
        let _file_lock = RepoFileLock::acquire(&root)?;
        git_ok(&root, ["worktree", "prune"]).await
    }

    async fn process_lock(&self, root: &Path) -> Arc<Mutex<()>> {
        let mut locks = self.locks.lock().await;
        locks
            .entry(root.to_path_buf())
            .or_insert_with(|| Arc::new(Mutex::new(())))
            .clone()
    }

    fn safe_worktree_path(&self, relative: &Path) -> Result<PathBuf, GitError> {
        if relative.is_absolute()
            || relative.components().any(|component| {
                matches!(
                    component,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            })
        {
            return Err(GitError::Policy(
                "managed worktree path must be a safe relative path".to_owned(),
            ));
        }
        Ok(self.worktree_root.join(relative))
    }
}

fn find_repository_roots(search_roots: &[PathBuf]) -> Result<Vec<PathBuf>, std::io::Error> {
    const MAX_DEPTH: usize = 4;
    const MAX_DIRECTORIES: usize = 20_000;
    const MAX_REPOSITORIES: usize = 200;

    let mut queue = VecDeque::new();
    let mut queued = BTreeSet::new();
    for root in search_roots {
        let Ok(root) = fs::canonicalize(root) else {
            continue;
        };
        if root.is_dir() && queued.insert(root.clone()) {
            queue.push_back((root, 0_usize));
        }
    }

    let mut visited = 0_usize;
    let mut repositories = BTreeSet::new();
    while let Some((directory, depth)) = queue.pop_front() {
        visited = visited.saturating_add(1);
        if visited > MAX_DIRECTORIES || repositories.len() >= MAX_REPOSITORIES {
            break;
        }
        if is_git_checkout(&directory) {
            repositories.insert(directory);
            continue;
        }
        if depth >= MAX_DEPTH {
            continue;
        }
        let Ok(entries) = fs::read_dir(&directory) else {
            continue;
        };
        let mut children = entries.filter_map(Result::ok).collect::<Vec<_>>();
        children.sort_by_key(std::fs::DirEntry::file_name);
        for entry in children {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if should_skip_discovery_directory(&name) {
                continue;
            }
            let Ok(file_type) = entry.file_type() else {
                continue;
            };
            if file_type.is_dir() && !file_type.is_symlink() {
                queue.push_back((entry.path(), depth + 1));
            }
        }
    }
    Ok(repositories.into_iter().collect())
}

fn is_git_checkout(path: &Path) -> bool {
    fs::symlink_metadata(path.join(".git")).is_ok_and(|metadata| {
        !metadata.file_type().is_symlink() && (metadata.is_dir() || metadata.is_file())
    })
}

fn should_skip_discovery_directory(name: &str) -> bool {
    name.starts_with('.')
        || matches!(
            name.to_ascii_lowercase().as_str(),
            "node_modules" | "target" | "dist" | "build" | "vendor"
        )
}

async fn worktree_snapshot(root: &Path) -> Result<String, GitError> {
    let head = git_text(root, ["rev-parse", "HEAD"]).await?;
    ensure_sha(&head)?;
    let status = git_bytes(
        root,
        ["status", "--porcelain=v2", "-z", "--untracked-files=all"],
    )
    .await?;
    let tracked_diff = git_bytes(
        root,
        [
            "diff",
            "--binary",
            "--full-index",
            "--submodule=diff",
            "HEAD",
            "--",
        ],
    )
    .await?;
    let mut untracked =
        split_nul(&git_bytes(root, ["ls-files", "--others", "--exclude-standard", "-z"]).await?)?;
    untracked.sort();
    untracked.dedup();

    let mut digest = Sha256::new();
    hash_framed(&mut digest, b"harness-worktree-fingerprint-v1");
    hash_framed(&mut digest, head.as_bytes());
    hash_framed(&mut digest, &status);
    hash_framed(&mut digest, &tracked_diff);
    for relative in untracked {
        validate_relative_repo_path(&relative)?;
        ensure_path_stays_in(root, &relative)?;
        hash_framed(&mut digest, relative.as_bytes());
        let path = root.join(&relative);
        let metadata = fs::symlink_metadata(&path)?;
        hash_framed(
            &mut digest,
            &u64::from(metadata.permissions().mode()).to_be_bytes(),
        );
        hash_framed(&mut digest, &metadata.len().to_be_bytes());
        if metadata.file_type().is_symlink() {
            hash_framed(&mut digest, b"symlink");
            hash_framed(&mut digest, fs::read_link(&path)?.as_os_str().as_bytes());
        } else if metadata.is_file() {
            hash_framed(&mut digest, b"file");
            let mut file = File::open(&path)?;
            let mut buffer = [0_u8; 64 * 1024];
            loop {
                let read = file.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                digest.update(&buffer[..read]);
            }
        } else {
            return Err(GitError::Policy(format!(
                "unsupported untracked filesystem entry: {relative}"
            )));
        }
    }
    Ok(hex::encode(digest.finalize()))
}

fn hash_framed(digest: &mut Sha256, value: &[u8]) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value);
}

struct RepoFileLock {
    file: File,
}

impl RepoFileLock {
    fn acquire(root: &Path) -> Result<Self, GitError> {
        let git_dir = root.join(".git");
        if !git_dir.is_dir() {
            return Err(GitError::Policy(format!(
                "coordination repository has no .git directory: {}",
                root.display()
            )));
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .mode(0o600)
            .open(git_dir.join("harness-console.lock"))?;
        file.try_lock_exclusive()
            .map_err(|_| GitError::RepositoryLocked(root.to_path_buf()))?;
        Ok(Self { file })
    }
}

impl Drop for RepoFileLock {
    fn drop(&mut self) {
        let _ = self.file.unlock();
    }
}

async fn canonical_repo_root(repository: &Path) -> Result<PathBuf, GitError> {
    let canonical = fs::canonicalize(repository)?;
    let output = git_text(&canonical, ["rev-parse", "--show-toplevel"]).await?;
    let root = fs::canonicalize(output)?;
    if root != canonical {
        return Err(GitError::Policy(format!(
            "path must identify the repository/worktree root exactly: {}",
            canonical.display()
        )));
    }
    Ok(root)
}

fn validate_reference(reference: &str) -> Result<(), GitError> {
    if reference.is_empty()
        || reference.starts_with('-')
        || reference.contains(['\0', '\n', '\r'])
        || reference.contains("..")
    {
        return Err(GitError::Policy("unsafe Git reference".to_owned()));
    }
    Ok(())
}

fn validate_branch(branch: &str) -> Result<(), GitError> {
    validate_reference(branch)?;
    if branch.starts_with('/') || branch.ends_with('/') || branch.contains("//") {
        return Err(GitError::Policy("unsafe branch name".to_owned()));
    }
    Ok(())
}

fn ensure_sha(sha: &str) -> Result<(), GitError> {
    if sha.len() == 40
        && sha
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        Ok(())
    } else {
        Err(GitError::Policy(format!(
            "not an exact 40-character Git SHA: {sha}"
        )))
    }
}

fn validate_relative_repo_path(path: &str) -> Result<(), GitError> {
    let path = Path::new(path);
    if path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(GitError::Policy(format!(
            "unsafe changed path: {}",
            path.display()
        )));
    }
    Ok(())
}

fn ensure_path_stays_in(root: &Path, relative: &str) -> Result<(), GitError> {
    let path = root.join(relative);
    let mut existing = path.as_path();
    loop {
        match fs::symlink_metadata(existing) {
            Ok(_) => break,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                existing = existing.parent().ok_or_else(|| {
                    GitError::Policy(format!("changed path escapes worktree: {relative}"))
                })?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    let resolved = fs::canonicalize(existing).map_err(|_| {
        GitError::Policy(format!(
            "changed path contains an unresolved symlink: {relative}"
        ))
    })?;
    if !resolved.starts_with(root) {
        return Err(GitError::Policy(format!(
            "symlink path escapes worktree: {relative}"
        )));
    }
    Ok(())
}

fn compile_globs(patterns: &[String]) -> Result<GlobSet, GitError> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        builder.add(Glob::new(pattern).map_err(|error| GitError::Policy(error.to_string()))?);
    }
    builder
        .build()
        .map_err(|error| GitError::Policy(error.to_string()))
}

fn split_nul(bytes: &[u8]) -> Result<Vec<String>, GitError> {
    bytes
        .split(|byte| *byte == 0)
        .filter(|part| !part.is_empty())
        .map(|part| {
            String::from_utf8(part.to_vec())
                .map_err(|_| GitError::Policy("Git returned a non-UTF-8 path".to_owned()))
        })
        .collect()
}

fn parse_numstat(value: &str) -> Result<(u64, u64, Vec<String>), GitError> {
    let mut additions = 0_u64;
    let mut deletions = 0_u64;
    let mut binary = Vec::new();
    for line in value.lines().filter(|line| !line.is_empty()) {
        let mut fields = line.splitn(3, '\t');
        let added = fields.next().unwrap_or("0");
        let deleted = fields.next().unwrap_or("0");
        let path = fields.next().unwrap_or("");
        if added == "-" || deleted == "-" {
            binary.push(path.to_owned());
        } else {
            additions =
                additions.saturating_add(added.parse().map_err(|_| {
                    GitError::Protocol(format!("invalid numstat additions: {line}"))
                })?);
            deletions =
                deletions.saturating_add(deleted.parse().map_err(|_| {
                    GitError::Protocol(format!("invalid numstat deletions: {line}"))
                })?);
        }
    }
    Ok((additions, deletions, binary))
}

fn count_text_lines(path: &Path) -> Result<Option<u64>, GitError> {
    let bytes = fs::read(path)?;
    if bytes.contains(&0) {
        return Ok(None);
    }
    Ok(Some(
        bytes.iter().filter(|byte| **byte == b'\n').count() as u64
            + u64::from(!bytes.is_empty() && !bytes.ends_with(b"\n")),
    ))
}

pub fn validate_public_change_metadata(message: &str) -> Result<(), GitError> {
    let lower = message.to_ascii_lowercase();
    const BLOCKED_NAMES: &[&str] = &[
        "codex",
        "grok",
        "muse",
        "claude",
        "chatgpt",
        "copilot",
        "gemini",
        "cursor",
        "windsurf",
        "devin",
        "aider",
        "openai",
        "anthropic",
        "deepseek",
        "perplexity",
        "qwen",
        "kimi",
        "gpt",
        "llm",
    ];
    let words = lower
        .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
        .filter(|word| !word.is_empty())
        .collect::<Vec<_>>();
    let names_blocked = words.iter().any(|word| BLOCKED_NAMES.contains(word));
    let normalized = words.join(" ");
    let attribution_blocked = [
        "generated",
        "authored",
        "written",
        "assisted",
        "created",
        "committed",
    ]
    .iter()
    .any(|verb| {
        ["ai", "bot", "agent", "model", "tool"].iter().any(|actor| {
            normalized.contains(&format!("{verb} by {actor}"))
                || normalized.contains(&format!("{verb} by a {actor}"))
                || normalized.contains(&format!("{verb} by an {actor}"))
        })
    }) || ["with help of", "with the help of"].iter().any(|prefix| {
        ["ai", "bot", "agent", "model", "tool"]
            .iter()
            .any(|actor| normalized.contains(&format!("{prefix} {actor}")))
    }) || lower.lines().any(|line| {
        line.trim_start().starts_with("co-authored-by:")
            && ["bot", "agent", "model", "ai"]
                .iter()
                .any(|actor| line.contains(actor))
    });
    if names_blocked || attribution_blocked {
        return Err(GitError::Policy(
            "public change metadata contains prohibited automation attribution language".to_owned(),
        ));
    }
    Ok(())
}

async fn git_ok<I, S>(cwd: &Path, args: I) -> Result<(), GitError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    let output = git_status(cwd, args).await?;
    if output.status.success() {
        Ok(())
    } else {
        Err(GitError::Command {
            cwd: cwd.to_path_buf(),
            stderr: safe_stderr(&output.stderr),
        })
    }
}

async fn git_ok_owned(cwd: &Path, args: &[String]) -> Result<(), GitError> {
    git_ok(cwd, args).await
}

async fn git_text<I, S>(cwd: &Path, args: I) -> Result<String, GitError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    let output = git_status(cwd, args).await?;
    if !output.status.success() {
        return Err(GitError::Command {
            cwd: cwd.to_path_buf(),
            stderr: safe_stderr(&output.stderr),
        });
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

async fn git_text_allow_empty<I, S>(cwd: &Path, args: I) -> Result<String, GitError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    git_text(cwd, args).await
}

async fn git_text_allow_failure<I, S>(cwd: &Path, args: I) -> Result<String, GitError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    let output = git_status(cwd, args).await?;
    Ok(format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        safe_stderr(&output.stderr)
    ))
}

async fn optional_git_text<I, S>(cwd: &Path, args: I) -> Result<Option<String>, GitError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    let output = git_status(cwd, args).await?;
    if !output.status.success() {
        return Ok(None);
    }
    let value = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    Ok((!value.is_empty()).then_some(value))
}

async fn git_bytes<I, S>(cwd: &Path, args: I) -> Result<Vec<u8>, GitError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    let output = git_status(cwd, args).await?;
    if output.status.success() {
        Ok(output.stdout)
    } else {
        Err(GitError::Command {
            cwd: cwd.to_path_buf(),
            stderr: safe_stderr(&output.stderr),
        })
    }
}

async fn git_status<I, S>(cwd: &Path, args: I) -> Result<std::process::Output, GitError>
where
    I: IntoIterator<Item = S>,
    S: AsRef<std::ffi::OsStr>,
{
    let arguments = args
        .into_iter()
        .map(|argument| argument.as_ref().to_os_string())
        .collect::<Vec<_>>();
    let mut command = Command::new("git");
    command
        .current_dir(cwd)
        .args(&arguments)
        .stdin(Stdio::null())
        .kill_on_drop(true)
        .env("GIT_TERMINAL_PROMPT", "0")
        .env("GCM_INTERACTIVE", "Never")
        .env("SSH_ASKPASS_REQUIRE", "never");
    debug!(cwd = %cwd.display(), command = ?command, "running controller Git command");
    timeout(Duration::from_secs(300), command.output())
        .await
        .map_err(|_| GitError::Timeout {
            cwd: cwd.to_path_buf(),
        })?
        .map_err(Into::into)
}

fn sanitize_remote_url(value: &str) -> String {
    let Ok(mut url) = Url::parse(value) else {
        return value.to_owned();
    };
    if matches!(url.scheme(), "http" | "https")
        && (!url.username().is_empty() || url.password().is_some())
    {
        let _ = url.set_username("");
        let _ = url.set_password(None);
    }
    url.to_string()
}

fn safe_stderr(bytes: &[u8]) -> String {
    let raw = String::from_utf8_lossy(bytes);
    let redacted = redact_http_userinfo(&redact_diagnostic(&raw));
    redacted.chars().take(65_536).collect()
}

fn redact_http_userinfo(value: &str) -> String {
    let mut output = value.to_owned();
    let mut cursor = 0_usize;
    while let Some(relative_scheme) = output[cursor..].find("://") {
        let authority_start = cursor + relative_scheme + 3;
        let authority_end = output[authority_start..]
            .find(|character: char| character == '/' || character.is_whitespace())
            .map(|offset| authority_start + offset)
            .unwrap_or(output.len());
        let Some(relative_at) = output[authority_start..authority_end].rfind('@') else {
            cursor = authority_end;
            continue;
        };
        let at = authority_start + relative_at;
        output.replace_range(authority_start..at, "<redacted>");
        cursor = authority_start + "<redacted>@".len();
    }
    output
}

#[derive(Debug, Error)]
pub enum GitError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Git command failed in {cwd}: {stderr}")]
    Command { cwd: PathBuf, stderr: String },
    #[error("Git command timed out after 300 seconds in {cwd}")]
    Timeout { cwd: PathBuf },
    #[error("repository is already locked: {0}")]
    RepositoryLocked(PathBuf),
    #[error("Git custody policy rejected the operation: {0}")]
    Policy(String),
    #[error("Git protocol output was invalid: {0}")]
    Protocol(String),
    #[error("integration conflict: {0}")]
    Conflict(String),
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::symlink;
    use std::process::Command as StdCommand;

    use tempfile::TempDir;

    use super::*;

    fn fixture_git(repository: &Path, args: &[&str]) {
        let output = StdCommand::new("git")
            .current_dir(repository)
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "git {args:?} failed: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    #[test]
    fn rejects_path_escape() {
        assert!(validate_relative_repo_path("../secret").is_err());
        assert!(validate_relative_repo_path("central/src/lib.rs").is_ok());
    }

    #[test]
    fn rejects_external_and_unresolved_symlink_paths() {
        let temp = TempDir::new().unwrap();
        let root = temp.path().join("root");
        let outside = temp.path().join("outside");
        fs::create_dir_all(&root).unwrap();
        fs::create_dir_all(&outside).unwrap();
        symlink(&outside, root.join("external")).unwrap();
        symlink("missing-target", root.join("broken")).unwrap();
        assert!(ensure_path_stays_in(&root, "external/file.txt").is_err());
        assert!(ensure_path_stays_in(&root, "broken").is_err());
    }

    #[test]
    fn parses_text_and_binary_numstat() {
        let (add, del, binary) = parse_numstat("12\t4\tsrc/lib.rs\n-\t-\timage.png\n").unwrap();
        assert_eq!((add, del), (12, 4));
        assert_eq!(binary, vec!["image.png"]);
    }

    #[test]
    fn rejects_automation_attribution_in_public_metadata() {
        assert!(
            validate_public_change_metadata("fix: x\n\nCo-authored-by: automation agent <x@y>")
                .is_err()
        );
        assert!(validate_public_change_metadata("created by an ai tool").is_err());
        for name in ["codex", "grok", "muse", "claude"] {
            assert!(validate_public_change_metadata(&format!("update from {name}")).is_err());
        }
        assert!(validate_public_change_metadata("provider-specific assistant name").is_ok());
        assert!(validate_public_change_metadata("fix: exact identity").is_ok());
    }

    #[test]
    fn remote_credentials_are_never_projected_or_logged() {
        assert_eq!(
            sanitize_remote_url("https://user:secret@example.com/org/repo.git"),
            "https://example.com/org/repo.git"
        );
        let diagnostic = redact_http_userinfo(
            "fatal: could not read https://token-value@example.com/org/repo.git",
        );
        assert!(!diagnostic.contains("token-value"));
        assert!(diagnostic.contains("https://<redacted>@example.com"));
    }

    #[tokio::test]
    async fn worktree_fingerprint_binds_tracked_and_untracked_contents() {
        let temp = TempDir::new().unwrap();
        let repository = temp.path().join("repository");
        fs::create_dir(&repository).unwrap();
        fixture_git(&repository, &["init", "-b", "main"]);
        fixture_git(&repository, &["config", "user.name", "Harness Test"]);
        fixture_git(
            &repository,
            &["config", "user.email", "harness@example.invalid"],
        );
        fs::write(repository.join("tracked.txt"), b"original\n").unwrap();
        fixture_git(&repository, &["add", "tracked.txt"]);
        fixture_git(&repository, &["commit", "-m", "test: initialize fixture"]);

        let manager = GitManager::new(&temp.path().join("managed-worktrees")).unwrap();
        let clean = manager.worktree_fingerprint(&repository).await.unwrap();
        assert_eq!(
            clean,
            manager.worktree_fingerprint(&repository).await.unwrap()
        );

        fs::write(repository.join("tracked.txt"), b"modified\n").unwrap();
        let tracked = manager.worktree_fingerprint(&repository).await.unwrap();
        assert_ne!(clean, tracked);

        fs::write(repository.join("untracked.txt"), b"first-value").unwrap();
        let first_untracked = manager.worktree_fingerprint(&repository).await.unwrap();
        fs::write(repository.join("untracked.txt"), b"other-value").unwrap();
        let second_untracked = manager.worktree_fingerprint(&repository).await.unwrap();
        assert_ne!(first_untracked, second_untracked);
    }

    #[tokio::test]
    async fn materializes_attested_tree_into_clean_worktree() {
        let temp = TempDir::new().unwrap();
        let repository = temp.path().join("repository");
        fs::create_dir(&repository).unwrap();
        fixture_git(&repository, &["init", "-b", "main"]);
        fixture_git(&repository, &["config", "user.name", "Harness Test"]);
        fixture_git(
            &repository,
            &["config", "user.email", "harness@example.invalid"],
        );
        fs::write(repository.join("tracked.txt"), b"original\n").unwrap();
        fixture_git(&repository, &["add", "tracked.txt"]);
        fixture_git(&repository, &["commit", "-m", "test: initialize fixture"]);
        let base = String::from_utf8(
            StdCommand::new("git")
                .current_dir(&repository)
                .args(["rev-parse", "HEAD"])
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap()
        .trim()
        .to_owned();
        fs::write(repository.join("tracked.txt"), b"candidate\n").unwrap();
        fixture_git(&repository, &["add", "tracked.txt"]);
        let tree = String::from_utf8(
            StdCommand::new("git")
                .current_dir(&repository)
                .args(["write-tree"])
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap()
        .trim()
        .to_owned();
        fixture_git(
            &repository,
            &["restore", "--staged", "--worktree", "tracked.txt"],
        );

        let manager = GitManager::new(&temp.path().join("managed-worktrees")).unwrap();
        manager
            .materialize_candidate_tree(&repository, &base, &repository.join(".git/objects"), &tree)
            .await
            .unwrap();
        assert_eq!(
            fs::read_to_string(repository.join("tracked.txt")).unwrap(),
            "candidate\n"
        );
        assert!(
            !git_bytes(&repository, ["status", "--porcelain=v2", "-z"])
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn clean_worktree_removal_discards_ignored_output_but_refuses_source_debris() {
        let temp = TempDir::new().unwrap();
        let repository = temp.path().join("repository");
        fs::create_dir(&repository).unwrap();
        fixture_git(&repository, &["init", "-b", "main"]);
        fixture_git(&repository, &["config", "user.name", "Harness Test"]);
        fixture_git(
            &repository,
            &["config", "user.email", "harness@example.invalid"],
        );
        fs::write(repository.join(".gitignore"), b"target/\n").unwrap();
        fs::write(repository.join("tracked.txt"), b"original\n").unwrap();
        fixture_git(&repository, &["add", ".gitignore", "tracked.txt"]);
        fixture_git(&repository, &["commit", "-m", "test: initialize fixture"]);
        let base = String::from_utf8(
            StdCommand::new("git")
                .current_dir(&repository)
                .args(["rev-parse", "HEAD"])
                .output()
                .unwrap()
                .stdout,
        )
        .unwrap()
        .trim()
        .to_owned();
        let manager = GitManager::new(&temp.path().join("managed-worktrees")).unwrap();

        let disposable = manager
            .create_worktree(&WorktreeSpec {
                repository_root: repository.clone(),
                relative_path: PathBuf::from("run/disposable"),
                base_sha: base.clone(),
                branch: None,
            })
            .await
            .unwrap();
        fs::create_dir_all(disposable.path.join("target/debug")).unwrap();
        fs::write(
            disposable.path.join("target/debug/generated-output"),
            b"disposable",
        )
        .unwrap();
        manager
            .remove_worktree(&repository, &disposable.path, false)
            .await
            .unwrap();
        assert!(!disposable.path.exists());

        let dirty = manager
            .create_worktree(&WorktreeSpec {
                repository_root: repository.clone(),
                relative_path: PathBuf::from("run/dirty"),
                base_sha: base,
                branch: None,
            })
            .await
            .unwrap();
        fs::write(dirty.path.join("untracked-source.txt"), b"preserve me").unwrap();
        assert!(
            manager
                .remove_worktree(&repository, &dirty.path, false)
                .await
                .is_err()
        );
        assert!(dirty.path.exists());
    }
}

//! Typed configuration, Linux XDG paths, and repository policy profiles.

use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
};

use harness_domain::{ImprovementMode, ImprovementRuntimeStatus, PricingSnapshot, ResourceClass};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

const DEFAULT_CONFIG: &str = include_str!("../../../config/harness.example.toml");
const FROZEN_SAFETY_ANCHOR: &str =
    include_str!("../../../config/self-improvement/frozen-safety-anchor.v1.md");
/// SHA-256 of `config/self-improvement/frozen-safety-anchor.v1.md` bytes.
pub const FROZEN_SAFETY_ANCHOR_SHA256: &str =
    "197cd971adb684a42509253054dc6987910a5b35fceac0d7f208449d42e50d77";
const GENERAL_PROFILE: &str = include_str!("../../../profiles/general/profile.toml");
const BILDR_PROFILE: &str = include_str!("../../../profiles/bildr/profile.toml");

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct HarnessConfig {
    pub schema_version: u32,
    pub server: ServerConfig,
    pub paths: PathsConfig,
    pub codex: CodexConfig,
    pub orchestration: OrchestrationConfig,
    pub git: GitConfig,
    pub security: SecurityConfig,
    pub storage: StorageConfig,
    pub usage: UsageConfig,
    pub pricing: PricingConfig,
    #[serde(default)]
    pub self_improvement: SelfImprovementConfig,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    pub bind: String,
    pub open_browser_on_start: bool,
    pub ui_event_replay_limit: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PathsConfig {
    pub data_dir: String,
    pub cache_dir: String,
    pub config_dir: String,
    pub worktree_root: String,
    pub artifact_root: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct CodexConfig {
    pub binary: String,
    pub transport: String,
    pub service_name: String,
    pub experimental_api: bool,
    pub required_version: String,
    pub required_protocol_schema_sha256: String,
    pub execution_on_schema_mismatch: String,
    pub reasoning_summary: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OrchestrationConfig {
    pub max_total_agent_threads: u32,
    pub max_mutable_tasks: u32,
    pub max_independent_verifiers: u32,
    pub max_read_only_discovery: u32,
    pub max_automatic_remediation_rounds: u32,
    #[serde(default = "default_automatic_plan_approval_max_execution_tokens")]
    pub automatic_plan_approval_max_execution_tokens: u64,
    pub default_task_token_budget: u64,
    pub default_turn_timeout_seconds: u64,
    pub lease_ttl_seconds: u64,
    pub heartbeat_interval_seconds: u64,
}

const fn default_automatic_plan_approval_max_execution_tokens() -> u64 {
    2_000_000
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GitConfig {
    pub primary_checkout_must_remain_clean: bool,
    pub fetch_before_run: bool,
    pub base_ref: String,
    pub auto_push: bool,
    pub auto_create_pr: bool,
    pub auto_mark_pr_ready: bool,
    pub auto_merge: bool,
    pub preserve_failed_worktrees: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SecurityConfig {
    pub bind_localhost_only: bool,
    pub network_default: String,
    pub approval_policy: String,
    pub store_raw_reasoning: bool,
    pub store_reasoning_summaries: bool,
    pub redact_environment_values: bool,
    pub redact_probable_secrets: bool,
    pub allow_agent_full_access: bool,
    pub allow_automatic_external_writes: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct StorageConfig {
    pub sqlite_wal: bool,
    pub raw_event_retention_days: u32,
    pub command_log_retention_days: u32,
    pub artifact_retention_days: u32,
    pub compress_raw_events: bool,
    pub hash_algorithm: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UsageConfig {
    pub label_subscription_costs_as_api_equivalent: bool,
    pub show_reasoning_tokens_separately: bool,
    pub never_double_count_reasoning_output: bool,
    pub cost_when_cache_write_missing: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PricingConfig {
    pub snapshots: Vec<PriceSnapshotConfig>,
}

/// The only shipped improvement capability is passive observation.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SelfImprovementConfig {
    #[serde(default)]
    pub mode: ImprovementMode,
    #[serde(default = "default_frozen_safety_anchor_sha256")]
    pub safety_anchor_sha256: String,
}

impl Default for SelfImprovementConfig {
    fn default() -> Self {
        Self {
            mode: ImprovementMode::Disabled,
            safety_anchor_sha256: default_frozen_safety_anchor_sha256(),
        }
    }
}

fn default_frozen_safety_anchor_sha256() -> String {
    FROZEN_SAFETY_ANCHOR_SHA256.to_owned()
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PriceSnapshotConfig {
    pub id: String,
    pub effective_at: String,
    pub model: String,
    pub currency: String,
    pub input_per_million: f64,
    pub cached_input_per_million: f64,
    pub output_per_million: f64,
    pub cache_write_multiplier: f64,
    pub long_context_threshold_tokens: Option<u64>,
    pub long_context_input_multiplier: Option<f64>,
    pub long_context_output_multiplier: Option<f64>,
}

impl PriceSnapshotConfig {
    pub fn to_domain(&self) -> Result<PricingSnapshot, ProfileError> {
        if self.currency != "USD" {
            return Err(ProfileError::Validation(format!(
                "pricing snapshot {} uses unsupported currency {}",
                self.id, self.currency
            )));
        }
        let rate = |value: f64, name: &str| -> Result<u64, ProfileError> {
            if !value.is_finite() || value < 0.0 {
                return Err(ProfileError::Validation(format!(
                    "pricing snapshot {} has invalid {name}",
                    self.id
                )));
            }
            Ok((value * 1_000_000.0).round() as u64)
        };
        let ratio = |value: Option<f64>, default: f64| -> Result<(u64, u64), ProfileError> {
            let value = value.unwrap_or(default);
            if !value.is_finite() || value <= 0.0 {
                return Err(ProfileError::Validation(format!(
                    "pricing snapshot {} has invalid multiplier",
                    self.id
                )));
            }
            Ok(((value * 1_000_000.0).round() as u64, 1_000_000))
        };
        let cache_ratio = ratio(Some(self.cache_write_multiplier), 1.0)?;
        let input_ratio = ratio(self.long_context_input_multiplier, 1.0)?;
        let output_ratio = ratio(self.long_context_output_multiplier, 1.0)?;
        Ok(PricingSnapshot {
            id: self.id.clone(),
            model: self.model.clone(),
            effective_at: self.effective_at.clone(),
            input_microusd_per_million: rate(self.input_per_million, "input price")?,
            cached_input_microusd_per_million: rate(
                self.cached_input_per_million,
                "cached input price",
            )?,
            output_microusd_per_million: rate(self.output_per_million, "output price")?,
            cache_write_multiplier_numerator: cache_ratio.0,
            cache_write_multiplier_denominator: cache_ratio.1,
            long_context_threshold_tokens: self.long_context_threshold_tokens,
            long_context_input_multiplier_numerator: Some(input_ratio.0),
            long_context_input_multiplier_denominator: Some(input_ratio.1),
            long_context_output_multiplier_numerator: Some(output_ratio.0),
            long_context_output_multiplier_denominator: Some(output_ratio.1),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedPaths {
    pub config_dir: PathBuf,
    pub data_dir: PathBuf,
    pub cache_dir: PathBuf,
    pub state_dir: PathBuf,
    pub database: PathBuf,
    pub artifact_root: PathBuf,
    pub worktree_root: PathBuf,
    pub log_dir: PathBuf,
}

impl HarnessConfig {
    pub fn load(path: Option<&Path>) -> Result<Self, ProfileError> {
        let text = match path {
            Some(path) if path.exists() => fs::read_to_string(path)?,
            Some(path) => return Err(ProfileError::Missing(path.to_path_buf())),
            None => DEFAULT_CONFIG.to_owned(),
        };
        let mut config: Self = toml::from_str(&text)?;
        config.apply_environment();
        config.validate()?;
        Ok(config)
    }

    fn apply_environment(&mut self) {
        if let Ok(value) = env::var("HARNESS_BIND") {
            self.server.bind = value;
        }
        if let Ok(value) = env::var("HARNESS_DATA_DIR") {
            self.paths.data_dir = value;
        }
        if let Ok(value) = env::var("HARNESS_CACHE_DIR") {
            self.paths.cache_dir = value;
        }
        if let Ok(value) = env::var("HARNESS_WORKTREE_ROOT") {
            self.paths.worktree_root = value;
        }
        if let Ok(value) = env::var("HARNESS_CODEX_BINARY") {
            self.codex.binary = value;
        }
    }

    pub fn validate(&self) -> Result<(), ProfileError> {
        if self.schema_version != 1 {
            return Err(ProfileError::Validation(format!(
                "unsupported config schema version {}",
                self.schema_version
            )));
        }
        if self.server.ui_event_replay_limit == 0 || self.server.ui_event_replay_limit > 100_000 {
            return Err(ProfileError::Validation(
                "ui_event_replay_limit must be between 1 and 100,000".to_owned(),
            ));
        }
        let host = self
            .server
            .bind
            .rsplit_once(':')
            .map_or(self.server.bind.as_str(), |x| x.0);
        if self.security.bind_localhost_only
            && !matches!(
                host.trim_matches(['[', ']']),
                "127.0.0.1" | "localhost" | "::1"
            )
        {
            return Err(ProfileError::Validation(
                "v1 refuses a non-loopback bind address".to_owned(),
            ));
        }
        if self.security.allow_agent_full_access {
            return Err(ProfileError::Validation(
                "allow_agent_full_access is forbidden by the v1 policy".to_owned(),
            ));
        }
        if self.git.auto_merge
            || self.git.auto_push
            || self.git.auto_create_pr
            || self.git.auto_mark_pr_ready
            || self.security.allow_automatic_external_writes
        {
            return Err(ProfileError::Validation(
                "automatic external writes and merge are forbidden in v1".to_owned(),
            ));
        }
        if !self.git.primary_checkout_must_remain_clean || !self.git.preserve_failed_worktrees {
            return Err(ProfileError::Validation(
                "v1 requires a clean primary checkout and preservation of failed worktrees"
                    .to_owned(),
            ));
        }
        if self.security.network_default != "disabled" {
            return Err(ProfileError::Validation(
                "v1 requires network_default = disabled".to_owned(),
            ));
        }
        if !self.security.redact_environment_values || !self.security.redact_probable_secrets {
            return Err(ProfileError::Validation(
                "v1 requires environment and probable-secret redaction".to_owned(),
            ));
        }
        if self.storage.hash_algorithm != "sha256" {
            return Err(ProfileError::Validation(
                "v1 supports only sha256 artifact and event custody".to_owned(),
            ));
        }
        if !self.usage.label_subscription_costs_as_api_equivalent {
            return Err(ProfileError::Validation(
                "subscription usage must be labeled as API-equivalent cost".to_owned(),
            ));
        }
        if self.codex.transport != "stdio" {
            return Err(ProfileError::Validation(
                "only Codex App Server stdio transport is supported".to_owned(),
            ));
        }
        if !matches!(
            self.security.approval_policy.as_str(),
            "untrusted" | "on-request" | "never"
        ) {
            return Err(ProfileError::Validation(
                "approval_policy must match the pinned App Server values: untrusted, on-request, or never"
                    .to_owned(),
            ));
        }
        if self.orchestration.max_total_agent_threads == 0
            || self.orchestration.max_mutable_tasks == 0
            || self.orchestration.max_independent_verifiers == 0
        {
            return Err(ProfileError::Validation(
                "total, mutable, and verifier capacities must be non-zero".to_owned(),
            ));
        }
        if self.orchestration.max_mutable_tasks > self.orchestration.max_total_agent_threads {
            return Err(ProfileError::Validation(
                "mutable task capacity exceeds total thread capacity".to_owned(),
            ));
        }
        if self.orchestration.max_independent_verifiers > self.orchestration.max_total_agent_threads
        {
            return Err(ProfileError::Validation(
                "verifier capacity exceeds total thread capacity".to_owned(),
            ));
        }
        if self
            .orchestration
            .automatic_plan_approval_max_execution_tokens
            == 0
        {
            return Err(ProfileError::Validation(
                "automatic plan approval execution threshold must be non-zero".to_owned(),
            ));
        }
        if self.orchestration.heartbeat_interval_seconds == 0
            || self.orchestration.lease_ttl_seconds <= self.orchestration.heartbeat_interval_seconds
        {
            return Err(ProfileError::Validation(
                "lease TTL must be greater than the non-zero heartbeat interval".to_owned(),
            ));
        }
        for snapshot in &self.pricing.snapshots {
            snapshot.to_domain()?;
        }
        Ok(())
    }

    #[must_use]
    pub fn self_improvement_runtime_status(&self) -> ImprovementRuntimeStatus {
        let calculated = hex::encode(Sha256::digest(FROZEN_SAFETY_ANCHOR.as_bytes()));
        let anchor_match = calculated == FROZEN_SAFETY_ANCHOR_SHA256
            && self.self_improvement.safety_anchor_sha256 == FROZEN_SAFETY_ANCHOR_SHA256;
        ImprovementRuntimeStatus::from_config(
            self.self_improvement.mode,
            FROZEN_SAFETY_ANCHOR_SHA256,
            &self.self_improvement.safety_anchor_sha256,
            anchor_match,
        )
    }

    pub fn resolve_paths(&self) -> Result<ResolvedPaths, ProfileError> {
        let home = env::var_os("HOME")
            .map(PathBuf::from)
            .ok_or(ProfileError::MissingHome)?;
        let config_base = env_path("XDG_CONFIG_HOME").unwrap_or_else(|| home.join(".config"));
        let data_base = env_path("XDG_DATA_HOME").unwrap_or_else(|| home.join(".local/share"));
        let cache_base = env_path("XDG_CACHE_HOME").unwrap_or_else(|| home.join(".cache"));
        let state_base = env_path("XDG_STATE_HOME").unwrap_or_else(|| home.join(".local/state"));

        let config_dir = configured_or(&self.paths.config_dir, config_base.join("harness-console"));
        let data_dir = configured_or(&self.paths.data_dir, data_base.join("harness-console"));
        let cache_dir = configured_or(&self.paths.cache_dir, cache_base.join("harness-console"));
        let state_dir = state_base.join("harness-console");
        let artifact_root =
            configured_or(&self.paths.artifact_root, data_dir.join("artifacts/sha256"));
        let worktree_root = configured_or(&self.paths.worktree_root, data_dir.join("worktrees"));
        Ok(ResolvedPaths {
            database: data_dir.join("harness.sqlite3"),
            log_dir: state_dir.join("logs"),
            config_dir,
            data_dir,
            cache_dir,
            state_dir,
            artifact_root,
            worktree_root,
        })
    }
}

impl ResolvedPaths {
    pub fn create_securely(&self) -> Result<(), ProfileError> {
        for path in [
            &self.config_dir,
            &self.data_dir,
            &self.cache_dir,
            &self.state_dir,
            &self.artifact_root,
            &self.worktree_root,
            &self.log_dir,
        ] {
            fs::create_dir_all(path)?;
            fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
        }
        Ok(())
    }
}

fn env_path(name: &str) -> Option<PathBuf> {
    env::var_os(name)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

fn configured_or(value: &str, fallback: PathBuf) -> PathBuf {
    if value.trim().is_empty() {
        fallback
    } else {
        PathBuf::from(value)
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RepositoryProfile {
    pub schema_version: u32,
    pub profile_id: String,
    pub display_name: String,
    pub repository: String,
    pub default_branch: String,
    pub base_ref: String,
    pub completion_authority: String,
    pub instruction_sources: Vec<String>,
    pub required_global_authorities: Vec<String>,
    pub protected_semantics: Vec<String>,
    pub serial_paths: Vec<String>,
    pub forbidden_generated_runtime_paths: Vec<String>,
    pub concurrency: ProfileConcurrency,
    pub models: ProfileModels,
    pub domains: Vec<DomainRule>,
    #[serde(default)]
    pub validation_policy: ValidationPolicy,
    pub validators: Vec<ValidatorRule>,
    #[serde(default)]
    pub acceptance: Vec<AcceptanceRule>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileConcurrency {
    pub max_total_agent_threads: u32,
    pub max_mutable_tasks: u32,
    pub max_independent_verifiers: u32,
    pub final_integration_mutable_tasks: u32,
    pub live_certification_mutable_tasks: u32,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileModels {
    pub architect: ModelRoute,
    pub governor: ModelRoute,
    pub explorer: ModelRoute,
    pub worker: ModelRoute,
    pub worker_escalation: ModelRoute,
    pub integrator: ModelRoute,
    pub verifier: ModelRoute,
    pub final_auditor: ModelRoute,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ModelRoute {
    pub model: String,
    pub reasoning_effort: String,
    pub sandbox: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DomainRule {
    pub id: String,
    pub globs: Vec<String>,
    pub authority_hints: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ValidationPolicy {
    #[serde(default)]
    pub review_ready: bool,
    #[serde(default = "default_true")]
    pub integration: bool,
    #[serde(default = "default_true")]
    pub require_behavioral_integration: bool,
    #[serde(default = "default_all_paths")]
    pub behavioral_required_globs: Vec<String>,
    #[serde(default = "default_true")]
    pub require_human_signoff: bool,
    #[serde(default)]
    pub require_draft_pr_ci: bool,
}

impl Default for ValidationPolicy {
    fn default() -> Self {
        Self {
            review_ready: false,
            integration: true,
            require_behavioral_integration: true,
            behavioral_required_globs: default_all_paths(),
            require_human_signoff: true,
            require_draft_pr_ci: false,
        }
    }
}

fn default_true() -> bool {
    true
}

fn default_all_paths() -> Vec<String> {
    vec!["**".to_owned()]
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationGate {
    ReviewReady,
    Integration,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidatorEvidenceClass {
    #[default]
    Custody,
    Contract,
    Behavioral,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AcceptanceKind {
    Automated,
    Attested,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptanceRule {
    pub id: String,
    pub kind: AcceptanceKind,
    #[serde(default)]
    pub command: Vec<String>,
    #[serde(default)]
    pub instructions: String,
    pub proof_tier: String,
    pub resource_class: String,
    #[serde(default)]
    pub path_globs: Vec<String>,
}

impl AcceptanceRule {
    #[must_use]
    pub fn class(&self) -> ResourceClass {
        match self.resource_class.as_str() {
            "control" => ResourceClass::Control,
            "medium" => ResourceClass::Medium,
            "heavy" => ResourceClass::Heavy,
            other => ResourceClass::Hardware(other.to_owned()),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ValidatorRule {
    pub id: String,
    pub command: Vec<String>,
    pub proof_tier: String,
    pub resource_class: String,
    #[serde(default)]
    pub manual_prerequisites: bool,
    #[serde(default)]
    pub path_globs: Vec<String>,
    #[serde(default)]
    pub gates: Vec<ValidationGate>,
    #[serde(default)]
    pub evidence_class: ValidatorEvidenceClass,
}

impl ValidatorRule {
    #[must_use]
    pub fn class(&self) -> ResourceClass {
        match self.resource_class.as_str() {
            "control" => ResourceClass::Control,
            "medium" => ResourceClass::Medium,
            "heavy" => ResourceClass::Heavy,
            other => ResourceClass::Hardware(other.to_owned()),
        }
    }
}

#[derive(Clone, Debug)]
pub struct LoadedProfile {
    pub profile: RepositoryProfile,
    pub source: PathBuf,
    pub digest: String,
}

pub fn load_profile(id_or_path: &str, config_dir: &Path) -> Result<LoadedProfile, ProfileError> {
    let (source, text) = if id_or_path == "general" {
        let installed = config_dir.join("profiles/general.toml");
        if installed.exists() {
            (installed.clone(), fs::read_to_string(installed)?)
        } else {
            (PathBuf::from("builtin:general"), GENERAL_PROFILE.to_owned())
        }
    } else if id_or_path == "bildr" {
        let installed = config_dir.join("profiles/bildr.toml");
        if installed.exists() {
            (installed.clone(), fs::read_to_string(installed)?)
        } else {
            (PathBuf::from("builtin:bildr"), BILDR_PROFILE.to_owned())
        }
    } else {
        let requested = PathBuf::from(id_or_path);
        let installed = profile_id_is_safe(id_or_path).then(|| {
            config_dir
                .join("profiles")
                .join(format!("{id_or_path}.toml"))
        });
        let source = installed
            .as_ref()
            .filter(|path| path.exists())
            .cloned()
            .or_else(|| requested.exists().then_some(requested))
            .ok_or_else(|| {
                installed.map_or_else(
                    || ProfileError::Missing(PathBuf::from(id_or_path)),
                    ProfileError::Missing,
                )
            })?;
        let text = fs::read_to_string(&source)?;
        (source, text)
    };
    let profile: RepositoryProfile = toml::from_str(&text)?;
    validate_profile(&profile)?;
    let digest = hex_digest(text.as_bytes());
    Ok(LoadedProfile {
        profile,
        source,
        digest,
    })
}

fn profile_id_is_safe(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn validate_profile(profile: &RepositoryProfile) -> Result<(), ProfileError> {
    if profile.schema_version != 1 {
        return Err(ProfileError::Validation(format!(
            "unsupported profile schema version {}",
            profile.schema_version
        )));
    }
    if profile.profile_id.trim().is_empty() {
        return Err(ProfileError::Validation(
            "profile id is required".to_owned(),
        ));
    }
    if !profile
        .forbidden_generated_runtime_paths
        .iter()
        .any(|path| path.starts_with(".harness-runtime"))
    {
        return Err(ProfileError::Validation(
            "profile must forbid repository-local Harness runtime state".to_owned(),
        ));
    }
    if !profile.validation_policy.integration {
        return Err(ProfileError::Validation(
            "integrated-head validation cannot be disabled".to_owned(),
        ));
    }
    if !profile.validation_policy.require_behavioral_integration {
        return Err(ProfileError::Validation(
            "behavioral integration proof cannot be disabled".to_owned(),
        ));
    }
    if !profile.validation_policy.require_human_signoff {
        return Err(ProfileError::Validation(
            "final human signoff cannot be disabled".to_owned(),
        ));
    }
    if profile
        .validation_policy
        .behavioral_required_globs
        .is_empty()
    {
        return Err(ProfileError::Validation(
            "behavioral_required_globs cannot be empty when behavioral integration proof is required"
                .to_owned(),
        ));
    }
    let mut validator_ids = BTreeMap::new();
    let mut integration_validator_declared = false;
    let mut review_ready_validator_declared = false;
    for validator in &profile.validators {
        if validator.id.trim().is_empty() {
            return Err(ProfileError::Validation(
                "validator id is required".to_owned(),
            ));
        }
        if validator.command.is_empty() || validator.command.iter().any(|part| part.is_empty()) {
            return Err(ProfileError::Validation(format!(
                "validator {} must have a non-empty command",
                validator.id
            )));
        }
        if !matches!(
            validator.proof_tier.as_str(),
            "T0" | "T1" | "T2" | "T3" | "T4" | "T5" | "T6"
        ) {
            return Err(ProfileError::Validation(format!(
                "validator {} has unknown proof tier {}",
                validator.id, validator.proof_tier
            )));
        }
        if validator_ids.insert(&validator.id, ()).is_some() {
            return Err(ProfileError::Validation(format!(
                "duplicate validator id {}",
                validator.id
            )));
        }
        let unique_gates = validator.gates.iter().copied().collect::<BTreeSet<_>>();
        if unique_gates.len() != validator.gates.len() {
            return Err(ProfileError::Validation(format!(
                "validator {} declares a lifecycle gate more than once",
                validator.id
            )));
        }
        if validator.manual_prerequisites && !validator.gates.is_empty() {
            return Err(ProfileError::Validation(format!(
                "validator {} cannot bind a manual-prerequisite command to an automatic lifecycle gate",
                validator.id
            )));
        }
        integration_validator_declared |= validator.gates.contains(&ValidationGate::Integration);
        review_ready_validator_declared |= validator.gates.contains(&ValidationGate::ReviewReady);
    }
    if !integration_validator_declared {
        return Err(ProfileError::Validation(
            "at least one integrated-head validator is required".to_owned(),
        ));
    }
    if profile.validation_policy.review_ready && !review_ready_validator_declared {
        return Err(ProfileError::Validation(
            "review_ready is enabled but no validator is bound to that gate".to_owned(),
        ));
    }
    for acceptance in &profile.acceptance {
        if acceptance.id.trim().is_empty()
            || validator_ids.insert(&acceptance.id, ()).is_some()
            || !matches!(
                acceptance.proof_tier.as_str(),
                "T0" | "T1" | "T2" | "T3" | "T4" | "T5" | "T6"
            )
        {
            return Err(ProfileError::Validation(format!(
                "acceptance id {} must be unique and use a known proof tier",
                acceptance.id
            )));
        }
        if acceptance.instructions.trim().is_empty() {
            return Err(ProfileError::Validation(format!(
                "acceptance {} needs operator-facing proof instructions",
                acceptance.id
            )));
        }
        match acceptance.kind {
            AcceptanceKind::Automated
                if acceptance.command.is_empty()
                    || acceptance.command.iter().any(|part| part.trim().is_empty()) =>
            {
                return Err(ProfileError::Validation(format!(
                    "automated acceptance {} needs a command",
                    acceptance.id
                )));
            }
            _ => {}
        }
    }
    Ok(())
}

#[must_use]
pub fn hex_digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

#[must_use]
pub fn redact_diagnostic(input: &str) -> String {
    const KEYS: &[&str] = &[
        "token",
        "secret",
        "password",
        "passwd",
        "api_key",
        "apikey",
        "authorization",
        "cookie",
        "credential",
    ];
    input
        .lines()
        .map(|line| {
            let lower = line.to_ascii_lowercase();
            if KEYS.iter().any(|key| lower.contains(key)) {
                let key = line.split(['=', ':']).next().unwrap_or("value").trim();
                format!("{key}=<redacted>")
            } else {
                line.to_owned()
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

#[must_use]
pub fn profile_model_map(profile: &RepositoryProfile) -> BTreeMap<&'static str, &ModelRoute> {
    BTreeMap::from([
        ("architect", &profile.models.architect),
        ("governor", &profile.models.governor),
        ("explorer", &profile.models.explorer),
        ("worker", &profile.models.worker),
        ("worker_escalation", &profile.models.worker_escalation),
        ("integrator", &profile.models.integrator),
        ("verifier", &profile.models.verifier),
        ("final_auditor", &profile.models.final_auditor),
    ])
}

#[derive(Debug, Error)]
pub enum ProfileError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("TOML error: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("missing configuration file: {0}")]
    Missing(PathBuf),
    #[error("HOME is not defined")]
    MissingHome,
    #[error("configuration validation failed: {0}")]
    Validation(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn supplied_config_and_profile_parse() {
        let config: HarnessConfig = toml::from_str(DEFAULT_CONFIG).expect("config parses");
        config.validate().expect("config validates");
        let profile: RepositoryProfile = toml::from_str(BILDR_PROFILE).expect("profile parses");
        validate_profile(&profile).expect("profile validates");
        assert_eq!(profile.models.governor.model, "gpt-5.6-sol");
        assert_eq!(profile.models.governor.reasoning_effort, "xhigh");
        assert_eq!(profile.models.governor.sandbox, "workspace-write");
        assert!(profile.validation_policy.integration);
        assert!(profile.validation_policy.require_behavioral_integration);
        assert!(profile.validation_policy.require_human_signoff);
        assert!(profile.validation_policy.require_draft_pr_ci);
        assert!(profile.validators.iter().any(|validator| {
            validator.gates.contains(&ValidationGate::Integration)
                && validator.evidence_class == ValidatorEvidenceClass::Behavioral
        }));
        assert!(
            profile
                .acceptance
                .iter()
                .any(|acceptance| acceptance.kind == AcceptanceKind::Automated)
        );

        let general: RepositoryProfile =
            toml::from_str(GENERAL_PROFILE).expect("general profile parses");
        validate_profile(&general).expect("general profile validates");
        assert_eq!(general.repository, "*");
        assert!(!general.validation_policy.require_draft_pr_ci);
        let improvement = config.self_improvement_runtime_status();
        assert_eq!(improvement.effective_mode, ImprovementMode::Disabled);
        assert!(improvement.anchor_match);
        assert!(!improvement.candidate_generation_enabled);
        assert!(!improvement.candidate_execution_enabled);
    }

    #[test]
    fn frozen_anchor_digest_is_reproducible_and_mismatch_fails_closed() {
        assert_eq!(
            hex::encode(Sha256::digest(FROZEN_SAFETY_ANCHOR.as_bytes())),
            FROZEN_SAFETY_ANCHOR_SHA256
        );
        let mut config: HarnessConfig = toml::from_str(DEFAULT_CONFIG).expect("config parses");
        config.self_improvement.mode = ImprovementMode::ObserveOnly;
        config.self_improvement.safety_anchor_sha256 = "not-a-digest".to_owned();
        let status = config.self_improvement_runtime_status();
        assert_eq!(status.effective_mode, ImprovementMode::Disabled);
        assert!(!status.anchor_match);
        assert!(!status.observation_enabled);
        assert_eq!(status.configured_anchor_sha256, "not-a-digest");
    }

    #[test]
    fn unsupported_improvement_modes_cannot_enable_future_capabilities() {
        let unsupported =
            DEFAULT_CONFIG.replace("mode = \"disabled\"", "mode = \"shadow_allowed\"");
        assert!(toml::from_str::<HarnessConfig>(&unsupported).is_err());
    }

    #[test]
    fn custom_profile_ids_resolve_from_local_configuration() {
        let config = TempDir::new().expect("temporary config directory");
        let profiles = config.path().join("profiles");
        fs::create_dir(&profiles).expect("profile directory");
        let source = profiles.join("custom.toml");
        fs::write(
            &source,
            GENERAL_PROFILE.replace("profile_id = \"general\"", "profile_id = \"custom\""),
        )
        .expect("custom profile");

        let loaded = load_profile("custom", config.path()).expect("custom profile loads");
        assert_eq!(loaded.source, source);
        assert_eq!(loaded.profile.profile_id, "custom");

        assert!(matches!(
            load_profile("missing", config.path()),
            Err(ProfileError::Missing(path)) if path == profiles.join("missing.toml")
        ));
    }

    #[test]
    fn redaction_hides_probable_secrets() {
        let redacted = redact_diagnostic("OPENAI_API_KEY=abc\nnormal=value\npassword: hunter2");
        assert!(!redacted.contains("abc"));
        assert!(!redacted.contains("hunter2"));
        assert!(redacted.contains("normal=value"));
    }

    #[test]
    fn unsafe_bind_is_rejected() {
        let mut config: HarnessConfig = toml::from_str(DEFAULT_CONFIG).expect("config parses");
        config.server.bind = "0.0.0.0:7310".to_owned();
        assert!(config.validate().is_err());
    }

    #[test]
    fn invalid_scheduler_capacity_and_lease_timing_are_rejected() {
        let mut config: HarnessConfig = toml::from_str(DEFAULT_CONFIG).expect("config parses");
        config.orchestration.max_independent_verifiers = config
            .orchestration
            .max_total_agent_threads
            .saturating_add(1);
        assert!(config.validate().is_err());

        let mut config: HarnessConfig = toml::from_str(DEFAULT_CONFIG).expect("config parses");
        config.orchestration.lease_ttl_seconds = config.orchestration.heartbeat_interval_seconds;
        assert!(config.validate().is_err());
    }

    #[test]
    fn safety_contract_cannot_be_disabled_by_configuration() {
        let mut config: HarnessConfig = toml::from_str(DEFAULT_CONFIG).expect("config parses");
        config.security.network_default = "enabled".to_owned();
        assert!(config.validate().is_err());

        let mut config: HarnessConfig = toml::from_str(DEFAULT_CONFIG).expect("config parses");
        config.git.preserve_failed_worktrees = false;
        assert!(config.validate().is_err());

        let mut config: HarnessConfig = toml::from_str(DEFAULT_CONFIG).expect("config parses");
        config.storage.hash_algorithm = "sha1".to_owned();
        assert!(config.validate().is_err());
    }

    #[test]
    fn automatic_validator_gates_reject_manual_or_missing_commands() {
        let mut profile: RepositoryProfile = toml::from_str(BILDR_PROFILE).expect("profile parses");
        profile.validators[0].manual_prerequisites = true;
        assert!(validate_profile(&profile).is_err());

        let mut profile: RepositoryProfile = toml::from_str(BILDR_PROFILE).expect("profile parses");
        profile.validation_policy.review_ready = true;
        assert!(validate_profile(&profile).is_err());
    }
}

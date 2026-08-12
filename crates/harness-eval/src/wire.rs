use crate::{DigestError, Split, canonical_digest_without_self, hash, sha40, token};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use std::collections::BTreeSet;
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvalCaseV1 {
    pub schema: String,
    pub case_id: String,
    pub revision: u64,
    pub title: String,
    pub task_family: String,
    pub objective: String,
    pub source: CaseSource,
    pub split: Split,
    pub runtime: CaseRuntime,
    pub custody: CaseCustody,
    pub acceptance: Vec<AcceptanceClaim>,
    pub grader_bundle_id: String,
    pub grader_bundle_revision: u64,
    pub grader_bundle_digest: String,
    pub privacy: CasePrivacy,
    #[serde(default, deserialize_with = "optional_leakage_status")]
    pub leakage_status: Option<LeakageStatus>,
    pub sha256: String,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaseSource {
    pub kind: CaseSourceKind,
    pub locator: String,
    pub digest: String,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CaseSourceKind {
    Curated,
    ProductionFailure,
    Regression,
    Synthetic,
    External,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaseRuntime {
    pub repository_fixture: String,
    pub base_sha: String,
    pub setup_digest: String,
    pub resource_class: String,
    pub timeout_seconds: u64,
    pub token_budget: u64,
    #[serde(default, deserialize_with = "optional_seeds")]
    pub seeds: Option<Vec<u64>>,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CaseCustody {
    pub owned_paths: Vec<String>,
    pub forbidden_paths: Vec<String>,
    pub grader_isolated: bool,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AcceptanceClaim {
    pub claim_id: String,
    pub kind: AcceptanceKind,
    pub required: bool,
    #[serde(default, deserialize_with = "optional_object")]
    pub spec: Option<Value>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AcceptanceKind {
    Command,
    State,
    Artifact,
    SideEffect,
    HumanRubric,
    DelayedOutcome,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CasePrivacy {
    pub classification: PrivacyClass,
    pub export_allowed: bool,
    #[serde(deserialize_with = "required_nullable_string")]
    pub license: Option<String>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PrivacyClass {
    Public,
    Internal,
    Confidential,
    Restricted,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeakageStatus {
    Clean,
    Suspected,
    Confirmed,
    NotApplicable,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CasePin {
    pub case_id: String,
    pub revision: u64,
    pub split: Split,
    pub case_digest: String,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TasksetV1 {
    pub schema: String,
    pub taskset_id: String,
    pub revision: u64,
    pub cases: Vec<CasePin>,
    pub sha256: String,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EvalSampleV1 {
    pub schema: String,
    pub sample_id: String,
    pub case_id: String,
    pub case_revision: u64,
    pub case_digest: String,
    pub taskset_digest: String,
    pub grader_bundle_digest: String,
    pub policy_digest: String,
    pub base_sha: String,
    pub fixture_digest: String,
    pub setup_digest: String,
    pub runtime_digest: String,
    pub isolation: IsolationCapability,
    pub command_digest: String,
    pub classification: SampleClassification,
    #[serde(deserialize_with = "required_nullable_digest")]
    pub trace_digest: NullableDigest,
    #[serde(deserialize_with = "required_nullable_digest")]
    pub evidence_digest: NullableDigest,
    #[serde(deserialize_with = "required_nullable_digest")]
    pub artifact_digest: NullableDigest,
    #[serde(deserialize_with = "required_nullable_digest")]
    pub cost_receipt_digest: NullableDigest,
    pub seed: u64,
    pub sha256: String,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SampleClassification {
    Pass,
    Fail,
    InfrastructureUnavailable,
    Invalidated,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum IsolationCapability {
    Available,
    InfrastructureUnavailable,
}
/// A nullable receipt which remains a required wire property.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NullableDigest(pub Option<String>);

fn required_nullable_digest<'de, D>(deserializer: D) -> Result<NullableDigest, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(NullableDigest(Option::<String>::deserialize(deserializer)?))
}

fn required_nullable_string<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<String>::deserialize(deserializer)
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraderSignal {
    pub id: String,
    pub kind: GraderKind,
    pub direction: SignalDirection,
    pub weight: f64,
    pub required: bool,
    pub definition_digest: String,
    pub calibration_set_digest: Option<String>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraderKind {
    Deterministic,
    StateEffect,
    ModelRubric,
    Human,
    Delayed,
    Cost,
    Latency,
    Safety,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SignalDirection {
    Maximize,
    Minimize,
    BooleanPass,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct NegativeControl {
    pub id: String,
    pub signal_id: String,
    pub expected_relationship: String,
    pub failure_action: NegativeControlAction,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NegativeControlAction {
    Block,
    Invalidate,
    Adjudicate,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraderIsolation {
    pub candidate_write_access: bool,
    pub holdout_answer_access: bool,
    pub grader_runtime: GraderRuntime,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraderRuntime {
    SeparateProcess,
    SeparateContainer,
    SeparateHost,
}
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GraderBundleV1 {
    pub schema: String,
    pub grader_bundle_id: String,
    pub revision: u64,
    pub signals: Vec<GraderSignal>,
    pub hard_gates: Vec<String>,
    pub negative_controls: Vec<NegativeControl>,
    pub reward_integrity_required: bool,
    pub isolation: GraderIsolation,
    pub sha256: String,
}

fn optional_object<'de, D>(deserializer: D) -> Result<Option<Value>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<Value>::deserialize(deserializer)?;
    match value {
        Some(value @ Value::Object(_)) => Ok(Some(value)),
        Some(_) | None => Err(serde::de::Error::custom(
            "acceptance spec must be an object when present",
        )),
    }
}

fn optional_seeds<'de, D>(deserializer: D) -> Result<Option<Vec<u64>>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<Vec<u64>>::deserialize(deserializer)?
        .ok_or_else(|| serde::de::Error::custom("runtime seeds must be an array when present"))
        .map(Some)
}

fn optional_leakage_status<'de, D>(deserializer: D) -> Result<Option<LeakageStatus>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<LeakageStatus>::deserialize(deserializer)?
        .ok_or_else(|| {
            serde::de::Error::custom("leakage_status must be a closed value when present")
        })
        .map(Some)
}
pub fn runner_isolation(c: IsolationCapability) -> Option<SampleClassification> {
    match c {
        IsolationCapability::Available => None,
        IsolationCapability::InfrastructureUnavailable => {
            Some(SampleClassification::InfrastructureUnavailable)
        }
    }
}
pub fn verify_case_v1(v: &EvalCaseV1) -> Result<(), DigestError> {
    (v.schema == "harness.eval-case.v1"
        && token(&v.case_id)
        && v.revision > 0
        && !v.title.is_empty()
        && !v.task_family.is_empty()
        && !v.objective.is_empty()
        && token(&v.grader_bundle_id)
        && v.grader_bundle_revision > 0
        && hash(&v.grader_bundle_digest)
        && !v.source.locator.is_empty()
        && hash(&v.source.digest)
        && !v.runtime.repository_fixture.is_empty()
        && sha40(&v.runtime.base_sha)
        && hash(&v.runtime.setup_digest)
        && !v.runtime.resource_class.is_empty()
        && v.runtime.timeout_seconds > 0
        && v.runtime.token_budget > 0
        && v.custody.grader_isolated
        && v.custody.owned_paths.iter().all(|path| !path.is_empty())
        && v.custody
            .forbidden_paths
            .iter()
            .all(|path| !path.is_empty())
        && v.runtime.seeds.as_ref().is_none_or(|seeds| {
            !seeds.is_empty() && seeds.iter().collect::<BTreeSet<_>>().len() == seeds.len()
        })
        && !v.acceptance.is_empty()
        && v.acceptance
            .iter()
            .all(|a| token(&a.claim_id) && a.spec.as_ref().is_none_or(Value::is_object))
        && canonical_digest_without_self(v)? == v.sha256)
        .then_some(())
        .ok_or(DigestError::DigestMismatch)
}
pub fn verify_taskset_v1(v: &TasksetV1) -> Result<(), DigestError> {
    (v.schema == "harness.taskset.v1"
        && token(&v.taskset_id)
        && v.revision > 0
        && !v.cases.is_empty()
        && v.cases
            .iter()
            .all(|c| token(&c.case_id) && c.revision > 0 && hash(&c.case_digest))
        && canonical_digest_without_self(v)? == v.sha256)
        .then_some(())
        .ok_or(DigestError::DigestMismatch)
}
pub fn verify_sample(v: &EvalSampleV1) -> Result<(), DigestError> {
    let nullable = [
        &v.trace_digest.0,
        &v.evidence_digest.0,
        &v.artifact_digest.0,
        &v.cost_receipt_digest.0,
    ];
    (v.schema == "harness.eval-sample.v1"
        && token(&v.sample_id)
        && token(&v.case_id)
        && v.case_revision > 0
        && [
            &v.case_digest,
            &v.taskset_digest,
            &v.grader_bundle_digest,
            &v.policy_digest,
            &v.fixture_digest,
            &v.setup_digest,
            &v.runtime_digest,
            &v.command_digest,
        ]
        .iter()
        .all(|x| hash(x))
        && sha40(&v.base_sha)
        && nullable.iter().all(|x| x.as_deref().is_none_or(hash))
        && runner_isolation(v.isolation).is_none_or(|x| x == v.classification)
        && canonical_digest_without_self(v)? == v.sha256)
        .then_some(())
        .ok_or(DigestError::DigestMismatch)
}
pub fn verify_grader_contract(v: &GraderBundleV1) -> Result<(), DigestError> {
    (v.schema == "harness.grader-bundle.v1"
        && token(&v.grader_bundle_id)
        && v.revision > 0
        && !v.signals.is_empty()
        && !v.hard_gates.is_empty()
        && !v.negative_controls.is_empty()
        && v.signals.iter().all(|s| {
            token(&s.id)
                && hash(&s.definition_digest)
                && s.calibration_set_digest.as_deref().is_none_or(hash)
                && s.weight.is_finite()
                && s.weight >= 0.0
        })
        && v.signals
            .iter()
            .map(|signal| &signal.id)
            .collect::<BTreeSet<_>>()
            .len()
            == v.signals.len()
        && v.hard_gates
            .iter()
            .all(|id| v.signals.iter().any(|s| s.id == *id))
        && v.hard_gates.iter().collect::<BTreeSet<_>>().len() == v.hard_gates.len()
        && v.hard_gates.iter().any(|id| {
            v.signals.iter().any(|s| {
                s.id == *id
                    && s.required
                    && matches!(s.kind, GraderKind::Deterministic | GraderKind::StateEffect)
            })
        })
        && v.negative_controls.iter().all(|c| {
            token(&c.id)
                && v.signals.iter().any(|s| s.id == c.signal_id)
                && !c.expected_relationship.is_empty()
        })
        && v.reward_integrity_required
        && !v.isolation.candidate_write_access
        && !v.isolation.holdout_answer_access
        && canonical_digest_without_self(v)? == v.sha256)
        .then_some(())
        .ok_or(DigestError::DigestMismatch)
}

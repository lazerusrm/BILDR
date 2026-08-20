//! Pure M3 learning contracts.  They are immutable display/suggestion records,
//! never execution or activation authority.

use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

/// The canonical `harness.knowledge-item.v1` and OpenAPI knowledge-token
/// ceiling. Other learning/evaluation identifiers intentionally retain their
/// narrower contract.
pub const MAX_KNOWLEDGE_TOKEN_LEN: usize = 160;

const SHA: usize = 64;
const MAX_REJECTED_SUGGESTIONS: usize = 256;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceReceipt {
    pub kind: ReceiptKind,
    pub revision_id: String,
    pub digest: String,
    pub split: Option<EvalSplit>,
    pub custody: Option<CustodyState>,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReceiptKind {
    Trace,
    Outcome,
    Failure,
    InvestigationArtifact,
    LivenessObservation,
    ReconciliationEpisode,
    EvalCase,
    Taskset,
    GraderBundle,
    Runtime,
    PolicyBundle,
    HumanReview,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvalSplit {
    Training,
    Development,
    Holdout,
    Canary,
    Quarantine,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CustodyState {
    Clean,
    Invalidated,
    Restricted,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeKind {
    Fact,
    Procedure,
    Warning,
    Heuristic,
    AntiPattern,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewState {
    Unreviewed,
    Accepted,
    Rejected,
    NeedsRevalidation,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeState {
    Candidate,
    Active,
    Expired,
    Contradicted,
    Superseded,
    Rejected,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KnowledgeReview {
    pub state: ReviewState,
    pub reviewer_id: Option<String>,
    pub reviewed_at: Option<u64>,
    pub receipt: Option<SourceReceipt>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KnowledgeFreshness {
    pub created_at: u64,
    pub revalidate_after: u64,
    pub expires_at: u64,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KnowledgeScope {
    pub repository_id: String,
    pub task_family: String,
    pub model_family: Option<String>,
    pub runtime_class: Option<String>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KnowledgeItemV1 {
    pub schema: String,
    pub knowledge_id: String,
    pub kind: KnowledgeKind,
    pub statement: String,
    pub scope: KnowledgeScope,
    pub evidence: Vec<SourceReceipt>,
    pub confidence_milli: u16,
    pub review: KnowledgeReview,
    pub freshness: KnowledgeFreshness,
    pub contradicts: Vec<String>,
    pub supersedes: Vec<String>,
    pub state: KnowledgeState,
    pub sha256: String,
}
impl KnowledgeItemV1 {
    pub fn digest(&self) -> Result<String, LearningContractError> {
        digest(self)
    }

    pub fn displayable(&self, now: u64) -> bool {
        self.verify().is_ok()
            && self.state == KnowledgeState::Active
            && self.review.state == ReviewState::Accepted
            && self.freshness.revalidate_after > now
            && self.freshness.expires_at > now
            && self.contradicts.is_empty()
    }
    pub fn verify(&self) -> Result<(), LearningContractError> {
        if self.schema != "harness.knowledge-item.v1"
            || !knowledge_token(&self.knowledge_id)
            || self.statement.is_empty()
            || !knowledge_token(&self.scope.repository_id)
            || !knowledge_token(&self.scope.task_family)
            || self
                .scope
                .model_family
                .as_deref()
                .is_some_and(|id| !knowledge_token(id))
            || self
                .scope
                .runtime_class
                .as_deref()
                .is_some_and(|id| !knowledge_token(id))
            || self.evidence.is_empty()
            || self.confidence_milli > 1000
            || self.freshness.created_at > self.freshness.revalidate_after
            || self.freshness.revalidate_after > self.freshness.expires_at
            || !unique_knowledge_tokens(&self.contradicts)
            || !unique_knowledge_tokens(&self.supersedes)
            || self.evidence.iter().any(|e| !valid_knowledge_receipt(e))
            || self
                .evidence
                .iter()
                .any(|e| e.revision_id == self.knowledge_id)
            || self.contradicts.iter().any(|id| id == &self.knowledge_id)
            || self.supersedes.iter().any(|id| id == &self.knowledge_id)
            || self
                .review
                .reviewer_id
                .as_deref()
                .is_some_and(|id| !knowledge_token(id))
            || self.review.receipt.as_ref().is_some_and(|r| {
                !valid_knowledge_receipt(r)
                    || r.kind != ReceiptKind::HumanReview
                    || r.revision_id == self.knowledge_id
            })
            || (self.state == KnowledgeState::Active
                && (self.review.state != ReviewState::Accepted
                    || self.review.reviewer_id.is_none()
                    || self.review.reviewed_at.is_none()
                    || self.review.receipt.is_none()))
            || self.digest()? != self.sha256
        {
            return Err(LearningContractError::InvalidKnowledge);
        }
        Ok(())
    }
}
/// Controller-resolved authentication, never supplied by a knowledge item.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TrustedKnowledgeResolution {
    pub human_action: SourceReceipt,
    pub evidence_clean: bool,
}
/// Store/API adapters may resolve this display projection, but it carries no
/// instruction or executable field and cannot alter active authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DisplayKnowledge {
    pub knowledge_id: String,
    pub statement: String,
    pub receipt: SourceReceipt,
}
pub fn resolve_trusted_display(
    item: &KnowledgeItemV1,
    resolution: &TrustedKnowledgeResolution,
    now: u64,
) -> Option<DisplayKnowledge> {
    (item.displayable(now)
        && resolution.evidence_clean
        && resolution.human_action.kind == ReceiptKind::HumanReview
        && item.review.receipt.as_ref() == Some(&resolution.human_action))
    .then(|| DisplayKnowledge {
        knowledge_id: item.knowledge_id.clone(),
        statement: item.statement.clone(),
        receipt: resolution.human_action.clone(),
    })
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComponentDimension {
    ContextSourceOrdering,
    TokenBudget,
    ReadOnlyProbeParameters,
    RetryTiming,
    CacheSummaryPolicy,
    RolePrompts,
    PlanningContracts,
    Skills,
    ProceduralMemory,
    ModelEffortRouting,
    DelegationStrategy,
    ValidatorSelection,
    ValidatorThresholds,
    KnowledgeRetrieval,
    CompactionHandoff,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EditRisk {
    Green,
    Amber,
}
impl ComponentDimension {
    pub const fn risk(self) -> EditRisk {
        match self {
            Self::ContextSourceOrdering
            | Self::TokenBudget
            | Self::ReadOnlyProbeParameters
            | Self::RetryTiming
            | Self::CacheSummaryPolicy => EditRisk::Green,
            _ => EditRisk::Amber,
        }
    }
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyComponent {
    pub dimension: ComponentDimension,
    pub manifest_digest: String,
    pub risk_class: EditRisk,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyBundleV1 {
    pub schema: String,
    pub bundle_id: String,
    pub repository_id: String,
    pub task_family: String,
    pub components: Vec<PolicyComponent>,
    pub parent_bundle_id: Option<String>,
    pub safety_anchor_digest: String,
    pub sha256: String,
}
impl PolicyBundleV1 {
    pub fn verify(&self) -> Result<(), LearningContractError> {
        if self.schema != "harness.policy-bundle.v1"
            || !token(&self.bundle_id)
            || !controller_token(&self.repository_id)
            || !token(&self.task_family)
            || !hash(&self.safety_anchor_digest)
            || self.components.is_empty()
            || self
                .components
                .iter()
                .any(|c| !hash(&c.manifest_digest) || c.risk_class != c.dimension.risk())
            || self
                .components
                .iter()
                .map(|c| c.dimension)
                .collect::<BTreeSet<_>>()
                .len()
                != self.components.len()
            || self
                .parent_bundle_id
                .as_deref()
                .is_some_and(|id| !token(id))
            || digest(self)? != self.sha256
        {
            return Err(LearningContractError::InvalidBundle);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EditOperation {
    Replace,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateEdit {
    pub dimension: ComponentDimension,
    pub risk_class: EditRisk,
    pub operation: EditOperation,
    pub before_digest: String,
    pub after_digest: String,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PredictionDirection {
    Increase,
    Decrease,
    Unchanged,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Prediction {
    pub signal_id: String,
    pub direction: PredictionDirection,
    pub minimum_delta_milli: i64,
}
/// Immutable controller scope for a candidate.  It prevents a candidate from
/// borrowing a similarly named champion or evaluation case from another
/// repository/base/runtime scope.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateScope {
    pub repository_id: String,
    pub task_family: String,
    pub model_family: Option<String>,
    pub runtime_class: Option<String>,
    pub base_sha: String,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CandidateV1 {
    pub schema: String,
    pub candidate_id: String,
    pub scope: CandidateScope,
    pub parent_bundle: SourceReceipt,
    pub target_failure: SourceReceipt,
    pub development_case: SourceReceipt,
    pub no_change_control: SourceReceipt,
    pub taskset: SourceReceipt,
    pub grader_bundle: SourceReceipt,
    pub runtime: SourceReceipt,
    pub hypothesis: String,
    pub edit: CandidateEdit,
    pub predictions: Vec<Prediction>,
    pub evidence: Vec<SourceReceipt>,
    pub rollback_bundle: SourceReceipt,
    pub state: CandidateState,
    pub sha256: String,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CandidateState {
    Proposed,
    Rejected,
    Superseded,
}
impl CandidateV1 {
    pub fn verify(&self) -> Result<(), LearningContractError> {
        let pins = [
            &self.parent_bundle,
            &self.target_failure,
            &self.development_case,
            &self.no_change_control,
            &self.taskset,
            &self.grader_bundle,
            &self.runtime,
            &self.rollback_bundle,
        ];
        if self.schema != "harness.improvement-candidate.v1"
            || !token(&self.candidate_id)
            || !controller_token(&self.scope.repository_id)
            || !token(&self.scope.task_family)
            || self
                .scope
                .model_family
                .as_deref()
                .is_some_and(|v| !token(v))
            || self
                .scope
                .runtime_class
                .as_deref()
                .is_some_and(|v| !token(v))
            || !hash40(&self.scope.base_sha)
            || self.hypothesis.is_empty()
            || self.predictions.is_empty()
            || self.evidence.is_empty()
            || self.edit.risk_class != self.edit.dimension.risk()
            || !hash(&self.edit.before_digest)
            || !hash(&self.edit.after_digest)
            || self.edit.before_digest == self.edit.after_digest
            || self.predictions.iter().any(|p| !token(&p.signal_id))
            || self
                .predictions
                .iter()
                .map(|p| &p.signal_id)
                .collect::<BTreeSet<_>>()
                .len()
                != self.predictions.len()
            || self.evidence.iter().any(|e| !valid_receipt(e))
            || pins.iter().any(|r| !valid_receipt(r))
            || self.parent_bundle.kind != ReceiptKind::PolicyBundle
            || self.rollback_bundle.kind != ReceiptKind::PolicyBundle
            || self.target_failure.kind != ReceiptKind::Failure
            || self.development_case.kind != ReceiptKind::EvalCase
            || !clean_development(&self.development_case)
            || self.no_change_control.kind != ReceiptKind::EvalCase
            || !clean_development(&self.no_change_control)
            || self.no_change_control.revision_id == self.development_case.revision_id
            || self.taskset.kind != ReceiptKind::Taskset
            || self.grader_bundle.kind != ReceiptKind::GraderBundle
            || self.runtime.kind != ReceiptKind::Runtime
            || self.parent_bundle.revision_id == self.candidate_id
            || self
                .evidence
                .iter()
                .any(|e| e.revision_id == self.candidate_id)
            || digest(self)? != self.sha256
        {
            return Err(LearningContractError::InvalidCandidate);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OptimizerInput {
    pub clusters: Vec<FailureSuggestionInput>,
    pub editable: Vec<ComponentDimension>,
    pub rejected: Vec<RejectedSuggestion>,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RejectedSuggestion {
    pub failure_revision_id: String,
    pub dimension: ComponentDimension,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FailureSuggestionInput {
    pub failure: SourceReceipt,
    pub development_evidence: Vec<SourceReceipt>,
    pub no_change_control: SourceReceipt,
}
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OptimizerSuggestion {
    pub target_failure: SourceReceipt,
    pub dimension: ComponentDimension,
    pub kind: SuggestionKind,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SuggestionKind {
    Targeted,
    Alternative,
    NoChangeControl,
}
pub fn suggest_candidates(
    input: &OptimizerInput,
) -> Result<Vec<OptimizerSuggestion>, LearningContractError> {
    if input.editable.is_empty()
        || input.editable.iter().collect::<BTreeSet<_>>().len() != input.editable.len()
        || input.rejected.len() > MAX_REJECTED_SUGGESTIONS
        || input
            .rejected
            .iter()
            .any(|r| !token(&r.failure_revision_id))
        || input.clusters.iter().any(|c| {
            c.failure.kind != ReceiptKind::Failure
                || !valid_receipt(&c.failure)
                || c.failure.split.is_some()
                || matches!(
                    c.failure.custody,
                    Some(CustodyState::Invalidated | CustodyState::Restricted)
                )
                || c.development_evidence.is_empty()
                || c.development_evidence.iter().any(|r| {
                    r.kind != ReceiptKind::EvalCase || !clean_development(r) || !valid_receipt(r)
                })
                || !clean_development(&c.no_change_control)
                || c.development_evidence
                    .iter()
                    .any(|r| r.revision_id == c.no_change_control.revision_id)
        })
    {
        return Err(LearningContractError::UnsafeSuggestionInput);
    }
    let mut clusters = input.clusters.clone();
    clusters.sort_by(|a, b| a.failure.revision_id.cmp(&b.failure.revision_id));
    let mut out = Vec::new();
    if let Some(first) = clusters.first() {
        out.push(OptimizerSuggestion {
            target_failure: first.failure.clone(),
            dimension: input.editable[0],
            kind: SuggestionKind::NoChangeControl,
        });
    }
    for c in clusters {
        for dimension in &input.editable {
            if out.len() == 3 {
                break;
            }
            if input.rejected.iter().any(|r| {
                r.failure_revision_id == c.failure.revision_id && r.dimension == *dimension
            }) {
                continue;
            }
            out.push(OptimizerSuggestion {
                target_failure: c.failure.clone(),
                dimension: *dimension,
                kind: if out.len() == 1 {
                    SuggestionKind::Targeted
                } else {
                    SuggestionKind::Alternative
                },
            })
        }
        if out.len() == 3 {
            break;
        }
    }
    Ok(out)
}

fn valid_receipt(r: &SourceReceipt) -> bool {
    token(&r.revision_id) && hash(&r.digest)
}
fn valid_knowledge_receipt(r: &SourceReceipt) -> bool {
    knowledge_token(&r.revision_id) && hash(&r.digest)
}
fn clean_development(r: &SourceReceipt) -> bool {
    r.split == Some(EvalSplit::Development) && r.custody == Some(CustodyState::Clean)
}
fn token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b':'))
}
fn controller_token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 160
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b':'))
}
fn knowledge_token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_KNOWLEDGE_TOKEN_LEN
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b':'))
}
fn hash(value: &str) -> bool {
    value.len() == SHA
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || matches!(b, b'a'..=b'f'))
}
fn hash40(value: &str) -> bool {
    value.len() == 40
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || matches!(b, b'a'..=b'f'))
}
fn unique_knowledge_tokens(values: &[String]) -> bool {
    values.iter().all(|v| knowledge_token(v))
        && values.iter().collect::<BTreeSet<_>>().len() == values.len()
}
fn digest<T: Serialize>(value: &T) -> Result<String, LearningContractError> {
    let mut v = serde_json::to_value(value).map_err(|_| LearningContractError::Digest)?;
    v.as_object_mut()
        .ok_or(LearningContractError::Digest)?
        .remove("sha256");
    Ok(hex::encode(Sha256::digest(
        serde_json::to_vec(&v).map_err(|_| LearningContractError::Digest)?,
    )))
}
#[derive(Debug, Error, Eq, PartialEq)]
pub enum LearningContractError {
    #[error("invalid knowledge contract")]
    InvalidKnowledge,
    #[error("invalid champion bundle")]
    InvalidBundle,
    #[error("invalid candidate contract")]
    InvalidCandidate,
    #[error("unsafe optimizer input")]
    UnsafeSuggestionInput,
    #[error("canonical digest error")]
    Digest,
}

#[cfg(test)]
mod tests {
    use super::*;
    fn h() -> String {
        "a".repeat(64)
    }
    fn receipt(kind: ReceiptKind, id: &str) -> SourceReceipt {
        SourceReceipt {
            kind,
            revision_id: id.into(),
            digest: h(),
            split: None,
            custody: None,
        }
    }
    fn development(id: &str) -> SourceReceipt {
        SourceReceipt {
            split: Some(EvalSplit::Development),
            custody: Some(CustodyState::Clean),
            ..receipt(ReceiptKind::EvalCase, id)
        }
    }
    fn active(now: u64) -> KnowledgeItemV1 {
        let mut x = KnowledgeItemV1 {
            schema: "harness.knowledge-item.v1".into(),
            knowledge_id: "lesson-1".into(),
            kind: KnowledgeKind::Procedure,
            statement: "reviewed display lesson".into(),
            scope: KnowledgeScope {
                repository_id: "repo".into(),
                task_family: "tasks".into(),
                model_family: None,
                runtime_class: None,
            },
            evidence: vec![receipt(ReceiptKind::Trace, "trace-1")],
            confidence_milli: 900,
            review: KnowledgeReview {
                state: ReviewState::Accepted,
                reviewer_id: Some("operator-1".into()),
                reviewed_at: Some(now),
                receipt: Some(receipt(ReceiptKind::HumanReview, "review-1")),
            },
            freshness: KnowledgeFreshness {
                created_at: 1,
                revalidate_after: now + 1,
                expires_at: now + 2,
            },
            contradicts: vec![],
            supersedes: vec![],
            state: KnowledgeState::Active,
            sha256: String::new(),
        };
        x.sha256 = digest(&x).unwrap();
        x
    }
    #[test]
    fn knowledge_is_display_only_and_authority_wins() {
        let x = active(10);
        assert!(x.displayable(10));
        assert!(!x.displayable(11));
        let mut bad = x.clone();
        bad.contradicts.push("other".into());
        bad.sha256 = digest(&bad).unwrap();
        assert!(!bad.displayable(10));
    }
    #[test]
    fn knowledge_rejects_free_text_scope_and_active_without_review() {
        let mut item = active(10);
        item.scope.model_family = Some("model family with spaces".into());
        item.sha256 = digest(&item).unwrap();
        assert!(item.verify().is_err());
        let mut item = active(10);
        item.review.state = ReviewState::Unreviewed;
        item.review.reviewer_id = None;
        item.review.reviewed_at = None;
        item.review.receipt = None;
        item.sha256 = digest(&item).unwrap();
        assert!(item.verify().is_err());
    }
    #[test]
    fn knowledge_tokens_accept_the_contract_ceiling_and_reject_one_more_byte() {
        let mut at_ceiling = active(10);
        let token = |byte: char| byte.to_string().repeat(MAX_KNOWLEDGE_TOKEN_LEN);
        at_ceiling.knowledge_id = token('a');
        at_ceiling.scope.repository_id = token('b');
        at_ceiling.scope.task_family = token('c');
        at_ceiling.scope.model_family = Some(token('d'));
        at_ceiling.scope.runtime_class = Some(token('e'));
        at_ceiling.evidence[0].revision_id = token('f');
        at_ceiling.review.reviewer_id = Some(token('g'));
        at_ceiling.review.receipt.as_mut().unwrap().revision_id = token('h');
        at_ceiling.contradicts = vec![token('i')];
        at_ceiling.supersedes = vec![token('j')];
        at_ceiling.sha256 = digest(&at_ceiling).unwrap();
        assert!(at_ceiling.verify().is_ok());

        let mut too_long = at_ceiling;
        too_long.knowledge_id.push('a');
        too_long.sha256 = digest(&too_long).unwrap();
        assert!(too_long.verify().is_err());
    }
    #[test]
    fn bundle_and_candidate_are_exactly_one_safe_edit() {
        let mut bundle = PolicyBundleV1 {
            schema: "harness.policy-bundle.v1".into(),
            bundle_id: "champion".into(),
            repository_id: "repo".into(),
            task_family: "tasks".into(),
            components: vec![PolicyComponent {
                dimension: ComponentDimension::TokenBudget,
                manifest_digest: h(),
                risk_class: EditRisk::Green,
            }],
            parent_bundle_id: None,
            safety_anchor_digest: h(),
            sha256: String::new(),
        };
        bundle.sha256 = digest(&bundle).unwrap();
        assert!(bundle.verify().is_ok());
        bundle.components.push(bundle.components[0].clone());
        bundle.sha256 = digest(&bundle).unwrap();
        assert!(bundle.verify().is_err());
        bundle.components.pop();
        bundle.sha256 = digest(&bundle).unwrap();
        bundle.repository_id = "r".repeat(160);
        bundle.sha256 = digest(&bundle).unwrap();
        assert!(bundle.verify().is_ok());
        bundle.repository_id.push('r');
        bundle.sha256 = digest(&bundle).unwrap();
        assert!(bundle.verify().is_err());
        bundle.repository_id = "repo".into();
        bundle.sha256 = digest(&bundle).unwrap();
        let mut c = CandidateV1 {
            schema: "harness.improvement-candidate.v1".into(),
            candidate_id: "candidate-1".into(),
            scope: CandidateScope {
                repository_id: "repo".into(),
                task_family: "tasks".into(),
                model_family: None,
                runtime_class: None,
                base_sha: "a".repeat(40),
            },
            parent_bundle: receipt(ReceiptKind::PolicyBundle, "champion-rev"),
            target_failure: receipt(ReceiptKind::Failure, "failure-1"),
            development_case: development("case-dev"),
            no_change_control: development("case-control"),
            taskset: receipt(ReceiptKind::Taskset, "set-dev"),
            grader_bundle: receipt(ReceiptKind::GraderBundle, "grader-1"),
            runtime: receipt(ReceiptKind::Runtime, "runtime-1"),
            hypothesis: "narrow".into(),
            edit: CandidateEdit {
                dimension: ComponentDimension::TokenBudget,
                risk_class: EditRisk::Green,
                operation: EditOperation::Replace,
                before_digest: h(),
                after_digest: "b".repeat(64),
            },
            predictions: vec![Prediction {
                signal_id: "quality".into(),
                direction: PredictionDirection::Unchanged,
                minimum_delta_milli: 0,
            }],
            evidence: vec![receipt(ReceiptKind::Trace, "trace-1")],
            rollback_bundle: receipt(ReceiptKind::PolicyBundle, "champion-rollback"),
            state: CandidateState::Proposed,
            sha256: String::new(),
        };
        c.sha256 = digest(&c).unwrap();
        assert!(c.verify().is_ok());
        c.scope.repository_id = "r".repeat(160);
        c.sha256 = digest(&c).unwrap();
        assert!(c.verify().is_ok());
        c.scope.repository_id.push('r');
        c.sha256 = digest(&c).unwrap();
        assert!(c.verify().is_err());
        c.scope.repository_id = "repo".into();
        c.sha256 = digest(&c).unwrap();
        c.edit.risk_class = EditRisk::Amber;
        c.sha256 = digest(&c).unwrap();
        assert!(c.verify().is_err());
        c.edit.risk_class = EditRisk::Green;
        c.edit.after_digest = c.edit.before_digest.clone();
        c.sha256 = digest(&c).unwrap();
        assert!(c.verify().is_err());
        c.edit.after_digest = "b".repeat(64);
        // JSON Schema can reject only duplicate whole prediction objects;
        // the wire verifier is the semantic boundary for duplicate signal IDs.
        c.predictions.push(Prediction {
            signal_id: "quality".into(),
            direction: PredictionDirection::Increase,
            minimum_delta_milli: 1,
        });
        c.sha256 = digest(&c).unwrap();
        assert!(c.verify().is_err());
    }
    #[test]
    fn optimizer_is_bounded_deterministic_and_rejects_holdout() {
        let input = OptimizerInput {
            clusters: vec![
                FailureSuggestionInput {
                    failure: receipt(ReceiptKind::Failure, "f2"),
                    development_evidence: vec![development("development-2")],
                    no_change_control: development("control-2"),
                },
                FailureSuggestionInput {
                    failure: receipt(ReceiptKind::Failure, "f1"),
                    development_evidence: vec![development("development-1")],
                    no_change_control: development("control-1"),
                },
            ],
            editable: vec![
                ComponentDimension::TokenBudget,
                ComponentDimension::RolePrompts,
            ],
            rejected: vec![],
        };
        let a = suggest_candidates(&input).unwrap();
        assert!(a.len() <= 3);
        assert_eq!(a[0].kind, SuggestionKind::NoChangeControl);
        assert_eq!(a[1].kind, SuggestionKind::Targeted);
        assert_eq!(a[1].target_failure.revision_id, "f1");
        let mut suppressed = input.clone();
        suppressed.rejected.push(RejectedSuggestion {
            failure_revision_id: "f1".into(),
            dimension: ComponentDimension::TokenBudget,
        });
        assert!(
            !suggest_candidates(&suppressed)
                .unwrap()
                .iter()
                .any(|s| s.kind != SuggestionKind::NoChangeControl
                    && s.target_failure.revision_id == "f1"
                    && s.dimension == ComponentDimension::TokenBudget)
        );
        let mut bad = input.clone();
        bad.clusters[0].development_evidence[0].split = Some(EvalSplit::Holdout);
        assert_eq!(
            suggest_candidates(&bad),
            Err(LearningContractError::UnsafeSuggestionInput)
        );
        let mut restricted = input.clone();
        restricted.clusters[0].failure.custody = Some(CustodyState::Restricted);
        assert_eq!(
            suggest_candidates(&restricted),
            Err(LearningContractError::UnsafeSuggestionInput)
        );
        let mut reused_control = input.clone();
        reused_control.clusters[0].no_change_control.revision_id = reused_control.clusters[0]
            .development_evidence[0]
            .revision_id
            .clone();
        assert_eq!(
            suggest_candidates(&reused_control),
            Err(LearningContractError::UnsafeSuggestionInput)
        );
    }

    #[test]
    fn checked_examples_are_exact_canonical_contracts() {
        let knowledge: KnowledgeItemV1 = serde_json::from_str(include_str!(
            "../../../examples/self-improvement/knowledge-item.example.json"
        ))
        .unwrap();
        assert_eq!(digest(&knowledge).unwrap(), knowledge.sha256);
        assert!(knowledge.verify().is_ok());
        let candidate: CandidateV1 = serde_json::from_str(include_str!(
            "../../../examples/self-improvement/candidate.example.json"
        ))
        .unwrap();
        assert_eq!(digest(&candidate).unwrap(), candidate.sha256);
        assert!(candidate.verify().is_ok());
        let bundle: PolicyBundleV1 = serde_json::from_str(include_str!(
            "../../../examples/self-improvement/policy-bundle.example.json"
        ))
        .unwrap();
        assert_eq!(digest(&bundle).unwrap(), bundle.sha256);
        assert!(bundle.verify().is_ok());
    }
}

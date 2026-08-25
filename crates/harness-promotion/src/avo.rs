//! Bounded, evidence-gated Agentic Variation Operator (AVO) episode contracts.
//!
//! This module deliberately models only the inner variation loop.  It cannot
//! schedule work, mutate a repository, promote a candidate, or contact an
//! external environment.  Callers must turn an `Improved` candidate into the
//! existing experiment and promotion contracts before it can have any effect.

use crate::{ContractError, Receipt, ReceiptKind, TaskFamily, digest, digest_without_self, id};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// An intentionally small bound prevents an episode from becoming an
/// unreviewable autonomous search process.
pub const MAX_VARIATIONS_PER_EPISODE: u16 = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VariationStrategy {
    Refine,
    Repair,
    Reframe,
    Retrieval,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CorrectnessResult {
    Passed,
    Failed,
    Inconclusive,
    InfrastructureUnavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VariationOutcome {
    Improved,
    NotImproved,
    Rejected,
    Inconclusive,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AvoDirective {
    /// The next bounded variation may be prepared.
    Continue,
    /// A supervisor may review history and suggest a new strategy.  This is
    /// advisory only and deliberately does not authorize another action.
    RequestAdvisoryRedirect,
    /// The immutable episode has consumed its configured search budget.
    StopBudget,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AvoVariationV1 {
    pub sequence: u16,
    pub variation_id: String,
    pub parent_candidate_id: Option<String>,
    pub strategy: VariationStrategy,
    pub parent_score_milli: u64,
    pub candidate_id: String,
    pub candidate_receipt: Receipt,
    pub correctness: CorrectnessResult,
    pub correctness_evidence: Receipt,
    pub candidate_score_milli: Option<u64>,
    pub outcome: VariationOutcome,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AvoEpisodeV1 {
    pub schema: String,
    pub episode_id: String,
    pub task_family: TaskFamily,
    pub champion_bundle_id: String,
    pub champion_bundle_receipt: Receipt,
    /// Immutable digest of the knowledge set offered for retrieval.  The
    /// episode records a snapshot rather than letting future retrieval change
    /// the meaning of an old variation.
    pub knowledge_snapshot_digest: String,
    /// Immutable digest of the hard correctness policy used by every
    /// variation.  Quality is considered only after this gate passes.
    pub hard_gate_policy_digest: String,
    pub initial_score_milli: u64,
    pub variation_budget: u16,
    pub stagnation_limit: u16,
    pub variations: Vec<AvoVariationV1>,
    pub sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AvoTrajectory {
    pub incumbent_score_milli: u64,
    pub incumbent_candidate_id: Option<String>,
    pub completed_variations: u16,
    pub stagnant_variations: u16,
    pub directive: AvoDirective,
}

pub fn verify_avo_episode(value: &AvoEpisodeV1) -> Result<(), ContractError> {
    if value.schema != "harness.avo-episode.v1"
        || !id(&value.episode_id)
        || !id(&value.champion_bundle_id)
        || !value
            .champion_bundle_receipt
            .valid_as(ReceiptKind::ChampionBundle)
        || value.champion_bundle_receipt.id != value.champion_bundle_id
        || !digest(&value.knowledge_snapshot_digest)
        || !digest(&value.hard_gate_policy_digest)
        || value.variation_budget == 0
        || value.variation_budget > MAX_VARIATIONS_PER_EPISODE
        || value.stagnation_limit == 0
        || value.stagnation_limit > value.variation_budget
        || value.variations.len() > usize::from(value.variation_budget)
        || digest_without_self(value)? != value.sha256
    {
        return Err(ContractError::Digest);
    }

    let mut score = value.initial_score_milli;
    let mut incumbent: Option<String> = None;
    let mut seen_candidates = BTreeSet::new();
    let mut seen_variations = BTreeSet::new();
    for (index, variation) in value.variations.iter().enumerate() {
        if variation.sequence != u16::try_from(index + 1).map_err(|_| ContractError::Budget)?
            || !seen_variations.insert(variation.variation_id.as_str())
            || !seen_candidates.insert(variation.candidate_id.as_str())
            || variation.parent_candidate_id != incumbent
        {
            return Err(ContractError::StageOrder);
        }
        verify_variation(variation, score)?;
        if variation.outcome == VariationOutcome::Improved {
            score = variation
                .candidate_score_milli
                .ok_or(ContractError::HardGate)?;
            incumbent = Some(variation.candidate_id.clone());
        }
    }
    Ok(())
}

fn verify_variation(
    value: &AvoVariationV1,
    expected_parent_score: u64,
) -> Result<(), ContractError> {
    if !id(&value.variation_id)
        || !id(&value.candidate_id)
        || !value.parent_candidate_id.as_deref().is_none_or(id)
        || value.parent_score_milli != expected_parent_score
        || !value.candidate_receipt.valid_as(ReceiptKind::Candidate)
        || value.candidate_receipt.id != value.candidate_id
        || !value.correctness_evidence.valid_as(ReceiptKind::HardGate)
    {
        return Err(ContractError::Invalid);
    }
    let valid_outcome = match value.correctness {
        CorrectnessResult::Passed => {
            value
                .candidate_score_milli
                .is_some_and(|score| match value.outcome {
                    VariationOutcome::Improved => score > value.parent_score_milli,
                    VariationOutcome::NotImproved => score <= value.parent_score_milli,
                    VariationOutcome::Rejected | VariationOutcome::Inconclusive => false,
                })
        }
        CorrectnessResult::Failed => {
            value.candidate_score_milli.is_none() && value.outcome == VariationOutcome::Rejected
        }
        CorrectnessResult::Inconclusive | CorrectnessResult::InfrastructureUnavailable => {
            value.candidate_score_milli.is_none() && value.outcome == VariationOutcome::Inconclusive
        }
    };
    valid_outcome.then_some(()).ok_or(ContractError::HardGate)
}

/// Summarize a verified trajectory.  The redirect is advisory: it deliberately
/// provides no command or authority to create another candidate.
pub fn avo_trajectory(value: &AvoEpisodeV1) -> Result<AvoTrajectory, ContractError> {
    verify_avo_episode(value)?;
    let mut incumbent_score_milli = value.initial_score_milli;
    let mut incumbent_candidate_id = None;
    let mut stagnant_variations = 0_u16;
    for variation in &value.variations {
        if variation.outcome == VariationOutcome::Improved {
            incumbent_score_milli = variation
                .candidate_score_milli
                .expect("verified improved variation has a score");
            incumbent_candidate_id = Some(variation.candidate_id.clone());
            stagnant_variations = 0;
        } else {
            stagnant_variations = stagnant_variations.saturating_add(1);
        }
    }
    let completed_variations =
        u16::try_from(value.variations.len()).map_err(|_| ContractError::Budget)?;
    let directive = if completed_variations == value.variation_budget {
        AvoDirective::StopBudget
    } else if stagnant_variations >= value.stagnation_limit {
        AvoDirective::RequestAdvisoryRedirect
    } else {
        AvoDirective::Continue
    };
    Ok(AvoTrajectory {
        incumbent_score_milli,
        incumbent_candidate_id,
        completed_variations,
        stagnant_variations,
        directive,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const H: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn receipt(kind: ReceiptKind, id: &str) -> Receipt {
        Receipt {
            kind,
            id: id.into(),
            digest: H.into(),
        }
    }

    fn variation(
        sequence: u16,
        parent_candidate_id: Option<&str>,
        candidate_id: &str,
        parent_score_milli: u64,
        correctness: CorrectnessResult,
        candidate_score_milli: Option<u64>,
        outcome: VariationOutcome,
    ) -> AvoVariationV1 {
        AvoVariationV1 {
            sequence,
            variation_id: format!("variation-{sequence}"),
            parent_candidate_id: parent_candidate_id.map(str::to_owned),
            strategy: VariationStrategy::Refine,
            parent_score_milli,
            candidate_id: candidate_id.into(),
            candidate_receipt: receipt(ReceiptKind::Candidate, candidate_id),
            correctness,
            correctness_evidence: receipt(ReceiptKind::HardGate, &format!("gate-{sequence}")),
            candidate_score_milli,
            outcome,
        }
    }

    fn episode(variations: Vec<AvoVariationV1>) -> AvoEpisodeV1 {
        let mut value = AvoEpisodeV1 {
            schema: "harness.avo-episode.v1".into(),
            episode_id: "episode-1".into(),
            task_family: TaskFamily::DevelopmentEval,
            champion_bundle_id: "champion-1".into(),
            champion_bundle_receipt: receipt(ReceiptKind::ChampionBundle, "champion-1"),
            knowledge_snapshot_digest: H.into(),
            hard_gate_policy_digest: H.into(),
            initial_score_milli: 100,
            variation_budget: 3,
            stagnation_limit: 2,
            variations,
            sha256: String::new(),
        };
        value.sha256 = digest_without_self(&value).unwrap();
        value
    }

    #[test]
    fn successful_variation_becomes_exact_parent_for_the_next_attempt() {
        let value = episode(vec![
            variation(
                1,
                None,
                "candidate-1",
                100,
                CorrectnessResult::Passed,
                Some(120),
                VariationOutcome::Improved,
            ),
            variation(
                2,
                Some("candidate-1"),
                "candidate-2",
                120,
                CorrectnessResult::Passed,
                Some(119),
                VariationOutcome::NotImproved,
            ),
        ]);
        assert!(verify_avo_episode(&value).is_ok());
        let trajectory = avo_trajectory(&value).unwrap();
        assert_eq!(trajectory.incumbent_score_milli, 120);
        assert_eq!(
            trajectory.incumbent_candidate_id.as_deref(),
            Some("candidate-1")
        );
        assert_eq!(trajectory.directive, AvoDirective::Continue);
    }

    #[test]
    fn gate_failures_do_not_compete_on_quality_and_trigger_only_an_advisory_redirect() {
        let value = episode(vec![
            variation(
                1,
                None,
                "candidate-1",
                100,
                CorrectnessResult::Failed,
                None,
                VariationOutcome::Rejected,
            ),
            variation(
                2,
                None,
                "candidate-2",
                100,
                CorrectnessResult::Inconclusive,
                None,
                VariationOutcome::Inconclusive,
            ),
        ]);
        assert_eq!(
            avo_trajectory(&value).unwrap().directive,
            AvoDirective::RequestAdvisoryRedirect
        );
    }

    #[test]
    fn invalid_score_or_lineage_is_rejected_before_a_trajectory_is_available() {
        let mut value = episode(vec![variation(
            1,
            None,
            "candidate-1",
            100,
            CorrectnessResult::Passed,
            Some(100),
            VariationOutcome::Improved,
        )]);
        value.sha256 = digest_without_self(&value).unwrap();
        assert_eq!(verify_avo_episode(&value), Err(ContractError::HardGate));

        let mut value = episode(vec![variation(
            1,
            Some("unexpected-parent"),
            "candidate-1",
            100,
            CorrectnessResult::Failed,
            None,
            VariationOutcome::Rejected,
        )]);
        value.sha256 = digest_without_self(&value).unwrap();
        assert_eq!(verify_avo_episode(&value), Err(ContractError::StageOrder));
    }

    #[test]
    fn canonical_example_has_an_exact_digest_and_lineage() {
        let value: AvoEpisodeV1 = serde_json::from_str(include_str!(
            "../../../examples/self-improvement/avo-episode.example.json"
        ))
        .unwrap();
        assert_eq!(digest_without_self(&value).unwrap(), value.sha256);
        assert!(verify_avo_episode(&value).is_ok());
    }
}

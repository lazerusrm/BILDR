//! Candidate-only knowledge derivation from controller-owned operational facts.
//!
//! This module deliberately accepts no statement, evidence, confidence, or
//! activation state from its caller. Repeated liveness recovery can be useful
//! review material, but it remains a governed, unreviewed suggestion until a
//! separate human-review pipeline accepts it.

use harness_domain::{
    CorrelationLink, CorrelationLinkId, ImprovementEventId, ImprovementRecordKind,
    ImprovementSchema, ImprovementState, KnowledgeReviewDecision, LivenessEpisode,
    LivenessObservation, LivenessObservationKind, LivenessState, ReconciliationActionKind,
    ReconciliationActionReceipt, ReconciliationEpisode, ReconciliationFinding,
    ReconciliationFindingKind, ReconciliationTrigger, RetentionClass, SensitivityClass,
    TraceContext, now_ms,
};
use harness_learning::{
    CustodyState, KnowledgeFreshness, KnowledgeItemV1, KnowledgeKind, KnowledgeReview,
    KnowledgeScope, KnowledgeState, MAX_KNOWLEDGE_TOKEN_LEN, ReceiptKind, ReviewState,
    SourceReceipt,
};
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};
use serde_json::json;
use sha2::{Digest, Sha256};

use super::{
    checked_action_receipt_row, checked_finding_row, checked_observation_row,
    checked_reconciliation_row, liveness::checked_episode_row,
};
use crate::{
    NewImprovementRevision, NewLivenessKnowledgeCandidate, NewReconciliationKnowledgeCandidate,
    ReviewKnowledgeCandidate, Store, StoreError,
};

const MAX_LIVENESS_KNOWLEDGE_EPISODES: i64 = 200;
const MAX_LIVENESS_KNOWLEDGE_OBSERVATIONS: i64 = 200;
const MAX_RECONCILIATION_KNOWLEDGE_EPISODES: i64 = 200;
const KNOWLEDGE_REVALIDATE_AFTER_MS: u64 = 7 * 24 * 60 * 60 * 1_000;
const KNOWLEDGE_EXPIRES_AFTER_MS: u64 = 30 * 24 * 60 * 60 * 1_000;
const LIVENESS_KNOWLEDGE_STATEMENT: &str = "Repeated confirmed liveness stalls recovered only after material progress; heartbeats and command activity did not clear the stall.";

#[derive(Clone)]
struct RecoveredLivenessEpisode {
    episode: LivenessEpisode,
    confirmed_stall: LivenessObservation,
    recovery: LivenessObservation,
}

#[derive(Clone)]
struct PreservedReconciliationEpisode {
    episode: ReconciliationEpisode,
    preserved_candidate: ReconciliationFinding,
    preservation_receipt: ReconciliationActionReceipt,
}

impl Store {
    /// Records one explicit human decision over the exact current knowledge
    /// candidate. The immutable review action is bound to the candidate's
    /// revision and wire digest before the resulting active/rejected revision
    /// is created, avoiding an authority cycle with the post-review hash.
    ///
    /// Acceptance is refused unless all source evidence still resolves cleanly
    /// and the candidate is still within its revalidation window. Neither
    /// outcome writes task context, changes controller authority, or executes
    /// a task.
    pub fn review_knowledge_candidate(
        &self,
        input: &ReviewKnowledgeCandidate,
    ) -> Result<crate::ImprovementRevisionRecord, StoreError> {
        validate_knowledge_review_input(input)?;
        let decision = knowledge_review_decision_name(input.decision);
        let identity = digest(&format!(
            "harness.operator-control.knowledge-review.v1\0{}\0{}\0{}\0{}",
            input.knowledge_id, input.expected_knowledge_sha256, decision, input.reviewer_id,
        ));
        let revision_id = format!("knowledge-review-{identity}");
        let event_id = format!("knowledge-review-event-{identity}");
        let idempotency_key = format!("knowledge:review:{identity}");
        let action_type = match input.decision {
            KnowledgeReviewDecision::Accept => "knowledge_review_accepted",
            KnowledgeReviewDecision::Reject => "knowledge_review_rejected",
        };
        let expected_state = match input.decision {
            KnowledgeReviewDecision::Accept => ImprovementState::Active,
            KnowledgeReviewDecision::Reject => ImprovementState::Rejected,
        };
        let expected_knowledge_state = match input.decision {
            KnowledgeReviewDecision::Accept => KnowledgeState::Active,
            KnowledgeReviewDecision::Reject => KnowledgeState::Rejected,
        };
        let expected_review_state = match input.decision {
            KnowledgeReviewDecision::Accept => ReviewState::Accepted,
            KnowledgeReviewDecision::Reject => ReviewState::Rejected,
        };

        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        if let Some(existing_revision_id) = transaction
            .query_row(
                "SELECT revision_id FROM improvement_events WHERE idempotency_key=?1",
                [&idempotency_key],
                |row| row.get::<_, String>(0),
            )
            .optional()?
        {
            let existing: crate::ImprovementRevisionRecord = transaction.query_row(
                "SELECT id,aggregate_kind,aggregate_id,revision,schema_name,lifecycle_state,payload_json,payload_sha256,sensitivity,retention_class,export_allowed,source_domain_event_id,created_at FROM improvement_revisions WHERE id=?1",
                [&existing_revision_id],
                crate::queries::map_improvement_revision,
            )?;
            let item: KnowledgeItemV1 = serde_json::from_value(existing.payload.clone())?;
            item.verify()
                .map_err(|error| StoreError::Conflict(error.to_string()))?;
            let review = item.review.receipt.as_ref().ok_or_else(|| {
                StoreError::Conflict(
                    "knowledge review replay lacks an immutable action receipt".to_owned(),
                )
            })?;
            let action_matches: bool = transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM human_actions WHERE id=?1 AND actor=?2 AND action_type=?3 AND target_type='knowledge' AND target_id=?4 AND payload_sha256=?5 AND json_extract(payload_json,'$.schema')='harness.operator-control.knowledge-review.v1' AND json_extract(payload_json,'$.candidate_wire_sha256')=?6 AND json_extract(payload_json,'$.decision')=?7)",
                params![review.revision_id, input.reviewer_id, action_type, input.knowledge_id, review.digest, input.expected_knowledge_sha256, decision],
                |row| row.get(0),
            )?;
            if existing.id != revision_id
                || existing.aggregate_kind != ImprovementRecordKind::Knowledge
                || existing.aggregate_id != input.knowledge_id
                || existing.state != expected_state
                || item.knowledge_id != input.knowledge_id
                || item.sha256.is_empty()
                || item.state != expected_knowledge_state
                || item.review.state != expected_review_state
                || item.review.reviewer_id.as_deref() != Some(input.reviewer_id.as_str())
                || !action_matches
            {
                return Err(StoreError::Conflict(
                    "knowledge review idempotency key was reused with different content".to_owned(),
                ));
            }
            transaction.commit()?;
            return Ok(existing);
        }

        let (candidate_revision_id, lifecycle_state, raw, sensitivity, retention_class, export_allowed): (
            String,
            String,
            String,
            String,
            String,
            bool,
        ) = transaction
            .query_row(
                "SELECT id,lifecycle_state,payload_json,sensitivity,retention_class,export_allowed FROM improvement_current_revisions WHERE aggregate_kind='knowledge' AND aggregate_id=?1",
                [&input.knowledge_id],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?)),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("knowledge item {}", input.knowledge_id)))?;
        let candidate: KnowledgeItemV1 = serde_json::from_str(&raw)?;
        candidate
            .verify()
            .map_err(|error| StoreError::Validation(error.to_string()))?;
        if lifecycle_state != "candidate"
            || candidate.state != KnowledgeState::Candidate
            || candidate.review.state != ReviewState::Unreviewed
            || candidate.review.reviewer_id.is_some()
            || candidate.review.reviewed_at.is_some()
            || candidate.review.receipt.is_some()
            || candidate.sha256 != input.expected_knowledge_sha256
        {
            return Err(StoreError::Conflict(
                "knowledge review requires the exact current unreviewed candidate".to_owned(),
            ));
        }

        let now = now_ms();
        let now_u64 = u64::try_from(now).map_err(|_| {
            StoreError::Validation("knowledge review clock must be non-negative".to_owned())
        })?;
        if input.decision == KnowledgeReviewDecision::Accept
            && (now_u64 >= candidate.freshness.revalidate_after
                || now_u64 >= candidate.freshness.expires_at
                || !candidate.evidence.iter().try_fold(true, |clean, receipt| {
                    crate::queries::learning_receipt_clean_tx(&transaction, receipt)
                        .map(|value| clean && value)
                })?)
        {
            return Err(StoreError::Conflict(
                "knowledge acceptance requires fresh controller-clean evidence".to_owned(),
            ));
        }

        let action_payload = json!({
            "schema": "harness.operator-control.knowledge-review.v1",
            "candidate_revision_id": candidate_revision_id,
            "candidate_wire_sha256": candidate.sha256,
            "decision": decision,
        });
        let action_raw = serde_json::to_string(&action_payload)?;
        let action_sha256 = digest(&action_raw);
        transaction.execute(
            "INSERT INTO human_actions(actor,action_type,target_type,target_id,occurred_at,payload_json,payload_sha256) VALUES(?1,?2,'knowledge',?3,?4,?5,?6)",
            params![input.reviewer_id, action_type, input.knowledge_id, now, action_raw, action_sha256],
        )?;
        let action_id = transaction.last_insert_rowid();

        let mut reviewed = candidate;
        reviewed.state = expected_knowledge_state;
        reviewed.review = KnowledgeReview {
            state: expected_review_state,
            reviewer_id: Some(input.reviewer_id.clone()),
            reviewed_at: Some(now_u64),
            receipt: Some(SourceReceipt {
                kind: ReceiptKind::HumanReview,
                revision_id: action_id.to_string(),
                digest: action_sha256,
                split: None,
                custody: Some(CustodyState::Clean),
            }),
        };
        reviewed.sha256 = reviewed
            .digest()
            .map_err(|error| StoreError::Validation(error.to_string()))?;
        reviewed
            .verify()
            .map_err(|error| StoreError::Validation(error.to_string()))?;
        let payload = serde_json::to_value(&reviewed)?;
        let payload_raw = serde_json::to_string(&payload)?;
        let payload_sha256 = digest(&payload_raw);
        let next_revision: i64 = transaction.query_row(
            "SELECT coalesce(max(revision),0)+1 FROM improvement_revisions WHERE aggregate_kind='knowledge' AND aggregate_id=?1",
            [&input.knowledge_id],
            |row| row.get(0),
        )?;
        transaction.execute(
            "INSERT INTO improvement_revisions(id,aggregate_kind,aggregate_id,revision,schema_name,lifecycle_state,payload_json,payload_sha256,sensitivity,retention_class,export_allowed,created_at) VALUES(?1,'knowledge',?2,?3,'harness.knowledge-item.v1',?4,?5,?6,?7,?8,?9,?10)",
            params![revision_id, input.knowledge_id, next_revision, improvement_state_name(expected_state), payload_raw, payload_sha256, sensitivity, retention_class, export_allowed, now],
        )?;
        let event_payload = serde_json::to_string(&json!({
            "schema": "harness.knowledge-item.v1",
            "state": improvement_state_name(expected_state),
        }))?;
        let event_payload_sha256 = digest(&event_payload);
        transaction.execute(
            "INSERT INTO improvement_events(id,aggregate_kind,aggregate_id,revision_id,sequence,event_type,payload_json,payload_sha256,idempotency_key,occurred_at) VALUES(?1,'knowledge',?2,?3,?4,'revision_recorded',?5,?6,?7,?8)",
            params![event_id, input.knowledge_id, revision_id, next_revision, event_payload, event_payload_sha256, idempotency_key, now],
        )?;
        let record: crate::ImprovementRevisionRecord = transaction.query_row(
            "SELECT id,aggregate_kind,aggregate_id,revision,schema_name,lifecycle_state,payload_json,payload_sha256,sensitivity,retention_class,export_allowed,source_domain_event_id,created_at FROM improvement_revisions WHERE id=?1",
            [&revision_id],
            crate::queries::map_improvement_revision,
        )?;
        transaction.commit()?;
        Ok(record)
    }

    /// Creates an unreviewed, display-only knowledge candidate from two exact
    /// independently recovered liveness episodes in one repository. The
    /// caller supplies only a selected episode revision and display scope;
    /// controller-owned immutable observations supply every factual claim.
    /// This method never activates or injects knowledge into task context.
    pub fn propose_knowledge_from_repeated_liveness(
        &self,
        input: &NewLivenessKnowledgeCandidate,
    ) -> Result<crate::ImprovementRevisionRecord, StoreError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let selected = transaction
            .query_row(
                "SELECT current_payload_json,current_payload_sha256 FROM liveness_episodes WHERE id=?1",
                [input.episode_id.as_str()],
                |row| checked_episode_row(row.get(0)?, row.get(1)?),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(input.episode_id.to_string()))?;
        if selected.sha256 != input.expected_episode_sha256 {
            return Err(StoreError::Conflict(
                "knowledge proposal liveness episode digest is stale".to_owned(),
            ));
        }
        if selected.state != LivenessState::Healthy
            || selected.run_id.is_none()
            || selected.task_id.is_none()
            || selected.attempt_id.is_none()
        {
            return Err(StoreError::Conflict(
                "knowledge proposal requires one exact recovered liveness episode".to_owned(),
            ));
        }
        let repository_id: String = transaction
            .query_row(
                "SELECT repository_id FROM runs WHERE id=?1",
                [selected.run_id.as_deref().expect("checked above")],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(selected.run_id.clone().expect("checked above")))?;

        let sources = recovered_liveness_episodes(&transaction, &repository_id)?;
        if sources.len() < 2
            || !sources
                .iter()
                .any(|source| source.episode.episode_id == input.episode_id)
        {
            return Err(StoreError::Conflict(
                "knowledge proposal requires two independently recovered liveness episodes"
                    .to_owned(),
            ));
        }
        let selected_source = sources
            .iter()
            .find(|source| source.episode.episode_id == input.episode_id)
            .expect("checked above")
            .clone();
        let other_source = sources
            .into_iter()
            .find(|source| source.episode.attempt_id != selected_source.episode.attempt_id)
            .ok_or_else(|| {
                StoreError::Conflict(
                    "knowledge proposal requires recoveries from two distinct attempts".to_owned(),
                )
            })?;
        let sources = vec![selected_source, other_source];

        let created_at = sources
            .iter()
            .map(|source| source.recovery.observed_at_ms)
            .min()
            .ok_or_else(|| StoreError::Conflict("no recovered liveness evidence".to_owned()))?;
        let created_at = u64::try_from(created_at).map_err(|_| {
            StoreError::Validation("liveness recovery time must not be negative".to_owned())
        })?;
        let revalidate_after = created_at
            .checked_add(KNOWLEDGE_REVALIDATE_AFTER_MS)
            .ok_or_else(|| StoreError::Validation("knowledge freshness overflow".to_owned()))?;
        let expires_at = created_at
            .checked_add(KNOWLEDGE_EXPIRES_AFTER_MS)
            .ok_or_else(|| StoreError::Validation("knowledge freshness overflow".to_owned()))?;
        let now = u64::try_from(now_ms()).unwrap_or(u64::MAX);
        if created_at > now || now >= revalidate_after {
            return Err(StoreError::Conflict(
                "recovered liveness evidence is no longer fresh enough to seed knowledge"
                    .to_owned(),
            ));
        }

        let scope = KnowledgeScope {
            repository_id,
            task_family: input.task_family.clone(),
            model_family: input.model_family.clone(),
            runtime_class: input.runtime_class.clone(),
        };
        let identity = digest(&serde_json::to_string(&json!({
            "schema": "harness.liveness-knowledge-proposal.v1",
            "selected_episode_id": selected.episode_id.to_string(),
            "selected_episode_sha256": selected.sha256,
            "scope": scope.clone(),
            "episodes": sources.iter().map(|source| json!({
                "episode_id": source.episode.episode_id.to_string(),
                "episode_sha256": source.episode.sha256,
                "confirmed_stall_observation_id": source.confirmed_stall.observation_id.to_string(),
                "confirmed_stall_sha256": source.confirmed_stall.sha256,
                "recovery_observation_id": source.recovery.observation_id.to_string(),
                "recovery_sha256": source.recovery.sha256,
            })).collect::<Vec<_>>(),
        }))?);
        transaction.commit()?;
        // `connection` is the store-wide SQLite mutex guard. Release it
        // before append_improvement_revision takes the same guard for its
        // own atomic append transaction.
        drop(connection);

        let mut evidence = Vec::with_capacity(4);
        for source in &sources {
            evidence.push(liveness_evidence_receipt(&source.confirmed_stall));
            evidence.push(liveness_evidence_receipt(&source.recovery));
        }
        let knowledge_id = format!("knowledge-liveness-{}", &identity[..32]);
        let mut item = KnowledgeItemV1 {
            schema: "harness.knowledge-item.v1".to_owned(),
            knowledge_id: knowledge_id.clone(),
            kind: KnowledgeKind::Heuristic,
            statement: LIVENESS_KNOWLEDGE_STATEMENT.to_owned(),
            scope,
            evidence,
            confidence_milli: 900,
            review: KnowledgeReview {
                state: ReviewState::Unreviewed,
                reviewer_id: None,
                reviewed_at: None,
                receipt: None,
            },
            freshness: KnowledgeFreshness {
                created_at,
                revalidate_after,
                expires_at,
            },
            contradicts: Vec::new(),
            supersedes: Vec::new(),
            state: KnowledgeState::Candidate,
            sha256: String::new(),
        };
        item.sha256 = item
            .digest()
            .map_err(|error| StoreError::Validation(error.to_string()))?;
        item.verify()
            .map_err(|error| StoreError::Validation(error.to_string()))?;
        let payload = serde_json::to_value(&item)?;
        let correlation_created_at = i64::try_from(created_at).map_err(|_| {
            StoreError::Validation("liveness knowledge creation time is out of range".to_owned())
        })?;
        let correlations = liveness_knowledge_candidate_correlations(
            &sources,
            &knowledge_id,
            correlation_created_at,
        )?;
        let (record, _) = self.append_improvement_revision_with_correlations(
            &NewImprovementRevision {
                id: format!("knowledge-revision-{identity}"),
                aggregate_kind: ImprovementRecordKind::Knowledge,
                aggregate_id: knowledge_id,
                schema: ImprovementSchema::KnowledgeItemV1,
                state: ImprovementState::Candidate,
                payload_sha256: digest(&serde_json::to_string(&payload)?),
                payload,
                sensitivity: SensitivityClass::Internal,
                retention_class: RetentionClass::Governance,
                export_allowed: false,
                idempotency_key: format!("knowledge:liveness:{identity}"),
                event_id: ImprovementEventId::from(format!("knowledge-event-{identity}")),
                source_raw_event_id: None,
                source_domain_event_id: None,
            },
            &correlations,
        )?;
        Ok(record)
    }

    /// Creates an unreviewed, display-only knowledge candidate from two exact
    /// reconciliation episodes in one repository that each preserved custody.
    /// The candidate records repeated operational evidence only: preservation
    /// is not a successful recovery, ownership proof, or authorization for a
    /// fresh attempt. This method never activates knowledge or changes task
    /// execution state.
    pub fn propose_knowledge_from_repeated_reconciliation(
        &self,
        input: &NewReconciliationKnowledgeCandidate,
    ) -> Result<crate::ImprovementRevisionRecord, StoreError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let selected = transaction
            .query_row(
                "SELECT current_payload_json,current_payload_sha256 FROM reconciliation_episodes WHERE id=?1",
                [input.episode_id.as_str()],
                |row| checked_reconciliation_row(row.get(0)?, row.get(1)?),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(input.episode_id.to_string()))?;
        if selected.sha256 != input.expected_episode_sha256 {
            return Err(StoreError::Conflict(
                "knowledge proposal reconciliation episode digest is stale".to_owned(),
            ));
        }
        let run_id = selected.run_id.as_deref().ok_or_else(|| {
            StoreError::Conflict(
                "knowledge proposal requires a reconciliation episode owned by one run".to_owned(),
            )
        })?;
        let repository_id: String = transaction
            .query_row(
                "SELECT repository_id FROM runs WHERE id=?1",
                [run_id],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(run_id.to_owned()))?;
        let sources =
            preserved_reconciliation_episodes(&transaction, &repository_id, selected.trigger_kind)?;
        if sources.len() < 2
            || !sources
                .iter()
                .any(|source| source.episode.episode_id == selected.episode_id)
        {
            return Err(StoreError::Conflict(
                "knowledge proposal requires two independently preserved reconciliation episodes"
                    .to_owned(),
            ));
        }
        let selected_source = sources
            .iter()
            .find(|source| source.episode.episode_id == selected.episode_id)
            .expect("selected source was checked")
            .clone();
        let other_source = sources
            .into_iter()
            .find(|source| source.episode.episode_id != selected_source.episode.episode_id)
            .ok_or_else(|| {
                StoreError::Conflict(
                    "knowledge proposal requires preservation evidence from two distinct episodes"
                        .to_owned(),
                )
            })?;
        let sources = vec![selected_source, other_source];
        let created_at = sources
            .iter()
            .map(reconciliation_source_observed_at)
            .min()
            .ok_or_else(|| StoreError::Conflict("no preservation evidence found".to_owned()))?;
        let created_at = u64::try_from(created_at).map_err(|_| {
            StoreError::Validation(
                "reconciliation preservation time must not be negative".to_owned(),
            )
        })?;
        let revalidate_after = created_at
            .checked_add(KNOWLEDGE_REVALIDATE_AFTER_MS)
            .ok_or_else(|| StoreError::Validation("knowledge freshness overflow".to_owned()))?;
        let expires_at = created_at
            .checked_add(KNOWLEDGE_EXPIRES_AFTER_MS)
            .ok_or_else(|| StoreError::Validation("knowledge freshness overflow".to_owned()))?;
        let now = u64::try_from(now_ms()).unwrap_or(u64::MAX);
        if created_at > now || now >= revalidate_after {
            return Err(StoreError::Conflict(
                "reconciliation preservation evidence is no longer fresh enough to seed knowledge"
                    .to_owned(),
            ));
        }
        let scope = KnowledgeScope {
            repository_id,
            task_family: input.task_family.clone(),
            model_family: input.model_family.clone(),
            runtime_class: input.runtime_class.clone(),
        };
        let identity = digest(&serde_json::to_string(&json!({
            "schema": "harness.reconciliation-knowledge-proposal.v1",
            "selected_episode_id": selected.episode_id.to_string(),
            "selected_episode_sha256": selected.sha256,
            "scope": scope.clone(),
            "episodes": sources.iter().map(|source| json!({
                "episode_id": source.episode.episode_id.to_string(),
                "episode_sha256": source.episode.sha256,
                "preserved_candidate_source_event_id": source.preserved_candidate.source_event_id,
                "preserved_candidate_sha256": source.preserved_candidate.sha256,
                "preservation_source_event_id": source.preservation_receipt.source_event_id,
                "preservation_sha256": source.preservation_receipt.sha256,
            })).collect::<Vec<_>>(),
        }))?);
        transaction.commit()?;
        drop(connection);

        let knowledge_id = format!("knowledge-reconciliation-{}", &identity[..32]);
        let mut item = KnowledgeItemV1 {
            schema: "harness.knowledge-item.v1".to_owned(),
            knowledge_id: knowledge_id.clone(),
            kind: KnowledgeKind::Warning,
            statement: reconciliation_knowledge_statement(selected.trigger_kind),
            scope,
            evidence: sources
                .iter()
                .map(reconciliation_evidence_receipt)
                .collect(),
            confidence_milli: 800,
            review: KnowledgeReview {
                state: ReviewState::Unreviewed,
                reviewer_id: None,
                reviewed_at: None,
                receipt: None,
            },
            freshness: KnowledgeFreshness {
                created_at,
                revalidate_after,
                expires_at,
            },
            contradicts: Vec::new(),
            supersedes: Vec::new(),
            state: KnowledgeState::Candidate,
            sha256: String::new(),
        };
        item.sha256 = item
            .digest()
            .map_err(|error| StoreError::Validation(error.to_string()))?;
        item.verify()
            .map_err(|error| StoreError::Validation(error.to_string()))?;
        let payload = serde_json::to_value(&item)?;
        let correlation_created_at = i64::try_from(created_at).map_err(|_| {
            StoreError::Validation(
                "reconciliation knowledge creation time is out of range".to_owned(),
            )
        })?;
        let correlations = reconciliation_knowledge_candidate_correlations(
            &sources,
            &knowledge_id,
            correlation_created_at,
        )?;
        let (record, _) = self.append_improvement_revision_with_correlations(
            &NewImprovementRevision {
                id: format!("knowledge-revision-{identity}"),
                aggregate_kind: ImprovementRecordKind::Knowledge,
                aggregate_id: knowledge_id,
                schema: ImprovementSchema::KnowledgeItemV1,
                state: ImprovementState::Candidate,
                payload_sha256: digest(&serde_json::to_string(&payload)?),
                payload,
                sensitivity: SensitivityClass::Internal,
                retention_class: RetentionClass::Governance,
                export_allowed: false,
                idempotency_key: format!("knowledge:reconciliation:{identity}"),
                event_id: ImprovementEventId::from(format!("knowledge-event-{identity}")),
                source_raw_event_id: None,
                source_domain_event_id: None,
            },
            &correlations,
        )?;
        Ok(record)
    }
}

fn validate_knowledge_review_input(input: &ReviewKnowledgeCandidate) -> Result<(), StoreError> {
    for value in [&input.knowledge_id, &input.reviewer_id] {
        if value.is_empty()
            || value.len() > MAX_KNOWLEDGE_TOKEN_LEN
            || !value.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':')
            })
        {
            return Err(StoreError::Validation(
                "knowledge review identifiers must be closed bounded tokens".to_owned(),
            ));
        }
    }
    if input.expected_knowledge_sha256.len() != 64
        || !input
            .expected_knowledge_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(StoreError::Validation(
            "knowledge review requires an exact lowercase candidate digest".to_owned(),
        ));
    }
    Ok(())
}

fn knowledge_review_decision_name(decision: KnowledgeReviewDecision) -> &'static str {
    match decision {
        KnowledgeReviewDecision::Accept => "accept",
        KnowledgeReviewDecision::Reject => "reject",
    }
}

fn improvement_state_name(state: ImprovementState) -> &'static str {
    match state {
        ImprovementState::Active => "active",
        ImprovementState::Rejected => "rejected",
        _ => unreachable!("knowledge review has one of two closed outcomes"),
    }
}

fn reconciliation_knowledge_trace_id(knowledge_id: &str) -> String {
    digest(&format!(
        "harness.reconciliation-knowledge.trace.v1:{knowledge_id}"
    ))[..32]
        .to_owned()
}

fn reconciliation_knowledge_candidate_correlations(
    sources: &[PreservedReconciliationEpisode],
    knowledge_id: &str,
    created_at_ms: i64,
) -> Result<Vec<CorrelationLink>, StoreError> {
    let trace_id = reconciliation_knowledge_trace_id(knowledge_id);
    let span_id = digest(&format!(
        "harness.reconciliation-knowledge.span.v1:{knowledge_id}"
    ));
    sources
        .iter()
        .map(|source| {
            let link_id = CorrelationLinkId::parse(format!(
                "correlation-{}",
                &digest(&format!(
                    "harness.reconciliation-knowledge.link.v1:{knowledge_id}:{}",
                    source.episode.episode_id
                ))[..48]
            ))
            .map_err(|error| StoreError::Validation(error.to_string()))?;
            Ok(CorrelationLink {
                schema: "harness.correlation-link.v1".to_owned(),
                link_id,
                trace: TraceContext {
                    trace_id: trace_id.clone(),
                    span_id: span_id[..16].to_owned(),
                    parent_span_id: None,
                },
                from_kind: "reconciliation_episode".to_owned(),
                from_id: source.episode.episode_id.to_string(),
                to_kind: "knowledge_candidate".to_owned(),
                to_id: knowledge_id.to_owned(),
                relation: "supports_knowledge_candidate".to_owned(),
                created_at_ms,
            })
        })
        .collect()
}

fn reconciliation_knowledge_statement(trigger: ReconciliationTrigger) -> String {
    format!(
        "Repeated {} reconciliation episodes preserved existing custody without authorizing automatic replacement; review recovery controls before changing policy.",
        reconciliation_trigger_label(trigger)
    )
}

fn reconciliation_trigger_label(trigger: ReconciliationTrigger) -> &'static str {
    match trigger {
        ReconciliationTrigger::DaemonRestart => "daemon-restart",
        ReconciliationTrigger::AppServerLoss => "App Server loss",
        ReconciliationTrigger::ProcessLoss => "process-loss",
        ReconciliationTrigger::VersionTransition => "version-transition",
        ReconciliationTrigger::AccountHandoff => "account-handoff",
        ReconciliationTrigger::WorktreeMismatch => "worktree-mismatch",
        ReconciliationTrigger::UncertainCommandCompletion => "uncertain-command-completion",
    }
}

fn reconciliation_evidence_receipt(source: &PreservedReconciliationEpisode) -> SourceReceipt {
    SourceReceipt {
        kind: ReceiptKind::ReconciliationEpisode,
        revision_id: source.episode.episode_id.to_string(),
        digest: source.episode.sha256.clone(),
        split: None,
        custody: Some(CustodyState::Clean),
    }
}

fn reconciliation_source_observed_at(source: &PreservedReconciliationEpisode) -> i64 {
    source
        .episode
        .updated_at_ms
        .max(source.preserved_candidate.observed_at_ms)
        .max(source.preservation_receipt.created_at_ms)
}

fn preserved_reconciliation_episodes(
    transaction: &Transaction<'_>,
    repository_id: &str,
    trigger: ReconciliationTrigger,
) -> Result<Vec<PreservedReconciliationEpisode>, StoreError> {
    let count: i64 = transaction.query_row(
        "SELECT count(*) FROM reconciliation_episodes episodes JOIN runs ON runs.id=episodes.run_id WHERE runs.repository_id=?1",
        [repository_id],
        |row| row.get(0),
    )?;
    if count > MAX_RECONCILIATION_KNOWLEDGE_EPISODES {
        return Err(StoreError::Conflict(format!(
            "repository has more than {MAX_RECONCILIATION_KNOWLEDGE_EPISODES} reconciliation episodes; bounded knowledge evidence is incomplete"
        )));
    }
    let mut statement = transaction.prepare(
        "SELECT episodes.current_payload_json,episodes.current_payload_sha256 FROM reconciliation_episodes episodes JOIN runs ON runs.id=episodes.run_id WHERE runs.repository_id=?1 ORDER BY episodes.updated_at DESC,episodes.id DESC LIMIT ?2",
    )?;
    let episodes = statement
        .query_map(
            params![repository_id, MAX_RECONCILIATION_KNOWLEDGE_EPISODES],
            |row| checked_reconciliation_row(row.get(0)?, row.get(1)?),
        )?
        .collect::<Result<Vec<_>, _>>()?;
    drop(statement);

    episodes
        .into_iter()
        .filter(|episode| episode.trigger_kind == trigger)
        .map(|episode| preservation_evidence(transaction, episode))
        .filter_map(|source| match source {
            Ok(Some(source)) => Some(Ok(source)),
            Ok(None) => None,
            Err(error) => Some(Err(error)),
        })
        .collect()
}

fn preservation_evidence(
    transaction: &Transaction<'_>,
    episode: ReconciliationEpisode,
) -> Result<Option<PreservedReconciliationEpisode>, StoreError> {
    let mut findings = transaction
        .prepare(
            "SELECT payload_json,payload_sha256 FROM reconciliation_findings WHERE episode_id=?1 AND kind='preserved_candidate' ORDER BY id ASC LIMIT 2",
        )?
        .query_map([episode.episode_id.as_str()], |row| {
            checked_finding_row(row.get(0)?, row.get(1)?)
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let Some(preserved_candidate) = findings.pop() else {
        return Ok(None);
    };
    if !findings.is_empty()
        || preserved_candidate.kind != ReconciliationFindingKind::PreservedCandidate
    {
        return Err(StoreError::Conflict(format!(
            "reconciliation episode {} has ambiguous preservation findings",
            episode.episode_id
        )));
    }
    let mut actions = transaction
        .prepare(
            "SELECT payload_json,payload_sha256 FROM reconciliation_actions WHERE episode_id=?1 AND kind='preserve' ORDER BY id ASC LIMIT 2",
        )?
        .query_map([episode.episode_id.as_str()], |row| {
            checked_action_receipt_row(row.get(0)?, row.get(1)?)
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let Some(preservation_receipt) = actions.pop() else {
        return Ok(None);
    };
    if !actions.is_empty()
        || preservation_receipt.kind != ReconciliationActionKind::Preserve
        || preservation_receipt.authority_event_id.is_some()
    {
        return Err(StoreError::Conflict(format!(
            "reconciliation episode {} has ambiguous preservation receipts",
            episode.episode_id
        )));
    }
    Ok(Some(PreservedReconciliationEpisode {
        episode,
        preserved_candidate,
        preservation_receipt,
    }))
}

fn liveness_knowledge_trace_id(knowledge_id: &str) -> String {
    digest(&format!(
        "harness.liveness-knowledge.trace.v1:{knowledge_id}"
    ))[..32]
        .to_owned()
}

/// A repeated-recovery candidate has four immutable observation sources. Keep
/// those fan-in edges explicit on one candidate trace rather than inferring a
/// recovery pattern from timestamp adjacency in a viewer.
fn liveness_knowledge_candidate_correlations(
    sources: &[RecoveredLivenessEpisode],
    knowledge_id: &str,
    created_at_ms: i64,
) -> Result<Vec<CorrelationLink>, StoreError> {
    let trace_id = liveness_knowledge_trace_id(knowledge_id);
    let span_id = digest(&format!(
        "harness.liveness-knowledge.span.v1:{knowledge_id}"
    ));
    let mut correlations = Vec::with_capacity(sources.len() * 2);
    for source in sources {
        for observation in [&source.confirmed_stall, &source.recovery] {
            let link_id = CorrelationLinkId::parse(format!(
                "correlation-{}",
                &digest(&format!(
                    "harness.liveness-knowledge.link.v1:{knowledge_id}:{}",
                    observation.observation_id
                ))[..48]
            ))
            .map_err(|error| StoreError::Validation(error.to_string()))?;
            correlations.push(CorrelationLink {
                schema: "harness.correlation-link.v1".to_owned(),
                link_id,
                trace: TraceContext {
                    trace_id: trace_id.clone(),
                    span_id: span_id[..16].to_owned(),
                    parent_span_id: None,
                },
                from_kind: "liveness_observation".to_owned(),
                from_id: observation.observation_id.to_string(),
                to_kind: "knowledge_candidate".to_owned(),
                to_id: knowledge_id.to_owned(),
                relation: "supports_knowledge_candidate".to_owned(),
                created_at_ms,
            });
        }
    }
    Ok(correlations)
}

fn recovered_liveness_episodes(
    transaction: &Transaction<'_>,
    repository_id: &str,
) -> Result<Vec<RecoveredLivenessEpisode>, StoreError> {
    let episode_count: i64 = transaction.query_row(
        "SELECT count(*) FROM liveness_episodes episodes JOIN runs ON runs.id=episodes.run_id WHERE runs.repository_id=?1",
        [repository_id],
        |row| row.get(0),
    )?;
    if episode_count > MAX_LIVENESS_KNOWLEDGE_EPISODES {
        return Err(StoreError::Conflict(format!(
            "repository has more than {MAX_LIVENESS_KNOWLEDGE_EPISODES} liveness episodes; bounded knowledge evidence is incomplete"
        )));
    }
    let mut statement = transaction.prepare(
        "SELECT episodes.current_payload_json,episodes.current_payload_sha256 FROM liveness_episodes episodes JOIN runs ON runs.id=episodes.run_id WHERE runs.repository_id=?1 ORDER BY episodes.updated_at DESC,episodes.id DESC LIMIT ?2",
    )?;
    let episodes = statement
        .query_map(
            params![repository_id, MAX_LIVENESS_KNOWLEDGE_EPISODES],
            |row| checked_episode_row(row.get(0)?, row.get(1)?),
        )?
        .collect::<Result<Vec<_>, _>>()?;
    drop(statement);

    let mut recovered = Vec::new();
    for episode in episodes {
        if episode.state != LivenessState::Healthy
            || episode.run_id.is_none()
            || episode.task_id.is_none()
            || episode.attempt_id.is_none()
        {
            continue;
        }
        if let Some((confirmed_stall, recovery)) = recovered_observations(transaction, &episode)? {
            recovered.push(RecoveredLivenessEpisode {
                episode,
                confirmed_stall,
                recovery,
            });
        }
    }
    Ok(recovered)
}

fn recovered_observations(
    transaction: &Transaction<'_>,
    episode: &LivenessEpisode,
) -> Result<Option<(LivenessObservation, LivenessObservation)>, StoreError> {
    let observation_count: i64 = transaction.query_row(
        "SELECT count(*) FROM liveness_observations WHERE episode_id=?1",
        [episode.episode_id.as_str()],
        |row| row.get(0),
    )?;
    if observation_count > MAX_LIVENESS_KNOWLEDGE_OBSERVATIONS {
        return Err(StoreError::Conflict(format!(
            "liveness episode {} has more than {MAX_LIVENESS_KNOWLEDGE_OBSERVATIONS} observations; bounded knowledge evidence is incomplete",
            episode.episode_id
        )));
    }
    let mut statement = transaction.prepare(
        "SELECT payload_json,payload_sha256 FROM liveness_observations WHERE episode_id=?1 ORDER BY observed_at ASC,id ASC LIMIT ?2",
    )?;
    let observations = statement
        .query_map(
            params![
                episode.episode_id.as_str(),
                MAX_LIVENESS_KNOWLEDGE_OBSERVATIONS
            ],
            |row| checked_observation_row(row.get(0)?, row.get(1)?),
        )?
        .collect::<Result<Vec<_>, _>>()?;
    drop(statement);

    let mut latest_confirmed_stall = None;
    for observation in observations {
        if is_confirmed_stall_observation(&observation) {
            latest_confirmed_stall = Some(observation);
            continue;
        }
        if observation.observation_kind == LivenessObservationKind::MaterialProgress
            && latest_confirmed_stall
                .as_ref()
                .is_some_and(|stall| observation.observed_at_ms > stall.observed_at_ms)
        {
            return Ok(latest_confirmed_stall.map(|stall| (stall, observation)));
        }
    }
    Ok(None)
}

fn is_confirmed_stall_observation(observation: &LivenessObservation) -> bool {
    let active_external_wait = observation
        .value
        .get("active_external_wait")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let ownership_uncertain = observation
        .value
        .get("ownership")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|value| value == "uncertain");
    let no_progress_boundary = observation
        .value
        .get("no_material_progress_boundary")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    let repeated_failures = observation
        .value
        .get("repeated_semantic_failures")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(0);
    let fresh_inspection = observation
        .value
        .get("fresh_inspection")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);
    no_progress_boundary
        && !active_external_wait
        && !ownership_uncertain
        && repeated_failures >= 2
        && fresh_inspection
}

fn liveness_evidence_receipt(observation: &LivenessObservation) -> SourceReceipt {
    SourceReceipt {
        kind: ReceiptKind::LivenessObservation,
        revision_id: observation.observation_id.to_string(),
        digest: observation.sha256.clone(),
        split: None,
        custody: Some(CustodyState::Clean),
    }
}

fn digest(raw: &str) -> String {
    hex::encode(Sha256::digest(raw.as_bytes()))
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use harness_domain::{
        KnowledgeReviewDecision, LivenessEpisodeId, LivenessObservationId,
        ReconciliationActionKind, ReconciliationActionReceipt, ReconciliationEpisode,
        ReconciliationEpisodeId, ReconciliationFinding, ReconciliationFindingKind,
        ReconciliationState, ReconciliationTrigger, RepositoryId, RunId,
    };
    use serde_json::{Value, json};
    use tempfile::TempDir;

    use super::*;
    use crate::{NewRepository, NewRun};

    fn insert_run(store: &Store, root: &Path) -> (RepositoryId, RunId) {
        let repository_id = RepositoryId::from("repository-liveness-knowledge");
        let run_id = RunId::from("run-liveness-knowledge");
        store
            .create_repository(&NewRepository {
                id: repository_id.clone(),
                profile_id: "fixture".to_owned(),
                profile_version: 1,
                display_name: "Liveness knowledge fixture".to_owned(),
                root_path: root.join("checkout"),
                origin_url: None,
                default_branch: "main".to_owned(),
                expected_coordination_branch: None,
                state: "READY".to_owned(),
            })
            .expect("repository");
        store
            .create_run(&NewRun {
                id: run_id.clone(),
                repository_id: repository_id.clone(),
                title: "Liveness knowledge fixture".to_owned(),
                objective: "Derive a review-only repeated recovery candidate".to_owned(),
                mode: "observe_only".to_owned(),
                publication_mode: "none".to_owned(),
                state: "CREATED".to_owned(),
                phase: "created".to_owned(),
                base_ref: "main".to_owned(),
                base_sha: "a".repeat(40),
                authority_digest: "c".repeat(64),
                profile_digest: "d".repeat(64),
                codex_version: None,
                protocol_schema_sha256: None,
                requested_by: "test".to_owned(),
                token_budget: None,
            })
            .expect("run");
        (repository_id, run_id)
    }

    fn recovered_episode(
        store: &Store,
        run_id: &RunId,
        sequence: &str,
        opened_at_ms: i64,
    ) -> LivenessEpisode {
        let mut episode = LivenessEpisode {
            schema: "harness.liveness-episode.v1".to_owned(),
            episode_id: LivenessEpisodeId::new(),
            run_id: Some(run_id.to_string()),
            task_id: Some(format!("task-liveness-{sequence}")),
            attempt_id: Some(format!("attempt-liveness-{sequence}")),
            state: LivenessState::Healthy,
            version: 1,
            opened_at_ms,
            updated_at_ms: opened_at_ms,
            state_reason_codes: vec!["opened".to_owned()],
            last_material_progress_at_ms: None,
            next_review_at_ms: None,
            intervention_count: 0,
            outcome: None,
            sha256: String::new(),
        };
        episode.sha256 = episode.digest().expect("episode digest");
        let opened = store.open_liveness_episode(&episode).expect("open");
        let stalled = store
            .record_liveness_observation(
                &opened.episode_id,
                opened.version,
                &observation(
                    opened.episode_id.clone(),
                    LivenessObservationKind::RuntimeHeartbeat,
                    format!("event-stall-{sequence}"),
                    opened_at_ms + 1,
                    json!({
                        "no_material_progress_boundary": true,
                        "repeated_semantic_failures": 2,
                        "fresh_inspection": true,
                    }),
                ),
            )
            .expect("confirmed stall");
        assert_eq!(stalled.state, LivenessState::ConfirmedStall);
        store
            .record_liveness_observation(
                &stalled.episode_id,
                stalled.version,
                &observation(
                    stalled.episode_id.clone(),
                    LivenessObservationKind::MaterialProgress,
                    format!("event-progress-{sequence}"),
                    opened_at_ms + 2,
                    json!({"progress_id": format!("progress-{sequence}")}),
                ),
            )
            .expect("material recovery")
    }

    fn additional_run(store: &Store, repository_id: &RepositoryId, suffix: &str) -> RunId {
        let run_id = RunId::from(format!("run-reconciliation-knowledge-{suffix}"));
        store
            .create_run(&NewRun {
                id: run_id.clone(),
                repository_id: repository_id.clone(),
                title: "Reconciliation knowledge fixture".to_owned(),
                objective: "Derive a review-only repeated preservation candidate".to_owned(),
                mode: "observe_only".to_owned(),
                publication_mode: "none".to_owned(),
                state: "CREATED".to_owned(),
                phase: "created".to_owned(),
                base_ref: "main".to_owned(),
                base_sha: "a".repeat(40),
                authority_digest: "c".repeat(64),
                profile_digest: "d".repeat(64),
                codex_version: None,
                protocol_schema_sha256: None,
                requested_by: "test".to_owned(),
                token_budget: None,
            })
            .expect("additional run");
        run_id
    }

    fn preserved_reconciliation_episode(
        store: &Store,
        run_id: &RunId,
        suffix: &str,
        opened_at_ms: i64,
    ) -> ReconciliationEpisode {
        let mut episode = ReconciliationEpisode {
            schema: "harness.reconciliation-episode.v1".to_owned(),
            episode_id: ReconciliationEpisodeId::new(),
            run_id: Some(run_id.to_string()),
            trigger_kind: ReconciliationTrigger::AppServerLoss,
            state: ReconciliationState::Open,
            version: 1,
            opened_at_ms,
            updated_at_ms: opened_at_ms,
            source_event_id: format!("reconciliation-source-{suffix}"),
            inventory_sha256: "b".repeat(64),
            finding_count: 0,
            action_count: 0,
            report: Some("Inventory recorded before preserving custody".to_owned()),
            sha256: String::new(),
        };
        episode.sha256 = episode.digest().expect("episode digest");
        let opened = store
            .open_reconciliation_episode(&episode)
            .expect("open episode");
        let mut finding = ReconciliationFinding {
            schema: "harness.reconciliation-finding.v1".to_owned(),
            episode_id: opened.episode_id.clone(),
            kind: ReconciliationFindingKind::PreservedCandidate,
            source_event_id: format!("reconciliation-preserved-{suffix}"),
            observed_at_ms: opened_at_ms + 1,
            payload: json!({
                "worktree_id": format!("worktree-{suffix}"),
                "result": "preserved",
            }),
            sha256: String::new(),
        };
        finding.sha256 = finding.digest().expect("finding digest");
        let after_finding = store
            .record_reconciliation_finding(&finding, opened.version)
            .expect("preserved candidate");
        let mut receipt = ReconciliationActionReceipt {
            schema: "harness.reconciliation-action-receipt.v1".to_owned(),
            episode_id: opened.episode_id,
            kind: ReconciliationActionKind::Preserve,
            source_event_id: format!("reconciliation-preservation-{suffix}"),
            authority_event_id: None,
            created_at_ms: opened_at_ms + 2,
            payload: json!({
                "worktree_id": format!("worktree-{suffix}"),
                "result": "preserved",
            }),
            sha256: String::new(),
        };
        receipt.sha256 = receipt.digest().expect("receipt digest");
        store
            .record_reconciliation_action_receipt(&receipt, after_finding.version)
            .expect("preservation receipt")
    }

    fn observation(
        episode_id: LivenessEpisodeId,
        observation_kind: LivenessObservationKind,
        source_event_id: String,
        observed_at_ms: i64,
        value: Value,
    ) -> LivenessObservation {
        let mut observation = LivenessObservation {
            schema: "harness.liveness-observation.v1".to_owned(),
            observation_id: LivenessObservationId::new(),
            episode_id,
            observation_kind,
            source_event_id,
            observed_at_ms,
            value,
            classifier_version: "liveness-v1".to_owned(),
            sha256: String::new(),
        };
        observation.sha256 = observation.digest().expect("observation digest");
        observation
    }

    #[test]
    fn repeated_recoveries_create_an_unreviewed_candidate_with_exact_receipts() {
        let temp = TempDir::new().expect("temp");
        let store = Store::in_memory(&temp.path().join("artifacts")).expect("store");
        let (repository_id, run_id) = insert_run(&store, temp.path());
        let base = now_ms().saturating_sub(10_000);
        recovered_episode(&store, &run_id, "one", base);
        let selected = recovered_episode(&store, &run_id, "two", base + 100);
        let input = NewLivenessKnowledgeCandidate {
            episode_id: selected.episode_id.clone(),
            expected_episode_sha256: selected.sha256.clone(),
            task_family: "operator_control".to_owned(),
            model_family: None,
            runtime_class: None,
        };

        let record = store
            .propose_knowledge_from_repeated_liveness(&input)
            .expect("candidate");
        let item: KnowledgeItemV1 = serde_json::from_value(record.payload.clone()).expect("wire");
        assert_eq!(record.state, ImprovementState::Candidate);
        assert_eq!(item.kind, KnowledgeKind::Heuristic);
        assert_eq!(item.state, KnowledgeState::Candidate);
        assert_eq!(item.review.state, ReviewState::Unreviewed);
        assert_eq!(item.scope.repository_id, repository_id.as_str());
        assert_eq!(item.evidence.len(), 4);
        assert!(item.evidence.iter().all(|receipt| {
            receipt.kind == ReceiptKind::LivenessObservation
                && receipt.custody == Some(CustodyState::Clean)
                && receipt.split.is_none()
        }));
        let correlations = store
            .correlation_links(&liveness_knowledge_trace_id(&item.knowledge_id), 10)
            .expect("candidate trace");
        assert_eq!(correlations.len(), 4);
        assert!(correlations.iter().all(|link| {
            link.from_kind == "liveness_observation"
                && link.to_kind == "knowledge_candidate"
                && link.to_id == item.knowledge_id
                && link.relation == "supports_knowledge_candidate"
        }));
        assert_eq!(
            store
                .current_knowledge_item(&item.knowledge_id)
                .expect("candidate remains readable"),
            item
        );
        assert!(
            store
                .resolved_active_knowledge(&repository_id, "operator_control", 0)
                .expect("candidate never activates itself")
                .is_empty()
        );
        assert_eq!(
            store
                .propose_knowledge_from_repeated_liveness(&input)
                .expect("replay")
                .id,
            record.id
        );

        let mut mismatched = item.clone();
        mismatched.knowledge_id = "knowledge-liveness-mismatched-receipt".to_owned();
        mismatched.evidence[0].digest = "e".repeat(64);
        mismatched.sha256 = mismatched.digest().expect("mismatched wire digest");
        let payload = serde_json::to_value(&mismatched).expect("mismatched payload");
        assert!(matches!(
            store.append_improvement_revision(&NewImprovementRevision {
                id: "knowledge-revision-liveness-mismatched-receipt".to_owned(),
                aggregate_kind: ImprovementRecordKind::Knowledge,
                aggregate_id: mismatched.knowledge_id.clone(),
                schema: ImprovementSchema::KnowledgeItemV1,
                state: ImprovementState::Candidate,
                payload_sha256: digest(&serde_json::to_string(&payload).expect("payload wire")),
                payload,
                sensitivity: SensitivityClass::Internal,
                retention_class: RetentionClass::Governance,
                export_allowed: false,
                idempotency_key: "knowledge:liveness:mismatched-receipt".to_owned(),
                event_id: ImprovementEventId::from("knowledge-event-liveness-mismatched-receipt"),
                source_raw_event_id: None,
                source_domain_event_id: None,
            }),
            Err(StoreError::Conflict(_))
        ));

        let mut stale = input;
        stale.expected_episode_sha256 = "e".repeat(64);
        assert!(matches!(
            store.propose_knowledge_from_repeated_liveness(&stale),
            Err(StoreError::Conflict(_))
        ));
    }

    #[test]
    fn human_review_is_candidate_bound_idempotent_and_never_injects_context() {
        let temp = TempDir::new().expect("temp");
        let store = Store::in_memory(&temp.path().join("artifacts")).expect("store");
        let (repository_id, run_id) = insert_run(&store, temp.path());
        let base = now_ms().saturating_sub(10_000);
        let task_family = "t".repeat(MAX_KNOWLEDGE_TOKEN_LEN);
        let reviewer_id = "r".repeat(MAX_KNOWLEDGE_TOKEN_LEN);
        recovered_episode(&store, &run_id, "review-one", base);
        let selected = recovered_episode(&store, &run_id, "review-two", base + 100);
        let candidate_record = store
            .propose_knowledge_from_repeated_liveness(&NewLivenessKnowledgeCandidate {
                episode_id: selected.episode_id.clone(),
                expected_episode_sha256: selected.sha256.clone(),
                task_family: task_family.clone(),
                model_family: None,
                runtime_class: None,
            })
            .expect("candidate");
        let candidate: KnowledgeItemV1 =
            serde_json::from_value(candidate_record.payload.clone()).expect("candidate wire");

        let review = ReviewKnowledgeCandidate {
            knowledge_id: candidate.knowledge_id.clone(),
            expected_knowledge_sha256: candidate.sha256.clone(),
            decision: KnowledgeReviewDecision::Accept,
            reviewer_id: reviewer_id.clone(),
        };
        let accepted = store
            .review_knowledge_candidate(&review)
            .expect("accepted review");
        let active: KnowledgeItemV1 =
            serde_json::from_value(accepted.payload.clone()).expect("active knowledge wire");
        assert_eq!(accepted.state, ImprovementState::Active);
        assert_eq!(active.state, KnowledgeState::Active);
        assert_eq!(active.review.state, ReviewState::Accepted);
        assert_eq!(
            active.review.reviewer_id.as_deref(),
            Some(reviewer_id.as_str())
        );
        assert!(active.review.receipt.is_some());
        assert_eq!(
            store
                .current_knowledge_item(&candidate.knowledge_id)
                .expect("active current wire"),
            active
        );
        assert_eq!(
            store
                .resolved_active_knowledge(
                    &repository_id,
                    &task_family,
                    u64::try_from(now_ms()).expect("non-negative now"),
                )
                .expect("trusted active display")
                .len(),
            1,
            "only the immutable reviewed display projection becomes available"
        );
        assert_eq!(
            store
                .review_knowledge_candidate(&review)
                .expect("exact replay")
                .id,
            accepted.id
        );
        assert!(matches!(
            store.review_knowledge_candidate(&ReviewKnowledgeCandidate {
                decision: KnowledgeReviewDecision::Reject,
                ..review
            }),
            Err(StoreError::Conflict(_))
        ));

        let rejected_candidate_record = store
            .propose_knowledge_from_repeated_liveness(&NewLivenessKnowledgeCandidate {
                episode_id: selected.episode_id,
                expected_episode_sha256: selected.sha256,
                task_family: "operator_control_rejected".to_owned(),
                model_family: None,
                runtime_class: None,
            })
            .expect("separate candidate");
        let rejected_candidate: KnowledgeItemV1 =
            serde_json::from_value(rejected_candidate_record.payload).expect("candidate wire");
        let rejected = store
            .review_knowledge_candidate(&ReviewKnowledgeCandidate {
                knowledge_id: rejected_candidate.knowledge_id.clone(),
                expected_knowledge_sha256: rejected_candidate.sha256,
                decision: KnowledgeReviewDecision::Reject,
                reviewer_id: "local-session-reviewer".to_owned(),
            })
            .expect("rejected review");
        let rejected_item: KnowledgeItemV1 =
            serde_json::from_value(rejected.payload).expect("rejected knowledge wire");
        assert_eq!(rejected.state, ImprovementState::Rejected);
        assert_eq!(rejected_item.state, KnowledgeState::Rejected);
        assert_eq!(rejected_item.review.state, ReviewState::Rejected);
        assert!(
            store
                .resolved_active_knowledge(
                    &repository_id,
                    "operator_control_rejected",
                    u64::try_from(now_ms()).expect("non-negative now"),
                )
                .expect("rejected knowledge is never displayable")
                .is_empty()
        );
        let action_payload: String = store
            .connection()
            .expect("connection")
            .query_row(
                "SELECT payload_json FROM human_actions WHERE action_type='knowledge_review_accepted'",
                [],
                |row| row.get(0),
            )
            .expect("human review action");
        assert!(action_payload.contains(&candidate_record.id));
        assert!(action_payload.contains(&candidate.sha256));
        assert!(
            !action_payload.contains(&active.sha256),
            "the action is bound to the pre-review candidate, not recursively to the post-review wire"
        );
    }

    #[test]
    fn one_recovery_cannot_seed_knowledge() {
        let temp = TempDir::new().expect("temp");
        let store = Store::in_memory(&temp.path().join("artifacts")).expect("store");
        let (_, run_id) = insert_run(&store, temp.path());
        let base = now_ms().saturating_sub(10_000);
        let recovered = recovered_episode(&store, &run_id, "only", base);
        assert!(matches!(
            store.propose_knowledge_from_repeated_liveness(&NewLivenessKnowledgeCandidate {
                episode_id: recovered.episode_id,
                expected_episode_sha256: recovered.sha256,
                task_family: "operator_control".to_owned(),
                model_family: None,
                runtime_class: None,
            }),
            Err(StoreError::Conflict(_))
        ));
    }

    #[test]
    fn repeated_preservations_create_an_unreviewed_candidate_with_exact_episode_evidence() {
        let temp = TempDir::new().expect("temp");
        let store = Store::in_memory(&temp.path().join("artifacts")).expect("store");
        let (repository_id, _) = insert_run(&store, temp.path());
        let first_run = additional_run(&store, &repository_id, "one");
        let second_run = additional_run(&store, &repository_id, "two");
        let base = now_ms().saturating_sub(10_000);
        preserved_reconciliation_episode(&store, &first_run, "one", base);
        let selected = preserved_reconciliation_episode(&store, &second_run, "two", base + 100);
        let input = NewReconciliationKnowledgeCandidate {
            episode_id: selected.episode_id.clone(),
            expected_episode_sha256: selected.sha256.clone(),
            task_family: "operator_control".to_owned(),
            model_family: None,
            runtime_class: None,
        };

        let record = store
            .propose_knowledge_from_repeated_reconciliation(&input)
            .expect("candidate");
        let item: KnowledgeItemV1 = serde_json::from_value(record.payload.clone()).expect("wire");
        assert_eq!(record.state, ImprovementState::Candidate);
        assert_eq!(item.kind, KnowledgeKind::Warning);
        assert_eq!(item.state, KnowledgeState::Candidate);
        assert_eq!(item.review.state, ReviewState::Unreviewed);
        assert_eq!(item.scope.repository_id, repository_id.as_str());
        assert_eq!(item.evidence.len(), 2);
        assert!(item.evidence.iter().all(|receipt| {
            receipt.kind == ReceiptKind::ReconciliationEpisode
                && receipt.custody == Some(CustodyState::Clean)
                && receipt.split.is_none()
        }));
        let correlations = store
            .correlation_links(&reconciliation_knowledge_trace_id(&item.knowledge_id), 10)
            .expect("candidate trace");
        assert_eq!(correlations.len(), 2);
        assert!(correlations.iter().all(|link| {
            link.from_kind == "reconciliation_episode"
                && link.to_kind == "knowledge_candidate"
                && link.to_id == item.knowledge_id
                && link.relation == "supports_knowledge_candidate"
        }));
        assert_eq!(
            store
                .current_knowledge_item(&item.knowledge_id)
                .expect("candidate remains readable"),
            item
        );
        assert!(
            store
                .resolved_active_knowledge(&repository_id, "operator_control", 0)
                .expect("candidate never activates itself")
                .is_empty()
        );
        assert_eq!(
            store
                .propose_knowledge_from_repeated_reconciliation(&input)
                .expect("replay")
                .id,
            record.id
        );

        let mut mismatched = item.clone();
        mismatched.knowledge_id = "knowledge-reconciliation-mismatched-receipt".to_owned();
        mismatched.evidence[0].digest = "e".repeat(64);
        mismatched.sha256 = mismatched.digest().expect("mismatched wire digest");
        let payload = serde_json::to_value(&mismatched).expect("mismatched payload");
        assert!(matches!(
            store.append_improvement_revision(&NewImprovementRevision {
                id: "knowledge-revision-reconciliation-mismatched-receipt".to_owned(),
                aggregate_kind: ImprovementRecordKind::Knowledge,
                aggregate_id: mismatched.knowledge_id.clone(),
                schema: ImprovementSchema::KnowledgeItemV1,
                state: ImprovementState::Candidate,
                payload_sha256: digest(&serde_json::to_string(&payload).expect("payload wire")),
                payload,
                sensitivity: SensitivityClass::Internal,
                retention_class: RetentionClass::Governance,
                export_allowed: false,
                idempotency_key: "knowledge:reconciliation:mismatched-receipt".to_owned(),
                event_id: ImprovementEventId::from(
                    "knowledge-event-reconciliation-mismatched-receipt",
                ),
                source_raw_event_id: None,
                source_domain_event_id: None,
            }),
            Err(StoreError::Conflict(_))
        ));

        let mut stale = input;
        stale.expected_episode_sha256 = "e".repeat(64);
        assert!(matches!(
            store.propose_knowledge_from_repeated_reconciliation(&stale),
            Err(StoreError::Conflict(_))
        ));
    }

    #[test]
    fn one_preservation_cannot_seed_knowledge() {
        let temp = TempDir::new().expect("temp");
        let store = Store::in_memory(&temp.path().join("artifacts")).expect("store");
        let (repository_id, _) = insert_run(&store, temp.path());
        let run_id = additional_run(&store, &repository_id, "only");
        let preserved =
            preserved_reconciliation_episode(&store, &run_id, "only", now_ms() - 10_000);
        assert!(matches!(
            store.propose_knowledge_from_repeated_reconciliation(
                &NewReconciliationKnowledgeCandidate {
                    episode_id: preserved.episode_id,
                    expected_episode_sha256: preserved.sha256,
                    task_family: "operator_control".to_owned(),
                    model_family: None,
                    runtime_class: None,
                }
            ),
            Err(StoreError::Conflict(_))
        ));
    }
}

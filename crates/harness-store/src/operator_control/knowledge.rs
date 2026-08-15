//! Candidate-only knowledge derivation from controller-owned operational facts.
//!
//! This module deliberately accepts no statement, evidence, confidence, or
//! activation state from its caller. Repeated liveness recovery can be useful
//! review material, but it remains a governed, unreviewed suggestion until a
//! separate human-review pipeline accepts it.

use harness_domain::{
    ImprovementEventId, ImprovementRecordKind, ImprovementSchema, ImprovementState,
    LivenessEpisode, LivenessObservation, LivenessObservationKind, LivenessState, RetentionClass,
    SensitivityClass, now_ms,
};
use harness_learning::{
    CustodyState, KnowledgeFreshness, KnowledgeItemV1, KnowledgeKind, KnowledgeReview,
    KnowledgeScope, KnowledgeState, ReceiptKind, ReviewState, SourceReceipt,
};
use rusqlite::{OptionalExtension, Transaction, params};
use serde_json::json;
use sha2::{Digest, Sha256};

use super::liveness::{checked_episode_row, checked_observation_row};
use crate::{NewImprovementRevision, NewLivenessKnowledgeCandidate, Store, StoreError};

const MAX_LIVENESS_KNOWLEDGE_EPISODES: i64 = 200;
const MAX_LIVENESS_KNOWLEDGE_OBSERVATIONS: i64 = 200;
const KNOWLEDGE_REVALIDATE_AFTER_MS: u64 = 7 * 24 * 60 * 60 * 1_000;
const KNOWLEDGE_EXPIRES_AFTER_MS: u64 = 30 * 24 * 60 * 60 * 1_000;
const LIVENESS_KNOWLEDGE_STATEMENT: &str = "Repeated confirmed liveness stalls recovered only after material progress; heartbeats and command activity did not clear the stall.";

#[derive(Clone)]
struct RecoveredLivenessEpisode {
    episode: LivenessEpisode,
    confirmed_stall: LivenessObservation,
    recovery: LivenessObservation,
}

impl Store {
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
        let (record, _) = self.append_improvement_revision(&NewImprovementRevision {
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
        })?;
        Ok(record)
    }
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

    use harness_domain::{LivenessEpisodeId, LivenessObservationId, RepositoryId, RunId};
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
}

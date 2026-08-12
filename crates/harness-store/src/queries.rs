use std::{
    collections::{BTreeMap, BTreeSet},
    path::{Path, PathBuf},
};

use harness_domain::{
    ActivityItem, AgentSessionId, AgentSummary, ApprovalId, ApprovalSummary, ArtifactId, AttemptId,
    CostConfidence, CostEstimate, DomainEvent, ImprovementRecordKind, ImprovementSchema,
    ImprovementState, LatestAgentMessage, ModelUsageSummary, OutcomeConfidence, OutcomeHistory,
    OutcomeId, OutcomeRevisionReceipt, OutcomeRevisionView, OutcomeSource, OutcomeSourceKind,
    OutcomeVector, OutcomeVectorItem, OutcomeWireV1, PlanRevisionId, RepositoryId,
    RepositorySummary, RetentionClass, RunId, RunPlan, RunState, RunSummary, SensitivityClass,
    TaskId, TaskState, TaskSummary, TokenUsage, UsageBreakdown, UsageGroup, UsageSummary,
    WorktreeId, WorktreeSummary, format_timestamp, now_ms, validate_operator_outcome_label,
};
use harness_learning::{
    CostAttribution, EditReason, FailureClass, FailureOccurrence, FailureScope, FailureWireCost,
    MembershipAction, Severity, TerminalCode,
};
use rusqlite::{OptionalExtension, Row, params};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::{
    ArtifactRecord, AuthoritativeOutcomeInput, FailureClusterOverview, FailureProjectionReceipt,
    FailureSplitMove, FailureTraceComposition, FailureTraceSummary, ImprovementEventRecord,
    ImprovementRevisionRecord, NativeSubagentActivityRecord, NewAgentSession, NewApproval,
    NewArtifact, NewCommandRecord, NewContextPacket, NewEvidenceRecord, NewImprovementRevision,
    NewOperatorOutcome, NewRepository, NewRun, NewTaskAttempt, NewValidationRecord, NewWorktree,
    PriorAttemptContext, RawEventInput, RepositoryHealthInput, Store, StoreError, StoredSession,
    TraceProjectionDomainReceipt, TraceProjectionRawReceipt, TraceProjectionSnapshot,
    TraceProjectionStructuralReceipt,
};

impl Store {
    // SI-007: project only Store-owned, typed terminal and outcome receipts.
    // Neither failure reasons nor outcome notes are read or persisted here.
    pub fn project_failures_for_run(
        &self,
        run_id: &RunId,
    ) -> Result<FailureProjectionReceipt, StoreError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let repository_id: String = transaction.query_row(
            "SELECT repository_id FROM runs WHERE id=?1",
            [run_id.as_str()],
            |row| row.get(0),
        )?;
        let now = now_ms();
        let mut inserted = 0;
        let mut already_projected = 0;
        let mut project = |source_kind: FailureScope,
                           source_id: String,
                           terminal: Option<TerminalCode>,
                           severity: Severity,
                           source_domain_event_id: Option<i64>|
         -> Result<(), StoreError> {
            validate_failure_identifier(&source_id)?;
            let automatic = terminal.map_or(FailureClass::Unknown, TerminalCode::class);
            // Cost accounting can finish after a terminal receipt arrives.
            // Keep the immutable occurrence independent of that live ledger;
            // the overview derives the latest priced run estimate read-only.
            let occurrence_cost = CostAttribution::unknown();
            let wire_cost = FailureWireCost::try_from(&occurrence_cost)
                .map_err(|error| StoreError::Validation(error.to_string()))?;
            let fingerprint =
                FailureOccurrence::fingerprint_for(&repository_id, source_kind, automatic);
            let (scope_id, lower, upper) = failure_cost_columns(wire_cost)?;
            let id = format!(
                "failure-{}",
                sha256(format!("{}\0{}", failure_scope_text(source_kind), source_id).as_bytes())
            );
            let changed = transaction.execute(
                "INSERT OR IGNORE INTO failure_occurrences(id,repository_id,source_kind,source_id,terminal_code,automatic_class,severity,taxonomy_version,fingerprint_sha256,cost_scope_id,cost_lower_microusd,cost_upper_microusd,source_domain_event_id,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,'harness.failure-taxonomy.v1',?8,?9,?10,?11,?12,?13)",
                params![id, repository_id, failure_scope_text(source_kind), source_id, terminal.map(terminal_code_text), failure_class_text(automatic), severity_text(severity), fingerprint, scope_id, lower, upper, source_domain_event_id, now],
            )?;
            if changed == 1 {
                let cluster_id = format!("failure-cluster-{fingerprint}");
                transaction.execute(
                    "INSERT OR IGNORE INTO failure_clusters(id,repository_id,version,created_at) VALUES(?1,?2,0,?3)",
                    params![cluster_id, repository_id, now],
                )?;
                append_failure_membership_tx(
                    &transaction,
                    &id,
                    &cluster_id,
                    MembershipAction::Assigned,
                    "system",
                    EditReason::SourceCorrection,
                )?;
                bump_failure_cluster(&transaction, &cluster_id)?;
                inserted += 1;
            } else {
                let matches: bool = transaction.query_row(
                    "SELECT EXISTS(SELECT 1 FROM failure_occurrences WHERE source_kind=?1 AND source_id=?2 AND repository_id=?3 AND terminal_code IS ?4 AND automatic_class=?5 AND severity=?6 AND fingerprint_sha256=?7 AND cost_scope_id IS ?8 AND cost_lower_microusd IS ?9 AND cost_upper_microusd IS ?10 AND source_domain_event_id IS ?11)",
                    params![failure_scope_text(source_kind),source_id,repository_id,terminal.map(terminal_code_text),failure_class_text(automatic),severity_text(severity),fingerprint,scope_id,lower,upper,source_domain_event_id], |r| r.get(0),
                )?;
                if !matches {
                    return Err(StoreError::Conflict(
                        "failure projection source collision has different immutable semantics"
                            .to_owned(),
                    ));
                }
                already_projected += 1;
            }
            Ok(())
        };
        let run_terminal: Option<String> = transaction.query_row(
            "SELECT failure_class FROM runs WHERE id=?1",
            [run_id.as_str()],
            |row| row.get(0),
        )?;
        if let Some(code) = run_terminal {
            let terminal_events = terminal_run_failure_events(&transaction, run_id, &code)?;
            for event_id in terminal_events {
                project(
                    FailureScope::RunTerminal,
                    format!("domain-event-{event_id}"),
                    TerminalCode::parse(&code),
                    Severity::Unknown,
                    Some(event_id),
                )?;
            }
        }
        let mut attempts = transaction.prepare("SELECT a.id,a.terminal_class FROM task_attempts a JOIN tasks t ON t.id=a.task_id WHERE t.run_id=?1 AND a.terminal_class IS NOT NULL ORDER BY a.id")?;
        let attempt_rows = attempts
            .query_map([run_id.as_str()], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        drop(attempts);
        for (id, code) in attempt_rows {
            project(
                FailureScope::AttemptTerminal,
                id.clone(),
                TerminalCode::parse(&code),
                Severity::Unknown,
                None,
            )?;
        }
        let mut outcomes = transaction.prepare("SELECT id,aggregate_kind,aggregate_id,revision,schema_name,lifecycle_state,payload_json,payload_sha256,sensitivity,retention_class,export_allowed,source_domain_event_id,created_at FROM improvement_revisions WHERE aggregate_kind='outcome' ORDER BY aggregate_id,revision")?;
        let outcome_rows = outcomes
            .query_map([], map_improvement_revision)?
            .collect::<Result<Vec<_>, _>>()?;
        drop(outcomes);
        for revision in outcome_rows {
            let outcome: OutcomeWireV1 = serde_json::from_value(revision.payload)?;
            outcome
                .validate()
                .map_err(|error| StoreError::Validation(error.to_string()))?;
            if outcome.run_id == *run_id
                && matches!(
                    outcome.classification,
                    harness_domain::OutcomeClassification::Negative
                        | harness_domain::OutcomeClassification::Unknown
                )
            {
                project(
                    FailureScope::TypedOutcome,
                    revision.id,
                    TerminalCode::parse(&outcome.code),
                    Severity::Unknown,
                    revision.source_domain_event_id,
                )?;
            }
        }
        transaction.commit()?;
        Ok(FailureProjectionReceipt {
            inserted,
            already_projected,
        })
    }

    pub fn create_failure_cluster(
        &self,
        repository_id: &RepositoryId,
        cluster_id: &str,
    ) -> Result<(), StoreError> {
        validate_failure_identifier(cluster_id)?;
        let connection = self.connection()?;
        connection.execute(
            "INSERT INTO failure_clusters(id,repository_id,version,created_at) VALUES(?1,?2,0,?3)",
            params![cluster_id, repository_id.as_str(), now_ms()],
        )?;
        Ok(())
    }

    pub fn reclassify_failure(
        &self,
        occurrence_id: &str,
        expected_revision: u64,
        class: FailureClass,
        actor: &str,
        reason: EditReason,
    ) -> Result<(), StoreError> {
        validate_failure_actor(actor)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let actual: i64 = transaction.query_row(
            "SELECT count(*) FROM failure_classification_revisions WHERE occurrence_id=?1",
            [occurrence_id],
            |r| r.get(0),
        )?;
        if u64::try_from(actual).ok() != Some(expected_revision) {
            return Err(StoreError::Conflict(format!(
                "stale failure occurrence {occurrence_id}"
            )));
        }
        let exists: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM failure_occurrences WHERE id=?1)",
            [occurrence_id],
            |r| r.get(0),
        )?;
        if !exists {
            return Err(StoreError::NotFound(format!(
                "failure occurrence {occurrence_id}"
            )));
        }
        transaction.execute("INSERT INTO failure_classification_revisions(occurrence_id,revision,class,actor,reason_code,created_at) VALUES(?1,?2,?3,?4,?5,?6)", params![occurrence_id, actual + 1, failure_class_text(class), actor, edit_reason_text(reason), now_ms()])?;
        transaction.commit()?;
        Ok(())
    }

    pub fn assign_failure_to_cluster(
        &self,
        occurrence_id: &str,
        cluster_id: &str,
        expected_cluster_version: u64,
        actor: &str,
        reason: EditReason,
    ) -> Result<(), StoreError> {
        self.append_failure_membership(
            occurrence_id,
            cluster_id,
            expected_cluster_version,
            MembershipAction::Assigned,
            actor,
            reason,
        )
    }

    pub fn failure_cluster_overview(
        &self,
        repository_id: &RepositoryId,
    ) -> Result<Vec<FailureClusterOverview>, StoreError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare("SELECT c.id,c.repository_id,c.version,m.occurrence_id,o.cost_scope_id,o.cost_lower_microusd,o.cost_upper_microusd,o.automatic_class,o.severity,o.source_kind FROM failure_clusters c LEFT JOIN failure_cluster_membership_revisions m ON m.cluster_id=c.id AND m.revision=(SELECT max(m2.revision) FROM failure_cluster_membership_revisions m2 WHERE m2.occurrence_id=m.occurrence_id) LEFT JOIN failure_occurrences o ON o.id=m.occurrence_id WHERE c.repository_id=?1 ORDER BY c.id,m.occurrence_id")?;
        let rows = statement
            .query_map([repository_id.as_str()], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                    row.get::<_, Option<String>>(3)?,
                    row.get::<_, Option<String>>(4)?,
                    row.get::<_, Option<i64>>(5)?,
                    row.get::<_, Option<i64>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<String>>(8)?,
                    row.get::<_, Option<String>>(9)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        let mut grouped = BTreeMap::<String, FailureClusterOverview>::new();
        let mut scopes = BTreeMap::<String, BTreeMap<String, (u64, u64)>>::new();
        for (
            id,
            repo,
            version,
            occurrence,
            scope,
            lower,
            upper,
            automatic,
            severity,
            source_kind,
        ) in rows
        {
            let entry = grouped.entry(id.clone()).or_insert(FailureClusterOverview {
                cluster_id: id,
                repository_id: RepositoryId::from(repo),
                version: u64::try_from(version).map_err(|_| {
                    StoreError::Validation("negative failure cluster version".to_owned())
                })?,
                occurrences: 0,
                unknown_cost_occurrences: 0,
                cost_lower_microusd: 0,
                cost_upper_microusd: 0,
                representative_occurrence_id: occurrence.clone(),
                representative_run_id: None,
                representative_trace_id: None,
                effective_class: automatic,
                severity,
            });
            if occurrence.is_some() {
                entry.occurrences += 1;
                let effective_cost = match (scope, lower, upper) {
                    (Some(scope), Some(lower), Some(upper)) => Some((scope, lower, upper)),
                    _ if source_kind.as_deref() == Some("run_terminal") => {
                        let run_id = match occurrence.as_deref() {
                            Some(occurrence_id) => {
                                failure_occurrence_run(&connection, occurrence_id)?
                            }
                            None => None,
                        };
                        match run_id {
                            Some(run_id) => failure_run_cost_columns(&connection, &run_id)?,
                            None => None,
                        }
                    }
                    _ => None,
                };
                match effective_cost {
                    Some((scope, lower, upper)) => {
                        scopes.entry(scope).or_default().insert(
                            entry.cluster_id.clone(),
                            (lower.max(0) as u64, upper.max(0) as u64),
                        );
                    }
                    None => entry.unknown_cost_occurrences += 1,
                }
            }
        }
        // A cost scope appearing in multiple cluster lineages is ambiguous;
        // never award it to an arbitrary first cluster.
        for affected in scopes.values() {
            if affected.len() == 1 {
                let (cluster_id, (lower, upper)) = affected.first_key_value().expect("one entry");
                let cluster = grouped.get_mut(cluster_id).expect("known cluster");
                cluster.cost_lower_microusd = cluster.cost_lower_microusd.saturating_add(*lower);
                cluster.cost_upper_microusd = cluster.cost_upper_microusd.saturating_add(*upper);
            } else {
                for cluster_id in affected.keys() {
                    if let Some(cluster) = grouped.get_mut(cluster_id) {
                        cluster.unknown_cost_occurrences =
                            cluster.unknown_cost_occurrences.saturating_add(1);
                    }
                }
            }
        }
        for cluster in grouped.values_mut() {
            if let Some(occurrence) = &cluster.representative_occurrence_id {
                let mut statement = connection.prepare("SELECT DISTINCT coalesce((SELECT class FROM failure_classification_revisions r WHERE r.occurrence_id=o.id ORDER BY revision DESC LIMIT 1),o.automatic_class),o.severity FROM failure_occurrences o JOIN failure_cluster_membership_revisions m ON m.occurrence_id=o.id WHERE m.cluster_id=?1 AND m.revision=(SELECT max(m2.revision) FROM failure_cluster_membership_revisions m2 WHERE m2.occurrence_id=o.id) ORDER BY 1,2")?;
                let values = statement
                    .query_map([cluster.cluster_id.as_str()], |r| {
                        Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
                    })?
                    .collect::<Result<Vec<_>, _>>()?;
                let classes = values
                    .iter()
                    .map(|(class, _)| class)
                    .collect::<BTreeSet<_>>();
                cluster.effective_class = (classes.len() == 1)
                    .then(|| classes.iter().next().expect("one").to_string())
                    .or_else(|| Some("unknown".to_owned()));
                cluster.severity = values
                    .iter()
                    .map(|(_, severity)| severity.as_str())
                    .max_by_key(|severity| severity_rank(severity))
                    .map(str::to_owned);
                cluster.representative_run_id = failure_occurrence_run(&connection, occurrence)?;
                if let Some(run_id) = &cluster.representative_run_id {
                    cluster.representative_trace_id = connection.query_row("SELECT aggregate_id FROM improvement_current_revisions WHERE aggregate_kind='trace' AND json_extract(payload_json,'$.run_id')=?1 ORDER BY created_at DESC LIMIT 1", [run_id.as_str()], |r| r.get(0)).optional()?;
                }
            }
        }
        let mut overview = grouped.into_values().collect::<Vec<_>>();
        overview.sort_by(|a, b| {
            b.cost_upper_microusd
                .cmp(&a.cost_upper_microusd)
                .then_with(|| b.occurrences.cmp(&a.occurrences))
                .then_with(|| a.cluster_id.cmp(&b.cluster_id))
        });
        Ok(overview)
    }

    pub fn failure_trace_summary(
        &self,
        occurrence_id: &str,
    ) -> Result<FailureTraceSummary, StoreError> {
        let connection = self.connection()?;
        connection.query_row("SELECT id,source_kind,source_id,source_domain_event_id,automatic_class,severity FROM failure_occurrences WHERE id=?1", [occurrence_id], |row| {
            let source_kind: String = row.get(1)?;
            let source_id: String = row.get(2)?;
            let source_domain_event_id: Option<i64> = row.get(3)?;
            let source_receipt_sha256 = sha256(
                format!(
                    "failure-source.v2\0{source_kind}\0{source_id}\0{}",
                    source_domain_event_id.map_or_else(String::new, |id| id.to_string())
                )
                .as_bytes(),
            );
            Ok(FailureTraceSummary { occurrence_id: row.get(0)?, source_receipt_sha256, source_kind, source_domain_event_id, automatic_class: row.get(4)?, severity: row.get(5)? })
        }).map_err(Into::into)
    }

    pub fn failure_trace_composition(
        &self,
        trace_id: &str,
    ) -> Result<FailureTraceComposition, StoreError> {
        validate_failure_identifier(trace_id)?;
        let record = self
            .improvement_current_revision(ImprovementRecordKind::Trace, trace_id)?
            .ok_or_else(|| StoreError::NotFound(format!("trace {trace_id}")))?;
        if record.schema != ImprovementSchema::TraceV2 {
            return Err(StoreError::Validation(
                "stored trace schema is not TraceV2".to_owned(),
            ));
        }
        let manifest = record.payload;
        if manifest.get("schema").and_then(Value::as_str) != Some("harness.trace.v2") {
            return Err(StoreError::Validation(
                "stored trace is not TraceV2".to_owned(),
            ));
        }
        let run_id = RunId::from(
            manifest
                .get("run_id")
                .and_then(Value::as_str)
                .filter(|v| safe_outcome_identifier(v, 128))
                .ok_or_else(|| StoreError::Validation("invalid persisted trace run_id".to_owned()))?
                .to_owned(),
        );
        let connection = self.connection()?;
        Ok(FailureTraceComposition {
            trace_id: trace_id.to_owned(),
            outcomes: outcome_vector_conn(&connection, &run_id)?,
            run_id,
            trace_manifest: manifest,
        })
    }

    pub fn merge_failure_clusters(
        &self,
        source_cluster_id: &str,
        expected_source_version: u64,
        target_cluster_id: &str,
        expected_target_version: u64,
        actor: &str,
        reason: EditReason,
    ) -> Result<(), StoreError> {
        if source_cluster_id == target_cluster_id {
            return Err(StoreError::Conflict(
                "cannot merge a cluster into itself".to_owned(),
            ));
        }
        validate_failure_actor(actor)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let source_repo = failure_cluster_repository(&transaction, source_cluster_id)?;
        if source_repo != failure_cluster_repository(&transaction, target_cluster_id)? {
            return Err(StoreError::Conflict(
                "failure merge crosses repositories".to_owned(),
            ));
        }
        require_failure_cluster_version(&transaction, source_cluster_id, expected_source_version)?;
        require_failure_cluster_version(&transaction, target_cluster_id, expected_target_version)?;
        for occurrence_id in current_failure_members(&transaction, source_cluster_id)? {
            append_failure_membership_tx(
                &transaction,
                &occurrence_id,
                target_cluster_id,
                MembershipAction::Merged,
                actor,
                reason,
            )?;
        }
        append_failure_cluster_edit(
            &transaction,
            source_cluster_id,
            Some(target_cluster_id),
            "merged",
            actor,
            reason,
            &[target_cluster_id],
        )?;
        bump_failure_cluster(&transaction, source_cluster_id)?;
        bump_failure_cluster(&transaction, target_cluster_id)?;
        transaction.commit()?;
        Ok(())
    }

    pub fn split_failure_cluster(
        &self,
        source_cluster_id: &str,
        expected_source_version: u64,
        moves: &[FailureSplitMove],
        actor: &str,
        reason: EditReason,
    ) -> Result<(), StoreError> {
        if moves.is_empty() {
            return Err(StoreError::Validation(
                "failure split needs at least one move".to_owned(),
            ));
        }
        validate_failure_actor(actor)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        require_failure_cluster_version(&transaction, source_cluster_id, expected_source_version)?;
        let repository = failure_cluster_repository(&transaction, source_cluster_id)?;
        let members = current_failure_members(&transaction, source_cluster_id)?;
        let mut seen = BTreeSet::new();
        let mut targets = BTreeSet::new();
        for movement in moves {
            if movement.target_cluster_id == source_cluster_id
                || !seen.insert(&movement.occurrence_id)
                || !members.contains(&movement.occurrence_id)
            {
                return Err(StoreError::Conflict(
                    "invalid failure split membership".to_owned(),
                ));
            }
            if repository != failure_cluster_repository(&transaction, &movement.target_cluster_id)?
            {
                return Err(StoreError::Conflict(
                    "failure split crosses repositories".to_owned(),
                ));
            }
            require_failure_cluster_version(
                &transaction,
                &movement.target_cluster_id,
                movement.expected_target_version,
            )?;
            targets.insert(movement.target_cluster_id.as_str());
        }
        for movement in moves {
            append_failure_membership_tx(
                &transaction,
                &movement.occurrence_id,
                &movement.target_cluster_id,
                MembershipAction::Split,
                actor,
                reason,
            )?;
        }
        let targets = targets.into_iter().collect::<Vec<_>>();
        append_failure_cluster_edit(
            &transaction,
            source_cluster_id,
            None,
            "split",
            actor,
            reason,
            &targets,
        )?;
        bump_failure_cluster(&transaction, source_cluster_id)?;
        for target in targets {
            bump_failure_cluster(&transaction, target)?;
        }
        transaction.commit()?;
        Ok(())
    }

    fn append_failure_membership(
        &self,
        occurrence_id: &str,
        cluster_id: &str,
        expected_cluster_version: u64,
        action: MembershipAction,
        actor: &str,
        reason: EditReason,
    ) -> Result<(), StoreError> {
        validate_failure_actor(actor)?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let occurrence_repository: String = transaction
            .query_row(
                "SELECT repository_id FROM failure_occurrences WHERE id=?1",
                [occurrence_id],
                |row| row.get(0),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("failure occurrence {occurrence_id}")))?;
        if occurrence_repository != failure_cluster_repository(&transaction, cluster_id)? {
            return Err(StoreError::Conflict(
                "failure membership crosses repositories".to_owned(),
            ));
        }
        require_failure_cluster_version(&transaction, cluster_id, expected_cluster_version)?;
        let prior: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM failure_cluster_membership_revisions WHERE occurrence_id=?1)",
            [occurrence_id],
            |row| row.get(0),
        )?;
        if prior {
            return Err(StoreError::Conflict(
                "failure occurrence already has cluster lineage".to_owned(),
            ));
        }
        append_failure_membership_tx(
            &transaction,
            occurrence_id,
            cluster_id,
            action,
            actor,
            reason,
        )?;
        bump_failure_cluster(&transaction, cluster_id)?;
        transaction.commit()?;
        Ok(())
    }
    /// Project closed outcome labels from durable Store authorities only.
    /// It intentionally has no API-shaped input and never creates a human action.
    pub fn project_authoritative_outcomes(&self, run_id: &RunId) -> Result<(), StoreError> {
        let connection = self.connection()?;
        let mut inputs = Vec::new();
        let mut validations = connection.prepare(
            "SELECT id,task_attempt_id,validator_id,proof_tier,result_class,source_sha,command_run_id,started_at,completed_at FROM validations WHERE run_id=?1 AND state='completed' AND result_class IS NOT NULL AND invalidated_at IS NULL ORDER BY id",
        )?;
        for row in validations.query_map([run_id.as_str()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, Option<i64>>(7)?,
                row.get::<_, i64>(8)?,
            ))
        })? {
            let (
                id,
                task_attempt_id,
                validator_id,
                proof_tier,
                result,
                source_sha,
                command_run_id,
                started_at,
                observed_at,
            ) = row?;
            let (classification, code) = authoritative_result_label(&result);
            let (dimension, code) = if validator_id == "draft-pr-required-ci" {
                (harness_domain::OutcomeDimension::CiRequiredChecks, code)
            } else {
                (harness_domain::OutcomeDimension::Validation, code)
            };
            let receipt = json!({
                "id": id, "task_attempt_id": task_attempt_id, "validator_id": validator_id,
                "proof_tier": proof_tier, "result_class": result, "source_sha": source_sha,
                "command_run_id": command_run_id, "started_at": started_at, "completed_at": observed_at,
            });
            inputs.push(AuthoritativeOutcomeInput {
                run_id: run_id.clone(),
                subject: outcome_subject(run_id, task_attempt_id),
                dimension,
                classification,
                code: code.to_owned(),
                source_kind: harness_domain::OutcomeSourceKind::Validation,
                source_record_sha256: sha256(serde_json::to_string(&receipt)?.as_bytes()),
                source_record_id: id,
                source_sha: Some(source_sha),
                source_domain_event_id: None,
                observed_at,
            });
        }
        drop(validations);
        let mut evidence = connection.prepare(
            "SELECT id,task_attempt_id,result_class,evidence_sha256,source_sha,created_at FROM evidence_records WHERE run_id=?1 AND invalidated_at IS NULL ORDER BY id",
        )?;
        for row in evidence.query_map([run_id.as_str()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, i64>(5)?,
            ))
        })? {
            let (id, task_attempt_id, result, evidence_sha256, source_sha, observed_at) = row?;
            let (classification, code) = authoritative_evidence_label(&result);
            inputs.push(AuthoritativeOutcomeInput {
                run_id: run_id.clone(),
                subject: outcome_subject(run_id, task_attempt_id),
                dimension: harness_domain::OutcomeDimension::Evidence,
                classification,
                code: code.to_owned(),
                source_kind: harness_domain::OutcomeSourceKind::Evidence,
                source_record_id: id,
                source_record_sha256: evidence_sha256,
                source_sha: Some(source_sha),
                source_domain_event_id: None,
                observed_at,
            });
        }
        drop(evidence);
        let mut findings = connection.prepare(
            "SELECT id,task_attempt_id,severity,state,created_at FROM findings WHERE run_id=?1 AND verifier_agent_session_id IS NOT NULL ORDER BY id",
        )?;
        for row in findings.query_map([run_id.as_str()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
            ))
        })? {
            let (id, task_attempt_id, severity, state, observed_at) = row?;
            let code = if matches!(severity.as_str(), "high" | "critical") {
                "blocking"
            } else {
                "nonblocking"
            };
            inputs.push(AuthoritativeOutcomeInput {
                run_id: run_id.clone(),
                subject: outcome_subject(run_id, task_attempt_id),
                dimension: harness_domain::OutcomeDimension::VerifierFindings,
                classification: harness_domain::OutcomeClassification::Negative,
                code: code.to_owned(),
                source_kind: harness_domain::OutcomeSourceKind::Finding,
                source_record_sha256: sha256(
                    format!("finding\0{id}\0{severity}\0{state}").as_bytes(),
                ),
                source_record_id: id,
                source_sha: None,
                source_domain_event_id: None,
                observed_at,
            });
        }
        drop(findings);
        let (budget, sample_count, total_tokens): (Option<i64>, i64, i64) = connection.query_row(
            "SELECT r.run_token_budget,count(ts.id),coalesce(sum(ts.total_tokens),0) FROM runs r LEFT JOIN agent_sessions a ON a.run_id=r.id LEFT JOIN codex_threads ct ON ct.agent_session_id=a.id LEFT JOIN token_samples ts ON ts.thread_id=ct.thread_id WHERE r.id=?1 GROUP BY r.id",
            [run_id.as_str()],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        let mut lifecycle = connection.prepare(
            "SELECT id,occurred_at,payload_json FROM domain_events WHERE run_id=?1 AND event_type='run.lifecycle.transitioned' ORDER BY id",
        )?;
        for row in lifecycle.query_map([run_id.as_str()], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
            ))
        })? {
            let (id, observed_at, payload_json) = row?;
            let payload: Value = serde_json::from_str(&payload_json)?;
            let Some(next) = payload.get("next_state").and_then(Value::as_str) else {
                continue;
            };
            let code = match next {
                "COMPLETED" => "completed",
                "BLOCKED" => "blocked",
                "CANCELED" => "stopped",
                _ => continue,
            };
            inputs.push(AuthoritativeOutcomeInput {
                run_id: run_id.clone(),
                subject: harness_domain::OutcomeSubject {
                    kind: harness_domain::OutcomeSubjectKind::Run,
                    id: run_id.to_string(),
                },
                dimension: harness_domain::OutcomeDimension::CompletionState,
                classification: harness_domain::OutcomeClassification::Neutral,
                code: code.to_owned(),
                source_kind: harness_domain::OutcomeSourceKind::DomainEvent,
                source_record_id: id.to_string(),
                source_record_sha256: sha256(payload_json.as_bytes()),
                source_sha: None,
                source_domain_event_id: Some(id),
                observed_at,
            });
            let (classification, resource_code) = match budget.filter(|budget| *budget > 0) {
                _ if sample_count == 0 => (
                    harness_domain::OutcomeClassification::Unknown,
                    "unavailable",
                ),
                Some(budget) if total_tokens > budget => (
                    harness_domain::OutcomeClassification::Negative,
                    "budget_exceeded",
                ),
                Some(_) => (
                    harness_domain::OutcomeClassification::Neutral,
                    "within_budget",
                ),
                None => (
                    harness_domain::OutcomeClassification::Unknown,
                    "unavailable",
                ),
            };
            let ledger = json!({"budget": budget, "sample_count": sample_count, "total_tokens": total_tokens});
            inputs.push(AuthoritativeOutcomeInput {
                run_id: run_id.clone(),
                subject: harness_domain::OutcomeSubject {
                    kind: harness_domain::OutcomeSubjectKind::Run,
                    id: run_id.to_string(),
                },
                dimension: harness_domain::OutcomeDimension::ResourceUse,
                classification,
                code: resource_code.to_owned(),
                source_kind: harness_domain::OutcomeSourceKind::DomainEvent,
                source_record_id: id.to_string(),
                source_record_sha256: sha256(
                    format!("{}\0{}", payload_json, serde_json::to_string(&ledger)?).as_bytes(),
                ),
                source_sha: None,
                source_domain_event_id: Some(id),
                observed_at,
            });
        }
        drop(lifecycle);
        let mut verified = connection.prepare(
            "SELECT id,occurred_at,payload_json FROM domain_events WHERE run_id=?1 AND event_type='task.verified' ORDER BY id",
        )?;
        for row in verified.query_map([run_id.as_str()], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, String>(2)?,
            ))
        })? {
            let (id, observed_at, payload_json) = row?;
            inputs.push(AuthoritativeOutcomeInput {
                run_id: run_id.clone(),
                subject: harness_domain::OutcomeSubject {
                    kind: harness_domain::OutcomeSubjectKind::Run,
                    id: run_id.to_string(),
                },
                dimension: harness_domain::OutcomeDimension::VerifierFindings,
                classification: harness_domain::OutcomeClassification::Positive,
                code: "none".to_owned(),
                source_kind: harness_domain::OutcomeSourceKind::DomainEvent,
                source_record_id: id.to_string(),
                source_record_sha256: sha256(payload_json.as_bytes()),
                source_sha: None,
                source_domain_event_id: Some(id),
                observed_at,
            });
        }
        drop(verified);
        drop(connection);
        for input in inputs {
            self.record_authoritative_outcome(&input)?;
        }
        Ok(())
    }

    fn record_authoritative_outcome(
        &self,
        input: &AuthoritativeOutcomeInput,
    ) -> Result<(), StoreError> {
        harness_domain::validate_outcome_label(input.dimension, input.classification, &input.code)
            .map_err(|error| StoreError::Validation(error.to_string()))?;
        let outcome_id = stable_authoritative_outcome_id(input)?;
        let payload = OutcomeWireV1 {
            schema: "harness.outcome.v1".to_owned(),
            outcome_id: outcome_id.clone(),
            run_id: input.run_id.clone(),
            subject: input.subject.clone(),
            dimension: input.dimension,
            classification: input.classification,
            code: input.code.clone(),
            observed_at: input.observed_at,
            confidence: OutcomeConfidence::Authoritative,
            source: OutcomeSource {
                kind: input.source_kind,
                record_id: input.source_record_id.clone(),
                record_sha256: input.source_record_sha256.clone(),
                source_sha: input.source_sha.clone(),
                source_domain_event_id: input.source_domain_event_id,
            },
            supersedes: Vec::new(),
            reason_code: None,
            correction_artifact_id: None,
            redactor_version: "outcome-redactor.v1".to_owned(),
            free_text_redacted: false,
        };
        payload.validate().map_err(|error| {
            StoreError::Validation(format!("invalid authoritative outcome: {error}"))
        })?;
        let raw = serde_json::to_value(&payload)?;
        let raw_sha256 = sha256(serde_json::to_string(&raw)?.as_bytes());
        let classification = serde_json::to_string(&input.classification)?;
        let identity = sha256(
            format!(
                "harness.outcome.authoritative.v1\0mapping-v1\0{}\0{}\0{}\0{}",
                outcome_id, input.source_record_sha256, classification, input.code
            )
            .as_bytes(),
        );
        self.append_improvement_revision(&NewImprovementRevision {
            id: format!("outcome-revision-{identity}"),
            aggregate_kind: ImprovementRecordKind::Outcome,
            aggregate_id: outcome_id.to_string(),
            schema: ImprovementSchema::OutcomeV1,
            state: ImprovementState::Observed,
            payload: raw,
            payload_sha256: raw_sha256,
            sensitivity: SensitivityClass::Internal,
            retention_class: RetentionClass::Governance,
            export_allowed: false,
            idempotency_key: format!("outcome:authoritative:{identity}"),
            event_id: harness_domain::ImprovementEventId::from(format!("outcome-event-{identity}")),
            source_raw_event_id: None,
            source_domain_event_id: input.source_domain_event_id,
        })?;
        Ok(())
    }

    pub fn record_operator_outcome(
        &self,
        input: &NewOperatorOutcome,
    ) -> Result<OutcomeRevisionReceipt, StoreError> {
        validate_operator_outcome_input(input)?;
        validate_operator_outcome_label(input.dimension, input.classification, &input.code)
            .map_err(|e| StoreError::Validation(e.to_string()))?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let exists: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM runs WHERE id=?1)",
            [input.run_id.as_str()],
            |r| r.get(0),
        )?;
        if !exists {
            return Err(StoreError::NotFound(format!("run {}", input.run_id)));
        }
        let subject_ok: bool = match input.subject.kind { harness_domain::OutcomeSubjectKind::Run => input.subject.id == input.run_id.as_str(), harness_domain::OutcomeSubjectKind::TaskAttempt => transaction.query_row("SELECT EXISTS(SELECT 1 FROM task_attempts a JOIN tasks t ON t.id=a.task_id WHERE a.id=?1 AND t.run_id=?2)", params![input.subject.id,input.run_id.as_str()], |r| r.get(0))?, harness_domain::OutcomeSubjectKind::Publication => transaction.query_row("SELECT EXISTS(SELECT 1 FROM publications WHERE id=?1 AND run_id=?2)", params![input.subject.id,input.run_id.as_str()], |r| r.get(0))? };
        if !subject_ok {
            return Err(StoreError::Validation(
                "outcome subject is not owned by run".to_owned(),
            ));
        }
        if let Some(artifact) = &input.correction_artifact_id {
            let ok: bool = transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM artifacts WHERE id=?1 AND run_id=?2)",
                params![artifact.as_str(), input.run_id.as_str()],
                |r| r.get(0),
            )?;
            if !ok {
                return Err(StoreError::NotFound(format!("artifact {artifact}")));
            }
        }
        let outcome_id = stable_outcome_id(input)?;
        let action_payload = json!({"dimension":input.dimension,"classification":input.classification,"code":input.code,"reason_code":input.reason_code,"note_redacted":input.note.is_some()});
        let action_json = serde_json::to_string(&action_payload)?;
        let action_sha = sha256(action_json.as_bytes());
        let now = now_ms();
        let identity = sha256(
            format!(
                "harness.outcome.replay.v1\0{}\0{}\0{}",
                input.idempotency_key, outcome_id, action_json
            )
            .as_bytes(),
        );
        let revision_id = format!("outcome-revision-{identity}");
        let event_id = format!("outcome-event-{identity}");
        let replay: Option<String> = transaction
            .query_row(
                "SELECT revision_id FROM improvement_events WHERE idempotency_key=?1",
                [&input.idempotency_key],
                |r| r.get(0),
            )
            .optional()?;
        if let Some(existing_revision_id) = replay {
            let (stored_id, stored_raw): (String, String) = transaction.query_row(
                "SELECT id,payload_json FROM improvement_revisions WHERE id=?1 AND aggregate_kind='outcome'",
                [&existing_revision_id], |r| Ok((r.get(0)?, r.get(1)?)),
            )?;
            let stored: OutcomeWireV1 = serde_json::from_str(&stored_raw)?;
            let event_matches: bool = transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM improvement_events WHERE idempotency_key=?1 AND id=?2 AND revision_id=?3 AND aggregate_kind='outcome' AND aggregate_id=?4 AND event_type='revision_recorded')",
                params![input.idempotency_key, event_id, revision_id, outcome_id.as_str()], |r| r.get(0),
            )?;
            let action_matches: bool = transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM human_actions WHERE id=?1 AND run_id=?2 AND actor=?3 AND action_type='operator_outcome' AND target_type='outcome' AND target_id=?4 AND payload_json=?5 AND payload_sha256=?6)",
                params![stored.source.record_id,input.run_id.as_str(),input.actor,outcome_id.as_str(),action_json,action_sha], |r| r.get(0),
            )?;
            if stored_id != revision_id
                || !event_matches
                || !action_matches
                || !outcome_matches_input(&stored, input, &outcome_id)
            {
                return Err(StoreError::Conflict(
                    "operator outcome idempotency key was reused with different content".to_owned(),
                ));
            }
            let vector = outcome_vector_tx(&transaction, &input.run_id)?;
            let revision: i64 = transaction.query_row(
                "SELECT revision FROM improvement_revisions WHERE id=?1",
                [&existing_revision_id],
                |r| r.get(0),
            )?;
            transaction.commit()?;
            return Ok(OutcomeRevisionReceipt {
                outcome_id,
                revision_id: existing_revision_id,
                revision: positive_database_u64(revision, "outcome revision")?,
                vector,
            });
        }
        validate_outcome_supersedes(&transaction, &outcome_id, &input.supersedes)?;
        transaction.execute("INSERT INTO human_actions(run_id,actor,action_type,target_type,target_id,occurred_at,payload_json,payload_sha256) VALUES(?1,?2,'operator_outcome','outcome',?3,?4,?5,?6)", params![input.run_id.as_str(),input.actor,outcome_id.as_str(),now,action_json,action_sha])?;
        let action_id = transaction.last_insert_rowid();
        let payload = OutcomeWireV1 {
            schema: "harness.outcome.v1".to_owned(),
            outcome_id: outcome_id.clone(),
            run_id: input.run_id.clone(),
            subject: input.subject.clone(),
            dimension: input.dimension,
            classification: input.classification,
            code: input.code.clone(),
            observed_at: now,
            confidence: OutcomeConfidence::OperatorAsserted,
            source: OutcomeSource {
                kind: OutcomeSourceKind::HumanAction,
                record_id: action_id.to_string(),
                record_sha256: action_sha,
                source_sha: None,
                source_domain_event_id: None,
            },
            supersedes: input.supersedes.clone(),
            reason_code: input.reason_code.clone(),
            correction_artifact_id: input.correction_artifact_id.clone(),
            redactor_version: "outcome-redactor.v1".to_owned(),
            free_text_redacted: input.note.is_some(),
        };
        payload.validate().map_err(|error| {
            StoreError::Validation(format!("invalid operator outcome: {error}"))
        })?;
        // Persist the canonical JSON `Value` representation used by the
        // generic immutable-revision validator (object key order is part of
        // the Store digest convention).
        let raw = serde_json::to_string(&serde_json::to_value(&payload)?)?;
        let digest = sha256(raw.as_bytes());
        let rev:i64=transaction.query_row("SELECT coalesce(max(revision),0)+1 FROM improvement_revisions WHERE aggregate_kind='outcome' AND aggregate_id=?1", [outcome_id.as_str()], |r| r.get(0))?;
        transaction.execute("INSERT INTO improvement_revisions(id,aggregate_kind,aggregate_id,revision,schema_name,lifecycle_state,payload_json,payload_sha256,sensitivity,retention_class,export_allowed,created_at) VALUES(?1,'outcome',?2,?3,'harness.outcome.v1','observed',?4,?5,'internal','governance',0,?6)", params![revision_id,outcome_id.as_str(),rev,raw,digest,now])?;
        transaction.execute("INSERT INTO improvement_events(id,aggregate_kind,aggregate_id,revision_id,sequence,event_type,payload_json,payload_sha256,idempotency_key,occurred_at) VALUES(?1,'outcome',?2,?3,?4,'revision_recorded','{}',?5,?6,?7)", params![event_id,outcome_id.as_str(),revision_id,rev,sha256(b"{}"),input.idempotency_key,now])?;
        let vector = outcome_vector_tx(&transaction, &input.run_id)?;
        transaction.commit()?;
        Ok(OutcomeRevisionReceipt {
            outcome_id,
            revision_id,
            revision: positive_database_u64(rev, "outcome revision")?,
            vector,
        })
    }
    pub fn outcome_vector(&self, run_id: &RunId) -> Result<OutcomeVector, StoreError> {
        let c = self.connection()?;
        outcome_vector_conn(&c, run_id)
    }
    pub fn outcome_history(&self, outcome_id: &OutcomeId) -> Result<OutcomeHistory, StoreError> {
        let c = self.connection()?;
        let record = c.query_row("SELECT id,aggregate_kind,aggregate_id,revision,schema_name,lifecycle_state,payload_json,payload_sha256,sensitivity,retention_class,export_allowed,source_domain_event_id,created_at FROM improvement_revisions WHERE aggregate_kind='outcome' AND aggregate_id=?1 ORDER BY revision LIMIT 1",[outcome_id.as_str()],map_improvement_revision)?;
        let outcome: OutcomeWireV1 = serde_json::from_value(record.payload)?;
        outcome
            .validate()
            .map_err(|error| StoreError::Validation(format!("invalid stored outcome: {error}")))?;
        let run = outcome.run_id.to_string();
        let v = outcome_vector_conn(&c, &RunId::from(run.clone()))?;
        let item = v
            .items
            .into_iter()
            .find(|x| x.outcome_id == *outcome_id)
            .ok_or_else(|| StoreError::NotFound(outcome_id.to_string()))?;
        Ok(OutcomeHistory {
            outcome_id: outcome_id.clone(),
            run_id: RunId::from(run),
            revisions: item.revisions,
            conflicted: item.conflicted,
        })
    }
    pub fn trace_projection_candidate_runs(&self) -> Result<Vec<RunId>, StoreError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare("SELECT id FROM runs ORDER BY created_at,id")?;
        statement
            .query_map([], |row| Ok(RunId::from(row.get::<_, String>(0)?)))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }
    /// Return one consistent, read-only receipt snapshot for a historical run.
    /// Raw rows without `run_id` are included only when their agent session is
    /// durably bound to this run, which covers legacy child-agent rows.
    pub fn trace_projection_snapshot(
        &self,
        run_id: &RunId,
    ) -> Result<TraceProjectionSnapshot, StoreError> {
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let (base_sha, authority_digest, profile_digest): (String, String, String) = transaction
            .query_row(
                "SELECT base_sha,authority_digest,profile_digest FROM runs WHERE id=?1",
                [run_id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )?;
        let mut raw_statement = transaction.prepare("SELECT re.id,re.agent_session_id,re.thread_id,re.turn_id,re.direction,re.method,re.request_id,re.received_at,re.payload_json,re.payload_sha256,re.source_sequence,re.redaction_class FROM raw_events re LEFT JOIN agent_sessions a ON a.id=re.agent_session_id WHERE re.run_id=?1 OR (re.run_id IS NULL AND a.run_id=?1) ORDER BY re.id")?;
        let raw_events = raw_statement
            .query_map([run_id.as_str()], |row| {
                let payload: Value =
                    serde_json::from_str(&row.get::<_, String>(8)?).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            8,
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })?;
                let payload_sha256: String = row.get(9)?;
                if sha256(
                    serde_json::to_string(&payload)
                        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?
                        .as_bytes(),
                ) != payload_sha256
                {
                    return Err(rusqlite::Error::FromSqlConversionFailure(
                        9,
                        rusqlite::types::Type::Text,
                        "raw payload digest mismatch".into(),
                    ));
                }
                Ok(TraceProjectionRawReceipt {
                    id: row.get(0)?,
                    agent_session_id: row.get(1)?,
                    thread_id: row.get(2)?,
                    turn_id: row.get(3)?,
                    direction: row.get(4)?,
                    method: row.get(5)?,
                    request_id: row.get(6)?,
                    received_at: row.get(7)?,
                    payload,
                    payload_sha256,
                    source_sequence: row.get(10)?,
                    redaction_class: row.get(11)?,
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        drop(raw_statement);
        let mut domain_statement = transaction.prepare("SELECT id,event_type,occurred_at,payload_json,source_raw_event_id FROM domain_events WHERE run_id=?1 ORDER BY id")?;
        let domain_events = domain_statement
            .query_map([run_id.as_str()], |row| {
                let payload: Value =
                    serde_json::from_str(&row.get::<_, String>(3)?).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            3,
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })?;
                let payload_sha256 = sha256(
                    serde_json::to_string(&payload)
                        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?
                        .as_bytes(),
                );
                Ok(TraceProjectionDomainReceipt {
                    id: row.get(0)?,
                    source_raw_event_id: row.get(4)?,
                    event_type: row.get(1)?,
                    occurred_at: row.get(2)?,
                    payload,
                    payload_sha256,
                    redaction_class: "none".to_owned(),
                })
            })?
            .collect::<Result<Vec<_>, _>>()?;
        drop(domain_statement);
        let mut structural = vec![TraceProjectionStructuralReceipt {
            id: format!("run:{run_id}"),
            kind: "run".to_owned(),
            occurred_at: None,
            metadata: json!({"base_sha":base_sha,"authority_digest":authority_digest,"profile_digest":profile_digest}),
        }];
        let mut statement = transaction.prepare("SELECT a.id,a.state,a.base_sha,a.head_sha,a.terminal_class,a.created_at,a.completed_at FROM task_attempts a JOIN tasks t ON t.id=a.task_id WHERE t.run_id=?1 ORDER BY a.created_at,a.id")?;
        for row in statement.query_map([run_id.as_str()], |row| Ok(TraceProjectionStructuralReceipt { id: format!("attempt:{}", row.get::<_, String>(0)?), kind: "attempt".to_owned(), occurred_at: row.get(5)?, metadata: json!({"state":row.get::<_, String>(1)?,"base_sha":row.get::<_, String>(2)?,"head_sha":row.get::<_, Option<String>>(3)?,"terminal_class":row.get::<_, Option<String>>(4)?,"completed_at":row.get::<_, Option<i64>>(6)?}) }))? { structural.push(row?); }
        drop(statement);
        let mut statement = transaction.prepare("SELECT id,parent_agent_session_id,task_attempt_id,role,state,effective_model,requested_model,effective_reasoning_effort,requested_reasoning_effort,started_at,completed_at FROM agent_sessions WHERE run_id=?1 ORDER BY started_at,id")?;
        for row in statement.query_map([run_id.as_str()], |row| Ok(TraceProjectionStructuralReceipt { id: format!("agent:{}", row.get::<_, String>(0)?), kind: "agent".to_owned(), occurred_at: row.get(9)?, metadata: json!({"parent_agent_session_id":row.get::<_, Option<String>>(1)?,"task_attempt_id":row.get::<_, Option<String>>(2)?,"role":row.get::<_, String>(3)?,"state":row.get::<_, String>(4)?,"model":row.get::<_, Option<String>>(5)?.or(row.get(6)?),"reasoning_effort":row.get::<_, Option<String>>(7)?.or(row.get(8)?),"completed_at":row.get::<_, Option<i64>>(10)?}) }))? { structural.push(row?); }
        drop(statement);
        let structural_json = serde_json::to_string(&structural)?;
        let mut relations = Vec::new();
        for raw in &raw_events {
            if let Some(agent) = &raw.agent_session_id {
                relations.push(crate::TraceProjectionRelation {
                    from: format!("structural:agent:{agent}"),
                    to: format!("raw:{}", raw.id),
                    kind: "context_parent".to_owned(),
                });
            }
        }
        let mut parent_agents = transaction.prepare(
            "SELECT id,parent_agent_session_id FROM agent_sessions WHERE run_id=?1 AND parent_agent_session_id IS NOT NULL ORDER BY id",
        )?;
        for row in parent_agents.query_map([run_id.as_str()], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })? {
            let (child, parent) = row?;
            relations.push(crate::TraceProjectionRelation {
                from: format!("structural:agent:{parent}"),
                to: format!("structural:agent:{child}"),
                kind: "spawned_by".to_owned(),
            });
        }
        drop(parent_agents);
        for domain in &domain_events {
            if let Some(raw) = domain.source_raw_event_id {
                relations.push(crate::TraceProjectionRelation {
                    from: format!("raw:{raw}"),
                    to: format!("domain:{}", domain.id),
                    kind: "derived_from".to_owned(),
                });
            }
        }
        let snapshot = TraceProjectionSnapshot {
            run_id: run_id.clone(),
            base_sha,
            authority_digest,
            profile_digest,
            max_raw_event_id: raw_events.last().map_or(0, |row| row.id),
            max_domain_event_id: domain_events.last().map_or(0, |row| row.id),
            structural_digest: sha256(structural_json.as_bytes()),
            raw_events,
            domain_events,
            structural_receipts: structural,
            relations,
        };
        transaction.commit()?;
        Ok(snapshot)
    }
    pub fn append_improvement_revision(
        &self,
        input: &NewImprovementRevision,
    ) -> Result<(ImprovementRevisionRecord, ImprovementEventRecord), StoreError> {
        validate_improvement_input(input)?;
        let payload_json = serde_json::to_string(&input.payload)?;
        let now = now_ms();
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        if let Some((revision_id, event_id)) = transaction
            .query_row(
                "SELECT revision_id,id FROM improvement_events WHERE idempotency_key=?1",
                [&input.idempotency_key],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?
        {
            let record = read_improvement_revision(&transaction, &revision_id)?;
            if record.aggregate_kind != input.aggregate_kind
                || record.aggregate_id != input.aggregate_id
                || record.payload_sha256 != input.payload_sha256
                || record.schema != input.schema
                || record.state != input.state
                || record.sensitivity != input.sensitivity
                || record.retention_class != input.retention_class
                || record.export_allowed != input.export_allowed
                || record.source_domain_event_id != input.source_domain_event_id
            {
                return Err(StoreError::Conflict(
                    "improvement idempotency key was reused with different content".to_owned(),
                ));
            }
            let event = read_improvement_event(&transaction, &event_id)?;
            if event.id != input.event_id || event.source_raw_event_id != input.source_raw_event_id
            {
                return Err(StoreError::Conflict(
                    "improvement idempotency key was reused with different event provenance"
                        .to_owned(),
                ));
            }
            transaction.commit()?;
            return Ok((record, event));
        }
        if let Some((prior_state, prior_sensitivity, prior_retention, prior_export_allowed)) = transaction.query_row(
            "SELECT lifecycle_state,sensitivity,retention_class,export_allowed FROM improvement_current_revisions WHERE aggregate_kind=?1 AND aggregate_id=?2",
            params![enum_text(&input.aggregate_kind)?, input.aggregate_id],
            |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?, row.get::<_, bool>(3)?)),
        ).optional()? {
            let prior_state = parse_improvement_enum(prior_state)?;
            let prior_sensitivity = parse_improvement_enum(prior_sensitivity)?;
            let prior_retention = parse_improvement_enum(prior_retention)?;
            if !improvement_transition_allowed(input.aggregate_kind, prior_state, input.state)
                || sensitivity_rank(prior_sensitivity) > sensitivity_rank(input.sensitivity)
                || retention_rank(prior_retention) > retention_rank(input.retention_class)
                || (!prior_export_allowed && input.export_allowed) {
                return Err(StoreError::Conflict("illegal improvement revision transition or classification downgrade".to_owned()));
            }
        }
        let next: i64 = transaction.query_row(
            "SELECT coalesce(max(revision),0)+1 FROM improvement_revisions WHERE aggregate_kind=?1 AND aggregate_id=?2",
            params![enum_text(&input.aggregate_kind)?, input.aggregate_id], |row| row.get(0),
        )?;
        transaction.execute(
            "INSERT INTO improvement_revisions(id,aggregate_kind,aggregate_id,revision,schema_name,lifecycle_state,payload_json,payload_sha256,sensitivity,retention_class,export_allowed,source_domain_event_id,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
            params![input.id, enum_text(&input.aggregate_kind)?, input.aggregate_id, next, input.schema.as_str(), enum_text(&input.state)?, payload_json, input.payload_sha256, enum_text(&input.sensitivity)?, enum_text(&input.retention_class)?, input.export_allowed, input.source_domain_event_id, now],
        )?;
        let event_payload =
            serde_json::json!({"schema": input.schema.as_str(), "state": enum_text(&input.state)?})
                .to_string();
        let event_payload_sha256 = sha256(event_payload.as_bytes());
        transaction.execute(
            "INSERT INTO improvement_events(id,aggregate_kind,aggregate_id,revision_id,sequence,event_type,payload_json,payload_sha256,idempotency_key,source_raw_event_id,occurred_at) VALUES(?1,?2,?3,?4,?5,'revision_recorded',?6,?7,?8,?9,?10)",
            params![input.event_id.as_str(), enum_text(&input.aggregate_kind)?, input.aggregate_id, input.id, next, event_payload, event_payload_sha256, input.idempotency_key, input.source_raw_event_id, now],
        )?;
        let record = read_improvement_revision(&transaction, &input.id)?;
        let event = read_improvement_event(&transaction, input.event_id.as_str())?;
        transaction.commit()?;
        Ok((record, event))
    }

    pub fn improvement_current_revision(
        &self,
        kind: ImprovementRecordKind,
        aggregate_id: &str,
    ) -> Result<Option<ImprovementRevisionRecord>, StoreError> {
        let connection = self.connection()?;
        connection.query_row("SELECT id,aggregate_kind,aggregate_id,revision,schema_name,lifecycle_state,payload_json,payload_sha256,sensitivity,retention_class,export_allowed,source_domain_event_id,created_at FROM improvement_current_revisions WHERE aggregate_kind=?1 AND aggregate_id=?2", params![enum_text(&kind)?, aggregate_id], map_improvement_revision).optional().map_err(Into::into)
    }

    pub fn list_improvement_events(
        &self,
        kind: ImprovementRecordKind,
        aggregate_id: &str,
    ) -> Result<Vec<ImprovementEventRecord>, StoreError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare("SELECT id,aggregate_kind,aggregate_id,revision_id,sequence,event_type,payload_sha256,idempotency_key,source_raw_event_id,occurred_at FROM improvement_events WHERE aggregate_kind=?1 AND aggregate_id=?2 ORDER BY sequence")?;
        statement
            .query_map(
                params![enum_text(&kind)?, aggregate_id],
                map_improvement_event,
            )?
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }
    pub fn put_runtime_metadata(&self, key: &str, value: &Value) -> Result<(), StoreError> {
        self.connection()?.execute(
            "INSERT INTO runtime_metadata(key,value_json,updated_at) VALUES(?1,?2,?3) ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json,updated_at=excluded.updated_at",
            params![key, serde_json::to_string(value)?, now_ms()],
        )?;
        Ok(())
    }

    pub fn runtime_metadata(&self, key: &str) -> Result<Option<Value>, StoreError> {
        self.connection()?
            .query_row(
                "SELECT value_json FROM runtime_metadata WHERE key=?1",
                [key],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|value| serde_json::from_str(&value).map_err(Into::into))
            .transpose()
    }

    pub fn delete_runtime_metadata(&self, key: &str) -> Result<(), StoreError> {
        self.connection()?
            .execute("DELETE FROM runtime_metadata WHERE key=?1", [key])?;
        Ok(())
    }

    pub fn governor_token_samples(
        &self,
        limit: u32,
        model: &str,
        reasoning_effort: &str,
        owner_profile: Option<&str>,
    ) -> Result<Vec<u64>, StoreError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT a.goal_tokens_used FROM agent_sessions a JOIN task_attempts ta ON ta.id=a.task_attempt_id JOIN tasks t ON t.id=ta.task_id WHERE a.role='governor' AND a.parent_agent_session_id IS NULL AND a.completed_at IS NOT NULL AND a.goal_tokens_used > 0 AND coalesce(a.failure_class,'') NOT IN ('infrastructure_unavailable','authentication_rejected') AND coalesce(a.effective_model,a.requested_model)=?1 AND coalesce(a.effective_reasoning_effort,a.requested_reasoning_effort)=?2 AND (?3 IS NULL OR t.owner_profile=?3) ORDER BY a.completed_at DESC LIMIT ?4",
        )?;
        let rows = statement.query_map(
            params![
                model,
                reasoning_effort,
                owner_profile,
                i64::from(limit.clamp(1, 100)),
            ],
            |row| row.get::<_, i64>(0),
        )?;
        rows.map(|row| {
            row.map(|value| u64::try_from(value).unwrap_or_default())
                .map_err(StoreError::from)
        })
        .collect()
    }

    /// Counts the task's root governors and their delegated descendants across
    /// attempts, excluding independent verifier trees.
    pub fn task_governor_usage(&self, task_id: &TaskId) -> Result<u64, StoreError> {
        let connection = self.connection()?;
        let total = connection.query_row(
            "WITH RECURSIVE governor_tree(id) AS (
                SELECT a.id
                FROM agent_sessions a
                JOIN task_attempts ta ON ta.id=a.task_attempt_id
                WHERE ta.task_id=?1
                  AND a.role='governor'
                  AND a.parent_agent_session_id IS NULL
                UNION ALL
                SELECT child.id
                FROM agent_sessions child
                JOIN governor_tree parent ON child.parent_agent_session_id=parent.id
             )
             SELECT coalesce(sum(a.goal_tokens_used),0)
             FROM agent_sessions a
             JOIN governor_tree tree ON tree.id=a.id",
            [task_id.as_str()],
            |row| row.get::<_, i64>(0),
        )?;
        Ok(u64::try_from(total).unwrap_or_default())
    }

    pub fn agent_goal_status(
        &self,
        agent_id: &AgentSessionId,
    ) -> Result<Option<String>, StoreError> {
        Ok(self
            .connection()?
            .query_row(
                "SELECT goal_status FROM agent_sessions WHERE id=?1",
                [agent_id.as_str()],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten())
    }

    pub fn record_handoff(
        &self,
        attempt_id: &AttemptId,
        agent_id: &AgentSessionId,
        handoff: &Value,
        schema_valid: bool,
    ) -> Result<(), StoreError> {
        let raw = serde_json::to_string(handoff)?;
        let digest = sha256(raw.as_bytes());
        self.connection()?.execute(
            "INSERT INTO handoffs(id,task_attempt_id,agent_session_id,handoff_json,handoff_sha256,schema_valid,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7) ON CONFLICT(task_attempt_id) DO UPDATE SET agent_session_id=excluded.agent_session_id,handoff_json=excluded.handoff_json,handoff_sha256=excluded.handoff_sha256,schema_valid=excluded.schema_valid,created_at=excluded.created_at",
            params![
                format!("handoff-{}", attempt_id.as_str()),
                attempt_id.as_str(),
                agent_id.as_str(),
                raw,
                digest,
                schema_valid,
                now_ms(),
            ],
        )?;
        Ok(())
    }

    pub fn attempt_handoff(&self, attempt_id: &AttemptId) -> Result<Option<String>, StoreError> {
        self.connection()?
            .query_row(
                "SELECT handoff_json FROM handoffs WHERE task_attempt_id=?1",
                [attempt_id.as_str()],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn recent_task_handoffs(
        &self,
        task_id: &TaskId,
        limit: u32,
    ) -> Result<Vec<String>, StoreError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT h.handoff_json FROM handoffs h JOIN task_attempts a ON a.id=h.task_attempt_id WHERE a.task_id=?1 AND h.schema_valid=1 ORDER BY a.attempt_number DESC LIMIT ?2",
        )?;
        let rows = statement.query_map(
            params![task_id.as_str(), i64::from(limit.clamp(1, 20))],
            |row| row.get::<_, String>(0),
        )?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn create_repository(
        &self,
        input: &NewRepository,
    ) -> Result<RepositorySummary, StoreError> {
        let now = now_ms();
        self.connection()?.execute(
            "INSERT INTO repositories(id,profile_id,profile_version,display_name,root_path,origin_url,default_branch,expected_coordination_branch,state,created_at,updated_at,version) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?10,1)",
            params![
                input.id.as_str(),
                input.profile_id,
                input.profile_version,
                input.display_name,
                input.root_path.to_string_lossy(),
                input.origin_url,
                input.default_branch,
                input.expected_coordination_branch,
                input.state,
                now,
            ],
        )?;
        self.repository(&input.id)
    }

    pub fn replace_repository_checkout(
        &self,
        repository_id: &RepositoryId,
        expected_root: &Path,
        replacement_root: &Path,
        origin_url: Option<&str>,
    ) -> Result<(), StoreError> {
        let changed = self.connection()?.execute(
            "UPDATE repositories SET root_path=?3,origin_url=?4,updated_at=?5,version=version+1 WHERE id=?1 AND root_path=?2",
            params![
                repository_id.as_str(),
                expected_root.to_string_lossy(),
                replacement_root.to_string_lossy(),
                origin_url,
                now_ms(),
            ],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(format!(
                "repository {repository_id} checkout changed while preparing its replacement"
            )));
        }
        Ok(())
    }

    pub fn record_repository_health(
        &self,
        input: &RepositoryHealthInput,
    ) -> Result<(), StoreError> {
        let id = ulid::Ulid::generate().to_string();
        let now = now_ms();
        let blockers = serde_json::to_string(&input.blockers)?;
        let details = serde_json::to_string(&input.details)?;
        let state = if input.blockers.is_empty() {
            "READY"
        } else {
            "BLOCKED"
        };
        let connection = self.connection()?;
        let transaction = connection.unchecked_transaction()?;
        transaction.execute(
            "INSERT INTO repository_health_snapshots(id,repository_id,observed_at,primary_branch,primary_head_sha,primary_clean,origin_head_sha,git_identity_name_present,git_identity_email_present,authority_digest,blockers_json,details_json) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
            params![
                id,
                input.repository_id.as_str(),
                now,
                input.primary_branch,
                input.primary_head_sha,
                input.primary_clean,
                input.origin_head_sha,
                input.git_identity_name_present,
                input.git_identity_email_present,
                input.authority_digest,
                blockers,
                details,
            ],
        )?;
        transaction.execute(
            "UPDATE repositories SET state=?2,updated_at=?3,version=version+1 WHERE id=?1",
            params![input.repository_id.as_str(), state, now],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn repository(&self, id: &RepositoryId) -> Result<RepositorySummary, StoreError> {
        let connection = self.connection()?;
        connection
            .query_row(repository_select(true), [id.as_str()], map_repository)
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("repository {id}")))
    }

    pub fn list_repositories(&self) -> Result<Vec<RepositorySummary>, StoreError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(repository_select(false))?;
        let rows = statement.query_map([], map_repository)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn create_run(&self, input: &NewRun) -> Result<RunSummary, StoreError> {
        let now = now_ms();
        self.connection()?.execute(
            "INSERT INTO runs(id,repository_id,title,requested_objective,mode,publication_mode,state,phase,base_ref,base_sha,authority_digest,profile_digest,codex_version,protocol_schema_sha256,requested_by,run_token_budget,created_at,updated_at,started_at,version) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?17,?17,1)",
            params![
                input.id.as_str(),
                input.repository_id.as_str(),
                input.title,
                input.objective,
                input.mode,
                input.publication_mode,
                input.state,
                input.phase,
                input.base_ref,
                input.base_sha,
                input.authority_digest,
                input.profile_digest,
                input.codex_version,
                input.protocol_schema_sha256,
                input.requested_by,
                input.token_budget.map(|value| value as i64),
                now,
            ],
        )?;
        self.run(&input.id)
    }

    pub fn run(&self, id: &RunId) -> Result<RunSummary, StoreError> {
        let connection = self.connection()?;
        connection
            .query_row(
                &format!("{} WHERE r.id=?1", run_select()),
                [id.as_str()],
                map_run,
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("run {id}")))
    }

    pub fn list_runs(
        &self,
        repository_id: Option<&RepositoryId>,
        include_terminal: bool,
    ) -> Result<Vec<RunSummary>, StoreError> {
        let connection = self.connection()?;
        let terminal_filter = if include_terminal {
            ""
        } else {
            " AND r.state NOT IN ('COMPLETED','CANCELED','FAILED','ARCHIVED')"
        };
        let sql = if repository_id.is_some() {
            format!(
                "{} WHERE r.repository_id=?1{} ORDER BY r.created_at DESC",
                run_select(),
                terminal_filter
            )
        } else {
            format!(
                "{} WHERE 1=1{} ORDER BY r.created_at DESC",
                run_select(),
                terminal_filter
            )
        };
        let mut statement = connection.prepare(&sql)?;
        let rows = if let Some(repository_id) = repository_id {
            statement.query_map([repository_id.as_str()], map_run)?
        } else {
            statement.query_map([], map_run)?
        };
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn transition_run(
        &self,
        id: &RunId,
        next: RunState,
        phase: &str,
        expected_version: Option<u64>,
        failure: Option<(&str, &str)>,
    ) -> Result<RunSummary, StoreError> {
        let connection = self.connection()?;
        let transaction = connection.unchecked_transaction()?;
        let current = transaction
            .query_row(
                &format!("{} WHERE r.id=?1", run_select()),
                [id.as_str()],
                map_run,
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("run {id}")))?;
        if expected_version.is_some_and(|version| version != current.version) {
            return Err(StoreError::Conflict(format!(
                "run {id} is version {}, not {}",
                current.version,
                expected_version.unwrap_or_default()
            )));
        }
        if !current.state.can_transition_to(next) {
            return Err(StoreError::Conflict(format!(
                "illegal run transition {} -> {}",
                current.state, next
            )));
        }
        let now = now_ms();
        let completed = next.is_terminal().then_some(now);
        let (failure_class, failure_reason) = failure.unzip();
        let current_version = sqlite_version(current.version)?;
        let changed = transaction.execute(
            "UPDATE runs SET state=?2,phase=?3,updated_at=?4,completed_at=coalesce(?5,completed_at),failure_class=?6,failure_reason=?7,version=version+1 WHERE id=?1 AND version=?8",
            params![
                id.as_str(),
                next.to_string(),
                phase,
                now,
                completed,
                failure_class,
                failure_reason,
                current_version,
            ],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(format!(
                "run {id} changed concurrently"
            )));
        }
        let resulting_version = current.version.checked_add(1).ok_or_else(|| {
            StoreError::Validation(format!("run {id} version exceeds supported range"))
        })?;
        let payload = json!({
            "prior_state": current.state.to_string(),
            "next_state": next.to_string(),
            "phase": phase,
            "run_version": resulting_version,
            "failure_class": failure_class,
        });
        transaction.execute(
            "INSERT INTO domain_events(run_id,aggregate_type,aggregate_id,event_type,occurred_at,payload_json,source_raw_event_id) VALUES(?1,?2,?3,?4,?5,?6,NULL)",
            params![
                id.as_str(),
                "run",
                id.as_str(),
                "run.lifecycle.transitioned",
                now,
                serde_json::to_string(&payload)?,
            ],
        )?;
        let updated = transaction.query_row(
            &format!("{} WHERE r.id=?1", run_select()),
            [id.as_str()],
            map_run,
        )?;
        transaction.commit()?;
        Ok(updated)
    }

    pub fn set_scheduler_paused(&self, id: &RunId, paused: bool) -> Result<RunSummary, StoreError> {
        let changed = self.connection()?.execute(
            "UPDATE runs SET scheduler_paused=?2,updated_at=?3,version=version+1 WHERE id=?1",
            params![id.as_str(), paused, now_ms()],
        )?;
        if changed != 1 {
            return Err(StoreError::NotFound(format!("run {id}")));
        }
        self.run(id)
    }

    pub fn set_run_token_budget_and_resume(
        &self,
        id: &RunId,
        token_budget: u64,
    ) -> Result<RunSummary, StoreError> {
        let changed = self.connection()?.execute(
            "UPDATE runs SET run_token_budget=?2,scheduler_paused=0,updated_at=?3,version=version+1 WHERE id=?1",
            params![id.as_str(), token_budget as i64, now_ms()],
        )?;
        if changed != 1 {
            return Err(StoreError::NotFound(format!("run {id}")));
        }
        self.run(id)
    }

    pub fn set_run_integration(
        &self,
        id: &RunId,
        branch: &str,
        sha: &str,
    ) -> Result<(), StoreError> {
        self.connection()?.execute(
            "UPDATE runs SET integration_branch=?2,integration_sha=?3,updated_at=?4,version=version+1 WHERE id=?1",
            params![id.as_str(), branch, sha, now_ms()],
        )?;
        Ok(())
    }

    pub fn clear_run_integration(&self, id: &RunId) -> Result<(), StoreError> {
        let changed = self.connection()?.execute(
            "UPDATE runs SET integration_branch=NULL,integration_sha=NULL,updated_at=?2,version=version+1 WHERE id=?1",
            params![id.as_str(), now_ms()],
        )?;
        if changed != 1 {
            return Err(StoreError::NotFound(format!("run {id}")));
        }
        Ok(())
    }

    pub fn create_worktree(&self, input: &NewWorktree) -> Result<(), StoreError> {
        self.connection()?.execute(
            "INSERT INTO worktrees(id,run_id,task_attempt_id,kind,path,branch,base_sha,head_sha,state,created_at,version) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,1)",
            params![
                input.id.as_str(),
                input.run_id.as_str(),
                input.task_attempt_id.as_ref().map(AttemptId::as_str),
                input.kind,
                input.path.to_string_lossy(),
                input.branch,
                input.base_sha,
                input.head_sha,
                input.state,
                now_ms(),
            ],
        )?;
        Ok(())
    }

    pub fn update_worktree(
        &self,
        id: &WorktreeId,
        state: &str,
        head_sha: Option<&str>,
        preserved_reason: Option<&str>,
    ) -> Result<(), StoreError> {
        let changed = self.connection()?.execute(
            "UPDATE worktrees SET state=?2,head_sha=coalesce(?3,head_sha),preserved_reason=coalesce(?4,preserved_reason),reconciled_at=?5,version=version+1 WHERE id=?1",
            params![id.as_str(), state, head_sha, preserved_reason, now_ms()],
        )?;
        if changed != 1 {
            return Err(StoreError::NotFound(format!("worktree {id}")));
        }
        Ok(())
    }

    pub fn mark_worktree_removed(&self, id: &WorktreeId) -> Result<(), StoreError> {
        let timestamp = now_ms();
        let changed = self.connection()?.execute(
            "UPDATE worktrees SET state='REMOVED',removed_at=coalesce(removed_at,?2),reconciled_at=?2,version=version+1 WHERE id=?1",
            params![id.as_str(), timestamp],
        )?;
        if changed != 1 {
            return Err(StoreError::NotFound(format!("worktree {id}")));
        }
        Ok(())
    }

    pub fn worktree_has_active_path_lease(&self, id: &WorktreeId) -> Result<bool, StoreError> {
        self.connection()?
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM worktrees w JOIN path_leases p ON p.task_attempt_id=w.task_attempt_id WHERE w.id=?1 AND p.released_at IS NULL AND p.expires_at>?2)",
                params![id.as_str(), now_ms()],
                |row| row.get::<_, bool>(0),
            )
            .map_err(Into::into)
    }

    pub fn set_worktree_composed_base(
        &self,
        id: &WorktreeId,
        base_sha: &str,
    ) -> Result<(), StoreError> {
        let changed = self.connection()?.execute(
            "UPDATE worktrees SET base_sha=?2,head_sha=?2,state='ACTIVE',reconciled_at=?3,version=version+1 WHERE id=?1",
            params![id.as_str(), base_sha, now_ms()],
        )?;
        if changed != 1 {
            return Err(StoreError::NotFound(format!("worktree {id}")));
        }
        Ok(())
    }

    pub fn list_worktrees(
        &self,
        run_id: Option<&RunId>,
    ) -> Result<Vec<WorktreeSummary>, StoreError> {
        let connection = self.connection()?;
        let base = "SELECT w.id,w.run_id,t.id,w.kind,w.path,w.branch,w.base_sha,w.head_sha,w.state,w.preserved_reason,coalesce(tr.diff_files,0),coalesce(tr.diff_additions,0),coalesce(tr.diff_deletions,0),w.version FROM worktrees w LEFT JOIN task_attempts a ON a.id=w.task_attempt_id LEFT JOIN tasks t ON t.id=a.task_id LEFT JOIN task_results tr ON tr.task_attempt_id=a.id";
        let sql = if run_id.is_some() {
            format!("{base} WHERE w.run_id=?1 ORDER BY w.created_at DESC")
        } else {
            format!("{base} ORDER BY w.created_at DESC")
        };
        let mut statement = connection.prepare(&sql)?;
        let rows = if let Some(run_id) = run_id {
            statement.query_map([run_id.as_str()], map_worktree)?
        } else {
            statement.query_map([], map_worktree)?
        };
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn create_agent_session(&self, input: &NewAgentSession) -> Result<(), StoreError> {
        self.connection()?.execute(
            "INSERT INTO agent_sessions(id,run_id,task_attempt_id,parent_agent_session_id,runtime_kind,codex_account_id,role,nickname,requested_model,requested_reasoning_effort,sandbox_mode,approval_policy,cwd,state,current_goal,goal_status,token_budget,started_at,last_heartbeat_at,version) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,'active',?16,?17,?17,1)",
            params![
                input.id.as_str(),
                input.run_id.as_str(),
                input.task_attempt_id.as_ref().map(AttemptId::as_str),
                input.parent_agent_session_id.as_ref().map(AgentSessionId::as_str),
                input.runtime_kind,
                input.codex_account_id,
                enum_text(&input.role)?,
                input.nickname,
                input.requested_model,
                input.requested_reasoning_effort,
                enum_text(&input.sandbox_mode)?,
                input.approval_policy,
                input.cwd.to_string_lossy(),
                input.state,
                input.current_goal,
                input.token_budget.map(|value| value as i64),
                now_ms(),
            ],
        )?;
        self.connection()?.execute(
            "INSERT INTO agent_runtime_details(agent_session_id,updated_at) VALUES(?1,?2)",
            params![input.id.as_str(), now_ms()],
        )?;
        Ok(())
    }

    pub fn set_agent_context_strategy(
        &self,
        agent_id: &AgentSessionId,
        strategy: &str,
        source_attempt_id: Option<&AttemptId>,
        reason: Option<&str>,
    ) -> Result<(), StoreError> {
        let changed = self.connection()?.execute(
            "UPDATE agent_runtime_details SET context_strategy=?2,context_source_attempt_id=?3,context_reuse_reason=?4,updated_at=?5 WHERE agent_session_id=?1",
            params![
                agent_id.as_str(),
                strategy,
                source_attempt_id.map(AttemptId::as_str),
                reason,
                now_ms(),
            ],
        )?;
        if changed != 1 {
            return Err(StoreError::NotFound(format!("agent {agent_id}")));
        }
        Ok(())
    }

    pub fn prepare_agent_continuation(
        &self,
        agent_id: &AgentSessionId,
        token_budget: u64,
        current_action: &str,
    ) -> Result<(), StoreError> {
        let now = now_ms();
        let connection = self.connection()?;
        let transaction = connection.unchecked_transaction()?;
        let changed = transaction.execute(
            "UPDATE agent_sessions SET token_budget=?2,state='STARTING',completed_at=NULL,failure_class=NULL,failure_reason=NULL,last_heartbeat_at=?3,version=version+1 WHERE id=?1",
            params![agent_id.as_str(), token_budget as i64, now],
        )?;
        if changed != 1 {
            return Err(StoreError::NotFound(format!("agent {agent_id}")));
        }
        transaction.execute(
            "UPDATE agent_runtime_details SET active_turn_id=NULL,current_action=?2,last_activity_kind='turn',last_activity_at=?3,updated_at=?3 WHERE agent_session_id=?1",
            params![agent_id.as_str(), current_action, now],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn attach_codex_thread(
        &self,
        agent_id: &AgentSessionId,
        thread_id: &str,
        parent_thread_id: Option<&str>,
        service_name: &str,
        branch: Option<&str>,
        sha: Option<&str>,
    ) -> Result<(), StoreError> {
        let now = now_ms();
        self.connection()?.execute(
            "INSERT INTO codex_threads(thread_id,agent_session_id,parent_thread_id,service_name,git_branch,git_sha,created_at,updated_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?7) ON CONFLICT(thread_id) DO UPDATE SET agent_session_id=excluded.agent_session_id,parent_thread_id=excluded.parent_thread_id,updated_at=excluded.updated_at",
            params![thread_id, agent_id.as_str(), parent_thread_id, service_name, branch, sha, now],
        )?;
        Ok(())
    }

    pub fn attach_codex_turn(
        &self,
        agent_id: &AgentSessionId,
        thread_id: &str,
        turn_id: &str,
        requested_model: Option<&str>,
        requested_effort: Option<&str>,
        authoritative_notification: bool,
    ) -> Result<(), StoreError> {
        let now = now_ms();
        let connection = self.connection()?;
        let transaction = connection.unchecked_transaction()?;
        transaction.execute(
            "INSERT INTO codex_turns(turn_id,thread_id,status,requested_model,requested_reasoning_effort,started_at,version) VALUES(?1,?2,'inProgress',?3,?4,?5,1) ON CONFLICT(turn_id) DO UPDATE SET status='inProgress',started_at=coalesce(started_at,excluded.started_at),version=version+1",
            params![turn_id, thread_id, requested_model, requested_effort, now],
        )?;
        transaction.execute(
            "UPDATE agent_runtime_details SET active_turn_id=CASE WHEN ?3 THEN ?2 ELSE coalesce(active_turn_id,?2) END,last_activity_kind='turn',last_activity_at=?4,updated_at=?4 WHERE agent_session_id=?1",
            params![agent_id.as_str(), turn_id, authoritative_notification, now],
        )?;
        transaction.execute(
            "UPDATE agent_sessions SET state='RUNNING',last_heartbeat_at=?2,version=version+1 WHERE id=?1",
            params![agent_id.as_str(), now],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn update_agent_state(
        &self,
        id: &AgentSessionId,
        state: &str,
        current_action: Option<&str>,
        effective_model: Option<&str>,
        effective_effort: Option<&str>,
        terminal_failure: Option<(&str, &str)>,
    ) -> Result<(), StoreError> {
        let now = now_ms();
        let (failure_class, failure_reason) = terminal_failure.unzip();
        let terminal = matches!(state, "COMPLETED" | "FAILED" | "INTERRUPTED" | "CANCELED");
        let connection = self.connection()?;
        let transaction = connection.unchecked_transaction()?;
        let changed = transaction.execute(
            "UPDATE agent_sessions SET state=?2,effective_model=coalesce(?3,effective_model),effective_reasoning_effort=coalesce(?4,effective_reasoning_effort),last_heartbeat_at=?5,completed_at=CASE WHEN ?6 THEN ?5 ELSE completed_at END,failure_class=?7,failure_reason=?8,version=version+1 WHERE id=?1",
            params![id.as_str(), state, effective_model, effective_effort, now, terminal, failure_class, failure_reason],
        )?;
        if changed != 1 {
            return Err(StoreError::NotFound(format!("agent {id}")));
        }
        transaction.execute(
            "UPDATE agent_runtime_details SET current_action=coalesce(?2,current_action),last_activity_at=?3,updated_at=?3 WHERE agent_session_id=?1",
            params![id.as_str(), current_action, now],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn clear_agent_active_turn(&self, id: &AgentSessionId) -> Result<(), StoreError> {
        self.connection()?.execute(
            "UPDATE agent_runtime_details SET active_turn_id=NULL,last_activity_kind='runtime',last_activity_at=?2,updated_at=?2 WHERE agent_session_id=?1",
            params![id.as_str(), now_ms()],
        )?;
        Ok(())
    }

    pub fn agent_by_thread(&self, thread_id: &str) -> Result<Option<AgentSessionId>, StoreError> {
        Ok(self
            .connection()?
            .query_row(
                "SELECT agent_session_id FROM codex_threads WHERE thread_id=?1",
                [thread_id],
                |row| row.get::<_, String>(0).map(AgentSessionId::from),
            )
            .optional()?)
    }

    pub fn native_subagent_activities(
        &self,
    ) -> Result<Vec<NativeSubagentActivityRecord>, StoreError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT re.agent_session_id,re.thread_id,pi.payload_json FROM projected_items pi JOIN raw_events re ON re.id=pi.source_raw_event_id WHERE pi.item_type='subAgentActivity' AND pi.summary='Subagent started' AND re.agent_session_id IS NOT NULL AND re.thread_id IS NOT NULL ORDER BY re.id",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        rows.map(|row| {
            let (parent_agent_session_id, parent_thread_id, payload) = row?;
            Ok(NativeSubagentActivityRecord {
                parent_agent_session_id: AgentSessionId::from(parent_agent_session_id),
                parent_thread_id,
                payload: serde_json::from_str(&payload)?,
            })
        })
        .collect()
    }

    pub fn latest_thread_turn_status(&self, thread_id: &str) -> Result<Option<String>, StoreError> {
        let event = self
            .connection()?
            .query_row(
                "SELECT method,payload_json FROM raw_events WHERE thread_id=?1 AND method IN ('turn/started','turn/completed') ORDER BY id DESC LIMIT 1",
                [thread_id],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?;
        event
            .map(|(method, payload)| {
                if method == "turn/started" {
                    return Ok(None);
                }
                let payload: Value = serde_json::from_str(&payload)?;
                Ok(Some(
                    payload
                        .pointer("/turn/status")
                        .and_then(Value::as_str)
                        .unwrap_or("failed")
                        .to_owned(),
                ))
            })
            .transpose()
            .map(Option::flatten)
    }

    pub fn agent(&self, id: &AgentSessionId) -> Result<AgentSummary, StoreError> {
        let connection = self.connection()?;
        connection
            .query_row(
                &format!("{} WHERE a.id=?1", agent_select()),
                [id.as_str()],
                map_agent,
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("agent {id}")))
    }

    pub fn list_agents(&self, run_id: &RunId) -> Result<Vec<AgentSummary>, StoreError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(&format!(
            "{} WHERE a.run_id=?1 ORDER BY a.started_at,a.id",
            agent_select()
        ))?;
        let rows = statement.query_map([run_id.as_str()], map_agent)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn store_plan(
        &self,
        run_id: &RunId,
        architect_agent_id: &AgentSessionId,
        plan: &RunPlan,
    ) -> Result<PlanRevisionId, StoreError> {
        let plan_json = serde_json::to_string(plan)?;
        let plan_sha = sha256(plan_json.as_bytes());
        let revision_id = PlanRevisionId::new();
        let now = now_ms();
        let connection = self.connection()?;
        let transaction = connection.unchecked_transaction()?;
        let revision: i64 = transaction.query_row(
            "SELECT coalesce(max(revision),0)+1 FROM run_plan_revisions WHERE run_id=?1",
            [run_id.as_str()],
            |row| row.get(0),
        )?;
        transaction.execute(
            "DELETE FROM tasks WHERE run_id=?1 AND plan_revision_id IN (SELECT id FROM run_plan_revisions WHERE run_id=?1 AND state IN ('PROPOSED','CERTIFIED','REVISION_REQUIRED'))",
            [run_id.as_str()],
        )?;
        transaction.execute(
            "UPDATE run_plan_revisions SET state='SUPERSEDED' WHERE run_id=?1 AND state IN ('PROPOSED','CERTIFIED','REVISION_REQUIRED')",
            [run_id.as_str()],
        )?;
        transaction.execute(
            "INSERT INTO run_plan_revisions(id,run_id,revision,architect_agent_session_id,plan_json,plan_sha256,state,created_at) VALUES(?1,?2,?3,?4,?5,?6,'PROPOSED',?7)",
            params![revision_id.as_str(),run_id.as_str(),revision,architect_agent_id.as_str(),plan_json,plan_sha,now],
        )?;

        let mut ids = BTreeMap::new();
        for packet in &plan.tasks {
            let task_id = TaskId::new();
            ids.insert(packet.task_id.clone(), task_id.clone());
            transaction.execute(
                "INSERT INTO tasks(id,run_id,plan_revision_id,external_task_id,title,objective,priority,owner_profile,reviewer_profile,state,created_at,updated_at,version) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,'PROPOSED',?10,?10,1)",
                params![task_id.as_str(),run_id.as_str(),revision_id.as_str(),packet.task_id,packet.title,packet.objective,packet.priority,packet.owner_profile,packet.reviewer_profile,now],
            )?;
        }
        for packet in &plan.tasks {
            let task_id = ids.get(&packet.task_id).ok_or_else(|| {
                StoreError::Validation(format!(
                    "task {} disappeared while storing the plan",
                    packet.task_id
                ))
            })?;
            for external_dependency in &packet.depends_on {
                let dependency_id = ids.get(external_dependency).ok_or_else(|| {
                    StoreError::Validation(format!(
                        "task {} depends on missing task {external_dependency}",
                        packet.task_id
                    ))
                })?;
                transaction.execute(
                    "INSERT INTO task_dependencies(task_id,depends_on_task_id,expected_dependency_sha) VALUES(?1,?2,?3)",
                    params![task_id.as_str(),dependency_id.as_str(),packet.dependency_shas.get(external_dependency)],
                )?;
            }
        }
        transaction.commit()?;
        Ok(revision_id)
    }

    pub fn latest_plan(
        &self,
        run_id: &RunId,
    ) -> Result<Option<(PlanRevisionId, RunPlan, String, u64)>, StoreError> {
        let connection = self.connection()?;
        connection
            .query_row(
                "SELECT id,plan_json,state,revision FROM run_plan_revisions WHERE run_id=?1 ORDER BY revision DESC LIMIT 1",
                [run_id.as_str()],
                |row| {
                    let json: String = row.get(1)?;
                    let plan: RunPlan = serde_json::from_str(&json).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(
                            json.len(),
                            rusqlite::types::Type::Text,
                            Box::new(error),
                        )
                    })?;
                    Ok((
                        PlanRevisionId::from(row.get::<_, String>(0)?),
                        plan,
                        row.get(2)?,
                        row.get::<_, i64>(3)? as u64,
                    ))
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn approve_latest_plan(&self, run_id: &RunId, actor: &str) -> Result<(), StoreError> {
        let Some((revision_id, _, state, _)) = self.latest_plan(run_id)? else {
            return Err(StoreError::NotFound(format!("plan for run {run_id}")));
        };
        if state != "CERTIFIED" {
            return Err(StoreError::Conflict(format!(
                "plan is {state}, not CERTIFIED"
            )));
        }
        let now = now_ms();
        let connection = self.connection()?;
        let transaction = connection.unchecked_transaction()?;
        transaction.execute(
            "UPDATE run_plan_revisions SET state='APPROVED',approved_at=?2,approved_by=?3 WHERE id=?1",
            params![revision_id.as_str(), now, actor],
        )?;
        transaction.execute(
            "UPDATE tasks SET state=CASE WHEN EXISTS(SELECT 1 FROM task_dependencies d WHERE d.task_id=tasks.id) THEN 'WAITING_DEPENDENCY' ELSE 'READY' END,updated_at=?2,version=version+1 WHERE plan_revision_id=?1",
            params![revision_id.as_str(), now],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn certify_latest_plan(&self, run_id: &RunId) -> Result<(), StoreError> {
        self.transition_latest_plan_state(run_id, "PROPOSED", "CERTIFIED")
    }

    pub fn mark_latest_plan_revision_required(&self, run_id: &RunId) -> Result<(), StoreError> {
        self.transition_latest_plan_state(run_id, "PROPOSED", "REVISION_REQUIRED")
    }

    pub fn request_latest_plan_revision(&self, run_id: &RunId) -> Result<(), StoreError> {
        let Some((_, _, state, _)) = self.latest_plan(run_id)? else {
            return Err(StoreError::NotFound(format!("plan for run {run_id}")));
        };
        match state.as_str() {
            "CERTIFIED" => {
                self.transition_latest_plan_state(run_id, "CERTIFIED", "REVISION_REQUIRED")
            }
            "REVISION_REQUIRED" => Ok(()),
            _ => Err(StoreError::Conflict(format!(
                "plan is {state}, not CERTIFIED or REVISION_REQUIRED"
            ))),
        }
    }

    pub fn reopen_latest_plan_for_review(&self, run_id: &RunId) -> Result<(), StoreError> {
        self.transition_latest_plan_state(run_id, "CERTIFIED", "PROPOSED")
    }

    fn transition_latest_plan_state(
        &self,
        run_id: &RunId,
        expected: &str,
        next: &str,
    ) -> Result<(), StoreError> {
        let Some((revision_id, _, state, _)) = self.latest_plan(run_id)? else {
            return Err(StoreError::NotFound(format!("plan for run {run_id}")));
        };
        if state != expected {
            return Err(StoreError::Conflict(format!(
                "plan is {state}, not {expected}"
            )));
        }
        let changed = self.connection()?.execute(
            "UPDATE run_plan_revisions SET state=?2 WHERE id=?1 AND state=?3",
            params![revision_id.as_str(), next, expected],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(
                "plan changed during state transition".to_owned(),
            ));
        }
        Ok(())
    }

    pub fn list_tasks(&self, run_id: &RunId) -> Result<Vec<TaskSummary>, StoreError> {
        let connection = self.connection()?;
        let sql = "SELECT t.id,t.run_id,t.external_task_id,t.title,t.objective,t.state,t.priority,t.owner_profile,t.reviewer_profile,t.current_attempt_number,coalesce(a.base_sha,r.base_sha),coalesce(a.head_sha,tr.verified_commit_sha),a.token_budget,t.version,(SELECT json_group_array(dt.external_task_id) FROM task_dependencies d JOIN tasks dt ON dt.id=d.depends_on_task_id WHERE d.task_id=t.id) FROM tasks t JOIN runs r ON r.id=t.run_id LEFT JOIN task_attempts a ON a.task_id=t.id AND a.attempt_number=t.current_attempt_number LEFT JOIN task_results tr ON tr.task_attempt_id=a.id WHERE t.run_id=?1 ORDER BY CASE t.priority WHEN 'P0' THEN 0 WHEN 'P1' THEN 1 WHEN 'P2' THEN 2 ELSE 3 END,t.created_at";
        let mut statement = connection.prepare(sql)?;
        let rows = statement.query_map([run_id.as_str()], map_task)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn task(&self, id: &TaskId) -> Result<TaskSummary, StoreError> {
        let connection = self.connection()?;
        let sql = "SELECT t.id,t.run_id,t.external_task_id,t.title,t.objective,t.state,t.priority,t.owner_profile,t.reviewer_profile,t.current_attempt_number,coalesce(a.base_sha,r.base_sha),coalesce(a.head_sha,tr.verified_commit_sha),a.token_budget,t.version,(SELECT json_group_array(dt.external_task_id) FROM task_dependencies d JOIN tasks dt ON dt.id=d.depends_on_task_id WHERE d.task_id=t.id) FROM tasks t JOIN runs r ON r.id=t.run_id LEFT JOIN task_attempts a ON a.task_id=t.id AND a.attempt_number=t.current_attempt_number LEFT JOIN task_results tr ON tr.task_attempt_id=a.id WHERE t.id=?1";
        connection
            .query_row(sql, [id.as_str()], map_task)
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("task {id}")))
    }

    pub fn task_packet(
        &self,
        id: &TaskId,
    ) -> Result<Option<(AttemptId, harness_domain::TaskPacket)>, StoreError> {
        let connection = self.connection()?;
        connection
            .query_row(
                "SELECT a.id,a.task_packet_json FROM task_attempts a JOIN tasks t ON t.id=a.task_id WHERE t.id=?1 ORDER BY a.attempt_number DESC LIMIT 1",
                [id.as_str()],
                |row| {
                    let raw: String = row.get(1)?;
                    let packet = serde_json::from_str(&raw).map_err(|error| {
                        rusqlite::Error::FromSqlConversionFailure(raw.len(), rusqlite::types::Type::Text, Box::new(error))
                    })?;
                    Ok((AttemptId::from(row.get::<_, String>(0)?), packet))
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn latest_attempt_context(
        &self,
        task_id: &TaskId,
    ) -> Result<Option<PriorAttemptContext>, StoreError> {
        let connection = self.connection()?;
        let attempt = connection
            .query_row(
                "SELECT id,attempt_number,state,terminal_class,failure_reason FROM task_attempts WHERE task_id=?1 ORDER BY attempt_number DESC LIMIT 1",
                [task_id.as_str()],
                |row| {
                    Ok((
                        AttemptId::from(row.get::<_, String>(0)?),
                        row.get::<_, i64>(1)? as u32,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, Option<String>>(4)?,
                    ))
                },
            )
            .optional()?;
        let Some((attempt_id, attempt_number, state, terminal_class, failure_reason)) = attempt
        else {
            return Ok(None);
        };
        let worktree_path = connection
            .query_row(
                "SELECT path FROM worktrees WHERE task_attempt_id=?1 ORDER BY created_at DESC LIMIT 1",
                [attempt_id.as_str()],
                |row| row.get::<_, String>(0).map(PathBuf::from),
            )
            .optional()?;
        let agent = connection
            .query_row(
                "SELECT id,role,requested_model,effective_model,requested_reasoning_effort,effective_reasoning_effort,coalesce(goal_tokens_used,0) FROM agent_sessions WHERE task_attempt_id=?1 AND parent_agent_session_id IS NULL AND role IN ('governor','worker','high_risk_worker') ORDER BY started_at DESC,id DESC LIMIT 1",
                [attempt_id.as_str()],
                |row| {
                    Ok((
                        AgentSessionId::from(row.get::<_, String>(0)?),
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, Option<String>>(5)?,
                        row.get::<_, i64>(6)? as u64,
                    ))
                },
            )
            .optional()?;
        let verifier_verdict = connection
            .query_row(
                "SELECT verifier_verdict FROM task_results WHERE task_attempt_id=?1",
                [attempt_id.as_str()],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()?
            .flatten();
        drop(connection);
        let last_agent_message = if let Some((agent_id, ..)) = agent.as_ref() {
            self.latest_agent_message(agent_id)?
                .map(|message| message.text)
        } else {
            None
        };
        let (
            agent_id,
            role,
            requested_model,
            effective_model,
            requested_reasoning_effort,
            effective_reasoning_effort,
            tokens_used,
        ) = agent.map_or((None, None, None, None, None, None, 0), |agent| {
            (
                Some(agent.0),
                Some(agent.1),
                Some(agent.2),
                agent.3,
                Some(agent.4),
                agent.5,
                agent.6,
            )
        });
        Ok(Some(PriorAttemptContext {
            attempt_id,
            attempt_number,
            state,
            terminal_class,
            failure_reason,
            worktree_path,
            agent_id,
            role,
            requested_model,
            effective_model,
            requested_reasoning_effort,
            effective_reasoning_effort,
            tokens_used,
            verifier_verdict,
            last_agent_message,
        }))
    }

    pub fn create_task_attempt(&self, input: &NewTaskAttempt) -> Result<(), StoreError> {
        let now = now_ms();
        let packet_json = serde_json::to_string(&input.packet)?;
        let connection = self.connection()?;
        let transaction = connection.unchecked_transaction()?;
        transaction.execute(
            "INSERT INTO task_attempts(id,task_id,attempt_number,state,task_packet_json,task_packet_sha256,base_sha,requested_model_route,token_budget,tool_budget,diff_file_budget,diff_line_budget,created_at,updated_at,version) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?13,1)",
            params![input.id.as_str(),input.task_id.as_str(),input.attempt_number,input.state,packet_json,input.packet_sha256,input.base_sha,input.requested_model_route,input.packet.token_budget as i64,input.packet.tool_budget.map(|value| value as i64),input.packet.diff_budget.files,input.packet.diff_budget.lines,now],
        )?;
        transaction.execute(
            "UPDATE tasks SET current_attempt_number=?2,state='LEASED',updated_at=?3,version=version+1 WHERE id=?1",
            params![input.task_id.as_str(), input.attempt_number, now],
        )?;
        transaction.execute(
            "INSERT INTO task_results(task_attempt_id,updated_at) VALUES(?1,?2)",
            params![input.id.as_str(), now],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn set_attempt_composed_base(
        &self,
        attempt_id: &AttemptId,
        packet: &harness_domain::TaskPacket,
        base_sha: &str,
    ) -> Result<(), StoreError> {
        let packet_json = serde_json::to_string(packet)?;
        let packet_sha = sha256(packet_json.as_bytes());
        let changed = self.connection()?.execute(
            "UPDATE task_attempts SET task_packet_json=?2,task_packet_sha256=?3,base_sha=?4,token_budget=?5,tool_budget=?6,updated_at=?7,version=version+1 WHERE id=?1",
            params![
                attempt_id.as_str(),
                packet_json,
                packet_sha,
                base_sha,
                packet.token_budget as i64,
                packet.tool_budget.map(|value| value as i64),
                now_ms(),
            ],
        )?;
        if changed != 1 {
            return Err(StoreError::NotFound(format!("attempt {attempt_id}")));
        }
        Ok(())
    }

    pub fn transition_task(
        &self,
        id: &TaskId,
        next: TaskState,
        expected_version: Option<u64>,
    ) -> Result<TaskSummary, StoreError> {
        let current = self.task(id)?;
        if expected_version.is_some_and(|version| version != current.version) {
            return Err(StoreError::Conflict(format!("stale task version for {id}")));
        }
        if !current.state.can_transition_to(next) {
            return Err(StoreError::Conflict(format!(
                "illegal task transition {} -> {}",
                current.state, next
            )));
        }
        let current_version = sqlite_version(current.version)?;
        let changed = self.connection()?.execute(
            "UPDATE tasks SET state=?2,updated_at=?3,version=version+1 WHERE id=?1 AND version=?4",
            params![id.as_str(), next.to_string(), now_ms(), current_version],
        )?;
        if changed != 1 {
            return Err(StoreError::Conflict(format!(
                "task {id} changed concurrently"
            )));
        }
        self.task(id)
    }

    pub fn task_attempt_for_agent(
        &self,
        id: &AgentSessionId,
    ) -> Result<Option<AttemptId>, StoreError> {
        Ok(self
            .connection()?
            .query_row(
                "SELECT task_attempt_id FROM agent_sessions WHERE id=?1",
                [id.as_str()],
                |row| {
                    row.get::<_, Option<String>>(0)
                        .map(|value| value.map(AttemptId::from))
                },
            )
            .optional()?
            .flatten())
    }

    pub fn task_for_attempt(&self, id: &AttemptId) -> Result<TaskId, StoreError> {
        self.connection()?
            .query_row(
                "SELECT task_id FROM task_attempts WHERE id=?1",
                [id.as_str()],
                |row| row.get::<_, String>(0).map(TaskId::from),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("attempt {id}")))
    }

    pub fn agent_context(
        &self,
        id: &AgentSessionId,
    ) -> Result<(RunId, Option<AttemptId>), StoreError> {
        self.connection()?
            .query_row(
                "SELECT run_id,task_attempt_id FROM agent_sessions WHERE id=?1",
                [id.as_str()],
                |row| {
                    Ok((
                        RunId::from(row.get::<_, String>(0)?),
                        row.get::<_, Option<String>>(1)?.map(AttemptId::from),
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("agent {id}")))
    }

    pub fn worktree_for_attempt(
        &self,
        id: &AttemptId,
    ) -> Result<
        (
            harness_domain::WorktreeId,
            std::path::PathBuf,
            String,
            Option<String>,
        ),
        StoreError,
    > {
        self.connection()?
            .query_row(
                "SELECT id,path,base_sha,head_sha FROM worktrees WHERE task_attempt_id=?1 AND removed_at IS NULL ORDER BY created_at DESC LIMIT 1",
                [id.as_str()],
                |row| {
                    Ok((
                        harness_domain::WorktreeId::from(row.get::<_, String>(0)?),
                        std::path::PathBuf::from(row.get::<_, String>(1)?),
                        row.get(2)?,
                        row.get(3)?,
                    ))
                },
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("worktree for attempt {id}")))
    }

    pub fn record_context_packet(&self, input: &NewContextPacket) -> Result<(), StoreError> {
        let connection = self.connection()?;
        let transaction = connection.unchecked_transaction()?;
        transaction.execute(
            "INSERT INTO context_packets(id,run_id,task_attempt_id,role,base_sha,profile_digest,packet_json,packet_sha256,estimated_tokens,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10)",
            params![
                input.id,
                input.run_id.as_str(),
                input.task_attempt_id.as_ref().map(AttemptId::as_str),
                input.role,
                input.base_sha,
                input.profile_digest,
                serde_json::to_string(&input.packet)?,
                input.packet_sha256,
                i64::try_from(input.estimated_tokens).unwrap_or(i64::MAX),
                now_ms(),
            ],
        )?;
        for source in &input.sources {
            transaction.execute(
                "INSERT INTO context_sources(context_packet_id,path,source_class,content_sha256,included,reason,estimated_tokens) VALUES(?1,?2,?3,?4,?5,?6,?7)",
                params![
                    input.id,
                    source.path,
                    source.source_class,
                    source.content_sha256,
                    source.included,
                    source.reason,
                    i64::try_from(source.estimated_tokens).unwrap_or(i64::MAX),
                ],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn mark_unblocked_tasks_ready(&self, run_id: &RunId) -> Result<u64, StoreError> {
        let changed = self.connection()?.execute(
            "UPDATE tasks SET state='READY',updated_at=?2,version=version+1 WHERE run_id=?1 AND state='WAITING_DEPENDENCY' AND NOT EXISTS (SELECT 1 FROM task_dependencies d JOIN tasks dependency ON dependency.id=d.depends_on_task_id WHERE d.task_id=tasks.id AND dependency.state NOT IN ('VERIFIED','INTEGRATION_QUEUED','INTEGRATING','INTEGRATED','CI_PROVEN','LIVE_PROVEN','CLOSED'))",
            params![run_id.as_str(), now_ms()],
        )?;
        Ok(changed as u64)
    }

    pub fn verified_task_commits(
        &self,
        run_id: &RunId,
    ) -> Result<Vec<(TaskId, String)>, StoreError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT t.id,tr.verified_commit_sha FROM tasks t JOIN task_attempts a ON a.task_id=t.id AND a.attempt_number=t.current_attempt_number JOIN task_results tr ON tr.task_attempt_id=a.id WHERE t.run_id=?1 AND t.state='VERIFIED' AND tr.verified_commit_sha IS NOT NULL ORDER BY t.created_at,t.id",
        )?;
        let rows = statement.query_map([run_id.as_str()], |row| {
            Ok((TaskId::from(row.get::<_, String>(0)?), row.get(1)?))
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn set_attempt_result(
        &self,
        attempt_id: &AttemptId,
        state: &str,
        head_sha: Option<&str>,
        terminal_class: Option<&str>,
        failure_reason: Option<&str>,
    ) -> Result<(), StoreError> {
        let terminal = matches!(state, "COMPLETED" | "FAILED" | "INTERRUPTED" | "STALLED");
        self.connection()?.execute(
            "UPDATE task_attempts SET state=?2,head_sha=coalesce(?3,head_sha),terminal_class=?4,failure_reason=?5,started_at=coalesce(started_at,?6),completed_at=CASE WHEN ?7 THEN ?6 ELSE completed_at END,updated_at=?6,version=version+1 WHERE id=?1",
            params![attempt_id.as_str(),state,head_sha,terminal_class,failure_reason,now_ms(),terminal],
        )?;
        Ok(())
    }

    pub fn set_task_diff_result(
        &self,
        attempt_id: &AttemptId,
        head_sha: Option<&str>,
        files: u32,
        additions: u64,
        deletions: u64,
        unexpected_paths: &[String],
    ) -> Result<(), StoreError> {
        self.connection()?.execute(
            "UPDATE task_results SET verified_commit_sha=coalesce(?2,verified_commit_sha),diff_files=?3,diff_additions=?4,diff_deletions=?5,unexpected_paths_json=?6,updated_at=?7 WHERE task_attempt_id=?1",
            params![attempt_id.as_str(),head_sha,files,additions as i64,deletions as i64,serde_json::to_string(unexpected_paths)?,now_ms()],
        )?;
        Ok(())
    }

    pub fn acquire_path_leases(
        &self,
        run_id: &RunId,
        attempt_id: &AttemptId,
        base_sha: &str,
        paths: &[String],
        ttl_seconds: u64,
    ) -> Result<(), StoreError> {
        let now = now_ms();
        let expires = now.saturating_add((ttl_seconds as i64).saturating_mul(1_000));
        let connection = self.connection()?;
        let transaction = connection.unchecked_transaction()?;
        for path in paths {
            let prefix = normalized_prefix(path);
            let conflict: Option<String> = transaction
                .query_row(
                    "SELECT path_glob FROM path_leases WHERE run_id=?1 AND task_attempt_id<>?4 AND released_at IS NULL AND expires_at>?2 AND (?3 LIKE normalized_prefix || '%' OR normalized_prefix LIKE ?3 || '%') LIMIT 1",
                    params![run_id.as_str(), now, prefix, attempt_id.as_str()],
                    |row| row.get(0),
                )
                .optional()?;
            if let Some(conflict) = conflict {
                return Err(StoreError::Conflict(format!(
                    "path lease {path} overlaps active lease {conflict}"
                )));
            }
            transaction.execute(
                "INSERT INTO path_leases(id,run_id,task_attempt_id,path_glob,normalized_prefix,lease_kind,base_sha,acquired_at,heartbeat_at,expires_at) VALUES(?1,?2,?3,?4,?5,'write',?6,?7,?7,?8)",
                params![ulid::Ulid::generate().to_string(),run_id.as_str(),attempt_id.as_str(),path,prefix,base_sha,now,expires],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn release_path_leases(
        &self,
        attempt_id: &AttemptId,
        reason: &str,
    ) -> Result<(), StoreError> {
        self.connection()?.execute(
            "UPDATE path_leases SET released_at=?2,release_reason=?3 WHERE task_attempt_id=?1 AND released_at IS NULL",
            params![attempt_id.as_str(), now_ms(), reason],
        )?;
        Ok(())
    }

    pub fn heartbeat_path_leases(
        &self,
        attempt_id: &AttemptId,
        ttl_seconds: u64,
    ) -> Result<u64, StoreError> {
        let now = now_ms();
        let expires = now.saturating_add(
            i64::try_from(ttl_seconds)
                .unwrap_or(i64::MAX)
                .saturating_mul(1_000),
        );
        let changed = self.connection()?.execute(
            "UPDATE path_leases SET heartbeat_at=?2,expires_at=?3 WHERE task_attempt_id=?1 AND released_at IS NULL",
            params![attempt_id.as_str(), now, expires],
        )?;
        Ok(changed as u64)
    }

    pub fn heartbeat_run_path_leases(
        &self,
        run_id: &RunId,
        ttl_seconds: u64,
    ) -> Result<u64, StoreError> {
        let now = now_ms();
        let expires = now.saturating_add(
            i64::try_from(ttl_seconds)
                .unwrap_or(i64::MAX)
                .saturating_mul(1_000),
        );
        let changed = self.connection()?.execute(
            "UPDATE path_leases SET heartbeat_at=?2,expires_at=?3 WHERE run_id=?1 AND released_at IS NULL AND task_attempt_id IN (SELECT a.id FROM task_attempts a JOIN tasks t ON t.id=a.task_id WHERE t.run_id=?1 AND a.attempt_number=t.current_attempt_number AND t.state IN ('LEASED','STARTING','IMPLEMENTING','REVIEW_READY','VERIFYING','WAITING_APPROVAL'))",
            params![run_id.as_str(), now, expires],
        )?;
        Ok(changed as u64)
    }

    pub fn release_run_path_leases(&self, run_id: &RunId, reason: &str) -> Result<(), StoreError> {
        let now = now_ms();
        self.connection()?.execute(
            "UPDATE path_leases SET released_at=?2,release_reason=?3 WHERE run_id=?1 AND released_at IS NULL",
            params![run_id.as_str(), now, reason],
        )?;
        Ok(())
    }

    pub fn append_raw_event(&self, input: &RawEventInput) -> Result<i64, StoreError> {
        let payload = serde_json::to_string(&input.payload)?;
        let digest = sha256(payload.as_bytes());
        let connection = self.connection()?;
        let result = connection.execute(
            "INSERT INTO raw_events(run_id,agent_session_id,thread_id,turn_id,direction,method,request_id,received_at,payload_json,payload_sha256,source_sequence,redaction_class) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)",
            params![input.run_id.as_ref().map(RunId::as_str),input.agent_session_id.as_ref().map(AgentSessionId::as_str),input.thread_id,input.turn_id,input.direction,input.method,input.request_id,now_ms(),payload,digest,input.source_sequence,input.redaction_class],
        );
        match result {
            Ok(_) => Ok(connection.last_insert_rowid()),
            Err(rusqlite::Error::SqliteFailure(error, _))
                if error.code == rusqlite::ErrorCode::ConstraintViolation =>
            {
                if let (Some(thread), Some(sequence)) = (&input.thread_id, &input.source_sequence) {
                    connection
                        .query_row(
                            "SELECT id FROM raw_events WHERE thread_id=?1 AND source_sequence=?2",
                            params![thread, sequence],
                            |row| row.get(0),
                        )
                        .map_err(Into::into)
                } else {
                    Err(StoreError::Conflict("duplicate raw event".to_owned()))
                }
            }
            Err(error) => Err(error.into()),
        }
    }

    pub fn emit_domain_event(
        &self,
        run_id: Option<&RunId>,
        aggregate_type: &str,
        aggregate_id: &str,
        event_type: &str,
        payload: &Value,
        source_raw_event_id: Option<i64>,
    ) -> Result<DomainEvent, StoreError> {
        let occurred_at = now_ms();
        let raw = serde_json::to_string(payload)?;
        let connection = self.connection()?;
        connection.execute(
            "INSERT OR IGNORE INTO domain_events(run_id,aggregate_type,aggregate_id,event_type,occurred_at,payload_json,source_raw_event_id) VALUES(?1,?2,?3,?4,?5,?6,?7)",
            params![run_id.map(RunId::as_str),aggregate_type,aggregate_id,event_type,occurred_at,raw,source_raw_event_id],
        )?;
        let id = if connection.changes() == 1 {
            connection.last_insert_rowid()
        } else {
            connection.query_row(
                "SELECT id FROM domain_events WHERE aggregate_type=?1 AND aggregate_id=?2 AND event_type=?3 AND source_raw_event_id IS ?4",
                params![aggregate_type,aggregate_id,event_type,source_raw_event_id],
                |row| row.get(0),
            )?
        };
        Ok(DomainEvent {
            id,
            run_id: run_id.cloned(),
            event_type: event_type.to_owned(),
            aggregate_type: aggregate_type.to_owned(),
            aggregate_id: aggregate_id.to_owned(),
            occurred_at,
            payload: payload.clone(),
        })
    }

    pub fn list_domain_events(
        &self,
        after: i64,
        run_id: Option<&RunId>,
        limit: u32,
    ) -> Result<Vec<DomainEvent>, StoreError> {
        let connection = self.connection()?;
        let sql = if run_id.is_some() {
            "SELECT id,run_id,aggregate_type,aggregate_id,event_type,occurred_at,payload_json FROM domain_events WHERE id>?1 AND run_id=?2 ORDER BY id LIMIT ?3"
        } else {
            "SELECT id,run_id,aggregate_type,aggregate_id,event_type,occurred_at,payload_json FROM domain_events WHERE id>?1 ORDER BY id LIMIT ?2"
        };
        let mut statement = connection.prepare(sql)?;
        let rows = if let Some(run_id) = run_id {
            statement.query_map(params![after, run_id.as_str(), limit], map_domain_event)?
        } else {
            statement.query_map(params![after, limit], map_domain_event)?
        };
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn latest_domain_cursor(&self) -> Result<i64, StoreError> {
        Ok(self.connection()?.query_row(
            "SELECT coalesce(max(id),0) FROM domain_events",
            [],
            |row| row.get(0),
        )?)
    }

    pub fn list_activity(
        &self,
        agent_id: &AgentSessionId,
        after: i64,
        limit: u32,
    ) -> Result<Vec<ActivityItem>, StoreError> {
        let connection = self.connection()?;
        let thread: Option<String> = connection
            .query_row(
                "SELECT thread_id FROM codex_threads WHERE agent_session_id=?1",
                [agent_id.as_str()],
                |row| row.get(0),
            )
            .optional()?;
        let Some(thread) = thread else {
            return Ok(Vec::new());
        };
        let mut statement = connection.prepare(
            "SELECT re.id,re.method,coalesce(pi.state,'completed'),coalesce(pi.summary,re.method),re.payload_json,re.received_at FROM raw_events re LEFT JOIN projected_items pi ON pi.source_raw_event_id=re.id WHERE re.thread_id=?1 AND re.id>?2 ORDER BY re.id LIMIT ?3",
        )?;
        let rows = statement.query_map(params![thread, after, limit], |row| {
            let raw: String = row.get(4)?;
            Ok(ActivityItem {
                id: row.get::<_, i64>(0)?.to_string(),
                sequence: row.get(0)?,
                kind: row.get(1)?,
                state: row.get(2)?,
                summary: row.get(3)?,
                payload: serde_json::from_str(&raw).unwrap_or(Value::Null),
                occurred_at: format_timestamp(row.get(5)?),
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn list_recent_activity(
        &self,
        agent_id: &AgentSessionId,
        limit: u32,
    ) -> Result<Vec<ActivityItem>, StoreError> {
        let connection = self.connection()?;
        let thread: Option<String> = connection
            .query_row(
                "SELECT thread_id FROM codex_threads WHERE agent_session_id=?1",
                [agent_id.as_str()],
                |row| row.get(0),
            )
            .optional()?;
        let Some(thread) = thread else {
            return Ok(Vec::new());
        };
        let mut statement = connection.prepare(
            "SELECT id,method,state,summary,payload_json,received_at FROM (SELECT re.id,re.method,coalesce(pi.state,'completed') AS state,coalesce(pi.summary,re.method) AS summary,re.payload_json,re.received_at FROM raw_events re LEFT JOIN projected_items pi ON pi.source_raw_event_id=re.id WHERE re.thread_id=?1 ORDER BY re.id DESC LIMIT ?2) ORDER BY id",
        )?;
        let rows = statement.query_map(params![thread, limit], |row| {
            let raw: String = row.get(4)?;
            Ok(ActivityItem {
                id: row.get::<_, i64>(0)?.to_string(),
                sequence: row.get(0)?,
                kind: row.get(1)?,
                state: row.get(2)?,
                summary: row.get(3)?,
                payload: serde_json::from_str(&raw).unwrap_or(Value::Null),
                occurred_at: format_timestamp(row.get(5)?),
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn latest_agent_message(
        &self,
        agent_id: &AgentSessionId,
    ) -> Result<Option<LatestAgentMessage>, StoreError> {
        self.connection()?
            .query_row(
                "SELECT pi.item_id,json_extract(pi.payload_json,'$.text'),json_extract(pi.payload_json,'$.phase'),coalesce(pi.completed_at,pi.started_at) FROM projected_items pi JOIN codex_threads ct ON ct.thread_id=pi.thread_id WHERE ct.agent_session_id=?1 AND pi.item_type='agentMessage' AND pi.state='completed' AND json_extract(pi.payload_json,'$.text') IS NOT NULL ORDER BY (json_extract(pi.payload_json,'$.phase')='final_answer') DESC,coalesce(pi.completed_at,pi.started_at) DESC LIMIT 1",
                [agent_id.as_str()],
                |row| {
                    Ok(LatestAgentMessage {
                        id: row.get(0)?,
                        text: row.get(1)?,
                        phase: row.get(2)?,
                        occurred_at: format_timestamp(row.get(3)?),
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn list_agent_messages(
        &self,
        agent_id: &AgentSessionId,
        limit: u32,
    ) -> Result<Vec<LatestAgentMessage>, StoreError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT item_id,text,phase,occurred_at FROM (SELECT pi.item_id AS item_id,json_extract(pi.payload_json,'$.text') AS text,json_extract(pi.payload_json,'$.phase') AS phase,coalesce(pi.completed_at,pi.started_at) AS occurred_at,pi.rowid AS sort_id FROM projected_items pi JOIN codex_threads ct ON ct.thread_id=pi.thread_id WHERE ct.agent_session_id=?1 AND pi.item_type='agentMessage' AND pi.state='completed' AND json_extract(pi.payload_json,'$.text') IS NOT NULL ORDER BY occurred_at DESC,sort_id DESC LIMIT ?2) ORDER BY occurred_at,sort_id",
        )?;
        let rows = statement.query_map(params![agent_id.as_str(), limit], |row| {
            Ok(LatestAgentMessage {
                id: row.get(0)?,
                text: row.get(1)?,
                phase: row.get(2)?,
                occurred_at: format_timestamp(row.get(3)?),
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn latest_agent_plan(
        &self,
        agent_id: &AgentSessionId,
    ) -> Result<Option<Value>, StoreError> {
        self.connection()?
            .query_row(
                "SELECT pi.payload_json FROM projected_items pi JOIN codex_threads ct ON ct.thread_id=pi.thread_id WHERE ct.agent_session_id=?1 AND pi.item_type='plan' ORDER BY pi.rowid DESC LIMIT 1",
                [agent_id.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|raw| serde_json::from_str(&raw).map_err(StoreError::from))
            .transpose()
    }

    pub fn list_task_governor_messages(
        &self,
        task_id: &TaskId,
        limit: u32,
    ) -> Result<Vec<LatestAgentMessage>, StoreError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT item_id,text,phase,occurred_at FROM (SELECT pi.item_id AS item_id,json_extract(pi.payload_json,'$.text') AS text,json_extract(pi.payload_json,'$.phase') AS phase,coalesce(pi.completed_at,pi.started_at) AS occurred_at,pi.rowid AS sort_id FROM projected_items pi JOIN codex_threads ct ON ct.thread_id=pi.thread_id JOIN agent_sessions a ON a.id=ct.agent_session_id JOIN task_attempts ta ON ta.id=a.task_attempt_id WHERE ta.task_id=?1 AND a.role='governor' AND pi.item_type='agentMessage' AND pi.state='completed' AND json_extract(pi.payload_json,'$.text') IS NOT NULL ORDER BY occurred_at DESC,sort_id DESC LIMIT ?2) ORDER BY occurred_at,sort_id",
        )?;
        let rows = statement.query_map(params![task_id.as_str(), limit], |row| {
            Ok(LatestAgentMessage {
                id: row.get(0)?,
                text: row.get(1)?,
                phase: row.get(2)?,
                occurred_at: format_timestamp(row.get(3)?),
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn create_approval(&self, input: &NewApproval, rpc_id: &Value) -> Result<(), StoreError> {
        let now = now_ms();
        let request_json = serde_json::to_string(&input.request)?;
        let request_sha = sha256(request_json.as_bytes());
        let connection = self.connection()?;
        let transaction = connection.unchecked_transaction()?;
        transaction.execute(
            "INSERT INTO approvals(id,run_id,task_attempt_id,agent_session_id,thread_id,turn_id,item_id,approval_type,risk_level,request_json,request_sha256,expected_head_sha,expected_worktree_fingerprint,state,created_at,version) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,'pending',?14,1)",
            params![input.id.as_str(),input.run_id.as_str(),input.task_attempt_id.as_ref().map(AttemptId::as_str),input.agent_session_id.as_ref().map(AgentSessionId::as_str),input.thread_id,input.turn_id,input.item_id,input.approval_type,enum_text(&input.risk_level)?,request_json,request_sha,input.expected_head_sha,input.expected_worktree_fingerprint,now],
        )?;
        transaction.execute(
            "INSERT INTO runtime_rpc_requests(request_key,rpc_id_json,method,run_id,agent_session_id,thread_id,turn_id,item_id,payload_json,state,received_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,'pending',?10)",
            params![input.id.as_str(),serde_json::to_string(rpc_id)?,input.approval_type,input.run_id.as_str(),input.agent_session_id.as_ref().map(AgentSessionId::as_str),input.thread_id,input.turn_id,input.item_id,request_json,now],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn list_approvals(
        &self,
        run_id: Option<&RunId>,
        state: Option<&str>,
    ) -> Result<Vec<ApprovalSummary>, StoreError> {
        let connection = self.connection()?;
        let mut conditions = Vec::new();
        let mut values = Vec::<String>::new();
        if let Some(run_id) = run_id {
            conditions.push(format!("ap.run_id=?{}", values.len() + 1));
            values.push(run_id.to_string());
        }
        if let Some(state) = state {
            conditions.push(format!("ap.state=?{}", values.len() + 1));
            values.push(state.to_owned());
        }
        let where_clause = if conditions.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", conditions.join(" AND "))
        };
        let sql = format!(
            "SELECT ap.id,ap.run_id,ap.agent_session_id,t.id,ap.thread_id,ap.turn_id,ap.approval_type,ap.risk_level,ap.request_json,ap.state,ap.decision,ap.created_at,ap.resolved_at,ap.version FROM approvals ap LEFT JOIN task_attempts a ON a.id=ap.task_attempt_id LEFT JOIN tasks t ON t.id=a.task_id{} ORDER BY CASE ap.risk_level WHEN 'critical' THEN 0 WHEN 'high' THEN 1 WHEN 'medium' THEN 2 ELSE 3 END,ap.created_at",
            where_clause
        );
        let mut statement = connection.prepare(&sql)?;
        let rows = statement.query_map(rusqlite::params_from_iter(values), map_approval)?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    pub fn approval(&self, id: &ApprovalId) -> Result<ApprovalSummary, StoreError> {
        let connection = self.connection()?;
        connection.query_row(
            "SELECT ap.id,ap.run_id,ap.agent_session_id,t.id,ap.thread_id,ap.turn_id,ap.approval_type,ap.risk_level,ap.request_json,ap.state,ap.decision,ap.created_at,ap.resolved_at,ap.version FROM approvals ap LEFT JOIN task_attempts a ON a.id=ap.task_attempt_id LEFT JOIN tasks t ON t.id=a.task_id WHERE ap.id=?1",
            [id.as_str()],
            map_approval,
        ).optional()?.ok_or_else(|| StoreError::NotFound(format!("approval {id}")))
    }

    pub fn approval_expected_custody(
        &self,
        id: &ApprovalId,
    ) -> Result<(Option<String>, Option<String>), StoreError> {
        self.connection()?
            .query_row(
                "SELECT expected_head_sha,expected_worktree_fingerprint FROM approvals WHERE id=?1",
                [id.as_str()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("approval {id}")))
    }

    pub fn decide_approval(
        &self,
        id: &ApprovalId,
        decision: &str,
        note: Option<&str>,
        actor: &str,
        expected_version: Option<u64>,
    ) -> Result<(ApprovalSummary, Value), StoreError> {
        let approval = self.approval(id)?;
        if approval.state != "pending" {
            return Err(StoreError::Conflict(format!(
                "approval {id} is already {}",
                approval.state
            )));
        }
        if expected_version.is_some_and(|version| version != approval.version) {
            return Err(StoreError::Conflict(format!(
                "approval {id} changed concurrently"
            )));
        }
        let connection = self.connection()?;
        let transaction = connection.unchecked_transaction()?;
        let rpc: String = transaction.query_row(
            "SELECT rpc_id_json FROM runtime_rpc_requests WHERE request_key=?1 AND state='pending'",
            [id.as_str()],
            |row| row.get(0),
        )?;
        let now = now_ms();
        let current_version = sqlite_version(approval.version)?;
        transaction.execute(
            "UPDATE approvals SET state='delivering',decision=?2,decision_note=?3,decided_by=?4,resolved_at=?5,version=version+1 WHERE id=?1 AND version=?6",
            params![id.as_str(),decision,note,actor,now,current_version],
        )?;
        transaction.commit()?;
        Ok((self.approval(id)?, serde_json::from_str(&rpc)?))
    }

    pub fn mark_approval_delivered(
        &self,
        id: &ApprovalId,
        delivery_error: Option<&str>,
    ) -> Result<(), StoreError> {
        let state = if delivery_error.is_some() {
            "pending"
        } else {
            "resolved"
        };
        let now = now_ms();
        let connection = self.connection()?;
        let transaction = connection.unchecked_transaction()?;
        transaction.execute(
            "UPDATE approvals SET state=?2,delivered_at=CASE WHEN ?3 IS NULL THEN ?4 ELSE delivered_at END,delivery_error=?3,version=version+1 WHERE id=?1",
            params![id.as_str(),state,delivery_error,now],
        )?;
        if delivery_error.is_none() {
            transaction.execute(
                "UPDATE runtime_rpc_requests SET state='resolved',resolved_at=?2 WHERE request_key=?1",
                params![id.as_str(), now],
            )?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub fn expire_pending_approvals(&self, run_id: &RunId, reason: &str) -> Result<(), StoreError> {
        let now = now_ms();
        let connection = self.connection()?;
        let transaction = connection.unchecked_transaction()?;
        transaction.execute(
            "UPDATE approvals SET state='expired',decision='cancel',decision_note=?2,decided_by='harnessd-recovery',resolved_at=?3,delivery_error='originating App Server process is unavailable',version=version+1 WHERE run_id=?1 AND state IN ('pending','delivering')",
            params![run_id.as_str(), reason, now],
        )?;
        transaction.execute(
            "UPDATE runtime_rpc_requests SET state='expired',resolved_at=?2 WHERE run_id=?1 AND state='pending'",
            params![run_id.as_str(), now],
        )?;
        transaction.commit()?;
        Ok(())
    }

    pub fn create_api_session(
        &self,
        id: &str,
        csrf_secret_hash: &str,
        ttl_seconds: u64,
    ) -> Result<StoredSession, StoreError> {
        let now = now_ms();
        let expires = now.saturating_add((ttl_seconds as i64).saturating_mul(1_000));
        let connection = self.connection()?;
        let transaction = connection.unchecked_transaction()?;
        transaction.execute(
            "DELETE FROM api_sessions WHERE expires_at<=?1 OR revoked_at IS NOT NULL",
            [now],
        )?;
        transaction.execute(
            "INSERT INTO api_sessions(id,created_at,expires_at,csrf_secret_hash,last_seen_at) VALUES(?1,?2,?3,?4,?2)",
            params![id,now,expires,csrf_secret_hash],
        )?;
        transaction.commit()?;
        Ok(StoredSession {
            id: id.to_owned(),
            expires_at: expires,
            csrf_secret_hash: csrf_secret_hash.to_owned(),
            revoked: false,
        })
    }

    pub fn api_session(&self, id: &str) -> Result<Option<StoredSession>, StoreError> {
        let now = now_ms();
        let connection = self.connection()?;
        let session = connection
            .query_row(
                "SELECT id,expires_at,csrf_secret_hash,revoked_at IS NOT NULL FROM api_sessions WHERE id=?1 AND expires_at>?2",
                params![id,now],
                |row| Ok(StoredSession { id: row.get(0)?, expires_at: row.get(1)?, csrf_secret_hash: row.get(2)?, revoked: row.get(3)? }),
            )
            .optional()?;
        if session.is_some() {
            connection.execute(
                "UPDATE api_sessions SET last_seen_at=?2 WHERE id=?1",
                params![id, now],
            )?;
        }
        Ok(session)
    }

    // This mirrors the narrow immutable audit-row boundary; grouping these
    // columns would obscure which values are committed in one transaction.
    #[allow(clippy::too_many_arguments)]
    pub fn record_human_action(
        &self,
        run_id: Option<&RunId>,
        attempt_id: Option<&AttemptId>,
        actor: &str,
        action_type: &str,
        target_type: &str,
        target_id: &str,
        payload: &Value,
    ) -> Result<i64, StoreError> {
        let raw = serde_json::to_string(payload)?;
        let digest = sha256(raw.as_bytes());
        let connection = self.connection()?;
        connection.execute(
            "INSERT INTO human_actions(run_id,task_attempt_id,actor,action_type,target_type,target_id,occurred_at,payload_json,payload_sha256) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)",
            params![run_id.map(RunId::as_str),attempt_id.map(AttemptId::as_str),actor,action_type,target_type,target_id,now_ms(),raw,digest],
        )?;
        Ok(connection.last_insert_rowid())
    }

    pub fn register_artifact(&self, input: &NewArtifact) -> Result<ArtifactId, StoreError> {
        let connection = self.connection()?;
        let existing = connection
            .query_row(
                "SELECT id FROM artifacts WHERE sha256=?1",
                [input.sha256.as_str()],
                |row| row.get::<_, String>(0),
            )
            .optional()?;
        if let Some(existing) = existing {
            return Ok(ArtifactId::from(existing));
        }
        connection.execute(
            "INSERT INTO artifacts(id,run_id,task_attempt_id,kind,logical_name,storage_path,sha256,media_type,compression,sensitivity,byte_length,retention_class,pinned,created_at,verified_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?14)",
            params![
                input.id.as_str(),
                input.run_id.as_ref().map(RunId::as_str),
                input.task_attempt_id.as_ref().map(AttemptId::as_str),
                input.kind,
                input.logical_name,
                input.storage_path.to_string_lossy(),
                input.sha256,
                input.media_type,
                input.compression,
                input.sensitivity,
                i64::try_from(input.byte_length).unwrap_or(i64::MAX),
                input.retention_class,
                input.pinned,
                now_ms(),
            ],
        )?;
        Ok(input.id.clone())
    }

    pub fn artifact(&self, id: &ArtifactId) -> Result<ArtifactRecord, StoreError> {
        self.connection()?
            .query_row(
                "SELECT id,kind,logical_name,storage_path,sha256,media_type,byte_length,verified_at FROM artifacts WHERE id=?1",
                [id.as_str()],
                |row| {
                    Ok(ArtifactRecord {
                        id: ArtifactId::from(row.get::<_, String>(0)?),
                        kind: row.get(1)?,
                        logical_name: row.get(2)?,
                        storage_path: Path::new(&row.get::<_, String>(3)?).to_path_buf(),
                        sha256: row.get(4)?,
                        media_type: row.get(5)?,
                        byte_length: row.get::<_, i64>(6)? as u64,
                        verified_at: row.get(7)?,
                    })
                },
            )
            .optional()?
            .ok_or_else(|| StoreError::NotFound(format!("artifact {id}")))
    }

    pub fn record_command(&self, input: &NewCommandRecord) -> Result<(), StoreError> {
        let command_json = serde_json::to_string(&input.command)?;
        let command_sha = sha256(command_json.as_bytes());
        self.connection()?.execute(
            "INSERT INTO command_runs(id,run_id,task_attempt_id,agent_session_id,worktree_id,command_json,command_sha256,cwd,source_sha_before,source_sha_after,resource_class,host_identity,target_profile,started_at,completed_at,exit_code,signal,timed_out,result_class,stdout_artifact_id,stderr_artifact_id,error_json,version) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,1)",
            params![
                input.id.as_str(),
                input.run_id.as_str(),
                input.task_attempt_id.as_ref().map(AttemptId::as_str),
                input.agent_session_id.as_ref().map(AgentSessionId::as_str),
                input.worktree_id.as_ref().map(harness_domain::WorktreeId::as_str),
                command_json,
                command_sha,
                input.cwd.to_string_lossy(),
                input.source_sha_before,
                input.source_sha_after,
                input.resource_class,
                input.host_identity,
                input.target_profile,
                input.started_at,
                input.completed_at,
                input.exit_code,
                input.signal,
                input.timed_out,
                enum_text(&input.result_class)?,
                input.stdout_artifact_id.as_ref().map(ArtifactId::as_str),
                input.stderr_artifact_id.as_ref().map(ArtifactId::as_str),
                input.error.as_ref().map(serde_json::to_string).transpose()?,
            ],
        )?;
        Ok(())
    }

    pub fn record_validation(&self, input: &NewValidationRecord) -> Result<(), StoreError> {
        self.connection()?.execute(
            "INSERT INTO validations(id,run_id,task_attempt_id,worktree_id,validator_id,proof_tier,source_sha,selector_reason,state,result_class,command_run_id,started_at,completed_at,version) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,'completed',?9,?10,?11,?12,1)",
            params![
                input.id.as_str(),
                input.run_id.as_str(),
                input.task_attempt_id.as_ref().map(AttemptId::as_str),
                input.worktree_id.as_str(),
                input.validator_id,
                enum_text(&input.proof_tier)?,
                input.source_sha,
                input.selector_reason,
                enum_text(&input.result_class)?,
                input.command_run_id.as_ref().map(harness_domain::CommandRunId::as_str),
                input.started_at,
                input.completed_at,
            ],
        )?;
        Ok(())
    }

    pub fn record_evidence(&self, input: &NewEvidenceRecord) -> Result<(), StoreError> {
        let evidence_json = serde_json::to_string(&input.evidence)?;
        let evidence_sha = sha256(evidence_json.as_bytes());
        self.connection()?.execute(
            "INSERT INTO evidence_records(id,run_id,task_attempt_id,validation_id,claim_id,checklist_rows_json,source_sha,proof_tier,result_class,evidence_json,evidence_sha256,unproved_claims_json,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)",
            params![
                input.id.as_str(),
                input.run_id.as_str(),
                input.task_attempt_id.as_ref().map(AttemptId::as_str),
                input.validation_id.as_ref().map(harness_domain::ValidationId::as_str),
                input.claim_id,
                serde_json::to_string(&input.checklist_rows)?,
                input.source_sha,
                enum_text(&input.proof_tier)?,
                enum_text(&input.result_class)?,
                evidence_json,
                evidence_sha,
                serde_json::to_string(&input.unproved_claims)?,
                now_ms(),
            ],
        )?;
        Ok(())
    }

    pub fn link_evidence_artifact(
        &self,
        evidence_id: &harness_domain::EvidenceId,
        artifact_id: &ArtifactId,
        purpose: &str,
    ) -> Result<(), StoreError> {
        self.connection()?.execute(
            "INSERT OR IGNORE INTO evidence_artifacts(evidence_id,artifact_id,purpose) VALUES(?1,?2,?3)",
            params![evidence_id.as_str(), artifact_id.as_str(), purpose],
        )?;
        Ok(())
    }

    pub fn evidence_snapshot(&self, run_id: &RunId) -> Result<Value, StoreError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT e.id,e.task_attempt_id,e.validation_id,e.claim_id,e.checklist_rows_json,e.source_sha,e.proof_tier,e.result_class,e.evidence_json,e.evidence_sha256,e.unproved_claims_json,e.created_at,e.invalidated_at,e.invalidated_reason FROM evidence_records e WHERE e.run_id=?1 ORDER BY e.created_at,e.id",
        )?;
        let evidence = statement
            .query_map([run_id.as_str()], |row| {
                let checklist: String = row.get(4)?;
                let evidence: String = row.get(8)?;
                let unproved: String = row.get(10)?;
                Ok(json!({
                    "id": row.get::<_, String>(0)?,
                    "task_attempt_id": row.get::<_, Option<String>>(1)?,
                    "validation_id": row.get::<_, Option<String>>(2)?,
                    "claim_id": row.get::<_, String>(3)?,
                    "checklist_rows": serde_json::from_str::<Value>(&checklist).unwrap_or(Value::Null),
                    "source_sha": row.get::<_, String>(5)?,
                    "proof_tier": row.get::<_, String>(6)?,
                    "result_class": row.get::<_, String>(7)?,
                    "evidence": serde_json::from_str::<Value>(&evidence).unwrap_or(Value::Null),
                    "evidence_sha256": row.get::<_, String>(9)?,
                    "unproved_claims": serde_json::from_str::<Value>(&unproved).unwrap_or(Value::Null),
                    "created_at": row.get::<_, i64>(11)?,
                    "invalidated_at": row.get::<_, Option<i64>>(12)?,
                    "invalidated_reason": row.get::<_, Option<String>>(13)?,
                }))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        drop(statement);

        let mut artifacts_statement = connection.prepare(
            "SELECT DISTINCT a.id,a.kind,a.logical_name,a.storage_path,a.sha256,a.media_type,a.byte_length FROM artifacts a JOIN evidence_artifacts ea ON ea.artifact_id=a.id JOIN evidence_records e ON e.id=ea.evidence_id WHERE e.run_id=?1 ORDER BY a.id",
        )?;
        let artifacts = artifacts_statement
            .query_map([run_id.as_str()], |row| {
                Ok(json!({
                    "id": row.get::<_, String>(0)?,
                    "kind": row.get::<_, String>(1)?,
                    "logical_name": row.get::<_, String>(2)?,
                    "storage_path": row.get::<_, String>(3)?,
                    "sha256": row.get::<_, String>(4)?,
                    "media_type": row.get::<_, String>(5)?,
                    "byte_length": row.get::<_, i64>(6)?,
                }))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        drop(artifacts_statement);
        drop(connection);
        let run = self.run(run_id)?;
        let tasks = self.list_tasks(run_id)?;
        let agents = self.list_agents(run_id)?;
        Ok(json!({
            "schema": "harness-evidence-snapshot/v1",
            "run": run,
            "tasks": tasks,
            "agents": agents,
            "evidence": evidence,
            "artifacts": artifacts,
        }))
    }

    pub fn run_usage(&self, run_id: &RunId) -> Result<UsageSummary, StoreError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT ts.effective_model,ts.input_tokens,ts.cached_input_tokens,ts.cache_write_input_tokens,ts.output_tokens,ts.reasoning_output_tokens,ts.total_tokens,ts.model_context_window,c.lower_microusd,c.upper_microusd,c.confidence,c.explanation,c.pricing_snapshot_id FROM token_samples ts JOIN codex_threads ct ON ct.thread_id=ts.thread_id JOIN agent_sessions a ON a.id=ct.agent_session_id LEFT JOIN cost_entries c ON c.token_sample_id=ts.id WHERE a.run_id=?1 ORDER BY ts.observed_at,ts.id",
        )?;
        let rows = statement.query_map([run_id.as_str()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                TokenUsage {
                    input_tokens: row.get::<_, i64>(1)? as u64,
                    cached_input_tokens: row.get::<_, i64>(2)? as u64,
                    cache_write_input_tokens: row
                        .get::<_, Option<i64>>(3)?
                        .map(|value| value as u64),
                    output_tokens: row.get::<_, i64>(4)? as u64,
                    reasoning_output_tokens: row.get::<_, i64>(5)? as u64,
                    total_tokens: row.get::<_, i64>(6)? as u64,
                    model_context_window: row.get::<_, Option<i64>>(7)?.map(|value| value as u64),
                },
                row.get::<_, Option<i64>>(8)?,
                row.get::<_, Option<i64>>(9)?,
                row.get::<_, Option<String>>(10)?,
                row.get::<_, Option<String>>(11)?,
                row.get::<_, Option<String>>(12)?,
            ))
        })?;
        let mut summary = UsageSummary::default();
        let mut models = BTreeMap::<String, ModelUsageSummary>::new();
        for row in rows {
            let (model, usage, lower, upper, confidence, explanation, pricing_id) = row?;
            let cost = CostEstimate {
                lower_microusd: lower.unwrap_or_default().max(0) as u64,
                upper_microusd: upper.unwrap_or_default().max(0) as u64,
                confidence: match confidence.as_deref() {
                    Some("exact") => CostConfidence::Exact,
                    Some("bounded") => CostConfidence::Bounded,
                    _ => CostConfidence::Unknown,
                },
                pricing_snapshot_ids: pricing_id.into_iter().collect(),
                explanation: explanation.unwrap_or_else(|| "No matching price snapshot".to_owned()),
            };
            harness_usage::add_sample(&mut summary, &usage, &cost)
                .map_err(|error| StoreError::Validation(error.to_string()))?;
            let model_summary = models
                .entry(model.clone())
                .or_insert_with(|| ModelUsageSummary {
                    model,
                    ..ModelUsageSummary::default()
                });
            model_summary.turns = model_summary.turns.saturating_add(1);
            let mut temporary = UsageSummary {
                input_tokens: model_summary.usage.input_tokens,
                cached_input_tokens: model_summary.usage.cached_input_tokens,
                cache_write_input_tokens: model_summary.usage.cache_write_input_tokens,
                output_tokens: model_summary.usage.output_tokens,
                reasoning_output_tokens: model_summary.usage.reasoning_output_tokens,
                total_tokens: model_summary.usage.total_tokens,
                cost: model_summary.cost.clone(),
                by_model: vec![],
            };
            harness_usage::add_sample(&mut temporary, &usage, &cost)
                .map_err(|error| StoreError::Validation(error.to_string()))?;
            model_summary.usage = TokenUsage {
                input_tokens: temporary.input_tokens,
                cached_input_tokens: temporary.cached_input_tokens,
                cache_write_input_tokens: temporary.cache_write_input_tokens,
                output_tokens: temporary.output_tokens,
                reasoning_output_tokens: temporary.reasoning_output_tokens,
                total_tokens: temporary.total_tokens,
                model_context_window: usage.model_context_window,
            };
            model_summary.cost = temporary.cost;
        }
        summary.by_model = models.into_values().collect();
        Ok(summary)
    }

    pub fn usage_breakdown(&self) -> Result<UsageBreakdown, StoreError> {
        let connection = self.connection()?;
        let mut statement = connection.prepare(
            "SELECT coalesce(a.codex_account_id,'legacy'),r.id,r.display_name,a.id,coalesce(a.nickname,a.role),a.role,ts.effective_model,ts.input_tokens,ts.cached_input_tokens,ts.cache_write_input_tokens,ts.output_tokens,ts.reasoning_output_tokens,ts.total_tokens,ts.model_context_window,c.lower_microusd,c.upper_microusd,c.confidence,c.explanation,c.pricing_snapshot_id FROM token_samples ts JOIN codex_threads ct ON ct.thread_id=ts.thread_id JOIN agent_sessions a ON a.id=ct.agent_session_id JOIN runs ru ON ru.id=a.run_id JOIN repositories r ON r.id=ru.repository_id LEFT JOIN cost_entries c ON c.token_sample_id=ts.id ORDER BY ts.observed_at,ts.id",
        )?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                TokenUsage {
                    input_tokens: row.get::<_, i64>(7)? as u64,
                    cached_input_tokens: row.get::<_, i64>(8)? as u64,
                    cache_write_input_tokens: row
                        .get::<_, Option<i64>>(9)?
                        .map(|value| value as u64),
                    output_tokens: row.get::<_, i64>(10)? as u64,
                    reasoning_output_tokens: row.get::<_, i64>(11)? as u64,
                    total_tokens: row.get::<_, i64>(12)? as u64,
                    model_context_window: row.get::<_, Option<i64>>(13)?.map(|value| value as u64),
                },
                row.get::<_, Option<i64>>(14)?,
                row.get::<_, Option<i64>>(15)?,
                row.get::<_, Option<String>>(16)?,
                row.get::<_, Option<String>>(17)?,
                row.get::<_, Option<String>>(18)?,
            ))
        })?;
        let mut result = UsageBreakdown::default();
        let mut accounts = BTreeMap::<String, UsageGroup>::new();
        let mut repositories = BTreeMap::<String, UsageGroup>::new();
        let mut agents = BTreeMap::<String, UsageGroup>::new();
        for row in rows {
            let (
                account_id,
                repository_id,
                repository_label,
                agent_id,
                agent_label,
                role,
                model,
                usage,
                lower,
                upper,
                confidence,
                explanation,
                pricing_id,
            ) = row?;
            let cost = CostEstimate {
                lower_microusd: lower.unwrap_or_default().max(0) as u64,
                upper_microusd: upper.unwrap_or_default().max(0) as u64,
                confidence: match confidence.as_deref() {
                    Some("exact") => CostConfidence::Exact,
                    Some("bounded") => CostConfidence::Bounded,
                    _ => CostConfidence::Unknown,
                },
                pricing_snapshot_ids: pricing_id.into_iter().collect(),
                explanation: explanation.unwrap_or_else(|| "No matching price snapshot".to_owned()),
            };
            harness_usage::add_sample(&mut result.total, &usage, &cost)
                .map_err(|error| StoreError::Validation(error.to_string()))?;
            add_usage_group(
                &mut accounts,
                account_id.clone(),
                if account_id == "legacy" {
                    "Unattributed history".to_owned()
                } else {
                    account_id
                },
                "Codex account".to_owned(),
                &usage,
                &cost,
            )?;
            add_usage_group(
                &mut repositories,
                repository_id,
                repository_label,
                "Repository".to_owned(),
                &usage,
                &cost,
            )?;
            add_usage_group(
                &mut agents,
                agent_id,
                agent_label,
                format!("{role} · {model}"),
                &usage,
                &cost,
            )?;
        }
        result.by_account = accounts.into_values().collect();
        result.by_repository = repositories.into_values().collect();
        result.by_agent = agents.into_values().collect();
        for groups in [
            &mut result.by_account,
            &mut result.by_repository,
            &mut result.by_agent,
        ] {
            groups.sort_by_key(|group| std::cmp::Reverse(group.usage.total_tokens));
        }
        Ok(result)
    }

    pub fn record_run_export(
        &self,
        run_id: &RunId,
        artifact_id: &ArtifactId,
        manifest_sha256: &str,
    ) -> Result<String, StoreError> {
        let id = ulid::Ulid::generate().to_string();
        let now = now_ms();
        self.connection()?.execute(
            "INSERT INTO run_exports(id,run_id,artifact_id,state,manifest_sha256,created_at,completed_at) VALUES(?1,?2,?3,'completed',?4,?5,?5)",
            params![id, run_id.as_str(), artifact_id.as_str(), manifest_sha256, now],
        )?;
        Ok(id)
    }
}

fn add_usage_group(
    groups: &mut BTreeMap<String, UsageGroup>,
    id: String,
    label: String,
    detail: String,
    usage: &TokenUsage,
    cost: &CostEstimate,
) -> Result<(), StoreError> {
    let group = groups.entry(id.clone()).or_insert_with(|| UsageGroup {
        id,
        label,
        detail,
        ..UsageGroup::default()
    });
    group.turns = group.turns.saturating_add(1);
    let mut summary = UsageSummary {
        input_tokens: group.usage.input_tokens,
        cached_input_tokens: group.usage.cached_input_tokens,
        cache_write_input_tokens: group.usage.cache_write_input_tokens,
        output_tokens: group.usage.output_tokens,
        reasoning_output_tokens: group.usage.reasoning_output_tokens,
        total_tokens: group.usage.total_tokens,
        cost: group.cost.clone(),
        by_model: Vec::new(),
    };
    harness_usage::add_sample(&mut summary, usage, cost)
        .map_err(|error| StoreError::Validation(error.to_string()))?;
    group.usage = TokenUsage {
        input_tokens: summary.input_tokens,
        cached_input_tokens: summary.cached_input_tokens,
        cache_write_input_tokens: summary.cache_write_input_tokens,
        output_tokens: summary.output_tokens,
        reasoning_output_tokens: summary.reasoning_output_tokens,
        total_tokens: summary.total_tokens,
        model_context_window: usage.model_context_window,
    };
    group.cost = summary.cost;
    Ok(())
}

fn repository_select(by_id: bool) -> &'static str {
    if by_id {
        "SELECT r.id,r.profile_id,r.display_name,r.root_path,r.origin_url,r.default_branch,h.primary_branch,h.primary_head_sha,coalesce(h.primary_clean,0),r.state,coalesce(h.blockers_json,'[]'),(SELECT count(*) FROM worktrees w JOIN runs ru ON ru.id=w.run_id WHERE ru.repository_id=r.id AND w.removed_at IS NULL),h.authority_digest,r.version FROM repositories r LEFT JOIN repository_health_snapshots h ON h.id=(SELECT id FROM repository_health_snapshots WHERE repository_id=r.id ORDER BY observed_at DESC LIMIT 1) WHERE r.id=?1"
    } else {
        "SELECT r.id,r.profile_id,r.display_name,r.root_path,r.origin_url,r.default_branch,h.primary_branch,h.primary_head_sha,coalesce(h.primary_clean,0),r.state,coalesce(h.blockers_json,'[]'),(SELECT count(*) FROM worktrees w JOIN runs ru ON ru.id=w.run_id WHERE ru.repository_id=r.id AND w.removed_at IS NULL),h.authority_digest,r.version FROM repositories r LEFT JOIN repository_health_snapshots h ON h.id=(SELECT id FROM repository_health_snapshots WHERE repository_id=r.id ORDER BY observed_at DESC LIMIT 1) ORDER BY r.display_name"
    }
}

fn map_repository(row: &Row<'_>) -> rusqlite::Result<RepositorySummary> {
    let blockers: String = row.get(10)?;
    let state: String = row.get(9)?;
    Ok(RepositorySummary {
        id: RepositoryId::from(row.get::<_, String>(0)?),
        profile_id: row.get(1)?,
        display_name: row.get(2)?,
        root_path: row.get(3)?,
        origin_url: row.get(4)?,
        default_branch: row.get(5)?,
        primary_branch: row.get(6)?,
        primary_head: row.get(7)?,
        primary_clean: row.get(8)?,
        health: state.to_ascii_lowercase(),
        blockers: serde_json::from_str(&blockers).unwrap_or_default(),
        managed_worktree_count: row.get::<_, i64>(11)? as u32,
        authority_digest: row.get(12)?,
        version: row.get::<_, i64>(13)? as u64,
    })
}

fn run_select() -> &'static str {
    "SELECT r.id,r.repository_id,r.title,r.requested_objective,r.mode,r.publication_mode,r.state,r.phase,r.base_ref,r.base_sha,r.integration_branch,r.integration_sha,r.authority_digest,r.created_at,r.started_at,r.completed_at,r.scheduler_paused,r.run_token_budget,r.version FROM runs r"
}

fn map_run(row: &Row<'_>) -> rusqlite::Result<RunSummary> {
    let state: String = row.get(6)?;
    Ok(RunSummary {
        id: RunId::from(row.get::<_, String>(0)?),
        repository_id: RepositoryId::from(row.get::<_, String>(1)?),
        title: row.get(2)?,
        objective: row.get(3)?,
        mode: row.get(4)?,
        publication_mode: row.get(5)?,
        state: state
            .parse()
            .map_err(|error: harness_domain::DomainError| {
                rusqlite::Error::FromSqlConversionFailure(
                    state.len(),
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?,
        phase: row.get(7)?,
        base_ref: row.get(8)?,
        base_sha: row.get(9)?,
        integration_branch: row.get(10)?,
        integration_sha: row.get(11)?,
        authority_digest: row.get(12)?,
        created_at: format_timestamp(row.get(13)?),
        started_at: row.get::<_, Option<i64>>(14)?.map(format_timestamp),
        completed_at: row.get::<_, Option<i64>>(15)?.map(format_timestamp),
        scheduler_paused: row.get(16)?,
        run_token_budget: row.get::<_, Option<i64>>(17)?.map(|value| value as u64),
        version: row.get::<_, i64>(18)? as u64,
    })
}

fn agent_select() -> &'static str {
    "SELECT a.id,a.parent_agent_session_id,t.id,a.role,a.codex_account_id,a.nickname,a.state,a.requested_model,a.effective_model,a.requested_reasoning_effort,a.effective_reasoning_effort,a.sandbox_mode,a.cwd,a.current_goal,d.current_action,a.token_budget,coalesce(a.goal_tokens_used,0),coalesce((SELECT sum(c.lower_microusd) FROM codex_threads ct JOIN token_samples ts ON ts.thread_id=ct.thread_id JOIN cost_entries c ON c.token_sample_id=ts.id WHERE ct.agent_session_id=a.id),0),coalesce((SELECT sum(c.upper_microusd) FROM codex_threads ct JOIN token_samples ts ON ts.thread_id=ct.thread_id JOIN cost_entries c ON c.token_sample_id=ts.id WHERE ct.agent_session_id=a.id),0),a.last_heartbeat_at,ct.thread_id,d.active_turn_id,coalesce(d.context_strategy,'fresh_independent'),d.context_source_attempt_id,d.context_reuse_reason,a.version,active_turn.started_at,active_usage.id,active_usage.input_tokens,active_usage.cached_input_tokens,active_usage.cache_write_input_tokens,active_usage.output_tokens,active_usage.reasoning_output_tokens,active_usage.total_tokens,active_usage.model_context_window FROM agent_sessions a LEFT JOIN task_attempts at ON at.id=a.task_attempt_id LEFT JOIN tasks t ON t.id=at.task_id LEFT JOIN agent_runtime_details d ON d.agent_session_id=a.id LEFT JOIN codex_threads ct ON ct.agent_session_id=a.id LEFT JOIN codex_turns active_turn ON active_turn.turn_id=d.active_turn_id LEFT JOIN token_samples active_usage ON active_usage.turn_id=d.active_turn_id AND active_usage.sample_kind='turn_total'"
}

fn map_agent(row: &Row<'_>) -> rusqlite::Result<AgentSummary> {
    let role: String = row.get(3)?;
    let sandbox: String = row.get(11)?;
    let active_turn_usage = if row.get::<_, Option<String>>(27)?.is_some() {
        Some(TokenUsage {
            input_tokens: row.get::<_, i64>(28)? as u64,
            cached_input_tokens: row.get::<_, i64>(29)? as u64,
            cache_write_input_tokens: row.get::<_, Option<i64>>(30)?.map(|value| value as u64),
            output_tokens: row.get::<_, i64>(31)? as u64,
            reasoning_output_tokens: row.get::<_, i64>(32)? as u64,
            total_tokens: row.get::<_, i64>(33)? as u64,
            model_context_window: row.get::<_, Option<i64>>(34)?.map(|value| value as u64),
        })
    } else {
        None
    };
    Ok(AgentSummary {
        id: AgentSessionId::from(row.get::<_, String>(0)?),
        parent_agent_id: row.get::<_, Option<String>>(1)?.map(AgentSessionId::from),
        task_id: row.get::<_, Option<String>>(2)?.map(TaskId::from),
        role: parse_enum(&role)?,
        codex_account_id: row.get(4)?,
        nickname: row.get(5)?,
        state: row.get(6)?,
        requested_model: row.get(7)?,
        effective_model: row.get(8)?,
        requested_reasoning_effort: row.get(9)?,
        effective_reasoning_effort: row.get(10)?,
        sandbox_mode: parse_enum(&sandbox)?,
        cwd: row.get(12)?,
        current_goal: row.get(13)?,
        current_action: row.get(14)?,
        token_budget: row.get::<_, Option<i64>>(15)?.map(|value| value as u64),
        tokens_used: row.get::<_, i64>(16)? as u64,
        budget_tokens_used: row.get::<_, i64>(16)? as u64,
        estimated_cost_lower: format_microusd(row.get(17)?),
        estimated_cost_upper: format_microusd(row.get(18)?),
        heartbeat_at: row.get::<_, Option<i64>>(19)?.map(format_timestamp),
        thread_id: row.get(20)?,
        active_turn_id: row.get(21)?,
        active_turn_started_at: row.get::<_, Option<i64>>(26)?.map(format_timestamp),
        active_turn_usage,
        context_strategy: row.get(22)?,
        context_source_attempt_id: row.get::<_, Option<String>>(23)?.map(AttemptId::from),
        context_reuse_reason: row.get(24)?,
        version: row.get::<_, i64>(25)? as u64,
    })
}

fn map_worktree(row: &Row<'_>) -> rusqlite::Result<WorktreeSummary> {
    Ok(WorktreeSummary {
        id: harness_domain::WorktreeId::from(row.get::<_, String>(0)?),
        run_id: RunId::from(row.get::<_, String>(1)?),
        task_id: row.get::<_, Option<String>>(2)?.map(TaskId::from),
        kind: row.get(3)?,
        path: row.get(4)?,
        branch: row.get(5)?,
        base_sha: row.get(6)?,
        head_sha: row.get(7)?,
        state: row.get(8)?,
        preserved_reason: row.get(9)?,
        dirty: false,
        files_changed: row.get::<_, i64>(10)? as u32,
        additions: row.get::<_, i64>(11)? as u64,
        deletions: row.get::<_, i64>(12)? as u64,
        version: row.get::<_, i64>(13)? as u64,
    })
}

fn map_task(row: &Row<'_>) -> rusqlite::Result<TaskSummary> {
    let state: String = row.get(5)?;
    let dependencies: Option<String> = row.get(14)?;
    Ok(TaskSummary {
        id: TaskId::from(row.get::<_, String>(0)?),
        run_id: RunId::from(row.get::<_, String>(1)?),
        external_task_id: row.get(2)?,
        title: row.get(3)?,
        objective: row.get(4)?,
        state: state
            .parse()
            .map_err(|error: harness_domain::DomainError| {
                rusqlite::Error::FromSqlConversionFailure(
                    state.len(),
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?,
        priority: row.get(6)?,
        owner_profile: row.get(7)?,
        reviewer_profile: row.get(8)?,
        attempt: row.get::<_, i64>(9)? as u32,
        base_sha: row.get(10)?,
        head_sha: row.get(11)?,
        token_budget: row.get::<_, Option<i64>>(12)?.map(|value| value as u64),
        version: row.get::<_, i64>(13)? as u64,
        dependencies: dependencies
            .as_deref()
            .and_then(|value| serde_json::from_str(value).ok())
            .unwrap_or_default(),
    })
}

fn map_domain_event(row: &Row<'_>) -> rusqlite::Result<DomainEvent> {
    let payload: String = row.get(6)?;
    Ok(DomainEvent {
        id: row.get(0)?,
        run_id: row.get::<_, Option<String>>(1)?.map(RunId::from),
        aggregate_type: row.get(2)?,
        aggregate_id: row.get(3)?,
        event_type: row.get(4)?,
        occurred_at: row.get(5)?,
        payload: serde_json::from_str(&payload).unwrap_or(Value::Null),
    })
}

fn map_approval(row: &Row<'_>) -> rusqlite::Result<ApprovalSummary> {
    let risk: String = row.get(7)?;
    let request: String = row.get(8)?;
    Ok(ApprovalSummary {
        id: ApprovalId::from(row.get::<_, String>(0)?),
        run_id: RunId::from(row.get::<_, String>(1)?),
        agent_id: row.get::<_, Option<String>>(2)?.map(AgentSessionId::from),
        task_id: row.get::<_, Option<String>>(3)?.map(TaskId::from),
        thread_id: row.get(4)?,
        turn_id: row.get(5)?,
        approval_type: row.get(6)?,
        risk_level: parse_enum(&risk)?,
        request: serde_json::from_str(&request).unwrap_or(Value::Null),
        state: row.get(9)?,
        decision: row.get(10)?,
        created_at: format_timestamp(row.get(11)?),
        resolved_at: row.get::<_, Option<i64>>(12)?.map(format_timestamp),
        version: row.get::<_, i64>(13)? as u64,
    })
}

fn enum_text<T: Serialize>(value: &T) -> Result<String, StoreError> {
    serde_json::to_value(value)?
        .as_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| StoreError::Validation("enum did not serialize as text".to_owned()))
}

fn sqlite_version(version: u64) -> Result<i64, StoreError> {
    i64::try_from(version)
        .map_err(|_| StoreError::Validation("resource version exceeds SQLite range".to_owned()))
}

fn parse_enum<T: serde::de::DeserializeOwned>(value: &str) -> rusqlite::Result<T> {
    serde_json::from_value(Value::String(value.to_owned())).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(
            value.len(),
            rusqlite::types::Type::Text,
            Box::new(error),
        )
    })
}

fn parse_improvement_enum<T: serde::de::DeserializeOwned>(value: String) -> rusqlite::Result<T> {
    serde_json::from_value(Value::String(value)).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })
}

fn parse_improvement_schema(value: &str) -> rusqlite::Result<ImprovementSchema> {
    let schema = match value {
        "harness.trace.v1" => ImprovementSchema::TraceV1,
        "harness.trace.v2" => ImprovementSchema::TraceV2,
        "harness.outcome.v1" => ImprovementSchema::OutcomeV1,
        "harness.eval-case.v1" => ImprovementSchema::EvalCaseV1,
        "harness.grader-bundle.v1" => ImprovementSchema::GraderBundleV1,
        "harness.improvement-candidate.v1" => ImprovementSchema::ImprovementCandidateV1,
        "harness.experiment.v1" => ImprovementSchema::ExperimentV1,
        "harness.knowledge-item.v1" => ImprovementSchema::KnowledgeItemV1,
        "harness.promotion-decision.v1" => ImprovementSchema::PromotionDecisionV1,
        _ => {
            return Err(rusqlite::Error::FromSqlConversionFailure(
                4,
                rusqlite::types::Type::Text,
                "unknown improvement schema".into(),
            ));
        }
    };
    Ok(schema)
}

fn validate_improvement_input(input: &NewImprovementRevision) -> Result<(), StoreError> {
    if input.id.is_empty() || input.aggregate_id.is_empty() || input.idempotency_key.is_empty() {
        return Err(StoreError::Validation(
            "improvement IDs and idempotency key must be nonempty".to_owned(),
        ));
    }
    if input.schema.kind() != input.aggregate_kind || !input.state.allowed_for(input.aggregate_kind)
    {
        return Err(StoreError::Validation(
            "improvement schema, kind, or state is not allowed".to_owned(),
        ));
    }
    if input.sensitivity == SensitivityClass::Restricted && input.export_allowed {
        return Err(StoreError::Validation(
            "restricted improvement records cannot be exportable".to_owned(),
        ));
    }
    if input.payload.get("schema").and_then(Value::as_str) != Some(input.schema.as_str()) {
        return Err(StoreError::Validation(
            "improvement payload schema discriminator mismatch".to_owned(),
        ));
    }
    let serialized = serde_json::to_string(&input.payload)?;
    if input.payload_sha256.len() != 64
        || !input
            .payload_sha256
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
        || sha256(serialized.as_bytes()) != input.payload_sha256
    {
        return Err(StoreError::Validation(
            "improvement payload SHA-256 is invalid or mismatched".to_owned(),
        ));
    }
    Ok(())
}

fn sensitivity_rank(value: SensitivityClass) -> u8 {
    match value {
        SensitivityClass::Public => 0,
        SensitivityClass::Internal => 1,
        SensitivityClass::Confidential => 2,
        SensitivityClass::Restricted => 3,
    }
}

fn retention_rank(value: RetentionClass) -> u8 {
    match value {
        RetentionClass::Ephemeral => 0,
        RetentionClass::Operational => 1,
        RetentionClass::Evaluation => 2,
        RetentionClass::Governance => 3,
        RetentionClass::LegalHold => 4,
    }
}

fn improvement_transition_allowed(
    kind: ImprovementRecordKind,
    from: ImprovementState,
    to: ImprovementState,
) -> bool {
    if !from.allowed_for(kind) || !to.allowed_for(kind) {
        return false;
    }
    if matches!(
        kind,
        ImprovementRecordKind::Taskset
            | ImprovementRecordKind::EvalCase
            | ImprovementRecordKind::GraderBundle
            | ImprovementRecordKind::PolicyBundle
    ) && from == ImprovementState::Proposed
        && to == ImprovementState::Active
    {
        return true;
    }
    if kind == ImprovementRecordKind::Experiment
        && from == ImprovementState::Validated
        && to == ImprovementState::Running
    {
        return true;
    }
    if kind == ImprovementRecordKind::Rollback
        && from == ImprovementState::Requested
        && to == ImprovementState::Completed
    {
        return true;
    }
    from == to
        || matches!(
            (from, to),
            (
                ImprovementState::Proposed,
                ImprovementState::Validated
                    | ImprovementState::Rejected
                    | ImprovementState::ExperimentReady
                    | ImprovementState::Superseded
            ) | (
                ImprovementState::Validated,
                ImprovementState::ExperimentReady
                    | ImprovementState::Rejected
                    | ImprovementState::Superseded
            ) | (
                ImprovementState::ExperimentReady,
                ImprovementState::Running | ImprovementState::Superseded
            ) | (
                ImprovementState::Running,
                ImprovementState::Passed
                    | ImprovementState::Failed
                    | ImprovementState::Inconclusive
                    | ImprovementState::Promoted
                    | ImprovementState::RolledBack
            ) | (
                ImprovementState::Candidate,
                ImprovementState::Active
                    | ImprovementState::Rejected
                    | ImprovementState::Expired
                    | ImprovementState::Contradicted
                    | ImprovementState::Superseded
            ) | (
                ImprovementState::Active,
                ImprovementState::Quarantined
                    | ImprovementState::Retired
                    | ImprovementState::Revoked
                    | ImprovementState::Expired
                    | ImprovementState::Contradicted
                    | ImprovementState::Superseded
            ) | (ImprovementState::Observed, ImprovementState::Superseded)
        )
}

fn map_improvement_revision(row: &Row<'_>) -> rusqlite::Result<ImprovementRevisionRecord> {
    let record = ImprovementRevisionRecord {
        id: row.get(0)?,
        aggregate_kind: parse_improvement_enum(row.get(1)?)?,
        aggregate_id: row.get(2)?,
        revision: positive_database_u64(row.get::<_, i64>(3)?, "improvement revision")?,
        schema: parse_improvement_schema(&row.get::<_, String>(4)?)?,
        state: parse_improvement_enum(row.get(5)?)?,
        payload: serde_json::from_str(&row.get::<_, String>(6)?).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                6,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?,
        payload_sha256: row.get(7)?,
        sensitivity: parse_improvement_enum(row.get(8)?)?,
        retention_class: parse_improvement_enum(row.get(9)?)?,
        export_allowed: row.get(10)?,
        source_domain_event_id: row.get(11)?,
        created_at: row.get(12)?,
    };
    if record.schema.kind() != record.aggregate_kind
        || !record.state.allowed_for(record.aggregate_kind)
        || (record.sensitivity == SensitivityClass::Restricted && record.export_allowed)
        || record.payload.get("schema").and_then(Value::as_str) != Some(record.schema.as_str())
        || record.payload_sha256.len() != 64
        || sha256(
            serde_json::to_string(&record.payload)
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?
                .as_bytes(),
        ) != record.payload_sha256
    {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            "invalid stored improvement revision".into(),
        ));
    }
    if record.schema == ImprovementSchema::OutcomeV1 {
        let outcome: OutcomeWireV1 =
            serde_json::from_value(record.payload.clone()).map_err(|error| {
                rusqlite::Error::FromSqlConversionFailure(
                    6,
                    rusqlite::types::Type::Text,
                    Box::new(error),
                )
            })?;
        outcome.validate().map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                6,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?;
    }
    Ok(record)
}

fn map_improvement_event(row: &Row<'_>) -> rusqlite::Result<ImprovementEventRecord> {
    Ok(ImprovementEventRecord {
        id: harness_domain::ImprovementEventId::from(row.get::<_, String>(0)?),
        aggregate_kind: parse_improvement_enum(row.get(1)?)?,
        aggregate_id: row.get(2)?,
        revision_id: row.get(3)?,
        sequence: positive_database_u64(row.get::<_, i64>(4)?, "improvement event sequence")?,
        event_type: row.get(5)?,
        payload_sha256: row.get(6)?,
        idempotency_key: row.get(7)?,
        source_raw_event_id: row.get(8)?,
        occurred_at: row.get(9)?,
    })
}

fn positive_database_u64(value: i64, field: &str) -> rusqlite::Result<u64> {
    u64::try_from(value)
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Integer,
                format!("invalid {field}").into(),
            )
        })
}

// SI-007 Store-only helpers. Identifiers are intentionally opaque, bounded
// grammar tokens so traces, edits, and their hashes cannot become text sinks.
fn failure_scope_text(scope: FailureScope) -> &'static str {
    match scope {
        FailureScope::AttemptTerminal => "attempt_terminal",
        FailureScope::RunTerminal => "run_terminal",
        FailureScope::TypedOutcome => "typed_outcome",
    }
}

fn terminal_code_text(code: TerminalCode) -> &'static str {
    match code {
        TerminalCode::PolicyBlocked => "policy_blocked",
        TerminalCode::BudgetExhausted => "budget_exhausted",
        TerminalCode::InfrastructureUnavailable => "infrastructure_unavailable",
        TerminalCode::ProtocolError => "protocol_error",
        TerminalCode::IntegrationConflict => "integration_conflict",
        TerminalCode::SourceFailure => "source_failure",
        TerminalCode::Inconclusive => "inconclusive",
        TerminalCode::CancelledSuperseded => "cancelled_superseded",
    }
}

fn failure_class_text(class: FailureClass) -> &'static str {
    match class {
        FailureClass::Unknown => "unknown",
        FailureClass::PolicyBlocked => "policy_blocked",
        FailureClass::BudgetExhausted => "budget_exhausted",
        FailureClass::InfrastructureUnavailable => "infrastructure_unavailable",
        FailureClass::ProtocolError => "protocol_error",
        FailureClass::IntegrationConflict => "integration_conflict",
        FailureClass::SourceFailure => "source_failure",
        FailureClass::Inconclusive => "inconclusive",
        FailureClass::CancelledSuperseded => "cancelled_superseded",
    }
}

fn severity_text(severity: Severity) -> &'static str {
    match severity {
        Severity::Unknown => "unknown",
        Severity::Low => "low",
        Severity::Medium => "medium",
        Severity::High => "high",
        Severity::Critical => "critical",
    }
}

fn severity_rank(value: &str) -> u8 {
    match value {
        "critical" => 5,
        "high" => 4,
        "medium" => 3,
        "low" => 2,
        _ => 1,
    }
}

fn edit_reason_text(reason: EditReason) -> &'static str {
    match reason {
        EditReason::OperatorCorrection => "operator_correction",
        EditReason::DuplicateCluster => "duplicate_cluster",
        EditReason::DistinctFailureMode => "distinct_failure_mode",
        EditReason::SourceCorrection => "source_correction",
    }
}

fn validate_failure_identifier(value: &str) -> Result<(), StoreError> {
    if !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':'))
    {
        Ok(())
    } else {
        Err(StoreError::Validation(
            "failure identifier must be an opaque bounded token".to_owned(),
        ))
    }
}

fn validate_failure_actor(value: &str) -> Result<(), StoreError> {
    validate_failure_identifier(value)
}

type FailureCostColumns = (Option<String>, Option<i64>, Option<i64>);

fn failure_cost_columns(cost: FailureWireCost) -> Result<FailureCostColumns, StoreError> {
    match cost {
        FailureWireCost::Unknown => Ok((None, None, None)),
        FailureWireCost::Known {
            scope_id,
            lower_microusd,
            additional_microusd,
        } => {
            validate_failure_identifier(&scope_id)?;
            let lower = i64::try_from(lower_microusd)
                .map_err(|_| StoreError::Validation("cost exceeds SQLite range".to_owned()))?;
            let upper = lower_microusd
                .checked_add(additional_microusd)
                .ok_or_else(|| StoreError::Validation("cost overflow".to_owned()))?;
            Ok((
                Some(scope_id),
                Some(lower),
                Some(
                    i64::try_from(upper).map_err(|_| {
                        StoreError::Validation("cost exceeds SQLite range".to_owned())
                    })?,
                ),
            ))
        }
    }
}

fn failure_run_cost(
    connection: &rusqlite::Connection,
    run_id: &RunId,
) -> Result<CostAttribution, StoreError> {
    let (samples, priced, lower, upper): (i64, i64, i64, i64) = connection.query_row(
        "SELECT count(ts.id),count(c.token_sample_id),coalesce(sum(c.lower_microusd),0),coalesce(sum(c.upper_microusd),0) FROM token_samples ts JOIN codex_threads ct ON ct.thread_id=ts.thread_id JOIN agent_sessions a ON a.id=ct.agent_session_id LEFT JOIN cost_entries c ON c.token_sample_id=ts.id WHERE a.run_id=?1",
        [run_id.as_str()], |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?)),
    )?;
    if samples == 0 || samples != priced || lower < 0 || upper < lower {
        return Ok(CostAttribution::unknown());
    }
    Ok(CostAttribution::known(
        format!("run:{}", run_id.as_str()),
        lower as u64,
        upper as u64,
    ))
}

fn failure_run_cost_columns(
    connection: &rusqlite::Connection,
    run_id: &RunId,
) -> Result<Option<(String, i64, i64)>, StoreError> {
    let wire = FailureWireCost::try_from(&failure_run_cost(connection, run_id)?)
        .map_err(|error| StoreError::Validation(error.to_string()))?;
    let (scope, lower, upper) = failure_cost_columns(wire)?;
    Ok(match (scope, lower, upper) {
        (Some(scope), Some(lower), Some(upper)) => Some((scope, lower, upper)),
        _ => None,
    })
}

fn terminal_run_failure_events(
    transaction: &rusqlite::Transaction<'_>,
    run_id: &RunId,
    failure_class: &str,
) -> Result<Vec<i64>, StoreError> {
    let mut statement = transaction.prepare(
        "SELECT id FROM domain_events WHERE run_id=?1 AND aggregate_type='run' AND aggregate_id=?1 AND event_type='run.lifecycle.transitioned' AND json_extract(payload_json,'$.failure_class')=?2 ORDER BY id",
    )?;
    Ok(statement
        .query_map(params![run_id.as_str(), failure_class], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?)
}

fn failure_cluster_repository(
    transaction: &rusqlite::Transaction<'_>,
    cluster_id: &str,
) -> Result<String, StoreError> {
    transaction
        .query_row(
            "SELECT repository_id FROM failure_clusters WHERE id=?1",
            [cluster_id],
            |row| row.get(0),
        )
        .optional()?
        .ok_or_else(|| StoreError::NotFound(format!("failure cluster {cluster_id}")))
}

fn failure_occurrence_run(
    connection: &rusqlite::Connection,
    occurrence_id: &str,
) -> Result<Option<RunId>, StoreError> {
    let (kind, source, source_domain_event_id): (String, String, Option<i64>) = connection.query_row(
        "SELECT source_kind,source_id,source_domain_event_id FROM failure_occurrences WHERE id=?1",
        [occurrence_id],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?;
    let run_id: Option<String> = match kind.as_str() {
        "run_terminal" => match source_domain_event_id {
            Some(event_id) => connection
                .query_row(
                    "SELECT run_id FROM domain_events WHERE id=?1 AND event_type='run.lifecycle.transitioned'",
                    [event_id],
                    |r| r.get(0),
                )
                .optional()?,
            None => None,
        },
        "attempt_terminal" => connection.query_row("SELECT t.run_id FROM task_attempts a JOIN tasks t ON t.id=a.task_id WHERE a.id=?1", [source], |r| r.get(0)).optional()?,
        "typed_outcome" => connection.query_row("SELECT json_extract(payload_json,'$.run_id') FROM improvement_revisions WHERE aggregate_kind='outcome' AND id=?1", [source], |r| r.get(0)).optional()?,
        _ => None,
    };
    Ok(run_id.map(RunId::from))
}

fn require_failure_cluster_version(
    transaction: &rusqlite::Transaction<'_>,
    cluster_id: &str,
    expected: u64,
) -> Result<(), StoreError> {
    let actual: i64 = transaction
        .query_row(
            "SELECT version FROM failure_clusters WHERE id=?1",
            [cluster_id],
            |row| row.get(0),
        )
        .optional()?
        .ok_or_else(|| StoreError::NotFound(format!("failure cluster {cluster_id}")))?;
    if u64::try_from(actual).ok() == Some(expected) {
        Ok(())
    } else {
        Err(StoreError::Conflict(format!(
            "stale failure cluster {cluster_id}"
        )))
    }
}

fn append_failure_membership_tx(
    transaction: &rusqlite::Transaction<'_>,
    occurrence_id: &str,
    cluster_id: &str,
    action: MembershipAction,
    actor: &str,
    reason: EditReason,
) -> Result<(), StoreError> {
    transaction.execute("INSERT INTO failure_cluster_membership_revisions(occurrence_id,cluster_id,action,actor,reason_code,created_at) VALUES(?1,?2,?3,?4,?5,?6)", params![occurrence_id,cluster_id,membership_action_text(action),actor,edit_reason_text(reason),now_ms()])?;
    Ok(())
}

fn membership_action_text(action: MembershipAction) -> &'static str {
    match action {
        MembershipAction::Assigned => "assigned",
        MembershipAction::Merged => "merged",
        MembershipAction::Split => "split",
    }
}

fn bump_failure_cluster(
    transaction: &rusqlite::Transaction<'_>,
    cluster_id: &str,
) -> Result<(), StoreError> {
    let changed = transaction.execute(
        "UPDATE failure_clusters SET version=version+1 WHERE id=?1",
        [cluster_id],
    )?;
    if changed == 1 {
        Ok(())
    } else {
        Err(StoreError::NotFound(format!(
            "failure cluster {cluster_id}"
        )))
    }
}

fn current_failure_members(
    transaction: &rusqlite::Transaction<'_>,
    cluster_id: &str,
) -> Result<BTreeSet<String>, StoreError> {
    let mut statement = transaction.prepare("SELECT occurrence_id FROM failure_cluster_membership_revisions WHERE cluster_id=?1 AND revision=(SELECT max(m2.revision) FROM failure_cluster_membership_revisions m2 WHERE m2.occurrence_id=failure_cluster_membership_revisions.occurrence_id) ORDER BY occurrence_id")?;
    Ok(statement
        .query_map([cluster_id], |row| row.get(0))?
        .collect::<Result<BTreeSet<_>, _>>()?)
}

fn append_failure_cluster_edit(
    transaction: &rusqlite::Transaction<'_>,
    source_cluster_id: &str,
    target_cluster_id: Option<&str>,
    action: &str,
    actor: &str,
    reason: EditReason,
    target_cluster_ids: &[&str],
) -> Result<(), StoreError> {
    let mut targets = target_cluster_ids
        .iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>();
    targets.sort();
    targets.dedup();
    if targets.is_empty()
        || targets
            .iter()
            .any(|id| validate_failure_identifier(id).is_err())
    {
        return Err(StoreError::Validation(
            "invalid failure cluster edit target".to_owned(),
        ));
    }
    let targets_json = serde_json::to_string(&targets)?;
    let digest = sha256(targets_json.as_bytes());
    let identity = sha256(format!("failure.cluster.edit.v1\0{source_cluster_id}\0{}\0{action}\0{actor}\0{}\0{targets_json}", target_cluster_id.unwrap_or(""), edit_reason_text(reason)).as_bytes());
    transaction.execute("INSERT INTO failure_cluster_edits(id,source_cluster_id,target_cluster_id,action,actor,reason_code,target_cluster_ids_json,target_cluster_ids_sha256,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9)", params![format!("failure-edit-{identity}"),source_cluster_id,target_cluster_id,action,actor,edit_reason_text(reason),targets_json,digest,now_ms()])?;
    Ok(())
}

fn safe_outcome_identifier(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

fn authoritative_result_label(
    result: &str,
) -> (harness_domain::OutcomeClassification, &'static str) {
    match result {
        "success" => (harness_domain::OutcomeClassification::Positive, "passed"),
        "inconclusive" | "not_selected" | "skipped_draft" => (
            harness_domain::OutcomeClassification::Unknown,
            "unavailable",
        ),
        "source_failure" | "infrastructure_unavailable" | "quarantined_failure" => {
            (harness_domain::OutcomeClassification::Negative, "failed")
        }
        _ => (
            harness_domain::OutcomeClassification::Unknown,
            "unavailable",
        ),
    }
}

fn authoritative_evidence_label(
    result: &str,
) -> (harness_domain::OutcomeClassification, &'static str) {
    match result {
        "success" => (harness_domain::OutcomeClassification::Positive, "proved"),
        "inconclusive" | "not_selected" | "skipped_draft" => (
            harness_domain::OutcomeClassification::Unknown,
            "unavailable",
        ),
        "source_failure" | "infrastructure_unavailable" | "quarantined_failure" => {
            (harness_domain::OutcomeClassification::Negative, "unproved")
        }
        _ => (
            harness_domain::OutcomeClassification::Unknown,
            "unavailable",
        ),
    }
}

fn outcome_subject(
    run_id: &RunId,
    task_attempt_id: Option<String>,
) -> harness_domain::OutcomeSubject {
    match task_attempt_id {
        Some(id) => harness_domain::OutcomeSubject {
            kind: harness_domain::OutcomeSubjectKind::TaskAttempt,
            id,
        },
        None => harness_domain::OutcomeSubject {
            kind: harness_domain::OutcomeSubjectKind::Run,
            id: run_id.to_string(),
        },
    }
}

fn stable_authoritative_outcome_id(
    input: &AuthoritativeOutcomeInput,
) -> Result<OutcomeId, StoreError> {
    let subject_kind = serde_json::to_string(&input.subject.kind)?;
    let dimension = serde_json::to_string(&input.dimension)?;
    Ok(OutcomeId::from(sha256(
        format!(
            "harness.outcome.id.v1\0{}\0{}\0{}\0{}",
            input.run_id, subject_kind, input.subject.id, dimension
        )
        .as_bytes(),
    )))
}

fn validate_operator_outcome_input(input: &NewOperatorOutcome) -> Result<(), StoreError> {
    if !safe_outcome_identifier(input.run_id.as_str(), 128)
        || !safe_outcome_identifier(&input.subject.id, 128)
        || !safe_outcome_identifier(&input.code, 80)
        || !safe_outcome_identifier(&input.actor, 128)
        || !safe_outcome_identifier(&input.idempotency_key, 200)
        || input
            .reason_code
            .as_ref()
            .is_some_and(|value| !harness_domain::is_safe_outcome_reason_code(value))
        || input
            .correction_artifact_id
            .as_ref()
            .is_some_and(|value| !safe_outcome_identifier(value.as_str(), 128))
        || input
            .note
            .as_ref()
            .is_some_and(|value| value.chars().count() > 1_000)
        || input
            .supersedes
            .iter()
            .any(|value| !safe_outcome_identifier(value, 128))
    {
        return Err(StoreError::Validation(
            "invalid bounded operator outcome input".to_owned(),
        ));
    }
    let mut unique = BTreeSet::new();
    if input.supersedes.iter().any(|value| !unique.insert(value)) {
        return Err(StoreError::Validation(
            "duplicate outcome supersedes target".to_owned(),
        ));
    }
    Ok(())
}

fn stable_outcome_id(input: &NewOperatorOutcome) -> Result<OutcomeId, StoreError> {
    let subject_kind = serde_json::to_string(&input.subject.kind)?;
    let dimension = serde_json::to_string(&input.dimension)?;
    Ok(OutcomeId::from(sha256(
        format!(
            "harness.outcome.id.v1\0{}\0{}\0{}\0{}",
            input.run_id, subject_kind, input.subject.id, dimension
        )
        .as_bytes(),
    )))
}

fn outcome_matches_input(
    stored: &OutcomeWireV1,
    input: &NewOperatorOutcome,
    outcome_id: &OutcomeId,
) -> bool {
    stored.schema == "harness.outcome.v1"
        && stored.outcome_id == *outcome_id
        && stored.run_id == input.run_id
        && stored.subject == input.subject
        && stored.dimension == input.dimension
        && stored.classification == input.classification
        && stored.code == input.code
        && stored.confidence == OutcomeConfidence::OperatorAsserted
        && stored.source.kind == OutcomeSourceKind::HumanAction
        && stored.supersedes == input.supersedes
        && stored.reason_code == input.reason_code
        && stored.correction_artifact_id == input.correction_artifact_id
        && stored.redactor_version == "outcome-redactor.v1"
        && stored.free_text_redacted == input.note.is_some()
}

fn validate_outcome_supersedes(
    transaction: &rusqlite::Transaction<'_>,
    outcome_id: &OutcomeId,
    supersedes: &[String],
) -> Result<(), StoreError> {
    if supersedes.is_empty() {
        return Ok(());
    }
    let mut heads = BTreeSet::new();
    let mut all = BTreeSet::new();
    let mut statement = transaction.prepare(
        "SELECT id,payload_json FROM improvement_revisions WHERE aggregate_kind='outcome' AND aggregate_id=?1",
    )?;
    let mut superseded = BTreeSet::new();
    for row in statement.query_map([outcome_id.as_str()], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?))
    })? {
        let (id, payload) = row?;
        let outcome: OutcomeWireV1 = serde_json::from_str(&payload)?;
        all.insert(id.clone());
        heads.insert(id);
        superseded.extend(outcome.supersedes);
    }
    for target in superseded {
        heads.remove(&target);
    }
    for target in supersedes {
        if !all.contains(target) {
            return Err(StoreError::Conflict(
                "outcome supersedes target is cross-outcome or unknown".to_owned(),
            ));
        }
        if !heads.contains(target) {
            return Err(StoreError::Conflict(
                "outcome supersedes target is not a current head".to_owned(),
            ));
        }
    }
    Ok(())
}

fn outcome_vector_from_rows(
    run_id: &RunId,
    rows: Vec<(String, i64, String)>,
) -> Result<OutcomeVector, StoreError> {
    let mut groups: BTreeMap<String, Vec<OutcomeRevisionView>> = BTreeMap::new();
    for (id, revision, raw) in rows {
        let outcome: OutcomeWireV1 = serde_json::from_str(&raw)?;
        groups
            .entry(outcome.outcome_id.to_string())
            .or_default()
            .push(OutcomeRevisionView {
                revision_id: id,
                revision: positive_database_u64(revision, "outcome revision")?,
                outcome,
                is_head: false,
            });
    }
    let items = groups
        .into_values()
        .map(|mut revisions| {
            let superseded: BTreeSet<String> = revisions
                .iter()
                .flat_map(|row| row.outcome.supersedes.iter().cloned())
                .collect();
            for row in &mut revisions {
                row.is_head = !superseded.contains(&row.revision_id);
            }
            let first = &revisions[0].outcome;
            let conflicted = revisions.iter().filter(|row| row.is_head).count() > 1;
            OutcomeVectorItem {
                outcome_id: first.outcome_id.clone(),
                subject: first.subject.clone(),
                dimension: first.dimension,
                revisions,
                conflicted,
            }
        })
        .collect();
    Ok(OutcomeVector {
        run_id: run_id.clone(),
        items,
    })
}

fn outcome_vector_conn(
    connection: &rusqlite::Connection,
    run_id: &RunId,
) -> Result<OutcomeVector, StoreError> {
    let mut s=connection.prepare("SELECT id,aggregate_kind,aggregate_id,revision,schema_name,lifecycle_state,payload_json,payload_sha256,sensitivity,retention_class,export_allowed,source_domain_event_id,created_at FROM improvement_revisions WHERE aggregate_kind='outcome' AND json_extract(payload_json,'$.run_id')=?1 ORDER BY aggregate_id,revision")?;
    let rows = s.query_map([run_id.as_str()], map_improvement_revision)?;
    let rows = rows
        .map(|row| {
            row.and_then(|record| {
                let revision = i64::try_from(record.revision).map_err(|_| {
                    rusqlite::Error::FromSqlConversionFailure(
                        3,
                        rusqlite::types::Type::Integer,
                        "outcome revision exceeds database range".into(),
                    )
                })?;
                let payload = serde_json::to_string(&record.payload)
                    .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
                Ok((record.id, revision, payload))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    outcome_vector_from_rows(run_id, rows)
}
fn outcome_vector_tx(
    transaction: &rusqlite::Transaction<'_>,
    run_id: &RunId,
) -> Result<OutcomeVector, StoreError> {
    let c = transaction;
    let mut s=c.prepare("SELECT id,aggregate_kind,aggregate_id,revision,schema_name,lifecycle_state,payload_json,payload_sha256,sensitivity,retention_class,export_allowed,source_domain_event_id,created_at FROM improvement_revisions WHERE aggregate_kind='outcome' AND json_extract(payload_json,'$.run_id')=?1 ORDER BY aggregate_id,revision")?;
    let rows = s.query_map([run_id.as_str()], map_improvement_revision)?;
    let rows = rows
        .map(|row| {
            row.and_then(|record| {
                let revision = i64::try_from(record.revision).map_err(|_| {
                    rusqlite::Error::FromSqlConversionFailure(
                        3,
                        rusqlite::types::Type::Integer,
                        "outcome revision exceeds database range".into(),
                    )
                })?;
                let payload = serde_json::to_string(&record.payload)
                    .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
                Ok((record.id, revision, payload))
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    outcome_vector_from_rows(run_id, rows)
}

fn read_improvement_revision(
    transaction: &rusqlite::Transaction<'_>,
    id: &str,
) -> Result<ImprovementRevisionRecord, StoreError> {
    transaction.query_row("SELECT id,aggregate_kind,aggregate_id,revision,schema_name,lifecycle_state,payload_json,payload_sha256,sensitivity,retention_class,export_allowed,source_domain_event_id,created_at FROM improvement_revisions WHERE id=?1", [id], map_improvement_revision).map_err(Into::into)
}

fn read_improvement_event(
    transaction: &rusqlite::Transaction<'_>,
    id: &str,
) -> Result<ImprovementEventRecord, StoreError> {
    transaction.query_row("SELECT id,aggregate_kind,aggregate_id,revision_id,sequence,event_type,payload_sha256,idempotency_key,source_raw_event_id,occurred_at FROM improvement_events WHERE id=?1", [id], map_improvement_event).map_err(Into::into)
}

pub(crate) fn sha256(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

fn normalized_prefix(path: &str) -> String {
    path.trim_start_matches("./")
        .split(['*', '?', '['])
        .next()
        .unwrap_or(path)
        .trim_end_matches('/')
        .to_owned()
}

fn format_microusd(value: i64) -> String {
    let value = value.max(0) as u64;
    format!("${}.{:02}", value / 1_000_000, (value % 1_000_000) / 10_000)
}

pub fn packet_digest<T: Serialize>(value: &T) -> Result<String, StoreError> {
    Ok(sha256(serde_json::to_string(value)?.as_bytes()))
}

pub fn operation_payload(kind: &str, target: &str) -> Value {
    json!({"kind": kind, "target": target})
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use tempfile::TempDir;

    use super::*;

    fn store_with_created_run() -> (Store, RunId) {
        let temp = TempDir::new().unwrap();
        let artifacts = temp.path().join("artifacts");
        let store = Store::in_memory(&artifacts).unwrap();
        let repository_id = RepositoryId::from("repository-lifecycle-event");
        store
            .create_repository(&NewRepository {
                id: repository_id.clone(),
                profile_id: "fixture".to_owned(),
                profile_version: 1,
                display_name: "Lifecycle event fixture".to_owned(),
                root_path: PathBuf::from("/tmp/lifecycle-event-fixture"),
                origin_url: None,
                default_branch: "main".to_owned(),
                expected_coordination_branch: None,
                state: "READY".to_owned(),
            })
            .unwrap();
        let run_id = RunId::from("run-lifecycle-event");
        store
            .create_run(&NewRun {
                id: run_id.clone(),
                repository_id,
                title: "Lifecycle event fixture".to_owned(),
                objective: "Verify lifecycle transition persistence".to_owned(),
                mode: "standard".to_owned(),
                publication_mode: "none".to_owned(),
                state: RunState::Created.to_string(),
                phase: "created".to_owned(),
                base_ref: "main".to_owned(),
                base_sha: "fixture".to_owned(),
                authority_digest: "fixture".to_owned(),
                profile_digest: "fixture".to_owned(),
                codex_version: None,
                protocol_schema_sha256: None,
                requested_by: "test".to_owned(),
                token_budget: None,
            })
            .unwrap();
        (store, run_id)
    }

    fn operator_outcome(run_id: &RunId, key: &str, code: &str) -> NewOperatorOutcome {
        NewOperatorOutcome {
            run_id: run_id.clone(),
            subject: harness_domain::OutcomeSubject {
                kind: harness_domain::OutcomeSubjectKind::Run,
                id: run_id.to_string(),
            },
            dimension: harness_domain::OutcomeDimension::OperatorAcceptance,
            classification: harness_domain::OutcomeClassification::Positive,
            code: code.to_owned(),
            reason_code: None,
            note: Some("operator-only free text".to_owned()),
            correction_artifact_id: None,
            supersedes: Vec::new(),
            actor: "local-user".to_owned(),
            idempotency_key: key.to_owned(),
        }
    }

    #[test]
    fn failure_clusters_enforce_repository_custody_and_hide_source_ids() {
        let (store, run_id) = store_with_created_run();
        let repository_id = RepositoryId::from("repository-lifecycle-event");
        let other_repository = RepositoryId::from("repository-other");
        store
            .create_repository(&NewRepository {
                id: other_repository.clone(),
                profile_id: "fixture".into(),
                profile_version: 1,
                display_name: "other".into(),
                root_path: PathBuf::from("/tmp/other"),
                origin_url: None,
                default_branch: "main".into(),
                expected_coordination_branch: None,
                state: "READY".into(),
            })
            .unwrap();
        store.connection().unwrap().execute(
            "INSERT INTO failure_occurrences(id,repository_id,source_kind,source_id,terminal_code,automatic_class,severity,taxonomy_version,fingerprint_sha256,created_at) VALUES('failure-one',?1,'run_terminal','run-lifecycle-event','budget_exhausted','budget_exhausted','unknown','harness.failure-taxonomy.v1',?2,1)",
            params![repository_id.as_str(), "a".repeat(64)],
        ).unwrap();
        store
            .create_failure_cluster(&repository_id, "cluster-a")
            .unwrap();
        store
            .create_failure_cluster(&other_repository, "cluster-b")
            .unwrap();
        assert!(
            store
                .assign_failure_to_cluster(
                    "failure-one",
                    "cluster-b",
                    0,
                    "operator",
                    EditReason::OperatorCorrection
                )
                .is_err()
        );
        store
            .assign_failure_to_cluster(
                "failure-one",
                "cluster-a",
                0,
                "operator",
                EditReason::OperatorCorrection,
            )
            .unwrap();
        let trace = store.failure_trace_summary("failure-one").unwrap();
        assert_ne!(trace.source_receipt_sha256, run_id.as_str());
        assert_eq!(trace.source_receipt_sha256.len(), 64);
        assert_eq!(
            store.failure_cluster_overview(&repository_id).unwrap()[0].occurrences,
            1
        );
    }

    #[test]
    fn known_priced_run_scope_is_accepted_by_failure_projection_contract() {
        let columns = failure_cost_columns(FailureWireCost::Known {
            scope_id: "run:priced-run".to_owned(),
            lower_microusd: 10,
            additional_microusd: 5,
        })
        .unwrap();
        assert_eq!(
            columns,
            (Some("run:priced-run".to_owned()), Some(10), Some(15))
        );
    }

    #[test]
    fn failure_projection_is_replay_stable_and_derives_late_cost_read_only() {
        let (store, run_id) = store_with_created_run();
        let lifecycle_event_id = {
            let connection = store.connection().unwrap();
            connection
                .execute(
                    "UPDATE runs SET failure_class='budget_exhausted' WHERE id=?1",
                    [run_id.as_str()],
                )
                .unwrap();
            connection
                .execute(
                    "INSERT INTO domain_events(run_id,aggregate_type,aggregate_id,event_type,occurred_at,payload_json) VALUES(?1,'run',?1,'run.lifecycle.transitioned',1,?2)",
                    params![run_id.as_str(), json!({"failure_class":"budget_exhausted"}).to_string()],
                )
                .unwrap();
            connection.last_insert_rowid()
        };

        assert_eq!(
            store.project_failures_for_run(&run_id).unwrap(),
            FailureProjectionReceipt {
                inserted: 1,
                already_projected: 0,
            }
        );
        assert_eq!(
            store.project_failures_for_run(&run_id).unwrap(),
            FailureProjectionReceipt {
                inserted: 0,
                already_projected: 1,
            }
        );
        let repository_id = RepositoryId::from("repository-lifecycle-event");
        let initial = store.failure_cluster_overview(&repository_id).unwrap();
        assert_eq!(initial.len(), 1);
        assert_eq!(initial[0].representative_run_id, Some(run_id.clone()));
        assert_eq!(initial[0].unknown_cost_occurrences, 1);
        assert_eq!(initial[0].cost_upper_microusd, 0);

        {
            let connection = store.connection().unwrap();
            connection
                .execute_batch(
                    "INSERT INTO agent_sessions(id,run_id,runtime_kind,role,requested_model,requested_reasoning_effort,sandbox_mode,approval_policy,cwd,state) VALUES('failure-agent','run-lifecycle-event','test','worker','model','low','read_only','never','/tmp','COMPLETED');
                     INSERT INTO codex_threads(thread_id,agent_session_id,created_at,updated_at) VALUES('failure-thread','failure-agent',1,1);
                     INSERT INTO token_samples(id,thread_id,effective_model,observed_at,input_tokens,cached_input_tokens,output_tokens,reasoning_output_tokens,total_tokens,sample_kind) VALUES('failure-sample','failure-thread','model',1,10,0,5,0,15,'session_total');
                     INSERT INTO pricing_snapshots(id,model,currency,effective_at,input_microusd_per_million,cached_input_microusd_per_million,output_microusd_per_million,cache_write_multiplier_numerator,cache_write_multiplier_denominator,source_label,created_at) VALUES('failure-price','model','USD',1,1,1,1,1,1,'fixture',1);
                     INSERT INTO cost_entries(id,token_sample_id,pricing_snapshot_id,lower_microusd,upper_microusd,confidence,explanation,created_at) VALUES('failure-cost','failure-sample','failure-price',10,15,'exact','fixture',1);",
                )
                .unwrap();
            connection
                .execute(
                    "INSERT INTO domain_events(run_id,aggregate_type,aggregate_id,event_type,occurred_at,payload_json) VALUES(?1,'run',?1,'run.lifecycle.transitioned',2,?2)",
                    params![run_id.as_str(), json!({"failure_class":"unrelated"}).to_string()],
                )
                .unwrap();
        }

        let replay = store.project_failures_for_run(&run_id).unwrap();
        assert_eq!(replay.inserted, 0);
        assert_eq!(replay.already_projected, 1);
        let priced = store.failure_cluster_overview(&repository_id).unwrap();
        assert_eq!(priced[0].unknown_cost_occurrences, 0);
        assert_eq!(priced[0].cost_lower_microusd, 10);
        assert_eq!(priced[0].cost_upper_microusd, 15);
        let occurrence_id: String = store
            .connection()
            .unwrap()
            .query_row(
                "SELECT id FROM failure_occurrences WHERE source_domain_event_id=?1",
                [lifecycle_event_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            failure_occurrence_run(&store.connection().unwrap(), &occurrence_id).unwrap(),
            Some(run_id.clone())
        );

        let mut negative = operator_outcome(&run_id, "failure-outcome", "changes_requested");
        negative.classification = harness_domain::OutcomeClassification::Negative;
        let receipt = store.record_operator_outcome(&negative).unwrap();
        store.project_failures_for_run(&run_id).unwrap();
        let typed_occurrence: String = store
            .connection()
            .unwrap()
            .query_row(
                "SELECT id FROM failure_occurrences WHERE source_kind='typed_outcome' AND source_id=?1",
                [receipt.revision_id],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            failure_occurrence_run(&store.connection().unwrap(), &typed_occurrence).unwrap(),
            Some(run_id)
        );
    }

    #[test]
    fn failure_overview_keeps_unique_cost_when_another_scope_is_shared() {
        let (store, _) = store_with_created_run();
        let repository_id = RepositoryId::from("repository-lifecycle-event");
        {
            let connection = store.connection().unwrap();
            connection
                .execute_batch(
                    "INSERT INTO failure_occurrences(id,repository_id,source_kind,source_id,automatic_class,severity,taxonomy_version,fingerprint_sha256,cost_scope_id,cost_lower_microusd,cost_upper_microusd,created_at) VALUES
                     ('failure-unique','repository-lifecycle-event','attempt_terminal','attempt-unique','unknown','low','harness.failure-taxonomy.v1','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','scope:unique',10,15,1),
                     ('failure-shared-a','repository-lifecycle-event','attempt_terminal','attempt-shared-a','unknown','medium','harness.failure-taxonomy.v1','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb','scope:shared',20,25,2),
                     ('failure-shared-b','repository-lifecycle-event','attempt_terminal','attempt-shared-b','unknown','high','harness.failure-taxonomy.v1','cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc','scope:shared',20,25,3);",
                )
                .unwrap();
        }
        store
            .create_failure_cluster(&repository_id, "cluster-cost-a")
            .unwrap();
        store
            .create_failure_cluster(&repository_id, "cluster-cost-b")
            .unwrap();
        store
            .assign_failure_to_cluster(
                "failure-unique",
                "cluster-cost-a",
                0,
                "operator",
                EditReason::OperatorCorrection,
            )
            .unwrap();
        store
            .assign_failure_to_cluster(
                "failure-shared-a",
                "cluster-cost-a",
                1,
                "operator",
                EditReason::OperatorCorrection,
            )
            .unwrap();
        store
            .assign_failure_to_cluster(
                "failure-shared-b",
                "cluster-cost-b",
                0,
                "operator",
                EditReason::OperatorCorrection,
            )
            .unwrap();

        let overview = store.failure_cluster_overview(&repository_id).unwrap();
        let cluster_a = overview
            .iter()
            .find(|cluster| cluster.cluster_id == "cluster-cost-a")
            .unwrap();
        assert_eq!(cluster_a.occurrences, 2);
        assert_eq!(cluster_a.cost_lower_microusd, 10);
        assert_eq!(cluster_a.cost_upper_microusd, 15);
        assert_eq!(cluster_a.unknown_cost_occurrences, 1);
        assert_eq!(cluster_a.severity.as_deref(), Some("medium"));
        let cluster_b = overview
            .iter()
            .find(|cluster| cluster.cluster_id == "cluster-cost-b")
            .unwrap();
        assert_eq!(cluster_b.cost_upper_microusd, 0);
        assert_eq!(cluster_b.unknown_cost_occurrences, 1);
        assert_eq!(cluster_b.severity.as_deref(), Some("high"));
    }

    #[test]
    fn operator_outcomes_are_idempotent_sanitized_and_head_aware() {
        let (store, run_id) = store_with_created_run();
        let first = operator_outcome(&run_id, "outcome-one", "accepted_without_correction");
        let first_receipt = store.record_operator_outcome(&first).unwrap();
        let replay = store.record_operator_outcome(&first).unwrap();
        assert_eq!(first_receipt.revision_id, replay.revision_id);
        let action_count: i64 = store
            .connection()
            .unwrap()
            .query_row("SELECT count(*) FROM human_actions", [], |r| r.get(0))
            .unwrap();
        assert_eq!(action_count, 1);
        let persisted: String = store
            .connection()
            .unwrap()
            .query_row("SELECT payload_json FROM human_actions", [], |r| r.get(0))
            .unwrap();
        assert!(!persisted.contains("operator-only free text"));
        let mut changed = first.clone();
        changed.code = "accepted_after_correction".to_owned();
        assert!(matches!(
            store.record_operator_outcome(&changed),
            Err(StoreError::Conflict(_))
        ));

        let second = operator_outcome(&run_id, "outcome-two", "accepted_after_correction");
        let second_receipt = store.record_operator_outcome(&second).unwrap();
        assert!(second_receipt.vector.items[0].conflicted);
        let mut resolution =
            operator_outcome(&run_id, "outcome-three", "accepted_after_correction");
        resolution.supersedes = vec![
            first_receipt.revision_id.clone(),
            second_receipt.revision_id.clone(),
        ];
        let resolved = store.record_operator_outcome(&resolution).unwrap();
        assert!(!resolved.vector.items[0].conflicted);
        assert_eq!(
            resolved.vector.items[0]
                .revisions
                .iter()
                .filter(|r| r.is_head)
                .count(),
            1
        );
        let mut stale = operator_outcome(&run_id, "outcome-four", "accepted_after_correction");
        stale.supersedes = vec![first_receipt.revision_id];
        assert!(matches!(
            store.record_operator_outcome(&stale),
            Err(StoreError::Conflict(_))
        ));

        let mut invalid_reason = operator_outcome(
            &run_id,
            "outcome-invalid-reason",
            "accepted_without_correction",
        );
        invalid_reason.reason_code = Some("Reason-Code".to_owned());
        assert!(matches!(
            store.record_operator_outcome(&invalid_reason),
            Err(StoreError::Validation(_))
        ));
    }

    #[test]
    fn authoritative_outcomes_are_typed_idempotent_and_never_human_actions() {
        let (store, run_id) = store_with_created_run();
        {
            let connection = store.connection().unwrap();
            connection
                .pragma_update(None, "foreign_keys", false)
                .unwrap();
            connection.execute_batch(
                "INSERT INTO validations(id,run_id,worktree_id,validator_id,proof_tier,source_sha,selector_reason,state,result_class,started_at,completed_at) VALUES('validation-1','run-lifecycle-event','worktree-1','validator','T1','aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa','fixture','completed','success',1,2);
                 INSERT INTO validations(id,run_id,worktree_id,validator_id,proof_tier,source_sha,selector_reason,state,result_class,started_at,completed_at) VALUES('validation-ci','run-lifecycle-event','worktree-1','draft-pr-required-ci','T1','dddddddddddddddddddddddddddddddddddddddd','fixture','completed','success',2,3);
                 INSERT INTO validations(id,run_id,worktree_id,validator_id,proof_tier,source_sha,selector_reason,state,result_class) VALUES('validation-pending','run-lifecycle-event','worktree-1','validator','T1','eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee','fixture','running',NULL);
                 INSERT INTO validations(id,run_id,worktree_id,validator_id,proof_tier,source_sha,selector_reason,state,result_class,completed_at,invalidated_at) VALUES('validation-invalid','run-lifecycle-event','worktree-1','validator','T1','ffffffffffffffffffffffffffffffffffffffff','fixture','completed','success',4,5);
                 INSERT INTO evidence_records(id,run_id,claim_id,checklist_rows_json,source_sha,proof_tier,result_class,evidence_json,evidence_sha256,unproved_claims_json,created_at) VALUES('evidence-1','run-lifecycle-event','claim','[]','bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb','T1','source_failure','{}','cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc','[]',3);
                 INSERT INTO evidence_records(id,run_id,claim_id,checklist_rows_json,source_sha,proof_tier,result_class,evidence_json,evidence_sha256,unproved_claims_json,created_at,invalidated_at) VALUES('evidence-invalid','run-lifecycle-event','claim','[]','eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee','T1','success','{}','ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff','[]',4,5);
                 INSERT INTO findings(id,run_id,verifier_agent_session_id,severity,category,invariant,description,required_correction,state,created_at) VALUES('finding-1','run-lifecycle-event','verifier-1','high','fixture','fixture','fixture','fixture','open',4);
                 UPDATE runs SET run_token_budget=1 WHERE id='run-lifecycle-event';
                 INSERT INTO domain_events(run_id,aggregate_type,aggregate_id,event_type,occurred_at,payload_json) VALUES('run-lifecycle-event','run','run-lifecycle-event','run.lifecycle.transitioned',5,'{\"next_state\":\"COMPLETED\"}');
                 INSERT INTO domain_events(run_id,aggregate_type,aggregate_id,event_type,occurred_at,payload_json) VALUES('run-lifecycle-event','task','task-1','task.verified',6,'{}');"
            ).unwrap();
            connection
                .pragma_update(None, "foreign_keys", true)
                .unwrap();
        }
        store.project_authoritative_outcomes(&run_id).unwrap();
        store.project_authoritative_outcomes(&run_id).unwrap();
        let vector = store.outcome_vector(&run_id).unwrap();
        assert_eq!(vector.items.len(), 6);
        assert!(vector.items.iter().any(|item| item.dimension
            == harness_domain::OutcomeDimension::Validation
            && item.revisions[0].outcome.code == "passed"));
        assert!(vector.items.iter().any(|item| item.dimension
            == harness_domain::OutcomeDimension::CiRequiredChecks
            && item.revisions[0].outcome.code == "passed"));
        assert!(vector.items.iter().any(|item| item.dimension
            == harness_domain::OutcomeDimension::Evidence
            && item.revisions[0].outcome.code == "unproved"));
        let verifier = vector
            .items
            .iter()
            .find(|item| item.dimension == harness_domain::OutcomeDimension::VerifierFindings)
            .unwrap();
        assert!(
            verifier
                .revisions
                .iter()
                .any(|revision| revision.outcome.code == "blocking")
        );
        assert!(verifier.revisions.iter().any(|revision| {
            revision.outcome.code == "none"
                && revision.outcome.classification
                    == harness_domain::OutcomeClassification::Positive
                && revision.outcome.source.kind == OutcomeSourceKind::DomainEvent
        }));
        let completion = vector
            .items
            .iter()
            .find(|item| item.dimension == harness_domain::OutcomeDimension::CompletionState)
            .unwrap();
        assert_eq!(
            completion.revisions[0].outcome.classification,
            harness_domain::OutcomeClassification::Neutral
        );
        assert_eq!(completion.revisions[0].outcome.code, "completed");
        assert!(vector.items.iter().any(|item| item.dimension
            == harness_domain::OutcomeDimension::ResourceUse
            && item.revisions[0].outcome.code == "unavailable"));
        assert!(
            vector.items.iter().all(
                |item| item.revisions[0].outcome.confidence == OutcomeConfidence::Authoritative
            )
        );
        let actions: i64 = store
            .connection()
            .unwrap()
            .query_row("SELECT count(*) FROM human_actions", [], |row| row.get(0))
            .unwrap();
        let revisions: i64 = store
            .connection()
            .unwrap()
            .query_row(
                "SELECT count(*) FROM improvement_revisions WHERE aggregate_kind='outcome'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(actions, 0);
        assert_eq!(revisions, 7);
        assert!(
            vector
                .items
                .iter()
                .flat_map(|item| item.revisions.iter())
                .all(|revision| {
                    !matches!(
                        revision.outcome.source.record_id.as_str(),
                        "validation-pending" | "validation-invalid" | "evidence-invalid"
                    )
                })
        );
        assert_eq!(
            authoritative_result_label("new_result_class").1,
            "unavailable"
        );
        assert_eq!(
            outcome_subject(&run_id, Some("attempt-1".to_owned())).kind,
            harness_domain::OutcomeSubjectKind::TaskAttempt
        );
    }

    #[test]
    fn outcome_id_wire_contract_is_stable() {
        let (_, run_id) = store_with_created_run();
        let input = operator_outcome(&run_id, "outcome-wire", "accepted_without_correction");
        assert_eq!(
            stable_outcome_id(&input).unwrap().as_str(),
            "bce7860801efea10255ba97f654d8dcfe4feb13829fc7fb59925c1734302a109"
        );

        let mut manual = operator_outcome(&run_id, "outcome-same-id", "passed");
        manual.dimension = harness_domain::OutcomeDimension::Validation;
        let authoritative = AuthoritativeOutcomeInput {
            run_id: run_id.clone(),
            subject: manual.subject.clone(),
            dimension: manual.dimension,
            classification: manual.classification,
            code: manual.code.clone(),
            source_kind: OutcomeSourceKind::Validation,
            source_record_id: "validation-same-id".to_owned(),
            source_record_sha256: "a".repeat(64),
            source_sha: Some("b".repeat(40)),
            source_domain_event_id: None,
            observed_at: 1,
        };
        assert_eq!(
            stable_outcome_id(&manual).unwrap(),
            stable_authoritative_outcome_id(&authoritative).unwrap()
        );
    }

    #[test]
    fn successful_run_transition_emits_one_lifecycle_event_after_cursor() {
        let (store, run_id) = store_with_created_run();
        let cursor = store.latest_domain_cursor().unwrap();

        let updated = store
            .transition_run(&run_id, RunState::Preparing, "prepare", Some(1), None)
            .unwrap();

        assert_eq!(updated.state, RunState::Preparing);
        assert_eq!(updated.version, 2);
        let events = store.list_domain_events(cursor, Some(&run_id), 10).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].run_id, Some(run_id.clone()));
        assert_eq!(events[0].aggregate_type, "run");
        assert_eq!(events[0].aggregate_id, run_id.as_str());
        assert_eq!(events[0].event_type, "run.lifecycle.transitioned");
        assert_eq!(
            events[0].payload,
            json!({
                "prior_state": "CREATED",
                "next_state": "PREPARING",
                "phase": "prepare",
                "run_version": 2,
                "failure_class": null,
            })
        );
        assert!(
            store
                .list_domain_events(events[0].id, Some(&run_id), 10)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn rejected_run_transition_leaves_run_and_event_journal_unchanged() {
        let (store, run_id) = store_with_created_run();
        let before = store.run(&run_id).unwrap();
        let cursor = store.latest_domain_cursor().unwrap();

        assert!(
            store
                .transition_run(&run_id, RunState::Completed, "complete", Some(1), None)
                .is_err()
        );
        assert!(
            store
                .transition_run(&run_id, RunState::Preparing, "prepare", Some(2), None)
                .is_err()
        );

        let after = store.run(&run_id).unwrap();
        assert_eq!(after.state, before.state);
        assert_eq!(after.version, before.version);
        assert!(
            store
                .list_domain_events(cursor, Some(&run_id), 10)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn improvement_transitions_are_kind_aware() {
        use harness_domain::{ImprovementRecordKind as K, ImprovementState as S};
        for kind in [K::Taskset, K::EvalCase, K::GraderBundle, K::PolicyBundle] {
            assert!(improvement_transition_allowed(kind, S::Proposed, S::Active));
        }
        assert!(improvement_transition_allowed(
            K::Experiment,
            S::Validated,
            S::Running
        ));
        assert!(improvement_transition_allowed(
            K::Rollback,
            S::Requested,
            S::Completed
        ));
        assert!(!improvement_transition_allowed(
            K::Candidate,
            S::Validated,
            S::Running
        ));
        assert!(!improvement_transition_allowed(
            K::Experiment,
            S::Requested,
            S::Completed
        ));
    }
}

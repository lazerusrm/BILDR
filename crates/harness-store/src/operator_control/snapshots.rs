use std::collections::BTreeMap;

use harness_domain::{
    AttentionItem, ControlPlaneSnapshot, ControlPlaneSnapshotId, ReturnView, ReturnViewId,
    SnapshotSection, SnapshotSectionState, SnapshotTruncation, now_ms,
};
use rusqlite::{OptionalExtension, Transaction, TransactionBehavior, params};
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use crate::{Store, StoreError};

use super::{
    external_conditions::checked_condition_row, investigations::checked_artifact_row,
    progress::MAX_CLASSIFIER_EVENTS_PER_PASS,
};

pub const CONTROL_PLANE_SNAPSHOT_SCHEMA: &str = "harness.control-plane-snapshot.v1";
pub const RETURN_VIEW_SCHEMA: &str = "harness.return-view.v1";
const SECTION_ROW_LIMIT: i64 = 100;
const RETURN_CHANGE_ROW_LIMIT: i64 = 100;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ReturnViewCursor {
    pub operator_id: String,
    pub acknowledged_cursor: u64,
    pub expected_snapshot_revision: u64,
}

impl Store {
    /// Compiles the deterministic control-plane snapshot within one immediate
    /// SQLite transaction. It is reused only when every represented source
    /// cursor matches, so direct attention updates cannot be hidden behind an
    /// unchanged controller-domain cursor.
    pub fn control_plane_snapshot(&self) -> Result<ControlPlaneSnapshot, StoreError> {
        self.refresh_control_plane_projections()?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let snapshot = Self::compile_control_plane_snapshot(&transaction)?;
        transaction.commit()?;
        Ok(snapshot)
    }

    #[cfg(test)]
    fn control_plane_snapshot_after_projection_refresh<F>(
        &self,
        after_projection_refresh: F,
    ) -> Result<ControlPlaneSnapshot, StoreError>
    where
        F: FnOnce() -> Result<(), StoreError>,
    {
        self.refresh_control_plane_projections()?;
        after_projection_refresh()?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let snapshot = Self::compile_control_plane_snapshot(&transaction)?;
        transaction.commit()?;
        Ok(snapshot)
    }

    /// Captures supervisor inputs and the operator-control evidence from one
    /// SQLite snapshot. The caller may persist a resulting supervisor receipt
    /// later, but its run/task evidence and operator-control facts always
    /// describe the same immutable database cut.
    pub fn capture_supervisor_observation_with_control_plane(
        &self,
        run_id: &harness_domain::RunId,
        max_events: u32,
    ) -> Result<(crate::SupervisorObservationInput, ControlPlaneSnapshot), StoreError> {
        self.refresh_control_plane_projections()?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let observation = Self::capture_supervisor_observation_in_transaction(
            &transaction,
            run_id,
            max_events,
            || Ok(()),
        )?;
        let control_plane = Self::compile_control_plane_snapshot(&transaction)?;
        transaction.commit()?;
        Ok((observation, control_plane))
    }

    fn refresh_control_plane_projections(&self) -> Result<(), StoreError> {
        self.classify_material_progress()?;
        self.refresh_approval_attention()?;
        self.refresh_notification_mirror()?;
        Ok(())
    }

    fn compile_control_plane_snapshot(
        transaction: &Transaction<'_>,
    ) -> Result<ControlPlaneSnapshot, StoreError> {
        let event_cursor = non_negative(
            transaction.query_row("SELECT coalesce(max(id),0) FROM domain_events", [], |row| {
                row.get::<_, i64>(0)
            })?,
            "domain event cursor",
        )?;
        let attention_cursor = non_negative(
            transaction.query_row(
                "SELECT coalesce(max(id),0) FROM attention_events",
                [],
                |row| row.get::<_, i64>(0),
            )?,
            "attention event cursor",
        )?;
        let investigation_cursor = non_negative(
            transaction.query_row("SELECT count(*) FROM investigation_artifacts", [], |row| {
                row.get::<_, i64>(0)
            })?,
            "investigation artifact cursor",
        )?;
        let external_condition_cursor = non_negative(
            transaction.query_row("SELECT count(*) FROM external_conditions", [], |row| {
                row.get::<_, i64>(0)
            })?,
            "external condition cursor",
        )?;
        let condition_observation_cursor = non_negative(
            transaction.query_row("SELECT count(*) FROM condition_observations", [], |row| {
                row.get::<_, i64>(0)
            })?,
            "condition observation cursor",
        )?;
        let material_progress_cursor = non_negative(
            transaction.query_row("SELECT count(*) FROM material_progress_events", [], |row| {
                row.get::<_, i64>(0)
            })?,
            "material progress cursor",
        )?;
        let material_progress_classifier_cursor = non_negative(
            transaction.query_row(
                "SELECT event_cursor FROM material_progress_classifier_state WHERE id=1",
                [],
                |row| row.get::<_, i64>(0),
            )?,
            "material progress classifier cursor",
        )?;
        let liveness_episode_cursor = non_negative(
            transaction.query_row("SELECT count(*) FROM liveness_episodes", [], |row| {
                row.get::<_, i64>(0)
            })?,
            "liveness episode cursor",
        )?;
        let liveness_observation_cursor = non_negative(
            transaction.query_row("SELECT count(*) FROM liveness_observations", [], |row| {
                row.get::<_, i64>(0)
            })?,
            "liveness observation cursor",
        )?;
        let reconciliation_cursor = non_negative(
            transaction.query_row("SELECT count(*) FROM reconciliation_episodes", [], |row| {
                row.get::<_, i64>(0)
            })?,
            "reconciliation cursor",
        )?;
        let notification_cursor = non_negative(
            transaction.query_row("SELECT count(*) FROM notification_deliveries", [], |row| {
                row.get::<_, i64>(0)
            })?,
            "notification cursor",
        )?;
        let presence_cursor = non_negative(
            transaction.query_row(
                // Presence rows are versioned updates, not append-only facts.
                // Counting rows leaves a snapshot falsely reusable after an
                // operator changes focus/unattended preference. Each version
                // only increases, so their bounded sum invalidates the
                // snapshot on every update.
                "SELECT coalesce(sum(version),0) FROM operator_presence",
                [],
                |row| row.get::<_, i64>(0),
            )?,
            "operator presence cursor",
        )?;
        let mut source_cursors = BTreeMap::new();
        source_cursors.insert("domain_events".to_owned(), event_cursor);
        source_cursors.insert("attention_events".to_owned(), attention_cursor);
        source_cursors.insert("investigation_artifacts".to_owned(), investigation_cursor);
        source_cursors.insert("external_conditions".to_owned(), external_condition_cursor);
        source_cursors.insert(
            "condition_observations".to_owned(),
            condition_observation_cursor,
        );
        source_cursors.insert(
            "material_progress_events".to_owned(),
            material_progress_cursor,
        );
        source_cursors.insert(
            "material_progress_classifier".to_owned(),
            material_progress_classifier_cursor,
        );
        source_cursors.insert("liveness_episodes".to_owned(), liveness_episode_cursor);
        source_cursors.insert(
            "liveness_observations".to_owned(),
            liveness_observation_cursor,
        );
        source_cursors.insert("reconciliation_episodes".to_owned(), reconciliation_cursor);
        source_cursors.insert("notification_deliveries".to_owned(), notification_cursor);
        source_cursors.insert("operator_presence".to_owned(), presence_cursor);
        let source_cursors_sha256 = digest(&serde_json::to_string(&source_cursors)?);

        if let Some(existing) = transaction
            .query_row(
                "SELECT payload_json,payload_sha256 FROM control_plane_snapshots WHERE event_cursor=?1 AND source_cursors_sha256=?2",
                params![to_i64(event_cursor, "domain event cursor")?, source_cursors_sha256],
                |row| checked_snapshot_row(row.get(0)?, row.get(1)?),
            )
            .optional()?
        {
            return Ok(existing);
        }
        let revision = non_negative(
            transaction.query_row(
                "SELECT coalesce(max(revision),0)+1 FROM control_plane_snapshots",
                [],
                |row| row.get::<_, i64>(0),
            )?,
            "control plane snapshot revision",
        )?;
        let (attention_rows, attention_truncation) = attention_rows(&transaction)?;
        let (run_rows, run_truncation) = run_rows(&transaction)?;
        let (attempt_rows, attempt_truncation) = attempt_rows(&transaction)?;
        let (investigation_rows, investigation_truncation) = investigation_rows(&transaction)?;
        let (external_condition_rows, external_condition_truncation) =
            external_condition_rows(&transaction)?;
        let (progress_rows, progress_truncation) = material_progress_rows(&transaction)?;
        let (liveness_rows, liveness_truncation) = liveness_rows(&transaction)?;
        let (reconciliation_rows, reconciliation_truncation) = reconciliation_rows(&transaction)?;
        let (notification_rows, notification_truncation) = notification_rows(&transaction)?;
        let active_agents: i64 = transaction.query_row(
            "SELECT count(*) FROM agent_sessions WHERE state NOT IN ('COMPLETED','FAILED','CANCELED')",
            [],
            |row| row.get(0),
        )?;
        let runtime_schema_version: String = transaction.query_row(
            "SELECT value FROM schema_migrations_meta WHERE key='runtime_schema_version'",
            [],
            |row| row.get(0),
        )?;
        let current = |rows, cursor| SnapshotSection {
            state: SnapshotSectionState::Current,
            rows,
            source_cursor: cursor,
            truncated: false,
            detail: None,
        };
        let unavailable = |detail: &str| SnapshotSection {
            state: SnapshotSectionState::Unknown,
            rows: Vec::new(),
            source_cursor: 0,
            truncated: false,
            detail: Some(detail.to_owned()),
        };
        let mut attention = current(attention_rows, attention_cursor);
        attention.truncated = attention_truncation.is_some();
        let mut runs = current(run_rows, event_cursor);
        runs.truncated = run_truncation.is_some();
        let mut attempts = current(attempt_rows, event_cursor);
        attempts.truncated = attempt_truncation.is_some();
        let mut truncation = Vec::new();
        for (section, omitted) in [
            ("attention", attention_truncation),
            ("runs", run_truncation),
            ("attempts", attempt_truncation),
            ("investigations", investigation_truncation),
            ("external_conditions", external_condition_truncation),
            ("progress", progress_truncation),
            ("liveness", liveness_truncation),
            ("reconciliation", reconciliation_truncation),
            ("notifications", notification_truncation),
        ] {
            if let Some(omitted_rows) = omitted {
                truncation.push(SnapshotTruncation {
                    section: section.to_owned(),
                    omitted_rows,
                    limit: SECTION_ROW_LIMIT as u64,
                });
            }
        }
        let material_progress_backlog =
            event_cursor.saturating_sub(material_progress_classifier_cursor);
        if material_progress_backlog > 0 {
            truncation.push(SnapshotTruncation {
                section: "progress_classifier".to_owned(),
                omitted_rows: material_progress_backlog,
                limit: u64::try_from(MAX_CLASSIFIER_EVENTS_PER_PASS).map_err(|_| {
                    StoreError::Validation(
                        "material progress classifier limit is invalid".to_owned(),
                    )
                })?,
            });
        }
        let snapshot_id = ControlPlaneSnapshotId::new();
        let mut snapshot = ControlPlaneSnapshot {
            schema: CONTROL_PLANE_SNAPSHOT_SCHEMA.to_owned(),
            snapshot_id,
            revision,
            compiled_at_ms: now_ms(),
            event_cursor,
            consistency: "sqlite_immediate_transaction.v1".to_owned(),
            system: current(
                vec![json!({
                    "runtime_schema_version": runtime_schema_version,
                    "projection": "operator_control_deterministic_slice"
                })],
                event_cursor,
            ),
            accounts: unavailable(
                "Account-limit observation is not wired into this deterministic slice.",
            ),
            scheduler: current(
                vec![json!({ "active_agent_sessions": active_agents })],
                event_cursor,
            ),
            runs,
            attention,
            attempts,
            investigations: {
                let mut section = current(investigation_rows, investigation_cursor);
                section.truncated = investigation_truncation.is_some();
                section
            },
            progress: {
                let mut section = current(progress_rows, material_progress_cursor);
                section.truncated = progress_truncation.is_some() || material_progress_backlog > 0;
                section.detail = Some(if material_progress_backlog > 0 {
                    format!(
                        "Replayable material-progress records are catching up through domain event {material_progress_classifier_cursor} of {event_cursor}; {material_progress_backlog} source events remain. This section is stale and cannot be treated as complete custody."
                    )
                } else {
                    "Replayable material-progress records from the closed controller-event allow-list; ordinary activity is excluded."
                        .to_owned()
                });
                if material_progress_backlog > 0 {
                    section.state = SnapshotSectionState::Stale;
                }
                section
            },
            liveness: {
                let mut section = current(
                    liveness_rows,
                    liveness_episode_cursor
                        .checked_add(liveness_observation_cursor)
                        .ok_or_else(|| {
                            StoreError::Validation("liveness snapshot cursor overflow".to_owned())
                        })?,
                );
                section.truncated = liveness_truncation.is_some();
                section.detail = Some(
                    "Observe-only deterministic liveness episodes. This projection never starts recovery or clears state from model prose."
                        .to_owned(),
                );
                section
            },
            reconciliation: {
                let mut section = current(reconciliation_rows, reconciliation_cursor);
                section.truncated = reconciliation_truncation.is_some();
                section.detail = Some(
                    "Closed reconciliation inventory records. This view cannot reset work, release a lease, or authorize a new attempt."
                        .to_owned(),
                );
                section
            },
            external_conditions: {
                let mut section = current(
                    external_condition_rows,
                    external_condition_cursor
                        .checked_add(condition_observation_cursor)
                        .ok_or_else(|| {
                            StoreError::Validation(
                                "external condition snapshot cursor overflow".to_owned(),
                            )
                        })?,
                );
                section.truncated = external_condition_truncation.is_some();
                section.detail = Some(
                    "Source-owned conditions plus deterministic local time gates. This view never wakes work or executes a result."
                        .to_owned(),
                );
                section
            },
            cost: unavailable("Cost projection is not wired into this deterministic slice."),
            notifications: {
                let mut section = current(notification_rows, notification_cursor);
                section.truncated = notification_truncation.is_some();
                section.detail = Some(
                    "In-product notification mirror only. Presence is stored separately and does not suppress, batch, send, or resolve any source item."
                        .to_owned(),
                );
                section
            },
            limits: unavailable(
                "Rate-limit observation is not wired into this deterministic slice.",
            ),
            truncation,
            source_cursors,
            sha256: String::new(),
        };
        snapshot.sha256 = snapshot_contract_digest(&snapshot)?;
        snapshot
            .validate()
            .map_err(|error| StoreError::Validation(error.to_string()))?;
        let raw = serde_json::to_string(&snapshot)?;
        let payload_sha256 = digest(&raw);
        transaction.execute(
            "INSERT INTO control_plane_snapshots(id,revision,event_cursor,source_cursors_sha256,consistency,payload_json,payload_sha256,created_at) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
            params![
                snapshot.snapshot_id.as_str(),
                to_i64(snapshot.revision, "control plane snapshot revision")?,
                to_i64(snapshot.event_cursor, "domain event cursor")?,
                source_cursors_sha256,
                snapshot.consistency,
                raw,
                payload_sha256,
                snapshot.compiled_at_ms,
            ],
        )?;
        for (name, section) in snapshot_sections(&snapshot) {
            let section_raw = serde_json::to_string(section)?;
            transaction.execute(
                "INSERT INTO snapshot_sections(snapshot_id,section_name,state,source_cursor,truncated,row_count,payload_json,payload_sha256) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
                params![
                    snapshot.snapshot_id.as_str(),
                    name,
                    section_state_name(section.state),
                    to_i64(section.source_cursor, "snapshot section cursor")?,
                    if section.truncated { 1_i64 } else { 0_i64 },
                    to_i64(section.rows.len() as u64, "snapshot section row count")?,
                    section_raw,
                    digest(&serde_json::to_string(section)?),
                ],
            )?;
        }
        Ok(snapshot)
    }

    pub fn latest_control_plane_snapshot(
        &self,
    ) -> Result<Option<ControlPlaneSnapshot>, StoreError> {
        self.connection()?
            .query_row(
                "SELECT payload_json,payload_sha256 FROM control_plane_snapshots ORDER BY revision DESC LIMIT 1",
                [],
                |row| checked_snapshot_row(row.get(0)?, row.get(1)?),
            )
            .optional()
            .map_err(Into::into)
    }

    /// Builds a bounded return view from a single immutable snapshot. It does
    /// not claim unimplemented sections are healthy; their `unknown` state is
    /// carried through verbatim for an operator returning to the console.
    pub fn control_plane_return_view(&self, operator_id: &str) -> Result<ReturnView, StoreError> {
        validate_operator_id(operator_id)?;
        let snapshot = self.control_plane_snapshot()?;
        let connection = self.connection()?;
        let cursor = connection
            .query_row(
                "SELECT acknowledged_cursor,expected_snapshot_revision FROM return_view_cursors WHERE operator_id=?1",
                [operator_id],
                |row| {
                    Ok(ReturnViewCursor {
                        operator_id: operator_id.to_owned(),
                        acknowledged_cursor: non_negative(row.get(0)?, "return cursor").map_err(to_sql_error)?,
                        expected_snapshot_revision: non_negative(row.get(1)?, "return snapshot revision").map_err(to_sql_error)?,
                    })
                },
            )
            .optional()?
            .unwrap_or(ReturnViewCursor {
                operator_id: operator_id.to_owned(),
                acknowledged_cursor: 0,
            expected_snapshot_revision: snapshot.revision,
        });
        let material_changes = controller_event_rows(
            &connection,
            cursor.acknowledged_cursor,
            snapshot.event_cursor,
        )?;
        let mut sections = BTreeMap::new();
        for (name, section) in snapshot_sections(&snapshot) {
            sections.insert(name.to_owned(), section.clone());
        }
        sections.insert("material_changes".to_owned(), material_changes);
        let mut view = ReturnView {
            schema: RETURN_VIEW_SCHEMA.to_owned(),
            return_view_id: ReturnViewId::new(),
            snapshot_id: snapshot.snapshot_id.clone(),
            snapshot_revision: snapshot.revision,
            event_cursor: snapshot.event_cursor,
            acknowledged_cursor: cursor.acknowledged_cursor,
            sections,
            sha256: String::new(),
        };
        view.sha256 = return_view_digest(&view)?;
        Ok(view)
    }

    pub fn advance_return_view_cursor(
        &self,
        operator_id: &str,
        expected_snapshot_revision: u64,
        acknowledged_cursor: u64,
    ) -> Result<ReturnViewCursor, StoreError> {
        validate_operator_id(operator_id)?;
        let expected_snapshot_revision = to_i64(
            expected_snapshot_revision,
            "return view expected snapshot revision",
        )?;
        let acknowledged_cursor = to_i64(acknowledged_cursor, "return view acknowledged cursor")?;
        let now = now_ms();
        let mut connection = self.connection()?;
        let transaction = connection.transaction()?;
        let current: Option<(i64, i64)> = transaction
            .query_row(
                "SELECT acknowledged_cursor,expected_snapshot_revision FROM return_view_cursors WHERE operator_id=?1",
                [operator_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let (current_snapshot_revision, current_event_cursor): (i64, i64) = transaction.query_row(
            "SELECT revision,event_cursor FROM control_plane_snapshots ORDER BY revision DESC LIMIT 1",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        if current_snapshot_revision != expected_snapshot_revision {
            return Err(StoreError::Conflict(
                "return view snapshot changed; refresh before acknowledging it".to_owned(),
            ));
        }
        if let Some((prior_cursor, _prior_revision)) = current {
            if acknowledged_cursor < prior_cursor {
                return Err(StoreError::Conflict(
                    "return view acknowledgement cursor cannot move backwards".to_owned(),
                ));
            }
        }
        if acknowledged_cursor > current_event_cursor {
            return Err(StoreError::Validation(
                "return view acknowledgement cursor exceeds the bound snapshot cursor".to_owned(),
            ));
        }
        transaction.execute(
            "INSERT INTO return_view_cursors(operator_id,acknowledged_cursor,expected_snapshot_revision,updated_at) VALUES(?1,?2,?3,?4) ON CONFLICT(operator_id) DO UPDATE SET acknowledged_cursor=excluded.acknowledged_cursor,expected_snapshot_revision=excluded.expected_snapshot_revision,updated_at=excluded.updated_at",
            params![operator_id, acknowledged_cursor, expected_snapshot_revision, now],
        )?;
        transaction.commit()?;
        Ok(ReturnViewCursor {
            operator_id: operator_id.to_owned(),
            acknowledged_cursor: non_negative(acknowledged_cursor, "return cursor")?,
            expected_snapshot_revision: non_negative(
                expected_snapshot_revision,
                "return snapshot revision",
            )?,
        })
    }
}

fn controller_event_rows(
    connection: &rusqlite::Connection,
    acknowledged_cursor: u64,
    snapshot_cursor: u64,
) -> Result<SnapshotSection, StoreError> {
    let acknowledged_cursor = to_i64(acknowledged_cursor, "return view acknowledged cursor")?;
    let snapshot_cursor = to_i64(snapshot_cursor, "return view snapshot cursor")?;
    if acknowledged_cursor > snapshot_cursor {
        return Err(StoreError::Validation(
            "return view acknowledgement cursor is newer than the bound snapshot".to_owned(),
        ));
    }
    let count = non_negative(
        connection.query_row(
            "SELECT count(*) FROM domain_events WHERE id>?1 AND id<=?2",
            params![acknowledged_cursor, snapshot_cursor],
            |row| row.get(0),
        )?,
        "return view event count",
    )?;
    let mut statement = connection.prepare(
        "SELECT id,run_id,aggregate_type,aggregate_id,event_type,occurred_at FROM domain_events WHERE id>?1 AND id<=?2 ORDER BY id ASC LIMIT ?3",
    )?;
    let rows = statement
        .query_map(
            params![
                acknowledged_cursor,
                snapshot_cursor,
                RETURN_CHANGE_ROW_LIMIT
            ],
            |row| {
                Ok(json!({
                    "event_id": row.get::<_, i64>(0)?,
                    "run_id": row.get::<_, Option<String>>(1)?,
                    "aggregate_type": row.get::<_, String>(2)?,
                    "aggregate_id": row.get::<_, String>(3)?,
                    "event_type": row.get::<_, String>(4)?,
                    "occurred_at_ms": row.get::<_, i64>(5)?,
                }))
            },
        )?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(SnapshotSection {
        state: SnapshotSectionState::Current,
        rows,
        source_cursor: u64::try_from(snapshot_cursor).map_err(|_| {
            StoreError::Validation("return view snapshot cursor is negative".to_owned())
        })?,
        truncated: count > RETURN_CHANGE_ROW_LIMIT as u64,
        detail: Some(
            "Chronological controller events since the last acknowledgement; the separate progress section contains only classifier-approved material changes."
                .to_owned(),
        ),
    })
}

fn attention_rows(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(Vec<Value>, Option<u64>), StoreError> {
    let count: i64 = transaction.query_row(
        "SELECT count(*) FROM attention_items WHERE state IN ('open','acknowledged','waiting_external')",
        [],
        |row| row.get(0),
    )?;
    let mut statement = transaction.prepare(
        "SELECT payload_json,payload_sha256 FROM attention_items WHERE state IN ('open','acknowledged','waiting_external') ORDER BY CASE severity WHEN 'critical' THEN 0 WHEN 'high' THEN 1 WHEN 'normal' THEN 2 ELSE 3 END,opened_at DESC,id DESC LIMIT ?1",
    )?;
    let rows = statement.query_map([SECTION_ROW_LIMIT], |row| {
        let raw: String = row.get(0)?;
        let stored: String = row.get(1)?;
        if digest(&raw) != stored {
            return Err(rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                "attention payload integrity check failed".into(),
            ));
        }
        let item: AttentionItem = serde_json::from_str(&raw).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?;
        item.validate().map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })?;
        serde_json::to_value(item)
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
    })?;
    let rows = rows.collect::<Result<Vec<_>, _>>()?;
    let count = non_negative(count, "attention count")?;
    Ok((
        rows,
        (count > SECTION_ROW_LIMIT as u64).then(|| count - SECTION_ROW_LIMIT as u64),
    ))
}

fn run_rows(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(Vec<Value>, Option<u64>), StoreError> {
    bounded_json_rows(
        transaction,
        "SELECT count(*) FROM runs",
        "SELECT id,title,state,phase,base_sha,created_at,updated_at FROM runs ORDER BY updated_at DESC,id DESC LIMIT ?1",
        |row| {
            Ok(json!({
                "run_id": row.get::<_, String>(0)?,
                "title": row.get::<_, String>(1)?,
                "state": row.get::<_, String>(2)?,
                "phase": row.get::<_, String>(3)?,
                "base_sha": row.get::<_, String>(4)?,
                "created_at_ms": row.get::<_, i64>(5)?,
                "updated_at_ms": row.get::<_, i64>(6)?,
            }))
        },
    )
}

fn attempt_rows(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(Vec<Value>, Option<u64>), StoreError> {
    bounded_json_rows(
        transaction,
        "SELECT count(*) FROM task_attempts",
        "SELECT a.id,t.run_id,t.external_task_id,a.state,a.attempt_number,a.started_at,a.completed_at,a.failure_reason FROM task_attempts a JOIN tasks t ON t.id=a.task_id ORDER BY a.updated_at DESC,a.id DESC LIMIT ?1",
        |row| {
            Ok(json!({
                "attempt_id": row.get::<_, String>(0)?,
                "run_id": row.get::<_, String>(1)?,
                "task_id": row.get::<_, String>(2)?,
                "state": row.get::<_, String>(3)?,
                "attempt_number": row.get::<_, i64>(4)?,
                "started_at_ms": row.get::<_, Option<i64>>(5)?,
                "completed_at_ms": row.get::<_, Option<i64>>(6)?,
                "failure_reason": row.get::<_, Option<String>>(7)?,
            }))
        },
    )
}

fn investigation_rows(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(Vec<Value>, Option<u64>), StoreError> {
    let count = non_negative(
        transaction.query_row("SELECT count(*) FROM investigation_artifacts", [], |row| {
            row.get::<_, i64>(0)
        })?,
        "investigation artifact count",
    )?;
    let mut statement = transaction.prepare(
        "SELECT payload_json,payload_sha256 FROM investigation_artifacts ORDER BY created_at DESC,id DESC LIMIT ?1",
    )?;
    let rows = statement
        .query_map([SECTION_ROW_LIMIT], |row| {
            let artifact = checked_artifact_row(row.get(0)?, row.get(1)?)?;
            serde_json::to_value(harness_domain::InvestigationArtifactSummary::from(
                &artifact,
            ))
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok((
        rows,
        (count > SECTION_ROW_LIMIT as u64).then(|| count - SECTION_ROW_LIMIT as u64),
    ))
}

fn external_condition_rows(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(Vec<Value>, Option<u64>), StoreError> {
    let count = non_negative(
        transaction.query_row(
            "SELECT count(*) FROM external_conditions WHERE state IN ('open','unknown')",
            [],
            |row| row.get::<_, i64>(0),
        )?,
        "active external condition count",
    )?;
    let mut statement = transaction.prepare(
        "SELECT current_payload_json,current_payload_sha256 FROM external_conditions WHERE state IN ('open','unknown') ORDER BY updated_at DESC,id DESC LIMIT ?1",
    )?;
    let rows = statement
        .query_map([SECTION_ROW_LIMIT], |row| {
            let condition = checked_condition_row(row.get(0)?, row.get(1)?)?;
            serde_json::to_value(harness_domain::ExternalConditionSummary::from(&condition))
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok((
        rows,
        (count > SECTION_ROW_LIMIT as u64).then(|| count - SECTION_ROW_LIMIT as u64),
    ))
}

fn material_progress_rows(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(Vec<Value>, Option<u64>), StoreError> {
    let count = non_negative(
        transaction.query_row("SELECT count(*) FROM material_progress_events", [], |row| {
            row.get::<_, i64>(0)
        })?,
        "material progress count",
    )?;
    let mut statement = transaction.prepare(
        "SELECT payload_json,payload_sha256 FROM material_progress_events ORDER BY occurred_at DESC,id DESC LIMIT ?1",
    )?;
    let rows = statement
        .query_map([SECTION_ROW_LIMIT], |row| {
            let progress = super::progress::checked_progress_row(row.get(0)?, row.get(1)?)?;
            serde_json::to_value(progress)
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok((
        rows,
        (count > SECTION_ROW_LIMIT as u64).then(|| count - SECTION_ROW_LIMIT as u64),
    ))
}

fn liveness_rows(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(Vec<Value>, Option<u64>), StoreError> {
    let count = non_negative(
        transaction.query_row("SELECT count(*) FROM liveness_episodes", [], |row| {
            row.get::<_, i64>(0)
        })?,
        "liveness episode count",
    )?;
    let mut statement = transaction.prepare(
        "SELECT current_payload_json,current_payload_sha256 FROM liveness_episodes ORDER BY updated_at DESC,id DESC LIMIT ?1",
    )?;
    let rows = statement
        .query_map([SECTION_ROW_LIMIT], |row| {
            let episode = super::liveness::checked_episode_row(row.get(0)?, row.get(1)?)?;
            serde_json::to_value(episode)
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok((
        rows,
        (count > SECTION_ROW_LIMIT as u64).then(|| count - SECTION_ROW_LIMIT as u64),
    ))
}

fn reconciliation_rows(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(Vec<Value>, Option<u64>), StoreError> {
    let count = non_negative(
        transaction.query_row("SELECT count(*) FROM reconciliation_episodes", [], |row| {
            row.get::<_, i64>(0)
        })?,
        "reconciliation episode count",
    )?;
    let mut statement = transaction.prepare(
        "SELECT current_payload_json,current_payload_sha256 FROM reconciliation_episodes ORDER BY updated_at DESC,id DESC LIMIT ?1",
    )?;
    let rows = statement
        .query_map([SECTION_ROW_LIMIT], |row| {
            let episode =
                super::reconciliation::checked_reconciliation_row(row.get(0)?, row.get(1)?)?;
            serde_json::to_value(episode)
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok((
        rows,
        (count > SECTION_ROW_LIMIT as u64).then(|| count - SECTION_ROW_LIMIT as u64),
    ))
}

fn notification_rows(
    transaction: &rusqlite::Transaction<'_>,
) -> Result<(Vec<Value>, Option<u64>), StoreError> {
    let count = non_negative(
        transaction.query_row("SELECT count(*) FROM notification_deliveries", [], |row| {
            row.get::<_, i64>(0)
        })?,
        "notification count",
    )?;
    let mut statement = transaction.prepare(
        "SELECT payload_json,payload_sha256 FROM notification_deliveries ORDER BY created_at DESC,id DESC LIMIT ?1",
    )?;
    let rows = statement
        .query_map([SECTION_ROW_LIMIT], |row| {
            let delivery = super::notifications::checked_delivery_row(row.get(0)?, row.get(1)?)?;
            serde_json::to_value(delivery)
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    Ok((
        rows,
        (count > SECTION_ROW_LIMIT as u64).then(|| count - SECTION_ROW_LIMIT as u64),
    ))
}

fn bounded_json_rows<F>(
    transaction: &rusqlite::Transaction<'_>,
    count_sql: &str,
    rows_sql: &str,
    map: F,
) -> Result<(Vec<Value>, Option<u64>), StoreError>
where
    F: FnMut(&rusqlite::Row<'_>) -> rusqlite::Result<Value>,
{
    let count = non_negative(
        transaction.query_row(count_sql, [], |row| row.get(0))?,
        "section count",
    )?;
    let mut statement = transaction.prepare(rows_sql)?;
    let rows = statement.query_map([SECTION_ROW_LIMIT], map)?;
    let values = rows.collect::<Result<Vec<_>, _>>()?;
    Ok((
        values,
        (count > SECTION_ROW_LIMIT as u64).then(|| count - SECTION_ROW_LIMIT as u64),
    ))
}

fn snapshot_sections(snapshot: &ControlPlaneSnapshot) -> [(&str, &SnapshotSection); 14] {
    [
        ("system", &snapshot.system),
        ("accounts", &snapshot.accounts),
        ("scheduler", &snapshot.scheduler),
        ("runs", &snapshot.runs),
        ("attention", &snapshot.attention),
        ("attempts", &snapshot.attempts),
        ("investigations", &snapshot.investigations),
        ("progress", &snapshot.progress),
        ("liveness", &snapshot.liveness),
        ("reconciliation", &snapshot.reconciliation),
        ("external_conditions", &snapshot.external_conditions),
        ("cost", &snapshot.cost),
        ("notifications", &snapshot.notifications),
        ("limits", &snapshot.limits),
    ]
}

fn checked_snapshot_row(
    raw: String,
    payload_sha256: String,
) -> rusqlite::Result<ControlPlaneSnapshot> {
    if digest(&raw) != payload_sha256 {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            "control plane snapshot payload integrity check failed".into(),
        ));
    }
    let snapshot: ControlPlaneSnapshot = serde_json::from_str(&raw).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })?;
    snapshot.validate().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })?;
    if snapshot_contract_digest(&snapshot).map_err(to_sql_error)? != snapshot.sha256 {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            "control plane snapshot contract digest mismatch".into(),
        ));
    }
    Ok(snapshot)
}

fn snapshot_contract_digest(snapshot: &ControlPlaneSnapshot) -> Result<String, StoreError> {
    let mut unsigned = snapshot.clone();
    unsigned.sha256.clear();
    Ok(digest(&serde_json::to_string(&unsigned)?))
}

fn return_view_digest(view: &ReturnView) -> Result<String, StoreError> {
    let mut unsigned = view.clone();
    unsigned.sha256.clear();
    Ok(digest(&serde_json::to_string(&unsigned)?))
}

fn section_state_name(state: SnapshotSectionState) -> &'static str {
    match state {
        SnapshotSectionState::Current => "current",
        SnapshotSectionState::Stale => "stale",
        SnapshotSectionState::Unknown => "unknown",
        SnapshotSectionState::Error => "error",
    }
}

fn validate_operator_id(operator_id: &str) -> Result<(), StoreError> {
    if operator_id.is_empty()
        || operator_id.len() > 160
        || !operator_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
    {
        return Err(StoreError::Validation(
            "operator id must be a bounded path-safe identifier".to_owned(),
        ));
    }
    Ok(())
}

fn non_negative(value: i64, field: &str) -> Result<u64, StoreError> {
    u64::try_from(value).map_err(|_| StoreError::Validation(format!("{field} is negative")))
}

fn to_i64(value: u64, field: &str) -> Result<i64, StoreError> {
    i64::try_from(value)
        .map_err(|_| StoreError::Validation(format!("{field} exceeds SQLite integer range")))
}

fn to_sql_error(error: StoreError) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(error))
}

fn digest(raw: &str) -> String {
    hex::encode(Sha256::digest(raw.as_bytes()))
}

#[cfg(test)]
mod tests {
    use harness_domain::OperatorPresenceMode;
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn snapshot_is_reused_only_at_the_exact_source_cursors() {
        let temp = TempDir::new().expect("temp");
        let store = Store::in_memory(&temp.path().join("artifacts")).expect("store");
        let first = store.control_plane_snapshot().expect("first snapshot");
        let same = store.control_plane_snapshot().expect("cached snapshot");
        assert_eq!(first.snapshot_id, same.snapshot_id);
        assert_eq!(first.revision, same.revision);
        assert_eq!(first.accounts.state, SnapshotSectionState::Unknown);
        assert!(first.accounts.rows.is_empty());
        assert!(first.limits.detail.is_some());
    }

    #[test]
    fn presence_revision_invalidates_a_control_plane_snapshot() {
        let temp = TempDir::new().expect("temp");
        let store = Store::in_memory(&temp.path().join("artifacts")).expect("store");
        let first = store.control_plane_snapshot().expect("first snapshot");
        let initial = store.operator_presence("operator-a").expect("presence");
        let focus = store
            .set_operator_presence("operator-a", OperatorPresenceMode::Focus, initial.version)
            .expect("focus");
        let second = store.control_plane_snapshot().expect("presence snapshot");
        assert_ne!(second.snapshot_id, first.snapshot_id);
        assert_eq!(second.source_cursors["operator_presence"], focus.version);
        let unattended = store
            .set_operator_presence(
                "operator-a",
                OperatorPresenceMode::Unattended,
                focus.version,
            )
            .expect("unattended");
        let third = store.control_plane_snapshot().expect("changed snapshot");
        assert_ne!(third.snapshot_id, second.snapshot_id);
        assert_eq!(
            third.source_cursors["operator_presence"],
            unattended.version
        );
    }

    #[test]
    fn return_view_preserves_current_observe_only_sections_and_cursor_cannot_regress() {
        let temp = TempDir::new().expect("temp");
        let store = Store::in_memory(&temp.path().join("artifacts")).expect("store");
        for event in 0..4 {
            store
                .emit_domain_event(
                    None,
                    "operator_control_test",
                    &format!("return_view_{event}"),
                    "operator_control.return_view_test",
                    &Value::Null,
                    None,
                )
                .expect("event");
        }
        let snapshot = store.control_plane_snapshot().expect("snapshot");
        store
            .advance_return_view_cursor("operator_a", snapshot.revision, snapshot.event_cursor)
            .expect("cursor");
        let view = store
            .control_plane_return_view("operator_a")
            .expect("return view");
        assert_eq!(view.event_cursor, snapshot.event_cursor);
        assert_eq!(view.acknowledged_cursor, snapshot.event_cursor);
        assert!(
            view.sections
                .get("material_changes")
                .expect("material changes")
                .rows
                .is_empty()
        );
        assert_eq!(
            view.sections.get("liveness").expect("liveness").state,
            SnapshotSectionState::Current
        );
        assert!(matches!(
            store.advance_return_view_cursor("operator_a", snapshot.revision, 3),
            Err(StoreError::Conflict(_))
        ));
        assert!(matches!(
            store.advance_return_view_cursor(
                "operator_a",
                snapshot.revision,
                snapshot.event_cursor + 1
            ),
            Err(StoreError::Validation(_))
        ));
    }

    #[test]
    fn return_view_preserves_chronological_controller_events_since_cursor() {
        let temp = TempDir::new().expect("temp");
        let store = Store::in_memory(&temp.path().join("artifacts")).expect("store");
        for event in 0..2 {
            store
                .emit_domain_event(
                    None,
                    "operator_control_test",
                    &format!("timeline_{event}"),
                    "operator_control.timeline",
                    &Value::Null,
                    None,
                )
                .expect("event");
        }
        let first = store.control_plane_snapshot().expect("first snapshot");
        store
            .advance_return_view_cursor("operator_a", first.revision, 1)
            .expect("cursor");
        store
            .emit_domain_event(
                None,
                "operator_control_test",
                "timeline_2",
                "operator_control.timeline",
                &Value::Null,
                None,
            )
            .expect("event");
        let view = store
            .control_plane_return_view("operator_a")
            .expect("return view");
        let events = &view.sections["material_changes"].rows;
        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["event_id"], Value::from(2));
        assert_eq!(events[1]["event_id"], Value::from(3));
    }

    #[test]
    fn bounded_classifier_backlog_is_explicitly_stale_until_caught_up() {
        let temp = TempDir::new().expect("temp");
        let store = Store::in_memory(&temp.path().join("artifacts")).expect("store");
        for event in 0..=MAX_CLASSIFIER_EVENTS_PER_PASS {
            store
                .emit_domain_event(
                    None,
                    "operator_control_test",
                    &format!("backlog_{event}"),
                    "operator_control.backlog",
                    &Value::Null,
                    None,
                )
                .expect("event");
        }
        let stale = store.control_plane_snapshot().expect("bounded snapshot");
        assert_eq!(stale.progress.state, SnapshotSectionState::Stale);
        assert!(stale.progress.truncated);
        assert!(stale.truncation.iter().any(|entry| {
            entry.section == "progress_classifier"
                && entry.omitted_rows == 1
                && entry.limit == u64::try_from(MAX_CLASSIFIER_EVENTS_PER_PASS).unwrap()
        }));
        assert_eq!(
            stale.source_cursors["material_progress_classifier"],
            u64::try_from(MAX_CLASSIFIER_EVENTS_PER_PASS).unwrap()
        );
        let current = store.control_plane_snapshot().expect("caught-up snapshot");
        assert_eq!(current.progress.state, SnapshotSectionState::Current);
        assert!(
            !current
                .truncation
                .iter()
                .any(|entry| entry.section == "progress_classifier")
        );
    }

    #[test]
    fn event_committed_between_projection_refresh_and_capture_is_stale_not_hidden() {
        let temp = TempDir::new().expect("temp");
        let database = temp.path().join("harness.sqlite3");
        let store = Store::open(&database, &temp.path().join("artifacts")).expect("store");
        let writer = Store::open(&database, &temp.path().join("writer-artifacts")).expect("writer");
        let captured = store
            .control_plane_snapshot_after_projection_refresh(|| {
                writer.emit_domain_event(
                    None,
                    "operator_control_test",
                    "race_event",
                    "task.verified",
                    &Value::Null,
                    None,
                )?;
                Ok(())
            })
            .expect("snapshot captures the later source cursor");
        assert_eq!(captured.event_cursor, 1);
        assert_eq!(captured.progress.state, SnapshotSectionState::Stale);
        assert!(
            captured
                .truncation
                .iter()
                .any(|entry| { entry.section == "progress_classifier" && entry.omitted_rows == 1 })
        );
    }
}

//! Controller-owned restart and runtime-loss inventory custody.
//!
//! The runtime-loss recovery path can preserve sessions and worktrees. This
//! module makes its inventory durable before that path changes any record. It does
//! not consume ownership proof, authorize a fresh attempt, or expose an
//! independent recovery action.

use harness_domain::{
    ReconciliationActionKind, ReconciliationActionReceipt, ReconciliationEpisode,
    ReconciliationEpisodeId, ReconciliationFinding, ReconciliationFindingKind, ReconciliationState,
    ReconciliationTrigger, RunSummary, now_ms,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::{Orchestrator, OrchestratorError};

const MAX_INVENTORY_BYTES: usize = 240 * 1024;
const RECONCILIATION_PAGE_LIMIT: u32 = 200;

impl Orchestrator {
    /// Records a bounded exact-identity inventory before the existing recovery
    /// path can stall, preserve, release, or otherwise alter runtime custody.
    pub(super) fn record_reconciliation_inventory(
        &self,
        run: &RunSummary,
        reason: &str,
    ) -> Result<ReconciliationEpisode, OrchestratorError> {
        let trigger = trigger_for_reason(reason);
        let inventory = self.reconciliation_inventory(run)?;
        let inventory_raw = serde_json::to_vec(&inventory)?;
        if inventory_raw.len() > MAX_INVENTORY_BYTES {
            return Err(OrchestratorError::Protocol(format!(
                "reconciliation inventory for run {} is {} bytes, exceeding the {} byte custody bound",
                run.id,
                inventory_raw.len(),
                MAX_INVENTORY_BYTES,
            )));
        }
        let inventory_sha256 = digest_bytes(&inventory_raw);
        let mut episodes = self
            .store
            .list_reconciliation_episodes(Some(run.id.as_str()), RECONCILIATION_PAGE_LIMIT)?;
        let existing = episodes.drain(..).find(|episode| {
            episode.trigger_kind == trigger
                && !matches!(
                    episode.state,
                    ReconciliationState::Resolved | ReconciliationState::Refused
                )
        });
        let now = now_ms();
        let episode = match existing {
            Some(episode) => episode,
            None => {
                let mut episode = ReconciliationEpisode {
                    schema: "harness.reconciliation-episode.v1".to_owned(),
                    episode_id: ReconciliationEpisodeId::new(),
                    run_id: Some(run.id.to_string()),
                    trigger_kind: trigger,
                    state: ReconciliationState::Open,
                    version: 1,
                    opened_at_ms: now,
                    updated_at_ms: now,
                    source_event_id: format!(
                        "reconciliation-{}-{}-v{}",
                        trigger_name(trigger),
                        run.id,
                        run.version
                    ),
                    inventory_sha256: inventory_sha256.clone(),
                    finding_count: 0,
                    action_count: 0,
                    report: Some(format!(
                        "controller inventory recorded before runtime recovery: {reason}"
                    )),
                    sha256: String::new(),
                };
                episode.sha256 = episode.digest().map_err(|error| {
                    OrchestratorError::Protocol(format!(
                        "reconciliation episode digest could not be calculated: {error}"
                    ))
                })?;
                self.store.open_reconciliation_episode(&episode)?
            }
        };
        let source_event_id = format!("reconciliation-inventory-{}", &inventory_sha256[..48]);
        let mut finding = ReconciliationFinding {
            schema: "harness.reconciliation-finding.v1".to_owned(),
            episode_id: episode.episode_id.clone(),
            kind: ReconciliationFindingKind::PreservedCandidate,
            source_event_id,
            // A source ID is the inventory digest, so retries must replay the
            // exact same immutable finding instead of replacing its timestamp.
            observed_at_ms: episode.opened_at_ms,
            payload: json!({
                "finding_scope": "pre_mutation_inventory",
                "inventory": inventory,
                "inventory_sha256": inventory_sha256,
                "reason": reason,
                "recovery_authority": "none",
            }),
            sha256: String::new(),
        };
        finding.sha256 = finding.digest().map_err(|error| {
            OrchestratorError::Protocol(format!(
                "reconciliation inventory finding digest could not be calculated: {error}"
            ))
        })?;
        self.store
            .record_reconciliation_finding(&finding, episode.version)
            .map_err(Into::into)
    }

    /// Records a durable, idempotent authorization receipt before the restart
    /// path may make an authority-neutral preservation change. The
    /// receipt intentionally says only that preservation was authorized from
    /// the recorded inventory; the resulting state remains independently
    /// observable in the task/agent/worktree records. Unknown custody never
    /// uses this helper to release a lease, invalidate an approval, resume an
    /// owner, or authorize a fresh attempt.
    pub(super) fn authorize_reconciliation_preservation(
        &self,
        episode: &mut ReconciliationEpisode,
        payload: Value,
    ) -> Result<(), OrchestratorError> {
        let payload_raw = serde_json::to_vec(&payload)?;
        let source_event_id = format!(
            "reconciliation-preserve-{}",
            &digest_bytes(&payload_raw)[..40]
        );
        let mut receipt = ReconciliationActionReceipt {
            schema: "harness.reconciliation-action-receipt.v1".to_owned(),
            episode_id: episode.episode_id.clone(),
            kind: ReconciliationActionKind::Preserve,
            source_event_id,
            authority_event_id: None,
            // `opened_at_ms` is immutable episode identity, making a restart
            // replay byte-identical rather than creating a second action.
            created_at_ms: episode.opened_at_ms,
            payload,
            sha256: String::new(),
        };
        receipt.sha256 = receipt.digest().map_err(|error| {
            OrchestratorError::Protocol(format!(
                "reconciliation preservation receipt digest could not be calculated: {error}"
            ))
        })?;
        *episode = self
            .store
            .record_reconciliation_action_receipt(&receipt, episode.version)?;
        Ok(())
    }

    fn reconciliation_inventory(&self, run: &RunSummary) -> Result<Value, OrchestratorError> {
        let tasks = self.store.list_tasks(&run.id)?;
        let task_inventory = tasks
            .iter()
            .map(|task| {
                let current_attempt_id = self
                    .store
                    .task_packet(&task.id)?
                    .map(|(attempt_id, _)| attempt_id.to_string());
                Ok(json!({
                    "task_id": task.id,
                    "current_attempt_id": current_attempt_id,
                    "state": task.state,
                    "attempt": task.attempt,
                    "base_sha": task.base_sha,
                    "head_sha": task.head_sha,
                    "version": task.version,
                }))
            })
            .collect::<Result<Vec<_>, OrchestratorError>>()?;
        let agent_inventory = self
            .store
            .list_agents(&run.id)?
            .into_iter()
            .map(|agent| {
                json!({
                    "agent_id": agent.id,
                    "parent_agent_id": agent.parent_agent_id,
                    "task_id": agent.task_id,
                    "role": agent.role,
                    "state": agent.state,
                    "thread_id": agent.thread_id,
                    "active_turn_id": agent.active_turn_id,
                    "version": agent.version,
                })
            })
            .collect::<Vec<_>>();
        let worktree_inventory = self
            .store
            .list_worktrees(Some(&run.id))?
            .into_iter()
            .map(|worktree| {
                json!({
                    "worktree_id": worktree.id,
                    "task_id": worktree.task_id,
                    "base_sha": worktree.base_sha,
                    "head_sha": worktree.head_sha,
                    "state": worktree.state,
                    "preserved_reason": worktree.preserved_reason,
                    "dirty": worktree.dirty,
                    "version": worktree.version,
                })
            })
            .collect::<Vec<_>>();
        let approval_inventory = self
            .store
            .list_approvals(Some(&run.id), None)?
            .into_iter()
            .map(|approval| {
                json!({
                    "approval_id": approval.id,
                    "task_id": approval.task_id,
                    "agent_id": approval.agent_id,
                    "approval_type": approval.approval_type,
                    "state": approval.state,
                    "decision": approval.decision,
                    "version": approval.version,
                })
            })
            .collect::<Vec<_>>();
        let native_subagent_activity_inventory = self
            .store
            .native_subagent_activities()?
            .into_iter()
            .filter_map(|activity| {
                self.store
                    .agent_context(&activity.parent_agent_session_id)
                    .ok()
                    .filter(|(activity_run_id, _)| activity_run_id == &run.id)
                    .map(|_| {
                        json!({
                            "parent_agent_session_id": activity.parent_agent_session_id,
                            "parent_thread_id": activity.parent_thread_id,
                            "payload": activity.payload,
                            "reconstruction": "not_attempted_without_a_receipted_action",
                        })
                    })
            })
            .collect::<Vec<_>>();
        Ok(json!({
            "schema": "harness.reconciliation-inventory.v1",
            "run": {
                "run_id": run.id,
                "state": run.state,
                "base_sha": run.base_sha,
                "integration_sha": run.integration_sha,
                "authority_digest": run.authority_digest,
                "version": run.version,
            },
            "tasks": task_inventory,
            "agents": agent_inventory,
            "worktrees": worktree_inventory,
            "approvals": approval_inventory,
            "native_subagent_activities": native_subagent_activity_inventory,
        }))
    }
}

fn trigger_for_reason(reason: &str) -> ReconciliationTrigger {
    match reason {
        "daemon restarted" => ReconciliationTrigger::DaemonRestart,
        "Codex App Server exited" => ReconciliationTrigger::AppServerLoss,
        _ => ReconciliationTrigger::ProcessLoss,
    }
}

fn trigger_name(trigger: ReconciliationTrigger) -> &'static str {
    match trigger {
        ReconciliationTrigger::DaemonRestart => "daemon_restart",
        ReconciliationTrigger::AppServerLoss => "app_server_loss",
        ReconciliationTrigger::ProcessLoss => "process_loss",
        ReconciliationTrigger::VersionTransition => "version_transition",
        ReconciliationTrigger::AccountHandoff => "account_handoff",
        ReconciliationTrigger::WorktreeMismatch => "worktree_mismatch",
        ReconciliationTrigger::UncertainCommandCompletion => "uncertain_command_completion",
    }
}

fn digest_bytes(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}

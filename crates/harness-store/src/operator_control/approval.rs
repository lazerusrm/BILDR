//! Approval-to-attention source adapter.
//!
//! It is intentionally the first adapter because pending approvals already
//! have durable controller identity and version custody. It never acts on an
//! approval; it only opens or closes the matching attention projection from an
//! approval state the core controller has already committed.

use harness_domain::{
    ApprovalSummary, AttentionCategory, AttentionItem, AttentionItemId, AttentionResolution,
    AttentionResurfacingPolicy, AttentionSeverity, AttentionSourceRef, AttentionSourceType,
    AttentionState, RiskLevel, now_ms,
};
use sha2::{Digest, Sha256};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::{Store, StoreError};

impl Store {
    /// Mirrors existing controller approvals into source-owned attention. This
    /// is an explicit pull by a control-plane read path, not a background
    /// thread poll: no schedule, runtime, or approval behavior changes here.
    pub fn refresh_approval_attention(&self) -> Result<(), StoreError> {
        for approval in self.list_approvals(None, None)? {
            let source_id = approval.id.to_string();
            let existing = self.attention_by_source(&AttentionSourceType::Approval, &source_id)?;
            match (approval.state.as_str(), existing) {
                ("pending", None) => {
                    self.upsert_source_attention(&approval_attention(&approval)?)?;
                }
                ("pending", Some(item))
                    if item.state.is_terminal()
                        && item.source.source_revision < approval.version =>
                {
                    self.upsert_source_attention(&approval_attention(&approval)?)?;
                }
                ("pending", Some(_)) => {}
                (_, Some(item)) if !item.state.is_terminal() => {
                    self.resolve_attention_from_source(
                        &item.attention_id,
                        item.version,
                        approval_terminal_state(&approval),
                        approval_resolution(&approval),
                    )?;
                }
                _ => {}
            }
        }
        Ok(())
    }
}

fn approval_attention(approval: &ApprovalSummary) -> Result<AttentionItem, StoreError> {
    let source_id = approval.id.to_string();
    let opened_at_ms = OffsetDateTime::parse(&approval.created_at, &Rfc3339)
        .map(|value| value.unix_timestamp_nanos() / 1_000_000)
        .ok()
        .and_then(|value| i64::try_from(value).ok())
        .unwrap_or_else(now_ms);
    Ok(AttentionItem {
        schema: "harness.attention-item.v1".to_owned(),
        attention_id: AttentionItemId::new(),
        repository_id: None,
        run_id: Some(approval.run_id.to_string()),
        task_id: approval.task_id.as_ref().map(ToString::to_string),
        source: AttentionSourceRef {
            source_type: AttentionSourceType::Approval,
            source_id: source_id.clone(),
            source_revision: approval.version,
        },
        category: AttentionCategory::Approval,
        severity: approval_severity(approval.risk_level),
        state: AttentionState::Open,
        title: bounded_title(&approval.approval_type),
        summary: format!(
            "A controller approval is pending for run {}. Review the source approval before work can continue.",
            approval.run_id
        ),
        option_refs: Vec::new(),
        evidence_refs: Vec::new(),
        blocked_refs: approval
            .task_id
            .as_ref()
            .map(|task| vec![task.to_string()])
            .unwrap_or_default(),
        dedupe_key: format!("approval:{source_id}"),
        opened_event_id: format!("approval:{source_id}:{}", approval.version),
        opened_at_ms,
        acknowledged_at_ms: None,
        due_at_ms: None,
        resurfacing: AttentionResurfacingPolicy {
            policy: "until_approval_authority_receipt".to_owned(),
            maximum_defer_ms: 0,
        },
        resolution: None,
        version: 1,
    })
}

fn approval_terminal_state(approval: &ApprovalSummary) -> AttentionState {
    if approval.state == "expired"
        || matches!(approval.decision.as_deref(), Some("decline" | "cancel"))
    {
        AttentionState::Declined
    } else {
        AttentionState::Resolved
    }
}

fn approval_resolution(approval: &ApprovalSummary) -> AttentionResolution {
    let resolved_at_ms = approval
        .resolved_at
        .as_deref()
        .and_then(|value| OffsetDateTime::parse(value, &Rfc3339).ok())
        .and_then(|value| i64::try_from(value.unix_timestamp_nanos() / 1_000_000).ok())
        .unwrap_or_else(now_ms);
    let outcome = format!(
        "approval_{}_{}",
        approval.state,
        approval.decision.as_deref().unwrap_or("none")
    );
    let receipt_sha256 = hex::encode(Sha256::digest(
        format!(
            "approval:{}:{}:{}:{}",
            approval.id,
            approval.state,
            approval.decision.as_deref().unwrap_or("none"),
            approval.version
        )
        .as_bytes(),
    ));
    AttentionResolution {
        outcome,
        actor_type: "controller_approval_source".to_owned(),
        actor_id: approval.id.to_string(),
        resolved_at_ms,
        authority_event_id: format!("approval:{}:{}", approval.id, approval.version),
        bound_head_sha: None,
        worktree_fingerprint: None,
        receipt_sha256,
    }
}

fn approval_severity(risk: RiskLevel) -> AttentionSeverity {
    match risk {
        RiskLevel::Low => AttentionSeverity::Info,
        RiskLevel::Medium => AttentionSeverity::Normal,
        RiskLevel::High => AttentionSeverity::High,
        RiskLevel::Critical => AttentionSeverity::Critical,
    }
}

fn bounded_title(approval_type: &str) -> String {
    let suffix = approval_type.chars().take(210).collect::<String>();
    format!("Approval required: {suffix}")
}

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;

    #[test]
    fn approval_adapter_opens_and_closes_only_from_durable_approval_state() {
        let temp = TempDir::new().expect("temp");
        let store = Store::in_memory(&temp.path().join("artifacts")).expect("store");
        store
            .connection()
            .expect("connection")
            .execute_batch(
                "INSERT INTO repositories(id,profile_id,profile_version,display_name,root_path,default_branch,state,created_at,updated_at) VALUES('repo_a','general',1,'repo','/repo-a','main','ready',1,1);
                 INSERT INTO runs(id,repository_id,title,requested_objective,mode,publication_mode,state,phase,base_ref,base_sha,authority_digest,profile_digest,requested_by,created_at,updated_at) VALUES('run_a','repo_a','run','objective','plan_and_implement','local_only','BLOCKED','planning','main','0000000000000000000000000000000000000000','0000000000000000000000000000000000000000000000000000000000000000','0000000000000000000000000000000000000000000000000000000000000000','operator',1,1);
                 INSERT INTO approvals(id,run_id,thread_id,approval_type,risk_level,request_json,request_sha256,state,created_at,version) VALUES('approval_a','run_a','thread_a','command','high','{}','0000000000000000000000000000000000000000000000000000000000000000','pending',1,1);",
            )
            .expect("fixture approval");
        store.refresh_approval_attention().expect("opens attention");
        let opened = store
            .attention_by_source(&AttentionSourceType::Approval, "approval_a")
            .expect("source lookup")
            .expect("opened attention");
        assert_eq!(opened.state, AttentionState::Open);
        store
            .connection()
            .expect("connection")
            .execute(
                "UPDATE approvals SET state='resolved',decision='accept',resolved_at=2,version=2 WHERE id='approval_a'",
                [],
            )
            .expect("resolve approval");
        store
            .refresh_approval_attention()
            .expect("closes attention");
        let closed = store
            .attention_by_source(&AttentionSourceType::Approval, "approval_a")
            .expect("source lookup")
            .expect("closed attention");
        assert_eq!(closed.state, AttentionState::Resolved);
        assert!(closed.resolution.is_some());
    }
}

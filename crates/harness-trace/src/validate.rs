use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

use crate::{
    canonical::{canonical_json, digest},
    model::{ProjectionError, SourceReceipt, TraceEdge, TraceManifest},
};

pub(crate) const BRANCH_PATH_LIMIT: usize = 256;
pub(crate) const BRANCH_DEPTH_LIMIT: usize = 4_096;
pub(crate) const BRANCH_NODE_REFERENCE_LIMIT: usize = 65_536;

/// Revalidate an externally loaded v2 manifest before it is accepted as a
/// durable trace. This deliberately checks the manifest's own closed shape
/// rather than trusting that it was produced by this crate in-process.
pub fn validate_manifest(manifest: &TraceManifest) -> Result<(), ProjectionError> {
    if manifest.schema != "harness.trace.v2" {
        return invalid_manifest("schema");
    }
    for (field, valid) in [
        (
            "trace_id",
            safe_trace_id(&manifest.trace_id),
        ),
        (
            "run_id",
            safe_controller_id(&manifest.run_id),
        ),
    ] {
        if !valid {
            return invalid_manifest(field);
        }
    }
    if manifest
        .task_attempt_id
        .as_deref()
        .is_some_and(|value| !safe_controller_id(value))
        || !is_digest(&manifest.runtime_digest)
        || !is_digest(&manifest.redaction_policy_digest)
        || !matches!(
            manifest.sensitivity.as_str(),
            "public" | "internal" | "confidential" | "restricted"
        )
        || !is_digest(&manifest.sha256)
    {
        return invalid_manifest("core");
    }
    let mut unsigned = manifest.clone();
    unsigned.sha256.clear();
    if digest(&[
        "harness.trace.manifest.v2",
        &canonical_json(&manifest_value(&unsigned)),
    ]) != manifest.sha256
    {
        return invalid_manifest("sha256");
    }
    let mut nodes = BTreeSet::new();
    let mut sources = BTreeSet::new();
    let mut raw_source_ids = BTreeSet::new();
    for node in &manifest.nodes {
        let source_keys = node
            .source_receipts
            .iter()
            .map(source_key)
            .collect::<Vec<_>>();
        let expected_id = format!(
            "n_{}",
            digest(&[
                "harness.trace.node.v2",
                &node.kind,
                &source_keys.join(","),
                &node.content_sha256,
            ])
        );
        let raw_origin = node
            .source_receipts
            .iter()
            .all(|source| matches!(source, SourceReceipt::RawEvent { .. }));
        let singleton_non_raw_origin = node.source_receipts.len() == 1
            && matches!(
                node.source_receipts.first(),
                Some(SourceReceipt::DomainEvent { .. } | SourceReceipt::Structural { .. })
            );
        let metadata_matches_origin = if raw_origin {
            node.metadata.len() == 1
                && node.metadata.get("receipt_count").and_then(Value::as_u64)
                    == Some(node.source_receipts.len() as u64)
        } else {
            singleton_non_raw_origin && node.metadata.is_empty()
        };
        if !prefixed_digest(&node.id, "n_")
            || node.id != expected_id
            || !is_digest(&node.content_sha256)
            || node.source_receipts.is_empty()
            || !matches!(
                node.kind.as_str(),
                "system_message"
                    | "developer_message"
                    | "user_message"
                    | "model_message"
                    | "reasoning_summary"
                    | "tool_request"
                    | "tool_result"
                    | "command"
                    | "file_read"
                    | "file_change"
                    | "approval_request"
                    | "approval_decision"
                    | "compaction"
                    | "subagent_spawn"
                    | "subagent_join"
                    | "validation"
                    | "finding"
                    | "operator_feedback"
                    | "outcome"
                    | "unknown_protocol"
                    | "run_lifecycle"
                    | "attempt_boundary"
                    | "runtime_restart"
            )
            || !matches!(
                node.redaction_class.as_str(),
                "none"
                    | "secret_removed"
                    | "private_reasoning_removed"
                    | "customer_data_removed"
                    | "content_withheld"
            )
            || !metadata_matches_origin
        {
            return invalid_manifest("node");
        }
        if !nodes.insert(node.id.as_str()) {
            return invalid_manifest("node.id");
        }
        for source in &node.source_receipts {
            let key = source_key(source);
            if !valid_source_receipt(source) || !sources.insert(key) {
                return invalid_manifest("source_receipt");
            }
            if let SourceReceipt::RawEvent { raw_event_id } = source {
                raw_source_ids.insert(*raw_event_id);
            }
        }
    }
    if nodes.is_empty() {
        return invalid_manifest("nodes");
    }
    let expected_range = raw_source_ids
        .first()
        .zip(raw_source_ids.last())
        .map(|(first, last)| (*first, *last));
    if manifest
        .source_event_range
        .as_ref()
        .map(|range| (range.first_id, range.last_id))
        != expected_range
    {
        return invalid_manifest("source_event_range");
    }
    let mut edges = BTreeSet::new();
    for edge in &manifest.edges {
        if edge.from == edge.to
            || !nodes.contains(edge.from.as_str())
            || !nodes.contains(edge.to.as_str())
            || !edges.insert((&edge.from, &edge.to, edge.kind))
        {
            return invalid_manifest("edge");
        }
    }
    let node_keys = manifest
        .nodes
        .iter()
        .map(|node| (node.id.clone(), node.id.clone()))
        .collect::<BTreeMap<_, _>>();
    validate_edges(&node_keys, &manifest.edges)?;
    if manifest.branches.is_empty() || manifest.branches.len() > BRANCH_PATH_LIMIT {
        return invalid_manifest("branches");
    }
    let inbound = manifest
        .edges
        .iter()
        .map(|edge| edge.to.as_str())
        .collect::<BTreeSet<_>>();
    let outbound = manifest
        .edges
        .iter()
        .map(|edge| edge.from.as_str())
        .collect::<BTreeSet<_>>();
    let paths_truncated = manifest
        .diagnostics
        .iter()
        .any(|diagnostic| diagnostic.code == "branch_path_bound_reached");
    let mut branch_ids = BTreeSet::new();
    let mut node_references = 0_usize;
    for branch in &manifest.branches {
        node_references = node_references
            .checked_add(branch.node_ids.len())
            .ok_or_else(|| ProjectionError::InvalidInput {
                field: "manifest.branch".to_owned(),
                value: "invalid".to_owned(),
            })?;
        let expected_id = format!(
            "b_{}",
            digest(&[
                "harness.trace.branch.v2",
                &branch.root_node_id,
                &branch.node_ids.join(","),
            ])
        );
        if branch.id != expected_id
            || !branch_ids.insert(branch.id.as_str())
            || !nodes.contains(branch.root_node_id.as_str())
            || !nodes.contains(branch.leaf_node_id.as_str())
            || inbound.contains(branch.root_node_id.as_str())
            || (!paths_truncated && outbound.contains(branch.leaf_node_id.as_str()))
            || branch.node_ids.is_empty()
            || branch.node_ids.len() > BRANCH_DEPTH_LIMIT
            || node_references > BRANCH_NODE_REFERENCE_LIMIT
            || branch.node_ids.first() != Some(&branch.root_node_id)
            || branch.node_ids.last() != Some(&branch.leaf_node_id)
            || branch
                .node_ids
                .iter()
                .any(|id| !nodes.contains(id.as_str()))
            || branch.node_ids.windows(2).any(|pair| {
                !manifest
                    .edges
                    .iter()
                    .any(|edge| edge.from == pair[0] && edge.to == pair[1])
            })
            || branch.metadata.len() != 1
            || branch.metadata.get("path_bound").and_then(Value::as_u64)
                != Some(BRANCH_PATH_LIMIT as u64)
        {
            return invalid_manifest("branch");
        }
    }
    for diagnostic in &manifest.diagnostics {
        let closed = matches!(
            (diagnostic.code.as_str(), diagnostic.detail.as_str()),
            (
                "ambiguous_mixed_sequence_order" | "invalid_source_sequence",
                "receipt_order_used"
            ) | ("branch_path_bound_reached", "path_limit_reached")
        );
        if !closed
            || diagnostic
                .source_receipts
                .iter()
                .any(|source| !valid_source_receipt(source))
        {
            return invalid_manifest("diagnostic");
        }
    }
    Ok(())
}

pub(crate) fn validate_edges(
    keys: &BTreeMap<String, String>,
    edges: &[TraceEdge],
) -> Result<(), ProjectionError> {
    let ids = keys.values().cloned().collect::<BTreeSet<_>>();
    let mut adjacent = BTreeMap::<String, BTreeSet<String>>::new();
    let mut inbound = ids
        .iter()
        .map(|id| (id.clone(), 0_u64))
        .collect::<BTreeMap<_, _>>();
    for edge in edges {
        if !ids.contains(&edge.from) {
            return Err(ProjectionError::UnknownRelationReceipt(edge.from.clone()));
        }
        if !ids.contains(&edge.to) {
            return Err(ProjectionError::UnknownRelationReceipt(edge.to.clone()));
        }
        if adjacent
            .entry(edge.from.clone())
            .or_default()
            .insert(edge.to.clone())
        {
            *inbound.get_mut(&edge.to).expect("validated destination") += 1;
        }
    }
    let mut ready = inbound
        .iter()
        .filter_map(|(id, count)| (*count == 0).then_some(id.clone()))
        .collect::<BTreeSet<_>>();
    let mut visited = 0_usize;
    while let Some(node) = ready.pop_first() {
        visited += 1;
        if let Some(next) = adjacent.get(&node) {
            for destination in next {
                let count = inbound.get_mut(destination).expect("validated destination");
                *count -= 1;
                if *count == 0 {
                    ready.insert(destination.clone());
                }
            }
        }
    }
    if visited == ids.len() {
        Ok(())
    } else {
        let node = inbound
            .into_iter()
            .find_map(|(id, count)| (count > 0).then_some(id))
            .expect("a cycle leaves inbound nodes");
        Err(ProjectionError::Cycle {
            from: node.clone(),
            to: node,
        })
    }
}

fn valid_source_receipt(source: &SourceReceipt) -> bool {
    match source {
        SourceReceipt::RawEvent { raw_event_id } => *raw_event_id > 0,
        SourceReceipt::DomainEvent { domain_event_id } => *domain_event_id > 0,
        SourceReceipt::Structural { receipt_id } => prefixed_digest(receipt_id, "r_"),
    }
}

fn source_key(receipt: &SourceReceipt) -> String {
    match receipt {
        SourceReceipt::RawEvent { raw_event_id } => format!("raw:{raw_event_id}"),
        SourceReceipt::DomainEvent { domain_event_id } => format!("domain:{domain_event_id}"),
        SourceReceipt::Structural { receipt_id } => format!("structural:{receipt_id}"),
    }
}

fn prefixed_digest(value: &str, prefix: &str) -> bool {
    value.strip_prefix(prefix).is_some_and(is_digest)
}

fn is_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn safe_trace_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':'))
}

fn safe_controller_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 160
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':'))
}

fn manifest_value(manifest: &TraceManifest) -> Value {
    serde_json::to_value(manifest).expect("trace manifest serializes")
}

fn invalid_manifest(field: &str) -> Result<(), ProjectionError> {
    Err(ProjectionError::InvalidInput {
        field: format!("manifest.{field}"),
        value: "invalid".to_owned(),
    })
}

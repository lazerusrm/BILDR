use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Value, json};

use crate::{
    canonical::{canonical_json, digest, payload_digest},
    model::{
        DomainEventReceipt, ProjectionDiagnostic, ProjectionError, RawEventReceipt, RelationInput,
        SourceEventRange, SourceReceipt, StructuralReceipt, TraceBranch, TraceEdge, TraceInput,
        TraceManifest, TraceNode, TraceRelationKind,
    },
    redaction::redact,
    validate::{
        BRANCH_DEPTH_LIMIT, BRANCH_NODE_REFERENCE_LIMIT, BRANCH_PATH_LIMIT, validate_edges,
    },
};

#[derive(Clone)]
struct Candidate {
    key: String,
    node: TraceNode,
    execution_scope_id: Option<String>,
    order: EventOrder,
}

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
struct EventOrder {
    sequence: Option<(usize, String)>,
    received_at: i64,
    id: i64,
}

pub fn project(input: &TraceInput) -> Result<TraceManifest, ProjectionError> {
    validate_receipts(input)?;
    if input.raw_events.is_empty()
        && input.structural_receipts.is_empty()
        && input.domain_events.is_empty()
    {
        return Err(ProjectionError::EmptyInput);
    }
    let mut diagnostics = Vec::new();
    let mut events = input.raw_events.clone();
    order_events(&mut events, &mut diagnostics);

    let mut candidates = group_raw_events(&events, &mut diagnostics);
    candidates.extend(domain_candidates(&input.domain_events));
    candidates.extend(structural_candidates(&input.structural_receipts));
    candidates.sort_by(|left, right| left.key.cmp(&right.key));

    let mut keys = BTreeMap::new();
    for candidate in &candidates {
        keys.insert(candidate.key.clone(), candidate.node.id.clone());
        for receipt in &candidate.node.source_receipts {
            keys.insert(source_key(receipt), candidate.node.id.clone());
        }
    }
    let mut edges = sequential_edges(&candidates);
    add_explicit_edges(&mut edges, &input.relations, &keys)?;
    edges.sort_by(|left, right| {
        (&left.from, &left.to, left.kind).cmp(&(&right.from, &right.to, right.kind))
    });
    edges.dedup_by(|left, right| {
        left.from == right.from && left.to == right.to && left.kind == right.kind
    });
    validate_edges(&keys, &edges)?;

    let mut nodes = candidates
        .into_iter()
        .map(|candidate| candidate.node)
        .collect::<Vec<_>>();
    nodes.sort_by(|left, right| left.id.cmp(&right.id));
    diagnostics
        .sort_by(|left, right| (&left.code, &left.detail).cmp(&(&right.code, &right.detail)));
    let branches = branches(&nodes, &edges, &mut diagnostics);
    let source_event_range =
        events
            .first()
            .zip(events.last())
            .map(|(first, last)| SourceEventRange {
                first_id: events
                    .iter()
                    .map(|event| event.id)
                    .min()
                    .unwrap_or(first.id),
                last_id: events.iter().map(|event| event.id).max().unwrap_or(last.id),
            });
    let mut manifest = TraceManifest {
        schema: "harness.trace.v2".to_owned(),
        trace_id: input.trace_id.clone(),
        run_id: input.run_id.clone(),
        task_attempt_id: input.task_attempt_id.clone(),
        source_event_range,
        runtime_digest: input.runtime_digest.clone(),
        redaction_policy_digest: input.redaction_policy_digest.clone(),
        sensitivity: input.sensitivity.clone(),
        nodes,
        edges,
        branches,
        diagnostics,
        sha256: String::new(),
    };
    manifest.sha256 = digest(&[
        "harness.trace.manifest.v2",
        &canonical_json(&manifest_value(&manifest)),
    ]);
    Ok(manifest)
}

fn validate_receipts(input: &TraceInput) -> Result<(), ProjectionError> {
    for (field, value) in [
        ("trace_id", &input.trace_id),
        ("run_id", &input.run_id),
        ("runtime_digest", &input.runtime_digest),
        ("redaction_policy_digest", &input.redaction_policy_digest),
    ] {
        if value.is_empty()
            || (!field.ends_with("digest")
                && match field {
                    "trace_id" => !safe_trace_id(value),
                    "run_id" => !safe_controller_id(value),
                    _ => true,
                })
            || (field.ends_with("digest") && !is_digest(value))
        {
            return Err(ProjectionError::InvalidInput {
                field: field.to_owned(),
                value: value.clone(),
            });
        }
    }
    if input
        .task_attempt_id
        .as_deref()
        .is_some_and(|value| !safe_controller_id(value))
    {
        return Err(ProjectionError::InvalidInput {
            field: "task_attempt_id".to_owned(),
            value: "invalid".to_owned(),
        });
    }
    if !matches!(
        input.sensitivity.as_str(),
        "public" | "internal" | "confidential" | "restricted"
    ) {
        return Err(ProjectionError::InvalidInput {
            field: "sensitivity".to_owned(),
            value: input.sensitivity.clone(),
        });
    }
    let mut raw = BTreeSet::new();
    for event in &input.raw_events {
        if event.id < 1 || !raw.insert(event.id) {
            return Err(ProjectionError::DuplicateRawReceipt(event.id));
        }
        if !is_digest(&event.payload_sha256)
            || payload_digest(&event.payload) != event.payload_sha256
        {
            return Err(ProjectionError::PayloadDigestMismatch {
                receipt: format!("raw:{}", event.id),
            });
        }
        for (field, value) in [
            ("execution_scope_id", event.execution_scope_id.as_deref()),
            ("lifecycle_group_id", event.lifecycle_group_id.as_deref()),
        ] {
            if value.is_some_and(|value| !safe_trace_id(value)) {
                return Err(ProjectionError::InvalidInput {
                    field: field.to_owned(),
                    value: "invalid".to_owned(),
                });
            }
        }
        if event.lifecycle_group_id.is_some() && event.execution_scope_id.is_none() {
            return Err(ProjectionError::InvalidInput {
                field: "lifecycle_group_id".to_owned(),
                value: "requires_execution_scope_id".to_owned(),
            });
        }
    }
    let mut structural = BTreeSet::new();
    for receipt in &input.structural_receipts {
        if !structural.insert(&receipt.id) {
            return Err(ProjectionError::DuplicateStructuralReceipt(
                receipt.id.clone(),
            ));
        }
    }
    let mut domain = BTreeSet::new();
    for event in &input.domain_events {
        if event.id < 1 || !domain.insert(event.id) || event.event_type.is_empty() {
            return Err(ProjectionError::InvalidInput {
                field: "domain_event".to_owned(),
                value: event.id.to_string(),
            });
        }
        if !is_digest(&event.payload_sha256)
            || payload_digest(&event.payload) != event.payload_sha256
        {
            return Err(ProjectionError::PayloadDigestMismatch {
                receipt: format!("domain:{}", event.id),
            });
        }
    }
    for receipt in &input.structural_receipts {
        if !safe_trace_id(&receipt.id) {
            return Err(ProjectionError::InvalidInput {
                field: "structural_receipt.id".to_owned(),
                value: "invalid".to_owned(),
            });
        }
    }
    Ok(())
}

fn is_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn safe_trace_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
}

fn safe_controller_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 160
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
}

fn order_events(events: &mut [RawEventReceipt], diagnostics: &mut Vec<ProjectionDiagnostic>) {
    let mut mixed_scopes = BTreeMap::new();
    for event in events.iter() {
        let Some(scope) = event.execution_scope_id.as_deref() else {
            continue;
        };
        let has_sequence =
            valid_sequence(event.source_sequence.as_deref().unwrap_or_default()).is_some();
        mixed_scopes
            .entry(scope.to_owned())
            .or_insert((false, false));
        let flags = mixed_scopes.get_mut(scope).expect("inserted");
        if has_sequence {
            flags.0 = true
        } else {
            flags.1 = true
        }
    }
    let ambiguous = mixed_scopes
        .into_iter()
        .filter_map(|(thread, (sequenced, unsequenced))| {
            (sequenced && unsequenced).then_some(thread)
        })
        .collect::<BTreeSet<_>>();
    for scope in &ambiguous {
        diagnostics.push(ProjectionDiagnostic {
            code: "ambiguous_mixed_sequence_order".to_owned(),
            detail: "receipt_order_used".to_owned(),
            source_receipts: events
                .iter()
                .filter(|event| event.execution_scope_id.as_deref() == Some(scope))
                .map(|event| SourceReceipt::RawEvent {
                    raw_event_id: event.id,
                })
                .collect(),
        });
    }
    events.sort_by_key(|event| (event.received_at, event.id));
}

fn event_order(event: &RawEventReceipt) -> EventOrder {
    let sequence = event.source_sequence.as_deref().and_then(valid_sequence);
    EventOrder {
        sequence,
        received_at: event.received_at,
        id: event.id,
    }
}

fn valid_sequence(value: &str) -> Option<(usize, String)> {
    let normalized = value.strip_prefix('+').unwrap_or(value);
    (!normalized.is_empty()
        && normalized.bytes().all(|byte| byte.is_ascii_digit())
        && (normalized == "0" || !normalized.starts_with('0')))
    .then(|| (normalized.len(), normalized.to_owned()))
}

fn group_raw_events(
    events: &[RawEventReceipt],
    diagnostics: &mut Vec<ProjectionDiagnostic>,
) -> Vec<Candidate> {
    let mut grouped = BTreeMap::<GroupKey, Vec<&RawEventReceipt>>::new();
    for event in events {
        let key = event.lifecycle_group_id.as_ref().map_or_else(
            || GroupKey::Raw(event.id),
            |group| {
                GroupKey::Lifecycle(
                    event
                        .execution_scope_id
                        .clone()
                        .expect("validated lifecycle scope"),
                    group.clone(),
                )
            },
        );
        if event.source_sequence.is_some()
            && valid_sequence(event.source_sequence.as_deref().unwrap_or_default()).is_none()
        {
            diagnostics.push(ProjectionDiagnostic {
                code: "invalid_source_sequence".to_owned(),
                detail: "receipt_order_used".to_owned(),
                source_receipts: vec![SourceReceipt::RawEvent {
                    raw_event_id: event.id,
                }],
            });
        }
        grouped.entry(key).or_default().push(event);
    }
    grouped
        .into_iter()
        .map(|(group_key, mut receipts)| {
            let mixed = receipts.iter().any(|receipt| {
                valid_sequence(receipt.source_sequence.as_deref().unwrap_or_default()).is_none()
            }) && receipts.iter().any(|receipt| {
                valid_sequence(receipt.source_sequence.as_deref().unwrap_or_default()).is_some()
            });
            if mixed {
                receipts.sort_by_key(|receipt| (receipt.received_at, receipt.id));
            } else {
                receipts.sort_by_key(|receipt| event_order(receipt));
            }
            raw_candidate(group_key.key(), &receipts)
        })
        .collect()
}

#[derive(Eq, Ord, PartialEq, PartialOrd)]
enum GroupKey {
    Raw(i64),
    Lifecycle(String, String),
}

impl GroupKey {
    fn key(&self) -> String {
        match self {
            Self::Raw(id) => format!("raw:{id}"),
            Self::Lifecycle(scope, group) => format!(
                "lifecycle:{}",
                digest(&["harness.trace.lifecycle-group.v2", scope, group])
            ),
        }
    }
}

fn raw_candidate(key: String, receipts: &[&RawEventReceipt]) -> Candidate {
    let latest = receipts
        .iter()
        .max_by_key(|receipt| (item_state_rank(receipt), event_order(receipt)))
        .expect("non-empty receipt group");
    let (payload, redaction_class) = redact(&latest.payload, &latest.redaction_class);
    let kind = raw_kind(latest);
    let source_receipts = receipts
        .iter()
        .map(|receipt| SourceReceipt::RawEvent {
            raw_event_id: receipt.id,
        })
        .collect::<Vec<_>>();
    let source_ids = source_receipts
        .iter()
        .map(source_key)
        .collect::<Vec<_>>()
        .join(",");
    let content_sha256 = digest(&["harness.trace.content.v2", &kind, &canonical_json(&payload)]);
    let id = format!(
        "n_{}",
        digest(&["harness.trace.node.v2", &kind, &source_ids, &content_sha256,])
    );
    let mut metadata = BTreeMap::new();
    metadata.insert("receipt_count".to_owned(), json!(receipts.len()));
    Candidate {
        key,
        node: TraceNode {
            id,
            kind,
            content_sha256,
            source_receipts,
            redaction_class,
            timestamp_ms: Some(latest.received_at),
            metadata,
        },
        execution_scope_id: latest.execution_scope_id.clone(),
        order: event_order(latest),
    }
}

fn structural_candidates(receipts: &[StructuralReceipt]) -> Vec<Candidate> {
    receipts
        .iter()
        .map(|receipt| {
            let kind = structural_kind(&receipt.kind).to_owned();
            let source_receipts = vec![SourceReceipt::Structural {
                receipt_id: format!(
                    "r_{}",
                    digest(&["harness.trace.structural-receipt.v2", &receipt.id])
                ),
            }];
            let content_sha256 = digest(&[
                "harness.trace.content.v2",
                &kind,
                &canonical_json(&json!({"kind": structural_kind(&receipt.kind)})),
            ]);
            let id = format!(
                "n_{}",
                digest(&[
                    "harness.trace.node.v2",
                    &kind,
                    &source_key(&source_receipts[0]),
                    &content_sha256,
                ])
            );
            Candidate {
                key: format!("structural:{}", receipt.id),
                node: TraceNode {
                    id,
                    kind,
                    content_sha256,
                    source_receipts,
                    redaction_class: "none".to_owned(),
                    timestamp_ms: receipt.occurred_at,
                    metadata: BTreeMap::new(),
                },
                execution_scope_id: None,
                order: EventOrder {
                    sequence: None,
                    received_at: receipt.occurred_at.unwrap_or_default(),
                    id: 0,
                },
            }
        })
        .collect()
}

fn domain_candidates(receipts: &[DomainEventReceipt]) -> Vec<Candidate> {
    receipts
        .iter()
        .map(|receipt| {
            let (payload, redaction_class) = redact(&receipt.payload, &receipt.redaction_class);
            let kind = structural_kind(&receipt.event_type).to_owned();
            let content_sha256 =
                digest(&["harness.trace.content.v2", &kind, &canonical_json(&payload)]);
            let id = format!(
                "n_{}",
                digest(&[
                    "harness.trace.node.v2",
                    &kind,
                    &format!("domain:{}", receipt.id),
                    &content_sha256
                ])
            );
            Candidate {
                key: format!("domain:{}", receipt.id),
                node: TraceNode {
                    id,
                    kind,
                    content_sha256,
                    source_receipts: vec![SourceReceipt::DomainEvent {
                        domain_event_id: receipt.id,
                    }],
                    redaction_class,
                    timestamp_ms: Some(receipt.occurred_at),
                    metadata: BTreeMap::new(),
                },
                execution_scope_id: None,
                order: EventOrder {
                    sequence: None,
                    received_at: receipt.occurred_at,
                    id: receipt.id,
                },
            }
        })
        .collect()
}

fn raw_kind(event: &RawEventReceipt) -> String {
    match event.payload.pointer("/item/type").and_then(Value::as_str) {
        Some("agentMessage") => "model_message",
        Some("reasoning") => "reasoning_summary",
        Some("commandExecution") => "command",
        Some("fileChange") => "file_change",
        Some("contextCompaction") => "compaction",
        Some("subAgentActivity") => {
            match event.payload.pointer("/item/kind").and_then(Value::as_str) {
                Some("completed" | "failed" | "interrupted") => "subagent_join",
                _ => "subagent_spawn",
            }
        }
        Some("toolResult") => "tool_result",
        Some("toolCall" | "mcpToolCall" | "dynamicToolCall") => "tool_request",
        Some("reviewFinding") => "finding",
        Some(_) | None => match event.method.as_str() {
            "thread/compacted" => "compaction",
            "turn/start" => "user_message",
            _ => "unknown_protocol",
        },
    }
    .to_owned()
}

fn structural_kind(kind: &str) -> &str {
    match kind {
        "run.lifecycle.transitioned" => "run_lifecycle",
        "retry" | "remediation" => "attempt_boundary",
        "restart" => "runtime_restart",
        other
            if matches!(
                other,
                "run_lifecycle" | "attempt_boundary" | "runtime_restart"
            ) =>
        {
            other
        }
        _ => "unknown_protocol",
    }
}

fn item_state_rank(event: &RawEventReceipt) -> u8 {
    match event.method.as_str() {
        "item/completed" => 2,
        "item/started" => 1,
        _ => 0,
    }
}

fn sequential_edges(candidates: &[Candidate]) -> Vec<TraceEdge> {
    let mut by_scope = BTreeMap::<&str, Vec<&Candidate>>::new();
    for candidate in candidates {
        let Some(scope) = candidate.execution_scope_id.as_deref() else {
            continue;
        };
        by_scope.entry(scope).or_default().push(candidate);
    }
    let mut result = Vec::new();
    for entries in by_scope.values_mut() {
        // Source sequences have ordering meaning only inside one thread/turn
        // chain. A mixed chain is deliberately receipt-ordered instead of
        // relying on `Option` ordering.
        if entries
            .iter()
            .all(|candidate| candidate.order.sequence.is_some())
        {
            entries.sort_by_key(|candidate| candidate.order.clone());
        } else {
            entries.sort_by_key(|candidate| (candidate.order.received_at, candidate.order.id));
        }
        for pair in entries.windows(2) {
            result.push(TraceEdge {
                from: pair[0].node.id.clone(),
                to: pair[1].node.id.clone(),
                kind: TraceRelationKind::Next,
            });
        }
    }
    result
}

fn add_explicit_edges(
    edges: &mut Vec<TraceEdge>,
    relations: &[RelationInput],
    keys: &BTreeMap<String, String>,
) -> Result<(), ProjectionError> {
    for relation in relations {
        let from = keys
            .get(&relation.from)
            .ok_or_else(|| ProjectionError::UnknownRelationReceipt(relation.from.clone()))?;
        let to = keys
            .get(&relation.to)
            .ok_or_else(|| ProjectionError::UnknownRelationReceipt(relation.to.clone()))?;
        edges.push(TraceEdge {
            from: from.clone(),
            to: to.clone(),
            kind: relation.kind,
        });
    }
    Ok(())
}

fn branches(
    nodes: &[TraceNode],
    edges: &[TraceEdge],
    diagnostics: &mut Vec<ProjectionDiagnostic>,
) -> Vec<TraceBranch> {
    let ids = nodes
        .iter()
        .map(|node| node.id.as_str())
        .collect::<BTreeSet<_>>();
    let inbound = edges
        .iter()
        .map(|edge| edge.to.as_str())
        .collect::<BTreeSet<_>>();
    let mut adjacent = BTreeMap::<String, Vec<String>>::new();
    for edge in edges {
        adjacent
            .entry(edge.from.clone())
            .or_default()
            .push(edge.to.clone());
    }
    for next in adjacent.values_mut() {
        next.sort_unstable();
        next.dedup();
    }
    let roots = ids
        .iter()
        .filter(|id| !inbound.contains(**id))
        .copied()
        .collect::<Vec<_>>();
    let mut branches = Vec::new();
    let mut node_references = 0_usize;
    let mut truncated = false;
    for root in roots {
        let remaining_paths = BRANCH_PATH_LIMIT.saturating_sub(branches.len());
        let remaining_references = BRANCH_NODE_REFERENCE_LIMIT.saturating_sub(node_references);
        if remaining_paths == 0 || remaining_references == 0 {
            truncated = true;
            break;
        }
        let (paths, root_truncated) = enumerate_paths(
            root,
            &adjacent,
            remaining_paths,
            BRANCH_DEPTH_LIMIT,
            remaining_references,
        );
        truncated |= root_truncated;
        for node_ids in paths {
            node_references += node_ids.len();
            let leaf = node_ids.last().cloned().unwrap_or_else(|| root.to_owned());
            branches.push(TraceBranch {
                id: format!(
                    "b_{}",
                    digest(&["harness.trace.branch.v2", root, &node_ids.join(",")])
                ),
                root_node_id: root.to_owned(),
                leaf_node_id: leaf,
                node_ids,
                metadata: BTreeMap::from([("path_bound".to_owned(), json!(BRANCH_PATH_LIMIT))]),
            });
        }
    }
    if truncated {
        diagnostics.push(ProjectionDiagnostic {
            code: "branch_path_bound_reached".to_owned(),
            detail: "path_limit_reached".to_owned(),
            source_receipts: Vec::new(),
        });
    }
    branches.sort_by(|left, right| left.id.cmp(&right.id));
    branches
}

fn enumerate_paths(
    root: &str,
    adjacent: &BTreeMap<String, Vec<String>>,
    path_bound: usize,
    depth_bound: usize,
    node_reference_bound: usize,
) -> (Vec<Vec<String>>, bool) {
    assert!(path_bound > 0 && depth_bound > 0 && node_reference_bound > 0);
    let mut paths = Vec::new();
    let mut node_references = 0_usize;
    let mut path = vec![root.to_owned()];
    let mut stack = vec![(root.to_owned(), 0_usize)];
    let mut truncated = false;
    while !stack.is_empty() {
        if paths.len() >= path_bound {
            truncated = true;
            break;
        }
        let (node, offset) = stack.last_mut().expect("non-empty traversal stack");
        let next = adjacent.get(node).map(Vec::as_slice).unwrap_or_default();
        if node_references + path.len() > node_reference_bound
            || (node_references + path.len() == node_reference_bound && !next.is_empty())
        {
            let remaining = node_reference_bound - node_references;
            if remaining > 0 {
                paths.push(path[..remaining.min(path.len())].to_vec());
            }
            truncated = true;
            break;
        } else if next.is_empty() {
            node_references += path.len();
            paths.push(path.clone());
            stack.pop();
            path.pop();
        } else if path.len() >= depth_bound {
            node_references += path.len();
            paths.push(path.clone());
            truncated = true;
            stack.pop();
            path.pop();
        } else if *offset >= next.len() {
            stack.pop();
            path.pop();
        } else {
            let destination = next[*offset].clone();
            *offset += 1;
            path.push(destination.clone());
            stack.push((destination, 0));
        }
    }
    (paths, truncated)
}

fn source_key(receipt: &SourceReceipt) -> String {
    match receipt {
        SourceReceipt::RawEvent { raw_event_id } => format!("raw:{raw_event_id}"),
        SourceReceipt::DomainEvent { domain_event_id } => format!("domain:{domain_event_id}"),
        SourceReceipt::Structural { receipt_id } => format!("structural:{receipt_id}"),
    }
}

fn manifest_value(manifest: &TraceManifest) -> Value {
    serde_json::to_value(manifest).expect("trace manifest serializes")
}

#[cfg(test)]
mod scale_tests {
    use super::*;
    use crate::validate::validate_manifest;

    fn resign(manifest: &mut TraceManifest) {
        manifest.sha256.clear();
        manifest.sha256 = digest(&[
            "harness.trace.manifest.v2",
            &canonical_json(&manifest_value(manifest)),
        ]);
    }

    fn manifest_fixture() -> TraceManifest {
        let payload = json!({"event": "fixture"});
        project(&TraceInput {
            trace_id: "trace-fixture".to_owned(),
            run_id: "run-fixture".to_owned(),
            task_attempt_id: None,
            runtime_digest: "a".repeat(64),
            redaction_policy_digest: "b".repeat(64),
            sensitivity: "internal".to_owned(),
            raw_events: vec![RawEventReceipt {
                id: 1,
                execution_scope_id: None,
                lifecycle_group_id: None,
                thread_id: None,
                turn_id: None,
                direction: "inbound".to_owned(),
                method: "fixture".to_owned(),
                request_id: None,
                received_at: 1,
                payload: payload.clone(),
                payload_sha256: payload_digest(&payload),
                source_sequence: None,
                redaction_class: "none".to_owned(),
            }],
            domain_events: Vec::new(),
            structural_receipts: Vec::new(),
            relations: Vec::new(),
        })
        .unwrap()
    }

    #[test]
    fn trace_manifest_keeps_160_character_controller_identities() {
        let mut manifest = manifest_fixture();
        manifest.run_id = "r".repeat(160);
        manifest.task_attempt_id = Some("a".repeat(160));
        resign(&mut manifest);
        validate_manifest(&manifest).expect("controller identities remain traceable");
    }

    #[test]
    fn manifest_validator_rejects_digest_and_endpoint_mutations() {
        let manifest = manifest_fixture();
        validate_manifest(&manifest).unwrap();

        let mut mutated_digest = manifest.clone();
        mutated_digest.runtime_digest = "c".repeat(64);
        assert!(validate_manifest(&mutated_digest).is_err());

        let mut endpoint = manifest;
        endpoint.nodes[0].source_receipts = vec![SourceReceipt::RawEvent { raw_event_id: 0 }];
        resign(&mut endpoint);
        assert!(validate_manifest(&endpoint).is_err());

        let mut open_node = manifest_fixture();
        open_node.nodes[0].kind = "customer supplied kind".to_owned();
        resign(&mut open_node);
        assert!(validate_manifest(&open_node).is_err());

        let mut forged_node = manifest_fixture();
        forged_node.nodes[0].id = format!("n_{}", "f".repeat(64));
        forged_node.branches[0].root_node_id = forged_node.nodes[0].id.clone();
        forged_node.branches[0].leaf_node_id = forged_node.nodes[0].id.clone();
        forged_node.branches[0].node_ids[0] = forged_node.nodes[0].id.clone();
        forged_node.branches[0].id = format!(
            "b_{}",
            digest(&[
                "harness.trace.branch.v2",
                &forged_node.branches[0].root_node_id,
                &forged_node.branches[0].node_ids.join(","),
            ])
        );
        resign(&mut forged_node);
        assert!(validate_manifest(&forged_node).is_err());

        let mut wrong_count = manifest_fixture();
        wrong_count.nodes[0]
            .metadata
            .insert("receipt_count".to_owned(), json!(2));
        resign(&mut wrong_count);
        assert!(validate_manifest(&wrong_count).is_err());

        let mut wrong_range = manifest_fixture();
        wrong_range.source_event_range = Some(SourceEventRange {
            first_id: 1,
            last_id: 2,
        });
        resign(&mut wrong_range);
        assert!(validate_manifest(&wrong_range).is_err());

        let mut duplicate_branch = manifest_fixture();
        duplicate_branch
            .branches
            .push(duplicate_branch.branches[0].clone());
        resign(&mut duplicate_branch);
        assert!(validate_manifest(&duplicate_branch).is_err());

        let mut forged_branch = manifest_fixture();
        forged_branch.branches[0].id = format!("b_{}", "f".repeat(64));
        resign(&mut forged_branch);
        assert!(validate_manifest(&forged_branch).is_err());

        let mut forged_bound = manifest_fixture();
        forged_bound.branches[0]
            .metadata
            .insert("path_bound".to_owned(), json!(1));
        resign(&mut forged_bound);
        assert!(validate_manifest(&forged_bound).is_err());

        let mut open_diagnostic = manifest_fixture();
        open_diagnostic.diagnostics.push(ProjectionDiagnostic {
            code: "free_text".to_owned(),
            detail: "customer supplied detail".to_owned(),
            source_receipts: Vec::new(),
        });
        resign(&mut open_diagnostic);
        assert!(validate_manifest(&open_diagnostic).is_err());
    }

    #[test]
    fn long_linear_graph_validation_and_branching_are_iterative() {
        let count = 50_000;
        let keys = (0..count)
            .map(|index| (format!("source:{index}"), format!("node:{index}")))
            .collect::<BTreeMap<_, _>>();
        let edges = (1..count)
            .map(|index| TraceEdge {
                from: format!("node:{}", index - 1),
                to: format!("node:{index}"),
                kind: TraceRelationKind::Next,
            })
            .collect::<Vec<_>>();
        validate_edges(&keys, &edges).unwrap();

        let adjacent = edges.iter().fold(BTreeMap::new(), |mut map, edge| {
            map.entry(edge.from.clone())
                .or_insert_with(Vec::new)
                .push(edge.to.clone());
            map
        });
        let (paths, truncated) = enumerate_paths(
            "node:0",
            &adjacent,
            BRANCH_PATH_LIMIT,
            BRANCH_DEPTH_LIMIT,
            BRANCH_NODE_REFERENCE_LIMIT,
        );
        assert!(truncated);
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0].len(), BRANCH_DEPTH_LIMIT);

        let mut cyclic = edges;
        cyclic.push(TraceEdge {
            from: format!("node:{}", count - 1),
            to: "node:0".to_owned(),
            kind: TraceRelationKind::Next,
        });
        assert!(matches!(
            validate_edges(&keys, &cyclic),
            Err(ProjectionError::Cycle { .. })
        ));
    }

    #[test]
    fn branch_references_are_bounded_globally_across_many_roots() {
        let root_count = 500;
        let tail_count = 1_000;
        let mut nodes = (0..root_count)
            .map(|index| test_node(format!("root:{index}")))
            .collect::<Vec<_>>();
        nodes.extend((0..tail_count).map(|index| test_node(format!("tail:{index}"))));
        let mut edges = (0..root_count)
            .map(|index| TraceEdge {
                from: format!("root:{index}"),
                to: "tail:0".to_owned(),
                kind: TraceRelationKind::Next,
            })
            .collect::<Vec<_>>();
        edges.extend((1..tail_count).map(|index| TraceEdge {
            from: format!("tail:{}", index - 1),
            to: format!("tail:{index}"),
            kind: TraceRelationKind::Next,
        }));
        let mut diagnostics = Vec::new();

        let projected = branches(&nodes, &edges, &mut diagnostics);

        assert!(projected.len() <= BRANCH_PATH_LIMIT);
        assert!(
            projected
                .iter()
                .map(|branch| branch.node_ids.len())
                .sum::<usize>()
                <= BRANCH_NODE_REFERENCE_LIMIT
        );
        assert!(projected.len() < root_count);
        assert_eq!(
            diagnostics
                .iter()
                .filter(|diagnostic| diagnostic.code == "branch_path_bound_reached")
                .count(),
            1
        );
    }

    fn test_node(id: String) -> TraceNode {
        TraceNode {
            id,
            kind: "unknown_protocol".to_owned(),
            content_sha256: "0".repeat(64),
            source_receipts: Vec::new(),
            redaction_class: "content_withheld".to_owned(),
            timestamp_ms: None,
            metadata: BTreeMap::new(),
        }
    }
}

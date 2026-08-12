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
};

#[derive(Clone)]
struct Candidate {
    key: String,
    node: TraceNode,
    thread_id: String,
    turn_id: Option<String>,
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
            || (!field.ends_with("digest") && !safe_id(value))
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
        .is_some_and(|value| !safe_id(value))
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
        if !safe_id(&receipt.id) {
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

fn safe_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
}

fn order_events(events: &mut [RawEventReceipt], diagnostics: &mut Vec<ProjectionDiagnostic>) {
    let mut mixed_threads = BTreeMap::new();
    for event in events.iter() {
        let thread = event.thread_id.as_deref().unwrap_or("unbound");
        let has_sequence =
            valid_sequence(event.source_sequence.as_deref().unwrap_or_default()).is_some();
        mixed_threads
            .entry(thread.to_owned())
            .or_insert((false, false));
        let flags = mixed_threads.get_mut(thread).expect("inserted");
        if has_sequence {
            flags.0 = true
        } else {
            flags.1 = true
        }
    }
    let ambiguous = mixed_threads
        .into_iter()
        .filter_map(|(thread, (sequenced, unsequenced))| {
            (sequenced && unsequenced).then_some(thread)
        })
        .collect::<BTreeSet<_>>();
    for thread in &ambiguous {
        diagnostics.push(ProjectionDiagnostic {
            code: "ambiguous_mixed_sequence_order".to_owned(),
            detail: "receipt_order_used".to_owned(),
            source_receipts: events
                .iter()
                .filter(|event| event.thread_id.as_deref().unwrap_or("unbound") == thread)
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
        let thread = event.thread_id.as_deref().unwrap_or("unbound");
        let item = event
            .payload
            .pointer("/item/id")
            .and_then(Value::as_str)
            .filter(|_| matches!(event.method.as_str(), "item/started" | "item/completed"));
        let key = item.map_or_else(
            || GroupKey::Raw(event.id),
            |item| GroupKey::Item(thread.to_owned(), item.to_owned()),
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
    Item(String, String),
}

impl GroupKey {
    fn key(&self) -> String {
        match self {
            Self::Raw(id) => format!("raw:{id}"),
            // The key only drives internal relation lookup; digest the tuple
            // rather than joining untrusted identifiers with delimiters.
            Self::Item(thread, item) => format!(
                "item:{}",
                digest(&["harness.trace.item-group.v2", thread, item])
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
        thread_id: latest
            .thread_id
            .clone()
            .unwrap_or_else(|| "unbound".to_owned()),
        turn_id: latest.turn_id.clone(),
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
                // Structural receipts have no trustworthy turn chain; their
                // causality is represented only by explicit durable edges.
                thread_id: "structural:opaque".to_owned(),
                turn_id: None,
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
                // Domain events likewise require an explicit relation rather
                // than an inferred cross-aggregate sequence.
                thread_id: format!("domain:receipt:{}", receipt.id),
                turn_id: None,
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
    let mut by_turn = BTreeMap::<(&str, Option<&str>), Vec<&Candidate>>::new();
    for candidate in candidates {
        if candidate.thread_id.starts_with("structural:") {
            continue;
        }
        by_turn
            .entry((&candidate.thread_id, candidate.turn_id.as_deref()))
            .or_default()
            .push(candidate);
    }
    let mut result = Vec::new();
    for entries in by_turn.values_mut() {
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

fn validate_edges(
    keys: &BTreeMap<String, String>,
    edges: &[TraceEdge],
) -> Result<(), ProjectionError> {
    let ids = keys.values().cloned().collect::<BTreeSet<_>>();
    let mut adjacent = BTreeMap::<String, Vec<String>>::new();
    for edge in edges {
        if !ids.contains(&edge.from) {
            return Err(ProjectionError::UnknownRelationReceipt(edge.from.clone()));
        }
        if !ids.contains(&edge.to) {
            return Err(ProjectionError::UnknownRelationReceipt(edge.to.clone()));
        }
        adjacent
            .entry(edge.from.clone())
            .or_default()
            .push(edge.to.clone());
    }
    for node in &ids {
        let mut visiting = BTreeSet::new();
        if reaches(node, node, &adjacent, &mut visiting, true) {
            return Err(ProjectionError::Cycle {
                from: node.clone(),
                to: node.clone(),
            });
        }
    }
    Ok(())
}

fn reaches(
    origin: &str,
    current: &str,
    adjacent: &BTreeMap<String, Vec<String>>,
    visiting: &mut BTreeSet<String>,
    first: bool,
) -> bool {
    if !first && current == origin {
        return true;
    }
    if !visiting.insert(current.to_owned()) {
        return false;
    }
    let result = adjacent
        .get(current)
        .into_iter()
        .flatten()
        .any(|next| reaches(origin, next, adjacent, visiting, false));
    visiting.remove(current);
    result
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
    let mut branches = ids
        .iter()
        .filter(|id| !inbound.contains(**id))
        .flat_map(|root| {
            let (paths, truncated) = enumerate_paths(root, edges, 256);
            if truncated {
                diagnostics.push(ProjectionDiagnostic {
                    code: "branch_path_bound_reached".to_owned(),
                    detail: "path_limit_reached".to_owned(),
                    source_receipts: Vec::new(),
                });
            }
            paths.into_iter().map(move |node_ids| {
                let leaf = node_ids
                    .last()
                    .cloned()
                    .unwrap_or_else(|| (*root).to_owned());
                TraceBranch {
                    id: format!(
                        "b_{}",
                        digest(&["harness.trace.branch.v2", root, &node_ids.join(",")])
                    ),
                    root_node_id: (*root).to_owned(),
                    leaf_node_id: leaf,
                    node_ids,
                    metadata: BTreeMap::from([("path_bound".to_owned(), json!(256))]),
                }
            })
        })
        .collect::<Vec<_>>();
    branches.sort_by(|left, right| left.id.cmp(&right.id));
    branches
}

fn enumerate_paths(root: &str, edges: &[TraceEdge], bound: usize) -> (Vec<Vec<String>>, bool) {
    fn walk(
        current: &str,
        edges: &[TraceEdge],
        path: &mut Vec<String>,
        paths: &mut Vec<Vec<String>>,
        bound: usize,
    ) {
        if paths.len() >= bound {
            return;
        }
        let mut next = edges
            .iter()
            .filter(|edge| edge.from == current)
            .map(|edge| edge.to.as_str())
            .collect::<Vec<_>>();
        next.sort_unstable();
        next.dedup();
        if next.is_empty() {
            paths.push(path.clone());
            return;
        }
        for value in next {
            path.push(value.to_owned());
            walk(value, edges, path, paths, bound);
            path.pop();
        }
    }
    let mut paths = Vec::new();
    walk(root, edges, &mut vec![root.to_owned()], &mut paths, bound);
    let truncated = paths.len() >= bound;
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

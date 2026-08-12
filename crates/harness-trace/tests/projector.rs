use harness_trace::{
    DomainEventReceipt, RawEventReceipt, RelationInput, SourceReceipt, StructuralReceipt,
    TraceInput, TraceRelationKind, project,
};
use serde_json::json;
use sha2::{Digest, Sha256};

fn receipt(id: i64, sequence: &str, method: &str, item: serde_json::Value) -> RawEventReceipt {
    let payload = json!({"threadId": "thread-a", "turnId": "turn-a", "item": item});
    RawEventReceipt {
        id,
        thread_id: Some("thread-a".to_owned()),
        turn_id: Some("turn-a".to_owned()),
        direction: "inbound".to_owned(),
        method: method.to_owned(),
        request_id: None,
        received_at: id,
        payload_sha256: raw_digest(&payload),
        payload,
        source_sequence: Some(sequence.to_owned()),
        redaction_class: "none".to_owned(),
    }
}

fn raw_digest(value: &serde_json::Value) -> String {
    hex::encode(Sha256::digest(
        serde_json::to_string(value).unwrap().as_bytes(),
    ))
}

fn input(raw_events: Vec<RawEventReceipt>) -> TraceInput {
    TraceInput {
        trace_id: "trace-a".to_owned(),
        run_id: "run-a".to_owned(),
        task_attempt_id: Some("attempt-a".to_owned()),
        runtime_digest: "a".repeat(64),
        redaction_policy_digest: "b".repeat(64),
        sensitivity: "internal".to_owned(),
        raw_events,
        domain_events: vec![],
        structural_receipts: vec![
            StructuralReceipt {
                id: "compact-1".to_owned(),
                kind: "attempt_boundary".to_owned(),
                occurred_at: Some(7),
                metadata: Default::default(),
            },
            StructuralReceipt {
                id: "restart-1".to_owned(),
                kind: "restart".to_owned(),
                occurred_at: Some(8),
                metadata: Default::default(),
            },
        ],
        relations: vec![
            RelationInput {
                from: "raw:2".to_owned(),
                to: "raw:3".to_owned(),
                kind: TraceRelationKind::ToolResultOf,
            },
            RelationInput {
                from: "raw:4".to_owned(),
                to: "raw:5".to_owned(),
                kind: TraceRelationKind::SpawnedBy,
            },
            RelationInput {
                from: "raw:5".to_owned(),
                to: "structural:compact-1".to_owned(),
                kind: TraceRelationKind::CompactedFrom,
            },
            RelationInput {
                from: "structural:compact-1".to_owned(),
                to: "structural:restart-1".to_owned(),
                kind: TraceRelationKind::RetryOf,
            },
        ],
    }
}

#[test]
fn projection_is_stable_across_reordered_receipts_and_captures_explicit_branches() {
    let events = vec![
        receipt(
            1,
            "1",
            "item/started",
            json!({"id": "message", "type": "agentMessage", "text": "hello"}),
        ),
        receipt(
            2,
            "2",
            "item/completed",
            json!({"id": "call", "type": "toolCall", "callId": "call-1"}),
        ),
        receipt(
            3,
            "3",
            "item/completed",
            json!({"id": "result", "type": "toolResult", "callId": "call-1"}),
        ),
        receipt(
            4,
            "4",
            "item/started",
            json!({"id": "spawn", "type": "subAgentActivity", "kind": "started", "agentThreadId": "child"}),
        ),
        receipt(
            5,
            "5",
            "item/completed",
            json!({"id": "join", "type": "subAgentActivity", "kind": "completed", "agentThreadId": "child"}),
        ),
    ];
    let first = project(&input(events.clone())).unwrap();
    let mut reversed = events;
    reversed.reverse();
    let second = project(&input(reversed)).unwrap();

    assert_eq!(first.sha256, second.sha256);
    assert!(
        first
            .edges
            .iter()
            .any(|edge| edge.kind == TraceRelationKind::ToolResultOf)
    );
    assert!(
        first
            .edges
            .iter()
            .any(|edge| edge.kind == TraceRelationKind::SpawnedBy)
    );
    assert!(
        first
            .edges
            .iter()
            .any(|edge| edge.kind == TraceRelationKind::CompactedFrom)
    );
    assert!(
        first
            .edges
            .iter()
            .any(|edge| edge.kind == TraceRelationKind::RetryOf)
    );
    assert!(
        first
            .branches
            .iter()
            .all(|branch| !branch.node_ids.is_empty())
    );
}

#[test]
fn item_lifecycle_deduplicates_and_private_reasoning_is_never_hashed_from_content() {
    let mut reasoning = receipt(
        6,
        "6",
        "item/completed",
        json!({"id": "thought", "type": "reasoning", "content": ["private chain"], "summary": ["also private"]}),
    );
    reasoning.redaction_class = "none".to_owned();
    let mut started = receipt(
        1,
        "1",
        "item/started",
        json!({"id": "same", "type": "agentMessage", "text": "before"}),
    );
    let completed = receipt(
        2,
        "2",
        "item/completed",
        json!({"id": "same", "type": "agentMessage", "text": "after"}),
    );
    started.received_at = 20;
    let mut trace_input = input(vec![started, completed, reasoning]);
    trace_input.relations.clear();
    let manifest = project(&trace_input).unwrap();

    let lifecycle = manifest
        .nodes
        .iter()
        .find(|node| node.metadata.get("receipt_count") == Some(&json!(2)))
        .unwrap();
    assert_eq!(lifecycle.source_receipts.len(), 2);
    let thought = manifest
        .nodes
        .iter()
        .find(|node| node.kind == "reasoning_summary")
        .unwrap();
    assert_eq!(thought.redaction_class, "private_reasoning_removed");
    let alternate = receipt(
        6,
        "6",
        "item/completed",
        json!({"id": "thought", "type": "reasoning", "content": ["different private chain"], "summary": ["different too"]}),
    );
    let mut alternate_input = input(vec![alternate]);
    alternate_input.relations.clear();
    let alternate_manifest = project(&alternate_input).unwrap();
    let alternate_thought = alternate_manifest
        .nodes
        .iter()
        .find(|node| node.kind == "reasoning_summary")
        .unwrap();
    assert_eq!(thought.content_sha256, alternate_thought.content_sha256);
    assert!(
        thought
            .source_receipts
            .contains(&SourceReceipt::RawEvent { raw_event_id: 6 })
    );
}

#[test]
fn rejects_cycles_and_unknown_relation_receipts() {
    let events = vec![receipt(
        1,
        "1",
        "turn/start",
        json!({"id": "one", "type": "agentMessage"}),
    )];
    let mut cyclic = input(events.clone());
    cyclic.relations.push(RelationInput {
        from: "structural:restart-1".to_owned(),
        to: "raw:1".to_owned(),
        kind: TraceRelationKind::DerivedFrom,
    });
    cyclic.relations.push(RelationInput {
        from: "raw:1".to_owned(),
        to: "structural:restart-1".to_owned(),
        kind: TraceRelationKind::DerivedFrom,
    });
    assert!(project(&cyclic).is_err());

    let mut unknown = input(events);
    unknown.relations = vec![RelationInput {
        from: "raw:404".to_owned(),
        to: "raw:1".to_owned(),
        kind: TraceRelationKind::DerivedFrom,
    }];
    assert!(project(&unknown).is_err());
}

#[test]
fn domain_provenance_and_non_none_redaction_are_truthful() {
    let mut secret = receipt(
        1,
        "1",
        "item/completed",
        json!({"id": "secret", "type": "agentMessage", "token": "Bearer top-secret"}),
    );
    secret.redaction_class = "secret_removed".to_owned();
    let mut trace_input = input(vec![secret]);
    trace_input.relations.clear();
    let payload = json!({"prior_state": "CREATED", "next_state": "PREPARING"});
    trace_input.domain_events = vec![DomainEventReceipt {
        id: 9,
        event_type: "run.lifecycle.transitioned".to_owned(),
        occurred_at: 9,
        payload_sha256: raw_digest(&payload),
        payload,
        redaction_class: "content_withheld".to_owned(),
    }];
    let manifest = project(&trace_input).unwrap();
    assert!(
        manifest
            .nodes
            .iter()
            .any(|node| node.kind == "run_lifecycle"
                && node.source_receipts == vec![SourceReceipt::DomainEvent { domain_event_id: 9 }])
    );
    assert!(
        manifest
            .nodes
            .iter()
            .any(|node| node.redaction_class == "secret_removed")
    );
}

#[test]
fn domain_only_receipts_use_store_compatible_payload_digests() {
    let payload = json!({"prior_state": "CREATED", "next_state": "PREPARING"});
    let manifest = project(&TraceInput {
        trace_id: "trace-domain".to_owned(),
        run_id: "run-domain".to_owned(),
        task_attempt_id: None,
        runtime_digest: "a".repeat(64),
        redaction_policy_digest: "b".repeat(64),
        sensitivity: "internal".to_owned(),
        raw_events: vec![],
        domain_events: vec![DomainEventReceipt {
            id: 17,
            event_type: "run.lifecycle.transitioned".to_owned(),
            occurred_at: 17,
            // This is exactly `sha256(serde_json::to_string(payload))`, the
            // Store raw-event convention rather than a trace-specific digest.
            payload_sha256: raw_digest(&payload),
            payload,
            redaction_class: "none".to_owned(),
        }],
        structural_receipts: vec![],
        relations: vec![],
    })
    .unwrap();
    assert!(manifest.source_event_range.is_none());
    assert_eq!(manifest.nodes.len(), 1);
    assert_eq!(
        manifest.nodes[0].source_receipts,
        vec![SourceReceipt::DomainEvent {
            domain_event_id: 17
        }]
    );
}

#[test]
fn arbitrary_ingress_values_are_neither_emitted_nor_hashed() {
    fn adversarial_input(secret: &str, email: &str) -> TraceInput {
        let payload = json!({
            "item": {"type": secret, "token": secret, "customer_email": email},
            "private-key": secret,
            "free_text": secret
        });
        let domain_payload = json!({"event": secret, "customer": email, "password": secret});
        TraceInput {
            trace_id: "trace-safe".to_owned(),
            run_id: "run-safe".to_owned(),
            task_attempt_id: Some("attempt:01JSAFE".to_owned()),
            runtime_digest: "a".repeat(64),
            redaction_policy_digest: "b".repeat(64),
            sensitivity: "restricted".to_owned(),
            raw_events: vec![RawEventReceipt {
                id: 1,
                thread_id: Some(email.to_owned()),
                turn_id: Some(secret.to_owned()),
                direction: email.to_owned(),
                method: secret.to_owned(),
                request_id: Some(secret.to_owned()),
                received_at: 1,
                payload_sha256: raw_digest(&payload),
                payload,
                source_sequence: Some("1".to_owned()),
                redaction_class: "none".to_owned(),
            }],
            domain_events: vec![DomainEventReceipt {
                id: 2,
                event_type: secret.to_owned(),
                occurred_at: 2,
                payload_sha256: raw_digest(&domain_payload),
                payload: domain_payload,
                redaction_class: "none".to_owned(),
            }],
            structural_receipts: vec![StructuralReceipt {
                id: "agent:01JSAFE".to_owned(),
                kind: secret.to_owned(),
                occurred_at: Some(3),
                metadata: [(
                    "arbitrary_metadata".to_owned(),
                    json!({"email": email, "secret": secret}),
                )]
                .into_iter()
                .collect(),
            }],
            relations: vec![],
        }
    }

    let first = project(&adversarial_input(
        "Bearer top-secret",
        "person@example.test",
    ))
    .unwrap();
    let second = project(&adversarial_input(
        "password=other-secret",
        "other@example.test",
    ))
    .unwrap();
    let serialized = serde_json::to_string(&first).unwrap();
    for forbidden in [
        "top-secret",
        "person@example.test",
        "Bearer",
        "private-key",
        "free_text",
    ] {
        assert!(
            !serialized.contains(forbidden),
            "manifest leaked {forbidden}"
        );
    }
    // Same trusted receipt identities/timestamps and safe shape produce the
    // same manifest digest despite mutating every untrusted ingress field.
    assert_eq!(first.sha256, second.sha256);
    assert!(
        first.nodes.iter().all(
            |node| node.redaction_class == "content_withheld" || node.redaction_class == "none"
        )
    );
}

#[test]
fn rejects_invalid_ids_and_non_lowercase_receipt_digests() {
    let mut invalid_raw = receipt(1, "1", "turn/start", json!({"id": "one"}));
    invalid_raw.id = 0;
    let mut raw_input = input(vec![invalid_raw]);
    raw_input.relations.clear();
    assert!(project(&raw_input).is_err());

    let payload = json!({"ok": true});
    let invalid_domain = TraceInput {
        raw_events: vec![],
        domain_events: vec![DomainEventReceipt {
            id: 0,
            event_type: "run.lifecycle.transitioned".to_owned(),
            occurred_at: 1,
            payload: payload.clone(),
            payload_sha256: "A".repeat(64),
            redaction_class: "none".to_owned(),
        }],
        structural_receipts: vec![],
        relations: vec![],
        ..input(vec![])
    };
    assert!(project(&invalid_domain).is_err());

    let invalid_structural = TraceInput {
        raw_events: vec![],
        domain_events: vec![DomainEventReceipt {
            id: 1,
            event_type: "run.lifecycle.transitioned".to_owned(),
            occurred_at: 1,
            payload_sha256: raw_digest(&payload),
            payload,
            redaction_class: "none".to_owned(),
        }],
        structural_receipts: vec![StructuralReceipt {
            id: "not safe/identifier".to_owned(),
            kind: "restart".to_owned(),
            occurred_at: None,
            metadata: Default::default(),
        }],
        relations: vec![],
        ..input(vec![])
    };
    assert!(project(&invalid_structural).is_err());
}

#[test]
fn delimiter_containing_item_ids_do_not_collide_in_lifecycle_grouping() {
    let mut first = receipt(
        1,
        "1",
        "item/completed",
        json!({"id": "b:c", "type": "agentMessage"}),
    );
    first.thread_id = Some("a".to_owned());
    let mut second = receipt(
        2,
        "2",
        "item/completed",
        json!({"id": "c", "type": "agentMessage"}),
    );
    second.thread_id = Some("a:b".to_owned());
    let mut trace_input = input(vec![first, second]);
    trace_input.structural_receipts.clear();
    trace_input.relations.clear();
    let manifest = project(&trace_input).unwrap();
    assert_eq!(manifest.nodes.len(), 2);
    assert!(
        manifest
            .nodes
            .iter()
            .all(|node| node.source_receipts.len() == 1)
    );
}

#[test]
fn independent_domain_and_structural_receipts_remain_unconnected_without_relations() {
    let first_payload = json!({"prior_state": "CREATED"});
    let second_payload = json!({"prior_state": "PREPARING"});
    let manifest = project(&TraceInput {
        trace_id: "trace-independent".to_owned(),
        run_id: "run-independent".to_owned(),
        task_attempt_id: None,
        runtime_digest: "a".repeat(64),
        redaction_policy_digest: "b".repeat(64),
        sensitivity: "internal".to_owned(),
        raw_events: vec![],
        domain_events: vec![
            DomainEventReceipt {
                id: 1,
                event_type: "run.lifecycle.transitioned".to_owned(),
                occurred_at: 1,
                payload_sha256: raw_digest(&first_payload),
                payload: first_payload,
                redaction_class: "none".to_owned(),
            },
            DomainEventReceipt {
                id: 2,
                event_type: "run.lifecycle.transitioned".to_owned(),
                occurred_at: 2,
                payload_sha256: raw_digest(&second_payload),
                payload: second_payload,
                redaction_class: "none".to_owned(),
            },
        ],
        structural_receipts: vec![
            StructuralReceipt {
                id: "run:01JONE".to_owned(),
                kind: "run.lifecycle.transitioned".to_owned(),
                occurred_at: Some(1),
                metadata: Default::default(),
            },
            StructuralReceipt {
                id: "agent:01JTWO".to_owned(),
                kind: "restart".to_owned(),
                occurred_at: Some(2),
                metadata: Default::default(),
            },
        ],
        relations: vec![],
    })
    .unwrap();
    assert!(manifest.edges.is_empty());
    assert_eq!(manifest.branches.len(), manifest.nodes.len());
}

use harness_domain::{
    CodexRuntimeStatus, ComponentStatus, ImprovementMode, ImprovementRuntimeStatus, RuntimeStatus,
    SchedulerStatus,
};
use serde_json::{Value, json};

const RUNTIME_STATUS_FIXTURE: &str =
    include_str!("../../../examples/openapi/runtime-status.example.json");

#[test]
fn runtime_status_wire_record_matches_the_checked_fixture() {
    let status = RuntimeStatus {
        daemon: ComponentStatus {
            state: "ready".to_owned(),
            detail: None,
        },
        codex: CodexRuntimeStatus {
            state: "ready".to_owned(),
            detail: None,
            version: Some("1.2.3".to_owned()),
            required_version: Some("1.2.3".to_owned()),
            protocol_schema_sha256: Some("a".repeat(64)),
            schema_match: true,
            native_multi_agent: true,
            native_multi_agent_feature: Some("multi_agent".to_owned()),
            pid: Some(4242),
            restart_count: 2,
        },
        database: ComponentStatus {
            state: "ready".to_owned(),
            detail: None,
        },
        scheduler: SchedulerStatus {
            paused: false,
            active_total: 1,
            max_total: 4,
            active_mutable: 1,
            max_mutable: 2,
            active_verifiers: 0,
            max_verifiers: 2,
            queued_tasks: 3,
        },
        self_improvement: ImprovementRuntimeStatus {
            configured_mode: ImprovementMode::ObserveOnly,
            effective_mode: ImprovementMode::ObserveOnly,
            anchor_sha256: "b".repeat(64),
            configured_anchor_sha256: "b".repeat(64),
            anchor_match: true,
            observation_enabled: true,
            candidate_generation_enabled: false,
            candidate_execution_enabled: false,
            detail: None,
        },
    };
    let fixture: Value = serde_json::from_str(RUNTIME_STATUS_FIXTURE).unwrap();

    assert_eq!(serde_json::to_value(&status).unwrap(), fixture);
    let decoded: RuntimeStatus = serde_json::from_value(fixture.clone()).unwrap();
    assert_eq!(serde_json::to_value(decoded).unwrap(), fixture);
    assert_eq!(
        fixture["self_improvement"]["effective_mode"],
        json!("observe_only")
    );
}

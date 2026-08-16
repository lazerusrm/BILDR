import { describe, expect, it } from "vitest";
import { createElement } from "react";
import { renderToStaticMarkup } from "react-dom/server";

import { AttentionCenter, ExternalConditionDetail, InvestigationDetail } from "./AttentionCenter";
import type { ExternalCondition, InvestigationArtifact } from "../types";

const investigation: InvestigationArtifact = {
  schema: "harness.investigation-artifact.v1",
  artifact_id: "investigation_a",
  run_id: "run_a",
  task_id: "task_a",
  attempt_id: "attempt_a",
  question: "Why is the verifier blocked?",
  scope: {
    owned_read_paths: ["crates/harness-store/**"],
    forbidden_paths: [".git/objects/**"],
    time_budget_ms: 60_000,
    token_budget: 8_000,
  },
  base_sha: "a".repeat(40),
  repository_state_digest: "b".repeat(64),
  methods: ["read source"],
  sources: ["fixture:source"],
  findings: [{
    finding_id: "finding_a",
    classification: "confirmed",
    summary: "The source revision does not match the evidence revision.",
    confidence_milli: 950,
    evidence_refs: ["fixture:source"],
    affected_refs: ["task:task_a"],
    risk: "high",
    limitations: [],
  }],
  recommendations: [{
    recommendation_id: "recommendation_a",
    summary: "Use the controller-owned revision.",
    required_authority: "controller",
    evidence_refs: ["fixture:source"],
    alternatives: [],
    risk: "high",
    next_verification: "Run the exact schema check.",
  }],
  decision_inventory: [{
    decision_id: "decision_a",
    question: "Which revision is authoritative?",
    state: "open",
    options: ["controller", "legacy"],
    evidence_refs: ["fixture:source"],
    impact: "Blocks publication.",
    recommended_option: "controller",
    required_actor: "operator",
    blocking_refs: ["task:task_a"],
    independent_work_can_continue: false,
  }],
  limitations: ["No hosted replay was available."],
  rejected_hypotheses: ["The source changed the schema."],
  sensitivity: "internal",
  artifact_refs: ["artifact:source"],
  created_at_ms: 1,
  sha256: "c".repeat(64),
};

const condition: ExternalCondition = {
  schema: "harness.external-condition.v1",
  condition_id: "condition_a",
  owner_type: "task",
  owner_id: "task_a",
  adapter: "ci_check",
  source_id: "check:required",
  spec: {},
  state: "open",
  sequence: 1,
  poll_policy: { initial_ms: 15_000, maximum_ms: 300_000, deadline_ms: null },
  source_identity_digest: "d".repeat(64),
  last_observation: {
    schema: "harness.condition-observation.v1",
    observation_id: "observation_a",
    condition_id: "condition_a",
    source_event_id: "check:event:1",
    sequence: 1,
    observed_at_ms: 2,
    state: "open",
    payload: {},
    sha256: "e".repeat(64),
  },
  version: 2,
  opened_at_ms: 1,
  updated_at_ms: 2,
  sha256: "f".repeat(64),
};

describe("AttentionCenter", () => {
  it("states that acknowledgement cannot resolve or resume work", () => {
    const markup = renderToStaticMarkup(createElement(AttentionCenter));
    expect(markup).toContain("Attention &amp; return view");
    expect(markup).toContain("Loading attention");
    expect(markup).toContain("Artifacts are evidence records");
    expect(markup).toContain("does not poll a provider, wake work, or execute a result");
    expect(markup).toContain("Material progress");
    expect(markup).toContain("Liveness episodes");
    expect(markup).toContain("Delivery mirror");
    expect(markup).toContain("No presence preference has been configured for this session.");
    expect(markup).toContain("Loading current delivery health");
    expect(markup).toContain("Run topology");
  });

  it("renders bounded evidence and explicit condition history without an action control", () => {
    const markup = renderToStaticMarkup(createElement(InvestigationDetail, {
      artifactId: investigation.artifact_id,
      detail: {
        artifactId: investigation.artifact_id,
        state: "loaded",
        artifact: investigation,
      },
      onLoad: () => undefined,
    }));
    expect(markup).toContain("Selected investigation");
    expect(markup).toContain("The source revision does not match the evidence revision.");
    expect(markup).toContain("Decision inventory");
    expect(markup).toContain("Blocks independent work");

    const conditionMarkup = renderToStaticMarkup(createElement(ExternalConditionDetail, {
      conditionId: condition.condition_id,
      detail: {
        conditionId: condition.condition_id,
        state: "loaded",
        condition,
      },
      history: {
        conditionId: condition.condition_id,
        state: "loaded",
        observations: [condition.last_observation!],
      },
      onLoadDetail: () => undefined,
      onLoadHistory: () => undefined,
    }));
    expect(conditionMarkup).toContain("Selected external condition");
    expect(conditionMarkup).toContain("Observation history");
    expect(conditionMarkup).toContain("check:event:1");
    expect(conditionMarkup).not.toContain("resume");
    expect(conditionMarkup).not.toContain("execute");
  });
});

//! W3C-shaped correlation context and explicit causal-link validation.
//!
//! This module does not trust protocol input by default. A caller must name an
//! allowlisted controller/runtime boundary before an inbound `traceparent` is
//! permitted to join controller-owned correlation. Cross-component causality
//! is recorded explicitly; no fan-in, fan-out, or ancestry is inferred.

use std::collections::{BTreeMap, BTreeSet};

use harness_domain::{CorrelationLink, OperatorControlError, TraceContext};
use thiserror::Error;

const MAX_CAUSAL_LINKS: usize = 8_192;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CorrelationPolicy {
    trusted_components: BTreeSet<String>,
}

impl CorrelationPolicy {
    pub fn new(
        trusted_components: impl IntoIterator<Item = String>,
    ) -> Result<Self, CorrelationError> {
        let mut components = BTreeSet::new();
        for component in trusted_components {
            validate_component(&component)?;
            components.insert(component);
        }
        Ok(Self {
            trusted_components: components,
        })
    }

    #[must_use]
    pub fn permits(&self, component: &str) -> bool {
        self.trusted_components.contains(component)
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum CorrelationError {
    #[error("invalid W3C traceparent: {0}")]
    InvalidTraceparent(&'static str),
    #[error("untrusted correlation boundary: {0}")]
    UntrustedBoundary(String),
    #[error("invalid correlation component: {0}")]
    InvalidComponent(String),
    #[error(transparent)]
    InvalidLink(#[from] OperatorControlError),
    #[error("correlation causal graph contains a cycle at {0}")]
    CausalCycle(String),
    #[error("too many correlation links")]
    LinkLimit,
}

/// Parses only a W3C v00 `traceparent` whose identifiers satisfy the
/// controller's fixed-size trace contract. The caller supplies the current
/// span when recording a child context; inbound span ids become its parent.
pub fn parse_traceparent(traceparent: &str) -> Result<TraceContext, CorrelationError> {
    let parts = traceparent.split('-').collect::<Vec<_>>();
    if parts.len() != 4 || parts[0] != "00" {
        return Err(CorrelationError::InvalidTraceparent("unsupported version"));
    }
    let [_, trace_id, parent_span_id, flags] = parts.as_slice() else {
        return Err(CorrelationError::InvalidTraceparent("invalid shape"));
    };
    if !is_lower_hex(trace_id, 32)
        || !is_lower_hex(parent_span_id, 16)
        || !is_lower_hex(flags, 2)
        || trace_id.bytes().all(|byte| byte == b'0')
        || parent_span_id.bytes().all(|byte| byte == b'0')
    {
        return Err(CorrelationError::InvalidTraceparent("invalid identifiers"));
    }
    Ok(TraceContext {
        trace_id: (*trace_id).to_owned(),
        span_id: (*parent_span_id).to_owned(),
        parent_span_id: None,
    })
}

/// Bridges an inbound context only through a configured controller/runtime
/// boundary. External callers remain untrusted until a specific adapter uses
/// this function with a policy that names it.
pub fn accept_inbound_traceparent(
    policy: &CorrelationPolicy,
    component: &str,
    traceparent: &str,
) -> Result<TraceContext, CorrelationError> {
    validate_component(component)?;
    if !policy.permits(component) {
        return Err(CorrelationError::UntrustedBoundary(component.to_owned()));
    }
    parse_traceparent(traceparent)
}

/// Derives a child context without accepting a caller-controlled trace id.
pub fn child_context(
    parent: &TraceContext,
    child_span_id: impl Into<String>,
) -> Result<TraceContext, CorrelationError> {
    parent.validate()?;
    let child = TraceContext {
        trace_id: parent.trace_id.clone(),
        span_id: child_span_id.into(),
        parent_span_id: Some(parent.span_id.clone()),
    };
    child.validate()?;
    Ok(child)
}

/// Validates a bounded explicit causal DAG. Inbound/outbound multiplicity is
/// intentionally allowed; the producer must persist every fan-in/fan-out link
/// rather than relying on a viewer to infer it from adjacency or timestamps.
pub fn validate_causal_links(links: &[CorrelationLink]) -> Result<(), CorrelationError> {
    if links.len() > MAX_CAUSAL_LINKS {
        return Err(CorrelationError::LinkLimit);
    }
    let mut adjacent = BTreeMap::<String, BTreeSet<String>>::new();
    let mut nodes = BTreeSet::new();
    let mut unique = BTreeSet::new();
    for link in links {
        link.validate()?;
        let from = format!(
            "{}:{}:{}",
            link.trace.trace_id, link.from_kind, link.from_id
        );
        let to = format!("{}:{}:{}", link.trace.trace_id, link.to_kind, link.to_id);
        if !unique.insert((
            link.trace.trace_id.as_str(),
            from.clone(),
            to.clone(),
            link.relation.as_str(),
        )) {
            return Err(CorrelationError::InvalidTraceparent(
                "duplicate causal link",
            ));
        }
        nodes.insert(from.clone());
        nodes.insert(to.clone());
        adjacent.entry(from).or_default().insert(to);
    }

    let mut visiting = BTreeSet::new();
    let mut visited = BTreeSet::new();
    for node in nodes {
        visit(&node, &adjacent, &mut visiting, &mut visited)?;
    }
    Ok(())
}

fn visit(
    node: &str,
    adjacent: &BTreeMap<String, BTreeSet<String>>,
    visiting: &mut BTreeSet<String>,
    visited: &mut BTreeSet<String>,
) -> Result<(), CorrelationError> {
    if visited.contains(node) {
        return Ok(());
    }
    if !visiting.insert(node.to_owned()) {
        return Err(CorrelationError::CausalCycle(node.to_owned()));
    }
    if let Some(next) = adjacent.get(node) {
        for child in next {
            visit(child, adjacent, visiting, visited)?;
        }
    }
    visiting.remove(node);
    visited.insert(node.to_owned());
    Ok(())
}

fn validate_component(component: &str) -> Result<(), CorrelationError> {
    if component.is_empty()
        || component.len() > 160
        || !component
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
    {
        return Err(CorrelationError::InvalidComponent(component.to_owned()));
    }
    Ok(())
}

fn is_lower_hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use harness_domain::{CorrelationLinkId, TraceContext};

    use super::*;

    fn link(from: &str, to: &str) -> CorrelationLink {
        CorrelationLink {
            schema: "harness.correlation-link.v1".to_owned(),
            link_id: CorrelationLinkId::new(),
            trace: TraceContext {
                trace_id: "a".repeat(32),
                span_id: "b".repeat(16),
                parent_span_id: None,
            },
            from_kind: "domain_event".to_owned(),
            from_id: from.to_owned(),
            to_kind: "attention".to_owned(),
            to_id: to.to_owned(),
            relation: "derived_from".to_owned(),
            created_at_ms: 1,
        }
    }

    #[test]
    fn only_allowlisted_components_can_bridge_inbound_context() {
        let policy =
            CorrelationPolicy::new(["controller.appserver".to_owned()]).expect("valid policy");
        let traceparent = format!("00-{}-{}-01", "a".repeat(32), "b".repeat(16));
        assert!(accept_inbound_traceparent(&policy, "controller.appserver", &traceparent).is_ok());
        assert!(matches!(
            accept_inbound_traceparent(&policy, "external.webhook", &traceparent),
            Err(CorrelationError::UntrustedBoundary(_))
        ));
        assert!(
            parse_traceparent("00-00000000000000000000000000000000-1111111111111111-01").is_err()
        );
    }

    #[test]
    fn explicit_causal_cycles_are_rejected_without_inference() {
        let first = link("event_a", "item_b");
        let second = CorrelationLink {
            from_kind: "attention".to_owned(),
            from_id: "item_b".to_owned(),
            to_kind: "domain_event".to_owned(),
            to_id: "event_a".to_owned(),
            ..link("ignored", "ignored")
        };
        assert!(matches!(
            validate_causal_links(&[first, second]),
            Err(CorrelationError::CausalCycle(_))
        ));
    }
}

//! Bounded, factual run-topology compiler.
//!
//! The table form is canonical. It derives only durable run/task/attempt,
//! agent, worktree, and dependency relationships; no graph layout, inference,
//! or mutable operation is attached to this projection.

use harness_domain::{TopologyEdge, TopologyNode, TopologySnapshot, TopologySnapshotId};
use rusqlite::{OptionalExtension, TransactionBehavior, params};
use sha2::{Digest, Sha256};

use crate::{Store, StoreError};

const TOPOLOGY_NODE_LIMIT: usize = 500;
const TOPOLOGY_EDGE_LIMIT: usize = 2_000;

impl Store {
    pub fn run_topology(&self, run_id: &str) -> Result<TopologySnapshot, StoreError> {
        validate_id(run_id, "topology run id")?;
        let mut connection = self.connection()?;
        let transaction = connection.transaction_with_behavior(TransactionBehavior::Immediate)?;
        let exists: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM runs WHERE id=?1)",
            [run_id],
            |row| row.get(0),
        )?;
        if !exists {
            return Err(StoreError::NotFound(format!("run {run_id}")));
        }
        let source_cursor = non_negative(transaction.query_row(
            "SELECT coalesce(max(id),0) FROM domain_events WHERE run_id=?1",
            [run_id],
            |row| row.get::<_, i64>(0),
        )?)?;
        if let Some((raw, stored_digest)) = transaction
            .query_row(
                "SELECT payload_json,payload_sha256 FROM topology_snapshots WHERE run_id=?1 AND source_cursor=?2",
                params![run_id, to_i64(source_cursor)?],
                |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)),
            )
            .optional()?
        {
            let snapshot = checked_topology_row(raw, stored_digest)?;
            transaction.commit()?;
            return Ok(snapshot);
        }
        let (nodes, edges) = compile_topology(&transaction, run_id)?;
        let mut snapshot = TopologySnapshot {
            schema: "harness.run-topology.v1".to_owned(),
            snapshot_id: TopologySnapshotId::new(),
            run_id: run_id.to_owned(),
            nodes,
            edges,
            source_cursor,
            sha256: String::new(),
        };
        snapshot.sha256 = snapshot
            .digest()
            .map_err(|error| StoreError::Validation(error.to_string()))?;
        snapshot
            .validate()
            .map_err(|error| StoreError::Validation(error.to_string()))?;
        let raw = serde_json::to_string(&snapshot)?;
        transaction.execute(
            "INSERT INTO topology_snapshots(id,run_id,source_cursor,payload_json,payload_sha256,created_at) VALUES(?1,?2,?3,?4,?5,?6)",
            params![
                snapshot.snapshot_id.as_str(), run_id, to_i64(source_cursor)?, raw,
                digest(&serde_json::to_string(&snapshot)?), harness_domain::now_ms(),
            ],
        )?;
        transaction.commit()?;
        Ok(snapshot)
    }
}

fn compile_topology(
    transaction: &rusqlite::Transaction<'_>,
    run_id: &str,
) -> Result<(Vec<TopologyNode>, Vec<TopologyEdge>), StoreError> {
    let run_node = format!("run:{run_id}");
    let mut nodes = vec![TopologyNode {
        id: run_node.clone(),
        kind: "run".to_owned(),
        source_ref: format!("run:{run_id}"),
    }];
    let mut edges = Vec::new();
    let tasks = {
        let mut statement = transaction.prepare(
            "SELECT id,external_task_id FROM tasks WHERE run_id=?1 ORDER BY external_task_id,id LIMIT ?2",
        )?;
        statement
            .query_map(params![run_id, (TOPOLOGY_NODE_LIMIT - 1) as i64], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    for (task_id, external_task_id) in &tasks {
        let task_node = format!("task:{task_id}");
        nodes.push(TopologyNode {
            id: task_node.clone(),
            kind: "task".to_owned(),
            source_ref: format!("task:{external_task_id}"),
        });
        edges.push(TopologyEdge {
            from: run_node.clone(),
            to: task_node,
            kind: "contains".to_owned(),
            source_ref: format!("task:{external_task_id}"),
        });
    }
    let mut add_node = |id: String, kind: &str, source_ref: String| -> Result<(), StoreError> {
        if nodes.len() >= TOPOLOGY_NODE_LIMIT {
            return Err(StoreError::Validation(
                "run topology exceeds its node limit".to_owned(),
            ));
        }
        nodes.push(TopologyNode {
            id,
            kind: kind.to_owned(),
            source_ref,
        });
        Ok(())
    };
    for (task_id, external_task_id) in &tasks {
        let task_node = format!("task:{task_id}");
        let attempts = {
            let mut statement = transaction.prepare(
                "SELECT id FROM task_attempts WHERE task_id=?1 ORDER BY attempt_number,id",
            )?;
            statement
                .query_map([task_id], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?
        };
        for attempt_id in attempts {
            let attempt_node = format!("attempt:{attempt_id}");
            add_node(
                attempt_node.clone(),
                "attempt",
                format!("attempt:{attempt_id}"),
            )?;
            add_edge(
                &mut edges,
                task_node.clone(),
                attempt_node.clone(),
                "owns",
                format!("task:{external_task_id}"),
            )?;
            let agents = {
                let mut statement = transaction.prepare(
                    "SELECT id FROM agent_sessions WHERE task_attempt_id=?1 ORDER BY coalesce(started_at,0),id",
                )?;
                statement
                    .query_map([&attempt_id], |row| row.get::<_, String>(0))?
                    .collect::<Result<Vec<_>, _>>()?
            };
            for agent_id in agents {
                let node = format!("agent:{agent_id}");
                add_node(node.clone(), "agent", format!("agent:{agent_id}"))?;
                add_edge(
                    &mut edges,
                    attempt_node.clone(),
                    node,
                    "owns",
                    format!("attempt:{attempt_id}"),
                )?;
            }
            let worktrees = {
                let mut statement = transaction.prepare(
                    "SELECT id FROM worktrees WHERE task_attempt_id=?1 ORDER BY created_at,id",
                )?;
                statement
                    .query_map([&attempt_id], |row| row.get::<_, String>(0))?
                    .collect::<Result<Vec<_>, _>>()?
            };
            for worktree_id in worktrees {
                let node = format!("worktree:{worktree_id}");
                add_node(node.clone(), "worktree", format!("worktree:{worktree_id}"))?;
                add_edge(
                    &mut edges,
                    attempt_node.clone(),
                    node,
                    "owns",
                    format!("attempt:{attempt_id}"),
                )?;
            }
        }
    }
    let dependencies = {
        let mut statement = transaction.prepare(
            "SELECT d.task_id,d.depends_on_task_id FROM task_dependencies d JOIN tasks t ON t.id=d.task_id WHERE t.run_id=?1 ORDER BY d.task_id,d.depends_on_task_id",
        )?;
        statement
            .query_map([run_id], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?
    };
    for (task_id, dependency_id) in dependencies {
        add_edge(
            &mut edges,
            format!("task:{task_id}"),
            format!("task:{dependency_id}"),
            "depends_on",
            format!("dependency:{task_id}:{dependency_id}"),
        )?;
    }
    Ok((nodes, edges))
}

fn add_edge(
    edges: &mut Vec<TopologyEdge>,
    from: String,
    to: String,
    kind: &str,
    source_ref: String,
) -> Result<(), StoreError> {
    if edges.len() >= TOPOLOGY_EDGE_LIMIT {
        return Err(StoreError::Validation(
            "run topology exceeds its edge limit".to_owned(),
        ));
    }
    edges.push(TopologyEdge {
        from,
        to,
        kind: kind.to_owned(),
        source_ref,
    });
    Ok(())
}

fn checked_topology_row(raw: String, payload_sha256: String) -> rusqlite::Result<TopologySnapshot> {
    if digest(&raw) != payload_sha256 {
        return Err(rusqlite::Error::FromSqlConversionFailure(
            0,
            rusqlite::types::Type::Text,
            "topology payload integrity check failed".into(),
        ));
    }
    let snapshot: TopologySnapshot = serde_json::from_str(&raw).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })?;
    snapshot.validate().map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })?;
    Ok(snapshot)
}

fn validate_id(value: &str, field: &str) -> Result<(), StoreError> {
    if value.is_empty()
        || value.len() > 160
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'-'))
    {
        return Err(StoreError::Validation(format!(
            "{field} must be a bounded path-safe identifier"
        )));
    }
    Ok(())
}
fn non_negative(value: i64) -> Result<u64, StoreError> {
    u64::try_from(value)
        .map_err(|_| StoreError::Validation("topology source cursor is negative".to_owned()))
}
fn to_i64(value: u64) -> Result<i64, StoreError> {
    i64::try_from(value).map_err(|_| {
        StoreError::Validation("topology source cursor exceeds SQLite integer range".to_owned())
    })
}
fn digest(raw: &str) -> String {
    hex::encode(Sha256::digest(raw.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn topology_is_bounded_digest_checked_and_reused_at_the_same_cursor() {
        let temp = TempDir::new().expect("temp");
        let store = Store::in_memory(&temp.path().join("artifacts")).expect("store");
        store.connection().unwrap().execute_batch(
            "INSERT INTO repositories(id,profile_id,profile_version,display_name,root_path,default_branch,state,created_at,updated_at) VALUES('repo-a','general',1,'repo','/repo','main','ready',1,1);
             INSERT INTO runs(id,repository_id,title,requested_objective,mode,publication_mode,state,phase,base_ref,base_sha,authority_digest,profile_digest,requested_by,created_at,updated_at) VALUES('run-a','repo-a','run','objective','plan_and_implement','local_only','BLOCKED','planning','main','0000000000000000000000000000000000000000','0000000000000000000000000000000000000000000000000000000000000000','0000000000000000000000000000000000000000000000000000000000000000','operator',1,1);
             INSERT INTO run_plan_revisions(id,run_id,revision,plan_json,plan_sha256,state,created_at) VALUES('plan-a','run-a',1,'{}','0000000000000000000000000000000000000000000000000000000000000000','proposed',1);
             INSERT INTO tasks(id,run_id,plan_revision_id,external_task_id,title,objective,priority,owner_profile,reviewer_profile,state,created_at,updated_at) VALUES('task-a','run-a','plan-a','task-a','task','objective','normal','general','general','BLOCKED',1,1);
             INSERT INTO task_attempts(id,task_id,attempt_number,state,task_packet_json,task_packet_sha256,base_sha,requested_model_route,created_at,updated_at) VALUES('attempt-a','task-a',1,'CREATED','{}','0000000000000000000000000000000000000000000000000000000000000000','0000000000000000000000000000000000000000','same',1,1);
             INSERT INTO agent_sessions(id,run_id,task_attempt_id,runtime_kind,role,requested_model,requested_reasoning_effort,sandbox_mode,approval_policy,cwd,state,started_at) VALUES('agent-a','run-a','attempt-a','test','WORKER','gpt-5.6-terra','medium','READ_ONLY','never','/tmp','COMPLETED',1);"
        ).unwrap();
        let first = store.run_topology("run-a").expect("topology");
        assert!(first.nodes.iter().any(|node| node.kind == "agent"));
        assert!(first.edges.iter().any(|edge| edge.kind == "owns"));
        assert_eq!(store.run_topology("run-a").unwrap(), first);

        let long_run = "r".repeat(160);
        let long_attempt = "a".repeat(160);
        let long_task = "t".repeat(160);
        let long_dependency = "d".repeat(160);
        store
            .connection()
            .unwrap()
            .execute_batch(&format!(
                "INSERT INTO runs(id,repository_id,title,requested_objective,mode,publication_mode,state,phase,base_ref,base_sha,authority_digest,profile_digest,requested_by,created_at,updated_at) VALUES('{long_run}','repo-a','run','objective','plan_and_implement','local_only','BLOCKED','planning','main','0000000000000000000000000000000000000000','0000000000000000000000000000000000000000000000000000000000000000','0000000000000000000000000000000000000000000000000000000000000000','operator',1,1);
                 INSERT INTO run_plan_revisions(id,run_id,revision,plan_json,plan_sha256,state,created_at) VALUES('plan-long','{long_run}',1,'{{}}','0000000000000000000000000000000000000000000000000000000000000000','proposed',1);
                 INSERT INTO tasks(id,run_id,plan_revision_id,external_task_id,title,objective,priority,owner_profile,reviewer_profile,state,created_at,updated_at) VALUES('task-long','{long_run}','plan-long','task-long','task','objective','normal','general','general','BLOCKED',1,1);
                 INSERT INTO tasks(id,run_id,plan_revision_id,external_task_id,title,objective,priority,owner_profile,reviewer_profile,state,created_at,updated_at) VALUES('{long_task}','{long_run}','plan-long','task-long-a','task','objective','normal','general','general','BLOCKED',1,1);
                 INSERT INTO tasks(id,run_id,plan_revision_id,external_task_id,title,objective,priority,owner_profile,reviewer_profile,state,created_at,updated_at) VALUES('{long_dependency}','{long_run}','plan-long','task-long-b','task','objective','normal','general','general','BLOCKED',1,1);
                 INSERT INTO task_dependencies(task_id,depends_on_task_id) VALUES('{long_task}','{long_dependency}');
                 INSERT INTO task_attempts(id,task_id,attempt_number,state,task_packet_json,task_packet_sha256,base_sha,requested_model_route,created_at,updated_at) VALUES('{long_attempt}','task-long',1,'CREATED','{{}}','0000000000000000000000000000000000000000000000000000000000000000','0000000000000000000000000000000000000000','same',1,1);"
            ))
            .expect("full-width controller topology setup");
        let topology = store.run_topology(&long_run).expect("full-width topology");
        assert!(
            topology
                .nodes
                .iter()
                .any(|node| node.id == format!("run:{long_run}"))
        );
        assert!(
            topology
                .nodes
                .iter()
                .any(|node| node.source_ref == format!("run:{long_run}"))
        );
        assert!(
            topology
                .nodes
                .iter()
                .any(|node| node.id == format!("attempt:{long_attempt}"))
        );
        assert!(
            topology
                .nodes
                .iter()
                .any(|node| node.source_ref == format!("attempt:{long_attempt}"))
        );
        assert!(
            topology.edges.iter().any(|edge| {
                edge.kind == "depends_on"
                    && edge.source_ref == format!("dependency:{long_task}:{long_dependency}")
            }),
            "a dependency provenance reference must retain two full-width task IDs"
        );
        topology
            .validate()
            .expect("full-width topology remains domain-valid");
    }
}

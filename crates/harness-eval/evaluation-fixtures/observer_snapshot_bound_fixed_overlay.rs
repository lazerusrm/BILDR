// Controller-owned fixed-version overlay. Append inside the existing
// `crates/harness-store/src/queries.rs` `#[cfg(test)] mod tests`.
#[test]
fn m2_trace_snapshot_bound_regression() {
    fn snapshot_with_domain_receipts(count: usize) -> Result<usize, StoreError> {
        let (store, run_id) = store_with_created_run();
        let connection = store.connection()?;
        let transaction = connection.unchecked_transaction()?;
        for id in 0..count {
            transaction.execute(
                "INSERT INTO domain_events(run_id,aggregate_type,aggregate_id,event_type,occurred_at,payload_json) VALUES(?1,'agent','fixture','fixture.receipt',?2,'{}')",
                rusqlite::params![run_id.as_str(), i64::try_from(id).unwrap()],
            )?;
        }
        transaction.commit()?;
        drop(connection);
        Ok(store.trace_projection_snapshot(&run_id)?.domain_events.len())
    }

    assert_eq!(snapshot_with_domain_receipts(10_000).unwrap(), 10_000);

    let (store, run_id) = store_with_created_run();
    let connection = store.connection().unwrap();
    let transaction = connection.unchecked_transaction().unwrap();
    for id in 0..10_001 {
        transaction.execute(
            "INSERT INTO domain_events(run_id,aggregate_type,aggregate_id,event_type,occurred_at,payload_json) VALUES(?1,'agent','fixture','fixture.receipt',?2,'{}')",
            rusqlite::params![run_id.as_str(), i64::try_from(id).unwrap()],
        ).unwrap();
    }
    transaction.commit().unwrap();
    drop(connection);
    assert!(matches!(
        store.trace_projection_snapshot(&run_id),
        Err(StoreError::TraceProjectionBound {
            raw_receipts: 0,
            domain_receipts: 10_001,
            payload_bytes: None,
        })
    ));
}

// Controller-owned historical-version overlay. Append inside the existing
// `crates/harness-store/src/queries.rs` `#[cfg(test)] mod tests`.
#[test]
fn m2_trace_snapshot_bound_regression() {
    fn snapshot_with_domain_receipts(count: usize) -> Result<TraceProjectionSnapshot, StoreError> {
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
        store.trace_projection_snapshot(&run_id)
    }

    let ceiling = snapshot_with_domain_receipts(10_000).unwrap();
    assert_eq!(ceiling.domain_events.len(), 10_000);
    assert_eq!(ceiling.max_domain_event_id, 10_000);

    // This is the reproduction signal: old code admitted the over-limit set.
    let admitted = snapshot_with_domain_receipts(10_001).unwrap();
    assert_eq!(admitted.domain_events.len(), 10_001);
    assert_eq!(admitted.max_domain_event_id, 10_001);
}

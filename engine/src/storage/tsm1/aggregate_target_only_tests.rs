use super::*;
use crate::query::storage_ir::{AggregateKind, GroupedAggregateTarget, GroupedAggregateWindow, MeasurementRef};

fn request(field: &str, typ: FieldType, kind: AggregateKind) -> GroupedAggregateRequest {
    GroupedAggregateRequest {
        measurement: MeasurementRef::new("cpu"),
        targets: vec![GroupedAggregateTarget {
            field: field.into(), expected_field_type: typ, aggregates: vec![kind],
        }],
        time_range: TimeRange::new(0, 299),
        window: GroupedAggregateWindow { duration_ns: 100, origin_ns: 0 },
        group_tags: vec!["host".into()], tag_filters: vec![], cancellation: None,
    }
}

fn verify(engine: &tests::Engine) {
    for (field, typ) in [("i", crate::models::INTEGER), ("f", crate::models::FLOAT), ("b", crate::models::BOOL)] {
        let kinds = if typ == crate::models::BOOL {
            vec![AggregateKind::Count]
        } else {
            vec![AggregateKind::Count, AggregateKind::Sum, AggregateKind::Avg, AggregateKind::Min, AggregateKind::Max]
        };
        for kind in kinds {
            for shifted in [false, true] {
                let mut req = request(field, typ, kind);
                if shifted { req.window.origin_ns = 30; }
                let old = engine.engine.grouped_aggregate_scan(req.clone()).unwrap()
                    .unwrap_or_else(|| panic!("Generic scan unavailable: {field} {kind:?}, shifted={shifted}"));
                let new = engine.engine.grouped_aggregate_scan_target_only(req).unwrap()
                    .unwrap_or_else(|| panic!("Target-only scan unavailable: {field} {kind:?}, shifted={shifted}"));
                assert_eq!(new.metrics.requires_datafusion_fallback,
                    old.metrics.requires_datafusion_fallback);
                if old.metrics.requires_datafusion_fallback {
                    // Float MIN/MAX retain the adapter's existing exact raw fallback.
                    assert_eq!(typ, crate::models::FLOAT);
                    assert!(matches!(kind, AggregateKind::Min | AggregateKind::Max));
                    assert!(old.groups.is_empty() && new.groups.is_empty());
                    continue;
                }
                let expected = old.groups.into_iter()
                    .filter(|g| g.states[0].count != 0).collect::<Vec<_>>();
                assert_eq!(new.groups, expected, "{field} {kind:?}, shifted={shifted}");
                assert!(!new.groups.is_empty());
                assert!(new.groups.iter().all(|g| g.key.tag_values == vec![Some("A".into())]));
                assert_eq!(new.groups.iter().map(|g| g.states[0].count).sum::<u64>(), 30);
                #[cfg(feature = "slow-trace")]
                {
                    let p = &new.metrics.profile;
                    assert_eq!(p.row_domain_precheck_scans, 0);
                    assert_eq!(p.row_domain_fallback_scans, 0);
                    assert_eq!(p.physical_row_domain_precheck_blocks_decoded, 0);
                    assert_eq!(p.physical_row_domain_fallback_blocks_decoded, 0);
                    assert!(old.metrics.profile.row_domain_fallback_scans > 0);
                }
            }
        }
    }
    let req = request("i", crate::models::INTEGER, AggregateKind::Sum);
    let result = engine.engine.grouped_aggregate_scan_target_only(req).unwrap().unwrap();
    assert_eq!(result.groups.len(), 3);
    for group in result.groups {
        assert_eq!(group.states[0].count, 10);
        assert_eq!(group.states[0].values,
            Some(AggregateValues::Integer { sum: 45, min: 0, max: 9 }));
        assert_eq!(group.states[0].average(), Ok(Some(4.5)));
    }
    let req = request("absent", crate::models::INTEGER, AggregateKind::Count);
    assert!(engine.engine.grouped_aggregate_scan_target_only(req).unwrap().is_none());
}

#[test]
fn target_only_grouped_scan_preserves_sparse_results_through_compaction_and_reopen() {
    let mut engine = tests::Engine::with_native_vlog();
    for (field, typ) in [("i", crate::models::INTEGER), ("f", crate::models::FLOAT),
                         ("b", crate::models::BOOL), ("other", crate::models::INTEGER)] {
        engine.engine.measurement_fields(b"cpu")
            .create_field_if_not_exists(field.as_bytes(), typ).unwrap();
    }
    engine.engine.fieldset.save().unwrap();
    for batch in 0..3 {
        let mut lines = Vec::new();
        for point in 0..10 {
            let time = batch * 100 + point;
            lines.push(format!("cpu,host=A i={point}i,f={point}.0,b={} {time}", point % 2 == 0));
        }
        // Target-null timestamps and a target-null device must not add output groups.
        for point in 30..90 {
            let time = batch * 100 + point;
            lines.push(format!("cpu,host=A other=99i {time}"));
            lines.push(format!("cpu,host=B other=99i {time}"));
        }
        engine.write_points_string(&lines.iter().map(String::as_str).collect::<Vec<_>>()).unwrap();
        engine.must_write_snapshot();
    }
    verify(&engine);
    aggregate_native_tests::compact_native_all(&engine);
    verify(&engine);
    engine.reopen();
    verify(&engine);
    engine.close();
}

#[test]
fn target_only_grouped_scan_rejects_multiple_or_missing_targets() {
    let mut engine = tests::Engine::with_native_vlog();
    let mut req = request("i", crate::models::INTEGER, AggregateKind::Sum);
    req.targets.clear();
    assert!(engine.engine.grouped_aggregate_scan_target_only(req.clone()).is_err());
    req.targets = vec![GroupedAggregateTarget {
        field: "i".into(), expected_field_type: crate::models::INTEGER,
        aggregates: vec![AggregateKind::Sum],
    }; 2];
    assert!(engine.engine.grouped_aggregate_scan_target_only(req).is_err());
    engine.close();
}

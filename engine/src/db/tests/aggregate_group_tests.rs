use super::*;
use crate::{
    AggregateKind, AggregateNumber, AggregateRequest, AggregateShard, AggregateShardGroup,
    TimeRange,
};

#[test]
fn cross_shard_public_api_preserves_scope_predicates_and_lifecycle() {
    let dir = must_temp_reefsdb_dir();
    let sfile = Arc::new(must_open_series_file().0);
    let mut options = EngineOptions::default();
    options.vlog_enabled = true;
    options.agg_index_enabled = true;
    options.compaction_disabled = true;
    let a = new_test_reefsdb(1, dir.path(), Arc::clone(&sfile), options.clone());
    let b = new_test_reefsdb(2, dir.path(), Arc::clone(&sfile), options.clone());
    a.write_points_string(&[
        "cpu,host=A value=2i 10",
        "cpu,host=B value=99i 10",
        "cpu,host=A value=9999i 110",
    ])
    .unwrap();
    b.write_points_string(&[
        "cpu,host=A value=3i 110",
        "cpu,host=B value=999i 110",
        "cpu,host=A value=9999i 10",
    ])
    .unwrap();
    let group = || {
        AggregateShardGroup::new(vec![
            AggregateShard {
                database: Arc::clone(&b),
                time_range: TimeRange::new(100, 199),
            },
            AggregateShard {
                database: Arc::clone(&a),
                time_range: TimeRange::new(0, 99),
            },
        ])
        .unwrap()
    };
    let req = AggregateRequest {
        measurement: MeasurementRef::new("cpu"),
        field: "value".into(),
        time_range: TimeRange::new(0, 199),
        aggregates: vec![AggregateKind::Count, AggregateKind::Sum],
    };
    let pred = StoragePredicate::Compare {
        op: CompareOp::Equal,
        lhs: StorageValueExpr::Column(ColumnRef::new("host", ColumnKind::Tag)),
        rhs: StorageValueExpr::Literal(crate::Val::String("A".into())),
    };
    let group = group();
    for snapshot in [false, true] {
        if snapshot {
            a.create_snapshot(false).unwrap();
            b.create_snapshot(false).unwrap();
        }
        let result = group
            .aggregate_projected_scan_windows_filtered(
                req.clone(),
                vec![req.time_range],
                pred.clone(),
            )
            .unwrap()
            .unwrap();
        assert_eq!(result.result.states[0].count, Some(2));
        assert_eq!(
            result.result.states[0].sum,
            Some(AggregateNumber::Integer(5))
        );
    }
    assert!(
        AggregateShardGroup::new(vec![
            AggregateShard {
                database: Arc::clone(&a),
                time_range: TimeRange::new(0, 100)
            },
            AggregateShard {
                database: Arc::clone(&b),
                time_range: TimeRange::new(100, 200)
            },
        ])
        .is_err()
    );
    assert!(
        AggregateShardGroup::new(vec![
            AggregateShard {
                database: Arc::clone(&a),
                time_range: TimeRange::new(0, 99)
            },
            AggregateShard {
                database: Arc::clone(&a),
                time_range: TimeRange::new(100, 199)
            },
        ])
        .is_err()
    );
    b.close().unwrap();
    assert!(
        group
            .aggregate_projected_scan_windows_filtered(req.clone(), vec![req.time_range], pred)
            .is_err()
    );
    a.close().unwrap();
    sfile.close().unwrap();
}

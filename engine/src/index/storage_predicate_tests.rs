use std::{collections::HashMap, sync::Arc};

use crate::models::{FLOAT, points::Val};
use tokio_util::sync::CancellationToken;

use crate::{
    index::{
        ActiveIndex, IndexSet,
        storage_predicate::{CandidateExactness, SeriesFilterResult},
        tsi1::index::tests::must_open_default_index,
    },
    query::storage_ir::{
        ArithmeticOp, ColumnKind, ColumnRef, CompareOp, LogicalOp, RegexOp, ScanCancellation,
        StoragePredicate, StorageValueExpr,
    },
    series::measurement_fields::MeasurementFieldSet,
    series::{series_file::tests::Series, series_set::SeriesIDSet},
};

const CPU: &[u8] = b"cpu";

struct Fixture {
    index_set: IndexSet,
    a_us_west: u64,
    b_us: u64,
    a_us: u64,
    c_missing_region: u64,
    d_us_east: u64,
    empty_tags: u64,
    missing_tags: u64,
}

impl Fixture {
    fn new() -> Self {
        let mut index = must_open_default_index();
        let a_us_west_tags = tags(&[("host", "a"), ("region", "us-west"), ("name", "cpu")]);
        let b_us_tags = tags(&[("host", "b"), ("region", "us"), ("name", "other")]);
        let a_us_tags = tags(&[("host", "a"), ("region", "us")]);
        let c_missing_region_tags = tags(&[("host", "c")]);
        let d_us_east_tags = tags(&[("host", "d"), ("region", "us-east")]);
        let empty_tags = tags(&[("host", ""), ("region", "")]);
        let missing_tags = tags(&[]);

        index
            .create_series_slice_if_not_exists(vec![
                series(CPU, &a_us_west_tags),
                series(CPU, &b_us_tags),
                series(CPU, &a_us_tags),
                series(CPU, &c_missing_region_tags),
                series(CPU, &d_us_east_tags),
                series(CPU, &empty_tags),
                series(CPU, &missing_tags),
                series(b"mem", &a_us_west_tags),
            ])
            .unwrap();

        let sfile = Arc::clone(&index.sfile);
        let fields = Arc::new(MeasurementFieldSet::default());
        fields
            .create_fields_if_not_exists(CPU)
            .create_field_if_not_exists(b"temperature", FLOAT)
            .unwrap();

        let a_us_west = sfile.series_id(CPU, &a_us_west_tags);
        let b_us = sfile.series_id(CPU, &b_us_tags);
        let a_us = sfile.series_id(CPU, &a_us_tags);
        let c_missing_region = sfile.series_id(CPU, &c_missing_region_tags);
        let d_us_east = sfile.series_id(CPU, &d_us_east_tags);
        let empty_tags = sfile.series_id(CPU, &empty_tags);
        let missing_tags = sfile.series_id(CPU, &missing_tags);
        assert!(
            [
                a_us_west,
                b_us,
                a_us,
                c_missing_region,
                d_us_east,
                empty_tags,
                missing_tags,
            ]
            .into_iter()
            .all(|id| id != 0)
        );

        Self {
            index_set: IndexSet::new(
                Arc::new(ActiveIndex::from_implementation(index.idx)),
                sfile,
                Some(fields),
            ),
            a_us_west,
            b_us,
            a_us,
            c_missing_region,
            d_us_east,
            empty_tags,
            missing_tags,
        }
    }

    fn all_cpu(&self) -> SeriesIDSet {
        id_set(&[
            self.a_us_west,
            self.b_us,
            self.a_us,
            self.c_missing_region,
            self.d_us_east,
            self.empty_tags,
            self.missing_tags,
        ])
    }

    fn filter(&self, predicate: &StoragePredicate) -> SeriesFilterResult {
        self.index_set
            .filter_series_by_storage_predicate(CPU, predicate)
            .unwrap()
    }
}

fn tags(values: &[(&str, &str)]) -> crate::models::Tags {
    crate::models::Tags::from(
        values
            .iter()
            .map(|(key, value)| (key.as_bytes().to_vec(), value.as_bytes().to_vec()))
            .collect::<HashMap<_, _>>(),
    )
}

fn series(name: &[u8], tags: &crate::models::Tags) -> Series {
    Series {
        name: name.to_vec(),
        tags: tags.clone(),
        delete: false,
    }
}

fn id_set(ids: &[u64]) -> SeriesIDSet {
    SeriesIDSet::new(ids)
}

fn column(name: &str, kind: ColumnKind) -> StorageValueExpr {
    StorageValueExpr::Column(ColumnRef::new(name, kind))
}

fn string(value: &str) -> StorageValueExpr {
    StorageValueExpr::Literal(Val::String(value.to_string()))
}

fn compare(name: &str, kind: ColumnKind, op: CompareOp, value: &str) -> StoragePredicate {
    StoragePredicate::Compare {
        op,
        lhs: column(name, kind),
        rhs: string(value),
    }
}

fn regex(name: &str, kind: ColumnKind, op: RegexOp, pattern: &str) -> StoragePredicate {
    StoragePredicate::Regex {
        op,
        value: column(name, kind),
        pattern: pattern.to_string(),
    }
}

fn logical(op: LogicalOp, lhs: StoragePredicate, rhs: StoragePredicate) -> StoragePredicate {
    StoragePredicate::Logical {
        op,
        lhs: Box::new(lhs),
        rhs: Box::new(rhs),
    }
}

fn exact_tag_set(name: &str, accepted_values: &[&str]) -> StoragePredicate {
    StoragePredicate::ExactTagSet {
        tag: ColumnRef::new(name, ColumnKind::Tag),
        accepted_values: accepted_values
            .iter()
            .map(|value| (*value).to_string())
            .collect(),
    }
}

fn assert_set_eq(actual: &SeriesIDSet, expected: &SeriesIDSet) {
    assert!(
        actual.equals(expected),
        "actual series {actual:?}, expected {expected:?}"
    );
}

fn assert_exact(result: &SeriesFilterResult, expected: &SeriesIDSet) {
    assert_eq!(result.exactness, CandidateExactness::Exact);
    assert!(result.residual_predicate.is_none());
    assert_set_eq(&result.candidate_series, expected);
}

fn large_index_set(series_count: usize) -> IndexSet {
    let mut index = must_open_default_index();
    let series = (0..series_count)
        .map(|value| {
            let tags = tags(&[("region", &format!("value-{value:04}"))]);
            series(CPU, &tags)
        })
        .collect();
    index.create_series_slice_if_not_exists(series).unwrap();
    let sfile = Arc::clone(&index.sfile);
    IndexSet::new(
        Arc::new(ActiveIndex::from_implementation(index.idx)),
        sfile,
        None,
    )
}

#[test]
fn native_tag_measurement_and_bitmap_predicates_select_exact_candidates() {
    let fixture = Fixture::new();

    let host_a = compare("host", ColumnKind::Tag, CompareOp::Equal, "a");
    assert_exact(
        &fixture.filter(&host_a),
        &id_set(&[fixture.a_us_west, fixture.a_us]),
    );

    let region_us = compare("region", ColumnKind::Tag, CompareOp::Equal, "us");
    assert_exact(
        &fixture.filter(&logical(LogicalOp::And, host_a.clone(), region_us.clone())),
        &id_set(&[fixture.a_us]),
    );
    assert_exact(
        &fixture.filter(&logical(LogicalOp::Or, host_a, region_us)),
        &id_set(&[fixture.a_us_west, fixture.b_us, fixture.a_us]),
    );

    assert_exact(
        &fixture.filter(&compare(
            "_name",
            ColumnKind::Measurement,
            CompareOp::Equal,
            "cpu",
        )),
        &fixture.all_cpu(),
    );
}

#[test]
fn exact_tag_sets_union_values_intersect_tags_and_distinguish_empty_from_missing() {
    let fixture = Fixture::new();

    let hosts = exact_tag_set("host", &["", "a", "a"]);
    assert_exact(
        &fixture.filter(&hosts),
        &id_set(&[fixture.empty_tags, fixture.a_us_west, fixture.a_us]),
    );

    let west_or_empty = exact_tag_set("region", &["", "us-west"]);
    let combined = logical(LogicalOp::And, hosts, west_or_empty);
    assert_exact(
        &fixture.filter(&combined),
        &id_set(&[fixture.empty_tags, fixture.a_us_west]),
    );
}

#[test]
fn native_field_or_tag_is_field_first_and_distinguishes_name_from_measurement() {
    let fixture = Fixture::new();

    let field = StoragePredicate::Compare {
        op: CompareOp::GreaterThan,
        lhs: column("temperature", ColumnKind::FieldOrTag),
        rhs: StorageValueExpr::Literal(Val::F64(10.0)),
    };
    let result = fixture.filter(&field);
    assert_eq!(result.exactness, CandidateExactness::Superset);
    assert_set_eq(&result.candidate_series, &fixture.all_cpu());
    assert_eq!(result.residual_predicate, Some(field));

    assert_exact(
        &fixture.filter(&compare(
            "host",
            ColumnKind::FieldOrTag,
            CompareOp::Equal,
            "a",
        )),
        &id_set(&[fixture.a_us_west, fixture.a_us]),
    );
    assert_exact(
        &fixture.filter(&compare(
            "name",
            ColumnKind::FieldOrTag,
            CompareOp::Equal,
            "cpu",
        )),
        &id_set(&[fixture.a_us_west]),
    );
    assert_exact(
        &fixture.filter(&compare(
            "rack",
            ColumnKind::FieldOrTag,
            CompareOp::Equal,
            "",
        )),
        &fixture.all_cpu(),
    );
}

#[test]
fn native_regex_excludes_missing_tags_and_preserves_present_empty_values() {
    let fixture = Fixture::new();

    assert_exact(
        &fixture.filter(&regex("region", ColumnKind::Tag, RegexOp::Match, "^us-.*")),
        &id_set(&[fixture.a_us_west, fixture.d_us_east]),
    );
    assert_exact(
        &fixture.filter(&regex(
            "region",
            ColumnKind::Tag,
            RegexOp::NotMatch,
            "^us-.*",
        )),
        &id_set(&[fixture.b_us, fixture.a_us, fixture.empty_tags]),
    );
    assert_exact(
        &fixture.filter(&regex("region", ColumnKind::Tag, RegexOp::Match, "^$")),
        &id_set(&[fixture.empty_tags]),
    );
}

#[test]
fn native_missing_present_empty_and_nonempty_tags_remain_distinct() {
    let fixture = Fixture::new();
    let cases = [
        (
            compare("host", ColumnKind::Tag, CompareOp::Equal, "a"),
            id_set(&[fixture.a_us_west, fixture.a_us]),
        ),
        (
            compare("host", ColumnKind::Tag, CompareOp::NotEqual, "a"),
            id_set(&[
                fixture.b_us,
                fixture.c_missing_region,
                fixture.d_us_east,
                fixture.empty_tags,
                fixture.missing_tags,
            ]),
        ),
        (
            compare("host", ColumnKind::Tag, CompareOp::Equal, ""),
            id_set(&[fixture.missing_tags]),
        ),
        (
            compare("host", ColumnKind::Tag, CompareOp::NotEqual, ""),
            id_set(&[
                fixture.a_us_west,
                fixture.b_us,
                fixture.a_us,
                fixture.c_missing_region,
                fixture.d_us_east,
                fixture.empty_tags,
            ]),
        ),
        (
            regex("host", ColumnKind::Tag, RegexOp::Match, "^$"),
            id_set(&[fixture.empty_tags]),
        ),
    ];

    for (predicate, expected) in cases {
        assert_exact(&fixture.filter(&predicate), &expected);
    }
}

#[test]
fn native_nested_boolean_pushdown_keeps_required_residuals() {
    let fixture = Fixture::new();
    let field = StoragePredicate::Compare {
        op: CompareOp::GreaterThan,
        lhs: column("temperature", ColumnKind::AnyField),
        rhs: StorageValueExpr::Literal(Val::F64(10.0)),
    };
    let tag_or_field = logical(
        LogicalOp::Or,
        compare("host", ColumnKind::Tag, CompareOp::Equal, "a"),
        field,
    );
    let predicate = logical(
        LogicalOp::And,
        tag_or_field.clone(),
        compare("region", ColumnKind::Tag, CompareOp::Equal, "us"),
    );
    let result = fixture.filter(&predicate);

    assert_eq!(result.exactness, CandidateExactness::Superset);
    assert_set_eq(
        &result.candidate_series,
        &id_set(&[fixture.b_us, fixture.a_us]),
    );
    assert_eq!(result.residual_predicate, Some(tag_or_field));
}

#[test]
fn native_field_and_arithmetic_predicates_remain_conservative_residuals() {
    let fixture = Fixture::new();
    let arithmetic = StoragePredicate::Compare {
        op: CompareOp::GreaterThan,
        lhs: StorageValueExpr::Arithmetic {
            op: ArithmeticOp::Multiply,
            lhs: Box::new(column("temperature", ColumnKind::Field(Some(FLOAT)))),
            rhs: Box::new(StorageValueExpr::Literal(Val::F64(2.0))),
        },
        rhs: StorageValueExpr::Literal(Val::F64(50.0)),
    };
    let result = fixture.filter(&arithmetic);
    assert_eq!(result.exactness, CandidateExactness::Superset);
    assert_set_eq(&result.candidate_series, &fixture.all_cpu());
    assert_eq!(result.residual_predicate, Some(arithmetic.clone()));

    let unsafe_or = logical(
        LogicalOp::Or,
        compare("host", ColumnKind::Tag, CompareOp::Equal, "a"),
        arithmetic,
    );
    let result = fixture.filter(&unsafe_or);
    assert_eq!(result.exactness, CandidateExactness::Superset);
    assert_set_eq(&result.candidate_series, &fixture.all_cpu());
    assert_eq!(result.residual_predicate, Some(unsafe_or));
}

#[test]
fn native_invalid_regex_remains_a_full_residual() {
    let fixture = Fixture::new();
    for op in [RegexOp::Match, RegexOp::NotMatch] {
        let predicate = regex("region", ColumnKind::Tag, op, "(");
        let result = fixture.filter(&predicate);
        assert_eq!(result.exactness, CandidateExactness::Superset);
        assert_set_eq(&result.candidate_series, &fixture.all_cpu());
        assert_eq!(result.residual_predicate, Some(predicate));
    }
}

#[test]
fn native_index_cancellation_interrupts_regex_value_scanning() {
    let index_set = large_index_set(1_100);
    let predicate = regex("region", ColumnKind::Tag, RegexOp::Match, "^never-matches$");
    let token = CancellationToken::new();
    let cancellation = ScanCancellation::cancel_after_checks(token.clone(), 3);

    assert!(matches!(
        index_set.filter_series_by_storage_predicate_with_cancellation(
            CPU,
            &predicate,
            Some(&cancellation),
        ),
        Err(super::Error::StoragePredicateCancelled)
    ));
    assert!(token.is_cancelled());
}

#[test]
fn native_index_cancellation_interrupts_all_series_collection() {
    let index_set = large_index_set(1_100);
    let predicate = StoragePredicate::Compare {
        op: CompareOp::GreaterThan,
        lhs: column("temperature", ColumnKind::Field(Some(FLOAT))),
        rhs: StorageValueExpr::Literal(Val::F64(10.0)),
    };
    let token = CancellationToken::new();
    let cancellation = ScanCancellation::cancel_after_checks(token.clone(), 3);

    assert!(matches!(
        index_set.filter_series_by_storage_predicate_with_cancellation(
            CPU,
            &predicate,
            Some(&cancellation),
        ),
        Err(super::Error::StoragePredicateCancelled)
    ));
    assert!(token.is_cancelled());
}

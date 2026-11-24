//! Storage-native residual predicate preparation and row evaluation.

use std::collections::HashMap;

use crate::models::points::Val;
use regex::Regex;

use crate::query::storage_ir::{
    ArithmeticOp, ColumnRef, CompareOp, LogicalOp, RegexOp, StoragePredicate, StorageValueExpr,
};

/// The per-series resolution of a storage column.
pub(crate) enum ColumnResolution {
    /// Read the value from a timestamp-aligned field cursor.
    Field(ColumnRef),
    /// Reuse a value that is constant for the series, or preserve a missing value.
    Static(Option<Val>),
    /// Use the current row timestamp.
    Time,
}

/// A residual predicate prepared once for one series.
pub(crate) struct PreparedStoragePredicate {
    root: PreparedPredicate,
    field_dependencies: Vec<ColumnRef>,
}

impl PreparedStoragePredicate {
    pub(crate) fn prepare<F>(predicate: &StoragePredicate, resolve: F) -> Self
    where
        F: FnMut(&ColumnRef) -> ColumnResolution,
    {
        let mut preparer = Preparer {
            resolve,
            columns: HashMap::new(),
            field_dependencies: Vec::new(),
        };
        let root = preparer.predicate(predicate);
        Self {
            root,
            field_dependencies: preparer.field_dependencies,
        }
    }

    pub(crate) fn field_dependencies(&self) -> &[ColumnRef] {
        &self.field_dependencies
    }

    pub(crate) fn matches(&self, timestamp: i64, field_values: &[Option<Val>]) -> bool {
        self.evaluate(timestamp, field_values).unwrap_or(false)
    }

    pub(crate) fn evaluate(&self, timestamp: i64, field_values: &[Option<Val>]) -> Option<bool> {
        debug_assert!(field_values.len() >= self.field_dependencies.len());
        let row = StorageRowContext {
            timestamp,
            field_values,
        };
        self.root.evaluate(&row)
    }
}

struct Preparer<F> {
    resolve: F,
    // The legacy evaluator keyed values only by column name. Reuse the first
    // resolution when a condition references one name with multiple markers.
    columns: HashMap<String, PreparedValueExpr>,
    field_dependencies: Vec<ColumnRef>,
}

impl<F> Preparer<F>
where
    F: FnMut(&ColumnRef) -> ColumnResolution,
{
    fn predicate(&mut self, predicate: &StoragePredicate) -> PreparedPredicate {
        match predicate {
            StoragePredicate::Value(value) => PreparedPredicate::Value(self.value(value)),
            StoragePredicate::Constant(value) => PreparedPredicate::Constant(*value),
            StoragePredicate::Missing => PreparedPredicate::Missing,
            StoragePredicate::ExactTagSet {
                tag,
                accepted_values,
            } => {
                let mut accepted_values = accepted_values.clone();
                accepted_values.sort();
                accepted_values.dedup();
                PreparedPredicate::ExactTagSet {
                    value: self.column(tag),
                    accepted_values,
                }
            }
            StoragePredicate::Compare { op, lhs, rhs } => PreparedPredicate::Compare {
                op: *op,
                lhs: self.value(lhs),
                rhs: self.value(rhs),
            },
            StoragePredicate::Regex { op, value, pattern } => PreparedPredicate::Regex {
                op: *op,
                value: self.value(value),
                regex: Regex::new(pattern).ok(),
            },
            StoragePredicate::Logical { op, lhs, rhs } => PreparedPredicate::Logical {
                op: *op,
                lhs: Box::new(self.predicate(lhs)),
                rhs: Box::new(self.predicate(rhs)),
            },
        }
    }

    fn value(&mut self, value: &StorageValueExpr) -> PreparedValueExpr {
        match value {
            StorageValueExpr::Column(column) => self.column(column),
            StorageValueExpr::Literal(value) => PreparedValueExpr::Literal(value.clone()),
            StorageValueExpr::Predicate(predicate) => {
                PreparedValueExpr::Predicate(Box::new(self.predicate(predicate)))
            }
            StorageValueExpr::Arithmetic { op, lhs, rhs } => PreparedValueExpr::Arithmetic {
                op: *op,
                lhs: Box::new(self.value(lhs)),
                rhs: Box::new(self.value(rhs)),
            },
        }
    }

    fn column(&mut self, column: &ColumnRef) -> PreparedValueExpr {
        if let Some(value) = self.columns.get(&column.name) {
            return value.clone();
        }

        let value = match (self.resolve)(column) {
            ColumnResolution::Field(field) => {
                let index = self.field_dependencies.len();
                self.field_dependencies.push(field);
                PreparedValueExpr::Field(index)
            }
            ColumnResolution::Static(Some(value)) => PreparedValueExpr::Literal(value),
            ColumnResolution::Static(None) => PreparedValueExpr::Missing,
            ColumnResolution::Time => PreparedValueExpr::Time,
        };
        self.columns.insert(column.name.clone(), value.clone());
        value
    }
}

struct StorageRowContext<'a> {
    timestamp: i64,
    field_values: &'a [Option<Val>],
}

#[derive(Clone)]
enum PreparedPredicate {
    Value(PreparedValueExpr),
    Constant(bool),
    Missing,
    ExactTagSet {
        value: PreparedValueExpr,
        accepted_values: Vec<String>,
    },
    Compare {
        op: CompareOp,
        lhs: PreparedValueExpr,
        rhs: PreparedValueExpr,
    },
    Regex {
        op: RegexOp,
        value: PreparedValueExpr,
        regex: Option<Regex>,
    },
    Logical {
        op: LogicalOp,
        lhs: Box<PreparedPredicate>,
        rhs: Box<PreparedPredicate>,
    },
}

impl PreparedPredicate {
    fn evaluate(&self, row: &StorageRowContext<'_>) -> Option<bool> {
        match self {
            Self::Value(value) => match value.evaluate(row) {
                Some(EvalValue::Bool(value)) => Some(value),
                Some(_) | None => None,
            },
            Self::Constant(value) => Some(*value),
            Self::Missing => None,
            Self::ExactTagSet {
                value,
                accepted_values,
            } => match value.evaluate(row) {
                Some(EvalValue::String(value)) => Some(
                    accepted_values
                        .binary_search_by(|item| item.as_str().cmp(value))
                        .is_ok(),
                ),
                Some(_) => Some(false),
                None => None,
            },
            Self::Compare { op, lhs, rhs } => {
                Some(compare(*op, lhs.evaluate(row)?, rhs.evaluate(row)?))
            }
            Self::Regex { op, value, regex } => {
                let Some(regex) = regex else {
                    return Some(false);
                };
                let Some(value) = value.evaluate(row) else {
                    return None;
                };
                let EvalValue::String(value) = value else {
                    return Some(false);
                };
                Some(match op {
                    RegexOp::Match => regex.is_match(value),
                    RegexOp::NotMatch => !regex.is_match(value),
                })
            }
            Self::Logical { op, lhs, rhs } => {
                let lhs = lhs.evaluate(row).unwrap_or(false);
                let rhs = rhs.evaluate(row).unwrap_or(false);
                Some(match op {
                    LogicalOp::And => lhs && rhs,
                    LogicalOp::Or => lhs || rhs,
                })
            }
        }
    }
}

#[derive(Clone)]
enum PreparedValueExpr {
    Field(usize),
    Literal(Val),
    Missing,
    Time,
    Predicate(Box<PreparedPredicate>),
    Arithmetic {
        op: ArithmeticOp,
        lhs: Box<PreparedValueExpr>,
        rhs: Box<PreparedValueExpr>,
    },
}

impl PreparedValueExpr {
    fn evaluate<'a>(&'a self, row: &'a StorageRowContext<'a>) -> Option<EvalValue<'a>> {
        match self {
            Self::Field(index) => row
                .field_values
                .get(*index)
                .and_then(Option::as_ref)
                .and_then(eval_value),
            Self::Literal(value) => eval_value(value),
            Self::Missing => None,
            Self::Time => Some(EvalValue::I64(row.timestamp)),
            Self::Predicate(predicate) => predicate.evaluate(row).map(EvalValue::Bool),
            Self::Arithmetic { op, lhs, rhs } => {
                evaluate_arithmetic(*op, lhs.evaluate(row)?, rhs.evaluate(row)?)
            }
        }
    }
}

enum EvalValue<'a> {
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    String(&'a str),
}

fn eval_value(value: &Val) -> Option<EvalValue<'_>> {
    match value {
        Val::I64(value) => Some(EvalValue::I64(*value)),
        Val::U64(value) => Some(EvalValue::U64(*value)),
        Val::F64(value) => Some(EvalValue::F64(*value)),
        Val::Bool(value) => Some(EvalValue::Bool(*value)),
        Val::String(value) => Some(EvalValue::String(value)),
        Val::Empty(_) => None,
    }
}

fn evaluate_arithmetic<'a>(
    op: ArithmeticOp,
    lhs: EvalValue<'a>,
    rhs: EvalValue<'a>,
) -> Option<EvalValue<'a>> {
    match (lhs, rhs) {
        (EvalValue::F64(lhs), EvalValue::F64(rhs)) => f64_arithmetic(op, lhs, rhs),
        (EvalValue::F64(lhs), EvalValue::I64(rhs)) => f64_arithmetic(op, lhs, rhs as f64),
        (EvalValue::F64(lhs), EvalValue::U64(rhs)) => f64_arithmetic(op, lhs, rhs as f64),
        (EvalValue::I64(lhs), EvalValue::F64(rhs)) => f64_arithmetic(op, lhs as f64, rhs),
        (EvalValue::U64(lhs), EvalValue::F64(rhs)) => f64_arithmetic(op, lhs as f64, rhs),
        (EvalValue::I64(lhs), EvalValue::I64(rhs)) => i64_arithmetic(op, lhs, rhs),
        (EvalValue::U64(lhs), EvalValue::U64(rhs)) => u64_arithmetic(op, lhs, rhs),
        (EvalValue::I64(lhs), EvalValue::U64(rhs)) => u64_arithmetic(op, lhs as u64, rhs),
        (EvalValue::U64(lhs), EvalValue::I64(rhs)) => u64_arithmetic(op, lhs, rhs as u64),
        _ => None,
    }
}

fn f64_arithmetic(op: ArithmeticOp, lhs: f64, rhs: f64) -> Option<EvalValue<'static>> {
    match op {
        ArithmeticOp::Add => Some(EvalValue::F64(lhs + rhs)),
        ArithmeticOp::Subtract => Some(EvalValue::F64(lhs - rhs)),
        ArithmeticOp::Multiply => Some(EvalValue::F64(lhs * rhs)),
        ArithmeticOp::Divide if rhs == 0.0 => Some(EvalValue::F64(0.0)),
        ArithmeticOp::Divide => Some(EvalValue::F64(lhs / rhs)),
        ArithmeticOp::Modulo => Some(EvalValue::F64(lhs % rhs)),
        ArithmeticOp::BitwiseAnd | ArithmeticOp::BitwiseOr | ArithmeticOp::BitwiseXor => None,
    }
}

fn i64_arithmetic(op: ArithmeticOp, lhs: i64, rhs: i64) -> Option<EvalValue<'static>> {
    match op {
        ArithmeticOp::Add => Some(EvalValue::I64(lhs.wrapping_add(rhs))),
        ArithmeticOp::Subtract => Some(EvalValue::I64(lhs.wrapping_sub(rhs))),
        ArithmeticOp::Multiply => Some(EvalValue::I64(lhs.wrapping_mul(rhs))),
        ArithmeticOp::Divide if rhs == 0 => Some(EvalValue::I64(0)),
        ArithmeticOp::Divide => Some(EvalValue::I64(lhs.wrapping_div(rhs))),
        ArithmeticOp::Modulo if rhs == 0 => Some(EvalValue::I64(0)),
        ArithmeticOp::Modulo => Some(EvalValue::I64(lhs.wrapping_rem(rhs))),
        ArithmeticOp::BitwiseAnd => Some(EvalValue::I64(lhs & rhs)),
        ArithmeticOp::BitwiseOr => Some(EvalValue::I64(lhs | rhs)),
        ArithmeticOp::BitwiseXor => Some(EvalValue::I64(lhs ^ rhs)),
    }
}

fn u64_arithmetic(op: ArithmeticOp, lhs: u64, rhs: u64) -> Option<EvalValue<'static>> {
    match op {
        ArithmeticOp::Add => Some(EvalValue::U64(lhs.wrapping_add(rhs))),
        ArithmeticOp::Subtract => Some(EvalValue::U64(lhs.wrapping_sub(rhs))),
        ArithmeticOp::Multiply => Some(EvalValue::U64(lhs.wrapping_mul(rhs))),
        ArithmeticOp::Divide if rhs == 0 => Some(EvalValue::U64(0)),
        ArithmeticOp::Divide => Some(EvalValue::U64(lhs.wrapping_div(rhs))),
        ArithmeticOp::Modulo if rhs == 0 => Some(EvalValue::U64(0)),
        ArithmeticOp::Modulo => Some(EvalValue::U64(lhs.wrapping_rem(rhs))),
        ArithmeticOp::BitwiseAnd => Some(EvalValue::U64(lhs & rhs)),
        ArithmeticOp::BitwiseOr => Some(EvalValue::U64(lhs | rhs)),
        ArithmeticOp::BitwiseXor => Some(EvalValue::U64(lhs ^ rhs)),
    }
}

fn compare(op: CompareOp, lhs: EvalValue<'_>, rhs: EvalValue<'_>) -> bool {
    match (lhs, rhs) {
        (EvalValue::Bool(lhs), EvalValue::Bool(rhs)) => match op {
            CompareOp::Equal => lhs == rhs,
            CompareOp::NotEqual => lhs != rhs,
            _ => false,
        },
        (EvalValue::F64(lhs), EvalValue::F64(rhs)) => compare_f64(op, lhs, rhs),
        (EvalValue::F64(lhs), EvalValue::I64(rhs)) => compare_f64(op, lhs, rhs as f64),
        (EvalValue::F64(lhs), EvalValue::U64(rhs)) => compare_f64(op, lhs, rhs as f64),
        (EvalValue::I64(lhs), EvalValue::F64(rhs)) => compare_f64(op, lhs as f64, rhs),
        (EvalValue::U64(lhs), EvalValue::F64(rhs)) => compare_f64(op, lhs as f64, rhs),
        (EvalValue::I64(lhs), EvalValue::I64(rhs)) => compare_i64(op, lhs, rhs),
        (EvalValue::U64(lhs), EvalValue::U64(rhs)) => compare_u64(op, lhs, rhs),
        (EvalValue::I64(lhs), EvalValue::U64(rhs)) => compare_i64_u64(op, lhs, rhs),
        (EvalValue::U64(lhs), EvalValue::I64(rhs)) => compare_u64_i64(op, lhs, rhs),
        (EvalValue::String(lhs), EvalValue::String(rhs)) => match op {
            CompareOp::Equal => lhs == rhs,
            CompareOp::NotEqual => lhs != rhs,
            _ => false,
        },
        _ => false,
    }
}

macro_rules! compare_ordered {
    ($op:expr, $lhs:expr, $rhs:expr) => {
        match $op {
            CompareOp::Equal => $lhs == $rhs,
            CompareOp::NotEqual => $lhs != $rhs,
            CompareOp::LessThan => $lhs < $rhs,
            CompareOp::LessThanOrEqual => $lhs <= $rhs,
            CompareOp::GreaterThan => $lhs > $rhs,
            CompareOp::GreaterThanOrEqual => $lhs >= $rhs,
        }
    };
}

fn compare_f64(op: CompareOp, lhs: f64, rhs: f64) -> bool {
    compare_ordered!(op, lhs, rhs)
}

fn compare_i64(op: CompareOp, lhs: i64, rhs: i64) -> bool {
    compare_ordered!(op, lhs, rhs)
}

fn compare_u64(op: CompareOp, lhs: u64, rhs: u64) -> bool {
    compare_ordered!(op, lhs, rhs)
}

fn compare_i64_u64(op: CompareOp, lhs: i64, rhs: u64) -> bool {
    match op {
        CompareOp::Equal => lhs >= 0 && lhs as u64 == rhs,
        CompareOp::NotEqual => lhs < 0 || lhs as u64 != rhs,
        CompareOp::LessThan => lhs < 0 || (lhs as u64) < rhs,
        CompareOp::LessThanOrEqual => lhs < 0 || (lhs as u64) <= rhs,
        CompareOp::GreaterThan => lhs >= 0 && (lhs as u64) > rhs,
        CompareOp::GreaterThanOrEqual => lhs >= 0 && (lhs as u64) >= rhs,
    }
}

fn compare_u64_i64(op: CompareOp, lhs: u64, rhs: i64) -> bool {
    match op {
        CompareOp::Equal => rhs >= 0 && lhs == rhs as u64,
        CompareOp::NotEqual => rhs < 0 || lhs != rhs as u64,
        CompareOp::LessThan => rhs >= 0 && lhs < rhs as u64,
        CompareOp::LessThanOrEqual => rhs >= 0 && lhs <= rhs as u64,
        CompareOp::GreaterThan => rhs < 0 || lhs > rhs as u64,
        CompareOp::GreaterThanOrEqual => rhs < 0 || lhs >= rhs as u64,
    }
}

#[cfg(test)]
mod tests {
    use crate::models::{FLOAT, points::Val};

    use super::{ColumnResolution, PreparedStoragePredicate};
    use crate::query::storage_ir::{
        ArithmeticOp, ColumnKind, ColumnRef, CompareOp, LogicalOp, RegexOp, StoragePredicate,
        StorageValueExpr,
    };

    fn column(name: &str, kind: ColumnKind) -> StorageValueExpr {
        StorageValueExpr::Column(ColumnRef::new(name, kind))
    }

    fn literal(value: Val) -> StorageValueExpr {
        StorageValueExpr::Literal(value)
    }

    fn compare(op: CompareOp, lhs: StorageValueExpr, rhs: StorageValueExpr) -> StoragePredicate {
        StoragePredicate::Compare { op, lhs, rhs }
    }

    fn arithmetic(
        op: ArithmeticOp,
        lhs: StorageValueExpr,
        rhs: StorageValueExpr,
    ) -> StorageValueExpr {
        StorageValueExpr::Arithmetic {
            op,
            lhs: Box::new(lhs),
            rhs: Box::new(rhs),
        }
    }

    fn prepare_with_engine_resolution(predicate: &StoragePredicate) -> PreparedStoragePredicate {
        PreparedStoragePredicate::prepare(predicate, |column| match column.kind {
            ColumnKind::Time => ColumnResolution::Time,
            ColumnKind::Measurement => {
                ColumnResolution::Static(Some(Val::String("cpu".to_string())))
            }
            ColumnKind::LegacyName => {
                ColumnResolution::Static(Some(Val::String("shadow".to_string())))
            }
            ColumnKind::Tag => ColumnResolution::Static(match column.name.as_str() {
                "host" => Some(Val::String("A".to_string())),
                "empty" => Some(Val::String(String::new())),
                _ => None,
            }),
            ColumnKind::Field(_) | ColumnKind::AnyField => ColumnResolution::Field(column.clone()),
            ColumnKind::FieldOrTag if column.name == "temperature" => {
                ColumnResolution::Field(ColumnRef::new(&column.name, ColumnKind::AnyField))
            }
            ColumnKind::FieldOrTag => ColumnResolution::Static(match column.name.as_str() {
                "host" => Some(Val::String("A".to_string())),
                "name" => Some(Val::String("shadow".to_string())),
                _ => None,
            }),
        })
    }

    #[test]
    fn native_evaluator_executes_field_any_field_and_field_or_tag_columns() {
        for kind in [
            ColumnKind::Field(Some(FLOAT)),
            ColumnKind::AnyField,
            ColumnKind::FieldOrTag,
        ] {
            let predicate = compare(
                CompareOp::GreaterThan,
                column("temperature", kind),
                literal(Val::F64(10.0)),
            );
            let prepared = prepare_with_engine_resolution(&predicate);
            assert_eq!(prepared.field_dependencies().len(), 1);
            assert!(prepared.matches(20, &[Some(Val::F64(15.0))]));
            assert!(!prepared.matches(20, &[Some(Val::F64(5.0))]));
            assert!(!prepared.matches(20, &[None]));
        }

        let tag = compare(
            CompareOp::Equal,
            column("host", ColumnKind::FieldOrTag),
            literal(Val::String("A".to_string())),
        );
        let prepared = prepare_with_engine_resolution(&tag);
        assert!(prepared.field_dependencies().is_empty());
        assert!(prepared.matches(0, &[]));

        let missing = compare(
            CompareOp::Equal,
            column("rack", ColumnKind::FieldOrTag),
            literal(Val::String(String::new())),
        );
        assert!(!prepare_with_engine_resolution(&missing).matches(0, &[]));
    }

    #[test]
    fn native_evaluator_distinguishes_measurement_name_ordinary_name_and_legacy_alias() {
        let measurement = compare(
            CompareOp::Equal,
            column("_name", ColumnKind::Measurement),
            literal(Val::String("cpu".to_string())),
        );
        assert!(prepare_with_engine_resolution(&measurement).matches(0, &[]));

        let ordinary = compare(
            CompareOp::Equal,
            column("name", ColumnKind::FieldOrTag),
            literal(Val::String("cpu".to_string())),
        );
        assert!(!prepare_with_engine_resolution(&ordinary).matches(0, &[]));

        let legacy = ordinary.with_legacy_name_alias();
        assert!(!prepare_with_engine_resolution(&legacy).matches(0, &[]));
        let legacy_without_name_tag = PreparedStoragePredicate::prepare(&legacy, |column| {
            assert_eq!(column.kind, ColumnKind::LegacyName);
            ColumnResolution::Static(Some(Val::String("cpu".to_string())))
        });
        assert!(legacy_without_name_tag.matches(0, &[]));
    }

    #[test]
    fn native_evaluator_preserves_regex_missing_and_nested_boolean_states() {
        let empty_regex = StoragePredicate::Regex {
            op: RegexOp::Match,
            value: column("empty", ColumnKind::Tag),
            pattern: "^$".to_string(),
        };
        assert!(prepare_with_engine_resolution(&empty_regex).matches(0, &[]));

        let missing_regex = StoragePredicate::Regex {
            op: RegexOp::NotMatch,
            value: column("rack", ColumnKind::Tag),
            pattern: "^a$".to_string(),
        };
        assert!(!prepare_with_engine_resolution(&missing_regex).matches(0, &[]));

        let inner = compare(
            CompareOp::GreaterThan,
            column("temperature", ColumnKind::AnyField),
            literal(Val::F64(10.0)),
        );
        let nested_true = compare(
            CompareOp::Equal,
            StorageValueExpr::Predicate(Box::new(inner.clone())),
            literal(Val::Bool(true)),
        );
        let nested_false = compare(
            CompareOp::Equal,
            StorageValueExpr::Predicate(Box::new(inner)),
            literal(Val::Bool(false)),
        );
        assert!(prepare_with_engine_resolution(&nested_true).matches(0, &[Some(Val::F64(15.0))]));
        assert!(prepare_with_engine_resolution(&nested_false).matches(0, &[Some(Val::F64(5.0))]));
        assert!(!prepare_with_engine_resolution(&nested_false).matches(0, &[None]));

        let combined = StoragePredicate::Logical {
            op: LogicalOp::Or,
            lhs: Box::new(nested_true),
            rhs: Box::new(compare(
                CompareOp::Equal,
                column("host", ColumnKind::Tag),
                literal(Val::String("A".to_string())),
            )),
        };
        assert!(prepare_with_engine_resolution(&combined).matches(0, &[None]));
    }

    #[test]
    fn native_evaluator_exact_tag_set_distinguishes_empty_from_missing() {
        let empty = StoragePredicate::ExactTagSet {
            tag: ColumnRef::new("empty", ColumnKind::Tag),
            accepted_values: vec!["A".to_string(), String::new(), "A".to_string()],
        };
        assert!(prepare_with_engine_resolution(&empty).matches(0, &[]));

        let missing = StoragePredicate::ExactTagSet {
            tag: ColumnRef::new("rack", ColumnKind::Tag),
            accepted_values: vec!["A".to_string(), String::new(), "A".to_string()],
        };
        assert!(!prepare_with_engine_resolution(&missing).matches(0, &[]));
    }

    #[test]
    fn native_evaluator_executes_arithmetic_bitwise_and_mixed_numeric_values() {
        let cases = [
            (
                arithmetic(
                    ArithmeticOp::Add,
                    literal(Val::I64(7)),
                    literal(Val::I64(5)),
                ),
                Val::I64(12),
            ),
            (
                arithmetic(
                    ArithmeticOp::Subtract,
                    literal(Val::U64(9)),
                    literal(Val::U64(4)),
                ),
                Val::U64(5),
            ),
            (
                arithmetic(
                    ArithmeticOp::Multiply,
                    literal(Val::F64(2.5)),
                    literal(Val::I64(4)),
                ),
                Val::F64(10.0),
            ),
            (
                arithmetic(
                    ArithmeticOp::Divide,
                    literal(Val::I64(9)),
                    literal(Val::I64(2)),
                ),
                Val::I64(4),
            ),
            (
                arithmetic(
                    ArithmeticOp::BitwiseAnd,
                    literal(Val::I64(0b110)),
                    literal(Val::I64(0b101)),
                ),
                Val::I64(0b100),
            ),
            (
                arithmetic(
                    ArithmeticOp::BitwiseOr,
                    literal(Val::U64(0b110)),
                    literal(Val::U64(0b001)),
                ),
                Val::U64(0b111),
            ),
            (
                arithmetic(
                    ArithmeticOp::BitwiseXor,
                    literal(Val::I64(0b110)),
                    literal(Val::I64(0b101)),
                ),
                Val::I64(0b011),
            ),
        ];

        for (expression, expected) in cases {
            let predicate = compare(CompareOp::Equal, expression, literal(expected));
            assert!(prepare_with_engine_resolution(&predicate).matches(0, &[]));
        }
    }
}

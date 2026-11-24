//! Storage-native TSI predicate pushdown.
//!
//! Candidate series may contain false positives, but must never omit a series
//! that could satisfy the full predicate. A residual predicate records the
//! work that still belongs to point-level evaluation.

use crate::models::points::Val;
use regex::Regex;

#[cfg(feature = "slow-trace")]
use crate::{index::TsiSelectionDiagnostics, query::storage_ir::StorageScanProfile};
use crate::{
    query::storage_ir::{
        ColumnKind, ColumnRef, CompareOp, LogicalOp, RegexOp, ScanCancellation, StoragePredicate,
        StorageValueExpr,
    },
    series::series_set::SeriesIDSet,
};

use super::{Error, IndexSet, Result, SeriesIDElem, SeriesIDIterator};

/// Whether the candidate bitmap completely resolves the predicate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CandidateExactness {
    Exact,
    Superset,
}

/// The series selected by TSI and any predicate still requiring row values.
#[derive(Debug)]
pub(crate) struct SeriesFilterResult {
    pub(crate) candidate_series: SeriesIDSet,
    pub(crate) residual_predicate: Option<StoragePredicate>,
    pub(crate) exactness: CandidateExactness,
}

impl SeriesFilterResult {
    fn exact(candidate_series: SeriesIDSet) -> Self {
        Self {
            candidate_series,
            residual_predicate: None,
            exactness: CandidateExactness::Exact,
        }
    }

    fn conservative(candidate_series: SeriesIDSet, residual: StoragePredicate) -> Self {
        Self {
            candidate_series,
            residual_predicate: Some(residual),
            exactness: CandidateExactness::Superset,
        }
    }
}

struct PushdownContext<'a> {
    index_set: &'a IndexSet,
    measurement: &'a [u8],
    cancellation: Option<&'a ScanCancellation>,
    all_series: Option<SeriesIDSet>,
    #[cfg(feature = "slow-trace")]
    profile: Option<&'a StorageScanProfile>,
    #[cfg(feature = "slow-trace")]
    selection_diagnostics: Option<&'a TsiSelectionDiagnostics>,
}

impl<'a> PushdownContext<'a> {
    fn new(
        index_set: &'a IndexSet,
        measurement: &'a [u8],
        cancellation: Option<&'a ScanCancellation>,
    ) -> Self {
        Self {
            index_set,
            measurement,
            cancellation,
            all_series: None,
            #[cfg(feature = "slow-trace")]
            profile: None,
            #[cfg(feature = "slow-trace")]
            selection_diagnostics: None,
        }
    }

    #[cfg(feature = "slow-trace")]
    fn new_profiled(
        index_set: &'a IndexSet,
        measurement: &'a [u8],
        cancellation: Option<&'a ScanCancellation>,
        profile: Option<&'a StorageScanProfile>,
        selection_diagnostics: Option<&'a TsiSelectionDiagnostics>,
    ) -> Self {
        Self {
            index_set,
            measurement,
            cancellation,
            all_series: None,
            profile,
            selection_diagnostics,
        }
    }

    fn check_cancelled(&self) -> Result<()> {
        if self
            .cancellation
            .is_some_and(ScanCancellation::is_cancelled)
        {
            return Err(Error::StoragePredicateCancelled);
        }
        Ok(())
    }

    fn all_series(&mut self) -> Result<SeriesIDSet> {
        if self.all_series.is_none() {
            let itr = self
                .index_set
                .measurement_series_id_iter2(self.measurement)?;
            self.all_series = Some(
                self.index_set
                    .collect_live_series_ids(itr, self.cancellation)?,
            );
        }
        Ok(self
            .all_series
            .as_ref()
            .expect("all-series bitmap is initialized")
            .clone())
    }

    fn filter(&mut self, predicate: &StoragePredicate) -> Result<SeriesFilterResult> {
        self.check_cancelled()?;
        match predicate {
            StoragePredicate::Constant(true) => Ok(SeriesFilterResult::exact(self.all_series()?)),
            StoragePredicate::Constant(false) => {
                Ok(SeriesFilterResult::exact(SeriesIDSet::default()))
            }
            StoragePredicate::Missing => Ok(SeriesFilterResult::exact(SeriesIDSet::default())),
            StoragePredicate::ExactTagSet {
                tag,
                accepted_values,
            } => self.filter_exact_tag_set(predicate, tag, accepted_values),
            StoragePredicate::Value(_) => Ok(SeriesFilterResult::conservative(
                self.all_series()?,
                predicate.clone(),
            )),
            StoragePredicate::Logical { op, lhs, rhs } => match op {
                LogicalOp::And => self.filter_and(lhs, rhs),
                LogicalOp::Or => self.filter_or(predicate, lhs, rhs),
            },
            StoragePredicate::Compare { op, lhs, rhs } => {
                self.filter_comparison(predicate, *op, lhs, rhs)
            }
            StoragePredicate::Regex { op, value, pattern } => {
                self.filter_regex(predicate, *op, value, pattern)
            }
        }
    }

    fn filter_and(
        &mut self,
        lhs: &StoragePredicate,
        rhs: &StoragePredicate,
    ) -> Result<SeriesFilterResult> {
        let left = self.filter(lhs)?;
        let right = self.filter(rhs)?;
        let candidates = left.candidate_series.and(&right.candidate_series);
        let residual = combine_and_residuals(left.residual_predicate, right.residual_predicate);

        Ok(match residual {
            Some(residual) => SeriesFilterResult::conservative(candidates, residual),
            None => SeriesFilterResult::exact(candidates),
        })
    }

    fn filter_or(
        &mut self,
        original: &StoragePredicate,
        lhs: &StoragePredicate,
        rhs: &StoragePredicate,
    ) -> Result<SeriesFilterResult> {
        let left = self.filter(lhs)?;
        let right = self.filter(rhs)?;

        if left.exactness == CandidateExactness::Exact
            && right.exactness == CandidateExactness::Exact
        {
            left.candidate_series
                .merge_in_place(&right.candidate_series);
            return Ok(SeriesFilterResult::exact(left.candidate_series));
        }

        // Narrowing to only one OR branch would exclude series that satisfy the
        // other branch. Retain the full expression for point-level evaluation.
        Ok(SeriesFilterResult::conservative(
            self.all_series()?,
            original.clone(),
        ))
    }

    fn filter_comparison(
        &mut self,
        original: &StoragePredicate,
        op: CompareOp,
        lhs: &StorageValueExpr,
        rhs: &StorageValueExpr,
    ) -> Result<SeriesFilterResult> {
        let Some((column, value)) = column_and_string_literal(lhs, rhs) else {
            return Ok(SeriesFilterResult::conservative(
                self.all_series()?,
                original.clone(),
            ));
        };

        if !matches!(op, CompareOp::Equal | CompareOp::NotEqual) {
            return Ok(SeriesFilterResult::conservative(
                self.all_series()?,
                original.clone(),
            ));
        }

        match self.resolve_column(column) {
            ResolvedColumn::Tag => self.filter_tag_string(column, value.as_bytes(), op),
            ResolvedColumn::Measurement => {
                let equal = value.as_bytes() == self.measurement;
                let matches = match op {
                    CompareOp::Equal => equal,
                    CompareOp::NotEqual => !equal,
                    _ => unreachable!("comparison operator was checked above"),
                };
                if matches {
                    Ok(SeriesFilterResult::exact(self.all_series()?))
                } else {
                    Ok(SeriesFilterResult::exact(SeriesIDSet::default()))
                }
            }
            ResolvedColumn::Residual => Ok(SeriesFilterResult::conservative(
                self.all_series()?,
                original.clone(),
            )),
        }
    }

    fn filter_tag_string(
        &mut self,
        column: &ColumnRef,
        value: &[u8],
        op: CompareOp,
    ) -> Result<SeriesFilterResult> {
        let key = column.name.as_bytes();
        let candidates = match (op, value.is_empty()) {
            (CompareOp::Equal, false) => self.tag_value_series(key, value)?,
            (CompareOp::Equal, true) => {
                let all = self.all_series()?;
                all.and_not(&self.tag_key_series(key)?)
            }
            (CompareOp::NotEqual, false) => {
                let all = self.all_series()?;
                all.and_not(&self.tag_value_series(key, value)?)
            }
            (CompareOp::NotEqual, true) => self.tag_key_series(key)?,
            _ => unreachable!("tag string comparison operator was checked above"),
        };
        Ok(SeriesFilterResult::exact(candidates))
    }

    fn filter_exact_tag_set(
        &mut self,
        original: &StoragePredicate,
        tag: &ColumnRef,
        accepted_values: &[String],
    ) -> Result<SeriesFilterResult> {
        if self.resolve_column(tag) != ResolvedColumn::Tag {
            return Ok(SeriesFilterResult::conservative(
                self.all_series()?,
                original.clone(),
            ));
        }

        let candidates = SeriesIDSet::default();
        for value in accepted_values {
            self.check_cancelled()?;
            candidates
                .merge_in_place(&self.tag_value_series(tag.name.as_bytes(), value.as_bytes())?);
        }
        Ok(SeriesFilterResult::exact(candidates))
    }

    fn filter_regex(
        &mut self,
        original: &StoragePredicate,
        op: RegexOp,
        value: &StorageValueExpr,
        pattern: &str,
    ) -> Result<SeriesFilterResult> {
        let Ok(regex) = Regex::new(pattern) else {
            return Ok(SeriesFilterResult::conservative(
                self.all_series()?,
                original.clone(),
            ));
        };
        let StorageValueExpr::Column(column) = value else {
            return Ok(SeriesFilterResult::conservative(
                self.all_series()?,
                original.clone(),
            ));
        };

        if self.resolve_column(column) != ResolvedColumn::Tag {
            return Ok(SeriesFilterResult::conservative(
                self.all_series()?,
                original.clone(),
            ));
        }

        let key = column.name.as_bytes();
        let matched = self.tag_regex_series(key, &regex)?;

        let candidates = match op {
            RegexOp::Match => matched,
            // Legacy row evaluation has no value for a missing tag, so both
            // regex operators evaluate false for that series. Restrict the
            // complement to series where the tag key exists.
            RegexOp::NotMatch => self.tag_key_series(key)?.and_not(&matched),
        };
        Ok(SeriesFilterResult::exact(candidates))
    }

    fn resolve_column(&self, column: &ColumnRef) -> ResolvedColumn {
        match column.kind {
            ColumnKind::Tag => ResolvedColumn::Tag,
            ColumnKind::Measurement => ResolvedColumn::Measurement,
            ColumnKind::FieldOrTag => {
                if self.index_set.has_field(self.measurement, &column.name) {
                    ResolvedColumn::Residual
                } else {
                    // An unknown column without field metadata follows the tag
                    // path. A missing tag key is represented by its empty value.
                    ResolvedColumn::Tag
                }
            }
            ColumnKind::Field(_)
            | ColumnKind::AnyField
            | ColumnKind::Time
            | ColumnKind::LegacyName => ResolvedColumn::Residual,
        }
    }

    fn tag_key_series(&self, key: &[u8]) -> Result<SeriesIDSet> {
        let itr = self
            .index_set
            .tag_key_series_id_iter2(self.measurement, key)?;
        self.index_set
            .collect_live_series_ids(itr, self.cancellation)
    }

    fn tag_value_series(&self, key: &[u8], value: &[u8]) -> Result<SeriesIDSet> {
        #[cfg(feature = "slow-trace")]
        if let Some(profile) = self.profile {
            profile.record_tsi_exact_lookup();
        }
        #[cfg(feature = "slow-trace")]
        let itr = if let Some(diagnostics) = self.selection_diagnostics {
            self.index_set.tag_value_series_id_iter2_profiled(
                self.measurement,
                key,
                value,
                diagnostics,
            )?
        } else {
            self.index_set
                .tag_value_series_id_iter2(self.measurement, key, value)?
        };
        #[cfg(not(feature = "slow-trace"))]
        let itr = self
            .index_set
            .tag_value_series_id_iter2(self.measurement, key, value)?;
        self.index_set
            .collect_live_series_ids(itr, self.cancellation)
    }

    fn tag_regex_series(&mut self, key: &[u8], regex: &Regex) -> Result<SeriesIDSet> {
        let matched = SeriesIDSet::default();
        // Tag-value iterators use an empty byte slice as their end sentinel.
        // Preserve the separately indexed, present-empty tag value through an
        // exact lookup before scanning the non-empty values.
        if regex.is_match("") {
            matched.merge_in_place(&self.tag_value_series(key, b"")?);
        }
        if let Some(mut values) = self.index_set.tag_value_iter(self.measurement, key)? {
            let mut scanned = 0usize;
            loop {
                if scanned % CANCELLATION_CHECK_INTERVAL == 0
                    && let Err(error) = self.check_cancelled()
                {
                    let _ = values.close();
                    return Err(error);
                }
                let Some(value) = values.next() else {
                    break;
                };
                scanned += 1;
                let Ok(value_text) = std::str::from_utf8(&value) else {
                    continue;
                };
                if regex.is_match(value_text) {
                    matched.merge_in_place(&self.tag_value_series(key, &value)?);
                }
            }
            let _ = values.close();
        }

        Ok(matched)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ResolvedColumn {
    Tag,
    Measurement,
    Residual,
}

impl IndexSet {
    /// Filters one measurement with storage-native predicate semantics.
    pub(crate) fn filter_series_by_storage_predicate(
        &self,
        measurement: &[u8],
        predicate: &StoragePredicate,
    ) -> Result<SeriesFilterResult> {
        self.filter_series_by_storage_predicate_with_cancellation(measurement, predicate, None)
    }

    pub(crate) fn filter_series_by_storage_predicate_with_cancellation(
        &self,
        measurement: &[u8],
        predicate: &StoragePredicate,
        cancellation: Option<&ScanCancellation>,
    ) -> Result<SeriesFilterResult> {
        PushdownContext::new(self, measurement, cancellation).filter(predicate)
    }

    #[cfg(feature = "slow-trace")]
    pub(crate) fn filter_series_by_storage_predicate_profiled(
        &self,
        measurement: &[u8],
        predicate: &StoragePredicate,
        cancellation: Option<&ScanCancellation>,
        profile: Option<&StorageScanProfile>,
        selection_diagnostics: Option<&TsiSelectionDiagnostics>,
    ) -> Result<SeriesFilterResult> {
        PushdownContext::new_profiled(
            self,
            measurement,
            cancellation,
            profile,
            selection_diagnostics,
        )
        .filter(predicate)
    }

    fn collect_live_series_ids<I>(
        &self,
        itr: Option<I>,
        cancellation: Option<&ScanCancellation>,
    ) -> Result<SeriesIDSet>
    where
        I: SeriesIDIterator,
    {
        let series = SeriesIDSet::default();
        let Some(mut itr) = itr else {
            return Ok(series);
        };

        let mut scanned = 0usize;
        loop {
            if scanned % CANCELLATION_CHECK_INTERVAL == 0
                && cancellation.is_some_and(ScanCancellation::is_cancelled)
            {
                let _ = itr.close();
                return Err(Error::StoragePredicateCancelled);
            }
            let Some(SeriesIDElem { series_id, .. }) = itr.next() else {
                break;
            };
            scanned += 1;
            if series_id != 0 && !self.series_file.series_key(series_id).is_empty() {
                series.add_no_lock(series_id);
            }
        }
        let _ = itr.close();
        Ok(series)
    }
}

const CANCELLATION_CHECK_INTERVAL: usize = 1024;

fn column_and_string_literal<'a>(
    lhs: &'a StorageValueExpr,
    rhs: &'a StorageValueExpr,
) -> Option<(&'a ColumnRef, &'a String)> {
    match (lhs, rhs) {
        (StorageValueExpr::Column(column), StorageValueExpr::Literal(Val::String(value)))
        | (StorageValueExpr::Literal(Val::String(value)), StorageValueExpr::Column(column)) => {
            Some((column, value))
        }
        _ => None,
    }
}

fn combine_and_residuals(
    lhs: Option<StoragePredicate>,
    rhs: Option<StoragePredicate>,
) -> Option<StoragePredicate> {
    match (lhs, rhs) {
        (None, None) => None,
        (Some(predicate), None) | (None, Some(predicate)) => Some(predicate),
        (Some(lhs), Some(rhs)) => Some(StoragePredicate::Logical {
            op: LogicalOp::And,
            lhs: Box::new(lhs),
            rhs: Box::new(rhs),
        }),
    }
}

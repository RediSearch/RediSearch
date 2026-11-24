/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Experimental access-path decision tree for filtered KNN.

use std::sync::Arc;

use field_spec::{FieldSpec, FieldSpecType};
use index_result::RSIndexResult;
use index_spec::IndexSpecReadGuard;
use inverted_index::NumericFilter;
use redis_reply::MapBuilder;
use rqe_core::DocId;
use rqe_iterators::{
    IteratorType, RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome,
    profile_print::{ProfilePrint, ProfilePrintCtx},
    utils::{AnyTimeoutContext, TimeoutContext},
};

use crate::{Evaluated, QueryEvalContext, QueryNode, QueryNodeRef};

enum Predicate {
    Single(NumericFilter),
    And(NumericFilter, NumericFilter),
    Or(NumericFilter, NumericFilter),
}

fn leaf(node: QueryNodeRef) -> Option<NumericFilter> {
    match node.as_enum() {
        QueryNode::Numeric { nf } if nf.is_numeric_filter() => Some(*nf),
        _ => None,
    }
}

fn predicate(node: QueryNodeRef) -> Option<Predicate> {
    let combine = match node.as_enum() {
        QueryNode::Phrase { exact: false } if node.num_children() == 2 => Predicate::And,
        QueryNode::Union if node.num_children() == 2 => Predicate::Or,
        _ => return leaf(node).map(Predicate::Single),
    };
    Some(combine(leaf(node.child(0))?, leaf(node.child(1))?))
}

struct Clause {
    filter: NumericFilter,
    values: Arc<[f64]>,
}

impl Clause {
    fn prepare(ctx: &QueryEvalContext, filter: NumericFilter, docs: usize) -> Option<Self> {
        let spec = ctx.spec();
        let field_idx = filter.field_index as usize;
        if field_idx >= spec.numFields as usize {
            return None;
        }
        // SAFETY: field_idx is within the field array pinned by the spec read lock.
        let field_ptr = unsafe { spec.fields.add(field_idx) };
        // SAFETY: the read lock also pins the field and its attached name buffers and tree.
        let field = unsafe { FieldSpec::from_raw(field_ptr) };
        if !field.types().contains(FieldSpecType::Numeric) {
            return None;
        }
        let tree = field.tree()?;
        if tree.find(&filter).len() < 4 {
            return None;
        }
        Some(Self {
            filter,
            values: tree.dense_values(docs)?,
        })
    }

    #[inline]
    fn matches(&self, id: usize) -> bool {
        self.filter.value_in_range(self.values[id])
    }
}

enum PreparedPredicate {
    Single(Clause),
    And(Clause, Clause),
    Or(Clause, Clause),
}

impl PreparedPredicate {
    fn seek(&self, mut ids: std::ops::Range<usize>) -> Option<usize> {
        match self {
            Self::Single(a) => ids.find(|&id| a.matches(id)),
            Self::And(a, b) => ids.find(|&id| a.matches(id) && b.matches(id)),
            Self::Or(a, b) => ids.find(|&id| a.matches(id) || b.matches(id)),
        }
    }

    fn prefers_exact(&self, docs: usize) -> bool {
        let Self::And(a, b) = self else {
            return false;
        };
        // Sample the conjunction itself so that correlated columns are not estimated
        // by multiplying the selectivity of the two ranges.
        let stride = docs.div_ceil(1024);
        // Use exact search in the range where a failed batch attempt can cost as much
        // as scoring the matching documents. Keep high-eligibility conjunctions in AUTO.
        let max_matches = docs.div_ceil(stride) / 10;
        (1..=docs)
            .step_by(stride)
            .filter(|&id| a.matches(id) && b.matches(id))
            .take(max_matches + 1)
            .count()
            <= max_matches
    }
}

pub(crate) struct DensePlan {
    predicate: PreparedPredicate,
    docs: usize,
    execution_hint: ffi::NumericKnnPlan,
}

impl DensePlan {
    pub(crate) const fn execution_hint(&self) -> ffi::NumericKnnPlan {
        self.execution_hint
    }

    pub(crate) fn into_iterator<'index>(
        self,
        regular: Evaluated<'index>,
        timeout: AnyTimeoutContext,
    ) -> Evaluated<'index> {
        // Keep the legacy estimate even when the dense lookup could count the exact
        // matches. The estimate also selects HNSW's initial strategy and batch sizes.
        let estimated = regular.into_boxed().num_estimated();
        Evaluated::RustLeaf(Box::new(DenseIterator {
            plan: self,
            estimated,
            timeout,
            result: RSIndexResult::build_virt().weight(1.0).build(),
            exhausted: false,
        }))
    }
}

pub(crate) fn try_plan(
    ctx: &QueryEvalContext,
    filter: QueryNodeRef,
    vq: &ffi::VectorQuery,
) -> Option<DensePlan> {
    if vq.type_ != ffi::VectorQueryType_VECSIM_QT_KNN
        || ctx.opts().flags & ffi::RSSearchFlags_Search_CanSkipRichResults == 0
        || !ctx.spec().diskSpec.is_null()
        || !ctx.doc_table().ttl.is_null()
    {
        return None;
    }
    let docs = usize::try_from(ctx.max_doc_id()).ok()?;
    // SAFETY: the query type was checked above.
    let k = unsafe { vq.__bindgen_anon_1.knn.k };
    if !(50_000..=1_000_000).contains(&docs)
        || !(1..=128).contains(&k)
        || ctx.spec().stats.scoring.numDocuments != docs
    {
        return None;
    }

    let prepare = |filter| Clause::prepare(ctx, filter, docs);
    let predicate = match predicate(filter)? {
        Predicate::Single(a) => PreparedPredicate::Single(prepare(a)?),
        Predicate::And(a, b) => PreparedPredicate::And(prepare(a)?, prepare(b)?),
        Predicate::Or(a, b) => PreparedPredicate::Or(prepare(a)?, prepare(b)?),
    };
    let execution_hint = if k <= 32 && predicate.prefers_exact(docs) {
        ffi::NumericKnnPlan_NumericKnnExact
    } else {
        ffi::NumericKnnPlan_NumericKnnDense
    };
    Some(DensePlan {
        predicate,
        docs,
        execution_hint,
    })
}

struct DenseIterator<'index> {
    plan: DensePlan,
    estimated: usize,
    timeout: AnyTimeoutContext,
    result: RSIndexResult<'index>,
    exhausted: bool,
}

impl<'index> DenseIterator<'index> {
    fn seek(
        &mut self,
        target: DocId,
    ) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.exhausted {
            return Ok(None);
        }
        let mut first = usize::try_from(target).unwrap_or(usize::MAX);
        let end = self.plan.docs + 1;
        while first < end {
            self.timeout.check_timeout()?;
            // Long stretches without a matching document still yield deadline probes.
            let limit = (first + 128).min(end);
            if let Some(id) = self.plan.predicate.seek(first..limit) {
                self.result.doc_id = id as DocId;
                return Ok(Some(&mut self.result));
            }
            first = limit;
        }
        self.exhausted = true;
        Ok(None)
    }
}

impl<'index> RQEIterator<'index> for DenseIterator<'index> {
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        (!self.exhausted).then_some(&mut self.result)
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        self.seek(self.result.doc_id + 1)
    }

    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        Ok(self.seek(doc_id)?.map(|result| {
            if result.doc_id == doc_id {
                SkipToOutcome::Found(result)
            } else {
                SkipToOutcome::NotFound(result)
            }
        }))
    }

    fn rewind(&mut self) {
        self.result.doc_id = 0;
        self.exhausted = false;
    }

    fn num_estimated(&self) -> usize {
        self.estimated
    }

    fn last_doc_id(&self) -> DocId {
        self.result.doc_id
    }

    fn at_eof(&self) -> bool {
        self.exhausted
    }

    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        let docs = self.plan.docs as DocId;
        // SAFETY: the guard holds the read lock for this live index spec.
        let spec = unsafe { &*spec.as_ptr() };
        if spec.docs.maxDocId == docs
            && spec.stats.scoring.numDocuments as DocId == docs
            && spec.docs.ttl.is_null()
        {
            Ok(RQEValidateStatus::Ok)
        } else {
            Ok(RQEValidateStatus::Aborted)
        }
    }

    fn type_(&self) -> IteratorType {
        IteratorType::IdListSorted
    }

    fn intersection_sort_weight(&self, _: bool) -> f64 {
        1.0
    }
}

impl ProfilePrint for DenseIterator<'_> {
    fn print_profile(&self, map: &mut MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        ctx.print_leaf(c"DENSE-NUMERIC-FILTER", map);
    }
}

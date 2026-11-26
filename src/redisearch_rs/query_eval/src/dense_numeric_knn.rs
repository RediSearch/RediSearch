/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Dense numeric range filters for KNN on in-memory indexes.

use std::{ptr::NonNull, sync::Arc};

use field_spec::{FieldSpec, FieldSpecType};
use index_result::{RSIndexResult, SuspendedIndexResult};
use index_spec::IndexSpecReadGuard;
use inverted_index::NumericFilter;
use redis_reply::MapBuilder;
use rqe_core::DocId;
use rqe_iterators::{
    IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    RQEValidateStatus, ResumeOutcome, SkipToOutcome,
    boxed::assert_layout_compatible,
    c2rust::{CRQEIterator, CRQESuspendedIterator},
    interop::RQEIteratorWrapper,
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
    fn prepare(
        ctx: &QueryEvalContext,
        filter: NumericFilter,
        docs: usize,
        timeout: &mut AnyTimeoutContext,
    ) -> Option<Self> {
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
        let can_cache = || {
            // SAFETY: the C helper takes no arguments and uses worker-safe reads.
            unsafe { ffi::RSConfig_DenseNumericKnnCanCache() }
        };
        Some(Self {
            filter,
            values: tree.dense_values(docs, || timeout.check_timeout().is_ok(), can_cache)?,
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
}

pub(crate) struct DensePlan {
    predicate: PreparedPredicate,
    docs: usize,
}

impl DensePlan {
    pub(crate) fn into_iterator(
        self,
        regular: Evaluated<'_>,
        timeout: AnyTimeoutContext,
    ) -> NonNull<ffi::QueryIterator> {
        // Keep the legacy estimate even when the dense lookup could count the exact
        // matches. The estimate also selects HNSW's initial strategy and batch sizes.
        // SAFETY: `into_c_iterator` returns an owning handle with the standard iterator
        // callbacks installed. The dense iterator takes ownership of that handle here.
        let regular = unsafe { CRQEIterator::new(regular.into_c_iterator()) };
        let estimated = regular.num_estimated();
        RQEIteratorWrapper::boxed_new(DenseIterator {
            plan: Some(self),
            regular: Some(Box::new(regular)),
            estimated,
            timeout,
            result: RSIndexResult::build_virt().weight(1.0).build(),
            first_doc: None,
            exhausted: false,
        })
    }
}

pub(crate) fn try_plan(
    ctx: &QueryEvalContext,
    filter: QueryNodeRef,
    vq: &ffi::VectorQuery,
    timeout: &mut AnyTimeoutContext,
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

    let mut prepare = |filter| Clause::prepare(ctx, filter, docs, timeout);
    let predicate = match predicate(filter)? {
        Predicate::Single(a) => PreparedPredicate::Single(prepare(a)?),
        Predicate::And(a, b) => PreparedPredicate::And(prepare(a)?, prepare(b)?),
        Predicate::Or(a, b) => PreparedPredicate::Or(prepare(a)?, prepare(b)?),
    };
    Some(DensePlan { predicate, docs })
}

#[repr(C)]
struct RawDenseIterator<Result, Child> {
    plan: Option<DensePlan>,
    // Retain the untouched postings iterator for a query that releases its
    // read lock and resumes after the dense document domain has changed.
    regular: Option<Box<Child>>,
    estimated: usize,
    timeout: AnyTimeoutContext,
    result: Result,
    first_doc: Option<DocId>,
    exhausted: bool,
}

type DenseIterator<'index> = RawDenseIterator<RSIndexResult<'index>, CRQEIterator>;
type SuspendedDenseIterator<'query> =
    RawDenseIterator<SuspendedIndexResult<'query>, CRQESuspendedIterator>;

const _: () = assert_layout_compatible::<DenseIterator<'static>, SuspendedDenseIterator<'static>>();

impl<Result, Child> RawDenseIterator<Result, Child> {
    fn valid_domain(&self, guard: &IndexSpecReadGuard) -> bool {
        let Some(plan) = &self.plan else { return false };
        let docs = plan.docs as DocId;
        // SAFETY: the guard holds the read lock for this live index spec.
        let state = unsafe { &*guard.as_ptr() };
        state.docs.maxDocId == docs
            && state.stats.scoring.numDocuments as DocId == docs
            && state.docs.ttl.is_null()
    }
}

impl<'index> DenseIterator<'index> {
    fn read_first(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        // BATCHES rewinds the filter for each batch. Postings can jump straight to
        // the first match even if the dense array has a long non-matching prefix.
        let id = match (self.first_doc, &mut self.regular) {
            (Some(id), _) => Some(id),
            (None, Some(regular)) => regular.read()?.map(|result| result.doc_id),
            (None, None) => return self.seek(1),
        };
        self.first_doc = id;
        match id {
            Some(id) => {
                self.result.doc_id = id;
                Ok(Some(&mut self.result))
            }
            None => {
                self.exhausted = true;
                Ok(None)
            }
        }
    }

    fn seek(
        &mut self,
        target: DocId,
    ) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        let mut first = usize::try_from(target).unwrap_or(usize::MAX);
        let plan = self.plan.as_ref().expect("dense iteration requires a plan");
        let end = plan.docs + 1;
        while first < end {
            self.timeout.check_timeout()?;
            // Long stretches without a matching document still yield deadline probes.
            let limit = (first + 128).min(end);
            if let Some(id) = plan.predicate.seek(first..limit) {
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
        if self.exhausted {
            return None;
        }
        match self.plan {
            Some(_) => Some(&mut self.result),
            None => self.regular.as_mut()?.current(),
        }
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.exhausted {
            return Ok(None);
        }
        match self.plan {
            Some(_) if self.result.doc_id == 0 => self.read_first(),
            Some(_) => self.seek(self.result.doc_id + 1),
            None => self
                .regular
                .as_mut()
                .map_or(Ok(None), |regular| regular.read()),
        }
    }

    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        if self.exhausted {
            return Ok(None);
        }
        if self.plan.is_none() {
            return self
                .regular
                .as_mut()
                .map_or(Ok(None), |regular| regular.skip_to(doc_id));
        }
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
        if self.plan.is_none() {
            if let Some(regular) = &mut self.regular {
                regular.rewind();
            } else {
                self.exhausted = true;
            }
        }
    }

    fn num_estimated(&self) -> usize {
        self.estimated
    }

    fn last_doc_id(&self) -> DocId {
        if self.plan.is_none() && !self.exhausted {
            if let Some(regular) = &self.regular {
                return regular.last_doc_id();
            }
        }
        self.result.doc_id
    }

    fn at_eof(&self) -> bool {
        self.exhausted
            || (self.plan.is_none() && self.regular.as_ref().is_none_or(|regular| regular.at_eof()))
    }

    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        if self.valid_domain(spec) {
            return Ok(RQEValidateStatus::Ok);
        }
        if self.exhausted {
            self.plan = None;
            return Ok(RQEValidateStatus::Ok);
        }
        if self.plan.is_none() {
            return self
                .regular
                .as_mut()
                .map_or(Ok(RQEValidateStatus::Aborted), |regular| {
                    regular.revalidate(spec)
                });
        }
        let Some(regular) = self.regular.as_mut() else {
            return Ok(RQEValidateStatus::Aborted);
        };
        if matches!(regular.revalidate(spec)?, RQEValidateStatus::Aborted) {
            return Ok(RQEValidateStatus::Aborted);
        }
        self.fall_back()
    }

    fn type_(&self) -> IteratorType {
        IteratorType::IdListSorted
    }

    fn intersection_sort_weight(&self, _: bool) -> f64 {
        1.0
    }
}

impl<'index> DenseIterator<'index> {
    fn fall_back(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        self.plan = None;
        if self.exhausted {
            return Ok(RQEValidateStatus::Ok);
        }
        let Some(regular) = self.regular.as_mut() else {
            return Ok(RQEValidateStatus::Aborted);
        };
        let id = self.result.doc_id;
        if id == 0 {
            return Ok(RQEValidateStatus::Ok);
        }
        if regular.last_doc_id() < id && !regular.at_eof() {
            regular.skip_to(id)?;
        }
        if !regular.at_eof() && regular.last_doc_id() == id {
            return Ok(RQEValidateStatus::Ok);
        }
        Ok(RQEValidateStatus::Moved {
            current: regular.current(),
        })
    }
}

impl<'index> RQEIteratorBoxed<'index> for DenseIterator<'index> {
    type Suspended = SuspendedDenseIterator<'index>;

    fn suspend(mut self: Box<Self>) -> Box<Self::Suspended> {
        let regular = self.regular.take().map(|child| child.suspend());
        let raw = Box::into_raw(self);
        // SAFETY: `raw` uniquely owns this iterator. Convert the only result in place,
        // keeping its allocation at the same address.
        unsafe {
            RSIndexResult::into_suspended_in_place(NonNull::new_unchecked(&raw mut (*raw).result))
        };
        // SAFETY: Both forms are `repr(C)` with matching field layouts. The child
        // slot is None and the result now holds the suspended form.
        let mut suspended = unsafe { Box::from_raw(raw.cast::<Self::Suspended>()) };
        suspended.regular = regular;
        suspended
    }
}

impl<'query> RQESuspendedIterator<'query> for SuspendedDenseIterator<'query> {
    type Resumed<'a>
        = DenseIterator<'a>
    where
        'query: 'a;

    fn resume<'a>(
        mut self: Box<Self>,
        guard: &IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        let (regular, moved) = match self
            .regular
            .take()
            .map(|child| child.resume(guard))
            .transpose()?
        {
            Some(ResumeOutcome::Ok(child)) => (Some(child), false),
            Some(ResumeOutcome::Moved(child)) => (Some(child), true),
            None | Some(ResumeOutcome::Aborted) => (None, false),
        };
        let raw = Box::into_raw(self);
        // SAFETY: `raw` uniquely owns the iterator. The dense record is virtual and
        // contains no index-backed borrows. The original postings child is resumed separately.
        unsafe {
            SuspendedIndexResult::into_active_in_place(NonNull::new_unchecked(
                &raw mut (*raw).result,
            ))
        };
        // SAFETY: Both forms are `repr(C)` with matching field layouts. The child
        // slot is None and the result now holds the active form for `'a`.
        let mut active = unsafe { Box::from_raw(raw.cast::<Self::Resumed<'a>>()) };
        active.regular = regular;
        if active.valid_domain(guard) {
            return Ok(ResumeOutcome::Ok(active));
        }
        if active.plan.is_some() {
            return Ok(match active.fall_back()? {
                RQEValidateStatus::Ok => ResumeOutcome::Ok(active),
                RQEValidateStatus::Moved { .. } => ResumeOutcome::Moved(active),
                RQEValidateStatus::Aborted => ResumeOutcome::Aborted,
            });
        }
        Ok(if active.exhausted {
            ResumeOutcome::Ok(active)
        } else if active.regular.is_none() {
            ResumeOutcome::Aborted
        } else if moved {
            ResumeOutcome::Moved(active)
        } else {
            ResumeOutcome::Ok(active)
        })
    }

    fn last_doc_id(&self) -> DocId {
        if self.plan.is_none() && !self.exhausted {
            if let Some(regular) = &self.regular {
                return regular.last_doc_id();
            }
        }
        self.result.doc_id
    }

    fn num_estimated(&self) -> usize {
        self.estimated
    }
}

impl<Result, Child: ProfilePrint> ProfilePrint for RawDenseIterator<Result, Child> {
    fn print_profile(&self, map: &mut MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        if self.plan.is_some() {
            ctx.print_leaf(c"DENSE-NUMERIC-FILTER", map);
        } else if let Some(regular) = &self.regular {
            regular.print_profile(map, ctx);
        } else {
            ctx.print_leaf(c"EMPTY", map);
        }
    }
}

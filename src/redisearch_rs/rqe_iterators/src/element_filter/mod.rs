/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`ElementFilter`], the iterator of a same-element query node.
//!
//! Its numeric predicates are tested on the values that the candidate elements hold, read from
//! the document itself, which avoids scanning the numeric index for a range that most elements
//! are in.

mod values;

use std::ops::ControlFlow;
use std::ptr::NonNull;

use index_result::{RSIndexResult, RSResultKind, RawIndexResult};
use index_spec::IndexSpecReadGuard;
use inverted_index::NumericFilter;
use ref_mode::{Active, Ref, Suspended};
use rqe_core::{DocId, RS_FIELDMASK_ALL};

pub use values::{DocumentElementValues, ElementValues, SortableElementValues};

use crate::{
    IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    RQEValidateStatus, ResumeOutcome, SkipToOutcome,
    boxed::{
        ResumeSlotOutcome, finish_resume_in_place, resume_child_slot_in_place,
        suspend_child_slot_in_place,
    },
    c2rust::CRQEIterator,
    interop::ProfileChildren,
    profile_print::{ProfilePrint, ProfilePrintCtx},
    utils::{AnyTimeoutContext, TimeoutContext},
};

/// A numeric condition on one field of an array element.
#[derive(Debug, Clone, Copy)]
pub struct ElementPredicate {
    /// The range that the element's value must be in.
    pub filter: NumericFilter,
    /// Which of a document's per-element fields holds the value, as
    /// [`DocumentElementValues::value`] numbers them.
    pub slot: usize,
}

/// Which elements of a document that the child yields are tested against the predicates.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Candidates {
    /// The [common positions](RSIndexResult::visit_common_positions) of the child's result:
    /// the child matches on specific elements and reports their numbers as positions.
    CommonPositions,
    /// Every element of the document: the child only says which documents to look at. The
    /// elements of a document are those of the field of the first predicate, so there must
    /// be one.
    AllElements,
}

/// The predicates of an [`ElementFilter`], the elements they are tested on, and the values
/// of those elements.
struct ElementCondition<V> {
    candidates: Candidates,
    predicates: Vec<ElementPredicate>,
    values: V,
}

impl<V: ElementValues> ElementCondition<V> {
    /// Whether a candidate element of the document of `result` passes. Without predicates,
    /// whether the result has a common position.
    fn holds_for(&self, result: &RSIndexResult) -> bool {
        // The values are only read when a predicate needs them.
        let document = if self.predicates.is_empty() {
            None
        } else {
            let Some(document) = self.values.document(result.doc_id) else {
                return false;
            };
            Some(document)
        };
        let satisfies_all = |element| {
            self.predicates.iter().all(|predicate| {
                document
                    .as_ref()
                    .and_then(|document| document.value(predicate.slot, element))
                    .is_some_and(|value| predicate.filter.value_in_range(value))
            })
        };
        match self.candidates {
            Candidates::CommonPositions => result
                .visit_common_positions(|element| {
                    if satisfies_all(element) {
                        ControlFlow::Break(())
                    } else {
                        ControlFlow::Continue(())
                    }
                })
                .is_break(),
            Candidates::AllElements => {
                // An element that passes has a value for the first predicate.
                let first = self
                    .predicates
                    .first()
                    .expect("checked by `ElementFilter::new`");
                let elements = document
                    .as_ref()
                    .map_or(0, |document| document.num_elements(first.slot));
                (0..elements).any(satisfies_all)
            }
        }
    }
}

/// Yields the documents of `child` in which one of the [`Candidates`] passes: it has a value
/// for every [`ElementPredicate`] and each value is in the predicate's range.
///
/// The yielded results are virtual: they name the document and carry the node's weight,
/// and nothing of the child's results. So the element numbers that the child reports as
/// positions never reach a consumer that would read them as word positions.
///
/// A query can reject any number of candidates in a row, so the iterator probes its
/// [`TimeoutContext`] for each one it rejects.
///
/// Parameterised over a [`Ref`] mode — see [`ElementFilter`] for the [`Active`]
/// instantiation that implements [`RQEIterator`].
///
/// # Invariants
///
/// 1. **Layout compatibility across modes.** `RawElementFilter` is `#[repr(C)]`. Its only
///    `Rf`-dependent field is `result`, which is layout-compatible across `Rf` (proven in
///    `index_result`), and only `child` depends on `I`, whose compatibility with
///    `I::Suspended` [`suspend_child_slot_in_place`] enforces. This is what lets
///    [`suspend`](RQEIteratorBoxed::suspend) / [`resume`](RQESuspendedIterator::resume)
///    reinterpret the owning `Box` in place. Proven by the `const _` block below.
#[repr(C)]
pub struct RawElementFilter<'query, Rf: Ref, I, V, TC> {
    child: I,
    condition: ElementCondition<V>,
    /// The yielded result, which holds the id of the last yielded document.
    result: RawIndexResult<'query, Rf>,
    timeout_ctx: TC,
}

/// Alias for an [`Active`] [`RawElementFilter`] — the only instantiation with an
/// [`RQEIterator`] impl.
pub type ElementFilter<'index, I, V, TC> = RawElementFilter<'index, Active<'index>, I, V, TC>;

/// The [`ElementFilter`] that query evaluation builds.
pub type QueryElementFilter<'index> =
    ElementFilter<'index, CRQEIterator, SortableElementValues, AnyTimeoutContext>;

// Compile-time proof of invariant 1 on `RawElementFilter` for a representative child whose
// active and suspended forms differ.
const _: () = {
    use crate::Wildcard;
    use std::mem::{align_of, offset_of, size_of};
    type AChild = Wildcard<'static>;
    type SChild = <Wildcard<'static> as RQEIteratorBoxed<'static>>::Suspended;
    type A = RawElementFilter<
        'static,
        Active<'static>,
        AChild,
        SortableElementValues,
        AnyTimeoutContext,
    >;
    type S = RawElementFilter<'static, Suspended, SChild, SortableElementValues, AnyTimeoutContext>;
    assert!(offset_of!(A, child) == offset_of!(S, child));
    assert!(offset_of!(A, condition) == offset_of!(S, condition));
    assert!(offset_of!(A, result) == offset_of!(S, result));
    assert!(offset_of!(A, timeout_ctx) == offset_of!(S, timeout_ctx));
    assert!(size_of::<A>() == size_of::<S>());
    assert!(align_of::<A>() == align_of::<S>());
};

impl<'index, I, V, TC> ElementFilter<'index, I, V, TC> {
    /// Creates the filter of `child`, which reads the values of the elements from `values`.
    /// `weight` is the weight of the yielded results.
    ///
    /// # Panics
    ///
    /// Panics for [`Candidates::AllElements`] without a predicate.
    pub fn new(
        child: I,
        weight: f64,
        candidates: Candidates,
        predicates: Vec<ElementPredicate>,
        values: V,
        timeout_ctx: TC,
    ) -> Self {
        assert!(
            candidates == Candidates::CommonPositions || !predicates.is_empty(),
            "the elements of a document are those of the first predicate's field"
        );
        Self {
            child,
            condition: ElementCondition {
                candidates,
                predicates,
                values,
            },
            result: RSIndexResult::build_virt()
                .weight(weight)
                .field_mask(RS_FIELDMASK_ALL)
                .build(),
            timeout_ctx,
        }
    }

    /// The iterator that names the candidate documents and elements.
    pub const fn child(&self) -> &I {
        &self.child
    }
}

impl<'index, I, V, TC> ElementFilter<'index, I, V, TC>
where
    I: RQEIterator<'index>,
    V: ElementValues,
    TC: TimeoutContext,
{
    /// Move to the document the child is on when it passes, else to the next one that does,
    /// and yield it.
    fn yield_current_or_next_match(
        &mut self,
    ) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        let mut candidate = self.child.current();
        loop {
            let Some(result) = candidate else {
                return Ok(None);
            };
            if self.condition.holds_for(result) {
                self.result.doc_id = result.doc_id;
                self.timeout_ctx.reset_counter();
                return Ok(Some(&mut self.result));
            }
            self.timeout_ctx.check_timeout()?;
            candidate = self.child.read()?;
        }
    }
}

impl<'index, I, V, TC> RQEIterator<'index> for ElementFilter<'index, I, V, TC>
where
    I: RQEIterator<'index>,
    V: ElementValues,
    TC: TimeoutContext,
{
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        (!self.child.at_eof()).then_some(&mut self.result)
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.child.read()?.is_none() {
            return Ok(None);
        }
        self.yield_current_or_next_match()
    }

    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        if self.child.skip_to(doc_id)?.is_none() {
            return Ok(None);
        }
        Ok(self.yield_current_or_next_match()?.map(|result| {
            if result.doc_id == doc_id {
                SkipToOutcome::Found(result)
            } else {
                SkipToOutcome::NotFound(result)
            }
        }))
    }

    fn rewind(&mut self) {
        self.result.doc_id = 0;
        self.child.rewind();
    }

    fn num_estimated(&self) -> usize {
        self.child.num_estimated()
    }

    fn last_doc_id(&self) -> DocId {
        self.result.doc_id
    }

    fn at_eof(&self) -> bool {
        self.child.at_eof()
    }

    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        match self.child.revalidate(spec)? {
            RQEValidateStatus::Ok => Ok(RQEValidateStatus::Ok),
            RQEValidateStatus::Aborted => Ok(RQEValidateStatus::Aborted),
            // The child moved to a document that the condition has not seen.
            RQEValidateStatus::Moved { .. } => Ok(RQEValidateStatus::Moved {
                current: self.yield_current_or_next_match()?,
            }),
        }
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        IteratorType::ElementFilter
    }

    fn intersection_sort_weight(&self, prioritize_union_children: bool) -> f64 {
        self.child
            .intersection_sort_weight(prioritize_union_children)
    }
}

impl<'index, V, TC> ProfileChildren<'index> for ElementFilter<'index, CRQEIterator, V, TC>
where
    V: ElementValues + 'index,
    TC: TimeoutContext + 'index,
{
    fn profile_children(self) -> Self {
        Self {
            child: self.child.into_profiled(),
            condition: self.condition,
            result: self.result,
            timeout_ctx: self.timeout_ctx,
        }
    }
}

impl<'index, I, V, TC> ProfilePrint for ElementFilter<'index, I, V, TC>
where
    I: RQEIterator<'index> + ProfilePrint,
{
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        ctx.print_single_child(c"ELEMENT-FILTER", Some(&self.child), map);
    }
}

impl<'index, I, V, TC> RQEIteratorBoxed<'index> for ElementFilter<'index, I, V, TC>
where
    I: RQEIteratorBoxed<'index>,
    V: ElementValues + 'index,
    TC: TimeoutContext + 'index,
{
    type Suspended = RawElementFilter<'index, Suspended, I::Suspended, V, TC>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: `raw` came from `Box::into_raw`, so it is valid and exclusively owned.
        // `&raw mut` forms the field pointer without creating a reference.
        let child_slot = unsafe { &raw mut (*raw).child };
        // SAFETY: a field pointer derived from the non-null `raw` is non-null.
        let child_slot = unsafe { NonNull::new_unchecked(child_slot) };
        // SAFETY: `child_slot` points at the valid, exclusively owned active child, which
        // the call replaces in place with its suspended form.
        unsafe { suspend_child_slot_in_place(child_slot) };
        // SAFETY: the child slot holds a valid `I::Suspended`, so by invariant 1 the
        // allocation is a valid suspended filter, which `Box::from_raw` takes over.
        unsafe {
            Box::from_raw(raw.cast::<RawElementFilter<'index, Suspended, I::Suspended, V, TC>>())
        }
    }
}

impl<'query, S, V, TC> RQESuspendedIterator<'query>
    for RawElementFilter<'query, Suspended, S, V, TC>
where
    S: RQESuspendedIterator<'query>,
    V: ElementValues + 'query,
    TC: TimeoutContext + 'query,
{
    type Resumed<'a>
        = ElementFilter<'a, S::Resumed<'a>, V, TC>
    where
        'query: 'a;

    fn resume<'a>(
        self: Box<Self>,
        guard: &IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        // As in `RawNot::resume`: only a virtual result, which carries no index pointers,
        // may be re-typed as active below.
        if self.result.kind() != RSResultKind::Virtual {
            return Ok(ResumeOutcome::Aborted);
        }

        let raw = Box::into_raw(self);
        // SAFETY: `raw` came from `Box::into_raw`, so it is valid and exclusively owned.
        // `&raw mut` forms the field pointer without creating a reference.
        let child_slot = unsafe { &raw mut (*raw).child };
        // SAFETY: a field pointer derived from the non-null `raw` is non-null.
        let child_slot = unsafe { NonNull::new_unchecked(child_slot) };
        // SAFETY: `child_slot` points at the valid, exclusively owned suspended child.
        let outcome = unsafe { resume_child_slot_in_place(child_slot, guard) };

        if !matches!(
            outcome,
            Ok(ResumeSlotOutcome::Unchanged | ResumeSlotOutcome::Moved)
        ) {
            // The child is gone, and `finish_resume_in_place` frees the allocation without
            // dropping what is left in it.
            // SAFETY: `raw` is still valid, and `&raw mut` forms the field pointer without
            // creating a reference.
            let condition = unsafe { &raw mut (*raw).condition };
            // SAFETY: `condition` is still a valid, exclusively owned value that nothing
            // reads after this.
            unsafe { std::ptr::drop_in_place(condition) };
            // SAFETY: `raw` is still valid; only its `child` and `condition` are gone, and
            // `&raw mut` reads neither.
            let result = unsafe { &raw mut (*raw).result };
            // SAFETY: `result` is the still-suspended result, valid and exclusively owned,
            // that nothing reads after this.
            unsafe { std::ptr::drop_in_place(result) };
            // SAFETY: `raw` is still valid, and `&raw mut` reads none of the dropped fields.
            let timeout_ctx = unsafe { &raw mut (*raw).timeout_ctx };
            // SAFETY: `timeout_ctx` is the last field that owns anything; it is valid,
            // exclusively owned, and nothing reads it after this.
            unsafe { std::ptr::drop_in_place(timeout_ctx) };
        }
        // SAFETY: `raw` is as `Box::into_raw` returned it and `outcome` is the result for
        // its child slot. On `Aborted`/`Err` the other fields were dropped above. With the
        // child resumed and `result` virtual (checked above), invariant 1 makes the
        // allocation a valid `ElementFilter<'a, S::Resumed<'a>, V, TC>`.
        let resumed = unsafe {
            finish_resume_in_place::<_, ElementFilter<'a, S::Resumed<'a>, V, TC>>(raw, outcome)
        }?;

        match resumed {
            // As in `revalidate`.
            ResumeOutcome::Moved(mut active) => {
                active.yield_current_or_next_match()?;
                Ok(ResumeOutcome::Moved(active))
            }
            unmoved => Ok(unmoved),
        }
    }

    fn last_doc_id(&self) -> DocId {
        self.result.doc_id
    }

    fn num_estimated(&self) -> usize {
        S::num_estimated(&self.child)
    }
}

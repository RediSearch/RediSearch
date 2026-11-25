/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use ffi::QueryIterator;
use rqe_core::DocId;
use rqe_iterators::{
    NewWildcardIterator, RQEIterator, RQEIteratorBoxed, RQESuspendedIterator, ResumeOutcome,
    boxed::{finish_resume_in_place, resume_child_slot_in_place, suspend_child_slot_in_place},
    c2rust::CRQEIterator,
    interop::RQEIteratorWrapper,
    not::Not,
    not_optimized::NotOptimized,
    not_reducer::{NewNotIterator, TIMEOUT_CHECK_GRANULARITY, new_not_iterator},
    utils::AnyTimeoutContext,
};

/// The [`Not`] iterator over a C child built by [`NewNotIterator()`].
pub type NotFfi<'index> = Not<'index, CRQEIterator, AnyTimeoutContext>;
/// The [`NotOptimized`] iterator over a C child built by [`NewNotIterator()`].
pub type NotOptimizedFfi<'index> =
    NotOptimized<'index, NewWildcardIterator<'index>, CRQEIterator, AnyTimeoutContext>;

/// Enum holding both NOT iterator variants with concrete [`CRQEIterator`] child.
///
/// The `NotOptimized` variant is intentionally large because it inlines a
/// `WildcardIterator` to avoid heap allocation. Both variants are
/// long-lived (query lifetime), and the size difference is acceptable.
///
/// `#[repr(C, u8)]`: see [`NotIteratorEnumSuspended`].
#[repr(C, u8)]
#[expect(
    clippy::large_enum_variant,
    reason = "both variants are query-lifetime; boxing would add a needless allocation"
)]
pub enum NotIteratorEnum<'index> {
    /// Scans every id up to the maximum, skipping the child's.
    Not(NotFfi<'index>),
    /// Scans the existing documents, skipping the child's.
    NotOptimized(NotOptimizedFfi<'index>),
}

impl rqe_iterators::profile_print::ProfilePrint for NotIteratorEnum<'_> {
    fn print_profile(
        &self,
        map: &mut redis_reply::MapBuilder<'_>,
        ctx: &mut rqe_iterators::profile_print::ProfilePrintCtx<'_>,
    ) {
        match self {
            Self::Not(it) => it.print_profile(map, ctx),
            Self::NotOptimized(it) => it.print_profile(map, ctx),
        }
    }
}

// Delegate `RQEIterator` to the inner variant.
impl<'index> RQEIterator<'index> for NotIteratorEnum<'index> {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut index_result::RSIndexResult<'index>> {
        match self {
            Self::Not(it) => it.current(),
            Self::NotOptimized(it) => it.current(),
        }
    }

    #[inline(always)]
    fn read(
        &mut self,
    ) -> Result<Option<&mut index_result::RSIndexResult<'index>>, rqe_iterators::RQEIteratorError>
    {
        match self {
            Self::Not(it) => it.read(),
            Self::NotOptimized(it) => it.read(),
        }
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<rqe_iterators::SkipToOutcome<'_, 'index>>, rqe_iterators::RQEIteratorError>
    {
        match self {
            Self::Not(it) => it.skip_to(doc_id),
            Self::NotOptimized(it) => it.skip_to(doc_id),
        }
    }

    #[inline(always)]
    fn revalidate(
        &mut self,
        spec: &index_spec::IndexSpecReadGuard,
    ) -> Result<rqe_iterators::RQEValidateStatus<'_, 'index>, rqe_iterators::RQEIteratorError> {
        match self {
            Self::Not(it) => it.revalidate(spec),
            Self::NotOptimized(it) => it.revalidate(spec),
        }
    }

    #[inline(always)]
    fn rewind(&mut self) {
        match self {
            Self::Not(it) => it.rewind(),
            Self::NotOptimized(it) => it.rewind(),
        }
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        match self {
            Self::Not(it) => it.num_estimated(),
            Self::NotOptimized(it) => it.num_estimated(),
        }
    }

    #[inline(always)]
    fn last_doc_id(&self) -> DocId {
        match self {
            Self::Not(it) => it.last_doc_id(),
            Self::NotOptimized(it) => it.last_doc_id(),
        }
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        match self {
            Self::Not(it) => it.at_eof(),
            Self::NotOptimized(it) => it.at_eof(),
        }
    }

    #[inline(always)]
    fn type_(&self) -> rqe_iterators::IteratorType {
        match self {
            Self::Not(it) => it.type_(),
            Self::NotOptimized(it) => it.type_(),
        }
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}

impl<'index> rqe_iterators::interop::ProfileChildren<'index> for NotIteratorEnum<'index> {
    fn profile_children(self) -> Self {
        match self {
            Self::Not(it) => Self::Not(it.profile_children()),
            Self::NotOptimized(it) => Self::NotOptimized(it.profile_children()),
        }
    }
}

/// Build the [`AnyTimeoutContext`] the iterator should use.
///
/// # Safety
///
/// Caller must uphold preconditions 3 and 4 of [`NewNotIterator()`] for the
/// lifetime of the returned context and every iterator built from it.
unsafe fn build_timeout_context(q: NonNull<ffi::QueryEvalCtx>) -> AnyTimeoutContext {
    // SAFETY: caller guarantees q is valid (3).
    let q_ref = unsafe { q.as_ref() };
    let sctx = NonNull::new(q_ref.sctx).expect("q.sctx must be non-null (precondition 4)");
    // SAFETY: precondition 4 keeps `q.sctx` and its request timeout valid and excludes source
    // changes and deadline writes during probes.
    unsafe { AnyTimeoutContext::from_sctx(sctx, TIMEOUT_CHECK_GRANULARITY) }
}

/// Suspended counterpart of [`NotIteratorEnum`].
///
/// Both are `#[repr(C, u8)]` so suspend and resume relabel the box in place,
/// keeping the address of the result a parent's aggregate result points into.
#[repr(C, u8)]
#[expect(
    clippy::large_enum_variant,
    reason = "matches the layout of the active variant; boxing would needlessly allocate"
)]
pub enum NotIteratorEnumSuspended<'query> {
    /// Suspended form of [`NotIteratorEnum::Not`].
    Not(<NotFfi<'query> as RQEIteratorBoxed<'query>>::Suspended),
    /// Suspended form of [`NotIteratorEnum::NotOptimized`].
    NotOptimized(<NotOptimizedFfi<'query> as RQEIteratorBoxed<'query>>::Suspended),
}

// The relabels below need each arm to resume to its own payload type, and the
// two enums to share a layout.
const _: () = {
    use rqe_iterators::boxed::{assert_layout_compatible, assert_resumes_to_itself};
    assert_resumes_to_itself::<NotFfi<'static>>();
    assert_resumes_to_itself::<NotOptimizedFfi<'static>>();
    assert_layout_compatible::<NotIteratorEnum<'static>, NotIteratorEnumSuspended<'static>>();
};

impl<'index> RQEIteratorBoxed<'index> for NotIteratorEnum<'index> {
    type Suspended = NotIteratorEnumSuspended<'index>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let mut raw = NonNull::new(Box::into_raw(self)).expect("Box pointers are non-null");
        // SAFETY: `raw` came from `Box::into_raw`; the borrow ends with the match.
        match unsafe { raw.as_mut() } {
            Self::Not(it) => {
                // SAFETY: `it` is the exclusively-owned payload, relabelled below.
                unsafe { suspend_child_slot_in_place(NonNull::from(it)) }
            }
            Self::NotOptimized(it) => {
                // SAFETY: as above.
                unsafe { suspend_child_slot_in_place(NonNull::from(it)) }
            }
        }
        // SAFETY: with the payload suspended and the tag unchanged, the
        // allocation is a valid `NotIteratorEnumSuspended` (proof above).
        unsafe { Box::from_raw(raw.cast::<NotIteratorEnumSuspended<'index>>().as_ptr()) }
    }
}

impl<'query> RQESuspendedIterator<'query> for NotIteratorEnumSuspended<'query> {
    type Resumed<'a>
        = NotIteratorEnum<'a>
    where
        'query: 'a;

    fn resume<'a>(
        self: Box<Self>,
        guard: &index_spec::IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, rqe_iterators::RQEIteratorError>
    where
        'query: 'a,
    {
        let mut raw = NonNull::new(Box::into_raw(self)).expect("Box pointers are non-null");
        // SAFETY: `raw` came from `Box::into_raw`; the borrow ends with the match.
        let outcome = match unsafe { raw.as_mut() } {
            Self::Not(s) => {
                // SAFETY: `s` is the valid, exclusively-owned payload. On a
                // recoverable outcome the slot holds its resumed form; otherwise it
                // is left uninitialised and `finish_resume_in_place` frees the box
                // without dropping it.
                unsafe { resume_child_slot_in_place(NonNull::from(s), guard) }
            }
            Self::NotOptimized(s) => {
                // SAFETY: as above.
                unsafe { resume_child_slot_in_place(NonNull::from(s), guard) }
            }
        };

        // SAFETY: 1. `raw` came from `Box::into_raw`; the borrow has ended.
        // 2. `outcome` is the payload's; the tag needs no drop.
        // 3. Each arm resumes to its own payload type (proof above).
        unsafe { finish_resume_in_place::<_, NotIteratorEnum<'a>>(raw.as_ptr(), outcome) }
    }

    fn last_doc_id(&self) -> DocId {
        match self {
            NotIteratorEnumSuspended::Not(s) => s.last_doc_id(),
            NotIteratorEnumSuspended::NotOptimized(s) => s.last_doc_id(),
        }
    }

    fn num_estimated(&self) -> usize {
        match self {
            NotIteratorEnumSuspended::Not(s) => s.num_estimated(),
            NotIteratorEnumSuspended::NotOptimized(s) => s.num_estimated(),
        }
    }
}

/// Creates a NOT iterator, choosing between non-optimized and optimized based
/// on the query evaluation context.
///
/// If the child is trivially reducible (empty or wildcard), a simplified
/// iterator is returned directly.
///
/// The request timeout reached through `q.sctx.limits` selects no timeout,
/// the Blocked Client Timeout, or the Clock Based Timeout. Clock deadlines are
/// read back on every probe so a re-armed deadline is honoured.
///
/// # Safety
///
/// 1. `child` must be null or a valid pointer to a [`QueryIterator`].
///    A null `child` is treated as empty.
/// 2. When non-null, `child` must not be aliased.
/// 3. `q` must be a valid non-null pointer to a [`QueryEvalCtx`](ffi::QueryEvalCtx).
/// 4. `q.sctx` must be a non-null pointer to a valid
///    [`RedisSearchCtx`](ffi::RedisSearchCtx), which must stay valid and at a stable
///    address for the lifetime of the returned iterator. Its request timeout must also remain
///    valid at a stable address for that lifetime. Timeout source changes and deadline writes
///    may happen only between probes;
///    only the blocked-client marker and the memory stop flag may change concurrently.
/// 5. `q.sctx.spec` must be a non-null pointer to a valid
///    [`IndexSpec`](ffi::IndexSpec).
/// 6. `q.sctx.spec.rule`, when non-null, must point to a valid
///    [`SchemaRule`](ffi::SchemaRule).
/// 7. When the optimized path is taken, the preconditions of
///    [`rqe_iterators::wildcard::new_wildcard_iterator`] must hold.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewNotIterator(
    child: *mut QueryIterator,
    max_doc_id: DocId,
    weight: f64,
    q: *mut ffi::QueryEvalCtx,
) -> *mut QueryIterator {
    let query = NonNull::new(q).expect("q must be non-null");

    // SAFETY: caller upholds preconditions (3, 4).
    let timeout_ctx = unsafe { build_timeout_context(query) };

    // Handle null child: reduce with Empty directly (always becomes wildcard).
    let Some(child_ptr) = NonNull::new(child) else {
        let empty = rqe_iterators::Empty;
        // SAFETY: caller guarantees preconditions (3–7).
        let result = unsafe { new_not_iterator(empty, max_doc_id, weight, timeout_ctx, query) };
        return match result {
            NewNotIterator::ReducedWildcard(wc) => RQEIteratorWrapper::boxed_new(wc).as_ptr(),
            NewNotIterator::ReducedEmpty(empty) => RQEIteratorWrapper::boxed_new(empty).as_ptr(),
            // Empty child always reduces; these arms are unreachable.
            NewNotIterator::Not(_) | NewNotIterator::NotOptimized(_) => {
                panic!("Empty not child always reduces")
            }
        };
    };

    // SAFETY: thanks to 1 + 2
    let child = unsafe { CRQEIterator::new(child_ptr) };

    // SAFETY: caller guarantees preconditions (3–7).
    let result = unsafe { new_not_iterator(child, max_doc_id, weight, timeout_ctx, query) };

    match result {
        NewNotIterator::ReducedWildcard(wc) => RQEIteratorWrapper::boxed_new(wc).as_ptr(),
        NewNotIterator::ReducedEmpty(empty) => RQEIteratorWrapper::boxed_new(empty).as_ptr(),
        NewNotIterator::Not(iter) => {
            RQEIteratorWrapper::boxed_new_compound(NotIteratorEnum::Not(iter)).as_ptr()
        }
        NewNotIterator::NotOptimized(iter) => {
            RQEIteratorWrapper::boxed_new_compound(NotIteratorEnum::NotOptimized(iter)).as_ptr()
        }
    }
}

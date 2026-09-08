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
    NewWildcardIterator, RQEIterator,
    c2rust::CRQEIterator,
    interop::RQEIteratorWrapper,
    not::Not,
    not_optimized::NotOptimized,
    not_reducer::{NewNotIterator, TIMEOUT_CHECK_GRANULARITY, new_not_iterator},
    utils::AnyTimeoutContext,
};

type NotFfi<'index> = Not<'index, CRQEIterator, AnyTimeoutContext>;
type NotOptimizedFfi<'index> =
    NotOptimized<'index, NewWildcardIterator<'index>, CRQEIterator, AnyTimeoutContext>;

/// Enum holding both NOT iterator variants with concrete [`CRQEIterator`] child.
///
/// The `NotOptimized` variant is intentionally large because it inlines a
/// `WildcardIterator` to avoid heap allocation. Both variants are
/// long-lived (query lifetime), and the size difference is acceptable.
#[expect(
    clippy::large_enum_variant,
    reason = "both variants are query-lifetime; boxing would add a needless allocation"
)]
enum NotIteratorEnum<'index> {
    Not(NotFfi<'index>),
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
    // SAFETY: caller guarantees `q.sctx` and its borrowed request timeout remain valid (4).
    unsafe { AnyTimeoutContext::from_sctx(sctx, TIMEOUT_CHECK_GRANULARITY) }
}
/// Creates a NOT iterator, choosing between non-optimized and optimized based
/// on the query evaluation context.
///
/// If the child is trivially reducible (empty or wildcard), a simplified
/// iterator is returned directly.
///
/// The request timeout reached through `q.sctx.timeout` selects no timeout,
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
///    address for the lifetime of the returned iterator: on the Clock Based Timeout path
///    the iterator reads the request-owned deadline back on every probe. No write to that
///    deadline may overlap a probe.
/// 5. `q.sctx.spec` must be a non-null pointer to a valid
///    [`IndexSpec`](ffi::IndexSpec).
/// 6. `q.sctx.spec.rule`, when non-null, must point to a valid
///    [`SchemaRule`](ffi::SchemaRule).
/// 7. When the optimized path is taken, the preconditions of
///    [`crate::wildcard::NewWildcardIterator`] must hold.
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
            NewNotIterator::ReducedWildcard(wc) => RQEIteratorWrapper::boxed_new(wc),
            NewNotIterator::ReducedEmpty(empty) => RQEIteratorWrapper::boxed_new(empty),
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
        NewNotIterator::ReducedWildcard(wc) => RQEIteratorWrapper::boxed_new(wc),
        NewNotIterator::ReducedEmpty(empty) => RQEIteratorWrapper::boxed_new(empty),
        NewNotIterator::Not(iter) => {
            RQEIteratorWrapper::boxed_new_compound(NotIteratorEnum::Not(iter))
        }
        NewNotIterator::NotOptimized(iter) => {
            RQEIteratorWrapper::boxed_new_compound(NotIteratorEnum::NotOptimized(iter))
        }
    }
}

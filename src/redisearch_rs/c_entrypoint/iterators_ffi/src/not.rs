/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use ffi::{QueryIterator, t_docId, timespec};
use rqe_iterator_type::IteratorType;
use rqe_iterators::{
    NewWildcardIterator, RQEIterator,
    c2rust::CRQEIterator,
    interop::RQEIteratorWrapper,
    not::Not,
    not_optimized::NotOptimized,
    not_reducer::{NewNotIterator, new_not_iterator},
};

type NotFfi<'index> = Not<'index, CRQEIterator>;
type NotOptimizedFfi<'index> = NotOptimized<'index, NewWildcardIterator<'index>, CRQEIterator>;

/// Enum holding both NOT iterator variants with concrete [`CRQEIterator`] child.
///
/// The `NotOptimized` variant is intentionally large because it inlines a
/// [`WildcardIterator`] to avoid heap allocation. Both variants are
/// long-lived (query lifetime), and the size difference is acceptable.
#[expect(
    clippy::large_enum_variant,
    reason = "both variants are query-lifetime; boxing would add a needless allocation"
)]
enum NotIteratorEnum<'index> {
    Not(NotFfi<'index>),
    NotOptimized(NotOptimizedFfi<'index>),
}

impl<'index> NotIteratorEnum<'index> {
    const fn child(&self) -> Option<&CRQEIterator> {
        match self {
            Self::Not(it) => it.child(),
            Self::NotOptimized(it) => it.child(),
        }
    }
}

// Delegate `RQEIterator` to the inner variant.
impl<'index> RQEIterator<'index> for NotIteratorEnum<'index> {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut inverted_index::RSIndexResult<'index>> {
        match self {
            Self::Not(it) => it.current(),
            Self::NotOptimized(it) => it.current(),
        }
    }

    #[inline(always)]
    fn read(
        &mut self,
    ) -> Result<Option<&mut inverted_index::RSIndexResult<'index>>, rqe_iterators::RQEIteratorError>
    {
        match self {
            Self::Not(it) => it.read(),
            Self::NotOptimized(it) => it.read(),
        }
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<rqe_iterators::SkipToOutcome<'_, 'index>>, rqe_iterators::RQEIteratorError>
    {
        match self {
            Self::Not(it) => it.skip_to(doc_id),
            Self::NotOptimized(it) => it.skip_to(doc_id),
        }
    }

    #[inline(always)]
    unsafe fn revalidate(
        &mut self,
        spec: std::ptr::NonNull<ffi::IndexSpec>,
    ) -> Result<rqe_iterators::RQEValidateStatus<'_, 'index>, rqe_iterators::RQEIteratorError> {
        match self {
            // SAFETY: Delegating to variant with the same `spec` passed by our caller.
            Self::Not(it) => unsafe { it.revalidate(spec) },
            // SAFETY: Delegating to variant with the same `spec` passed by our caller.
            Self::NotOptimized(it) => unsafe { it.revalidate(spec) },
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
    fn last_doc_id(&self) -> t_docId {
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

/// FFI wrapper for non-reduced NOT iterators ([`NotIteratorEnum`]).
///
/// Used by [`GetNotIteratorChild`] to recover the Rust iterator from a raw
/// [`QueryIterator`] pointer.
type NotIteratorWrapper<'index> = RQEIteratorWrapper<NotIteratorEnum<'index>>;

/// Creates a NOT iterator, choosing between non-optimized and optimized based
/// on the query evaluation context.
///
/// If the child is trivially reducible (empty or wildcard), a simplified
/// iterator is returned directly.
///
/// # Safety
///
/// 1. `child` must be null or a valid pointer to a [`QueryIterator`].
///    A null `child` is treated as empty.
/// 2. When non-null, `child` must not be aliased.
/// 3. `q` must be a valid non-null pointer to a [`QueryEvalCtx`](ffi::QueryEvalCtx).
/// 4. `q.sctx` must be a non-null pointer to a valid
///    [`RedisSearchCtx`](ffi::RedisSearchCtx).
/// 5. `q.sctx.spec` must be a non-null pointer to a valid
///    [`IndexSpec`](ffi::IndexSpec).
/// 6. `q.sctx.spec.rule`, when non-null, must point to a valid
///    [`SchemaRule`](ffi::SchemaRule).
/// 7. When the optimized path is taken, the preconditions of
///    [`crate::wildcard::NewWildcardIterator_Optimized`] must hold.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewNotIterator(
    child: *mut QueryIterator,
    max_doc_id: t_docId,
    weight: f64,
    timeout: timespec,
    q: *mut ffi::QueryEvalCtx,
) -> *mut QueryIterator {
    let query = NonNull::new(q).expect("q must be non-null");

    let (rust_timeout, skip_timeout_checks) = {
        // SAFETY: caller guarantees q is valid (3).
        let q_ref = unsafe { query.as_ref() };
        // SAFETY: caller guarantees q.sctx is valid (4).
        let sctx = unsafe { &*q_ref.sctx };
        if sctx.time.skipTimeoutChecks {
            (std::time::Duration::ZERO, true)
        } else {
            match crate::timespec::duration_from_redis_timespec(timeout) {
                Some(d) => (d, false),
                // Redis sentinel (no timeout) => skip timeout checks
                None => (std::time::Duration::ZERO, true),
            }
        }
    };

    // Handle null child: reduce with Empty directly (always becomes wildcard).
    let Some(child_ptr) = NonNull::new(child) else {
        let empty = rqe_iterators::Empty;
        // SAFETY: caller guarantees preconditions (3–7).
        let result = unsafe {
            new_not_iterator(
                empty,
                max_doc_id,
                weight,
                rust_timeout,
                skip_timeout_checks,
                query,
            )
        };
        return match result {
            NewNotIterator::ReducedWildcard(wc) => RQEIteratorWrapper::boxed_new(wc),
            NewNotIterator::ReducedEmpty(empty) => {
                RQEIteratorWrapper::boxed_new(Box::new(empty) as Box<dyn RQEIterator>)
            }
            // Empty child always reduces; these arms are unreachable.
            NewNotIterator::Not(_) | NewNotIterator::NotOptimized(_) => {
                panic!("Empty not child always reduces")
            }
        };
    };

    // SAFETY: thanks to 1 + 2
    let child = unsafe { CRQEIterator::new(child_ptr) };

    // SAFETY: caller guarantees preconditions (3–7).
    let result = unsafe {
        new_not_iterator(
            child,
            max_doc_id,
            weight,
            rust_timeout,
            skip_timeout_checks,
            query,
        )
    };

    match result {
        NewNotIterator::ReducedWildcard(wc) => RQEIteratorWrapper::boxed_new(wc),
        NewNotIterator::ReducedEmpty(empty) => {
            RQEIteratorWrapper::boxed_new(Box::new(empty) as Box<dyn RQEIterator>)
        }
        NewNotIterator::Not(iter) => {
            RQEIteratorWrapper::boxed_new_compound(NotIteratorEnum::Not(iter))
        }
        NewNotIterator::NotOptimized(iter) => {
            RQEIteratorWrapper::boxed_new_compound(NotIteratorEnum::NotOptimized(iter))
        }
    }
}

/// Get the child pointer of a NOT iterator, or NULL if there is no child.
///
/// # Safety
///
/// 1. `it` must be a valid non-null pointer to a non-reduced NOT iterator
///    created via [`NewNotIterator`]. Must not be called on a reduced
///    (wildcard/empty) iterator returned by [`NewNotIterator`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GetNotIteratorChild(it: *const QueryIterator) -> *const QueryIterator {
    debug_assert!(!it.is_null());
    debug_assert!(
        matches!(
            // SAFETY: Safe thanks to 1
            unsafe { (*it).type_ },
            IteratorType::Not | IteratorType::NotOptimized
        ),
        "Expected a NOT or NOT_OPTIMIZED iterator"
    );
    // SAFETY: Safe thanks to 1
    let wrapper = unsafe { NotIteratorWrapper::ref_from_header_ptr(it) };
    wrapper
        .inner
        .child()
        .map(|c| c.as_ref() as *const _)
        .unwrap_or(std::ptr::null())
}

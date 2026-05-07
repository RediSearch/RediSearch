/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use ffi::{QueryEvalCtx, QueryIterator, t_docId};
use rqe_iterator_type::IteratorType;
use rqe_iterators::c2rust::CRQEIterator;
use rqe_iterators::interop::RQEIteratorWrapper;
use rqe_iterators::optional::Optional;
use rqe_iterators::optional_reducer::{
    NewOptionalIterator as OptionalIteratorOutcome, new_optional_iterator,
};

#[unsafe(no_mangle)]
/// Create an optional iterator over `child`, applying shortcircuit reductions where possible.
///
/// - If `child` is null or an empty iterator, a wildcard iterator is returned instead (all results will be virtual hits).
/// - If `child` is a wildcard iterator, it is returned as-is with `weight` applied.
/// - Otherwise, an [`Optional`] or [`OptionalOptimized`](rqe_iterators::optional_optimized::OptionalOptimized)
///   iterator is constructed based on whether `q.sctx.spec.rule.index_all` is set.
///
/// # Safety
///
/// 1. `child`, when non-null, must be a valid owning pointer to a C query iterator that is not aliased.
/// 2. `q` must be a valid non-null pointer to a [`QueryEvalCtx`] satisfying all preconditions of
///    [`new_optional_iterator`](rqe_iterators::optional_reducer::new_optional_iterator).
pub unsafe extern "C" fn NewOptionalIterator(
    child: *mut QueryIterator,
    q: *mut QueryEvalCtx,
    max_doc_id: t_docId,
    weight: f64,
) -> *mut QueryIterator {
    let query = NonNull::new(q).expect("q is null");

    // Handle NULL child: equivalent to an empty iterator — return a wildcard fallback.
    let Some(child_nn) = NonNull::new(child) else {
        // SAFETY: thanks to 2.
        let wc = unsafe { rqe_iterators::wildcard::new_wildcard_iterator(query, 0.0) };
        return RQEIteratorWrapper::boxed_new(wc);
    };

    // SAFETY: thanks to 1.
    let child = unsafe { CRQEIterator::new(child_nn) };
    // SAFETY: thanks to 2.
    let result = unsafe { new_optional_iterator(child, weight, query, max_doc_id) };

    match result {
        OptionalIteratorOutcome::WildcardFallback(wc) => RQEIteratorWrapper::boxed_new(wc),
        OptionalIteratorOutcome::WildcardPassthrough(child) => child.into_raw().as_ptr(),
        OptionalIteratorOutcome::Optional(opt) => RQEIteratorWrapper::boxed_new_compound(opt),
        OptionalIteratorOutcome::OptionalOptimized(opt_opt) => {
            RQEIteratorWrapper::boxed_new_compound(opt_opt)
        }
    }
}

#[unsafe(no_mangle)]
/// Return the child pointer of an optional iterator (optimized or non-optimized), or NULL if there is no child.
///
/// # Safety
///
/// 1. `base` must be a valid non-null pointer to an optional iterator created via [`NewOptionalIterator`].
pub unsafe extern "C" fn GetOptionalIteratorChild(
    base: *const QueryIterator,
) -> *const QueryIterator {
    debug_assert!(!base.is_null());
    // SAFETY: thanks to 1
    if unsafe { (*base).type_ } == IteratorType::OptionalOptimized {
        // SAFETY: thanks to 1
        unsafe { get_optional_optimized_iterator_child(base) }
    } else {
        // SAFETY: thanks to 1
        unsafe { get_optional_non_optimized_iterator_child(base) }
    }
}

/// Get the child pointer of the optional (non-optimized) iterator or NULL
/// in case there is no child.
///
/// # Safety
///
/// 1. `header` must be a valid non-null pointer to an iterator with `type_` equal to
///    [`rqe_iterator_type::IteratorType::Optional`], as returned by [`NewOptionalIterator`].
unsafe fn get_optional_non_optimized_iterator_child(
    header: *const QueryIterator,
) -> *const QueryIterator {
    debug_assert!(!header.is_null());
    debug_assert_eq!(
        // SAFETY: Safe thanks to 1
        unsafe { (*header).type_ },
        IteratorType::Optional,
        "Expected an optional (Non-Optimized) iterator"
    );
    // SAFETY: Safe thanks to 1
    let wrapper =
        unsafe { RQEIteratorWrapper::<Optional<CRQEIterator>>::ref_from_header_ptr(header) };
    wrapper
        .inner
        .child()
        .map(|p| p.as_ref() as *const _)
        .unwrap_or(std::ptr::null())
}

/// Get the child pointer of the optimized optional iterator, or NULL if there is no child.
///
/// # Safety
///
/// 1. `header` must be a valid non-null pointer to an iterator with `type_` equal to
///    [`ffi::IteratorType::OptionalOptimized`], as returned by [`crate::optional::NewOptionalIterator`].
unsafe fn get_optional_optimized_iterator_child(
    header: *const QueryIterator,
) -> *const QueryIterator {
    use rqe_iterators::NewWildcardIterator;
    use rqe_iterators::optional_optimized::OptionalOptimized;

    type OptionalOptimizedFfi<'a> = OptionalOptimized<'a, NewWildcardIterator<'a>, CRQEIterator>;
    debug_assert!(!header.is_null());
    debug_assert_eq!(
        // SAFETY: Safe thanks to 1
        unsafe { &*header }.type_,
        IteratorType::OptionalOptimized,
        "Expected an optimized optional iterator"
    );
    // SAFETY: Safe thanks to 1
    let wrapper =
        unsafe { RQEIteratorWrapper::<OptionalOptimizedFfi>::ref_from_header_ptr(header) };
    wrapper
        .inner
        .child()
        .map(|p| p.as_ref() as *const _)
        .unwrap_or(std::ptr::null())
}

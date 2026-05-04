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
use rqe_iterators::c2rust::CRQEIterator;
use rqe_iterators::interop::RQEIteratorWrapper;
use rqe_iterators::optional_reducer::{
    NewOptionalIterator as OptionalIteratorOutcome, new_optional_iterator,
};

#[unsafe(no_mangle)]
/// Create an optional iterator over `child`, applying shortcircuit reductions where possible.
///
/// - If `child` is null or an empty iterator, a wildcard iterator is returned instead (all results will be virtual hits).
/// - If `child` is a wildcard iterator, it is returned as-is with `weight` applied.
/// - Otherwise, an [`Optional`](rqe_iterators::optional::Optional) or [`OptionalOptimized`](rqe_iterators::optional_optimized::OptionalOptimized)
///   iterator is constructed based on whether `q.sctx.spec.rule.index_all` is set.
///
/// # Safety
///
/// 1. `child`, when non-null, must be a valid owning pointer to a C query iterator that is not aliased.
/// 2. `q` must be a valid non-null pointer to a [`QueryEvalCtx`] satisfying all preconditions of
///    [`new_optional_iterator`].
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

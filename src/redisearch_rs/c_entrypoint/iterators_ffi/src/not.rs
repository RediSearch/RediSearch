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
use rqe_iterators::RQEIterator;
use rqe_iterators::c2rust::CRQEIterator;
use rqe_iterators::not::{NewNotIterator, NotIterator, new_not_iterator};
use rqe_iterators_interop::RQEIteratorWrapper;

type NotIteratorWrapper<'index> = RQEIteratorWrapper<Box<dyn NotIterator<'index> + 'index>>;

/// Creates a NOT iterator, choosing between non-optimized and optimized based
/// on the query evaluation context.
///
/// If the child is trivially reducible (empty or wildcard), a simplified
/// iterator is returned directly.
///
/// # Safety
///
/// 1. `child` must be a valid non-null pointer to a [`QueryIterator`].
/// 2. `child` must not be aliased.
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
    let child: Box<dyn RQEIterator + '_> = match NonNull::new(child) {
        Some(child) => {
            // SAFETY: thanks to 1 + 2
            Box::new(unsafe { CRQEIterator::new(child) })
        }
        None => {
            // NULL child is treated as empty by `new_not_iterator` (reduction
            // rule 1), so we use an explicit `Empty` iterator.
            Box::new(rqe_iterators::Empty)
        }
    };

    let (rust_timeout, skip_timeout_checks) = {
        // SAFETY: caller guarantees q is valid (3).
        let q_ref = unsafe { &*q };
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

    let query = NonNull::new(q).expect("q must be non-null");
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
        NewNotIterator::Reduced(iter, iter_type) => RQEIteratorWrapper::boxed_new(iter_type, iter),
        NewNotIterator::NotReduced(iter, iter_type) => {
            RQEIteratorWrapper::boxed_new(iter_type, iter)
        }
    }
}

/// Get the child pointer of a NOT iterator, or NULL if there is no child.
///
/// # Safety
///
/// 1. `it` must be a valid non-null pointer to a NOT iterator created via
///    [`NewNotIterator`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GetNotIteratorChild(it: *const QueryIterator) -> *const QueryIterator {
    debug_assert!(!it.is_null());
    // SAFETY: Safe thanks to 1
    let wrapper = unsafe { NotIteratorWrapper::ref_from_header_ptr(it) };
    wrapper
        .inner
        .child()
        .and_then(|c| c.as_c_header_ptr())
        .map(|p| p.as_ptr() as *const _)
        .unwrap_or(std::ptr::null())
}

/// Set (or overwrite) the child iterator of a NOT iterator.
///
/// # Safety
///
/// 1. `it` must be a valid non-null pointer to a NOT iterator created via
///    [`NewNotIterator`].
/// 2. `child` must be null or a valid non-null non-aliased pointer for a
///    valid [`QueryIterator`] respecting the C API.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn SetNotIteratorChild(it: *mut QueryIterator, child: *mut QueryIterator) {
    debug_assert!(!it.is_null());
    // SAFETY: thanks to 1
    let wrapper = unsafe { NotIteratorWrapper::mut_ref_from_header_ptr(it) };

    match NonNull::new(child) {
        Some(child) => {
            // SAFETY: thanks to 2 + null check from this match statement
            let child = unsafe { CRQEIterator::new(child) };
            wrapper.inner.set_child(Box::new(child));
        }
        None => {
            // Unset the child by taking and dropping it.
            drop(wrapper.inner.take_child());
        }
    }
}

/// Take ownership over the child of a NOT iterator.
///
/// # Safety
///
/// 1. `it` must be a valid non-null pointer to a NOT iterator created via
///    [`NewNotIterator`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TakeNotIteratorChild(it: *mut QueryIterator) -> *mut QueryIterator {
    debug_assert!(!it.is_null());
    // SAFETY: thanks to 1
    let wrapper = unsafe { NotIteratorWrapper::mut_ref_from_header_ptr(it) };
    wrapper
        .inner
        .take_child()
        .and_then(|c| c.into_c_header_ptr())
        .map(|p| p.as_ptr())
        .unwrap_or(std::ptr::null_mut())
}

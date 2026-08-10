/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{ptr::NonNull, time::Duration};

use ffi::{IteratorType_NOT_ITERATOR, QueryIterator, t_docId, timespec};
use rqe_iterators::interop::RQEIteratorWrapper;
use rqe_iterators::not::Not;

use rqe_iterators::c2rust::CRQEIterator;

#[unsafe(no_mangle)]
/// Creates a new not iterator.
///
/// `timeout` is the deadline the iterator probes, read back through the pointer on every probe so
/// that a cursor read which re-arms it in place is measured against its own budget rather than an
/// earlier read's. Pass NULL when the query has no deadline; the iterator then skips timeout checks
/// entirely, which is also the only correct choice for a caller that cannot satisfy 3 and 4.
///
/// # Safety
///
/// 1. `child` must be a valid non-null pointer to an implementation of the C query iterator API.
/// 2. `child` must not be aliased.
/// 3. `timeout` must be NULL, or a valid pointer to a `timespec` that stays alive and at a stable
///    address for as long as the returned iterator is used - not merely for this call.
/// 4. No write to `*timeout` may overlap a read of the returned iterator.
pub unsafe extern "C" fn NewNotIteratorNonOptimized(
    child: *mut QueryIterator,
    max_doc_id: t_docId,
    weight: f64,
    timeout: *const timespec,
    skip_timeout_checks: bool,
) -> *mut QueryIterator {
    let child = NonNull::new(child)
        .expect("Trying to create a not iterator using a NULL child iterator pointer");
    // SAFETY: thanks to 1 + 2
    let child = unsafe { CRQEIterator::new(child) };

    let rust_iterator = match NonNull::new(timeout.cast_mut()) {
        // SAFETY: forwarded to this function's caller by clauses 3 and 4.
        Some(deadline) => unsafe {
            Not::new_with_deadline(child, max_doc_id, weight, deadline, skip_timeout_checks)
        },
        // No deadline to probe. The duration is unused because the checks are skipped outright,
        // which is what this entry point already did for a query whose deadline was the "no
        // timeout" sentinel.
        None => Not::new(child, max_doc_id, weight, Duration::ZERO, true),
    };

    RQEIteratorWrapper::boxed_new(IteratorType_NOT_ITERATOR, rust_iterator)
}

#[unsafe(no_mangle)]
/// Get the child pointer of the not (non-optimized) iterator or NULL
/// in case there is no child.
///
/// # Safety
///
/// 1. `header` must be a valid non-null pointer created via [`NewNotIteratorNonOptimized`].
pub unsafe extern "C" fn GetNotIteratorNonOptimizedChild(
    header: *const QueryIterator,
) -> *const QueryIterator {
    debug_assert!(!header.is_null());
    debug_assert_eq!(
        // SAFETY: Safe thanks to 1
        unsafe { *header }.type_,
        IteratorType_NOT_ITERATOR,
        "Expected an not (Non-Optimized) iterator"
    );
    // SAFETY: Safe thanks to 1
    let wrapper = unsafe { RQEIteratorWrapper::<Not<CRQEIterator>>::ref_from_header_ptr(header) };
    wrapper
        .inner
        .child()
        .map(|p| p.as_ref() as *const _)
        .unwrap_or(std::ptr::null())
}

#[unsafe(no_mangle)]
/// Take ownership over the child of the not (non-optimized) iterator.
///
/// # Safety
///
/// 1. `header` must be a valid non-null pointer created via [`NewNotIteratorNonOptimized`].
pub unsafe extern "C" fn TakeNotIteratorNonOptimizedChild(
    header: *mut QueryIterator,
) -> *mut QueryIterator {
    debug_assert!(!header.is_null());
    debug_assert_eq!(
        // SAFETY: Safe thanks to 1
        unsafe { *header }.type_,
        IteratorType_NOT_ITERATOR,
        "Expected an not (Non-Optimized) iterator"
    );
    // SAFETY: Safe thanks to 1
    let wrapper =
        unsafe { RQEIteratorWrapper::<Not<CRQEIterator>>::mut_ref_from_header_ptr(header) };
    wrapper
        .inner
        .take_child()
        .map(|p| p.into_raw().as_ptr())
        .unwrap_or(std::ptr::null_mut())
}

#[unsafe(no_mangle)]
/// Set (or overwrite) the child iterator of the not (non-optimized) iterator.
///
/// # Safety
///
/// 1. `header` must be a valid non-null pointer created via [`NewNotIteratorNonOptimized`].
/// 2. `child` must be null or a valid non-null non-aliased pointer for a valid [`QueryIterator`] respecting the C API.
pub unsafe extern "C" fn SetNotIteratorNonOptimizedChild(
    header: *mut QueryIterator,
    child: *mut QueryIterator,
) {
    debug_assert!(!header.is_null());
    debug_assert_eq!(
        // SAFETY: thanks to 1
        unsafe { *header }.type_,
        IteratorType_NOT_ITERATOR,
        "Expected an not (Non-Optimized) iterator"
    );
    // SAFETY: thanks to 1
    let wrapper =
        unsafe { RQEIteratorWrapper::<Not<CRQEIterator>>::mut_ref_from_header_ptr(header) };

    match NonNull::new(child) {
        Some(child) => {
            // SAFETY: thanks to 2 + null check from this match statement
            let child = unsafe { CRQEIterator::new(child) };
            wrapper.inner.set_child(child);
        }
        None => {
            wrapper.inner.unset_child();
        }
    }
}

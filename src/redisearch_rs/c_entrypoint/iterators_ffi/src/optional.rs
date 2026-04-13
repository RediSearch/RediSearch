/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;

use ffi::{QueryIterator, t_docId};
use rqe_iterator_type::IteratorType;
use rqe_iterators::interop::RQEIteratorWrapper;
use rqe_iterators::optional::Optional;

use rqe_iterators::c2rust::CRQEIterator;

#[unsafe(no_mangle)]
/// Create a new non-optimized optional iterator.
///
/// # Safety
///
/// 1. `child_it` must be a valid non-null pointer to an implementation of the C query iterator API.
/// 2. `child_it` must not be aliased.
pub unsafe extern "C" fn NewOptionalNonOptimizedIterator(
    child: *mut QueryIterator,
    max_id: t_docId,
    weight: f64,
) -> *mut QueryIterator {
    let child = NonNull::new(child).expect(
        "Trying to create a non-optimized optional iterator using a NULL child iterator pointer",
    );
    // SAFETY: thanks to 1 + 2
    let child = unsafe { CRQEIterator::new(child) };
    RQEIteratorWrapper::boxed_new_compound(Optional::new(max_id, weight, child))
}

#[unsafe(no_mangle)]
/// Get the child pointer of the optional (non-optimized) iterator or NULL
/// in case there is no child.
///
/// # Safety
///
/// 1. `header` must be a valid non-null pointer created via [`NewOptionalNonOptimizedIterator`].
pub unsafe extern "C" fn GetOptionalNonOptimizedIteratorChild(
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

/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Reach the vector-score [`RLookupKey`] slot through a type-erased iterator
//! header.
//!
//! A vector top-k iterator yields the distance under a lookup key that is only
//! resolved during pipeline construction, long after the iterator is built. The
//! query evaluator therefore hands the metric request a pointer to the
//! iterator's own slot, and hands the iterator back the handle it invalidates
//! when freed. Both sides hold nothing but the erased [`QueryIterator`] header
//! by then, hence these two accessors.

use std::ptr::NonNull;

use ffi::QueryIterator;
use rlookup::{RLookupKey, RLookupKeyHandle};
use rqe_iterators::{IteratorType, interop::RQEIteratorWrapper};

use crate::VectorTopKIterator;

/// Recover the wrapper around a vector top-k iterator from its header.
///
/// # Safety
///
/// Same requirements as [`own_key_ref`].
///
/// # Panics
///
/// Panics unless the header's type tag is [`IteratorType::Hybrid`].
unsafe fn wrapper_mut<'index>(
    header: NonNull<QueryIterator>,
) -> &'index mut RQEIteratorWrapper<VectorTopKIterator<'index>> {
    // SAFETY: guaranteed by 3.
    let iterator_type = unsafe { header.as_ref() }.type_;
    assert_eq!(
        iterator_type,
        IteratorType::Hybrid,
        "expected a vector top-k iterator: unexpected type: {iterator_type}"
    );
    // SAFETY: guaranteed by 1 + 2 + 3.
    unsafe { RQEIteratorWrapper::mut_ref_from_header_ptr(header.as_ptr()) }
}

/// Return a pointer to the slot holding this iterator's vector-score
/// [`RLookupKey`], initially null.
///
/// The pointer aliases a slot inside the iterator, so writes through it must
/// not be interleaved with use of the iterator. The slot is boxed, so the
/// pointer survives moves of the iterator itself (the `FT.PROFILE` rebox, for
/// one).
///
/// `'index` is unconstrained by the erased header, exactly as for
/// [`rqe_iterators::metric::own_key_ref`]: the caller picks it, and a caller
/// with no lifetime to offer should discard it rather than name `'static`.
///
/// # Safety
///
/// 1. `header` points to an iterator boxed from a [`VectorTopKIterator`]
///    carrying the default [`FieldExpirationChecker`]. Any other
///    [`ExpirationChecker`] gives the wrapper a different layout, and the type
///    tag does not tell the two apart — this one rests on the caller alone.
/// 2. That iterator did not reduce to `Empty`. [`new_vector_top_k`] answers
///    [`ReducedEmpty`] for an empty child or `k == 0`, and a handle boxed from
///    that holds no [`VectorScoreSource`] to reach into.
/// 3. The iterator is alive and held exclusively for the duration of the call,
///    as are the `index`, `query_vector` and `sctx` it was built from: the
///    wrapper names them at a lifetime the erased header cannot constrain.
///
/// # Panics
///
/// Panics unless the header's type tag is [`IteratorType::Hybrid`], which
/// catches a violation of requirement 2 but never one of requirement 1.
///
/// [`FieldExpirationChecker`]: rqe_iterators::FieldExpirationChecker
/// [`ExpirationChecker`]: rqe_iterators::ExpirationChecker
/// [`new_vector_top_k`]: crate::new_vector_top_k
/// [`ReducedEmpty`]: crate::NewVectorTopK::ReducedEmpty
/// [`VectorScoreSource`]: crate::VectorScoreSource
pub unsafe fn own_key_ref<'index>(header: NonNull<QueryIterator>) -> *mut *mut RLookupKey<'index> {
    // SAFETY: guaranteed by the caller.
    let wrapper = unsafe { wrapper_mut(header) };
    &raw mut *wrapper.inner.source_mut().own_key
}

/// Give the iterator the [`RLookupKeyHandle`] it invalidates when freed.
///
/// # Safety
///
/// 1. Same requirements as [`own_key_ref`].
/// 2. `handle` is null, or points to a valid [`RLookupKeyHandle`] that outlives
///    the iterator. The iterator clears the handle's validity flag on its way
///    out, so freeing the handle first is a use-after-free at a distance: the
///    write lands whenever the iterator is dropped, not during this call.
///
/// # Panics
///
/// Panics on any header [`own_key_ref`] would panic on.
pub unsafe fn set_key_handle<'index>(
    header: NonNull<QueryIterator>,
    handle: *mut RLookupKeyHandle<'index>,
) {
    // SAFETY: guaranteed by 1.
    let wrapper = unsafe { wrapper_mut::<'index>(header) };
    wrapper.inner.source_mut().key_handle = handle;
}

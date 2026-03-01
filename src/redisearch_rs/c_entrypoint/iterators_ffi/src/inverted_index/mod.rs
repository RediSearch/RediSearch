/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

mod numeric;
mod term;
mod wildcard;

use numeric::NumericIterator;
use rqe_iterators_interop::RQEIteratorWrapper;
use wildcard::WildcardIterator;

/// Gets the flags of the underlying IndexReader from a numeric inverted index iterator.
///
/// # Safety
///
/// 1. `it` must be a valid non-NULL pointer to a `QueryIterator`.
/// 2. If `it` iterator type is IteratorType_INV_IDX_NUMERIC_ITERATOR, it has been created using `NewInvIndIterator_NumericQuery`.
/// 3. If `it` iterator type is IteratorType_INV_IDX_WILDCARD_ITERATOR, it has been created using `NewInvIndIterator_WildcardQuery`.
/// 4. If `it` has a different iterator type, its `reader` field must be a valid non-NULL pointer to an `IndexReader`.
///
/// # Returns
///
/// The flags of the `IndexReader`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn InvIndIterator_GetReaderFlags(
    it: *const ffi::InvIndIterator,
) -> ffi::IndexFlags {
    debug_assert!(!it.is_null());

    // SAFETY: 1.
    let it_ref = unsafe { &*it };

    match it_ref.base.type_ {
        ffi::IteratorType_INV_IDX_NUMERIC_ITERATOR => {
            // SAFETY: the numeric iterator is in Rust.
            let wrapper = unsafe {
                RQEIteratorWrapper::<NumericIterator<'static>>::ref_from_header_ptr(it.cast())
            };
            wrapper.inner.flags()
        }
        ffi::IteratorType_INV_IDX_WILDCARD_ITERATOR => {
            // SAFETY: 3. the wildcard iterator is in Rust.
            let wrapper = unsafe {
                RQEIteratorWrapper::<WildcardIterator<'static>>::ref_from_header_ptr(it.cast())
            };
            wrapper.inner.flags()
        }
        _ => {
            // C iterator
            let reader: *mut inverted_index_ffi::IndexReader = it_ref.reader.cast();
            // SAFETY: 4.
            let reader_ref = unsafe { &*reader };
            reader_ref.flags()
        }
    }
}

/// Swap the inverted index of an inverted index iterator. This is only used by C tests
/// to trigger revalidation on the iterator's underlying reader.
///
/// # Safety
///
/// 1. `it` must be a valid non-NULL pointer to an `InvIndIterator`.
/// 2. If `it` iterator type is `IteratorType_INV_IDX_WILDCARD_ITERATOR`, it has been created
///    using `NewInvIndIterator_WildcardQuery`.
/// 3. If `it` is a C iterator, its `reader` field must be a valid non-NULL
///    pointer to an `IndexReader`.
/// 4. `ii` must be a valid non-NULL pointer to an `InvertedIndex` whose type matches the
///    iterator's underlying index type.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn InvIndIterator_Rs_SwapIndex(
    it: *mut ffi::InvIndIterator,
    ii: *const ffi::InvertedIndex,
) {
    debug_assert!(!it.is_null());
    debug_assert!(!ii.is_null());

    // SAFETY: 1.
    let it_ref = unsafe { &*it };

    match it_ref.base.type_ {
        ffi::IteratorType_INV_IDX_NUMERIC_ITERATOR => {
            unimplemented!(
                "Numeric iterators use revision ID for revalidation, not index swapping"
            );
        }
        ffi::IteratorType_INV_IDX_WILDCARD_ITERATOR => {
            unimplemented!("Wildcard iterator is tested in Rust which does no use index swapping")
        }
        _ => {
            // C iterator
            let reader: *mut inverted_index_ffi::IndexReader = it_ref.reader.cast();
            // SAFETY: 3. guarantees reader is valid.
            let reader_ref = unsafe { &mut *reader };
            let ii: *const inverted_index_ffi::InvertedIndex = ii.cast();
            // SAFETY: 4. guarantees ii is valid and matching.
            let ii_ref = unsafe { &*ii };
            reader_ref.swap_index(ii_ref);
        }
    }
}

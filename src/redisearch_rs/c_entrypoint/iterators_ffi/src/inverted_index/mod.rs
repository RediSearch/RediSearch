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

use inverted_index::IndexReader as _;
use numeric::NumericIterator;
use rqe_iterators_interop::RQEIteratorWrapper;
pub use term::NewInvIndIterator_TermQuery;
use term::TermIterator;

/// Gets the flags of the underlying IndexReader from an inverted index iterator.
///
/// # Safety
///
/// 1. `it` must be a valid non-NULL pointer to a `QueryIterator`.
/// 2. If `it` iterator type is IteratorType_INV_IDX_NUMERIC_ITERATOR, it has been created using `NewInvIndIterator_NumericQuery`.
/// 3. If `it` iterator type is IteratorType_INV_IDX_TERM_ITERATOR, it has been created using `NewInvIndIterator_TermQuery`.
/// 4. If `it` has a different iterator type (other than INV_IDX_WILDCARD_ITERATOR and INV_IDX_TERM_ITERATOR), its `reader`
///    field must be a valid non-NULL pointer to an `IndexReader`.
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
            // SAFETY: 2. the numeric iterator is in Rust.
            let wrapper = unsafe {
                RQEIteratorWrapper::<NumericIterator<'static>>::ref_from_header_ptr(it.cast())
            };
            wrapper.inner.flags()
        }
        ffi::IteratorType_INV_IDX_WILDCARD_ITERATOR => {
            // Wildcard iterators always read from `spec.existingDocs`, which is
            // created with `Index_DocIdsOnly` flags (see indexer.c). We return the
            // flags directly instead of casting to a concrete wrapper type, because
            // the iterator may have been created by either
            // `NewInvIndIterator_WildcardQuery` (RQEIteratorWrapper<WildcardIterator>)
            // or `NewWildcardIterator` (RQEIteratorWrapper<Box<dyn RQEIterator>>).
            ffi::IndexFlags_Index_DocIdsOnly
        }
        ffi::IteratorType_INV_IDX_TERM_ITERATOR => {
            // SAFETY: 3. the term iterator is in Rust.
            let wrapper = unsafe {
                RQEIteratorWrapper::<TermIterator<'static>>::ref_from_header_ptr(it.cast())
            };
            wrapper.inner.reader().flags()
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
        ffi::IteratorType_INV_IDX_TERM_ITERATOR => {
            panic!("SwapIndex is not meant to be used with term iterators");
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

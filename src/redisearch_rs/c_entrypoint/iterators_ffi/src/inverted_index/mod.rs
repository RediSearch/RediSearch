/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

mod missing;
mod numeric;
mod tag;
mod term;
mod wildcard;

use inverted_index::IndexReader as _;
use missing::MissingIterator;
use numeric::NumericIterator;
use rqe_iterator_type::IteratorType;
use rqe_iterators::interop::RQEIteratorWrapper;
use tag::TagIterator;
pub use term::NewInvIndIterator_TermQuery;
use term::TermIterator;

/// Gets the flags of the underlying IndexReader from an inverted index iterator.
///
/// # Safety
///
/// 1. `it` must be a valid non-NULL pointer to a `QueryIterator`.
/// 2. If `it` iterator type is [`IteratorType::InvIdxNumeric`], it has been created using `NewNumericFilterIterator`.
/// 3. If `it` iterator type is [`IteratorType::InvIdxTerm`], it has been created using `NewInvIndIterator_TermQuery`.
/// 4. If `it` iterator type is [`IteratorType::InvIdxMissing`], it has been created using `NewInvIndIterator_MissingQuery`.
/// 5. If `it` iterator type is [`IteratorType::InvIdxTag`], it has been created using `NewInvIndIterator_TagQuery`.
///
/// # Panics
///
/// Panics if the iterator type is not one of the supported inverted index
/// iterator types.
///
/// # Returns
///
/// The flags of the `IndexReader`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn InvIndIterator_GetReaderFlags(
    it: *const ffi::QueryIterator,
) -> ffi::IndexFlags {
    debug_assert!(!it.is_null());

    // SAFETY: 1.
    let it_ref = unsafe { &*it };

    match it_ref.type_ {
        IteratorType::InvIdxNumeric => {
            // SAFETY: 2. the numeric iterator is in Rust.
            let wrapper = unsafe {
                RQEIteratorWrapper::<NumericIterator<'static>>::ref_from_header_ptr(it.cast())
            };
            wrapper.inner.flags()
        }
        IteratorType::InvIdxWildcard => {
            // Wildcard iterators always read from `spec.existingDocs`, which is
            // created with `Index_DocIdsOnly` flags (see indexer.c). We return the
            // flags directly instead of casting to a concrete wrapper type, because
            // the iterator may have been created by either
            // `NewInvIndIterator_WildcardQuery` (RQEIteratorWrapper<WildcardIterator>)
            // or `NewWildcardIterator` (RQEIteratorWrapper<Box<dyn RQEIterator>>).
            ffi::IndexFlags_Index_DocIdsOnly
        }
        IteratorType::InvIdxTerm => {
            // SAFETY: 3. the term iterator is in Rust.
            let wrapper = unsafe {
                RQEIteratorWrapper::<TermIterator<'static>>::ref_from_header_ptr(it.cast())
            };
            wrapper.inner.reader().flags()
        }
        IteratorType::InvIdxMissing => {
            // SAFETY: 4. the missing iterator is in Rust.
            let wrapper = unsafe {
                RQEIteratorWrapper::<MissingIterator<'static>>::ref_from_header_ptr(it.cast())
            };
            wrapper.inner.flags()
        }
        IteratorType::InvIdxTag => {
            // SAFETY: 5. the tag iterator is in Rust.
            let wrapper = unsafe {
                RQEIteratorWrapper::<TagIterator<'static>>::ref_from_header_ptr(it.cast())
            };
            wrapper.inner.flags()
        }
        other => {
            panic!("InvIndIterator_GetReaderFlags: unexpected iterator type {other}")
        }
    }
}

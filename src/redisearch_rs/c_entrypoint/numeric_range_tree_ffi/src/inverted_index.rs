/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI functions for accessing inverted indexes within numeric ranges.

use ffi::t_docId;

use crate::InvertedIndexNumeric;

// ============================================================================
// InvertedIndexNumeric accessor functions
// ============================================================================

/// Get the number of documents in a numeric inverted index.
///
/// # Safety
///
/// - `idx` must point to a valid [`InvertedIndexNumeric`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn InvertedIndexNumeric_NumDocs(idx: *const InvertedIndexNumeric) -> u32 {
    debug_assert!(!idx.is_null(), "idx cannot be NULL");
    // SAFETY: Caller ensures `idx` is valid per function safety docs.
    let idx = unsafe { &*idx };
    match idx {
        InvertedIndexNumeric::Uncompressed(entries) => entries.summary().number_of_docs,
        InvertedIndexNumeric::Compressed(entries) => entries.summary().number_of_docs,
    }
}

/// Get the number of blocks in a numeric inverted index.
///
/// # Safety
///
/// - `idx` must point to a valid [`InvertedIndexNumeric`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn InvertedIndexNumeric_NumBlocks(idx: *const InvertedIndexNumeric) -> usize {
    debug_assert!(!idx.is_null(), "idx cannot be NULL");
    // SAFETY: Caller ensures `idx` is valid per function safety docs.
    let idx = unsafe { &*idx };
    match idx {
        InvertedIndexNumeric::Uncompressed(entries) => entries.summary().number_of_blocks,
        InvertedIndexNumeric::Compressed(entries) => entries.summary().number_of_blocks,
    }
}

/// Get the first document ID in a specific block of a numeric inverted index.
///
/// This is used by fork GC to find the starting point for HLL cardinality
/// recalculation after applying GC deltas.
///
/// Returns 0 if the block index is out of bounds.
///
/// # Safety
///
/// - `idx` must point to a valid [`InvertedIndexNumeric`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn InvertedIndexNumeric_BlockFirstId(
    idx: *const InvertedIndexNumeric,
    block_idx: usize,
) -> t_docId {
    debug_assert!(!idx.is_null(), "idx cannot be NULL");
    // SAFETY: Caller ensures `idx` is valid per function safety docs.
    let idx = unsafe { &*idx };
    match idx {
        InvertedIndexNumeric::Uncompressed(entries) => entries
            .block_ref(block_idx)
            .map(|b| b.first_block_id())
            .unwrap_or(0),
        InvertedIndexNumeric::Compressed(entries) => entries
            .block_ref(block_idx)
            .map(|b| b.first_block_id())
            .unwrap_or(0),
    }
}

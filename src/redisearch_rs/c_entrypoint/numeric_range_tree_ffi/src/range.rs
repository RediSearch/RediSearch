/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI functions for accessing numeric ranges and their HLL cardinality estimators.

use inverted_index::{FilterGeoReader, FilterNumericReader, NumericFilter};
use numeric_range_tree::NumericRange;

use crate::{IndexReader, InvertedIndexNumeric};

// ============================================================================
// NumericRange accessor functions
// ============================================================================

/// Get the estimated cardinality (number of distinct values) for a range.
///
/// This uses HyperLogLog estimation and may have some error margin.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `range` must point to a valid [`NumericRange`] obtained from
///   [`crate::node::NumericRangeNode_GetRange`] and cannot be NULL.
/// - The tree from which this range came must still be valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRange_GetCardinality(range: *const NumericRange) -> usize {
    debug_assert!(!range.is_null(), "range cannot be NULL");

    // SAFETY: Caller is to ensure that `range` is a valid, non-null pointer
    // to a NumericRange obtained from NumericRangeNode_GetRange.
    let range = unsafe { &*range };
    range.cardinality()
}

/// Get the minimum value in a range.
///
/// # Safety
///
/// - `range` must point to a valid [`NumericRange`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRange_MinVal(range: *const NumericRange) -> f64 {
    debug_assert!(!range.is_null(), "range cannot be NULL");
    // SAFETY: Caller ensures `range` is valid per function safety docs.
    let range = unsafe { &*range };
    range.min_val()
}

/// Get the maximum value in a range.
///
/// # Safety
///
/// - `range` must point to a valid [`NumericRange`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRange_MaxVal(range: *const NumericRange) -> f64 {
    debug_assert!(!range.is_null(), "range cannot be NULL");
    // SAFETY: Caller ensures `range` is valid per function safety docs.
    let range = unsafe { &*range };
    range.max_val()
}

/// Get the inverted index size in bytes.
///
/// # Safety
///
/// - `range` must point to a valid [`NumericRange`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRange_InvertedIndexSize(range: *const NumericRange) -> usize {
    debug_assert!(!range.is_null(), "range cannot be NULL");
    // SAFETY: Caller ensures `range` is valid per function safety docs.
    let range = unsafe { &*range };
    range.memory_usage()
}

/// Get the inverted index entries from a range.
///
/// Returns a pointer to the [`InvertedIndexNumeric`] (which is a `NumericIndex` enum)
/// stored inside the range. The returned pointer is valid until the tree is modified or freed.
///
/// # Safety
///
/// - `range` must point to a valid [`NumericRange`] and cannot be NULL.
/// - The returned pointer points to memory owned by the range; do not free it.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRange_GetEntries(
    range: *const NumericRange,
) -> *const InvertedIndexNumeric {
    debug_assert!(!range.is_null(), "range cannot be NULL");
    // SAFETY: Caller ensures `range` is valid per function safety docs.
    let range = unsafe { &*range };
    range.entries() as *const InvertedIndexNumeric
}

/// Create an [`IndexReader`] for iterating over a [`NumericRange`]'s entries.
///
/// This is the primary way to iterate over numeric index entries from C code.
/// The returned reader can be used with `IndexReader_Next()`, `IndexReader_Seek()`, etc.
/// from `inverted_index_ffi`.
///
/// If `filter` is NULL, all entries are returned. Otherwise, entries are filtered
/// according to the numeric filter (or geo filter if the filter's `geo_filter` is set).
///
/// # Safety
///
/// - `range` must point to a valid [`NumericRange`] and cannot be NULL.
/// - `filter` may be NULL for no filtering, or must point to a valid [`NumericFilter`].
/// - The returned reader holds a reference to the range's inverted index. The range
///   must not be freed or modified while the reader exists.
/// - The filter (if non-NULL) must remain valid for the lifetime of the reader.
/// - Free the returned reader with `IndexReader_Free()` when done.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRange_NewIndexReader<'a>(
    range: *const NumericRange,
    filter: *const NumericFilter,
) -> *mut IndexReader<'a> {
    debug_assert!(!range.is_null(), "range cannot be NULL");

    // SAFETY: Caller guarantees range is valid and non-NULL
    let range = unsafe { &*range };

    let index_reader = match range.entries() {
        InvertedIndexNumeric::Compressed(entries) => {
            let reader = entries.reader();

            if filter.is_null() {
                IndexReader::NumericFloatCompression(reader)
            } else {
                // SAFETY: Caller guarantees filter is valid if non-NULL
                let filter = unsafe { &*filter };

                if filter.is_numeric_filter() {
                    IndexReader::NumericFilteredFloatCompression(FilterNumericReader::new(
                        filter, reader,
                    ))
                } else {
                    IndexReader::NumericGeoFilteredFloatCompression(FilterGeoReader::new(
                        filter, reader,
                    ))
                }
            }
        }
        InvertedIndexNumeric::Uncompressed(entries) => {
            let reader = entries.reader();

            if filter.is_null() {
                IndexReader::Numeric(reader)
            } else {
                // SAFETY: Caller guarantees filter is valid if non-NULL
                let filter = unsafe { &*filter };

                if filter.is_numeric_filter() {
                    IndexReader::NumericFiltered(FilterNumericReader::new(filter, reader))
                } else {
                    IndexReader::NumericGeoFiltered(FilterGeoReader::new(filter, reader))
                }
            }
        }
    };

    Box::into_raw(Box::new(index_reader))
}

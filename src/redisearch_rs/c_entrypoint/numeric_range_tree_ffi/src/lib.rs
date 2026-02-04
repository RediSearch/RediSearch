/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI bindings for the Numeric Range Tree.
//!
//! This crate provides C-callable functions to interact with the Rust
//! [`numeric_range_tree`] implementation.
//!
//! # Module Organization
//!
//! - [`iterator`]: Tree iteration (depth-first traversal)
//! - [`tree`]: Tree-level accessors and mutations
//! - [`node`]: Node accessors (range, children, etc.)
//! - [`range`]: NumericRange accessors and HLL functions
//! - [`inverted_index`]: InvertedIndexNumeric accessors and reader
//! - [`gc`]: Garbage collection scan and apply functions

#![allow(non_camel_case_types, non_snake_case)]

pub mod debug;
pub mod gc;
pub mod iterator;
pub mod node;
pub mod range;
pub mod tree;

// Re-export all public FFI functions from submodules
pub use debug::*;
pub use gc::*;
pub use iterator::*;
pub use node::*;
pub use range::*;
pub use tree::*;

use ::inverted_index::NumericFilter;
use ffi::t_docId;
use numeric_range_tree::{
    DepthFirstNumericRangeTreeIterator as IteratorInner, NumericRangeTree as NumericRangeTreeInner,
};
use std::ffi::c_int;
use tracing::debug;

// Re-export IndexReader type from inverted_index_ffi for C code to use.
pub use inverted_index_ffi::IndexReader;

// Re-export the Numeric encoder types for use in the FFI.
pub use ::inverted_index::numeric::{Numeric, NumericFloatCompression};

// Re-export NumericIndex from numeric_range_tree as InvertedIndexNumeric for FFI.
// This provides the opaque type that C code uses to access numeric index entries.
pub use numeric_range_tree::NumericIndex as InvertedIndexNumeric;

/// Result of adding a value to a [`NumericRangeTree`].
///
/// This struct is C-compatible and mirrors the information returned by
/// [`numeric_range_tree::AddResult`].
#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct NRN_AddRv {
    /// The number of bytes the tree's memory usage changed by.
    pub sz: c_int,
    /// The number of records added.
    pub numRecords: c_int,
    /// Whether the tree structure changed (1 if splits occurred, 0 otherwise).
    pub changed: c_int,
    /// The change in the number of ranges.
    pub numRanges: c_int,
    /// The change in the number of leaves.
    pub numLeaves: c_int,
}

/// Opaque wrapper around the Rust [`NumericRangeTreeInner`].
pub struct NumericRangeTree(pub(crate) NumericRangeTreeInner);

// Note: NumericRangeNode and NumericRange are exposed directly from the numeric_range_tree crate.
// They are opaque types from C's perspective - C code should only use pointers to them.

/// Opaque wrapper around a [`IteratorInner`].
///
/// The iterator holds references to nodes in the tree. The tree must not be
/// freed or mutated while this iterator exists.
pub struct NumericRangeTreeIterator<'a>(pub(crate) IteratorInner<'a>);

/// Result of [`NumericRangeTree_Find`] - an array of range pointers.
///
/// The caller is responsible for freeing this result using
/// [`NumericRangeTreeFindResult_Free`]. The ranges themselves are owned by
/// the tree and must not be freed individually.
#[repr(C)]
pub struct NumericRangeTreeFindResult {
    /// Pointer to array of range pointers.
    pub ranges: *const *const numeric_range_tree::NumericRange,
    /// Number of ranges in the array.
    pub len: usize,
    /// Capacity of the array (for deallocation).
    capacity: usize,
}

// ============================================================================
// Core lifecycle functions
// ============================================================================

/// Create a new [`NumericRangeTree`].
///
/// Returns an opaque pointer to the newly created tree.
/// To free the tree, use [`NumericRangeTree_Free`].
///
/// If `compress_floats` is true, the tree will use float compression which
/// attempts to store f64 values as f32 when precision loss is acceptable (< 0.01).
/// This corresponds to the `RSGlobalConfig.numericCompress` setting.
#[unsafe(no_mangle)]
pub extern "C" fn NewNumericRangeTree(compress_floats: bool) -> *mut NumericRangeTree {
    let tree = Box::new(NumericRangeTree(NumericRangeTreeInner::new(
        compress_floats,
    )));
    Box::into_raw(tree)
}

/// Add a (docId, value) pair to the tree.
///
/// If `isMulti` is non-zero, duplicate document IDs are allowed.
/// `maxDepthRange` specifies the maximum depth at which to retain ranges on inner nodes.
///
/// Returns an [`NRN_AddRv`] struct containing information about what changed
/// during the add operation.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`NumericRangeTree`] obtained from
///   [`NewNumericRangeTree`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_Add(
    t: *mut NumericRangeTree,
    doc_id: t_docId,
    value: f64,
    isMulti: c_int,
    maxDepthRange: usize,
) -> NRN_AddRv {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    debug!(
        doc_id,
        value, isMulti, maxDepthRange, "NumericRangeTree_Add FFI called"
    );

    // SAFETY: Caller is to ensure that `t` is a valid, non-null pointer
    // to a NumericRangeTree obtained from NewNumericRangeTree.
    let NumericRangeTree(tree) = unsafe { &mut *t };

    let result = tree.add(doc_id, value, isMulti != 0, maxDepthRange);

    debug!(
        sz = result.size_delta,
        numRecords = result.num_records,
        changed = result.changed,
        "NumericRangeTree_Add FFI completed"
    );

    NRN_AddRv {
        sz: result.size_delta as c_int,
        numRecords: result.num_records,
        changed: if result.changed { 1 } else { 0 },
        numRanges: result.num_ranges_delta,
        numLeaves: result.num_leaves_delta,
    }
}

/// Free a [`NumericRangeTree`] and all its contents.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`NumericRangeTree`] obtained from
///   [`NewNumericRangeTree`], or be NULL (in which case this is a no-op).
/// - After calling this function, `t` must not be used again.
/// - Any iterators obtained from this tree must be freed before calling this.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_Free(t: *mut NumericRangeTree) {
    if t.is_null() {
        return;
    }

    // SAFETY: Caller is to ensure that `t` is a valid pointer to a
    // NumericRangeTree obtained from NewNumericRangeTree.
    // Reconstructing the Box will free the memory when it's dropped.
    unsafe {
        let _ = Box::from_raw(t);
    };
}

/// Get the total memory usage of the tree in bytes.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`NumericRangeTree`] obtained from
///   [`NewNumericRangeTree`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_MemUsage(t: *const NumericRangeTree) -> usize {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: Caller is to ensure that `t` is a valid, non-null pointer
    // to a NumericRangeTree obtained from NewNumericRangeTree.
    let NumericRangeTree(tree) = unsafe { &*t };
    tree.mem_usage()
}

/// Find all numeric ranges that match the given filter.
///
/// Returns a [`NumericRangeTreeFindResult`] containing pointers to the matching
/// ranges. The ranges are owned by the tree and must not be freed individually.
/// The result itself must be freed using [`NumericRangeTreeFindResult_Free`].
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`NumericRangeTree`] obtained from
///   [`NewNumericRangeTree`] and cannot be NULL.
/// - `nf` must point to a valid [`NumericFilter`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_Find(
    t: *const NumericRangeTree,
    nf: *const NumericFilter,
) -> NumericRangeTreeFindResult {
    debug_assert!(!t.is_null(), "t cannot be NULL");
    debug_assert!(!nf.is_null(), "nf cannot be NULL");

    // SAFETY: Caller ensures `t` is a valid, non-null pointer.
    let NumericRangeTree(tree) = unsafe { &*t };
    // SAFETY: Caller ensures `nf` is a valid, non-null pointer.
    let filter = unsafe { &*nf };

    debug!(
        min = filter.min,
        max = filter.max,
        ascending = filter.ascending,
        limit = filter.limit,
        offset = filter.offset,
        "NumericRangeTree_Find FFI called"
    );

    let ranges = tree.find(filter);

    debug!(
        num_ranges_found = ranges.len(),
        "NumericRangeTree_Find FFI: ranges found"
    );

    // Log details about each range found
    for (i, range) in ranges.iter().enumerate() {
        debug!(
            index = i,
            min_val = range.min_val(),
            max_val = range.max_val(),
            num_docs = range.num_docs(),
            num_entries = range.num_entries(),
            "Found range details"
        );
    }

    // Convert Vec<&NumericRange> to array of pointers
    let range_ptrs: Vec<*const numeric_range_tree::NumericRange> = ranges
        .into_iter()
        .map(|r| r as *const numeric_range_tree::NumericRange)
        .collect();

    let result = NumericRangeTreeFindResult {
        ranges: range_ptrs.as_ptr(),
        len: range_ptrs.len(),
        capacity: range_ptrs.capacity(),
    };

    // Prevent Vec from being dropped (caller will free via NumericRangeTreeFindResult_Free)
    std::mem::forget(range_ptrs);

    result
}

/// Free a [`NumericRangeTreeFindResult`].
///
/// This frees the array allocation but NOT the ranges themselves (they are
/// owned by the tree).
///
/// # Safety
///
/// - `result` must have been obtained from [`NumericRangeTree_Find`].
/// - After calling this function, the result must not be used again.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTreeFindResult_Free(result: NumericRangeTreeFindResult) {
    if result.ranges.is_null() {
        return;
    }

    // SAFETY: We reconstruct the Vec with the original capacity to properly free it.
    // The ranges pointer came from a Vec<*const NumericRange>.
    unsafe {
        let _ = Vec::from_raw_parts(
            result.ranges as *mut *const numeric_range_tree::NumericRange,
            result.len,
            result.capacity,
        );
    }
}

/// Trim empty leaves from the tree (garbage collection).
///
/// Removes leaf nodes that have no documents and prunes the tree structure
/// accordingly.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`NumericRangeTree`] obtained from
///   [`NewNumericRangeTree`] and cannot be NULL.
/// - No iterators should be active on this tree while calling this function.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_TrimEmptyLeaves(t: *mut NumericRangeTree) -> NRN_AddRv {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: Caller is to ensure that `t` is a valid, non-null pointer
    // to a NumericRangeTree obtained from NewNumericRangeTree.
    let NumericRangeTree(tree) = unsafe { &mut *t };

    let result = tree.trim_empty_leaves();

    NRN_AddRv {
        sz: result.size_delta as c_int,
        numRecords: result.num_records,
        changed: if result.changed { 1 } else { 0 },
        numRanges: result.num_ranges_delta,
        numLeaves: result.num_leaves_delta,
    }
}

// ============================================================================
// Size constants for memory overhead calculations
// ============================================================================

/// Get the base size of a NumericRangeTree struct (not including contents).
///
/// This is used for memory overhead calculations.
#[unsafe(no_mangle)]
pub const extern "C" fn NumericRangeTree_BaseSize() -> usize {
    std::mem::size_of::<NumericRangeTree>()
}

#[cfg(test)]
mod tests {
    use super::*;

    // Provide stubs for C functions that the inverted_index crate requires for linking.
    // These are required because NumericRange contains an inverted index.
    redis_mock::mock_or_stub_missing_redis_c_symbols!();

    #[unsafe(no_mangle)]
    pub extern "C" fn ResultMetrics_Free(metrics: *mut ffi::RSYieldableMetric) {
        if metrics.is_null() {
            return;
        }
        panic!(
            "did not expect any test to set metrics, but got: {:?}",
            // SAFETY: We just checked that metrics is not null
            unsafe { *metrics }
        );
    }

    #[unsafe(no_mangle)]
    pub extern "C" fn Term_Free(t: *mut ffi::RSQueryTerm) {
        if !t.is_null() {
            panic!("No test created a term record");
        }
    }

    #[test]
    fn test_create_and_free_tree() {
        let tree = NewNumericRangeTree(false);
        assert!(!tree.is_null());

        // SAFETY: tree is valid and was just created
        unsafe {
            NumericRangeTree_Free(tree);
        }
    }

    #[test]
    fn test_create_and_free_compressed_tree() {
        let tree = NewNumericRangeTree(true);
        assert!(!tree.is_null());

        // SAFETY: tree is valid and was just created
        unsafe {
            NumericRangeTree_Free(tree);
        }
    }

    #[test]
    fn test_free_null_tree() {
        // Should be a no-op, not crash
        // SAFETY: Passing NULL is explicitly allowed
        unsafe {
            NumericRangeTree_Free(std::ptr::null_mut());
        }
    }

    #[test]
    fn test_add_values() {
        let tree = NewNumericRangeTree(false);

        // SAFETY: tree is valid
        unsafe {
            let result = NumericRangeTree_Add(tree, 1, 10.0, 0, 0);
            assert_eq!(result.numRecords, 1);

            let result = NumericRangeTree_Add(tree, 2, 20.0, 0, 0);
            assert_eq!(result.numRecords, 1);

            NumericRangeTree_Free(tree);
        }
    }

    #[test]
    fn test_mem_usage() {
        let tree = NewNumericRangeTree(false);

        // SAFETY: tree is valid
        unsafe {
            let initial_usage = NumericRangeTree_MemUsage(tree);
            assert!(initial_usage > 0);

            NumericRangeTree_Add(tree, 1, 10.0, 0, 0);
            let usage_after_add = NumericRangeTree_MemUsage(tree);
            assert!(usage_after_add >= initial_usage);

            NumericRangeTree_Free(tree);
        }
    }

    #[test]
    fn test_iterator() {
        let tree = NewNumericRangeTree(false);

        // SAFETY: tree is valid
        unsafe {
            NumericRangeTree_Add(tree, 1, 10.0, 0, 0);
            NumericRangeTree_Add(tree, 2, 20.0, 0, 0);

            let iter = NumericRangeTreeIterator_New(tree);
            assert!(!iter.is_null());

            // Should get at least the root node
            let node = NumericRangeTreeIterator_Next(iter);
            assert!(!node.is_null());

            // Eventually should return NULL
            loop {
                let next = NumericRangeTreeIterator_Next(iter);
                if next.is_null() {
                    break;
                }
            }

            NumericRangeTreeIterator_Free(iter);
            NumericRangeTree_Free(tree);
        }
    }

    #[test]
    fn test_free_null_iterator() {
        // Should be a no-op, not crash
        // SAFETY: Passing NULL is explicitly allowed
        unsafe {
            NumericRangeTreeIterator_Free(std::ptr::null_mut());
        }
    }

    #[test]
    fn test_node_get_range() {
        let tree = NewNumericRangeTree(false);

        // SAFETY: tree is valid
        unsafe {
            NumericRangeTree_Add(tree, 1, 10.0, 0, 0);

            let iter = NumericRangeTreeIterator_New(tree);
            let node = NumericRangeTreeIterator_Next(iter);
            assert!(!node.is_null());

            // Root node should have a range
            let range = NumericRangeNode_GetRange(node);
            assert!(!range.is_null());

            NumericRangeTreeIterator_Free(iter);
            NumericRangeTree_Free(tree);
        }
    }

    #[test]
    fn test_range_cardinality() {
        let tree = NewNumericRangeTree(false);

        // SAFETY: tree is valid
        unsafe {
            NumericRangeTree_Add(tree, 1, 10.0, 0, 0);
            NumericRangeTree_Add(tree, 2, 20.0, 0, 0);
            NumericRangeTree_Add(tree, 3, 10.0, 0, 0); // Duplicate value

            let iter = NumericRangeTreeIterator_New(tree);
            let node = NumericRangeTreeIterator_Next(iter);
            let range = NumericRangeNode_GetRange(node);
            assert!(!range.is_null());

            let cardinality = NumericRange_GetCardinality(range);
            // Should have 2 distinct values (10.0 and 20.0)
            assert!(cardinality >= 1); // HLL has some error margin

            NumericRangeTreeIterator_Free(iter);
            NumericRangeTree_Free(tree);
        }
    }

    #[test]
    fn test_find_ranges() {
        let tree = NewNumericRangeTree(false);

        // SAFETY: tree is valid
        unsafe {
            // Add some values
            NumericRangeTree_Add(tree, 1, 10.0, 0, 0);
            NumericRangeTree_Add(tree, 2, 20.0, 0, 0);
            NumericRangeTree_Add(tree, 3, 30.0, 0, 0);

            // Create a filter
            let filter = NumericFilter {
                min: 5.0,
                max: 25.0,
                min_inclusive: true,
                max_inclusive: true,
                ascending: true,
                limit: 0,
                offset: 0,
                field_spec: std::ptr::null(),
                geo_filter: std::ptr::null(),
            };

            let result = NumericRangeTree_Find(tree, &filter);
            assert!(result.len >= 1); // Should find at least the root range

            // Free the result
            NumericRangeTreeFindResult_Free(result);

            NumericRangeTree_Free(tree);
        }
    }

    #[test]
    fn test_trim_empty_leaves() {
        let tree = NewNumericRangeTree(false);

        // SAFETY: tree is valid
        unsafe {
            // Add and then test trim (even with non-empty leaves, it should work)
            NumericRangeTree_Add(tree, 1, 10.0, 0, 0);

            let result = NumericRangeTree_TrimEmptyLeaves(tree);
            // No empty leaves to trim
            assert_eq!(result.changed, 0);

            NumericRangeTree_Free(tree);
        }
    }
}

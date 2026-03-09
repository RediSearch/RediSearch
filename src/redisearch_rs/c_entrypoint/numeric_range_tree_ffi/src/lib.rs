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
use numeric_range_tree::AddResult;
use numeric_range_tree::TrimEmptyLeavesResult;
pub use range::*;
pub use tree::*;

use ::inverted_index::NumericFilter;
use ffi::t_docId;
use std::ffi::c_int;

// Re-export IndexReader type from inverted_index_ffi for C code to use.
pub use inverted_index_ffi::IndexReader;

// Re-export the Numeric encoder types for use in the FFI.
pub use ::inverted_index::numeric::{Numeric, NumericFloatCompression};

// Re-export NumericIndex from numeric_range_tree as InvertedIndexNumeric for FFI.
// This provides the opaque type that C code uses to access numeric index entries.
pub use numeric_range_tree::NumericIndex as InvertedIndexNumeric;

// Re-export core types directly — they are opaque to C code (accessed via pointers).
pub use numeric_range_tree::NumericRangeTree;

/// Type alias for the tree iterator, providing a C-friendly name.
///
/// The iterator holds references to nodes in the tree. The tree must not be
/// freed or mutated while this iterator exists.
pub type NumericRangeTreeIterator<'a> = numeric_range_tree::ReversePreOrderDfsIterator<'a>;

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
    Box::into_raw(Box::new(NumericRangeTree::new(compress_floats)))
}

/// Add a (docId, value) pair to the tree.
///
/// If `isMulti` is non-zero, duplicate document IDs are allowed.
/// `maxDepthRange` specifies the maximum depth at which to retain ranges on inner nodes.
///
/// Returns information about what changed during the add operation.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`NumericRangeTree`] obtained from
///   [`NewNumericRangeTree`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn _NumericRangeTree_Add(
    t: *mut NumericRangeTree,
    doc_id: t_docId,
    value: f64,
    isMulti: c_int,
    maxDepthRange: usize,
) -> AddResult {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: Caller is to ensure that `t` is a valid, non-null pointer
    // to a NumericRangeTree obtained from NewNumericRangeTree.
    let tree = unsafe { &mut *t };

    tree.add(doc_id, value, isMulti != 0, maxDepthRange)
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
    let tree = unsafe { &*t };
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
    let tree = unsafe { &*t };
    // SAFETY: Caller ensures `nf` is a valid, non-null pointer.
    let filter = unsafe { &*nf };

    let ranges = tree.find(filter);

    // Convert Vec<&NumericRange> to a boxed slice of pointers.
    let range_ptrs: Box<[*const numeric_range_tree::NumericRange]> = ranges
        .into_iter()
        .map(|r| r as *const numeric_range_tree::NumericRange)
        .collect();

    let len = range_ptrs.len();
    let ptr = Box::into_raw(range_ptrs) as *const *const numeric_range_tree::NumericRange;

    NumericRangeTreeFindResult { ranges: ptr, len }
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

    // SAFETY: The pointer came from `Box::into_raw` in `NumericRangeTree_Find`.
    unsafe {
        let slice_ptr = std::ptr::slice_from_raw_parts_mut(
            result.ranges as *mut *const numeric_range_tree::NumericRange,
            result.len,
        );
        let _ = Box::from_raw(slice_ptr);
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
pub unsafe extern "C" fn NumericRangeTree_TrimEmptyLeaves(
    t: *mut NumericRangeTree,
) -> TrimEmptyLeavesResult {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: Caller is to ensure that `t` is a valid, non-null pointer
    // to a NumericRangeTree obtained from NewNumericRangeTree.
    let tree = unsafe { &mut *t };

    tree.trim_empty_leaves()
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

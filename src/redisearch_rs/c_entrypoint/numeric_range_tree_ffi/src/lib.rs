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

#![allow(non_camel_case_types, non_snake_case)]

use ffi::t_docId;
use numeric_range_tree::{NumericRange, NumericRangeNode, NumericRangeTree, NumericRangeTreeIterator};
use std::ffi::c_int;

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

/// Opaque wrapper around a [`NumericRangeTree`].
pub struct NumericRangeTree_FFI(NumericRangeTree);

/// Opaque wrapper around a [`NumericRangeTreeIterator`].
///
/// The iterator holds references to nodes in the tree. The tree must not be
/// freed or mutated while this iterator exists.
pub struct NumericRangeTreeIterator_FFI<'a>(NumericRangeTreeIterator<'a>);

/// Create a new [`NumericRangeTree`].
///
/// Returns an opaque pointer to the newly created tree.
/// To free the tree, use [`NumericRangeTree_Free`].
#[unsafe(no_mangle)]
pub extern "C" fn NewNumericRangeTree() -> *mut NumericRangeTree_FFI {
    let tree = Box::new(NumericRangeTree_FFI(NumericRangeTree::new()));
    Box::into_raw(tree)
}

/// Add a (docId, value) pair to the tree.
///
/// If `isMulti` is non-zero, duplicate document IDs are allowed.
///
/// Returns an [`NRN_AddRv`] struct containing information about what changed
/// during the add operation.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`NumericRangeTree_FFI`] obtained from
///   [`NewNumericRangeTree`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_Add(
    t: *mut NumericRangeTree_FFI,
    doc_id: t_docId,
    value: f64,
    isMulti: c_int,
) -> NRN_AddRv {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: Caller is to ensure that `t` is a valid, non-null pointer
    // to a NumericRangeTree_FFI obtained from NewNumericRangeTree.
    let NumericRangeTree_FFI(tree) = unsafe { &mut *t };

    let result = tree.add(doc_id, value, isMulti != 0);

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
/// - `t` must point to a valid [`NumericRangeTree_FFI`] obtained from
///   [`NewNumericRangeTree`], or be NULL (in which case this is a no-op).
/// - After calling this function, `t` must not be used again.
/// - Any iterators obtained from this tree must be freed before calling this.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_Free(t: *mut NumericRangeTree_FFI) {
    if t.is_null() {
        return;
    }

    // SAFETY: Caller is to ensure that `t` is a valid pointer to a
    // NumericRangeTree_FFI obtained from NewNumericRangeTree.
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
/// - `t` must point to a valid [`NumericRangeTree_FFI`] obtained from
///   [`NewNumericRangeTree`] and cannot be NULL.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTree_MemUsage(t: *const NumericRangeTree_FFI) -> usize {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: Caller is to ensure that `t` is a valid, non-null pointer
    // to a NumericRangeTree_FFI obtained from NewNumericRangeTree.
    let NumericRangeTree_FFI(tree) = unsafe { &*t };
    tree.mem_usage()
}

/// Create a new iterator over all nodes in the tree.
///
/// The iterator performs a depth-first traversal, visiting each node exactly once.
/// Use [`NumericRangeTreeIterator_Next`] to advance the iterator.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`NumericRangeTree_FFI`] obtained from
///   [`NewNumericRangeTree`] and cannot be NULL.
/// - `t` must not be freed while the iterator lives.
/// - The tree must not be mutated while the iterator lives.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTreeIterator_New<'a>(
    t: *const NumericRangeTree_FFI,
) -> *mut NumericRangeTreeIterator_FFI<'a> {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: Caller is to ensure that `t` is a valid, non-null pointer
    // to a NumericRangeTree_FFI obtained from NewNumericRangeTree.
    let NumericRangeTree_FFI(tree) = unsafe { &*t };

    let iter = Box::new(NumericRangeTreeIterator_FFI(NumericRangeTreeIterator::new(
        tree,
    )));
    Box::into_raw(iter)
}

/// Advance the iterator and return the next node.
///
/// Returns a pointer to the next [`NumericRangeNode`] in the traversal,
/// or NULL if the iteration is complete.
///
/// The returned pointer is valid until the tree is modified or freed.
/// Do NOT free the returned pointer - it points to memory owned by the tree.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `it` must point to a valid [`NumericRangeTreeIterator_FFI`] obtained from
///   [`NumericRangeTreeIterator_New`] and cannot be NULL.
/// - The tree from which this iterator was created must still be valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTreeIterator_Next(
    it: *mut NumericRangeTreeIterator_FFI,
) -> *const NumericRangeNode {
    debug_assert!(!it.is_null(), "it cannot be NULL");

    // SAFETY: Caller is to ensure that `it` is a valid, non-null pointer
    // to a NumericRangeTreeIterator_FFI obtained from NumericRangeTreeIterator_New.
    let NumericRangeTreeIterator_FFI(iter) = unsafe { &mut *it };

    match iter.next() {
        Some(node) => node as *const NumericRangeNode,
        None => std::ptr::null(),
    }
}

/// Free a [`NumericRangeTreeIterator_FFI`].
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `it` must point to a valid [`NumericRangeTreeIterator_FFI`] obtained from
///   [`NumericRangeTreeIterator_New`], or be NULL (in which case this is a no-op).
/// - After calling this function, `it` must not be used again.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTreeIterator_Free(it: *mut NumericRangeTreeIterator_FFI) {
    if it.is_null() {
        return;
    }

    // SAFETY: Caller is to ensure that `it` is a valid pointer to a
    // NumericRangeTreeIterator_FFI obtained from NumericRangeTreeIterator_New.
    unsafe {
        let _ = Box::from_raw(it);
    };
}

/// Get the [`NumericRange`] from a node, if present.
///
/// Returns a pointer to the range, or NULL if the node has no range
/// (e.g., an internal node whose range has been trimmed).
///
/// The returned pointer is valid until the tree is modified or freed.
/// Do NOT free the returned pointer - it points to memory owned by the tree.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `node` must point to a valid [`NumericRangeNode`] obtained from
///   [`NumericRangeTreeIterator_Next`] and cannot be NULL.
/// - The tree from which this node came must still be valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeNode_GetRange(
    node: *const NumericRangeNode,
) -> *const NumericRange {
    debug_assert!(!node.is_null(), "node cannot be NULL");

    // SAFETY: Caller is to ensure that `node` is a valid, non-null pointer
    // to a NumericRangeNode obtained from NumericRangeTreeIterator_Next.
    let node = unsafe { &*node };

    match node.range() {
        Some(range) => range as *const NumericRange,
        None => std::ptr::null(),
    }
}

/// Get the estimated cardinality (number of distinct values) for a range.
///
/// This uses HyperLogLog estimation and may have some error margin.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `range` must point to a valid [`NumericRange`] obtained from
///   [`NumericRangeNode_GetRange`] and cannot be NULL.
/// - The tree from which this range came must still be valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRange_GetCardinality(range: *const NumericRange) -> usize {
    debug_assert!(!range.is_null(), "range cannot be NULL");

    // SAFETY: Caller is to ensure that `range` is a valid, non-null pointer
    // to a NumericRange obtained from NumericRangeNode_GetRange.
    let range = unsafe { &*range };
    range.cardinality()
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
        let tree = NewNumericRangeTree();
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
        let tree = NewNumericRangeTree();

        // SAFETY: tree is valid
        unsafe {
            let result = NumericRangeTree_Add(tree, 1, 10.0, 0);
            assert_eq!(result.numRecords, 1);

            let result = NumericRangeTree_Add(tree, 2, 20.0, 0);
            assert_eq!(result.numRecords, 1);

            NumericRangeTree_Free(tree);
        }
    }

    #[test]
    fn test_mem_usage() {
        let tree = NewNumericRangeTree();

        // SAFETY: tree is valid
        unsafe {
            let initial_usage = NumericRangeTree_MemUsage(tree);
            assert!(initial_usage > 0);

            NumericRangeTree_Add(tree, 1, 10.0, 0);
            let usage_after_add = NumericRangeTree_MemUsage(tree);
            assert!(usage_after_add >= initial_usage);

            NumericRangeTree_Free(tree);
        }
    }

    #[test]
    fn test_iterator() {
        let tree = NewNumericRangeTree();

        // SAFETY: tree is valid
        unsafe {
            NumericRangeTree_Add(tree, 1, 10.0, 0);
            NumericRangeTree_Add(tree, 2, 20.0, 0);

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
        let tree = NewNumericRangeTree();

        // SAFETY: tree is valid
        unsafe {
            NumericRangeTree_Add(tree, 1, 10.0, 0);

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
        let tree = NewNumericRangeTree();

        // SAFETY: tree is valid
        unsafe {
            NumericRangeTree_Add(tree, 1, 10.0, 0);
            NumericRangeTree_Add(tree, 2, 20.0, 0);
            NumericRangeTree_Add(tree, 3, 10.0, 0); // Duplicate value

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
}

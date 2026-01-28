/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI functions for iterating over numeric range tree nodes.

use numeric_range_tree::{DepthFirstNumericRangeTreeIterator as IteratorInner, NumericRangeNode};

use crate::{NumericRangeTree, NumericRangeTreeIterator};

/// Create a new iterator over all nodes in the tree.
///
/// The iterator performs a depth-first traversal, visiting each node exactly once.
/// Use [`NumericRangeTreeIterator_Next`] to advance the iterator.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `t` must point to a valid [`NumericRangeTree`] obtained from
///   [`crate::NewNumericRangeTree`] and cannot be NULL.
/// - `t` must not be freed while the iterator lives.
/// - The tree must not be mutated while the iterator lives.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTreeIterator_New<'a>(
    t: *const NumericRangeTree,
) -> *mut NumericRangeTreeIterator<'a> {
    debug_assert!(!t.is_null(), "t cannot be NULL");

    // SAFETY: Caller is to ensure that `t` is a valid, non-null pointer
    // to a NumericRangeTree obtained from NewNumericRangeTree.
    let NumericRangeTree(tree) = unsafe { &*t };

    let iter = Box::new(NumericRangeTreeIterator(IteratorInner::new(tree)));
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
/// - `it` must point to a valid [`NumericRangeTreeIterator`] obtained from
///   [`NumericRangeTreeIterator_New`] and cannot be NULL.
/// - The tree from which this iterator was created must still be valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTreeIterator_Next(
    it: *mut NumericRangeTreeIterator,
) -> *const NumericRangeNode {
    debug_assert!(!it.is_null(), "it cannot be NULL");

    // SAFETY: Caller is to ensure that `it` is a valid, non-null pointer
    // to a NumericRangeTreeIterator obtained from NumericRangeTreeIterator_New.
    let NumericRangeTreeIterator(iter) = unsafe { &mut *it };

    match iter.next() {
        Some(node) => node as *const NumericRangeNode,
        None => std::ptr::null(),
    }
}

/// Free a [`NumericRangeTreeIterator`].
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// - `it` must point to a valid [`NumericRangeTreeIterator`] obtained from
///   [`NumericRangeTreeIterator_New`], or be NULL (in which case this is a no-op).
/// - After calling this function, `it` must not be used again.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NumericRangeTreeIterator_Free(it: *mut NumericRangeTreeIterator) {
    if it.is_null() {
        return;
    }

    // SAFETY: Caller is to ensure that `it` is a valid pointer to a
    // NumericRangeTreeIterator obtained from NumericRangeTreeIterator_New.
    unsafe {
        let _ = Box::from_raw(it);
    };
}

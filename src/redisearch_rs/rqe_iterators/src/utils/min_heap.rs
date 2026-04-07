/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A specialized min-heap for the union iterator.
//!
//! This module provides [`DocIdMinHeap`], a min-heap optimized for the union iterator
//! pattern. It stores `(doc_id, child_index)` pairs ordered by `doc_id` and provides
//! efficient operations that Rust's [`std::collections::BinaryHeap`] lacks:
//!
//! - [`DocIdMinHeap::replace_root`]: O(log n) in-place root replacement with single sift-down
//! - [`DocIdMinHeap::index<usize>`]: Direct access to heap data for manual traversal
use ffi::t_docId;
use std::ops::Index;

/// A specialized min-heap for the union iterator.
///
/// Stores `(doc_id, child_index)` pairs ordered by `doc_id` (minimum at root).
/// Provides efficient operations for the union iterator pattern:
/// - O(log n) in-place root replacement via [`Self::replace_root`]
/// - Direct access to heap data via [`Self::index`] for manual traversal
///
/// # Example
///
/// ```ignore
/// use rqe_iterators::util::DocIdMinHeap;
///
/// let mut heap = DocIdMinHeap::new();
/// heap.push(10, 0);  // doc_id=10, child_index=0
/// heap.push(5, 1);   // doc_id=5, child_index=1
/// heap.push(5, 2);   // doc_id=5, child_index=2
///
/// assert_eq!(heap.peek(), Some((5, 1)));
///
/// // Access heap data directly for traversal
/// let root_doc_id = heap[0].0;
/// ```

#[derive(Debug, Clone, Default)]
pub struct DocIdMinHeap {
    /// Backing storage: Vec of (doc_id, child_index) pairs.
    data: Vec<(t_docId, usize)>,
}

/// Direct access to heap data for manual traversal.
impl Index<usize> for DocIdMinHeap {
    type Output = (t_docId, usize);

    fn index(&self, index: usize) -> &Self::Output {
        &self.data[index]
    }
}

impl DocIdMinHeap {
    /// Creates a new empty heap.
    #[must_use]
    pub const fn new() -> Self {
        Self { data: Vec::new() }
    }

    /// Creates a new heap with the specified capacity.
    #[must_use]
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
        }
    }

    /// Returns the number of entries in the heap.
    #[inline]
    pub const fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns `true` if the heap is empty.
    #[inline]
    pub const fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Removes all entries from the heap.
    #[inline]
    pub fn clear(&mut self) {
        self.data.clear();
    }

    /// Returns the minimum entry without removing it.
    ///
    /// Returns `None` if the heap is empty.
    #[inline]
    pub fn peek(&self) -> Option<(t_docId, usize)> {
        self.data.first().copied()
    }

    /// Returns a reference to the underlying heap data.
    ///
    /// Borrowing the slice once allows the caller to iterate over the heap
    /// structure without per-access bounds checks through the [`Index`] trait,
    /// which is measurably faster for bulk traversals such as the DFS in
    /// [`crate::UnionHeap::build_aggregate_result`].
    ///
    /// The data is stored as `(doc_id, child_index)` tuples in heap order
    /// (smallest doc_id at index 0). Children of index `i` are at `2*i+1` and `2*i+2`.
    #[inline]
    pub fn data(&self) -> &[(t_docId, usize)] {
        &self.data
    }

    /// Pushes an entry onto the heap.
    ///
    /// # Complexity
    ///
    /// O(log n) - bubbles up to restore heap property.
    pub fn push(&mut self, doc_id: t_docId, child_idx: usize) {
        self.data.push((doc_id, child_idx));
        self.sift_up(self.data.len() - 1);
    }

    /// Removes and returns the minimum entry.
    ///
    /// Returns `None` if the heap is empty.
    ///
    /// # Complexity
    ///
    /// O(log n) - sifts down to restore heap property.
    pub fn pop(&mut self) -> Option<(t_docId, usize)> {
        if self.data.is_empty() {
            return None;
        }

        let result = self.data[0];
        let last_idx = self.data.len() - 1;

        if last_idx > 0 {
            self.data.swap(0, last_idx);
            self.data.pop();
            self.sift_down(0);
        } else {
            self.data.pop();
        }

        Some(result)
    }

    /// Replaces the root entry in-place and restores heap property.
    ///
    /// This is more efficient than `pop()` + `push()` as it performs only
    /// a single sift-down operation instead of sift-up + sift-down.
    ///
    /// # Panics
    ///
    /// Panics if the heap is empty.
    ///
    /// # Complexity
    ///
    /// O(log n) - single sift-down operation.
    pub fn replace_root(&mut self, doc_id: t_docId, child_idx: usize) {
        debug_assert!(!self.data.is_empty(), "cannot replace root of empty heap");
        self.data[0] = (doc_id, child_idx);
        self.sift_down(0);
    }

    /// Sifts an element up the tree to restore heap property.
    ///
    /// Used after pushing a new element at the end of the heap.
    fn sift_up(&mut self, mut idx: usize) {
        while idx > 0 {
            let parent = (idx - 1) / 2;
            if self.data[idx].0 >= self.data[parent].0 {
                break;
            }
            self.data.swap(idx, parent);
            idx = parent;
        }
    }

    /// Sifts an element down the tree to restore heap property.
    ///
    /// Uses the "hole" technique: saves the element being sifted, moves children
    /// up one at a time (1 write per level instead of 3 for a swap), and writes
    /// the saved element once at the final position.
    ///
    /// Also uses unchecked indexing in the inner loop since all indices are
    /// validated against `self.data.len()` before access.
    fn sift_down(&mut self, mut idx: usize) {
        let len = self.data.len();
        if len <= 1 {
            return;
        }

        // Save the element we're sifting down
        let element = self.data[idx];

        loop {
            let left = 2 * idx + 1;
            if left >= len {
                break;
            }

            let right = left + 1;

            // Find the smaller child.
            let smallest = if right < len {
                // SAFETY: `right < len` is checked by the enclosing `if`.
                let right_val = unsafe { self.data.get_unchecked(right).0 };
                // SAFETY: `left < len` is checked at the top of the loop (`left < len`),
                // and `left < right < len`.
                let left_val = unsafe { self.data.get_unchecked(left).0 };
                if right_val < left_val { right } else { left }
            } else {
                left
            };

            // SAFETY: `smallest` is either `left` or `right`, both validated < len.
            let smallest_val = unsafe { *self.data.get_unchecked(smallest) };

            if element.0 <= smallest_val.0 {
                break;
            }

            // Move the smaller child up into the hole (1 write instead of 3 for swap).
            // SAFETY: `idx < len` (invariant: starts at a valid index, only moves to
            // `smallest` which was validated < len).
            unsafe {
                *self.data.get_unchecked_mut(idx) = smallest_val;
            }
            idx = smallest;
        }

        // Place the saved element in its final position.
        // SAFETY: `idx` is always a valid index (initialized from parameter, only updated
        // to `smallest` which is validated < len).
        unsafe {
            *self.data.get_unchecked_mut(idx) = element;
        }
    }
}

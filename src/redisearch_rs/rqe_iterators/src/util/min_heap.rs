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
//! - [`DocIdMinHeap::for_each_root`]: O(k) iteration over all entries equal to root
//!
//! See `CUSTOM_HEAP_DESIGN.md` for detailed design rationale.

use ffi::t_docId;

/// A specialized min-heap for the union iterator.
///
/// Stores `(doc_id, child_index)` pairs ordered by `doc_id` (minimum at root).
/// Provides efficient operations for the union iterator pattern:
/// - O(log n) in-place root replacement via [`Self::replace_root`]
/// - O(k) iteration over entries equal to root via [`Self::for_each_root`]
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
/// // Iterate over all entries with minimum doc_id
/// let mut found = Vec::new();
/// heap.for_each_root(|doc_id, idx| found.push((doc_id, idx)));
/// assert_eq!(found.len(), 2);  // Both entries with doc_id=5
/// ```
#[derive(Debug, Clone)]
pub struct DocIdMinHeap {
    /// Backing storage: Vec of (doc_id, child_index) pairs.
    data: Vec<(t_docId, usize)>,
}

impl Default for DocIdMinHeap {
    fn default() -> Self {
        Self::new()
    }
}

impl DocIdMinHeap {
    /// Creates a new empty heap.
    #[must_use]
    pub fn new() -> Self {
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
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns `true` if the heap is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
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

    /// Iterates over all entries with the same doc_id as the root.
    ///
    /// The callback receives `(doc_id, child_index)` for each matching entry.
    /// The heap is **not modified** during iteration.
    ///
    /// # Complexity
    ///
    /// O(k) where k = number of entries with minimum doc_id.
    pub fn for_each_root<F>(&self, mut f: F)
    where
        F: FnMut(t_docId, usize),
    {
        if self.data.is_empty() {
            return;
        }

        let root_doc_id = self.data[0].0;
        self.for_each_root_recursive(0, root_doc_id, &mut f);
    }

    /// Recursive helper for `for_each_root`.
    ///
    /// Traverses the heap tree, visiting all nodes equal to `root_doc_id`.
    /// Due to heap property, if a node has doc_id > root_doc_id, all its
    /// descendants also have doc_id >= that node's doc_id, so we can prune.
    fn for_each_root_recursive<F>(&self, idx: usize, root_doc_id: t_docId, f: &mut F)
    where
        F: FnMut(t_docId, usize),
    {
        if idx >= self.data.len() {
            return;
        }

        let (doc_id, child_idx) = self.data[idx];
        if doc_id != root_doc_id {
            // Heap property: children have >= priority, so no need to recurse
            return;
        }

        f(doc_id, child_idx);

        // Recurse to children
        let left = 2 * idx + 1;
        let right = 2 * idx + 2;
        self.for_each_root_recursive(left, root_doc_id, f);
        self.for_each_root_recursive(right, root_doc_id, f);
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
    /// Used after removing the root or replacing it.
    fn sift_down(&mut self, mut idx: usize) {
        loop {
            let left = 2 * idx + 1;
            let right = 2 * idx + 2;
            let mut smallest = idx;

            if left < self.data.len() && self.data[left].0 < self.data[smallest].0 {
                smallest = left;
            }
            if right < self.data.len() && self.data[right].0 < self.data[smallest].0 {
                smallest = right;
            }

            if smallest == idx {
                break;
            }

            self.data.swap(idx, smallest);
            idx = smallest;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_heap_is_empty() {
        let heap = DocIdMinHeap::new();
        assert!(heap.is_empty());
        assert_eq!(heap.len(), 0);
        assert_eq!(heap.peek(), None);
    }

    #[test]
    fn test_push_and_peek() {
        let mut heap = DocIdMinHeap::new();
        heap.push(10, 0);
        assert_eq!(heap.peek(), Some((10, 0)));
        assert_eq!(heap.len(), 1);
    }

    #[test]
    fn test_push_maintains_min_heap() {
        let mut heap = DocIdMinHeap::new();
        heap.push(10, 0);
        heap.push(5, 1);
        heap.push(15, 2);
        heap.push(3, 3);

        assert_eq!(heap.peek(), Some((3, 3)));
    }

    #[test]
    fn test_pop_returns_minimum() {
        let mut heap = DocIdMinHeap::new();
        heap.push(10, 0);
        heap.push(5, 1);
        heap.push(15, 2);

        assert_eq!(heap.pop(), Some((5, 1)));
        assert_eq!(heap.pop(), Some((10, 0)));
        assert_eq!(heap.pop(), Some((15, 2)));
        assert_eq!(heap.pop(), None);
    }

    #[test]
    fn test_pop_empty_returns_none() {
        let mut heap = DocIdMinHeap::new();
        assert_eq!(heap.pop(), None);
    }

    #[test]
    fn test_replace_root_larger_value() {
        let mut heap = DocIdMinHeap::new();
        heap.push(5, 0);
        heap.push(10, 1);
        heap.push(15, 2);

        // Replace root (5) with larger value (20)
        heap.replace_root(20, 3);

        // New minimum should be 10
        assert_eq!(heap.peek(), Some((10, 1)));
    }

    #[test]
    fn test_replace_root_smaller_value() {
        let mut heap = DocIdMinHeap::new();
        heap.push(10, 0);
        heap.push(20, 1);
        heap.push(30, 2);

        // Replace root (10) with smaller value (5)
        heap.replace_root(5, 3);

        // Root should still be at top (smallest)
        assert_eq!(heap.peek(), Some((5, 3)));
    }

    #[test]
    fn test_for_each_root_single_entry() {
        let mut heap = DocIdMinHeap::new();
        heap.push(10, 0);

        let mut found = Vec::new();
        heap.for_each_root(|doc_id, idx| found.push((doc_id, idx)));

        assert_eq!(found, vec![(10, 0)]);
    }

    #[test]
    fn test_for_each_root_multiple_same_doc_id() {
        let mut heap = DocIdMinHeap::new();
        heap.push(5, 0);
        heap.push(5, 1);
        heap.push(5, 2);
        heap.push(10, 3);

        let mut found = Vec::new();
        heap.for_each_root(|doc_id, idx| found.push((doc_id, idx)));

        // Should find all entries with doc_id=5
        assert_eq!(found.len(), 3);
        for (doc_id, _) in &found {
            assert_eq!(*doc_id, 5);
        }
    }

    #[test]
    fn test_for_each_root_mixed_doc_ids() {
        let mut heap = DocIdMinHeap::new();
        heap.push(10, 0);
        heap.push(5, 1);
        heap.push(15, 2);
        heap.push(5, 3);
        heap.push(20, 4);

        let mut found = Vec::new();
        heap.for_each_root(|doc_id, idx| found.push((doc_id, idx)));

        // Should find only entries with minimum doc_id=5
        assert_eq!(found.len(), 2);
        for (doc_id, _) in &found {
            assert_eq!(*doc_id, 5);
        }
    }

    #[test]
    fn test_for_each_root_empty_heap() {
        let heap = DocIdMinHeap::new();

        let mut found = Vec::new();
        heap.for_each_root(|doc_id, idx| found.push((doc_id, idx)));

        assert!(found.is_empty());
    }

    #[test]
    fn test_clear() {
        let mut heap = DocIdMinHeap::new();
        heap.push(10, 0);
        heap.push(5, 1);
        heap.clear();

        assert!(heap.is_empty());
        assert_eq!(heap.peek(), None);
    }

    #[test]
    fn test_with_capacity() {
        let heap = DocIdMinHeap::with_capacity(100);
        assert!(heap.is_empty());
    }

    #[test]
    fn test_sorted_extraction() {
        // Test that repeated pop() returns elements in sorted order
        let mut heap = DocIdMinHeap::new();
        let values = [50, 30, 70, 10, 90, 20, 80, 40, 60];

        for (idx, &val) in values.iter().enumerate() {
            heap.push(val, idx);
        }

        let mut extracted = Vec::new();
        while let Some((doc_id, _)) = heap.pop() {
            extracted.push(doc_id);
        }

        // Should be sorted in ascending order
        let mut sorted = extracted.clone();
        sorted.sort();
        assert_eq!(extracted, sorted);
    }
}

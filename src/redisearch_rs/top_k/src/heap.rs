/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A fixed-capacity heap that retains the top-k scored documents.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::num::NonZeroUsize;

use rqe_core::DocId;

/// A (doc_id, score) pair stored in the heap.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ScoredResult {
    /// Document identifier.
    pub doc_id: DocId,
    /// Score for the document (lower or higher is "better" depending on the comparator).
    pub score: f64,
}

/// Wraps a [`ScoredResult`] so that [`BinaryHeap`] (a max-heap) keeps the *worst*
/// element at the top, making it cheap to evict.
///
/// "Worst" is defined by the [`TopKHeap`]'s comparator:
///
/// - `compare(a, b) == Less`  → `a` is **better** than `b`.
/// - `compare(a, b) == Greater` → `a` is **worse** than `b` (= heap-max, evicted first).
///
/// On equal scores the tie-breaker uses `tiebreak_ascending` (see [`TopKHeap::new`]).
struct HeapEntry {
    result: ScoredResult,
    /// Cached comparison function so [`Ord`] can be implemented without extra state.
    compare: fn(f64, f64) -> Ordering,
    /// On equal scores, whether a lower (`true`) or higher (`false`) `doc_id` wins the tie.
    tiebreak_ascending: bool,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // We want the worst element at the top of the heap so eviction is O(log k).
        // `compare(self, other) == Greater` means self is worse.
        let score_ord = (self.compare)(self.result.score, other.result.score);
        match score_ord {
            Ordering::Less => Ordering::Less,
            Ordering::Greater => Ordering::Greater,
            Ordering::Equal => {
                let id_ord = self.result.doc_id.cmp(&other.result.doc_id);
                if self.tiebreak_ascending {
                    id_ord
                } else {
                    id_ord.reverse()
                }
            }
        }
    }
}

/// A fixed-capacity heap that retains the k best-scored documents.
///
/// Internally a max-heap whose root is the *worst* of the retained elements,
/// making insertion and eviction O(log k).
///
/// # Comparator convention
///
/// `compare(a, b)` must return:
/// - [`Ordering::Less`]    if score `a` is **better** than score `b`
/// - [`Ordering::Greater`] if score `a` is **worse**  than score `b`
/// - [`Ordering::Equal`]   if the scores are equally good
///
/// For ascending order (lower score = better, e.g. vector distance):
/// `compare = |a, b| a.partial_cmp(&b).unwrap_or(Ordering::Equal)`
///
/// For descending order (higher score = better, e.g. numeric SORTBY):
/// `compare = |a, b| b.partial_cmp(&a).unwrap_or(Ordering::Equal)`
pub struct TopKHeap {
    inner: BinaryHeap<HeapEntry>,
    capacity: usize,
    compare: fn(f64, f64) -> Ordering,
    tiebreak_ascending: bool,
}

impl TopKHeap {
    /// Creates a new heap that holds at most `capacity` elements.
    ///
    /// `compare` determines score order (see type-level docs).
    ///
    /// `tiebreak_ascending` breaks ties when two results share an equal score:
    /// - `true`  — higher `doc_id` is evicted first.
    /// - `false` — lower  `doc_id` is evicted first.
    pub fn new(
        capacity: NonZeroUsize,
        compare: fn(f64, f64) -> Ordering,
        tiebreak_ascending: bool,
    ) -> Self {
        let capacity = capacity.into();
        Self {
            inner: BinaryHeap::with_capacity(capacity),
            capacity,
            compare,
            tiebreak_ascending,
        }
    }

    /// Returns the number of elements currently in the heap.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Returns `true` if the heap contains no elements.
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Returns `true` if the heap has reached its capacity.
    pub fn is_full(&self) -> bool {
        self.inner.len() >= self.capacity
    }

    /// Returns the worst element currently retained (the one that would be evicted next),
    /// without removing it.
    pub fn peek_worst(&self) -> Option<ScoredResult> {
        self.inner.peek().map(|e| e.result)
    }

    /// Attempts to insert `(doc_id, score)` into the heap.
    ///
    /// - If the heap is not full, the element is always inserted.
    /// - If the heap is full and the new element is **better** than the current worst,
    ///   the worst is evicted and the new element takes its place.
    /// - Otherwise the element is discarded.
    ///
    /// Returns `true` if the element was inserted.
    pub fn push(&mut self, doc_id: DocId, score: f64) -> bool {
        let entry = HeapEntry {
            result: ScoredResult { doc_id, score },
            compare: self.compare,
            tiebreak_ascending: self.tiebreak_ascending,
        };

        if !self.is_full() {
            self.inner.push(entry);
            true
        }
        // The heap is full. Only insert if the new element is strictly better than the
        // current worst (root). `entry > worst` means entry is worse → discard.
        // `entry < worst` means entry is better → evict worst, insert entry.
        // Equal (same score AND same doc_id) → discard to avoid duplicates.
        else if let Some(mut worst) = self.inner.peek_mut()
            && entry < *worst
        {
            *worst = entry;
            true
        } else {
            false
        }
    }

    /// Removes and returns the worst element currently retained.
    pub fn pop_worst(&mut self) -> Option<ScoredResult> {
        self.inner.pop().map(|e| e.result)
    }

    /// Drains all elements and returns them sorted best-first.
    ///
    /// Consumes the heap.
    pub fn drain_sorted(self) -> Vec<ScoredResult> {
        // BinaryHeap::into_sorted_vec() returns elements in ascending Ord order.
        // In our Ord impl "better" == "Less", so ascending == best-first already.
        self.inner
            .into_sorted_vec()
            .into_iter()
            .map(|e| e.result)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn non_zero_capacity(capacity: usize) -> NonZeroUsize {
        NonZeroUsize::new(capacity).unwrap()
    }

    /// Ascending comparator: lower score is better (e.g. vector distance).
    fn asc(a: f64, b: f64) -> Ordering {
        a.partial_cmp(&b).unwrap_or(Ordering::Equal)
    }

    /// Descending comparator: higher score is better (e.g. numeric SORTBY).
    fn desc(a: f64, b: f64) -> Ordering {
        b.partial_cmp(&a).unwrap_or(Ordering::Equal)
    }

    #[test]
    fn heap_fewer_than_k_preserves_all() {
        let mut heap = TopKHeap::new(non_zero_capacity(5), asc, true);
        heap.push(1, 3.0);
        heap.push(2, 1.0);
        heap.push(3, 2.0);

        let results = heap.drain_sorted();

        assert_eq!(results.len(), 3);
        assert_eq!(results[0].score, 1.0);
        assert_eq!(results[1].score, 2.0);
        assert_eq!(results[2].score, 3.0);
    }

    #[test]
    fn heap_evicts_worst_when_full_asc() {
        let mut heap = TopKHeap::new(non_zero_capacity(3), asc, true);
        heap.push(1, 5.0);
        heap.push(2, 3.0);
        heap.push(3, 4.0);

        // Better score evicts the current worst (5.0)
        let inserted = heap.push(4, 2.0);
        assert!(inserted);
        assert_eq!(heap.len(), 3);

        // Worse score is rejected without changing the heap
        let not_inserted = heap.push(5, 6.0);
        assert!(!not_inserted);

        let results = heap.drain_sorted();
        let scores: Vec<f64> = results.iter().map(|r| r.score).collect();
        assert_eq!(scores, vec![2.0, 3.0, 4.0]);
    }

    #[test]
    fn heap_evicts_worst_when_full_desc() {
        let mut heap = TopKHeap::new(non_zero_capacity(3), desc, true);
        heap.push(1, 1.0);
        heap.push(2, 3.0);
        heap.push(3, 2.0);

        // Better score (higher in DESC) evicts the current worst (1.0)
        let inserted = heap.push(4, 4.0);
        assert!(inserted);

        // Worse score (lower in DESC) is rejected
        let not_inserted = heap.push(5, 0.5);
        assert!(!not_inserted);

        let results = heap.drain_sorted();
        let scores: Vec<f64> = results.iter().map(|r| r.score).collect();
        assert_eq!(scores, vec![4.0, 3.0, 2.0]);
    }

    #[test]
    fn heap_capacity_one_keeps_best_asc() {
        let mut heap = TopKHeap::new(non_zero_capacity(1), asc, true);
        heap.push(1, 5.0);
        heap.push(2, 3.0);
        heap.push(3, 4.0);

        let results = heap.drain_sorted();

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].score, 3.0);
        assert_eq!(results[0].doc_id, 2);
    }

    #[test]
    fn heap_tie_breaking_keeps_lower_doc_id() {
        let mut heap = TopKHeap::new(non_zero_capacity(2), asc, true);
        heap.push(10, 1.0);
        heap.push(5, 1.0);
        heap.push(3, 1.0); // third tied entry evicts doc_id 10 (highest loses the tie)

        let results = heap.drain_sorted();
        let ids: Vec<DocId> = results.iter().map(|r| r.doc_id).collect();

        assert!(ids.contains(&3));
        assert!(ids.contains(&5));
        assert!(!ids.contains(&10));
    }

    #[test]
    fn heap_exact_duplicate_not_inserted_when_full() {
        let mut heap = TopKHeap::new(non_zero_capacity(2), asc, true);
        heap.push(1, 1.0);
        heap.push(2, 2.0);

        let inserted = heap.push(2, 2.0);

        assert!(!inserted);
        assert_eq!(heap.len(), 2);
        let results = heap.drain_sorted();
        assert_eq!(results.len(), 2);
        assert_eq!(results.iter().filter(|r| r.doc_id == 2).count(), 1);
    }

    #[test]
    fn heap_tie_breaking_keeps_lower_doc_id_desc() {
        let mut heap = TopKHeap::new(non_zero_capacity(2), desc, true);
        heap.push(10, 1.0);
        heap.push(5, 1.0);
        heap.push(3, 1.0); // third tied entry evicts doc_id 10 (highest loses the tie, regardless of sort direction)

        let results = heap.drain_sorted();
        let ids: Vec<DocId> = results.iter().map(|r| r.doc_id).collect();

        assert!(ids.contains(&3));
        assert!(ids.contains(&5));
        assert!(!ids.contains(&10));
    }

    #[test]
    fn heap_tiebreak_desc_evicts_lower_doc_id() {
        // Mirrors cmpByFields behaviour when the last SORTBY key is DESC:
        // lower doc_id is considered worse and evicted first.
        let mut heap = TopKHeap::new(non_zero_capacity(2), asc, false);
        heap.push(3, 1.0);
        heap.push(5, 1.0);
        heap.push(10, 1.0); // third tied entry evicts doc_id 3 (lowest loses the tie)

        let results = heap.drain_sorted();
        let ids: Vec<DocId> = results.iter().map(|r| r.doc_id).collect();

        assert!(ids.contains(&5));
        assert!(ids.contains(&10));
        assert!(!ids.contains(&3));
    }

    #[test]
    fn heap_tiebreak_asc_and_desc_differ_on_same_input() {
        // Confirms the two directions produce opposite retained sets.
        let push_all = |heap: &mut TopKHeap| {
            heap.push(3, 1.0);
            heap.push(5, 1.0);
            heap.push(10, 1.0);
        };

        let mut asc_heap = TopKHeap::new(non_zero_capacity(2), asc, true);
        push_all(&mut asc_heap);
        let asc_ids: Vec<DocId> = asc_heap.drain_sorted().iter().map(|r| r.doc_id).collect();

        let mut desc_heap = TopKHeap::new(non_zero_capacity(2), asc, false);
        push_all(&mut desc_heap);
        let desc_ids: Vec<DocId> = desc_heap.drain_sorted().iter().map(|r| r.doc_id).collect();

        // ASC tiebreak keeps lowest ids; DESC tiebreak keeps highest ids.
        assert_eq!(asc_ids, vec![3, 5]);
        // DESC best-first: id=10 is "better" (Less in Ord) because tiebreak is reversed.
        assert_eq!(desc_ids, vec![10, 5]);
    }

    #[test]
    fn heap_peek_worst_returns_eviction_candidate() {
        let mut heap = TopKHeap::new(non_zero_capacity(3), asc, true);
        heap.push(1, 2.0);
        heap.push(2, 5.0);
        heap.push(3, 3.0);

        assert_eq!(heap.peek_worst().unwrap().score, 5.0);
    }

    #[test]
    fn drain_sorted_best_first_asc() {
        let mut heap = TopKHeap::new(non_zero_capacity(4), asc, true);
        for (id, score) in [(1, 4.0), (2, 1.0), (3, 3.0), (4, 2.0)] {
            heap.push(id, score);
        }

        let results = heap.drain_sorted();

        let scores: Vec<f64> = results.iter().map(|r| r.score).collect();
        assert_eq!(scores, vec![1.0, 2.0, 3.0, 4.0]);
    }

    #[test]
    fn drain_sorted_best_first_desc() {
        let mut heap = TopKHeap::new(non_zero_capacity(4), desc, true);
        for (id, score) in [(1, 4.0), (2, 1.0), (3, 3.0), (4, 2.0)] {
            heap.push(id, score);
        }

        let results = heap.drain_sorted();

        let scores: Vec<f64> = results.iter().map(|r| r.score).collect();
        assert_eq!(scores, vec![4.0, 3.0, 2.0, 1.0]);
    }

    #[test]
    fn pop_worst_removes_eviction_candidate_asc() {
        let mut heap = TopKHeap::new(non_zero_capacity(3), asc, true);
        heap.push(1, 2.0);
        heap.push(2, 5.0);
        heap.push(3, 3.0);

        let worst = heap.pop_worst().unwrap();

        assert_eq!(worst.score, 5.0);
        assert_eq!(worst.doc_id, 2);
        assert_eq!(heap.len(), 2);
    }

    #[test]
    fn pop_worst_removes_eviction_candidate_desc() {
        let mut heap = TopKHeap::new(non_zero_capacity(3), desc, true);
        heap.push(1, 4.0);
        heap.push(2, 1.0);
        heap.push(3, 2.0);

        let worst = heap.pop_worst().unwrap();

        assert_eq!(worst.score, 1.0);
        assert_eq!(worst.doc_id, 2);
        assert_eq!(heap.len(), 2);
    }

    #[test]
    fn pop_worst_on_empty_returns_none() {
        let mut heap = TopKHeap::new(non_zero_capacity(3), asc, true);
        assert!(heap.pop_worst().is_none());
    }

    #[test]
    fn peek_worst_on_empty_returns_none() {
        let heap = TopKHeap::new(non_zero_capacity(3), asc, true);
        assert!(heap.peek_worst().is_none());
    }

    #[test]
    fn pop_worst_allows_reinsertion() {
        let mut heap = TopKHeap::new(non_zero_capacity(2), asc, true);
        heap.push(1, 3.0);
        heap.push(2, 1.0);
        heap.pop_worst();

        let inserted = heap.push(3, 2.5);

        assert!(inserted);
        assert_eq!(heap.len(), 2);
    }

    #[test]
    fn heap_is_empty_and_is_full() {
        let mut heap = TopKHeap::new(non_zero_capacity(2), asc, true);
        assert!(heap.is_empty());
        assert!(!heap.is_full());

        heap.push(1, 1.0);
        assert!(!heap.is_empty());
        assert!(!heap.is_full());

        heap.push(2, 2.0);
        assert!(!heap.is_empty());
        assert!(heap.is_full());
    }
}

/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Value-ordered iteration over a numeric range tree.
//!
//! [`NumericRangeIterator`] turns a [`NumericRangeTree`] into a stream of
//! score brackets: it asks the tree for the ranges matching a filter window —
//! already ordered best-score-first per the filter's `ascending` flag — and
//! hands them out in fixed-size chunks via [`next_n`](NumericRangeIterator::next_n).
//! Each chunk is opened as a doc-id-ordered [`NumericScoreBatch`], so a
//! batch is *value-bucketed across* the stream yet *doc-id-sorted within*.

use std::{collections::HashSet, marker::PhantomData, ptr::NonNull};

use inverted_index::NumericFilter;
use numeric_range_tree::{NumericRange, NumericRangeTree, RangeWindow};
use rqe_core::DocId;

use crate::score_batch::NumericScoreBatch;

/// A tree borrowed for a query, including while its spec lock is released.
///
/// Immutable Rust trees convert through [`From`]; mutable C-owned trees enter
/// through [`from_raw`](Self::from_raw), which specifies the lifetime and revalidation
/// contract used by the numeric source's safe constructors.
pub struct NumericTreeRef<'index> {
    ptr: NonNull<NumericRangeTree>,
    _lifetime: PhantomData<&'index NumericRangeTree>,
}

impl<'index> NumericTreeRef<'index> {
    /// Borrow a tree that may change while the query is suspended.
    ///
    /// # Safety
    ///
    /// 1. `tree` points to an initialized [`NumericRangeTree`] that remains
    ///    [valid] at a stable address for `'index`.
    /// 2. If the tree may be mutated during `'index`, `tree` must retain the
    ///    owner's raw-pointer provenance, not come from a reference to the tree
    ///    that a later mutable borrow would invalidate.
    /// 3. Mutation or mutable borrowing is allowed only while all consuming
    ///    iterators are idle across a lock release. Revalidate each iterator
    ///    before using it again, and discard it if revalidation aborts.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub unsafe fn from_raw(tree: NonNull<NumericRangeTree>) -> Self {
        Self {
            ptr: tree,
            _lifetime: PhantomData,
        }
    }

    fn get(&self) -> &'index NumericRangeTree {
        // SAFETY: the shared borrow or `from_raw` (1, 2) keeps this pointer valid;
        // (3) excludes mutation during iterator operations.
        unsafe { self.ptr.as_ref() }
    }
}

impl<'index> From<&'index NumericRangeTree> for NumericTreeRef<'index> {
    fn from(tree: &'index NumericRangeTree) -> Self {
        Self {
            ptr: NonNull::from(tree),
            _lifetime: PhantomData,
        }
    }
}

/// Streams a numeric tree's ranges in value order, restricted to a
/// [`RangeWindow`] and chunked by [`next_n`](Self::next_n).
pub struct NumericRangeIterator<'index> {
    tree: NumericTreeRef<'index>,
    /// Ranges matching `window`, best-score-first, or `None` until they are
    /// next resolved from the tree.
    ///
    /// Held as pointers, like the tree itself, because the cache can outlive a
    /// spec lock release until revalidation discards it; see
    /// [`resolve`](Self::resolve).
    ranges: Option<Vec<NonNull<NumericRange>>>,
    window: RangeWindow,
    /// Index of the next range to hand out.
    pos: usize,
    /// Value predicate applied to each record as a batch is read.
    filter: NumericFilter,
    /// Doc ids currently holding a top-k heap slot, or `None` when no document
    /// in the tree carries more than one value.
    ///
    /// A multivalue field indexes one entry per value, so a doc's values can
    /// fall in different range chunks and be split across `next_n` batches — or
    /// even across expanded windows. Ranges are strictly value-ordered and
    /// disjoint, so a doc's later occurrences score worse than its first, and
    /// one the heap rejected or evicted cannot win a slot back: only heap
    /// members need tracking. A single-valued field occupies one range per doc
    /// and needs no such tracking, so it skips the per-record set lookup
    /// entirely.
    retained: Option<HashSet<DocId>>,
    /// Construction-time snapshot of [`NumericRangeTree::revision_id`], never
    /// refreshed: a changed tree revision invalidates this cursor.
    revision_id: u32,
}

impl<'index> NumericRangeIterator<'index> {
    /// Resolve `filter` against `tree` over `window` and prepare to stream the
    /// matching ranges best-score-first.
    pub fn new(
        tree: impl Into<NumericTreeRef<'index>>,
        filter: &NumericFilter,
        window: RangeWindow,
    ) -> Self {
        let tree = tree.into();
        let revision_id = tree.get().revision_id();
        Self {
            tree,
            ranges: None,
            window,
            pos: 0,
            filter: *filter,
            retained: None,
            revision_id,
        }
    }

    fn ranges(&mut self) -> &[NonNull<NumericRange>] {
        self.ranges.get_or_insert_with(|| {
            let tree = self.tree.get();
            if self.retained.is_none() && tree.has_multivalued_docs() {
                self.retained = Some(HashSet::new());
            }
            tree.find_windowed(&self.filter, self.window)
                .into_iter()
                .map(NonNull::from)
                .collect()
        })
    }

    /// Borrow a cached range.
    fn resolve(range: NonNull<NumericRange>) -> &'index NumericRange {
        // SAFETY: `range` was resolved from the tree, which
        // `NumericTreeRef::from_raw` (3) keeps unchanged while the iterator is in
        // use. A cache that outlived a lock release is discarded by
        // `release_index_handles` before the iterator is used again.
        unsafe { range.as_ref() }
    }

    /// Discard cached ranges without accessing their contents.
    ///
    /// Suspension calls this before unlocking, so no range is read again until
    /// resumption re-derives them.
    pub fn release_index_handles(&mut self) {
        self.ranges = None;
    }

    /// Whether the tree still has the structure this cursor was built against.
    /// Call only with the spec lock held.
    pub fn can_collect_after_lock_release(&self) -> bool {
        self.tree.get().revision_id() == self.revision_id
    }

    /// Re-resolve onto `window`, usually the next one, and restart from its
    /// first matching range.
    ///
    /// The retained-doc set is kept, as the heap carries over.
    pub fn refind(&mut self, window: RangeWindow) {
        self.ranges = None;
        self.window = window;
        self.pos = 0;
    }

    pub(crate) fn window(&self) -> RangeWindow {
        self.window
    }

    pub(crate) fn ascending(&self) -> bool {
        self.filter.ascending
    }

    /// Forget every tracked heap member.
    pub fn forget_retained(&mut self) {
        if let Some(retained) = &mut self.retained {
            retained.clear();
        }
    }

    /// Sum of `num_docs` across every range in the current window.
    ///
    /// Used as the per-window document estimate, both for the source's
    /// `num_estimated` and to advance the window's `offset` past a consumed
    /// window on retry.
    pub fn total_docs_estimate(&mut self) -> usize {
        self.ranges()
            .iter()
            .map(|&range| Self::resolve(range).num_docs() as usize)
            .sum()
    }

    /// Whether every range of the current window has been handed out.
    pub fn is_exhausted(&mut self) -> bool {
        self.pos >= self.ranges().len()
    }

    /// Open the next `n` (at least `1`) value-ordered ranges as one batch, or
    /// `None` once the window is exhausted.
    pub fn next_n(&mut self, n: usize) -> Option<NumericScoreBatch<'index>> {
        let len = self.ranges().len();
        if self.pos >= len {
            return None;
        }
        let end = (self.pos + n.max(1)).min(len);
        let ranges = self.ranges.as_deref().unwrap_or_default()[self.pos..end]
            .iter()
            .map(|&range| Self::resolve(range));
        let batch = NumericScoreBatch::new(ranges, self.filter, self.retained.is_some());
        self.pos = end;
        Some(batch)
    }

    /// Whether `doc_id` already holds a heap slot.
    #[inline(always)]
    pub fn is_retained(&self, doc_id: DocId) -> bool {
        self.retained
            .as_ref()
            .is_some_and(|retained| retained.contains(&doc_id))
    }

    /// Track that `doc_id` took a heap slot from `evicted`.
    pub fn record_admission(&mut self, doc_id: DocId, evicted: Option<DocId>) {
        if let Some(retained) = &mut self.retained {
            if let Some(evicted) = evicted {
                retained.remove(&evicted);
            }
            retained.insert(doc_id);
        }
    }
}

#[cfg(test)]
mod tests {
    use inverted_index::NumericFilter;
    use numeric_range_tree::{NumericRangeTree, RangeWindow};
    use rqe_core::DocId;
    use top_k::ScoreBatch;

    use super::NumericRangeIterator;

    /// Drain every range into the first-emitted `(doc_id, score)` pairs, taking
    /// `per_batch` ranges at a time so tests can force multi-batch behavior.
    fn drain(
        tree: &NumericRangeTree,
        filter: &NumericFilter,
        per_batch: usize,
    ) -> Vec<(DocId, f64)> {
        let mut it = NumericRangeIterator::new(tree, filter, RangeWindow::UNBOUNDED);
        let mut pairs = Vec::new();
        while let Some(mut batch) = it.next_n(per_batch) {
            while let Some((doc_id, score)) = batch.next().unwrap() {
                if !it.is_retained(doc_id) {
                    it.record_admission(doc_id, None);
                    pairs.push((doc_id, score));
                }
            }
        }
        pairs
    }

    /// The scores `doc` is emitted with.
    fn scores_of(pairs: &[(DocId, f64)], doc: DocId) -> Vec<f64> {
        pairs
            .iter()
            .filter(|(id, _)| *id == doc)
            .map(|(_, score)| *score)
            .collect()
    }

    #[test]
    fn next_n_keeps_only_records_inside_the_value_window() {
        let mut tree = NumericRangeTree::new(false);
        for id in 0..30u64 {
            tree.add(id, id as f64, false, false, 0);
        }
        for inclusive in [true, false] {
            let filter = NumericFilter {
                min: 10.0,
                max: 20.0,
                min_inclusive: inclusive,
                max_inclusive: inclusive,
                ..NumericFilter::default()
            };
            let mut scores: Vec<f64> = drain(&tree, &filter, 8)
                .into_iter()
                .map(|(_, s)| s)
                .collect();
            scores.sort_by(f64::total_cmp);
            let expected: Vec<f64> = if inclusive {
                (10..=20).map(f64::from).collect()
            } else {
                (11..20).map(f64::from).collect()
            };
            assert_eq!(scores, expected, "inclusive: {inclusive}");
        }
    }

    #[test]
    fn multivalue_doc_is_coalesced_to_its_best_value() {
        // `is_multivalued` lets a doc id repeat with several values, as a
        // multivalue field does.
        let mut tree = NumericRangeTree::new(false);
        tree.add(1, 90.0, false, true, 0);
        tree.add(1, 5.0, false, true, 0);
        tree.add(2, 40.0, false, false, 0);

        for (ascending, expected) in [
            (true, [(1, 5.0), (2, 40.0)]),
            (false, [(1, 90.0), (2, 40.0)]),
        ] {
            let filter = NumericFilter {
                ascending,
                ..NumericFilter::default()
            };
            assert_eq!(drain(&tree, &filter, 8), expected, "ascending: {ascending}");
        }
    }

    #[test]
    fn multivalue_doc_spanning_batches_is_scored_once_on_its_best_value() {
        let doc = 1000;
        let mut tree = NumericRangeTree::new(false);
        // Enough distinct values to force a split into two value-disjoint leaves.
        for value in 1..=20u64 {
            tree.add(value, value as f64, false, false, 0);
        }
        assert!(
            tree.find(&NumericFilter::default()).len() >= 2,
            "expected a split"
        );
        tree.add(doc, 1.0, false, true, 0);
        tree.add(doc, 20.0, false, true, 0);

        // One range per batch: the doc's two values land in different batches,
        // which per-batch coalescing alone cannot reconcile.
        for (ascending, best) in [(true, 1.0), (false, 20.0)] {
            let filter = NumericFilter {
                ascending,
                ..NumericFilter::default()
            };
            assert_eq!(
                scores_of(&drain(&tree, &filter, 1), doc),
                [best],
                "ascending: {ascending}"
            );
        }
    }

    #[test]
    fn multivalue_doc_across_several_ranges_of_one_batch_is_scored_once() {
        let doc = 1000;
        let mut tree = NumericRangeTree::new(false);
        for value in 1..=80u64 {
            tree.add(value, value as f64, false, false, 0);
        }
        for value in [1.5, 20.5, 79.5] {
            tree.add(doc, value, false, true, 0);
        }
        assert!(
            tree.find(&NumericFilter::default()).len() >= 3,
            "expected several ranges"
        );

        for (ascending, best) in [(true, 1.5), (false, 79.5)] {
            let filter = NumericFilter {
                ascending,
                ..NumericFilter::default()
            };
            assert_eq!(
                scores_of(&drain(&tree, &filter, 8), doc),
                [best],
                "ascending: {ascending}"
            );
        }
    }

    #[test]
    fn one_batch_merges_ranges_with_interleaved_doc_ids() {
        // Odd ids take low values and even ids high ones, so the value split
        // leaves every range's doc ids interleaved with another range's.
        let docs = 40u64;
        let value_of = |id: u64| {
            if id % 2 == 1 {
                id as f64
            } else {
                100.0 + id as f64
            }
        };
        let mut tree = NumericRangeTree::new(false);
        for id in 1..=docs {
            tree.add(id, value_of(id), false, false, 0);
        }
        let filter = NumericFilter::default();
        let ranges = tree.find(&filter).len();
        assert!(ranges >= 2, "expected a split");

        let single_batch = || {
            let mut it = NumericRangeIterator::new(&tree, &filter, RangeWindow::UNBOUNDED);
            let batch = it.next_n(ranges).unwrap();
            assert!(it.is_exhausted(), "every range must land in the one batch");
            batch
        };

        let mut batch = single_batch();
        let mut pairs = Vec::new();
        while let Some(pair) = batch.next().unwrap() {
            pairs.push(pair);
        }
        let expected: Vec<(DocId, f64)> = (1..=docs).map(|id| (id, value_of(id))).collect();
        assert_eq!(pairs, expected);

        // `read` seeks every range, across run boundaries.
        let mut batch = single_batch();
        let target = docs / 2;
        assert_eq!(
            batch.skip_to(target).unwrap(),
            Some((target, value_of(target)))
        );
        assert_eq!(
            batch.next().unwrap(),
            Some((target + 1, value_of(target + 1)))
        );
        assert_eq!(batch.skip_to(docs + 1).unwrap(), None);
    }
}

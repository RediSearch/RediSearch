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
//! Each chunk is materialized into a doc-id-ordered [`NumericScoreBatch`], so a
//! batch is *value-bucketed across* the stream yet *doc-id-sorted within*.

use std::collections::HashSet;

use inverted_index::NumericFilter;
use numeric_range_tree::{NumericRange, NumericRangeTree, RangeWindow};
use rqe_core::DocId;

use crate::score_batch::NumericScoreBatch;

/// Streams a numeric tree's ranges in value order, restricted to a
/// [`RangeWindow`] and chunked by [`next_n`](Self::next_n).
pub struct NumericRangeIterator<'index> {
    tree: &'index NumericRangeTree,
    /// Ranges matching the current filter window, best-score-first.
    ranges: Vec<&'index NumericRange>,
    /// Index of the next range to hand out.
    pos: usize,
    /// Value predicate applied to each record as a batch is materialized.
    filter: NumericFilter,
    /// Doc ids already handed out by an earlier batch or window, or `None` when
    /// no document in the tree carries more than one value.
    ///
    /// A multivalue field indexes one entry per value, so a doc's values can
    /// fall in different range chunks and be split across `next_n` batches — or
    /// even across expanded windows. Ranges are strictly value-ordered and
    /// disjoint, so a doc's first emission is from its best-scored range; later
    /// occurrences are dropped to keep it scored exactly once. A single-valued
    /// field occupies one range per doc and needs no such tracking, so it skips
    /// the per-record set lookup entirely.
    emitted: Option<HashSet<DocId>>,
}

impl<'index> NumericRangeIterator<'index> {
    /// Resolve `filter` against `tree` over `window` and prepare to stream the
    /// matching ranges best-score-first.
    pub fn new(
        tree: &'index NumericRangeTree,
        filter: &NumericFilter,
        window: RangeWindow,
    ) -> Self {
        Self {
            tree,
            ranges: tree.find_windowed(filter, window),
            pos: 0,
            filter: *filter,
            emitted: tree.has_multivalued_docs().then(HashSet::new),
        }
    }

    /// Re-resolve onto `window`, usually the next one, and restart from its
    /// first matching range.
    ///
    /// The emitted-doc set is kept: expansion moves to a strictly worse,
    /// disjoint value window, so a doc already scored on a better value must
    /// stay suppressed. Use [`forget_emitted`](Self::forget_emitted) to reset it
    /// when restarting the whole query.
    pub fn refind(&mut self, filter: &NumericFilter, window: RangeWindow) {
        self.ranges = self.tree.find_windowed(filter, window);
        self.pos = 0;
        self.filter = *filter;
    }

    /// Drop the record of already-emitted doc ids, so a full rewind can score
    /// every doc afresh.
    pub fn forget_emitted(&mut self) {
        if let Some(emitted) = &mut self.emitted {
            emitted.clear();
        }
    }

    /// Sum of `num_docs` across every range in the current window.
    ///
    /// Used as the per-window document estimate, both for the source's
    /// `num_estimated` and to advance the window's `offset` past a consumed
    /// window on retry.
    pub fn total_docs_estimate(&self) -> usize {
        self.ranges.iter().map(|r| r.num_docs() as usize).sum()
    }

    /// Whether every range of the current window has been handed out.
    pub fn is_exhausted(&self) -> bool {
        self.pos >= self.ranges.len()
    }

    /// Open the next `n` (at least `1`) value-ordered ranges as one batch, or
    /// `None` once the window is exhausted.
    pub fn next_n(&mut self, n: usize) -> Option<NumericScoreBatch<'index>> {
        if self.pos >= self.ranges.len() {
            return None;
        }
        let end = (self.pos + n.max(1)).min(self.ranges.len());
        let ranges = &self.ranges[self.pos..end];
        if let Some(emitted) = &mut self.emitted {
            emitted.reserve(ranges.iter().map(|r| r.num_docs() as usize).sum());
        }
        let batch = NumericScoreBatch::new(ranges, self.filter, self.emitted.is_some());
        self.pos = end;
        Some(batch)
    }

    /// Record `doc_id` as handed out, returning `false` if an earlier batch or
    /// window already did, on a better value.
    #[inline(always)]
    pub fn first_emission(&mut self, doc_id: DocId) -> bool {
        self.emitted
            .as_mut()
            .is_none_or(|emitted| insert(emitted, doc_id))
    }
}

#[inline(never)]
fn insert(emitted: &mut HashSet<DocId>, doc_id: DocId) -> bool {
    emitted.insert(doc_id)
}

#[cfg(test)]
mod tests {
    use inverted_index::NumericFilter;
    use numeric_range_tree::{NumericRangeTree, RangeWindow};
    use rqe_core::DocId;

    use super::NumericRangeIterator;

    /// Drain every window into the list of scores it yields.
    fn drain_scores(tree: &NumericRangeTree, filter: &NumericFilter) -> Vec<f64> {
        drain_pairs(tree, filter)
            .into_iter()
            .map(|(_id, score)| score)
            .collect()
    }

    /// Drain every window into the `(doc_id, score)` pairs it yields, taking
    /// `per_batch` ranges at a time so tests can force multi-batch behavior.
    fn drain_pairs_in_chunks(
        tree: &NumericRangeTree,
        filter: &NumericFilter,
        per_batch: usize,
    ) -> Vec<(DocId, f64)> {
        let mut it = NumericRangeIterator::new(tree, filter, RangeWindow::UNBOUNDED);
        let mut pairs = Vec::new();
        while let Some(mut batch) = it.next_n(per_batch) {
            while let Some((doc_id, score)) = batch.read(0).unwrap() {
                if it.first_emission(doc_id) {
                    pairs.push((doc_id, score));
                }
            }
        }
        pairs
    }

    /// Drain every window into the `(doc_id, score)` pairs it yields.
    fn drain_pairs(tree: &NumericRangeTree, filter: &NumericFilter) -> Vec<(DocId, f64)> {
        drain_pairs_in_chunks(tree, filter, 8)
    }

    /// Build a two-leaf tree, then index `doc_id` as a multivalue doc with one
    /// value in each leaf so its occurrences span a range (and hence batch)
    /// boundary.
    fn tree_with_multivalue_doc_spanning_two_ranges(doc_id: DocId) -> NumericRangeTree {
        let mut tree = NumericRangeTree::new(false);
        // Enough distinct values to force a split into two value-disjoint leaves.
        for value in 1..=20u64 {
            tree.add(value, value as f64, false, false, 0);
        }
        assert!(
            tree.find(&NumericFilter::default()).len() >= 2,
            "expected a split"
        );
        tree.add(doc_id, 1.0, false, true, 0);
        tree.add(doc_id, 20.0, false, true, 0);
        tree
    }

    #[test]
    fn next_n_drops_records_outside_the_value_window() {
        let mut tree = NumericRangeTree::new(false);
        for id in 0..30u64 {
            tree.add(id, id as f64, false, false, 0);
        }
        let filter = NumericFilter {
            min: 10.0,
            max: 20.0,
            ..NumericFilter::default()
        };

        let scores = drain_scores(&tree, &filter);

        assert!(
            scores.iter().all(|&s| (10.0..=20.0).contains(&s)),
            "leaked out-of-range values: {scores:?}"
        );
        assert!(scores.contains(&10.0) && scores.contains(&20.0));
    }

    #[test]
    fn next_n_honors_exclusive_endpoints() {
        let mut tree = NumericRangeTree::new(false);
        for id in 0..30u64 {
            tree.add(id, id as f64, false, false, 0);
        }
        let filter = NumericFilter {
            min: 10.0,
            max: 20.0,
            min_inclusive: false,
            max_inclusive: false,
            ..NumericFilter::default()
        };

        let scores = drain_scores(&tree, &filter);

        assert!(scores.iter().all(|&s| s > 10.0 && s < 20.0));
        assert!(!scores.contains(&10.0) && !scores.contains(&20.0));
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
        while let Some(pair) = batch.read(0).unwrap() {
            pairs.push(pair);
        }
        let expected: Vec<(DocId, f64)> = (1..=docs).map(|id| (id, value_of(id))).collect();
        assert_eq!(pairs, expected);

        // `read` seeks every range, across run boundaries.
        let mut batch = single_batch();
        let target = docs / 2;
        assert_eq!(
            batch.read(target).unwrap(),
            Some((target, value_of(target)))
        );
        assert_eq!(
            batch.read(0).unwrap(),
            Some((target + 1, value_of(target + 1)))
        );
        assert_eq!(batch.read(docs + 1).unwrap(), None);
    }

    #[test]
    fn multivalue_doc_is_coalesced_to_its_first_value_ascending() {
        // `is_multivalued` lets a doc id repeat with several values, as a
        // multivalue field does.
        let mut tree = NumericRangeTree::new(false);
        tree.add(1, 90.0, false, true, 0);
        tree.add(1, 5.0, false, true, 0);
        tree.add(2, 40.0, false, false, 0);

        let pairs = drain_pairs(&tree, &NumericFilter::default());

        let ids: Vec<DocId> = pairs.iter().map(|(id, _)| *id).collect();
        assert_eq!(ids, [1, 2], "each doc id appears exactly once");
        assert_eq!(pairs[0].1, 90.0, "ascending keeps the doc's first value");
    }

    #[test]
    fn multivalue_doc_is_coalesced_to_its_first_value_descending() {
        let mut tree = NumericRangeTree::new(false);
        tree.add(1, 5.0, false, true, 0);
        tree.add(1, 90.0, false, true, 0);

        let filter = NumericFilter {
            ascending: false,
            ..NumericFilter::default()
        };
        let pairs = drain_pairs(&tree, &filter);

        assert_eq!(pairs, [(1, 5.0)], "descending keeps the doc's first value");
    }

    #[test]
    fn multivalue_doc_spanning_batches_is_scored_once_on_its_best_value() {
        let doc = 1000;
        let tree = tree_with_multivalue_doc_spanning_two_ranges(doc);

        // One range per batch: the doc's two values land in different batches,
        // which per-batch coalescing alone cannot reconcile.
        let pairs = drain_pairs_in_chunks(&tree, &NumericFilter::default(), 1);

        let occurrences: Vec<f64> = pairs
            .iter()
            .filter(|(id, _)| *id == doc)
            .map(|(_, score)| *score)
            .collect();
        assert_eq!(
            occurrences,
            [1.0],
            "ascending must emit the doc once, on its smallest value"
        );
    }

    #[test]
    fn multivalue_doc_spanning_batches_is_scored_once_descending() {
        let doc = 1000;
        let tree = tree_with_multivalue_doc_spanning_two_ranges(doc);
        let filter = NumericFilter {
            ascending: false,
            ..NumericFilter::default()
        };

        let pairs = drain_pairs_in_chunks(&tree, &filter, 1);

        let occurrences: Vec<f64> = pairs
            .iter()
            .filter(|(id, _)| *id == doc)
            .map(|(_, score)| *score)
            .collect();
        assert_eq!(
            occurrences,
            [20.0],
            "descending must emit the doc once, on its largest value"
        );
    }

    #[test]
    fn multivalue_doc_spanning_ranges_in_one_batch_is_scored_from_its_best_range() {
        let doc = 1000;
        let tree = tree_with_multivalue_doc_spanning_two_ranges(doc);
        let filter = NumericFilter {
            ascending: false,
            ..NumericFilter::default()
        };

        let pairs = drain_pairs(&tree, &filter);

        let occurrences: Vec<f64> = pairs
            .iter()
            .filter(|(id, _)| *id == doc)
            .map(|(_, score)| *score)
            .collect();
        assert_eq!(occurrences, [20.0], "scored from its best-scored range");
    }
}

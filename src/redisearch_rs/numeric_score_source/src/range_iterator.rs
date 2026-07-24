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

use index_result::RSIndexResult;
use inverted_index::{FilterNumericReader, IndexReader as _, NumericFilter};
use numeric_range_tree::{NumericRange, NumericRangeTree};
use rqe_core::DocId;

use crate::score_batch::NumericScoreBatch;

/// Streams a numeric tree's ranges in value order, windowed by the filter's
/// `offset`/`limit` and chunked by [`next_n`](Self::next_n).
pub struct NumericRangeIterator<'index> {
    tree: &'index NumericRangeTree,
    /// Ranges matching the current filter window, best-score-first.
    ranges: Vec<&'index NumericRange>,
    /// Index of the next range to hand out.
    pos: usize,
    /// Value predicate applied to each record as a batch is materialized.
    filter: NumericFilter,
    /// Doc ids already handed out by an earlier batch or window.
    ///
    /// A multivalue field indexes one entry per value, so a doc's values can
    /// fall in different range chunks and be split across `next_n` batches — or
    /// even across expanded windows. Ranges are strictly value-ordered and
    /// disjoint, so a doc's first emission is on its best value; later, worse
    /// occurrences are dropped to keep it scored exactly once.
    emitted: HashSet<DocId>,
}

impl<'index> NumericRangeIterator<'index> {
    /// Resolve `filter` against `tree` and prepare to stream the matching
    /// ranges best-score-first.
    pub fn new(tree: &'index NumericRangeTree, filter: &NumericFilter) -> Self {
        Self {
            tree,
            ranges: tree.find(filter),
            pos: 0,
            filter: *filter,
            emitted: HashSet::new(),
        }
    }

    /// Re-resolve the (typically expanded) `filter` and restart from the first
    /// matching range.
    ///
    /// The emitted-doc set is kept: expansion moves to a strictly worse,
    /// disjoint value window, so a doc already scored on a better value must
    /// stay suppressed. Use [`forget_emitted`](Self::forget_emitted) to reset it
    /// when restarting the whole query.
    pub fn refind(&mut self, filter: &NumericFilter) {
        self.ranges = self.tree.find(filter);
        self.pos = 0;
        self.filter = *filter;
    }

    /// Drop the record of already-emitted doc ids, so a full rewind can score
    /// every doc afresh.
    pub fn forget_emitted(&mut self) {
        self.emitted.clear();
    }

    /// Sum of `num_docs` across every range in the current window.
    ///
    /// Used as the per-window document estimate, both for the source's
    /// `num_estimated` and to advance the filter `offset` past a consumed
    /// window on retry.
    pub fn total_docs_estimate(&self) -> usize {
        self.ranges.iter().map(|r| r.num_docs() as usize).sum()
    }

    /// Whether every range of the current window has been handed out.
    pub fn is_exhausted(&self) -> bool {
        self.pos >= self.ranges.len()
    }

    /// Materialize the next `n` value-ordered ranges into one doc-id-ordered
    /// batch, or `Ok(None)` once the window is exhausted.
    ///
    /// `n` is clamped to at least `1`.
    pub fn next_n(&mut self, n: usize) -> std::io::Result<Option<NumericScoreBatch>> {
        if self.pos >= self.ranges.len() {
            return Ok(None);
        }
        let end = (self.pos + n.max(1)).min(self.ranges.len());
        let batch = merge_ranges(&self.ranges[self.pos..end], self.filter, &mut self.emitted)?;
        self.pos = end;
        Ok(Some(batch))
    }
}

/// Read each range's records that satisfy `filter` into a single
/// `(doc_id, score)` vector, one strictly-increasing entry per doc id.
///
/// A range read through [`FilterNumericReader`] yields only records whose value
/// lies in the filter's window, since the tree's buckets are coarser than the
/// window. Ranges overlap in doc-id space, so reading them back-to-back yields
/// interleaved ids; the sort restores the increasing order that
/// [`NumericScoreBatch`] requires for its `skip_to` `partition_point`.
///
/// A multivalue field indexes one entry per value, so a doc id can occur several
/// times with different scores. Occurrences within this batch's ranges are
/// coalesced to a single entry carrying the doc's best score for the sort
/// direction; occurrences already handed out by an earlier batch (tracked in
/// `emitted`) are dropped, since their better value was scored there.
fn merge_ranges(
    ranges: &[&NumericRange],
    filter: NumericFilter,
    emitted: &mut HashSet<DocId>,
) -> std::io::Result<NumericScoreBatch> {
    let mut items: Vec<(DocId, f64)> = Vec::new();
    let mut record = RSIndexResult::build_numeric(0.0).build();
    for range in ranges {
        let mut reader = FilterNumericReader::new(filter, range.reader());
        while reader.next_record(&mut record)? {
            if emitted.contains(&record.doc_id) {
                continue;
            }
            let score = record
                .as_numeric()
                .expect("numeric range yields numeric records");
            items.push((record.doc_id, score));
        }
    }
    items.sort_unstable_by_key(|(doc_id, _)| *doc_id);
    coalesce_by_doc_id(&mut items, filter.ascending);
    emitted.extend(items.iter().map(|(doc_id, _)| *doc_id));
    Ok(NumericScoreBatch::new(items))
}

/// Collapse each run of equal doc ids in a doc-id-sorted `items` to one entry,
/// keeping the best score for the sort direction: the smallest when `ascending`,
/// the largest otherwise.
fn coalesce_by_doc_id(items: &mut Vec<(DocId, f64)>, ascending: bool) {
    items.dedup_by(|dropped, kept| {
        if dropped.0 != kept.0 {
            return false;
        }
        // `dedup_by` retains `kept`, so fold the better score into it.
        let dropped_is_better = if ascending {
            dropped.1 < kept.1
        } else {
            dropped.1 > kept.1
        };
        if dropped_is_better {
            kept.1 = dropped.1;
        }
        true
    });
}

#[cfg(test)]
mod tests {
    use inverted_index::NumericFilter;
    use numeric_range_tree::NumericRangeTree;
    use rqe_core::DocId;
    use top_k::ScoreBatch;

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
        let mut it = NumericRangeIterator::new(tree, filter);
        let mut pairs = Vec::new();
        while let Some(mut batch) = it.next_n(per_batch).unwrap() {
            while let Some(pair) = batch.next() {
                pairs.push(pair);
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
            tree.add(value, value as f64, false, 0);
        }
        assert!(
            tree.find(&NumericFilter::default()).len() >= 2,
            "expected a split"
        );
        tree.add(doc_id, 1.0, true, 0);
        tree.add(doc_id, 20.0, true, 0);
        tree
    }

    #[test]
    fn next_n_drops_records_outside_the_value_window() {
        let mut tree = NumericRangeTree::new(false);
        for id in 0..30u64 {
            tree.add(id, id as f64, false, 0);
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
            tree.add(id, id as f64, false, 0);
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
    fn multivalue_doc_is_coalesced_to_its_best_ascending_value() {
        // `is_multivalued` lets a doc id repeat with several values, as a
        // multivalue field does.
        let mut tree = NumericRangeTree::new(false);
        tree.add(1, 90.0, true, 0);
        tree.add(1, 5.0, true, 0);
        tree.add(2, 40.0, false, 0);

        let pairs = drain_pairs(&tree, &NumericFilter::default());

        let ids: Vec<DocId> = pairs.iter().map(|(id, _)| *id).collect();
        assert_eq!(ids, [1, 2], "each doc id appears exactly once");
        assert_eq!(pairs[0].1, 5.0, "ascending keeps the doc's smallest value");
    }

    #[test]
    fn multivalue_doc_is_coalesced_to_its_best_descending_value() {
        let mut tree = NumericRangeTree::new(false);
        tree.add(1, 90.0, true, 0);
        tree.add(1, 5.0, true, 0);

        let filter = NumericFilter {
            ascending: false,
            ..NumericFilter::default()
        };
        let pairs = drain_pairs(&tree, &filter);

        assert_eq!(
            pairs,
            [(1, 90.0)],
            "descending keeps the doc's largest value"
        );
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
}

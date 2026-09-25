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

use std::{collections::HashSet, ops::Range};

use index_result::RSIndexResult;
use inverted_index::{FilterNumericReader, IndexReader, NumericFilter};
use numeric_range_tree::{NumericRange, NumericRangeTree, RangeWindow};
use rqe_core::DocId;
use rqe_iterators::{RQEIteratorError, utils::TimeoutContext};
use top_k::ChildCursor;

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
    /// disjoint, so a doc's first emission is on its best value; later, worse
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

    /// Materialize the next `n` value-ordered ranges into one doc-id-ordered
    /// batch, or `Ok(None)` once the window is exhausted.
    ///
    /// `n` is clamped to at least `1`. `timeout` is polled once per record read
    /// so a long materialization aborts with [`RQEIteratorError::TimedOut`]
    /// rather than running past the query deadline.
    pub fn next_n(
        &mut self,
        n: usize,
        timeout: &mut impl TimeoutContext,
    ) -> Result<Option<NumericScoreBatch>, RQEIteratorError> {
        let Some(span) = self.take_next(n) else {
            return Ok(None);
        };
        let batch = merge_ranges(
            &self.ranges[span],
            self.filter,
            self.emitted.as_mut(),
            timeout,
        )?;
        Ok(Some(batch))
    }

    /// Whether a doc id can occur in more than one range, once per value of a
    /// multivalue field.
    pub const fn is_multivalued(&self) -> bool {
        self.emitted.is_some()
    }

    /// Pass the documents the next `n` value-ordered ranges share with `child`
    /// to [`ChildCursor::accept`], or return `Ok(false)` once the window is
    /// exhausted.
    ///
    /// One leapfrog walks `child` against the ranges' merged doc-id order, so
    /// the blocks holding no candidate are never decoded and `child` is walked
    /// once per call. `keep` vets each match before it is accepted.
    ///
    /// Only for a tree that is not [`is_multivalued`](Self::is_multivalued):
    /// a doc id several ranges hold would be accepted once per value rather
    /// than once, on its best one.
    pub fn next_n_matched(
        &mut self,
        n: usize,
        child: &mut dyn ChildCursor,
        keep: impl FnMut(DocId, f64) -> bool,
        timeout: &mut impl TimeoutContext,
    ) -> Result<bool, RQEIteratorError> {
        debug_assert!(
            !self.is_multivalued(),
            "a multivalue field needs the full read"
        );
        let Some(span) = self.take_next(n) else {
            return Ok(false);
        };
        leapfrog_ranges(&self.ranges[span], self.filter, child, keep, timeout)?;
        Ok(true)
    }

    /// Hand out the positions of the next `n` (at least `1`) ranges, or `None`
    /// once the window is exhausted.
    fn take_next(&mut self, n: usize) -> Option<Range<usize>> {
        if self.pos >= self.ranges.len() {
            return None;
        }
        let start = self.pos;
        self.pos = (start + n.max(1)).min(self.ranges.len());
        Some(start..self.pos)
    }
}

/// Read each range's records that satisfy `filter` into a single
/// `(doc_id, score)` vector, one strictly-increasing entry per doc id.
///
/// A range read through [`FilterNumericReader`] yields only records whose value
/// lies in the filter's window, since the tree's buckets are coarser than the
/// window.
///
/// A range is written under increasing doc ids, so its records arrive already
/// ordered; ranges overlap in doc-id space, so reading several back-to-back
/// yields one ascending run per range. Ordering therefore only has to merge
/// those runs into the increasing order [`NumericScoreBatch`] requires for its
/// `skip_to` `partition_point` — a single run is already there, and the stable
/// sort detects and merges the rest rather than re-sorting from scratch.
///
/// A multivalue field indexes one entry per value, so a doc id can occur several
/// times with different scores. Occurrences within this batch's ranges are
/// coalesced to a single entry carrying the doc's best score for the sort
/// direction; occurrences already handed out by an earlier batch (tracked in
/// `emitted`) are dropped, since their better value was scored there.
///
/// `emitted` is the single statement of whether the field is multivalued at all:
/// `None` says no document carries more than one value, so a doc id cannot
/// repeat — within a batch or across batches — and both de-duplication steps are
/// skipped.
///
/// `timeout` is polled once per record and once more before the sort, so a
/// large batch stays deadline-aware across its ordering pass. The amortized
/// counter accumulates across records and ranges, so the real clock check
/// fires every `granularity` reads.
fn merge_ranges(
    ranges: &[&NumericRange],
    filter: NumericFilter,
    emitted: Option<&mut HashSet<DocId>>,
    timeout: &mut impl TimeoutContext,
) -> Result<NumericScoreBatch, RQEIteratorError> {
    let mut items: Vec<(DocId, f64)> = Vec::with_capacity(reserved_capacity(ranges, filter));
    let mut record = RSIndexResult::build_numeric(0.0).build();
    // Ranges that contributed at least one record, i.e. the number of ascending
    // runs `items` holds.
    let mut runs = 0usize;
    for range in ranges {
        let run_start = items.len();
        let mut reader = FilterNumericReader::new(filter, range.reader());
        while reader.next_record(&mut record)? {
            timeout.check_timeout()?;
            if emitted
                .as_ref()
                .is_some_and(|emitted| emitted.contains(&record.doc_id))
            {
                continue;
            }
            let score = record
                .as_numeric()
                .expect("numeric range yields numeric records");
            items.push((record.doc_id, score));
        }
        runs += usize::from(items.len() > run_start);
    }
    timeout.check_timeout()?;
    if runs > 1 {
        // Stable sort: it finds the per-range runs and merges them, where an
        // unstable sort would re-order data that is already mostly in place.
        items.sort_by_key(|(doc_id, _)| *doc_id);
    }
    if let Some(emitted) = emitted {
        coalesce_by_doc_id(&mut items, filter.ascending);
        emitted.extend(items.iter().map(|(doc_id, _)| *doc_id));
    }
    debug_assert!(
        items.windows(2).all(|w| w[0].0 < w[1].0),
        "a batch must hold one strictly-increasing entry per doc id"
    );
    Ok(NumericScoreBatch::new(items))
}

/// Records to reserve for reading `ranges` under `filter`: each range's
/// [`NumericRange::num_docs`], capped at
/// [`NumericRangeTree::MAXIMUM_RANGE_SIZE`] for a range that passes `filter`
/// only in part.
///
/// Such a range may yield none of its documents, and a single-value range never
/// splits however large it grows, so an uncapped count could reserve for
/// millions of records that are all filtered out.
fn reserved_capacity(ranges: &[&NumericRange], filter: NumericFilter) -> usize {
    ranges
        .iter()
        .map(|r| {
            let num_docs = r.num_docs() as usize;
            if filter.value_in_range(r.min_val()) && filter.value_in_range(r.max_val()) {
                num_docs
            } else {
                num_docs.min(NumericRangeTree::MAXIMUM_RANGE_SIZE)
            }
        })
        .sum()
}

/// Pass each document `ranges` (read through `filter`) share with `child` and
/// `keep` admits to [`ChildCursor::accept`].
///
/// The child drives: every range behind it seeks up to its doc id, and it then
/// either sits on a doc id a range holds — a match — or advances to the
/// smallest doc id any range holds. Each seek skips whole index blocks, so a
/// selective child leaves most of the ranges undecoded, and the ranges' merged
/// order means `child` is rewound and walked once however many ranges there
/// are.
///
/// A doc id is held by at most one range, the ranges being value-disjoint and
/// each doc carrying a single value.
fn leapfrog_ranges(
    ranges: &[&NumericRange],
    filter: NumericFilter,
    child: &mut dyn ChildCursor,
    mut keep: impl FnMut(DocId, f64) -> bool,
    timeout: &mut impl TimeoutContext,
) -> Result<(), RQEIteratorError> {
    let mut cursors: Vec<_> = ranges
        .iter()
        .map(|range| RangeCursor {
            reader: FilterNumericReader::new(filter, range.reader()),
            record: RSIndexResult::build_numeric(0.0).build(),
            doc_id: UNPOSITIONED,
        })
        .collect();

    child.rewind();
    let Some(mut child_doc) = child.next()? else {
        return Ok(());
    };
    loop {
        timeout.check_timeout()?;
        let mut smallest: Option<DocId> = None;
        let mut hit: Option<f64> = None;
        let mut i = 0;
        while i < cursors.len() {
            let cursor = &mut cursors[i];
            if cursor.doc_id < child_doc {
                if !cursor.reader.seek_record(child_doc, &mut cursor.record)? {
                    // Exhausted: nothing left in this range for any later doc id.
                    cursors.swap_remove(i);
                    continue;
                }
                cursor.doc_id = cursor.record.doc_id;
            }
            if cursor.doc_id == child_doc {
                hit = Some(
                    cursor
                        .record
                        .as_numeric()
                        .expect("numeric range yields numeric records"),
                );
            }
            smallest = Some(smallest.map_or(cursor.doc_id, |s| s.min(cursor.doc_id)));
            i += 1;
        }
        let Some(smallest) = smallest else {
            break;
        };
        let next = match hit {
            Some(score) => {
                if keep(child_doc, score) {
                    child.accept(child_doc, score);
                }
                child.next()?
            }
            None => child.advance_to(smallest)?,
        };
        let Some(next) = next else {
            break;
        };
        child_doc = next;
    }
    Ok(())
}

/// One range's reader and the record it is positioned on, during
/// [`leapfrog_ranges`].
struct RangeCursor<'index, R> {
    reader: R,
    record: RSIndexResult<'index>,
    /// Doc id of `record`, or [`UNPOSITIONED`] before the first seek.
    doc_id: DocId,
}

/// A [`RangeCursor`] doc id below every real one, so the first comparison
/// seeks the reader.
const UNPOSITIONED: DocId = 0;

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
    use numeric_range_tree::{NumericRangeTree, RangeWindow};
    use rqe_core::DocId;
    use rqe_iterators::{RQEIteratorError, utils::NoTimeoutChecker};
    use top_k::{ChildCursor, ScoreBatch};

    use super::{NumericRangeIterator, reserved_capacity};

    /// A child over sorted `ids` that records what it is asked to accept.
    struct MockChild {
        ids: Vec<DocId>,
        /// Index of the next id [`next`](ChildCursor::next) returns.
        pos: usize,
        rewinds: usize,
        accepted: Vec<(DocId, f64)>,
    }

    impl MockChild {
        fn new(ids: Vec<DocId>) -> Self {
            Self {
                ids,
                pos: 0,
                rewinds: 0,
                accepted: Vec::new(),
            }
        }

        /// The doc id the cursor is on, if any.
        fn current(&self) -> Option<DocId> {
            self.pos.checked_sub(1).map(|i| self.ids[i])
        }
    }

    impl ChildCursor for MockChild {
        fn next(&mut self) -> Result<Option<DocId>, RQEIteratorError> {
            let id = self.ids.get(self.pos).copied();
            self.pos += usize::from(id.is_some());
            Ok(id)
        }

        fn advance_to(&mut self, target: DocId) -> Result<Option<DocId>, RQEIteratorError> {
            self.pos += self.ids[self.pos..].partition_point(|&id| id < target);
            self.next()
        }

        fn rewind(&mut self) {
            self.pos = 0;
            self.rewinds += 1;
        }

        fn accept(&mut self, doc_id: DocId, score: f64) {
            assert_eq!(self.current(), Some(doc_id), "accepted off the cursor");
            self.accepted.push((doc_id, score));
        }
    }

    /// Odd ids take low values and even ids high ones, so the value split
    /// leaves every range's doc ids interleaved with another range's.
    fn interleaved_value(id: DocId) -> f64 {
        if id % 2 == 1 {
            id as f64
        } else {
            100.0 + id as f64
        }
    }

    fn interleaved_tree(docs: DocId) -> NumericRangeTree {
        let mut tree = NumericRangeTree::new(false);
        for id in 1..=docs {
            tree.add(id, interleaved_value(id), false, false, 0);
        }
        tree
    }

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
        let mut timeout = NoTimeoutChecker;
        let mut pairs = Vec::new();
        while let Some(mut batch) = it.next_n(per_batch, &mut timeout).unwrap() {
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
    fn next_n_matched_accepts_the_child_docs_the_ranges_hold_in_one_pass() {
        let docs = 40;
        let tree = interleaved_tree(docs);
        // Cuts through both value halves, so some ranges pass only in part.
        let filter = NumericFilter {
            min: 10.0,
            max: 130.0,
            ..NumericFilter::default()
        };
        let ranges = tree.find(&filter).len();
        assert!(ranges >= 2, "expected a split");
        // Every third id, plus ids past the index the ranges cannot hold.
        let child_ids: Vec<DocId> = (1..=docs + 10).filter(|id| id % 3 == 0).collect();

        let mut it = NumericRangeIterator::new(&tree, &filter, RangeWindow::UNBOUNDED);
        let mut child = MockChild::new(child_ids.clone());
        let more = it
            .next_n_matched(ranges, &mut child, |_, _| true, &mut NoTimeoutChecker)
            .unwrap();

        assert!(more);
        assert!(it.is_exhausted(), "every range must land in the one call");
        let expected: Vec<(DocId, f64)> = child_ids
            .into_iter()
            .filter(|&id| id <= docs && filter.value_in_range(interleaved_value(id)))
            .map(|id| (id, interleaved_value(id)))
            .collect();
        assert_eq!(child.accepted, expected);
        assert_eq!(
            child.rewinds, 1,
            "the child is walked once, not once per range"
        );
        assert!(
            !it.next_n_matched(ranges, &mut child, |_, _| true, &mut NoTimeoutChecker)
                .unwrap()
        );
    }

    #[test]
    fn next_n_matched_skips_the_matches_keep_rejects() {
        let docs = 40;
        let tree = interleaved_tree(docs);
        let filter = NumericFilter::default();
        let ranges = tree.find(&filter).len();

        let mut it = NumericRangeIterator::new(&tree, &filter, RangeWindow::UNBOUNDED);
        let mut child = MockChild::new((1..=docs).collect());
        it.next_n_matched(
            ranges,
            &mut child,
            |doc_id, _| doc_id % 2 == 0,
            &mut NoTimeoutChecker,
        )
        .unwrap();

        let expected: Vec<(DocId, f64)> = (1..=docs)
            .filter(|id| id % 2 == 0)
            .map(|id| (id, interleaved_value(id)))
            .collect();
        assert_eq!(child.accepted, expected);
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
            let batch = it.next_n(ranges, &mut NoTimeoutChecker).unwrap().unwrap();
            assert!(it.is_exhausted(), "every range must land in the one batch");
            batch
        };

        let mut batch = single_batch();
        let mut pairs = Vec::new();
        while let Some(pair) = batch.next() {
            pairs.push(pair);
        }
        let expected: Vec<(DocId, f64)> = (1..=docs).map(|id| (id, value_of(id))).collect();
        assert_eq!(pairs, expected);

        // `skip_to` binary-searches the merged order, across run boundaries.
        let mut batch = single_batch();
        let target = docs / 2;
        assert_eq!(batch.skip_to(target), Some((target, value_of(target))));
        assert_eq!(batch.next(), Some((target + 1, value_of(target + 1))));
        assert_eq!(batch.skip_to(docs + 1), None);
    }

    #[test]
    fn range_excluded_at_its_only_value_reserves_at_most_a_split_size() {
        let mut tree = NumericRangeTree::new(false);
        let docs = 2 * NumericRangeTree::MAXIMUM_RANGE_SIZE as u64;
        for id in 1..=docs {
            tree.add(id, 5.0, false, false, 0);
        }
        let excluding = NumericFilter {
            min: 5.0,
            max: 10.0,
            min_inclusive: false,
            ..NumericFilter::default()
        };
        let including = NumericFilter {
            min_inclusive: true,
            ..excluding
        };

        // The inclusive bounds check still selects the single, unsplit range.
        let ranges = tree.find(&excluding);
        assert_eq!(ranges.len(), 1);

        assert_eq!(
            reserved_capacity(&ranges, excluding),
            NumericRangeTree::MAXIMUM_RANGE_SIZE
        );
        assert_eq!(
            reserved_capacity(&ranges, including),
            ranges[0].num_docs() as usize
        );
        assert!(drain_pairs(&tree, &excluding).is_empty());
    }

    #[test]
    fn multivalue_doc_is_coalesced_to_its_best_ascending_value() {
        // `is_multivalued` lets a doc id repeat with several values, as a
        // multivalue field does.
        let mut tree = NumericRangeTree::new(false);
        tree.add(1, 90.0, false, true, 0);
        tree.add(1, 5.0, false, true, 0);
        tree.add(2, 40.0, false, false, 0);

        let pairs = drain_pairs(&tree, &NumericFilter::default());

        let ids: Vec<DocId> = pairs.iter().map(|(id, _)| *id).collect();
        assert_eq!(ids, [1, 2], "each doc id appears exactly once");
        assert_eq!(pairs[0].1, 5.0, "ascending keeps the doc's smallest value");
    }

    #[test]
    fn multivalue_doc_is_coalesced_to_its_best_descending_value() {
        let mut tree = NumericRangeTree::new(false);
        tree.add(1, 90.0, false, true, 0);
        tree.add(1, 5.0, false, true, 0);

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

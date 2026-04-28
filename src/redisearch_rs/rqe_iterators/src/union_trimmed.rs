/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Trimmed union iterator that reads children sequentially (not in doc-id order).
//!
//! # Background
//!
//! A numeric-range query (e.g. `@price:[10 100]`) is executed by building a
//! union over every numeric-tree leaf node whose range overlaps the query
//! range. The leaves are ordered by their range boundaries, so the child
//! iterators within the union are already sorted by numeric range.
//!
//! When the query also carries a `LIMIT offset count` clause **and** no
//! explicit `SORTBY`, the query optimizer knows that only a bounded number
//! of documents are needed. It can therefore *trim* the union: drop the
//! children whose cumulative estimated result count exceeds the limit,
//! keeping only enough children to satisfy the requested window. In
//! ascending order a prefix of children is kept; in descending order a
//! suffix.
//!
//! # Why a separate iterator?
//!
//! After trimming, the union no longer needs to produce documents in
//! globally sorted doc-id order — the result processor will re-sort by
//! the numeric field anyway. Dropping the sorted-merge requirement means
//! we can use a much simpler strategy: drain each child completely before
//! moving to the next. This avoids the min-finding overhead of
//! [`super::UnionFlat`] / [`super::UnionHeap`] and skips the heap or
//! array bookkeeping entirely.
//!
//! Children are read from last to first (reverse insertion order). This
//! means [`RQEIterator::skip_to`] is not supported — calling it will
//! panic.
//!
//! # Ownership
//!
//! All children — including those outside the active window — are kept
//! alive in the `Vec`. Profile display queries children dynamically
//! via [`UnionTrimmed::child_at`], so trimmed-away children must remain
//! accessible even though they are inactive.

use ffi::t_docId;
use inverted_index::RSIndexResult;

use crate::{IteratorType, RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome};

/// Union iterator that drains children sequentially in reverse order.
///
/// Created by the query optimizer when a numeric-range union can be trimmed
/// to satisfy a `LIMIT` clause. See the [module documentation](self) for
/// the full rationale.
///
/// Use [`new`](Self::new) to construct.
///
/// # Panics
///
/// - [`new`](Self::new): if fewer than 3 children are provided.
/// - [`skip_to`](RQEIterator::skip_to): children are drained sequentially
///   (not in doc-id order), so skipping to a specific doc-id has no
///   meaningful semantics.
/// - [`revalidate`](RQEIterator::revalidate): trimmed unions run in a
///   single, short-lived read path that does not interleave with GC cycles,
///   so revalidation should never be needed.
///
/// # Type Parameters
///
/// - `'index`: Lifetime of the index data.
/// - `I`: The child iterator type, must implement [`RQEIterator`].
pub struct UnionTrimmed<'index, I> {
    /// All child iterators. The cursor only visits children in the trimmed
    /// window `[trim_start..trim_end)`. Children outside the window are kept
    /// alive (not dropped) so that profile display can query them by index.
    children: Vec<I>,
    /// First index of the active window (stored for [`rewind`](RQEIterator::rewind)).
    trim_start: usize,
    /// One past the last index of the active window (stored for [`rewind`](RQEIterator::rewind)).
    trim_end: usize,
    /// Sum of active children's estimated counts (upper bound).
    num_estimated: usize,
    /// Index of the child currently being drained. Starts at
    /// `trim_end - 1` and decrements toward `trim_start` as children
    /// exhaust. Once the child at `trim_start` is exhausted, `is_eof`
    /// is set to `true`.
    cursor: usize,
    /// Whether all children in the active window have been exhausted.
    is_eof: bool,
    /// Aggregate result combining children's results, reused to avoid allocations.
    result: RSIndexResult<'index>,
}

impl<'index, I> UnionTrimmed<'index, I>
where
    I: RQEIterator<'index>,
{
    /// Creates a trimmed union by selecting a subset of `children` based on
    /// the LIMIT optimizer heuristic, then wrapping them for unsorted
    /// sequential read.
    ///
    /// Children are assumed to be ordered by their numeric range. In
    /// ascending mode the first children cover the lowest ranges, so we
    /// keep a prefix; in descending mode we keep a suffix.
    ///
    /// The anchor child is always kept: in ascending mode the first
    /// child (lowest range) is never trimmed; in descending mode the
    /// last child (highest range) is never trimmed. Trimming scans
    /// inward from the second child (asc) or second-to-last child
    /// (desc), accumulating
    /// [`num_estimated`](RQEIterator::num_estimated) until `limit` is
    /// exceeded, then cuts.
    ///
    /// All children remain owned by the iterator even if they fall
    /// outside the active window. See [module docs](self) for why.
    ///
    /// # Panics
    ///
    /// Panics if `children` has fewer than 3 elements. Trimming only
    /// makes sense when there are enough children to actually drop some.
    pub fn new(children: Vec<I>, limit: usize, asc: bool) -> Self {
        let num = children.len();
        assert!(
            num >= 3,
            "UnionTrimmed requires at least 3 children, got {num}"
        );

        let mut cur_total: usize = 0;

        if asc {
            let mut keep = num;
            for (i, child) in children[1..].iter().enumerate() {
                cur_total += child.num_estimated();
                if cur_total > limit {
                    keep = i + 2; // i is 0-based within the [1..] slice
                    break;
                }
            }
            Self::from_range(children, 0, keep)
        } else {
            // desc: scan from the end, skipping the anchor (last child).
            // We also skip children[0] because it is the farthest from
            // the anchor and is scanned last (in reverse). Even if its
            // estimate were accumulated, it can only trigger at slice
            // index 0, producing skip = 0 (keep everything) — the same
            // outcome as never scanning it. So omitting it is safe.
            let mut skip = 0;
            for (i, child) in children[1..num - 1].iter().enumerate().rev() {
                cur_total += child.num_estimated();
                if cur_total > limit {
                    skip = i + 1; // i is 0-based within the [1..num-1] slice
                    break;
                }
            }
            Self::from_range(children, skip, num)
        }
    }

    /// Builds a [`UnionTrimmed`] whose active window is `children[start..end]`.
    ///
    /// # Panics
    ///
    /// Panics (debug only) if `end - start < 2` or `end > children.len()`.
    fn from_range(children: Vec<I>, start: usize, end: usize) -> Self {
        debug_assert!(end - start >= 2 && end <= children.len());
        let num_estimated: usize = children[start..end].iter().map(|c| c.num_estimated()).sum();
        let num_active = end - start;
        Self {
            result: RSIndexResult::build_union(num_active).build(),
            children,
            trim_start: start,
            trim_end: end,
            num_estimated,
            cursor: end - 1,
            is_eof: false,
        }
    }

    /// Returns the total number of children (including trimmed and exhausted ones).
    pub const fn num_children_total(&self) -> usize {
        self.children.len()
    }

    /// Returns the number of currently active (non-exhausted, non-trimmed) children.
    pub const fn num_children_active(&self) -> usize {
        if self.is_eof {
            0
        } else {
            self.cursor - self.trim_start + 1
        }
    }

    /// Returns a shared reference to the child at `idx` (across all children).
    /// Returns `None` if the index is out of range.
    pub fn child_at(&self, idx: usize) -> Option<&I> {
        self.children.get(idx)
    }

    /// Returns a mutable iterator over all children (including trimmed and exhausted ones).
    pub fn children_mut(&mut self) -> impl Iterator<Item = &mut I> {
        self.children.iter_mut()
    }

    /// Consumes the iterator and returns a new [`UnionTrimmed`] with different
    /// trim parameters over the same children, or [`None`] if the iterator
    /// has fewer than 3 children.
    pub fn into_trimmed(self, limit: usize, asc: bool) -> Option<Self> {
        (self.children.len() >= 3).then(|| Self::new(self.children, limit, asc))
    }
}

impl<'index, I> RQEIterator<'index> for UnionTrimmed<'index, I>
where
    I: RQEIterator<'index>,
{
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        (!self.at_eof()).then_some(&mut self.result)
    }

    #[inline(always)]
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        self.result.reset_aggregate();
        // Drain active children from last to first using the cursor for O(1) access.
        while !self.is_eof {
            let child = &mut self.children[self.cursor];
            match child.read()? {
                Some(child_result) => {
                    self.result.doc_id = child_result.doc_id;
                    let drained_metrics = std::mem::take(&mut child_result.metrics);
                    let child_ptr: *const RSIndexResult<'index> = child_result;
                    // SAFETY: child_result and self.result are disjoint fields — no aliasing.
                    // The child is owned by self, so the 'index data remains valid.
                    let child_ref = unsafe { &*child_ptr };
                    self.result.push_borrowed(child_ref, drained_metrics);
                    return Ok(Some(&mut self.result));
                }
                None => {
                    if self.cursor > self.trim_start {
                        self.cursor -= 1;
                    } else {
                        self.is_eof = true;
                    }
                }
            }
        }
        Ok(None)
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        _doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        // UnionTrimmed drains children sequentially, not in doc-id order,
        // so skip_to has no meaningful semantics. Panic to surface misuse
        // immediately rather than silently returning wrong results.
        panic!(
            "skip_to is not supported on UnionTrimmed — documents are not yielded in doc-id order"
        );
    }

    #[inline(always)]
    unsafe fn revalidate(
        &mut self,
        _spec: std::ptr::NonNull<ffi::IndexSpec>,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        // Trimmed unions run in a single, short-lived read path that does not
        // interleave with GC cycles, so revalidation should never be called.
        panic!(
            "revalidate is not supported on UnionTrimmed — trimmed unions are not subject to GC"
        );
    }

    #[inline(always)]
    fn rewind(&mut self) {
        self.result.reset_aggregate();
        self.is_eof = false;
        self.cursor = self.trim_end - 1;
        for child in &mut self.children[self.trim_start..self.trim_end] {
            child.rewind();
        }
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        self.num_estimated
    }

    #[inline(always)]
    fn last_doc_id(&self) -> t_docId {
        self.result.doc_id
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.is_eof
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        IteratorType::Union
    }

    #[inline(always)]
    fn intersection_sort_weight(&self, prioritize_union_children: bool) -> f64 {
        if prioritize_union_children {
            self.num_children_active().max(1) as f64
        } else {
            1.0
        }
    }
}

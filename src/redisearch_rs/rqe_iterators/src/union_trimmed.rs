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

use index_result::{RSIndexResult, RawIndexResult};
use ref_mode::{Active, Ref, Suspended};
use rqe_core::DocId;

use crate::{
    IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    ResumeOutcome, SkipToOutcome,
};
use index_spec::IndexSpecReadGuard;

/// Union iterator that drains children sequentially in reverse order.
///
/// Parameterised over a [`Ref`] mode — see [`UnionTrimmed`] for the
/// [`Active`] instantiation that implements [`RQEIterator`].
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
/// - [`resume`](RQESuspendedIterator::resume): the partial-range pipeline
///   drains a trimmed union in one locked pass, so it is never resumed, and it
///   has no position to re-derive. [`suspend`](RQEIteratorBoxed::suspend) itself
///   is supported, since the pipeline suspends the tree on every spec read-lock
///   release, including the final one after the drain.
///
/// # Type Parameters
///
/// - `Rf`: The [`Ref`] mode.
/// - `I`: The child iterator type, must implement [`RQEIterator`].
#[repr(C)]
pub struct RawUnionTrimmed<'query, Rf: Ref, I> {
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
    result: RawIndexResult<'query, Rf>,
}

/// Alias for an [`Active`] [`RawUnionTrimmed`] — the only instantiation
/// with an [`RQEIterator`] impl today.
pub type UnionTrimmed<'index, I> = RawUnionTrimmed<'index, Active<'index>, I>;

// Compile-time proof that the `Active` and `Suspended` instantiations are
// layout-identical, which the whole-box cast in `suspend` relies on.
const _: () = {
    use crate::Wildcard;
    use std::mem::{align_of, offset_of, size_of};
    type AChild = Wildcard<'static>;
    type SChild = <Wildcard<'static> as RQEIteratorBoxed<'static>>::Suspended;
    type A = RawUnionTrimmed<'static, Active<'static>, AChild>;
    type S = RawUnionTrimmed<'static, Suspended, SChild>;
    assert!(offset_of!(A, children) == offset_of!(S, children));
    assert!(offset_of!(A, trim_start) == offset_of!(S, trim_start));
    assert!(offset_of!(A, trim_end) == offset_of!(S, trim_end));
    assert!(offset_of!(A, num_estimated) == offset_of!(S, num_estimated));
    assert!(offset_of!(A, cursor) == offset_of!(S, cursor));
    assert!(offset_of!(A, is_eof) == offset_of!(S, is_eof));
    assert!(offset_of!(A, result) == offset_of!(S, result));
    assert!(size_of::<A>() == size_of::<S>());
    assert!(align_of::<A>() == align_of::<S>());
};

impl<'query, Rf: Ref, I> RawUnionTrimmed<'query, Rf, I> {
    /// Returns the total number of children (including trimmed and exhausted ones).
    /// Mode-independent.
    pub const fn num_children_total(&self) -> usize {
        self.children.len()
    }

    /// Returns the number of currently active (non-exhausted, non-trimmed) children.
    /// Mode-independent.
    pub const fn num_children_active(&self) -> usize {
        if self.is_eof {
            0
        } else {
            self.cursor - self.trim_start + 1
        }
    }

    /// Returns a shared reference to the child at `idx` (across all children).
    /// Returns `None` if the index is out of range. Mode-independent.
    pub fn child_at(&self, idx: usize) -> Option<&I> {
        self.children.get(idx)
    }
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
        _doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        // UnionTrimmed drains children sequentially, not in doc-id order,
        // so skip_to has no meaningful semantics. Panic to surface misuse
        // immediately rather than silently returning wrong results.
        unreachable!(
            "skip_to is not supported on UnionTrimmed — documents are not yielded in doc-id order"
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
    fn last_doc_id(&self) -> DocId {
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

impl<'index, I> RQEIteratorBoxed<'index> for UnionTrimmed<'index, I>
where
    I: RQEIteratorBoxed<'index>,
{
    type Suspended = RawUnionTrimmed<'index, Suspended, I::Suspended>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        // Reached on the spec read-lock release that ends the partial-range
        // drain: `RPSorter` pulls every result while the lock is held, then the
        // query iterator suspends the tree as it unlocks. Nothing resumes it
        // afterwards (see `resume`), but the transition itself must succeed.
        const {
            crate::boxed::assert_layout_compatible::<Self, Self::Suspended>();
        };
        let raw = Box::into_raw(self);
        // Walked through the buffer pointer, as in `UnionFlat::suspend`: each
        // transition retypes a slot from `I` to `I::Suspended`. Every child is
        // suspended, including those outside the trimmed window, since the
        // whole buffer is relabelled below.
        let (base, len) = {
            // SAFETY: `raw` came from `Box::into_raw` and is exclusively owned
            // for the rest of this function, so the children Vec is reachable
            // and unaliased. The borrow only reaches the buffer pointer and
            // length, and ends here.
            let children: &mut Vec<I> = unsafe { &mut (*raw).children };
            (children.as_mut_ptr(), children.len())
        };
        for i in 0..len {
            // SAFETY: `i` is in bounds of the children buffer.
            let child = unsafe { base.add(i) };
            // SAFETY: `child` holds a valid, owned `I`; the helper leaves the
            // slot holding a valid `I::Suspended`.
            unsafe { crate::boxed::suspend_child_slot_in_place(child) };
        }
        // SAFETY: every child slot now holds its suspended counterpart, and the
        // two instantiations are layout-identical (the `const` proof above and
        // the assertion at the top of this function). `Box::from_raw` reuses
        // the same allocation.
        unsafe { Box::from_raw(raw as *mut RawUnionTrimmed<'index, Suspended, I::Suspended>) }
    }
}

/// Suspended counterpart of the [`RQEIteratorBoxed`] impl above. A value of it
/// exists only between the final spec read-lock release and the iterator being
/// freed; it is never resumed.
impl<'query, S> RQESuspendedIterator<'query> for RawUnionTrimmed<'query, Suspended, S>
where
    S: RQESuspendedIterator<'query>,
{
    type Resumed<'a>
        = UnionTrimmed<'a, S::Resumed<'a>>
    where
        'query: 'a;

    fn resume<'a>(
        self: Box<Self>,
        _guard: &IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        // This is a design constraint, not a gap to be filled later: a trimmed
        // union yields documents out of doc-id order and its `skip_to` panics,
        // so there is no position to re-derive once the children have moved.
        // It is also never needed: the partial-range pipeline drains the union
        // in one locked pass, so the only suspend it sees is the final one.
        unreachable!(
            "resume is not supported on UnionTrimmed — a trimmed union has no position to re-derive"
        );
    }

    fn last_doc_id(&self) -> DocId {
        self.result.doc_id
    }

    fn num_estimated(&self) -> usize {
        self.num_estimated
    }
}

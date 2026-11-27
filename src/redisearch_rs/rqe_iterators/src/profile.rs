/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Profile iterator for collecting performance metrics.
//!
//! This module provides a wrapper iterator that collects profiling metrics
//! (read/skip counts and wall-clock time) from a child iterator without
//! modifying its behavior.

use std::time::{Duration, Instant};

use crate::{
    IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    RQEValidateStatus, ResumeOutcome, SkipToOutcome,
    boxed::{resume_single_child, suspend_single_child},
};
use index_result::RSIndexResult;
use index_spec::IndexSpecReadGuard;
use ref_mode::{Active, Ref, Suspended};
use rqe_core::DocId;

/// Profile counters collected during query execution.
///
/// This struct is `#[repr(C)]` so that C code can access its fields directly.
#[cheadergen::config(export)]
#[derive(Debug, Default, Clone)]
#[repr(C)]
pub struct ProfileCounters {
    /// Number of `read()` calls made.
    pub read: usize,
    /// Number of `skip_to()` calls made.
    pub skip_to: usize,
    /// Whether the iterator reached EOF.
    pub eof: bool,
}

impl ProfileCounters {
    /// Returns the number of reading operations for profile display.
    ///
    /// This is the sum of `read` and `skip_to` counts, minus one if EOF was
    /// reached (to exclude the final unsuccessful read). The result is
    /// clamped to zero.
    pub fn num_reading_operations(&self) -> usize {
        (self.read + self.skip_to).saturating_sub(usize::from(self.eof))
    }
}

/// A wrapper iterator that collects profiling metrics from a child iterator.
///
/// Parameterised over a [`Ref`] mode — see [`Profile`] for the [`Active`]
/// instantiation that implements [`RQEIterator`].
///
/// This iterator delegates all operations to its inner child iterator while:
/// - Tracking the number of [`read()`](RQEIterator::read) and [`skip_to()`](RQEIterator::skip_to) calls
/// - Measuring wall-clock time spent in these operations
/// - Recording whether EOF was reached
///
/// The collected metrics can be accessed via [`Profile::counters()`] and
/// [`Profile::wall_time_ns()`].
///
/// # Invariants
///
/// 1. **Layout compatibility across modes.** `RawProfile` is `#[repr(C)]` and
///    carries no `Rf`-dependent field other than the zero-sized
///    `_marker: PhantomData<Rf>`, so its `Active` and `Suspended`
///    instantiations are layout-identical given that the child `I` and its
///    `I::Suspended` are. That child-level compatibility is enforced at
///    monomorphization by [`suspend_child_slot_in_place`](crate::boxed::suspend_child_slot_in_place); the wrapper's own
///    mode-independence is proven by the `const` block below. Together they let
///    [`suspend`](RQEIteratorBoxed::suspend) reinterpret the owning `Box` in
///    place.
#[repr(C)]
pub struct RawProfile<Rf: Ref, I> {
    child: I,
    counters: ProfileCounters,
    /// Time spent in child iterator operations.
    wall_time: Duration,
    /// The child's [`RQEIterator::num_estimated`], captured at construction
    /// under the spec lock — the value query planning consumed. The profile
    /// print reads this cache instead of calling the live iterator: stored
    /// replies print on the main thread without the spec lock, possibly after
    /// the index itself is gone.
    estimated: usize,
    _marker: std::marker::PhantomData<Rf>,
}

/// Alias for an [`Active`] [`RawProfile`] — the only instantiation with an
/// [`RQEIterator`] impl today.
pub type Profile<'index, I> = RawProfile<Active<'index>, I>;

impl<Rf: Ref, I> RawProfile<Rf, I> {
    /// The child field of the decorator at `raw`, formed without a reference.
    ///
    /// # Safety
    ///
    /// 1. `raw` points to a live allocation of `Self`.
    unsafe fn child_ptr(raw: *mut Self) -> *mut I {
        // SAFETY: 1.
        unsafe { &raw mut (*raw).child }
    }

    /// Drop every field of the decorator at `raw` but the child, in place.
    ///
    /// # Safety
    ///
    /// 1. `raw` points to a live, exclusively owned allocation of `Self` whose
    ///    fields other than the child are valid. The child slot is not read.
    /// 2. Those fields are not used again.
    unsafe fn drop_rest(raw: *mut Self) {
        // SAFETY: 1.
        let counters = unsafe { &raw mut (*raw).counters };
        // SAFETY: 1 and 2: the field is valid, owned and never used again.
        unsafe { std::ptr::drop_in_place(counters) };
        // SAFETY: 1.
        let wall_time = unsafe { &raw mut (*raw).wall_time };
        // SAFETY: 1 and 2: the field is valid, owned and never used again.
        unsafe { std::ptr::drop_in_place(wall_time) };
    }
}

// Compile-time proof of invariant 1 on `RawProfile`: for a representative
// concrete child, the `Active` and `Suspended` instantiations are
// layout-identical. The child slot's own layout compatibility is the child's
// invariant 1 (enforced generically for every implementer by
// `suspend_child_slot_in_place`); the wrapper adds only `Rf`-free fields
// (`counters`, `wall_time`) plus the zero-sized `PhantomData<Rf>`, so the two
// modes cannot diverge here. `Wildcard` is a representative child whose own
// `Active`/`Suspended` forms differ but stay layout-compatible.
const _: () = {
    use crate::Wildcard;
    use std::mem::{align_of, offset_of, size_of};
    type AChild = Wildcard<'static>;
    type SChild = <Wildcard<'static> as RQEIteratorBoxed<'static>>::Suspended;
    type A = RawProfile<Active<'static>, AChild>;
    type S = RawProfile<Suspended, SChild>;
    assert!(offset_of!(A, child) == offset_of!(S, child));
    assert!(offset_of!(A, counters) == offset_of!(S, counters));
    assert!(offset_of!(A, wall_time) == offset_of!(S, wall_time));
    assert!(offset_of!(A, estimated) == offset_of!(S, estimated));
    assert!(size_of::<A>() == size_of::<S>());
    assert!(align_of::<A>() == align_of::<S>());
};

impl<'index, I: RQEIterator<'index>> Profile<'index, I> {
    /// Creates a new Profile iterator wrapping the given child iterator.
    ///
    /// The counters are initialized to zero and wall time starts at 0. The
    /// child's [`RQEIterator::num_estimated`] is captured into
    /// [`estimated`](Self::estimated) here, while the caller still holds the
    /// spec lock.
    pub fn new(child: I) -> Self {
        let estimated = child.num_estimated();
        Self {
            child,
            counters: ProfileCounters::default(),
            wall_time: Duration::ZERO,
            estimated,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<Rf: Ref, I> RawProfile<Rf, I> {
    /// Returns a reference to the child iterator.
    #[inline]
    pub const fn child(&self) -> &I {
        &self.child
    }

    /// Returns a reference to the collected profile counters.
    #[inline]
    pub const fn counters(&self) -> &ProfileCounters {
        &self.counters
    }

    /// Returns the accumulated wall time in nanoseconds assuming u64 is enough and there is
    /// no risk of overflow.
    #[inline]
    pub const fn wall_time_ns(&self) -> u64 {
        self.wall_time.as_nanos() as u64
    }

    /// Returns the child's construction-time [`RQEIterator::num_estimated`]
    /// capture (see [`Self::new`]).
    #[inline]
    pub const fn estimated(&self) -> usize {
        self.estimated
    }
}

impl<'index, I: RQEIterator<'index>> RQEIterator<'index> for Profile<'index, I> {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        self.child.current()
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        let start = Instant::now();
        let result = self.child.read();
        self.wall_time += start.elapsed();

        self.counters.read += 1;
        if matches!(&result, Ok(None)) {
            self.counters.eof = true;
        }
        result
    }

    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        let start = Instant::now();
        let result = self.child.skip_to(doc_id);
        self.wall_time += start.elapsed();

        self.counters.skip_to += 1;
        if matches!(&result, Ok(None)) {
            self.counters.eof = true;
        }
        result
    }

    fn rewind(&mut self) {
        self.child.rewind();
    }

    fn num_estimated(&self) -> usize {
        self.child.num_estimated()
    }

    fn last_doc_id(&self) -> DocId {
        self.child.last_doc_id()
    }

    fn at_eof(&self) -> bool {
        self.child.at_eof()
    }

    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        self.child.revalidate(spec)
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        IteratorType::Profile
    }

    fn intersection_sort_weight(&self, prioritize_union_children: bool) -> f64 {
        self.child
            .intersection_sort_weight(prioritize_union_children)
    }

    fn reports_score_bounds(&self) -> bool {
        self.child.reports_score_bounds()
    }

    fn current_max_score(&self) -> f64 {
        self.child.current_max_score()
    }

    fn current_block_bound(&self) -> Option<crate::score_bound::BlockBound> {
        self.child.current_block_bound()
    }

    fn read_with_threshold(
        &mut self,
        min_competitive_score: crate::score_bound::MinCompetitiveScore,
    ) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        let start = Instant::now();
        let result = self.child.read_with_threshold(min_competitive_score);
        self.wall_time += start.elapsed();

        self.counters.read += 1;
        if matches!(&result, Ok(None)) {
            self.counters.eof = true;
        }
        result
    }
}

use crate::profile_print::{ProfilePrint, ProfilePrintCtx};

impl<Rf: Ref, I> ProfilePrint for RawProfile<Rf, I>
where
    I: ProfilePrint,
{
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        let counters = self.counters();
        let mut child_ctx = ctx.with_counters(counters, self.wall_time_ns(), self.estimated);
        self.child().print_profile(map, &mut child_ctx);
    }
}

impl<'index, I> RQEIteratorBoxed<'index> for Profile<'index, I>
where
    I: RQEIteratorBoxed<'index>,
{
    type Suspended = RawProfile<Suspended, I::Suspended>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        // Profile delegates `current()` into the child's storage, which the
        // reused allocation keeps in place.
        // SAFETY: (1) `child_ptr` projects to the child slot without forming a
        // reference. (2) The `Active` and `Suspended` forms are layout-identical
        // apart from that slot (invariant 1 on `RawProfile`).
        unsafe { suspend_single_child(self, Self::child_ptr) }
    }
}

impl<'query, S> RQESuspendedIterator<'query> for RawProfile<Suspended, S>
where
    S: RQESuspendedIterator<'query>,
{
    type Resumed<'a>
        = Profile<'a, S::Resumed<'a>>
    where
        'query: 'a;

    fn resume<'a>(
        self: Box<Self>,
        guard: &IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        // Profile owns no aggregate result and simply mirrors the child's
        // `Ok`/`Moved`/`Aborted` outcome.
        //
        // SAFETY: (1) `child_ptr` projects to the child slot without forming a
        // reference. (2) The forms are layout-identical apart from that slot
        // (invariant 1 on `RawProfile`). (3) `drop_rest` drops every other field
        // in place, without reading the child slot.
        unsafe { resume_single_child(self, guard, Self::child_ptr, Self::drop_rest) }
    }

    fn last_doc_id(&self) -> DocId {
        S::last_doc_id(&self.child)
    }

    fn num_estimated(&self) -> usize {
        S::num_estimated(&self.child)
    }
}

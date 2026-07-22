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
    boxed::{ResumeSlotOutcome, resume_child_slot_in_place},
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
///    `I::Suspended` are (the [`RQEIteratorBoxed`] contract). This is what lets
///    [`suspend`](RQEIteratorBoxed::suspend) reinterpret the owning `Box` in
///    place.
#[repr(C)]
pub struct RawProfile<Rf: Ref, I> {
    child: I,
    counters: ProfileCounters,
    /// Time spent in child iterator operations.
    wall_time: Duration,
    _marker: std::marker::PhantomData<Rf>,
}

/// Alias for an [`Active`] [`RawProfile`] — the only instantiation with an
/// [`RQEIterator`] impl today.
pub type Profile<'index, I> = RawProfile<Active<'index>, I>;

impl<'index, I: RQEIterator<'index>> Profile<'index, I> {
    /// Creates a new Profile iterator wrapping the given child iterator.
    ///
    /// The counters are initialized to zero and wall time starts at 0.
    pub fn new(child: I) -> Self {
        Self {
            child,
            counters: ProfileCounters::default(),
            wall_time: Duration::ZERO,
            _marker: std::marker::PhantomData,
        }
    }

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
}

use crate::profile_print::{ProfilePrint, ProfilePrintCtx};

impl<'index, I> ProfilePrint for Profile<'index, I>
where
    I: RQEIterator<'index> + ProfilePrint,
{
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        let counters = self.counters();
        let mut child_ctx = ctx.with_counters(counters, self.wall_time_ns());
        self.child().print_profile(map, &mut child_ctx);
    }
}

impl<'index, I> RQEIteratorBoxed<'index> for Profile<'index, I>
where
    I: RQEIteratorBoxed<'index>,
{
    type Suspended = RawProfile<Suspended, I::Suspended>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: `RawProfile` is `#[repr(C)]`. The `Rf`-dependent field is
        // only `_marker: PhantomData<Rf>` (zero-sized). `child: I` and the
        // suspended counterpart's `child: I::Suspended` are layout-
        // compatible by the [`RQEIteratorBoxed`] contract. `counters` and
        // `wall_time` carry no `Rf`. Box::from_raw reuses the same heap
        // allocation, so the box address is preserved.
        unsafe { Box::from_raw(raw as *mut RawProfile<Suspended, I::Suspended>) }
    }
}

/// Free a [`RawProfile`] allocation whose `child` slot has already been consumed
/// (moved-from) by an aborted/failed child resume, without running the child's
/// drop glue.
///
/// # Safety
///
/// * `raw` came from `Box::into_raw` and is still exclusively owned by the caller.
/// * `(*raw).child` is moved-from (uninitialised) and must NOT be dropped.
/// * No other reference to the allocation exists.
unsafe fn dealloc_after_child_gone<S>(raw: *mut RawProfile<Suspended, S>) {
    // SAFETY: `raw` is valid; `&raw mut` forms a field pointer without a reference.
    let counters = unsafe { &raw mut (*raw).counters };
    // SAFETY: `counters` is still a valid, owned value; drop it in place.
    unsafe { std::ptr::drop_in_place(counters) };
    // SAFETY: `raw` is valid; `&raw mut` forms a field pointer without a reference.
    let wall_time = unsafe { &raw mut (*raw).wall_time };
    // SAFETY: `wall_time` is still a valid, owned value; drop it in place.
    unsafe { std::ptr::drop_in_place(wall_time) };
    // `_marker` is a ZST with no drop glue.
    // SAFETY: `raw` was allocated by `Box` with exactly this layout; the `child`
    // slot is moved-from so it is not dropped. Frees the allocation.
    unsafe {
        std::alloc::dealloc(
            raw.cast::<u8>(),
            std::alloc::Layout::new::<RawProfile<Suspended, S>>(),
        )
    };
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
        // Resume the child in place and reuse this box's allocation, mirroring
        // `RawInvIndIterator::resume`. Profile delegates `current()` — and hence
        // the FFI wrapper's cached `header.current` pointer — into the child's
        // storage, so the box (and the child slot inside it) must keep its
        // address across the cycle; rebuilding via `Box::new` would dangle that
        // pointer. Profile owns no aggregate result and simply mirrors the
        // child's `Ok`/`Moved`/`Aborted` outcome.
        let raw = Box::into_raw(self);
        // SAFETY: `raw` came from `Box::into_raw` (non-null, aligned,
        // initialised, exclusively owned). `&raw mut` forms a field pointer to
        // the `child` slot without creating a reference.
        let child_slot = unsafe { &raw mut (*raw).child };
        // SAFETY: `child_slot` points at the valid, exclusively-owned suspended
        // child. On `Unchanged`/`Moved` the helper reinitialises the slot as
        // `S::Resumed<'a>`; on `Aborted`/`Err` the child is consumed and the slot
        // is left uninitialised (handled by the teardown arms below).
        match unsafe { resume_child_slot_in_place(child_slot, guard) } {
            Ok(outcome @ (ResumeSlotOutcome::Unchanged | ResumeSlotOutcome::Moved)) => {
                // SAFETY: the child slot now holds a valid `S::Resumed<'a>`, and
                // every other field is `Rf`-independent, so the allocation is a
                // valid `Profile<'a, S::Resumed<'a>>` — layout-identical to the
                // suspended form (invariant 1: `RawProfile` is `#[repr(C)]`).
                // `Box::from_raw` reuses the same allocation, preserving the box
                // and child-slot addresses.
                let active = unsafe { Box::from_raw(raw.cast::<Profile<'a, S::Resumed<'a>>>()) };
                Ok(if matches!(outcome, ResumeSlotOutcome::Moved) {
                    ResumeOutcome::Moved(active)
                } else {
                    ResumeOutcome::Ok(active)
                })
            }
            Ok(ResumeSlotOutcome::Aborted) => {
                // SAFETY: the child was consumed by its `resume` (slot
                // uninitialised); free the reused allocation without dropping the
                // moved-from child.
                unsafe { dealloc_after_child_gone(raw) };
                Ok(ResumeOutcome::Aborted)
            }
            Err(e) => {
                // SAFETY: as the `Aborted` arm — the child is gone, tear the rest down.
                unsafe { dealloc_after_child_gone(raw) };
                Err(e)
            }
        }
    }

    fn last_doc_id(&self) -> DocId {
        S::last_doc_id(&self.child)
    }

    fn num_estimated(&self) -> usize {
        S::num_estimated(&self.child)
    }
}

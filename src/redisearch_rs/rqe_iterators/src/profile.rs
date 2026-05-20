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
    RQEValidateStatus, SkipToOutcome,
};
use ffi::ValidateStatus;
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
}

/// `Rf`-independent metadata accessors.
///
/// These methods read fields stored directly on [`RawProfile`] — `child`,
/// `counters`, `wall_time` — none of which depend on whether the iterator is in
/// [`Active`] or [`Suspended`] mode. Exposing them on the underlying
/// `Rf`-generic struct lets FFI introspection sites (FT.PROFILE printing) read
/// them via [`RQEIteratorWrapper::state`](crate::interop::RQEIteratorWrapper::state)
/// in either typestate by branching on the variant and calling the matching
/// method on each side.
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

impl<S> RQESuspendedIterator for RawProfile<Suspended, S>
where
    S: RQESuspendedIterator,
{
    type Resumed<'a> = Profile<'a, S::Resumed<'a>>;

    fn resume<'a>(
        self: Box<Self>,
        guard: &'a IndexSpecReadGuard<'a>,
    ) -> (Box<Self::Resumed<'a>>, ValidateStatus) {
        // Field-by-field rebuild: we can't whole-box-cast on resume
        // because the child needs `resume` driven on it (re-validating
        // its state against the lock), and the active iterator's
        // `PhantomData<Active<'a>>` carries a borrow we have to construct.
        let RawProfile {
            child,
            counters,
            wall_time,
            _marker,
        } = *self;
        // Box the child to drive its `resume`. Profile has no aggregate
        // result, so the child's heap address is not load-bearing — the
        // re-allocation is acceptable.
        let (active_child, status) = Box::new(child).resume(guard);
        let active = Box::new(Profile {
            child: *active_child,
            counters,
            wall_time,
            _marker: std::marker::PhantomData,
        });
        (active, status)
    }

    fn last_doc_id(&self) -> DocId {
        S::last_doc_id(&self.child)
    }
}
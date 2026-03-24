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

use crate::{IteratorType, RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome};
use ffi::t_docId;
use inverted_index::RSIndexResult;

/// Profile counters collected during query execution.
///
/// This struct is `#[repr(C)]` so that C code can access its fields directly.
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

/// A wrapper iterator that collects profiling metrics from a child iterator.
///
/// This iterator delegates all operations to its inner child iterator while:
/// - Tracking the number of [`read()`](RQEIterator::read) and [`skip_to()`](RQEIterator::skip_to) calls
/// - Measuring wall-clock time spent in these operations
/// - Recording whether EOF was reached
///
/// The collected metrics can be accessed via [`Profile::counters()`] and
/// [`Profile::wall_time_ns()`].
pub struct Profile<'index, I: RQEIterator<'index>> {
    child: I,
    counters: ProfileCounters,
    /// Time spent in child iterator operations.
    wall_time: Duration,
    _marker: std::marker::PhantomData<&'index ()>,
}

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

/// Trait for iterators whose subtree can be wrapped with [`Profile`] instrumentation.
///
/// Composite iterators override [`profile_children`](Profilable::profile_children)
/// to recurse into their children first; leaf iterators return `self` unchanged.
pub trait Profilable<'index>: RQEIterator<'index> {
    /// The type of `Self` after [`profile_children`](Profilable::profile_children):
    /// all children wrapped with [`Profile`], but `self` left unwrapped.
    ///
    /// For leaf iterators this is `Self` (no children to transform).
    /// For composites the child type parameter changes, e.g.
    /// [`Not`](crate::not::Not)`<I>` → [`Not`](crate::not::Not)`<I::IntoProfiled>` (each child wrapped with [`Profile`]).
    type ProfileChildren: RQEIterator<'index>
    where
        Self: Sized;

    /// The type of `Self` after [`into_profiled`](Profilable::into_profiled):
    /// all children wrapped with [`Profile`] **and** `self` wrapped too.
    ///
    /// For most types this is `Profile<'index, Self::ProfileChildren>`.
    /// [`CRQEIterator`](crate::c2rust::CRQEIterator) returns [`CRQEIterator`](crate::c2rust::CRQEIterator)
    /// (type-erased, the [`Profile`] layer is hidden behind the pointer).
    type IntoProfiled: RQEIterator<'index>
    where
        Self: Sized;

    /// Leaf iterators have no children to profile.
    ///
    /// When `true`, [`RQEIteratorWrapper::boxed_new`](crate::interop::RQEIteratorWrapper::boxed_new)
    /// sets the C vtable `ProfileChildren` callback to `NULL`, so the
    /// wrapper is never consumed and reallocated during profiling. This
    /// keeps interior pointers (held by C code) valid.
    fn is_leaf() -> bool
    where
        Self: Sized;

    /// Wrap all children with [`Profile`] iterators, without wrapping `self`.
    ///
    /// Leaf iterators return `self` unchanged. Composite iterators call
    /// [`into_profiled`](Profilable::into_profiled) on each child.
    fn profile_children(self) -> Self::ProfileChildren
    where
        Self: Sized;

    /// Wrap the entire subtree — children first, then `self` — with [`Profile`].
    fn into_profiled(self) -> Self::IntoProfiled
    where
        Self: Sized;

    /// Dyn-compatible version of [`into_profiled`](Profilable::into_profiled).
    ///
    /// Concrete types delegate to `(*self).into_profiled()`.
    fn into_profiled_boxed(self: Box<Self>) -> Box<dyn RQEIterator<'index> + 'index>;
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
        doc_id: t_docId,
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

    fn last_doc_id(&self) -> t_docId {
        self.child.last_doc_id()
    }

    fn at_eof(&self) -> bool {
        self.child.at_eof()
    }

    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        self.child.revalidate()
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        IteratorType::Profile
    }
}

impl<'index, P: Profilable<'index> + 'index> Profilable<'index> for Box<P>
where
    Box<P::ProfileChildren>: RQEIterator<'index>,
{
    type ProfileChildren = Box<P::ProfileChildren>;
    type IntoProfiled = Profile<'index, Self::ProfileChildren>;

    fn is_leaf() -> bool {
        P::is_leaf()
    }

    fn profile_children(self) -> Self::ProfileChildren {
        Box::new((*self).profile_children())
    }

    fn into_profiled(self) -> Self::IntoProfiled {
        Profile::new(self.profile_children())
    }

    fn into_profiled_boxed(self: Box<Self>) -> Box<dyn RQEIterator<'index> + 'index> {
        Box::new((*self).into_profiled())
    }
}

/// No-op: a [`Profile`] wrapper is the outermost layer added by
/// [`into_profiled`](Profilable::into_profiled) — its child is already
/// profiled, so there is nothing left to recurse into.
impl<'index, P: Profilable<'index> + 'index> Profilable<'index> for Profile<'index, P> {
    type ProfileChildren = Self;
    type IntoProfiled = Self;

    fn is_leaf() -> bool {
        true
    }

    fn profile_children(self) -> Self::ProfileChildren {
        self
    }

    fn into_profiled(self) -> Self {
        self
    }

    fn into_profiled_boxed(self: Box<Self>) -> Box<dyn RQEIterator<'index> + 'index> {
        self
    }
}

/// Leaf impl for type-erased iterators (used by wildcard FFI).
///
/// Because we cannot reach the inner iterator's [`Profilable`] impl
/// through the trait object, this is a no-op leaf implementation.
/// This is fine in practice: only leaf iterators are boxed as
/// `dyn RQEIterator` today (wildcard FFI). If a composite were ever
/// boxed this way, its children would silently not be profiled.
impl<'index> Profilable<'index> for Box<dyn RQEIterator<'index> + 'index> {
    type ProfileChildren = Self;
    type IntoProfiled = Profile<'index, Self>;

    fn is_leaf() -> bool {
        true
    }

    fn profile_children(self) -> Self {
        self
    }

    fn into_profiled(self) -> Self::IntoProfiled {
        Profile::new(self.profile_children())
    }

    fn into_profiled_boxed(self: Box<Self>) -> Box<dyn RQEIterator<'index> + 'index> {
        Box::new((*self).into_profiled())
    }
}

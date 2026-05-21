/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types for [`Not`].

use std::time::Duration;

use ffi::{RS_FIELDMASK_ALL, ValidateStatus, ValidateStatus_VALIDATE_ABORTED, ValidateStatus_VALIDATE_OK, t_docId};
use index_result::{RSIndexResult, RawIndexResult};
use ref_mode::{Active, Ref, Suspended};

use crate::{
    IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    RQEValidateStatus, SkipToOutcome,
    maybe_empty::MaybeEmpty, utils::TimeoutContext,
};

use index_spec::IndexSpecReadGuard;
/// An iterator that negates the results of its child iterator.
///
/// Parameterised over a [`Ref`] mode — see [`Not`] for the [`Active`]
/// instantiation that implements [`RQEIterator`].
///
/// Yields all document IDs from 1 to `max_doc_id` (inclusive) that are **not**
/// present in the child iterator.
#[repr(C)]
pub struct RawNot<Rf: Ref, I> {
    /// The child iterator whose results are negated.
    child: MaybeEmpty<I>,
    /// The maximum document ID to iterate up to (inclusive).
    max_doc_id: t_docId,
    /// Set to `true` in case the NOT Iterator
    /// detected using the [`TimeoutContext`] a timeout,
    /// and reset to `false` at [`RQEIterator::rewind`].
    forced_eof: bool,
    /// A reusable result object to avoid allocations on each [`read`](RQEIterator::read) call.
    result: RawIndexResult<Rf>,
    /// Tracks the execution deadline for this iterator.
    ///
    /// Uses an amortized check to minimize overhead in hot paths. The timeout
    /// is absolute for the iterator's lifetime and does not reset upon rewinding.
    timeout_ctx: Option<TimeoutContext>,
}

/// Alias for an [`Active`] [`RawNot`] — the only instantiation with an
/// [`RQEIterator`] impl today.
pub type Not<'index, I> = RawNot<Active<'index>, I>;

impl<'index, I> Not<'index, I>
where
    I: RQEIterator<'index>,
{
    pub fn new(
        child: I,
        max_doc_id: t_docId,
        weight: f64,
        timeout: Duration,
        skip_timeout_checks: bool,
    ) -> Self {
        Self {
            child: MaybeEmpty::new(child),
            max_doc_id,
            forced_eof: false,
            result: RSIndexResult::build_virt()
                .weight(weight)
                .field_mask(RS_FIELDMASK_ALL)
                .build(),
            // The `limit` of 5_000 determines the granularity of the timeout check.
            // Each time [`TimeoutContext::check_timeout`] is called (during `read` / `skip_to`),
            // the internal counter goes up. When it reaches this `limit` of 5_000 it will
            // reset that counter and do the actual (OS) expensive timeout check.
            timeout_ctx: if skip_timeout_checks {
                None
            } else {
                Some(TimeoutContext::new(timeout, 5_000, false))
            },
        }
    }

    /// Wrapper around [`TimeoutContext::check_timeout`] to ensure that in case of an error (timeout),
    /// we also mark this iterator as EOF.
    ///
    /// Returns error [`RQEIteratorError::TimedOut`] if the deadline has been reached or exceeded.
    ///
    /// In case no timeout is enforced it will just return `Ok(())`.
    #[inline(always)]
    fn check_timeout(&mut self) -> Result<(), RQEIteratorError> {
        let Some(result) = self.timeout_ctx.as_mut().map(|ctx| ctx.check_timeout()) else {
            return Ok(());
        };
        if matches!(result, Err(RQEIteratorError::TimedOut)) {
            // NOTE: this is not done for optimized version of NOT iterator in C
            self.forced_eof = true;
        }
        result
    }

    /// Wrapper around [`TimeoutContext::reset_counter`] to reset the timeout counter.
    ///
    /// Does nothing when no timeout is enforced.
    #[inline(always)]
    const fn reset_timeout(&mut self) {
        if let Some(ctx) = self.timeout_ctx.as_mut() {
            ctx.reset_counter();
        }
    }

}

impl<Rf: Ref, I> RawNot<Rf, I> {
    /// Get a shared reference to the _child_ iterator
    /// wrapped by this [`Not`] iterator. Mode-independent.
    pub const fn child(&self) -> Option<&I> {
        self.child.as_ref()
    }
}

impl<'index, I> RQEIterator<'index> for Not<'index, I>
where
    I: RQEIterator<'index>,
{
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        Some(&mut self.result)
    }

    #[inline(always)]
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        // skip all child docs, while not EOF and in sync with child
        while !self.at_eof() {
            self.result.doc_id += 1;

            // Sync child if we've moved past its last known position
            let child_at_eof = if self.result.doc_id > self.child.last_doc_id() {
                self.child.read()?.is_none()
            } else {
                false
            };

            // Comparison Logic
            // If child is EOF, or we haven't reached the child's position,
            // or the child skipped past us, this document is a valid result.
            if child_at_eof || self.result.doc_id != self.child.last_doc_id() {
                self.reset_timeout();
                return Ok(Some(&mut self.result));
            }

            // Unified Checkpoint: Exactly one check per iteration.
            // This occurs AFTER the child.read() and before we decide to return.
            self.check_timeout()?;

            // Otherwise: doc_id == child.last_doc_id(), so we skip and loop again.
        }

        debug_assert!(self.at_eof());
        Ok(None)
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        debug_assert!(self.last_doc_id() < doc_id);

        if self.at_eof() {
            return Ok(None);
        }

        // Do not skip beyond max_doc_id
        if doc_id > self.max_doc_id {
            self.result.doc_id = self.max_doc_id;
            return Ok(None);
        }

        // Case 1: Child is ahead or at EOF - docId is not in child
        // When child is at EOF, only accept doc_id if it's past the child's last document
        if self.child.last_doc_id() > doc_id
            || (self.child.at_eof() && doc_id > self.child.last_doc_id())
        {
            self.result.doc_id = doc_id;
            self.check_timeout()?;

            return Ok(Some(SkipToOutcome::Found(&mut self.result)));
        }
        // Case 2: Child is behind docId - need to check if docId is in child
        if self.child.last_doc_id() < doc_id {
            let rc = self.child.skip_to(doc_id)?;
            match rc {
                Some(SkipToOutcome::Found(_)) => {
                    // Found value - do not return
                }
                None | Some(SkipToOutcome::NotFound(_)) => {
                    // Not found or EOF - return
                    self.result.doc_id = doc_id;

                    self.check_timeout()?;

                    return Ok(Some(SkipToOutcome::Found(&mut self.result)));
                }
            }
        }

        self.check_timeout()?;

        // If we are here, Child has DocID (either already lastDocID == docId or the SkipTo returned OK)
        // We need to return NOTFOUND and set the current result to the next valid docId
        self.result.doc_id = doc_id;
        match self.read()? {
            Some(_) => Ok(Some(SkipToOutcome::NotFound(&mut self.result))),
            None => Ok(None),
        }
    }

    #[inline(always)]
    fn rewind(&mut self) {
        self.forced_eof = false;
        self.result.doc_id = 0;
        self.child.rewind();
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        self.max_doc_id as usize
    }

    #[inline(always)]
    fn last_doc_id(&self) -> t_docId {
        self.result.doc_id
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.forced_eof || self.result.doc_id >= self.max_doc_id
    }

    #[inline(always)]
    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        // Get child status
        match self.child.revalidate(spec)? {
            RQEValidateStatus::Aborted => {
                self.child = MaybeEmpty::new_empty();
                Ok(RQEValidateStatus::Ok)
            }
            RQEValidateStatus::Moved { .. } => {
                // Invariant: after read/skip_to, child is always ahead of NOT's position (or at EOF).
                // Moved means child moved forward (can't move backward), so our doc remains valid.
                // Special case: both at initial state (doc_id = 0) is also valid.
                debug_assert!(
                    self.child.at_eof()
                        || self.child.last_doc_id() > self.last_doc_id()
                        || (self.child.last_doc_id() == 0 && self.last_doc_id() == 0)
                );
                Ok(RQEValidateStatus::Ok)
            }
            RQEValidateStatus::Ok => {
                // Child did not move - we did not move
                Ok(RQEValidateStatus::Ok)
            }
        }
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        IteratorType::Not
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}

impl<'index, I> RQEIteratorBoxed<'index> for Not<'index, I>
where
    I: RQEIteratorBoxed<'index>,
{
    type Suspended = RawNot<Suspended, I::Suspended>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: `RawNot` is `#[repr(C)]`. The only `Rf`-dependent field
        // is `result: RawIndexResult<Rf>`, layout-compatible across `Rf`
        // via `SharedPtr` transparency. `MaybeEmpty<I>` and
        // `MaybeEmpty<I::Suspended>` are layout-compatible by the
        // [`RQEIteratorBoxed`] contract (see [`MaybeEmpty::suspend`]).
        // Box::from_raw reuses the same heap allocation.
        unsafe { Box::from_raw(raw as *mut RawNot<Suspended, I::Suspended>) }
    }

    fn cascade_suspend(&mut self) {
        self.child.cascade_suspend();
    }
}

impl<S> RQESuspendedIterator for RawNot<Suspended, S>
where
    S: RQESuspendedIterator,
{
    type Resumed<'a> = Not<'a, S::Resumed<'a>>;

    fn resume<'a>(
        self: Box<Self>,
        guard: &'a IndexSpecReadGuard<'a>,
    ) -> (Box<Self::Resumed<'a>>, ValidateStatus) {
        let RawNot {
            child,
            max_doc_id,
            forced_eof,
            result,
            timeout_ctx,
        } = *self;

        let (active_child, child_status) = Box::new(child).resume(guard);
        let mut child = *active_child;

        // Mirror the existing `revalidate` semantics: if the child aborted,
        // `NOT (aborted)` collapses to "NOT empty" — drop the child and
        // return OK so the iterator stays usable. A child move doesn't
        // shift NOT's position because NOT keeps its own cursor.
        if child_status == ValidateStatus_VALIDATE_ABORTED {
            child = MaybeEmpty::new_empty();
        }

        // SAFETY: `Not`'s `result` is a virtual sentinel built via
        // `build_virt()` — its `data` is `RawResultData::Virtual` (carries
        // no pointers), `dmd` is null at this iterator's construction, and
        // `metrics` is empty. There are therefore no aliased pointers
        // whose validity could be in question, so the `Active<'a>`
        // re-typing is unconditionally sound.
        let result = unsafe { result.into_active::<'a>() };

        let active = Box::new(Not {
            child,
            max_doc_id,
            forced_eof,
            result,
            timeout_ctx,
        });
        (active, ValidateStatus_VALIDATE_OK)
    }

    fn last_doc_id(&self) -> t_docId {
        self.result.doc_id
    }

    fn num_estimated(&self) -> usize {
        self.max_doc_id as usize
    }
}

/// Trait for NOT iterators ([`Not`] and [`crate::not_optimized::NotOptimized`]).
pub trait NotIterator<'index>: RQEIterator<'index> {
    // Those methods are used by profile.c to wrap the child iterator.
    // They can be removed once this code is ported to Rust.
    /// Get a shared reference to the child iterator, or `None` if unset.
    fn child(&self) -> Option<&dyn RQEIterator<'index>>;
}

impl<'index> NotIterator<'index> for Not<'index, Box<dyn RQEIterator<'index> + 'index>> {
    fn child(&self) -> Option<&dyn RQEIterator<'index>> {
        self.child
            .as_ref()
            .map(|c| &**c as &dyn RQEIterator<'index>)
    }
}

impl<'index> crate::interop::ProfileChildren<'index> for Not<'index, crate::c2rust::CRQEIterator> {
    fn profile_children(self) -> Self {
        Not {
            child: self.child.map(crate::c2rust::CRQEIterator::into_profiled),
            max_doc_id: self.max_doc_id,
            forced_eof: self.forced_eof,
            result: self.result,
            timeout_ctx: self.timeout_ctx,
        }
    }
}

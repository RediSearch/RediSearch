/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types for [`Not`].

use index_result::{RSIndexResult, RSResultKind, RawIndexResult};
use ref_mode::{Active, Ref, Suspended};

use crate::{
    IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    RQEValidateStatus, ResumeOutcome, SkipToOutcome,
    boxed::{ResumeSlotOutcome, resume_child_slot_in_place, suspend_child_slot_in_place},
    maybe_empty::MaybeEmpty,
    profile_print::{ProfilePrint, ProfilePrintCtx},
    utils::TimeoutContext,
};

use index_spec::IndexSpecReadGuard;
use rqe_core::{DocId, RS_FIELDMASK_ALL};
/// An iterator that negates the results of its child iterator.
///
/// Parameterised over a [`Ref`] mode — see [`Not`] for the [`Active`]
/// instantiation that implements [`RQEIterator`].
///
/// Yields all document IDs from 1 to `max_doc_id` (inclusive) that are **not**
/// present in the child iterator.
///
/// # Type parameters
///
/// * `Rf` - The [`Ref`] mode.
/// * `I` - The child iterator type whose results are negated.
/// * `TC` - The [`TimeoutContext`] implementation. The variant is chosen at
///   construction time and monomorphized into the hot path.
#[repr(C)]
pub struct RawNot<'query, Rf: Ref, I, TC> {
    /// The child iterator whose results are negated.
    child: MaybeEmpty<I>,
    /// The maximum document ID to iterate up to (inclusive).
    max_doc_id: DocId,
    /// Set to `true` in case the NOT Iterator
    /// detected using the [`TimeoutContext`] a timeout,
    /// and reset to `false` at [`RQEIterator::rewind`].
    forced_eof: bool,
    /// A reusable result object to avoid allocations on each [`read`](RQEIterator::read) call.
    result: RawIndexResult<'query, Rf>,
    /// Whether the iterator has run *past* its last result, i.e. a
    /// `read`/`skip_to` found nothing.
    ///
    /// The state behind [`current`](RQEIterator::current) and
    /// [`at_eof`](RQEIterator::at_eof). It cannot be folded into `result.doc_id`:
    /// that field *is* [`last_doc_id`](RQEIterator::last_doc_id).
    past_end: bool,
    /// Tracks the execution deadline for this iterator. Pass
    /// [`NoTimeoutChecker`](timeout::NoTimeoutChecker) to opt out of timeout checks
    /// entirely; monomorphization collapses the no-op context to dead code.
    ///
    /// The timeout is absolute for the iterator's lifetime and does not
    /// reset upon rewinding.
    timeout_ctx: TC,
}

/// Alias for an [`Active`] [`RawNot`] — the only instantiation with an
/// [`RQEIterator`] impl today.
pub type Not<'index, I, TC> = RawNot<'index, Active<'index>, I, TC>;

// Compile-time proof of invariant 1 on `RawNot`: for a representative concrete
// child, the `Active` and `Suspended` instantiations are layout-identical. The
// `result: RawIndexResult<Rf>` field's own cross-`Rf` layout compatibility is
// proven in `index_result`; the `MaybeEmpty<I>` slot's is the child's invariant 1
// (enforced generically by `suspend_child_slot_in_place`); the remaining fields
// are `Rf`-free.
const _: () = {
    use crate::Wildcard;
    use crate::utils::NoTimeoutChecker;
    use std::mem::{align_of, offset_of, size_of};
    type AChild = Wildcard<'static>;
    type SChild = <Wildcard<'static> as RQEIteratorBoxed<'static>>::Suspended;
    type A = RawNot<'static, Active<'static>, AChild, NoTimeoutChecker>;
    type S = RawNot<'static, Suspended, SChild, NoTimeoutChecker>;
    assert!(offset_of!(A, child) == offset_of!(S, child));
    assert!(offset_of!(A, max_doc_id) == offset_of!(S, max_doc_id));
    assert!(offset_of!(A, forced_eof) == offset_of!(S, forced_eof));
    assert!(offset_of!(A, result) == offset_of!(S, result));
    assert!(offset_of!(A, past_end) == offset_of!(S, past_end));
    assert!(offset_of!(A, timeout_ctx) == offset_of!(S, timeout_ctx));
    assert!(size_of::<A>() == size_of::<S>());
    assert!(align_of::<A>() == align_of::<S>());
};

impl<'index, I, TC> Not<'index, I, TC>
where
    I: RQEIterator<'index>,
    TC: TimeoutContext,
{
    /// Build a new [`Not`] iterator.
    ///
    /// `timeout_ctx` is the [`TimeoutContext`] implementation to use. Pass
    /// [`NoTimeoutChecker`](timeout::NoTimeoutChecker) to disable timeout checks
    /// entirely on this iterator's hot path.
    pub fn new(child: I, max_doc_id: DocId, weight: f64, timeout_ctx: TC) -> Self {
        Self {
            child: MaybeEmpty::new(child),
            max_doc_id,
            forced_eof: false,
            past_end: false,
            result: RSIndexResult::build_virt()
                .weight(weight)
                .field_mask(RS_FIELDMASK_ALL)
                .build(),
            timeout_ctx,
        }
    }

    /// Wrapper around [`TimeoutContext::check_timeout`] to ensure that in case of an error (timeout),
    /// we also mark this iterator as EOF.
    ///
    /// Returns error [`RQEIteratorError::TimedOut`] if the deadline has been reached or exceeded.
    #[inline(always)]
    fn check_timeout(&mut self) -> Result<(), RQEIteratorError> {
        let result = self.timeout_ctx.check_timeout();
        if matches!(result, Err(RQEIteratorError::TimedOut)) {
            // NOTE: this is not done for optimized version of NOT iterator in C
            self.forced_eof = true;
        }
        result
    }

    /// Wrapper around [`TimeoutContext::reset_counter`] to reset the timeout counter.
    #[inline(always)]
    fn reset_timeout(&mut self) {
        self.timeout_ctx.reset_counter();
    }

    /// Get a shared reference to the _child_ iterator
    /// wrapped by this [`Not`] iterator.
    pub const fn child(&self) -> Option<&I> {
        self.child.as_ref()
    }

    /// Whether there is another result to yield: the complement has not been
    /// walked up to `max_doc_id`, and no timeout has forced the iterator to stop.
    ///
    /// Goes `false` one step before [`Self::past_end`] is set, while the final
    /// result is still current.
    #[inline(always)]
    const fn has_next(&self) -> bool {
        !self.forced_eof && self.result.doc_id < self.max_doc_id
    }
}

impl<'index, I, TC> RQEIterator<'index> for Not<'index, I, TC>
where
    I: RQEIterator<'index>,
    TC: TimeoutContext,
{
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        if self.past_end {
            return None;
        }
        Some(&mut self.result)
    }

    #[inline(always)]
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        // The finished state is this flag, not something derived from the
        // position: `skip_to` puts the position back when it finds nothing, so
        // `has_next()` on its own would let a finished iterator resume below a
        // target its caller has already moved past.
        if self.past_end {
            return Ok(None);
        }

        // skip all child docs, while not EOF and in sync with child
        while self.has_next() {
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

        debug_assert!(!self.has_next());
        self.past_end = true;
        Ok(None)
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        debug_assert!(self.last_doc_id() < doc_id);

        if self.past_end || !self.has_next() {
            self.past_end = true;
            return Ok(None);
        }

        // Do not skip beyond max_doc_id. The position is left where it was:
        // returning `None` here means no result was produced, and
        // [`skip_to`](RQEIterator::skip_to) owes its caller an untouched
        // `last_doc_id()` in that case.
        if doc_id > self.max_doc_id {
            self.past_end = true;
            return Ok(None);
        }

        // Case 1: Child is ahead or at EOF - docId is not in child
        // When child is at EOF, only accept doc_id if it's past the child's last document
        //
        // `at_eof()` is the trailing state, so a child still sitting on its last
        // result falls through to Case 2's probe, which returns the same
        // `Found(doc_id)` after one extra call.
        if self.child.last_doc_id() > doc_id
            || (self.child.at_eof() && doc_id > self.child.last_doc_id())
        {
            // Checked before the position is published, not after: a timeout
            // carries no result, and `skip_to` may not leave the probe target
            // behind as the position in that case.
            self.check_timeout()?;
            self.result.doc_id = doc_id;

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
                    // Not found or EOF - return. Timeout checked first, for the
                    // reason given in Case 1.
                    self.check_timeout()?;
                    self.result.doc_id = doc_id;

                    return Ok(Some(SkipToOutcome::Found(&mut self.result)));
                }
            }
        }

        self.check_timeout()?;

        // If we are here, Child has DocID (either already lastDocID == docId or the SkipTo returned OK)
        // We need to return NOTFOUND and set the current result to the next valid docId
        //
        // The scan below has to start from `doc_id`, but the probe target only
        // becomes this iterator's position once a result is in hand: the scan can
        // run off the end or fail, and a `skip_to` that carries no result must
        // leave `last_doc_id()` alone. Publishing it early is what let a parent
        // read a position as a promise of a result at it.
        let resume_from = self.result.doc_id;
        self.result.doc_id = doc_id;
        match self.read() {
            Ok(Some(_)) => Ok(Some(SkipToOutcome::NotFound(&mut self.result))),
            Ok(None) => {
                self.result.doc_id = resume_from;
                Ok(None)
            }
            Err(e) => {
                self.result.doc_id = resume_from;
                Err(e)
            }
        }
    }

    #[inline(always)]
    fn rewind(&mut self) {
        self.forced_eof = false;
        self.past_end = false;
        self.result.doc_id = 0;
        self.child.rewind();
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        self.max_doc_id as usize
    }

    #[inline(always)]
    fn last_doc_id(&self) -> DocId {
        self.result.doc_id
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.past_end
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

impl<'index, I, TC> RQEIteratorBoxed<'index> for Not<'index, I, TC>
where
    I: RQEIteratorBoxed<'index>,
    TC: TimeoutContext + 'static,
{
    type Suspended = RawNot<'index, Suspended, I::Suspended, TC>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // Transition the child in place. A whole-box cast alone is *not* enough
        // for a type-erased child: its active and suspended forms carry different
        // `dyn` vtables, so the transition must be dispatched through the child's
        // own `suspend` — which `suspend_child_slot_in_place` does (a no-op
        // whole-box cast for concrete children, a vtable swap for erased ones).
        // The slot is a `MaybeEmpty<I>`, itself an `RQEIteratorBoxed` that walks
        // its own `Some(I)` arm.
        //
        // SAFETY: `raw` came from `Box::into_raw` (non-null, aligned, initialised,
        // exclusively owned); `&raw mut` forms a field pointer to `child`.
        let child = unsafe { &raw mut (*raw).child };
        // SAFETY: `child` points at a valid, owned `MaybeEmpty<I>`; the helper
        // dispatches its `suspend` and reinitialises the slot as a valid
        // `MaybeEmpty<I::Suspended>`.
        unsafe { suspend_child_slot_in_place(child) };
        // SAFETY: the child slot now holds its `Suspended` form;
        // `result: RawIndexResult<Rf>` is layout-compatible across `Rf`, and the
        // remaining fields are `Rf`-free — so the allocation is a valid suspended
        // `RawNot`, layout-identical to the active form by invariant 1 (const
        // proof above). `Box::from_raw` reuses the same heap allocation.
        unsafe { Box::from_raw(raw as *mut RawNot<'index, Suspended, I::Suspended, TC>) }
    }
}

impl<'query, S, TC> RQESuspendedIterator<'query> for RawNot<'query, Suspended, S, TC>
where
    S: RQESuspendedIterator<'query>,
    TC: TimeoutContext + 'static,
{
    type Resumed<'a>
        = Not<'a, S::Resumed<'a>, TC>
    where
        'query: 'a;

    fn resume<'a>(
        self: Box<Self>,
        guard: &IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        // `Not`'s `result` must still be a virtual sentinel. It is handed out
        // mutably via `current()`/`read()`/`skip_to`, so a consumer could in
        // principle have replaced its `data` with a real, index-backed payload —
        // and reinterpreting that `Suspended → Active` would assert `'index`
        // borrows this iterator cannot re-validate (it owns no backing for the
        // sentinel). `kind() == Virtual` is the whole condition: `data` is the
        // only `Rf`-parametrized field. Checked safely via `&self`, before
        // `Box::into_raw` opens the raw-pointer critical section, so a violation
        // just drops `self`; the state is recoverable, hence `Aborted` rather
        // than `Err`.
        if self.result.kind() != RSResultKind::Virtual {
            return Ok(ResumeOutcome::Aborted);
        }

        let raw = Box::into_raw(self);

        // SAFETY: `raw` came from `Box::into_raw` (non-null, aligned, initialised,
        // exclusively owned); `&raw mut` forms a field pointer to `child`.
        let child_slot = unsafe { &raw mut (*raw).child };

        // Resume the child in place. `MaybeEmpty<S>`'s own `resume` walks its
        // `Some` arm and propagates an inner abort; its `None(Empty)` arm resumes
        // unchanged.
        //
        // SAFETY: `child_slot` holds a valid, owned `MaybeEmpty<S>`. On
        // `Unchanged`/`Moved` the helper rewrites the slot as a valid
        // `MaybeEmpty<S::Resumed<'a>>`; on `Aborted`/`Err` it consumes the child
        // and leaves the slot uninitialised (handled in each arm below).
        let child_moved = match unsafe { resume_child_slot_in_place(child_slot, guard) } {
            Ok(ResumeSlotOutcome::Unchanged) => false,
            Ok(ResumeSlotOutcome::Moved) => true,
            Ok(ResumeSlotOutcome::Aborted) => {
                // Mirror `revalidate`: `NOT (aborted)` collapses to "NOT empty".
                // The consumed slot is reinitialised with the `I`-free empty arm,
                // typed as the *resumed* slot the reinterpretation below expects.
                // SAFETY: the slot is uninitialised and exclusively owned;
                // `ptr::write` does not drop the moved-from child, and
                // `MaybeEmpty<S>`/`MaybeEmpty<S::Resumed>` share size/alignment
                // (enforced by `resume_child_slot_in_place`).
                unsafe {
                    child_slot
                        .cast::<MaybeEmpty<S::Resumed<'a>>>()
                        .write(MaybeEmpty::new_empty())
                };
                false
            }
            Err(e) => {
                // Free the reused allocation without dropping the moved-from
                // child: restore the `I`-free empty arm, then reclaim and drop
                // the box normally (frees `result` + the allocation).
                // SAFETY: the slot is uninitialised and exclusively owned;
                // `ptr::write` does not drop the moved-from child.
                unsafe {
                    child_slot
                        .cast::<MaybeEmpty<S::Resumed<'a>>>()
                        .write(MaybeEmpty::new_empty())
                };
                // SAFETY: every field of `raw` is initialised again, and the
                // child slot holds the resumed type this cast names — `result`
                // stays suspended, since this path never re-types it. Reclaim
                // the allocation `Box::into_raw` released above and drop it.
                drop(unsafe {
                    Box::from_raw(raw.cast::<RawNot<'query, Suspended, S::Resumed<'a>, TC>>())
                });
                return Err(e);
            }
        };

        // Reinterpret the owning box in place, reusing the allocation so the
        // `result` pointer handed out by `current()`/`read()`/`skip_to` — and the
        // FFI's cached `header.current` — stays valid across the cycle.
        //
        // SAFETY: the child slot holds its resumed form (or the empty arm);
        // `result` is a virtual sentinel (checked above), so its
        // `Suspended → Active<'a>` re-typing carries no index pointers; the
        // remaining fields (`max_doc_id`, `forced_eof`, `past_end` — an iterator
        // already past its end must stay there, only `rewind` clears it —
        // `timeout_ctx`) are `Rf`-free. Layout-identical to the suspended form by
        // invariant 1 on `RawNot` (const proof above). `Box::from_raw` reuses the
        // same allocation.
        let active = unsafe { Box::from_raw(raw.cast::<Not<'a, S::Resumed<'a>, TC>>()) };

        if child_moved {
            // A child only ever moves forward, so a move leaves NOT's own position
            // valid: read/skip_to always leave the child ahead of us or at EOF, and
            // both still sitting at 0 is the fresh-iterator case. Asserted after the
            // reinterpretation because only there is the child slot typed as an
            // active iterator; the same check guards `revalidate`'s `Moved` arm.
            debug_assert!(
                active.child.at_eof()
                    || RQEIterator::last_doc_id(&active.child) > active.last_doc_id()
                    || (RQEIterator::last_doc_id(&active.child) == 0 && active.last_doc_id() == 0)
            );
        }

        // Mirror `revalidate`, which reports `Ok` in all three child outcomes: a
        // child move doesn't shift NOT's position (NOT keeps its own cursor), and
        // an aborted child was absorbed above.
        Ok(ResumeOutcome::Ok(active))
    }

    fn last_doc_id(&self) -> DocId {
        self.result.doc_id
    }

    fn num_estimated(&self) -> usize {
        // Mode-independent — mirrors the active `num_estimated`.
        self.max_doc_id as usize
    }
}

impl<'index, TC> crate::interop::ProfileChildren<'index>
    for Not<'index, crate::c2rust::CRQEIterator, TC>
where
    TC: TimeoutContext + 'index,
{
    fn profile_children(self) -> Self {
        Not {
            child: self.child.map(crate::c2rust::CRQEIterator::into_profiled),
            max_doc_id: self.max_doc_id,
            forced_eof: self.forced_eof,
            result: self.result,
            past_end: self.past_end,
            timeout_ctx: self.timeout_ctx,
        }
    }
}

impl<'index, I, TC> ProfilePrint for Not<'index, I, TC>
where
    I: RQEIterator<'index> + ProfilePrint,
    TC: TimeoutContext,
{
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        ctx.print_single_child(c"NOT", self.child(), map);
    }
}

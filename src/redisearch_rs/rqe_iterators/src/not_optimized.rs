/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types for [`NotOptimized`].

use index_result::{RSIndexResult, RSResultKind, RawIndexResult};
use ref_mode::{Active, Ref, Suspended};

use crate::{
    IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    RQEValidateStatus, ResumeOutcome, SkipToOutcome, WildcardIterator,
    boxed::suspend_child_slot_in_place,
    maybe_empty::MaybeEmpty,
    profile_print::{ProfilePrint, ProfilePrintCtx},
    utils::TimeoutContext,
};
use index_spec::IndexSpecReadGuard;
use rqe_core::{DocId, RS_FIELDMASK_ALL};

/// An optimized NOT iterator that uses a wildcard inverted index iterator.
///
/// Parameterised over a [`Ref`] mode — see [`NotOptimized`] for the [`Active`]
/// instantiation that implements [`RQEIterator`].
///
/// Unlike [`Not`](super::not::Not) which iterates sequentially from 1 to
/// `max_doc_id`, this variant uses a
/// [wildcard iterator](crate::wildcard) that reads from the existing-documents inverted
/// index. It yields all documents present in the wildcard iterator that
/// are **not** present in the child iterator.
///
/// This is applicable when the index has an `existingDocs` inverted index
/// (i.e. `index_all` is enabled), providing better performance by only
/// visiting documents that actually exist.
///
/// # Type Parameters
///
/// * `Rf` - The [`Ref`] mode.
/// * `W` - The wildcard iterator type, must implement [`WildcardIterator`].
/// * `I` - The child iterator type whose results are negated.
/// * `TC` - The [`TimeoutContext`] implementation. Chosen at construction
///   time and monomorphized into the hot path.
#[repr(C)]
pub struct RawNotOptimized<'query, Rf: Ref, W, I, TC> {
    /// The wildcard iterator over all existing documents.
    wcii: W,
    /// The child iterator whose results are negated.
    child: MaybeEmpty<I>,
    /// The maximum document ID (used as upper bound guard).
    max_doc_id: DocId,
    /// Sticky EOF flag, set when iteration completes.
    forced_eof: bool,
    /// A reusable result object to avoid allocations on each [`read`](RQEIterator::read) call.
    result: RawIndexResult<'query, Rf>,
    /// Whether the iterator has run *past* its last result, i.e. a
    /// `read`/`skip_to` found nothing.
    ///
    /// The state behind [`current`](RQEIterator::current) and
    /// [`at_eof`](RQEIterator::at_eof). It cannot be folded into `result.doc_id`:
    /// that field *is* [`last_doc_id`](RQEIterator::last_doc_id).
    ///
    /// Unlike [`Self::forced_eof`] this is not sticky; see
    /// [`read`](RQEIterator::read) for why.
    past_end: bool,
    /// Tracks the execution deadline for this iterator. Pass
    /// [`NoTimeoutChecker`](timeout::NoTimeoutChecker) to opt out of timeout checks
    /// entirely; monomorphization collapses the no-op context to dead code.
    timeout_ctx: TC,
}

/// Alias for an [`Active`] [`RawNotOptimized`] — the only instantiation
/// with an [`RQEIterator`] impl today.
pub type NotOptimized<'index, W, I, TC> = RawNotOptimized<'index, Active<'index>, W, I, TC>;

// Compile-time proof of invariant 1 on `RawNotOptimized`: for representative
// concrete `wcii`/`child` types, the `Active` and `Suspended` instantiations are
// layout-identical. The child slots' own compatibility is their invariant 1
// (enforced generically by `suspend_child_slot_in_place`); `result` is
// layout-compatible across `Rf` (proven in `index_result`); the remaining fields
// are `Rf`-free.
const _: () = {
    use crate::Wildcard;
    use crate::utils::NoTimeoutChecker;
    use std::mem::{align_of, offset_of, size_of};
    type AChild = Wildcard<'static>;
    type SChild = <Wildcard<'static> as RQEIteratorBoxed<'static>>::Suspended;
    type A = RawNotOptimized<'static, Active<'static>, AChild, AChild, NoTimeoutChecker>;
    type S = RawNotOptimized<'static, Suspended, SChild, SChild, NoTimeoutChecker>;
    assert!(offset_of!(A, wcii) == offset_of!(S, wcii));
    assert!(offset_of!(A, child) == offset_of!(S, child));
    assert!(offset_of!(A, max_doc_id) == offset_of!(S, max_doc_id));
    assert!(offset_of!(A, forced_eof) == offset_of!(S, forced_eof));
    assert!(offset_of!(A, result) == offset_of!(S, result));
    assert!(offset_of!(A, past_end) == offset_of!(S, past_end));
    assert!(offset_of!(A, timeout_ctx) == offset_of!(S, timeout_ctx));
    assert!(size_of::<A>() == size_of::<S>());
    assert!(align_of::<A>() == align_of::<S>());
};

impl<'index, W, I, TC> NotOptimized<'index, W, I, TC>
where
    W: WildcardIterator<'index>,
    I: RQEIterator<'index>,
    TC: TimeoutContext,
{
    /// Create a new optimized NOT iterator.
    ///
    /// `wcii` is the wildcard iterator over all existing documents.
    /// `child` is the iterator whose documents will be excluded.
    /// `max_doc_id` is the upper bound for document IDs.
    /// `weight` is the score weight applied to every returned result.
    /// `timeout_ctx` is the [`TimeoutContext`] implementation to use; pass
    /// [`NoTimeoutChecker`](timeout::NoTimeoutChecker) to disable timeout checks
    /// entirely.
    pub fn new(wcii: W, child: I, max_doc_id: DocId, weight: f64, timeout_ctx: TC) -> Self {
        Self {
            wcii,
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
}

impl<'index, W, I, TC> NotOptimized<'index, W, I, TC>
where
    // Helpers below only call generic `RQEIterator` methods on the wildcard
    // base, so they don't need the `WildcardIterator` marker (enforced in `new`).
    W: RQEIterator<'index>,
    I: RQEIterator<'index>,
    TC: TimeoutContext,
{
    /// Wrapper around [`TimeoutContext::check_timeout`].
    #[inline(always)]
    fn check_timeout(&mut self) -> Result<(), RQEIteratorError> {
        self.timeout_ctx.check_timeout()
    }

    /// Advance the wildcard iterator and set [`forced_eof`](Self::forced_eof)
    /// if it is exhausted.
    ///
    /// Returns `Ok(true)` if the wildcard iterator produced a new document,
    /// `Ok(false)` if it reached EOF.
    #[inline(always)]
    fn advance_wcii_or_eof(&mut self) -> Result<bool, RQEIteratorError> {
        if self.wcii.read()?.is_none() {
            self.forced_eof = true;
            return Ok(false);
        }
        Ok(true)
    }

    /// Get a shared reference to the _child_ iterator.
    pub const fn child(&self) -> Option<&I> {
        self.child.as_ref()
    }

    /// Whether there is another result to yield: iteration has not completed and
    /// the wildcard has not run out.
    ///
    /// Goes `false` one step before [`Self::past_end`] is set, while the final
    /// result is still current.
    #[inline(always)]
    const fn has_next(&self) -> bool {
        !self.forced_eof && self.result.doc_id < self.max_doc_id
    }

    /// Check whether the child iterator is positionally past `doc_id`
    /// (already advanced beyond it) or fully exhausted, meaning `doc_id`
    /// cannot be in the child without performing additional reads.
    ///
    /// "Exhausted" is the trailing state, so a child still sitting on its last
    /// result does not qualify: the first probe past that result reads the child
    /// once more instead of short-circuiting here, gets the same answer, and every
    /// later probe takes this path again. The look-ahead that would have caught it
    /// a step earlier is each iterator's own business now, and is not on the trait,
    /// so this cannot be tightened from here.
    #[inline(always)]
    fn child_is_ahead_or_depleted(&self, doc_id: DocId) -> bool {
        doc_id < self.child.last_doc_id()
            || (self.child.at_eof() && doc_id > self.child.last_doc_id())
    }

    /// Internal read logic shared by [`read`](RQEIterator::read) and
    /// [`skip_to`](RQEIterator::skip_to).
    ///
    /// Returns `Ok(true)` if a valid result was found (stored in
    /// `self.result.doc_id`), `Ok(false)` if EOF was reached.
    fn read_inner(&mut self) -> Result<bool, RQEIteratorError> {
        if !self.has_next() {
            self.forced_eof = true;
            return Ok(false);
        }

        // Advance the wildcard iterator to the next document.
        if !self.advance_wcii_or_eof()? {
            return Ok(false);
        }

        loop {
            let wcii_last = self.wcii.last_doc_id();

            // The wildcard can land beyond `max_doc_id` in a single step — a sparse
            // existing-documents index, or documents added since the bound was
            // captured when the plan was built. Everything it has left is then
            // outside the range this iterator covers, so there is nothing more to
            // yield: the same answer [`skip_to`](RQEIterator::skip_to) gives for a
            // target past the bound, and sticky for the same reason.
            //
            // Checked here rather than beside the assignment below, because Case 2
            // re-advances the wildcard and loops back round.
            if wcii_last > self.max_doc_id {
                self.forced_eof = true;
                return Ok(false);
            }

            if self.child_is_ahead_or_depleted(wcii_last) {
                // Case 1: The wildcard document is not in the child.
                self.result.doc_id = wcii_last;
                return Ok(true);
            } else if wcii_last == self.child.last_doc_id() {
                // Case 2: Both iterators at the same position, advance both.
                self.child.read()?;
                if !self.advance_wcii_or_eof()? {
                    return Ok(false);
                }
            } else {
                // Case 3: Child is behind, read it forward to catch up.
                //
                // We use a read loop rather than `skip_to` because the
                // child almost always needs only a single read to reach
                // or pass `wcii_last`. The only scenario where the child
                // lags behind is when GC has removed a doc ID from the
                // wildcard inverted index but not yet from the child's
                // index — and even then the gap is typically tiny.
                // `read` is cheaper than `skip_to`, so the loop is
                // faster in the common case.
                while !self.child.at_eof() && self.child.last_doc_id() < wcii_last {
                    self.child.read()?;
                }
            }
            self.check_timeout()?;
        }
    }
}

impl<'index, W, I, TC> RQEIterator<'index> for NotOptimized<'index, W, I, TC>
where
    // Marker enforced at construction (`new`); only generic methods used here.
    W: RQEIterator<'index>,
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
        let found = self.read_inner()?;
        self.past_end = !found;

        if found {
            Ok(Some(&mut self.result))
        } else {
            Ok(None)
        }
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        debug_assert!(self.last_doc_id() < doc_id);

        if !self.has_next() {
            self.past_end = true;
            return Ok(None);
        }
        if doc_id > self.max_doc_id {
            self.forced_eof = true;
            self.past_end = true;
            return Ok(None);
        }

        // Skip wcii to docId.
        if self.wcii.skip_to(doc_id)?.is_none() {
            self.forced_eof = true;
            self.past_end = true;
            return Ok(None);
        }

        let wcii_last = self.wcii.last_doc_id();

        // The target was checked against `max_doc_id` above, but the wildcard
        // answers with the next document it *has*, which can be well past it over a
        // sparse stretch. That landing is outside the range this iterator covers, so
        // there is nothing left to yield — the same answer the read loop gives.
        // Checked before the child is synced to it, and before it is published as
        // this iterator's position.
        if wcii_last > self.max_doc_id {
            self.forced_eof = true;
            self.past_end = true;
            return Ok(None);
        }

        // If child is behind wcii, advance it to catch up.
        if !self.child.at_eof() && self.child.last_doc_id() < wcii_last {
            self.child.skip_to(wcii_last)?;
        }

        // If child landed at the same position, the document is in the
        // child. Advance to find the next valid NOT result.
        if self.child.last_doc_id() == wcii_last {
            let found = self.read_inner()?;
            self.past_end = !found;

            if found {
                return Ok(Some(SkipToOutcome::NotFound(&mut self.result)));
            } else {
                return Ok(None);
            }
        }

        // Child is ahead or depleted: wcii_last is a valid result.
        self.past_end = false;
        self.result.doc_id = wcii_last;
        if self.result.doc_id == doc_id {
            Ok(Some(SkipToOutcome::Found(&mut self.result)))
        } else {
            Ok(Some(SkipToOutcome::NotFound(&mut self.result)))
        }
    }

    #[inline(always)]
    fn rewind(&mut self) {
        self.forced_eof = false;
        self.past_end = false;
        self.result.doc_id = 0;
        self.wcii.rewind();
        self.child.rewind();
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        self.wcii.num_estimated()
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
        // 1. Revalidate the wildcard iterator first.
        let wcii_status = self.wcii.revalidate(spec)?;
        if matches!(wcii_status, RQEValidateStatus::Aborted) {
            return Ok(RQEValidateStatus::Aborted);
        }

        // 2. Revalidate the child iterator.
        let child_aborted = matches!(self.child.revalidate(spec)?, RQEValidateStatus::Aborted);
        if child_aborted {
            // When child is aborted, NOT becomes "NOT nothing" = everything
            // from the wildcard iterator.
            self.child = MaybeEmpty::new_empty();
        }

        // 3. If the wildcard moved, sync state.
        if matches!(wcii_status, RQEValidateStatus::Moved { .. }) {
            // Sync the EOF flag with the wildcard iterator. This clears a
            // previously-set forced_eof so the iterator can recover.
            self.forced_eof = self.wcii.at_eof();
            // Track whether we land on a valid NOT result. Starts true
            // when wcii is not at EOF (we have a candidate position).
            //
            // A wildcard that revalidated onto a document past `max_doc_id` has
            // nothing left inside this iterator's range, so it is not a candidate
            // — the third place the bound has to be applied, alongside the read
            // loop and `skip_to`, because each publishes a wildcard position of
            // its own. Without it, a concurrent index change could hand a native
            // parent `Moved { current: Some(_) }` with an out-of-range id.
            let mut have_valid_pos = !self.forced_eof && self.wcii.last_doc_id() <= self.max_doc_id;
            if have_valid_pos {
                self.result.doc_id = self.wcii.last_doc_id();

                // If child is behind, skip it forward — the only thing that makes
                // the membership test below mean anything.
                //
                // A failure here must not be swallowed. The contract forbids a
                // skip that carries no result from leaving the child's position
                // *on* the target, exactly so a parent cannot mistake it for a
                // hit — so the test below would read the failure as "not in the
                // child" and publish a document this iterator exists to exclude.
                // Undecided is not the same as absent.
                if self.child.last_doc_id() < self.result.doc_id {
                    let _ = self.child.skip_to(self.result.doc_id)?;
                }

                // If child landed on the same position, the current
                // result is in the child and invalid for NOT. Advance to
                // the next valid position.
                if self.child.last_doc_id() == self.result.doc_id {
                    // A failing scan leaves nothing to report, and neither answer
                    // available here would be true: `Moved { current: None }` says
                    // exhausted, which every composite acts on — `Intersection`
                    // ends, `OptionalOptimized` latches `past_end`, both unions drop
                    // the child — so a later `read` finding a document would be
                    // resurrecting past a parent that has already written this
                    // iterator off. The error goes to the caller instead, which
                    // aborts the iterator rather than trusting a position that does
                    // not exist. Same trade `UnionFlat` makes when catching up a
                    // lagging child fails.
                    have_valid_pos = self.read_inner()?;
                }
            }

            // Keep the has-current state in step with what we are about to
            // report: a `Moved { current: None }` must leave `current()` — and
            // `at_eof()`, its negation — agreeing with it, rather than handing
            // back the stale pre-revalidation result. Landing on a valid position
            // clears the flag, mirroring the `forced_eof` recovery above.
            self.past_end = !have_valid_pos;

            Ok(RQEValidateStatus::Moved {
                current: if have_valid_pos {
                    Some(&mut self.result)
                } else {
                    None
                },
            })
        } else {
            Ok(RQEValidateStatus::Ok)
        }
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        IteratorType::NotOptimized
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}

impl<'index, W, I, TC> RQEIteratorBoxed<'index> for NotOptimized<'index, W, I, TC>
where
    // Marker enforced at construction (`new`), not on the suspend/resume path.
    W: RQEIteratorBoxed<'index>,
    I: RQEIteratorBoxed<'index>,
    TC: TimeoutContext + 'index + 'static,
{
    type Suspended = RawNotOptimized<'index, Suspended, W::Suspended, I::Suspended, TC>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // Dispatch each sub-iterator's own `suspend` in place. A whole-box cast
        // alone is *not* enough: for a type-erased `wcii`/`child` the active and
        // suspended forms carry different `dyn` vtables, so the transition must
        // be dispatched through the child's own `suspend` (a no-op cast for
        // concrete children, a vtable swap for erased ones). `child` is a
        // `MaybeEmpty<I>`, itself an `RQEIteratorBoxed` that walks its own
        // `Some(I)` arm.
        //
        // SAFETY: `raw` came from `Box::into_raw` (non-null, aligned, initialised,
        // exclusively owned); `&raw mut` forms a field pointer to `wcii`.
        let wcii = unsafe { &raw mut (*raw).wcii };
        // SAFETY: `wcii` points at a valid, owned `W`; the helper dispatches its
        // `suspend` and reinitialises the slot as a valid `W::Suspended`.
        unsafe { suspend_child_slot_in_place(wcii) };
        // SAFETY: `&raw mut` forms a field pointer to `child`.
        let child = unsafe { &raw mut (*raw).child };
        // SAFETY: `child` points at a valid, owned `MaybeEmpty<I>`; the helper
        // dispatches its `suspend` and reinitialises the slot as a valid
        // `MaybeEmpty<I::Suspended>`.
        unsafe { suspend_child_slot_in_place(child) };
        // SAFETY: both sub-iterators now hold their `Suspended` forms;
        // `result: RawIndexResult<Rf>` is layout-compatible across `Rf`, and the
        // remaining fields are `Rf`-free — so the allocation is a valid suspended
        // `RawNotOptimized`, layout-identical to the active form by invariant 1
        // (const proof above). `Box::from_raw` reuses the same heap allocation.
        unsafe {
            Box::from_raw(
                raw as *mut RawNotOptimized<'index, Suspended, W::Suspended, I::Suspended, TC>,
            )
        }
    }
}

/// RAII guard used by [`RawNotOptimized`]'s `resume` while its two child slots
/// are moved out into owned locals. It owns the leftover shell: on drop (an
/// early return or a panic) it drops every field the shell still owns — the
/// still-suspended `result` and the `timeout_ctx` — and frees the allocation. It
/// never touches the moved-out `wcii`/`child` slots, so its behaviour is
/// independent of how far the resume got. It is disarmed with
/// [`std::mem::forget`] once the resumed children are written back.
struct FreeSuspendedShell<'q, WS, IS, TC> {
    raw: *mut RawNotOptimized<'q, Suspended, WS, IS, TC>,
}

impl<'q, WS, IS, TC> Drop for FreeSuspendedShell<'q, WS, IS, TC> {
    fn drop(&mut self) {
        // SAFETY: `raw` is a valid, exclusively-owned allocation; `&raw mut`
        // forms a field pointer to the still-suspended `result`.
        let result = unsafe { &raw mut (*self.raw).result };
        // SAFETY: `result` is a valid, owned suspended result; drop it in place.
        unsafe { std::ptr::drop_in_place(result) };
        // `TC` is an unconstrained generic, so it may carry drop glue of its own
        // even though no shipped [`TimeoutContext`] does today. Every other field
        // besides `wcii`/`child` is a scalar.
        //
        // SAFETY: `&raw mut` forms a field pointer to the owned `timeout_ctx`.
        let timeout_ctx = unsafe { &raw mut (*self.raw).timeout_ctx };
        // SAFETY: `timeout_ctx` is a valid, owned `TC`; drop it in place.
        unsafe { std::ptr::drop_in_place(timeout_ctx) };
        // SAFETY: `raw` was allocated by `Box` with exactly this layout, and every
        // field that owns anything has now been dropped except the moved-from
        // `wcii`/`child` slots — the only ones that must *not* be. Free it.
        unsafe {
            std::alloc::dealloc(
                self.raw.cast::<u8>(),
                std::alloc::Layout::new::<RawNotOptimized<'q, Suspended, WS, IS, TC>>(),
            )
        };
    }
}

impl<'query, WS, IS, TC> RQESuspendedIterator<'query>
    for RawNotOptimized<'query, Suspended, WS, IS, TC>
where
    WS: RQESuspendedIterator<'query>,
    IS: RQESuspendedIterator<'query>,
    TC: TimeoutContext + 'static,
{
    type Resumed<'a>
        = NotOptimized<'a, WS::Resumed<'a>, IS::Resumed<'a>, TC>
    where
        'query: 'a;

    fn resume<'a>(
        self: Box<Self>,
        guard: &IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        // `result` must still be a virtual sentinel. It is handed out mutably via
        // `current()`/`read()`/`skip_to`, and on resume its `Suspended → Active`
        // reinterpretation would assert index borrows this iterator cannot
        // re-validate. `kind() == Virtual` is the whole condition (`data` is the
        // only `Rf`-parametrized field). Checked safely via `&self`, before
        // `Box::into_raw`, so a violation just drops `self`; recoverable, hence
        // `Aborted` rather than `Err`.
        if self.result.kind() != RSResultKind::Virtual {
            return Ok(ResumeOutcome::Aborted);
        }

        let raw = Box::into_raw(self);

        // Move both children out into owned locals; their slots become
        // uninitialised. `result` stays in place (it must keep its address — it
        // is handed out via `current()`/`read()`/`skip_to`), as do the `Rf`-free
        // scalar fields.
        //
        // SAFETY: `raw` came from `Box::into_raw` (non-null, aligned, initialised,
        // exclusively owned); `&raw const` forms a field pointer to `wcii`.
        let wcii_ptr = unsafe { &raw const (*raw).wcii };
        // SAFETY: move the owned suspended `wcii` out exactly once.
        let wcii = unsafe { std::ptr::read(wcii_ptr) };
        // SAFETY: field pointer to `child`.
        let child_ptr = unsafe { &raw const (*raw).child };
        // SAFETY: move the owned suspended `child` out exactly once.
        let child = unsafe { std::ptr::read(child_ptr) };

        // From here until success, `raw` is a shell whose child slots are
        // moved-out. [`FreeSuspendedShell`] frees it on any early return or
        // panic; the `wcii`/`child` locals clean themselves up via ownership.
        let shell = FreeSuspendedShell { raw };

        // Resume the wildcard base and act on its outcome *before* the child is
        // touched, as `revalidate` does: if the base is unrecoverable there is
        // nothing left to enumerate the complement over, so the whole iterator
        // aborts and the child is never driven at all. The still-suspended
        // `child` local drops by ownership on that early return. (`wcii` being a
        // wildcard is enforced at construction, not by the bounds here — see the
        // [`RQEIteratorBoxed`] impl above.)
        let (wcii, wcii_moved) = match Box::new(wcii).resume(guard)? {
            ResumeOutcome::Aborted => return Ok(ResumeOutcome::Aborted),
            ResumeOutcome::Ok(w) => (w, false),
            ResumeOutcome::Moved(w) => (w, true),
        };
        // An aborted query child collapses to `Empty` (mirroring the legacy
        // behaviour); a moved child doesn't shift NOT's cursor.
        let child: Box<MaybeEmpty<IS::Resumed<'a>>> = match Box::new(child).resume(guard)? {
            ResumeOutcome::Aborted => Box::new(MaybeEmpty::new_empty()),
            ResumeOutcome::Ok(c) | ResumeOutcome::Moved(c) => c,
        };

        // Success: disarm the guard and write the resumed children back into
        // their original slots (preserving their addresses — and `result`'s,
        // which never moved), then reinterpret the reused allocation as the
        // active form. Everything between this `forget` and the two writes below
        // is moves, pointer arithmetic and `const` assertions — none of it can
        // unwind, so no unwind can observe the uninitialised slots.
        std::mem::forget(shell);
        // Statically enforce the size/alignment invariants the in-place writes
        // below rely on, mirroring the guard inside the shared slot helpers. The
        // whole-struct pair is asserted too: `FreeSuspendedShell` deallocates at
        // the *suspended* layout and the cast below reclaims the same allocation
        // at the *resumed* one, so a per-slot match alone is not enough. The
        // standalone `const _` proof above covers only representative concrete
        // children; this fires at the generics actually instantiated.
        const { crate::boxed::assert_layout_compatible::<Self, Self::Resumed<'a>>() };
        const { crate::boxed::assert_layout_compatible::<WS, WS::Resumed<'a>>() };
        const { crate::boxed::assert_layout_compatible::<MaybeEmpty<IS>, MaybeEmpty<IS::Resumed<'a>>>() };
        // SAFETY: the `wcii` slot is uninitialised; `&raw mut` forms a field
        // pointer to it.
        let wcii_slot = unsafe { &raw mut (*raw).wcii };
        // SAFETY: write the resumed `wcii` back at the same offset, as its
        // resumed type.
        unsafe { std::ptr::write(wcii_slot.cast::<WS::Resumed<'a>>(), *wcii) };
        // SAFETY: `&raw mut` forms a field pointer to the uninitialised `child`
        // slot.
        let child_slot = unsafe { &raw mut (*raw).child };
        // SAFETY: write the resumed `child` back at the same offset.
        unsafe { std::ptr::write(child_slot.cast::<MaybeEmpty<IS::Resumed<'a>>>(), *child) };

        // SAFETY: both slots now hold their resumed forms and `result` is a valid
        // (pointerless) virtual sentinel (checked above), so the allocation is a
        // valid `NotOptimized<'a, …>` — layout-identical to the suspended form by
        // invariant 1 on `RawNotOptimized` (const proof above), with `result`
        // re-typing `Suspended → Active` soundly. `Box::from_raw` reuses the same
        // allocation, so the FFI's cached `header.current` and any parent's
        // pointer into `result` stay valid across the cycle.
        let mut active = unsafe {
            Box::from_raw(raw.cast::<NotOptimized<'a, WS::Resumed<'a>, IS::Resumed<'a>, TC>>())
        };

        if wcii_moved {
            active.forced_eof = active.wcii.at_eof();
            // Bounded, as in `revalidate`: a wildcard that came back on a document
            // beyond `max_doc_id` is out of this iterator's range, so there is no
            // valid position to publish.
            let mut have_valid_pos =
                !active.forced_eof && active.wcii.last_doc_id() <= active.max_doc_id;
            if have_valid_pos {
                active.result.doc_id = active.wcii.last_doc_id();
                // Catching the child up is the only thing that makes the
                // membership test below mean anything, so a failure here must
                // reach the caller rather than be read as "not in the child" —
                // see `revalidate`, which spells out why undecided is not absent.
                if active.child.last_doc_id() < active.result.doc_id {
                    let _ = active.child.skip_to(active.result.doc_id)?;
                }
                if active.child.last_doc_id() == active.result.doc_id {
                    // Likewise for a failing scan: neither outcome available here
                    // would be true, and reporting `Moved` with no position says
                    // "exhausted" to a parent that acts on it. See `revalidate`.
                    have_valid_pos = active.read_inner()?;
                }
            }
            // Mirrors `revalidate`: no valid position means the iterator has run past
            // its end, and a recovered one clears the flag. Discarding this (as this
            // path used to) left `current()`/`at_eof()` disagreeing with the outcome
            // just reported.
            active.past_end = !have_valid_pos;
            return Ok(ResumeOutcome::Moved(active));
        }

        Ok(ResumeOutcome::Ok(active))
    }

    fn last_doc_id(&self) -> DocId {
        self.result.doc_id
    }

    fn num_estimated(&self) -> usize {
        // Mirrors the active `num_estimated`, which delegates to the wildcard base.
        self.wcii.num_estimated()
    }
}

impl<'index, W, TC> crate::interop::ProfileChildren<'index>
    for NotOptimized<'index, W, crate::c2rust::CRQEIterator, TC>
where
    W: crate::WildcardIterator<'index> + 'index,
    TC: TimeoutContext + 'index,
{
    fn profile_children(self) -> Self {
        NotOptimized {
            wcii: self.wcii,
            child: self.child.map(crate::c2rust::CRQEIterator::into_profiled),
            max_doc_id: self.max_doc_id,
            forced_eof: self.forced_eof,
            result: self.result,
            past_end: self.past_end,
            timeout_ctx: self.timeout_ctx,
        }
    }
}

impl<'index, W, I, TC> ProfilePrint for NotOptimized<'index, W, I, TC>
where
    W: crate::WildcardIterator<'index>,
    I: RQEIterator<'index> + ProfilePrint,
    TC: TimeoutContext,
{
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        ctx.print_single_child(c"NOT", self.child(), map);
    }
}

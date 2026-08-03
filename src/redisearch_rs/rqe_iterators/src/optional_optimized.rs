/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types for [`OptionalOptimized`].
//!
//! This is the optimized variant of the optional iterator. Instead of scanning
//! all doc IDs from 1 to `maxDocId`, it uses a [wildcard iterator](crate::wildcard) over
//! `spec.existingDocs` to visit only real document IDs, yielding real or virtual
//! results accordingly.

use index_result::{RSIndexResult, RSResultKind, RawIndexResult};
use ref_mode::{Active, Ref, Suspended};

use crate::{
    RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator, RQEValidateStatus,
    ResumeOutcome, SkipToOutcome,
    boxed::suspend_child_slot_in_place,
    maybe_empty::MaybeEmpty,
    profile_print::{ProfilePrint, ProfilePrintCtx},
    wildcard::WildcardIterator,
};

use index_spec::IndexSpecReadGuard;
use rqe_core::{DocId, RS_FIELDMASK_ALL};
/// An iterator that emits results for all document IDs present in the index,
/// driven by a [wildcard iterator](crate::wildcard) over the existing-documents inverted index.
///
/// Parameterised over a [`Ref`] mode — see [`OptionalOptimized`] for the
/// [`Active`] instantiation that implements [`RQEIterator`].
///
/// For each doc ID that `wcii` yields:
/// - If the query child also has a hit at that doc ID, a **real** result is
///   returned with [`OptionalOptimized::weight`] applied.
/// - Otherwise a **virtual** result is returned with zero weight.
///
/// This avoids scanning doc IDs 1..=maxDocId sequentially. When the index is
/// sparse (few documents relative to `maxDocId`), the optimized variant is
/// significantly faster.
///
/// # Invariants
///
/// 1. **Layout compatibility across modes.** `RawOptionalOptimized` is
///    `#[repr(C)]` and its only `Rf`-dependent field is
///    `virt: RawIndexResult<Rf>`, which is layout-compatible across `Rf`
///    (proven in `index_result`). Given that the wildcard base `W`/`W::Suspended`
///    and the child `I`/`I::Suspended` are layout-compatible (the
///    [`RQEIteratorBoxed`] contract — the latter through the `#[repr(C)]`
///    [`MaybeEmpty`] slot), the `Active` and `Suspended` instantiations are
///    layout-identical. This is what lets
///    [`suspend`](RQEIteratorBoxed::suspend) reinterpret the owning `Box` in
///    place.
#[repr(C)]
pub struct RawOptionalOptimized<'query, Rf: Ref, W, I> {
    /// Wildcard iterator over `spec.existingDocs` — the authoritative source of doc IDs.
    wcii: W,
    /// Query child — provides real hits at positions where it has a match.
    /// Wrapped in [`MaybeEmpty`] so it can be replaced with an empty iterator
    /// when it is aborted during [`RQEIterator::revalidate`].
    child: MaybeEmpty<I>,
    /// Virtual result returned when `wcii` has a doc but `child` does not.
    virt: RawIndexResult<'query, Rf>,
    /// Inclusive upper bound (matches C `maxDocId`).
    max_doc_id: DocId,
    /// Weight applied to real results from `child`.
    weight: f64,
    /// Tracks the doc ID of the last result yielded.
    ///
    /// `0` in the initial state and after [`rewind`](RQEIterator::rewind),
    /// which is treated as virtual. Doc IDs start from 1, so 0 is a safe sentinel.
    last_doc_id: DocId,
    /// Whether the iterator has reached EOF.
    at_eof: bool,
}

/// Alias for an [`Active`] [`RawOptionalOptimized`] — the only instantiation
/// with an [`RQEIterator`] impl today.
pub type OptionalOptimized<'index, W, I> = RawOptionalOptimized<'index, Active<'index>, W, I>;

// Compile-time proof of invariant 1 on `RawOptionalOptimized`: for representative
// concrete `wcii`/`child` types, the `Active` and `Suspended` instantiations are
// layout-identical. The `wcii`/`child` slots' own compatibility is their invariant
// 1 (enforced generically by `suspend_child_slot_in_place`); `virt` is
// layout-compatible across `Rf` (proven in `index_result`); the wrapper adds only
// `Rf`-free scalar fields.
const _: () = {
    use crate::Wildcard;
    use std::mem::{align_of, offset_of, size_of};
    type AChild = Wildcard<'static>;
    type SChild = <Wildcard<'static> as RQEIteratorBoxed<'static>>::Suspended;
    type A = RawOptionalOptimized<'static, Active<'static>, AChild, AChild>;
    type S = RawOptionalOptimized<'static, Suspended, SChild, SChild>;
    assert!(offset_of!(A, wcii) == offset_of!(S, wcii));
    assert!(offset_of!(A, child) == offset_of!(S, child));
    assert!(offset_of!(A, virt) == offset_of!(S, virt));
    assert!(offset_of!(A, max_doc_id) == offset_of!(S, max_doc_id));
    assert!(size_of::<A>() == size_of::<S>());
    assert!(align_of::<A>() == align_of::<S>());
};

impl<'index, W, I> OptionalOptimized<'index, W, I>
where
    W: WildcardIterator<'index>,
    I: RQEIterator<'index>,
{
    /// Returns a reference to the child iterator, if any.
    pub const fn child(&self) -> Option<&I> {
        self.child.as_ref()
    }

    /// Sets the child iterator.
    pub fn set_child(&mut self, child: I) {
        self.child = MaybeEmpty::new(child);
    }

    /// Creates a new [`OptionalOptimized`] iterator.
    ///
    /// * `wcii` — wildcard iterator over `spec.existingDocs`; drives which doc IDs
    ///   are visited.
    /// * `child` — query child iterator that provides real hits.
    /// * `max_doc_id` — inclusive upper bound on doc IDs.
    /// * `weight` — applied to results produced by `child`.
    pub fn new(wcii: W, child: I, max_doc_id: DocId, weight: f64) -> Self {
        Self {
            wcii,
            child: MaybeEmpty::new(child),
            virt: RSIndexResult::build_virt()
                .frequency(1)
                .field_mask(RS_FIELDMASK_ALL)
                .build(),
            max_doc_id,
            weight,
            last_doc_id: 0,
            at_eof: false,
        }
    }
}

impl<'index, W, I> OptionalOptimized<'index, W, I>
where
    I: RQEIterator<'index>,
{
    /// Settle on `doc_id`, the position `wcii` has just landed on, and report
    /// whether the child has a real hit there.
    ///
    /// Advances the child to catch up, records the new position, keeps `at_eof`
    /// consistent, and prepares the result to hand out: the child's hit with
    /// [`Self::weight`] applied when it has one at `doc_id`, otherwise the
    /// virtual sentinel.
    ///
    /// Shared by `read`, `skip_to`, `revalidate` and the resume path so this
    /// bookkeeping — in particular applying the optional weight — exists once
    /// rather than in four copies that can drift apart.
    ///
    /// `doc_id` must already be within `max_doc_id`; every caller bound-checks
    /// before settling.
    fn settle_at(&mut self, doc_id: DocId) -> Result<bool, RQEIteratorError> {
        debug_assert!(
            doc_id <= self.max_doc_id,
            "callers must bound-check against max_doc_id before settling",
        );

        // Advance the child to catch up with wcii.
        if doc_id > self.child.last_doc_id() {
            let _ = self.child.skip_to(doc_id)?;
        }

        self.last_doc_id = doc_id;
        // Look-ahead, so callers see true immediately after the last result.
        self.at_eof = doc_id >= self.max_doc_id;

        let weight = self.weight;
        // The `current()` guard mirrors `RQEIterator::current` so both agree on
        // what a real hit is: a child may report a matching `last_doc_id` while
        // having run past its own last result, in which case we fall back to the
        // virtual sentinel rather than unwrapping a `None`.
        if self.child.last_doc_id() == doc_id
            && let Some(result) = self.child.current()
        {
            // Real hit: apply the optional weight.
            result.weight = weight;
            Ok(true)
        } else {
            // Virtual hit: wcii has a doc ID but the child does not.
            self.virt.doc_id = doc_id;
            Ok(false)
        }
    }

    /// The result [`settle_at`](Self::settle_at) settled on, given the `is_real`
    /// it returned.
    fn settled_result(&mut self, is_real: bool) -> &mut RSIndexResult<'index> {
        if is_real {
            self.child
                .current()
                .expect("settle_at established a child result at this position")
        } else {
            &mut self.virt
        }
    }
}

impl<'index, W, I> RQEIterator<'index> for OptionalOptimized<'index, W, I>
where
    // Only generic `RQEIterator` methods are called on the wildcard base here;
    // the `WildcardIterator` marker is enforced at construction (see `new`).
    W: RQEIterator<'index>,
    I: RQEIterator<'index>,
{
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        if self.last_doc_id != 0
            && self.child.last_doc_id() == self.last_doc_id
            && let Some(result) = self.child.current()
        {
            return Some(result);
        }

        Some(&mut self.virt)
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.at_eof {
            return Ok(None);
        }

        // Advance wcii to the next existing document.
        let wcii_doc_id = match self.wcii.read()? {
            None => {
                self.at_eof = true;
                return Ok(None);
            }
            Some(r) => r.doc_id,
        };

        // wcii may jump past max_doc_id in a single step (e.g. sparse index).
        if wcii_doc_id > self.max_doc_id {
            self.at_eof = true;
            return Ok(None);
        }

        let is_real = self.settle_at(wcii_doc_id)?;
        Ok(Some(self.settled_result(is_real)))
    }

    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        debug_assert!(doc_id > self.last_doc_id);

        if doc_id > self.max_doc_id || self.at_eof {
            self.at_eof = true;
            return Ok(None);
        }

        // Promote wcii to doc_id. It may land on a different doc if doc_id is not
        // present in the existing-documents index.
        let (found, effective_id) = match self.wcii.skip_to(doc_id)? {
            None => {
                self.at_eof = true;
                return Ok(None);
            }
            Some(SkipToOutcome::Found(r)) => (true, r.doc_id),
            Some(SkipToOutcome::NotFound(r)) => (false, r.doc_id),
        };

        // wcii may jump past max_doc_id in a single step (e.g. sparse index).
        if effective_id > self.max_doc_id {
            self.at_eof = true;
            return Ok(None);
        }

        let is_real = self.settle_at(effective_id)?;
        let result = self.settled_result(is_real);
        // Found/NotFound mirrors wcii, for real and virtual hits alike.
        Ok(Some(if found {
            SkipToOutcome::Found(result)
        } else {
            SkipToOutcome::NotFound(result)
        }))
    }

    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        // Simple enum to avoid holding a borrow through the match.
        enum ValidateOutcome {
            Ok,
            Moved,
        }

        // Step 1: Revalidate wcii. If it aborts or is at EOF, we can return immediately.
        let wcii_outcome = match self.wcii.revalidate(spec)? {
            RQEValidateStatus::Ok => ValidateOutcome::Ok,
            RQEValidateStatus::Moved { current: Some(_) } => ValidateOutcome::Moved,
            RQEValidateStatus::Moved { current: None } => {
                self.at_eof = true;
                return Ok(RQEValidateStatus::Moved { current: None });
            }
            RQEValidateStatus::Aborted => return Ok(RQEValidateStatus::Aborted),
        };
        let wcii_at_eof = self.wcii.at_eof();
        self.at_eof = wcii_at_eof;

        // `last_doc_id` is `None` in the initial/rewound state, which is always
        // virtual.
        let current_was_virtual =
            self.last_doc_id == 0 || self.child.last_doc_id() != self.last_doc_id;

        // Step 2: Revalidate child. If it aborts, replace with an empty iterator.
        // Abort is treated as Moved: child's state changed, so we must re-evaluate.
        let child_outcome = match self.child.revalidate(spec)? {
            RQEValidateStatus::Ok => ValidateOutcome::Ok,
            RQEValidateStatus::Moved { .. } => ValidateOutcome::Moved,
            RQEValidateStatus::Aborted => {
                let _ = self.child.take_iterator(); // replace with Empty
                ValidateOutcome::Moved
            }
        };

        // Step 3: Determine the outcome based on wcii's and child's status.
        match wcii_outcome {
            ValidateOutcome::Ok => {
                if matches!(child_outcome, ValidateOutcome::Ok) || current_was_virtual {
                    // Child is still valid, or the current result was virtual — no change.
                    return Ok(RQEValidateStatus::Ok);
                }
                // Child moved or aborted while current was a real result.
                // Advance to the next valid state.
                let current = self.read()?;
                Ok(RQEValidateStatus::Moved { current })
            }
            ValidateOutcome::Moved => {
                // wcii moved to a new valid position; update child accordingly.
                let wcii_doc_id = self.wcii.last_doc_id();

                // wcii may have moved past max_doc_id.
                if wcii_doc_id > self.max_doc_id {
                    self.at_eof = true;
                    return Ok(RQEValidateStatus::Moved { current: None });
                }

                let is_real = self.settle_at(wcii_doc_id)?;
                // `settle_at` sets `at_eof` from the `max_doc_id` bound alone.
                // Unlike `read`/`skip_to`/resume, `revalidate` also keeps the
                // wildcard's own EOF (captured at step 1, above) — an exhausted
                // wcii means the next `read` yields nothing regardless of the
                // bound.
                self.at_eof |= wcii_at_eof;
                Ok(RQEValidateStatus::Moved {
                    current: Some(self.settled_result(is_real)),
                })
            }
        }
    }

    #[inline(always)]
    fn rewind(&mut self) {
        self.last_doc_id = 0;
        self.at_eof = false;
        self.virt.doc_id = 0;
        self.wcii.rewind();
        self.child.rewind();
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        self.wcii.num_estimated()
    }

    #[inline(always)]
    fn last_doc_id(&self) -> DocId {
        self.last_doc_id
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        self.at_eof
    }

    fn type_(&self) -> crate::IteratorType {
        crate::IteratorType::OptionalOptimized
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}

impl<'index, W, I> RQEIteratorBoxed<'index> for OptionalOptimized<'index, W, I>
where
    // Marker enforced at construction (`new`), not on the suspend/resume path —
    // the suspend/resume impls only move the wildcard child through the box
    // cast, so `W: RQEIteratorBoxed` suffices. This drops the recursive
    // `for<'a> …: WildcardIterator + RQEIteratorBoxed<Suspended = …>` HRTB,
    // which is otherwise unsatisfiable once `'query` narrows on resume.
    W: RQEIteratorBoxed<'index>,
    I: RQEIteratorBoxed<'index>,
{
    type Suspended = RawOptionalOptimized<'index, Suspended, W::Suspended, I::Suspended>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // Dispatch each sub-iterator's own `suspend` in place. A whole-box cast
        // alone is *not* enough: for a type-erased `wcii`/`child` the active and
        // suspended forms carry different `dyn` vtables, so the transition must be
        // dispatched through the child's own `suspend` (a no-op cast for concrete
        // children, a vtable swap for erased ones). `child` is a `MaybeEmpty<I>`,
        // itself an `RQEIteratorBoxed` that walks its own `Some(I)` arm.

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
        // SAFETY: both sub-iterators now hold their `Suspended` forms; `virt` is
        // `Rf`-dependent but layout-compatible across `Rf`, and the remaining
        // fields are `Rf`-free. So the allocation is a valid
        // `RawOptionalOptimized<Suspended, W::Suspended, I::Suspended>` —
        // layout-identical to the active form by invariant 1 (const proof above),
        // with the child slots' size/alignment matches enforced by
        // `suspend_child_slot_in_place`. `Box::from_raw` reuses the allocation.
        unsafe {
            Box::from_raw(
                raw as *mut RawOptionalOptimized<'index, Suspended, W::Suspended, I::Suspended>,
            )
        }
    }
}

/// RAII guard used by [`RawOptionalOptimized`]'s `resume` while its two child
/// slots are moved out into owned locals. It owns the leftover shell: on drop
/// (an early return or a panic) it drops the still-suspended `virt` and frees the
/// allocation — it never touches the moved-out `wcii`/`child` slots, so its
/// behaviour is independent of how far the resume got. It is disarmed with
/// [`std::mem::forget`] once the resumed children are written back.
struct FreeSuspendedShell<'q, WS, IS> {
    raw: *mut RawOptionalOptimized<'q, Suspended, WS, IS>,
}

impl<'q, WS, IS> Drop for FreeSuspendedShell<'q, WS, IS> {
    fn drop(&mut self) {
        // SAFETY: `raw` is a valid, exclusively-owned allocation; `&raw mut`
        // forms a field pointer to the still-suspended `virt`.
        let virt = unsafe { &raw mut (*self.raw).virt };
        // SAFETY: `virt` is a valid, owned suspended result; drop it in place.
        unsafe { std::ptr::drop_in_place(virt) };
        // SAFETY: `raw` was allocated by `Box` with exactly this layout; the
        // `wcii`/`child` slots are moved-from and must not be dropped. Free it.
        unsafe {
            std::alloc::dealloc(
                self.raw.cast::<u8>(),
                std::alloc::Layout::new::<RawOptionalOptimized<'q, Suspended, WS, IS>>(),
            )
        };
    }
}

impl<'query, WS, IS> RQESuspendedIterator<'query>
    for RawOptionalOptimized<'query, Suspended, WS, IS>
where
    WS: RQESuspendedIterator<'query>,
    IS: RQESuspendedIterator<'query>,
{
    type Resumed<'a>
        = OptionalOptimized<'a, WS::Resumed<'a>, IS::Resumed<'a>>
    where
        'query: 'a;

    fn resume<'a>(
        self: Box<Self>,
        guard: &IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        // `virt` must still be a virtual sentinel. It is handed out mutably via
        // `current()`/`read()`/`skip_to`, and on resume its `Suspended → Active`
        // reinterpretation would assert index borrows we cannot re-validate. Only
        // `data` is `Rf`-parametrized, so `kind() == Virtual` is the whole
        // condition (`dmd`/`metrics` are not touched by the reinterpretation).
        // Checked safely via `&self`, before `Box::into_raw`, so a violation just
        // drops `self`; we abort the resume rather than risk UB.
        if self.virt.kind() != RSResultKind::Virtual {
            return Ok(ResumeOutcome::Aborted);
        }

        // Capture pre-resume positions for the post-resume logic below (both reads
        // happen before `self` is consumed by `Box::into_raw`). The `wcii` (base)
        // child is genuinely a wildcard; its resumed type is a `WildcardIterator`.
        let pre_wcii_last_doc_id = RQESuspendedIterator::last_doc_id(&self.wcii);
        let pre_child_last_doc_id = RQESuspendedIterator::last_doc_id(&self.child);

        let raw = Box::into_raw(self);

        // Move both children out into owned locals; their slots become
        // uninitialised. `virt` stays in place (it must keep its address — it is
        // handed out via `current()`/`read()`/`skip_to`), as do the `Rf`-free
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
        // moved-out. This guard frees it — dropping the still-suspended `virt` and
        // the allocation, but never the moved-out slots — on any early return or
        // panic. The `wcii`/`child` locals clean themselves up via ownership, so
        // there is no manual teardown and no uninitialised-slot window.
        let shell = FreeSuspendedShell { raw };

        // Resume both children up front. This differs from `revalidate`, which
        // drives the child only *after* the `wcii` outcome and skips it entirely
        // on a `wcii` abort or move-to-EOF; resuming both here keeps the
        // move-out/write-back teardown uniform and is benign — a `wcii` abort
        // still wins (its outcome is consumed first, below), and the child is
        // simply dropped in that case. They are owned values, so `?` / early
        // return drop them (and, via `shell`, `virt` + the allocation) with zero
        // manual teardown.
        let wcii_out = Box::new(wcii).resume(guard);
        let child_out = Box::new(child).resume(guard);

        // `wcii` is the base: if it is unrecoverable there is nothing to
        // enumerate, so the whole iterator aborts.
        let (wcii, wcii_moved) = match wcii_out? {
            ResumeOutcome::Aborted => return Ok(ResumeOutcome::Aborted),
            ResumeOutcome::Ok(w) => (w, false),
            ResumeOutcome::Moved(w) => (w, true),
        };
        // An aborted query child is replaced by `Empty` (mirroring `revalidate`'s
        // `take_iterator`) and treated as "moved".
        let (child, child_moved_or_aborted) = match child_out? {
            ResumeOutcome::Aborted => (Box::new(MaybeEmpty::new_empty()), true),
            ResumeOutcome::Ok(c) => (c, false),
            ResumeOutcome::Moved(c) => (c, true),
        };

        // Success: disarm the guard and write the resumed children back into their
        // original slots (preserving their addresses — and `virt`'s, which never
        // moved), then reinterpret the reused allocation as the active form.
        std::mem::forget(shell);
        // Statically enforce the size/alignment invariant the in-place writes
        // below rely on. `WildcardIterator` and the child are public safe traits,
        // so a custom impl whose resumed type differs in layout would corrupt the
        // slot; a mismatch fails to compile here, mirroring the guard inside
        // `resume_child_slot_in_place`/`suspend_child_slot_in_place`.
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

        // SAFETY: both slots now hold their resumed forms and `virt` is a valid
        // (pointerless) virtual sentinel (checked above), so the allocation is a
        // valid `OptionalOptimized<'a, …>` — layout-identical to the suspended
        // form by invariant 1 on `RawOptionalOptimized` (const proof above), with
        // `virt` re-typing `Suspended → Active` soundly. `Box::from_raw` reuses
        // the same allocation.
        let mut active = unsafe {
            Box::from_raw(raw.cast::<OptionalOptimized<'a, WS::Resumed<'a>, IS::Resumed<'a>>>())
        };

        // Distinguish "wcii moved to a new valid position" from "wcii moved
        // past all docs (no new current)". `ResumeOutcome::Moved` doesn't
        // carry the {Some, None} distinction that legacy `revalidate` had, so
        // we recover it by comparing wcii's pre/post `last_doc_id`: a true
        // "Moved to EOF without a new doc" leaves the cached doc_id unchanged
        // (the wcii has nothing new to surface). A "Moved to a new valid doc"
        // advances the cached doc_id even if `at_eof` is now true (because
        // that new doc is the LAST valid one).
        if wcii_moved && active.wcii.at_eof() && active.wcii.last_doc_id() == pre_wcii_last_doc_id {
            active.at_eof = true;
            return Ok(ResumeOutcome::Moved(active));
        }
        // NB: do NOT reset `at_eof` from `wcii.at_eof()` here. For an unchanged
        // wcii we keep the pre-suspend flag, so a bound-EOF from an earlier
        // `skip_to(max_doc_id + 1)` survives resume; for a moved wcii it is set
        // explicitly below from `wcii_doc_id >= max_doc_id`, matching `read`/`skip_to`.

        let current_was_virtual =
            active.last_doc_id == 0 || pre_child_last_doc_id != active.last_doc_id;

        if !wcii_moved {
            // wcii stayed at the same position.
            if !child_moved_or_aborted || current_was_virtual {
                // Child is still valid, or the current result was virtual — no change.
                return Ok(ResumeOutcome::Ok(active));
            }
            // Child moved or aborted while the current was a real result:
            // advance to the next valid state.
            active.read()?;
            return Ok(ResumeOutcome::Moved(active));
        }

        // wcii moved to a new valid position; update the child accordingly.
        let wcii_doc_id = active.wcii.last_doc_id();
        if wcii_doc_id > active.max_doc_id {
            active.at_eof = true;
            return Ok(ResumeOutcome::Moved(active));
        }
        // Settle exactly as `read`/`skip_to` do — including applying the optional
        // weight to a moved-to real hit, which this path used to omit. `at_eof`
        // comes from the `max_doc_id` bound alone (absolute, not OR'd with the
        // stale pre-suspend flag); `wcii_doc_id <= max_doc_id` here, so it is EOF
        // only at exactly the bound, leaving a valid in-bound hit visible.
        let _ = active.settle_at(wcii_doc_id)?;
        Ok(ResumeOutcome::Moved(active))
    }

    fn last_doc_id(&self) -> DocId {
        self.last_doc_id
    }

    fn num_estimated(&self) -> usize {
        // Mirrors the active `num_estimated`, which delegates to the wildcard base.
        self.wcii.num_estimated()
    }
}

impl<'index, W: WildcardIterator<'index> + 'index> crate::interop::ProfileChildren<'index>
    for OptionalOptimized<'index, W, crate::c2rust::CRQEIterator>
{
    fn profile_children(self) -> Self {
        OptionalOptimized {
            max_doc_id: self.max_doc_id,
            weight: self.weight,
            child: self.child.map(crate::c2rust::CRQEIterator::into_profiled),
            wcii: self.wcii,
            virt: self.virt,
            last_doc_id: self.last_doc_id,
            at_eof: self.at_eof,
        }
    }
}

impl<'index, W, I> ProfilePrint for OptionalOptimized<'index, W, I>
where
    W: crate::WildcardIterator<'index>,
    I: RQEIterator<'index> + ProfilePrint,
{
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        ctx.print_single_child(c"OPTIONAL", self.child(), map);
    }
}

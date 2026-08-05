/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types for [`Optional`].

use index_result::{RSIndexResult, RSResultKind, RawIndexResult};
use ref_mode::{Active, Ref, Suspended};
use std::cmp;

use crate::{
    IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    RQEValidateStatus, ResumeOutcome, SkipToOutcome,
    boxed::{ResumeSlotOutcome, resume_child_slot_in_place, suspend_child_slot_in_place},
    profile_print::{ProfilePrint, ProfilePrintCtx},
};
use index_spec::IndexSpecReadGuard;
use rqe_core::{DocId, RS_FIELDMASK_ALL};

/// An iterator that emits a sequence of results with no gaps, up to a given document id.
/// Results are pulled from an underlying [`RQEIterator`] instance. If there is no entry
/// for a given document id, a virtual result is yielded in its place.
///
/// Parameterised over a [`Ref`] mode — see [`Optional`] for the [`Active`]
/// instantiation that implements [`RQEIterator`].
///
/// # Invariants
///
/// 1. **Layout compatibility across modes.** `RawOptional` is `#[repr(C)]` and
///    its only `Rf`-dependent field is `result: RawIndexResult<Rf>`, which is
///    layout-compatible across `Rf` (proven in `index_result`). Given that the
///    child `I` and its `I::Suspended` are layout-compatible (the
///    [`RQEIteratorBoxed`] contract, preserved through the `#[repr(C)]`
///    [`OptionalChild`] slot), the `Active` and `Suspended` instantiations are
///    layout-identical. This is what lets
///    [`suspend`](RQEIteratorBoxed::suspend) reinterpret the owning `Box` in
///    place.
#[repr(C)]
pub struct RawOptional<'query, Rf: Ref, I> {
    /// Inclusive upper bound on document identifiers to iterate over.
    /// Reads from the [`Optional::child`] beyond this bound are ignored.
    /// If the [`Optional::child`] ends before this bound, this [`Optional`] iterator yields virtual
    /// results with no [`Optional::weight`] applied until [`Optional::max_doc_id`] is reached.
    max_doc_id: DocId,

    /// Weight applied to results produced by the inner [`Optional::child`] iterator.
    /// This weight is not applied to virtual results.
    weight: f64,

    /// Virtual result which will always contain the last doc id,
    /// even if that doc id came from the [`Optional::child`] iterator.
    ///
    /// Only for actual virtual results do we return a reference to it in
    /// functions such as Read/SkipTo.
    result: RawIndexResult<'query, Rf>,

    /// The child [`RQEIterator`] provided at construction time.
    /// It is used while it can still produce results. Once exhausted,
    /// the iterator yields virtual results until [`Optional::max_doc_id`] is reached.
    ///
    /// In case child aborts during [`RQEIterator::revalidate`],
    /// this child is turned into [`OptionalChild::Gone`], changed from the
    /// [`OptionalChild::Present`] state it starts at when creating using
    /// [`Optional::new`]. From that point onward all results will be virtual
    /// until `max_doc_id` is reached.
    child: OptionalChild<I>,

    /// Whether the iterator has run *past* its last result, i.e. a
    /// `read`/`skip_to` found nothing beyond `max_doc_id` — the state behind
    /// [`current`](RQEIterator::current) and [`at_eof`](RQEIterator::at_eof),
    /// and the *only* record of it: both entry points check it before anything
    /// else, so an exhausted iterator stays exhausted until
    /// [`rewind`](RQEIterator::rewind) whatever the position says.
    ///
    /// It cannot be folded into `result.doc_id`: that field *is*
    /// [`last_doc_id`](RQEIterator::last_doc_id), so moving it to record
    /// exhaustion — past `max_doc_id`, or onto it when a skip overshoots —
    /// would report a position never yielded.
    past_end: bool,
}

/// Child slot for [`RawOptional`].
///
/// `#[repr(C)]` so that `OptionalChild<I>` is layout-compatible with
/// `OptionalChild<I::Suspended>` — a plain `Option<I>` is niche-dependent and
/// not transmute-stable across the `I` → `I::Suspended` swap that the
/// suspend/resume machinery relies on. Mirrors [`crate::maybe_empty`]'s
/// `MaybeEmptyOption` for the same reason.
#[repr(C)]
enum OptionalChild<I> {
    /// Child aborted during [`RQEIterator::revalidate`] (or otherwise gone):
    /// only virtual results from here on.
    Gone,
    /// Child still producing results.
    Present(I),
}

impl<I> OptionalChild<I> {
    /// Shared reference to the child, if it is still present.
    #[inline(always)]
    const fn as_ref(&self) -> Option<&I> {
        match self {
            Self::Gone => None,
            Self::Present(i) => Some(i),
        }
    }

    /// Mutable reference to the child, if it is still present.
    #[inline(always)]
    const fn as_mut(&mut self) -> Option<&mut I> {
        match self {
            Self::Gone => None,
            Self::Present(i) => Some(i),
        }
    }

    /// Map the child (if present) into a new type, preserving the [`Gone`] state.
    ///
    /// [`Gone`]: OptionalChild::Gone
    #[inline(always)]
    fn map<J>(self, f: impl FnOnce(I) -> J) -> OptionalChild<J> {
        match self {
            Self::Gone => OptionalChild::Gone,
            Self::Present(i) => OptionalChild::Present(f(i)),
        }
    }
}

/// Alias for an [`Active`] [`RawOptional`] — the only instantiation with an
/// [`RQEIterator`] impl today.
pub type Optional<'index, I> = RawOptional<'index, Active<'index>, I>;

// Compile-time proof of invariant 1 on `RawOptional`: for a representative
// concrete child, the `Active` and `Suspended` instantiations are
// layout-identical. The `result: RawIndexResult<Rf>` field's own cross-`Rf`
// layout compatibility is proven in `index_result`; the `OptionalChild<I>` slot's
// is the child's invariant 1 (enforced generically by
// `suspend_child_slot_in_place`). The `offset_of!` asserts pin down that neither
// the enum child nor the result field shifts across modes.
const _: () = {
    use crate::Wildcard;
    use std::mem::{align_of, offset_of, size_of};
    type AChild = Wildcard<'static>;
    type SChild = <Wildcard<'static> as RQEIteratorBoxed<'static>>::Suspended;
    type A = RawOptional<'static, Active<'static>, AChild>;
    type S = RawOptional<'static, Suspended, SChild>;
    assert!(offset_of!(A, max_doc_id) == offset_of!(S, max_doc_id));
    assert!(offset_of!(A, weight) == offset_of!(S, weight));
    assert!(offset_of!(A, result) == offset_of!(S, result));
    assert!(offset_of!(A, child) == offset_of!(S, child));
    assert!(offset_of!(A, past_end) == offset_of!(S, past_end));
    assert!(size_of::<A>() == size_of::<S>());
    assert!(align_of::<A>() == align_of::<S>());
};

impl<'index, I> Optional<'index, I>
where
    I: RQEIterator<'index>,
{
    #[inline(always)]
    /// Creates a new [`Optional`] iterator.
    ///
    /// * `max_id` is the inclusive upper bound of document identifiers visited by
    ///   [`RQEIterator::read`] and [`RQEIterator::skip_to`].
    /// * `weight` is applied to [`RSIndexResult`] values returned by the
    ///   child [`RQEIterator`]. When the child is exhausted, the iterator
    ///   yields virtual [`RSIndexResult`] values without weight until `max_id` is reached.
    /// * `child` [`RQEIterator`] used and wrapped around by this [`Optional`] iterator
    pub fn new(max_id: DocId, weight: f64, child: I) -> Self {
        Self {
            max_doc_id: max_id,
            weight,
            result: RSIndexResult::build_virt()
                .frequency(1)
                .field_mask(RS_FIELDMASK_ALL)
                .build(),
            child: OptionalChild::Present(child),
            past_end: false,
        }
    }

    /// Get a shared reference to the _child_ iterator
    /// wrapped by this [`Optional`] iterator.
    pub const fn child(&self) -> Option<&I> {
        self.child.as_ref()
    }

    /// Whether there is another result to yield: `max_doc_id` has not been
    /// reached yet.
    ///
    /// Goes `false` one step before [`Self::past_end`] is set, while
    /// `max_doc_id` is still the current result.
    #[inline(always)]
    const fn has_next(&self) -> bool {
        self.result.doc_id < self.max_doc_id
    }

    /// Whether this iterator owes no further result, recording it if the
    /// position has just reached `max_doc_id`.
    ///
    /// The single gate on both entry points: once
    /// [`past_end`](Self::past_end) is set, only a
    /// [`rewind`](RQEIterator::rewind) clears it. Checking it before
    /// [`Self::has_next`] is what lets an overshooting
    /// [`skip_to`](RQEIterator::skip_to) leave the position alone — the position
    /// then sits below `max_doc_id` while the iterator owes nothing.
    #[inline(always)]
    const fn exhausted(&mut self) -> bool {
        if self.past_end || !self.has_next() {
            self.past_end = true;
            return true;
        }
        false
    }
}

impl<'index, I> RQEIterator<'index> for Optional<'index, I>
where
    I: RQEIterator<'index>,
{
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        if self.past_end {
            return None;
        }
        if let Some(child) = self.child.as_mut()
            && child.last_doc_id() == self.result.doc_id
            && let Some(child_result) = child.current()
        {
            Some(child_result)
        } else {
            Some(&mut self.result)
        }
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.exhausted() {
            return Ok(None);
        }

        let maybe_real = self
            .child
            .as_mut()
            .map(|child| {
                let child_last_doc_id = child.last_doc_id();
                match child_last_doc_id.cmp(&(self.result.doc_id + 1)) {
                    cmp::Ordering::Less => child.read(),
                    cmp::Ordering::Equal => Ok(child.current()),
                    cmp::Ordering::Greater => Ok(None),
                }
            })
            .transpose()?
            .flatten();

        self.result.doc_id += 1;

        if let Some(real) = maybe_real {
            debug_assert!(
                real.doc_id >= self.result.doc_id,
                "no backwards reads should be possible"
            );

            if real.doc_id == self.result.doc_id {
                real.weight = self.weight;
                return Ok(Some(real));
            }
        }

        Ok(Some(&mut self.result))
    }

    /// Skip to a specific docId. If the child has a hit on this docId, return it.
    /// Otherwise, return a virtual hit.
    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        // Checked before the precondition below, so a probe arriving after this
        // iterator ran out is answered rather than asserted on: the position it
        // holds is the last result it yielded, which such a probe legitimately
        // sits above.
        if self.exhausted() {
            return Ok(None);
        }

        debug_assert!(doc_id > self.result.doc_id);

        if doc_id > self.max_doc_id {
            // Beyond the last document: this skip carries no result, so it may
            // not move the position. `past_end` is what records the step.
            self.past_end = true;
            return Ok(None);
        }

        if let Some(child) = self.child.as_mut() {
            if doc_id > child.last_doc_id() {
                // use current() here to work around
                // borrowing rules to be able to handle
                // both of `doc_id >= child.last_doc_id` cases...
                let _ = child.skip_to(doc_id)?;
            }

            if let Some(real) = child.current()
                && real.doc_id == doc_id
            {
                real.weight = self.weight;
                self.result.doc_id = real.doc_id;
                return Ok(Some(SkipToOutcome::Found(real)));
            }
        }

        self.result.doc_id = doc_id;
        Ok(Some(SkipToOutcome::Found(&mut self.result)))
    }

    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        let Some(child) = self.child.as_mut() else {
            return Ok(RQEValidateStatus::Ok);
        };
        let last_child_doc_id = child.last_doc_id();

        // Revalidate the child iterator
        match child.revalidate(spec)? {
            // Abort: Handle child validation results (but continue processing)
            status @ (RQEValidateStatus::Aborted | RQEValidateStatus::Moved { .. }) => {
                if matches!(status, RQEValidateStatus::Aborted) {
                    self.child = OptionalChild::Gone; // Drop it so we become fully virtual until max is reached
                }

                Ok(if last_child_doc_id != self.result.doc_id {
                    // virtual
                    RQEValidateStatus::Ok
                } else {
                    // was real before abort, re-read to
                    // prevent returning stale data.
                    RQEValidateStatus::Moved {
                        current: self.read()?,
                    }
                })
            }
            // If the current result is virtual,
            // or if the child was not moved, we can return VALIDATE_OK
            RQEValidateStatus::Ok => Ok(RQEValidateStatus::Ok),
        }
    }

    #[inline(always)]
    fn rewind(&mut self) {
        self.result.doc_id = 0;
        self.past_end = false;
        if let Some(child) = self.child.as_mut() {
            child.rewind();
        }
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
    fn type_(&self) -> IteratorType {
        IteratorType::Optional
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}

impl<'index, I> RQEIteratorBoxed<'index> for Optional<'index, I>
where
    I: RQEIteratorBoxed<'index>,
{
    type Suspended = RawOptional<'index, Suspended, I::Suspended>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // Transition the child in place (if present). A whole-box cast alone is
        // *not* enough for a type-erased child: its active and suspended forms
        // carry different `dyn` vtables, so the transition must be dispatched
        // through the child's own `suspend` — which `suspend_child_slot_in_place`
        // does (a no-op whole-box cast for concrete children, a vtable swap for
        // erased ones). A `Gone` child has no payload to transition.
        //
        // SAFETY: `raw` came from `Box::into_raw` (non-null, aligned, initialised,
        // exclusively owned). The `&mut` borrow of the child enum is confined to
        // this `if let`; `c` is the valid, exclusively-owned active child payload.
        if let OptionalChild::Present(c) = unsafe { &mut (*raw).child } {
            // SAFETY: `c` points at a valid, exclusively-owned `I`; the helper
            // reinitialises the slot as a valid `I::Suspended` in place.
            unsafe { suspend_child_slot_in_place(c as *mut I) };
        }
        // SAFETY: the `Present` child (if any) now holds `I::Suspended` and `Gone`
        // has no payload, so the allocation is a valid
        // `RawOptional<Suspended, I::Suspended>`: layout-identical to the active
        // form by invariant 1 on `RawOptional` (const proof above) —
        // `result: RawIndexResult<Rf>` is layout-compatible across `Rf`, and the
        // child slot's `I`/`I::Suspended` size/alignment match is statically
        // enforced by `suspend_child_slot_in_place`. `Box::from_raw` reuses the
        // same allocation, so the box address is preserved.
        unsafe { Box::from_raw(raw as *mut RawOptional<'index, Suspended, I::Suspended>) }
    }
}

impl<'query, S> RQESuspendedIterator<'query> for RawOptional<'query, Suspended, S>
where
    S: RQESuspendedIterator<'query>,
{
    type Resumed<'a>
        = Optional<'a, S::Resumed<'a>>
    where
        'query: 'a;

    fn resume<'a>(
        self: Box<Self>,
        guard: &IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        /// Outcome of resuming the child slot, captured so the `&mut` borrow of
        /// the child enum is released before we touch the slot as a raw pointer.
        enum ChildResume {
            /// Child was already `Gone` — stays fully virtual.
            Absent,
            /// Child resumed into the slot; `moved` mirrors `Moved`.
            Active { last: DocId, moved: bool },
            /// Child aborted and was consumed; the slot must be set to `Gone`.
            Aborted { last: DocId },
            /// Child resume failed; the slot must be restored to `Gone` and the
            /// box freed.
            Failed(RQEIteratorError),
        }

        // `Optional`'s `result` must still be a virtual sentinel. It is handed
        // out mutably via `current()`/`read()`/`skip_to`, so a consumer could in
        // principle have replaced its `data` with a real, index-backed payload.
        // Reinterpreting that `Suspended → Active` would assert `'index` borrows
        // that `Optional` cannot re-validate (it owns no backing for this
        // sentinel), so we abort the resume rather than risk UB. `kind() ==
        // Virtual` is the whole condition: `data` is the only `Rf`-parametrized
        // field, so it is all the reinterpretation touches — `metrics` are
        // `'query`-scoped and `dmd` is a plain `*const` raw pointer in both modes.
        // Checked here, safely via `&self`, before `Box::into_raw` opens the
        // raw-pointer critical section, so a violation just drops `self` (the
        // `Box`) normally.
        if self.result.kind() != RSResultKind::Virtual {
            return Ok(ResumeOutcome::Aborted);
        }

        let raw = Box::into_raw(self);

        // SAFETY: `raw` came from `Box::into_raw` (non-null, aligned, initialised,
        // exclusively owned). `&raw mut` forms a field pointer to the child enum
        // without creating a reference.
        let child_slot = unsafe { &raw mut (*raw).child };

        // Resume the child in place. The `&mut` borrow of the enum is confined to
        // this block so the raw `child_slot` writes below never alias it.
        // SAFETY: `child_slot` is non-null, aligned, initialised, and
        // exclusively owned (from `Box::into_raw`), so `&mut *child_slot` is a
        // sound unique borrow of the child enum.
        let step = match unsafe { &mut *child_slot } {
            OptionalChild::Gone => ChildResume::Absent,
            OptionalChild::Present(c) => {
                let last = S::last_doc_id(c);
                // SAFETY: `c` is the valid, exclusively-owned suspended child
                // payload. On Unchanged/Moved the helper rewrites the slot as a
                // valid `S::Resumed<'a>`; on Aborted/Err it consumes the child,
                // leaving the payload uninitialised (handled below).
                match unsafe { resume_child_slot_in_place(c as *mut S, guard) } {
                    Ok(ResumeSlotOutcome::Unchanged) => ChildResume::Active { last, moved: false },
                    Ok(ResumeSlotOutcome::Moved) => ChildResume::Active { last, moved: true },
                    Ok(ResumeSlotOutcome::Aborted) => ChildResume::Aborted { last },
                    Err(e) => ChildResume::Failed(e),
                }
            }
        };

        // Mirror `revalidate`: an aborted child is dropped (`Gone`), leaving us
        // fully virtual — it never aborts the whole `Optional`.
        let (child_disturbed, last_child_doc_id) = match step {
            ChildResume::Absent => (false, 0),
            ChildResume::Active { last, moved } => (moved, last),
            ChildResume::Aborted { last } => {
                // Child consumed → payload uninitialised. Overwrite the enum with
                // `Gone` (no payload); Optional becomes fully virtual.
                // SAFETY: `child_slot` is valid and exclusively owned; `ptr::write`
                // does not drop the moved-from payload.
                unsafe { child_slot.write(OptionalChild::Gone) };
                (true, last)
            }
            ChildResume::Failed(e) => {
                // As `Aborted`, but the failure aborts the whole resume. Restore a
                // valid `Gone` child so the box is a well-formed owned value again,
                // then drop it (frees `result` + the allocation) and propagate.
                // SAFETY: `child_slot` valid+owned; the payload is moved-from, so
                // `ptr::write` must not drop it.
                unsafe { child_slot.write(OptionalChild::Gone) };
                // SAFETY: `raw` is again a valid, exclusively-owned
                // `RawOptional<Suspended, S>` (child `Gone`, `result` still
                // suspended); reclaim it as a `Box` and drop it.
                drop(unsafe { Box::from_raw(raw) });
                return Err(e);
            }
        };

        // The child slot now holds a valid active child (or `Gone`). Reinterpret
        // the owning box in place, reusing the allocation so the `result` pointer
        // handed out by `current()`/`read()`/`skip_to` — and the FFI's cached
        // `header.current` — stays valid across the cycle; rebuilding via
        // `Box::new` would move `result` and dangle those pointers.
        //
        // SAFETY: layout-identical to the suspended form by invariant 1 on
        // `RawOptional` (const proof above). `result` is a virtual sentinel built
        // via `build_virt()` (its `data` is `RawResultData::Virtual`, `dmd` is
        // null, `metrics` is empty), so it carries no index pointers and its
        // `Suspended → Active<'a>` re-typing is unconditionally sound; the child
        // slot's `S`/`S::Resumed` size/alignment match is enforced by
        // `resume_child_slot_in_place`; the remaining fields carry no `Rf`.
        // `Box::from_raw` reuses the same allocation.
        let mut active = unsafe { Box::from_raw(raw.cast::<Optional<'a, S::Resumed<'a>>>()) };

        // Mirror `revalidate`: a child that aborted or moved forces a re-read
        // iff the previous result came from the child (its doc id matches the
        // aggregate's current doc id) rather than being a virtual sentinel.
        //
        // The re-read's outcome needs no inspection here: if it ran off the end
        // it set `past_end`, so `current()` reports no current and the `Moved`
        // below carries the same "nothing left" signal that `revalidate`'s
        // `Moved { current: None }` did.
        let moved = if child_disturbed && last_child_doc_id == active.result.doc_id {
            active.read()?;
            true
        } else {
            false
        };

        Ok(if moved {
            ResumeOutcome::Moved(active)
        } else {
            ResumeOutcome::Ok(active)
        })
    }

    fn last_doc_id(&self) -> DocId {
        self.result.doc_id
    }

    fn num_estimated(&self) -> usize {
        self.max_doc_id as usize
    }
}
impl<'index> crate::interop::ProfileChildren<'index>
    for Optional<'index, crate::c2rust::CRQEIterator>
{
    fn profile_children(self) -> Self {
        Optional {
            max_doc_id: self.max_doc_id,
            weight: self.weight,
            result: self.result,
            child: self.child.map(crate::c2rust::CRQEIterator::into_profiled),
            past_end: self.past_end,
        }
    }
}

impl<'index, I> ProfilePrint for Optional<'index, I>
where
    I: RQEIterator<'index> + ProfilePrint,
{
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        ctx.print_single_child(c"OPTIONAL", self.child(), map);
    }
}

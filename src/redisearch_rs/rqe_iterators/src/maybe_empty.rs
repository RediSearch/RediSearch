/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Helper wrapping either [`Empty`] or the provided [`RQEIterator`].

use index_result::RSIndexResult;
use index_spec::IndexSpecReadGuard;
use rqe_core::DocId;

use crate::{
    IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    RQEValidateStatus, ResumeOutcome, SkipToOutcome,
    boxed::{ResumeSlotOutcome, resume_child_slot_in_place},
    empty::Empty,
};

/// An iterator that is either [`Empty`] or the provided [`RQEIterator`].
///
/// # Invariants
///
/// 1. **Layout compatibility across the child swap.** `MaybeEmpty` is
///    `#[repr(C)]` over the `#[repr(C)]` [`MaybeEmptyOption`], so `MaybeEmpty<I>`
///    and `MaybeEmpty<I::Suspended>` are layout-identical given that the `Some`
///    payload `I` and its `I::Suspended` are. This is what lets
///    [`suspend`](RQEIteratorBoxed::suspend) / [`resume`](RQESuspendedIterator::resume)
///    reinterpret the owning `Box` in place. Proven by the `const _` block below.
#[repr(C)]
pub struct MaybeEmpty<I>(MaybeEmptyOption<I>);

// Compile-time proof of invariant 1 on `MaybeEmpty`: for a representative child,
// `MaybeEmpty<I>` and `MaybeEmpty<I::Suspended>` are layout-identical. As a
// newtype over a `#[repr(C)]` enum we assert size and alignment; the `Some`
// payload's `I`/`I::Suspended` compatibility is the child's invariant 1 (enforced
// generically by `suspend_child_slot_in_place`), and `None(Empty)` is `I`-free.
const _: () = {
    use crate::Wildcard;
    use std::mem::{align_of, size_of};
    type AChild = Wildcard<'static>;
    type SChild = <Wildcard<'static> as RQEIteratorBoxed<'static>>::Suspended;
    assert!(size_of::<MaybeEmpty<AChild>>() == size_of::<MaybeEmpty<SChild>>());
    assert!(align_of::<MaybeEmpty<AChild>>() == align_of::<MaybeEmpty<SChild>>());
};

impl<'index, I> MaybeEmpty<I>
where
    I: RQEIterator<'index>,
{
    /// Create a new [`MaybeEmpty`] with the given iterator as the underlying [`RQEIterator`].
    #[inline(always)]
    pub const fn new(iterator: I) -> Self {
        Self(MaybeEmptyOption::Some(iterator))
    }

    /// Create a new [`MaybeEmpty`] with [`Empty`] as the underlying [`RQEIterator`].
    #[inline(always)]
    pub const fn new_empty() -> Self {
        Self(MaybeEmptyOption::None(Empty))
    }

    /// Get a ref to child iterator, if any.
    #[inline(always)]
    pub const fn as_ref(&self) -> Option<&I> {
        match &self.0 {
            MaybeEmptyOption::None(_) => None,
            MaybeEmptyOption::Some(it) => Some(it),
        }
    }

    /// Transform the inner iterator (if present) into a new type.
    pub fn map<'b, J>(self, f: impl FnOnce(I) -> J) -> MaybeEmpty<J>
    where
        J: RQEIterator<'b>,
    {
        match self.0 {
            MaybeEmptyOption::None(_) => MaybeEmpty(MaybeEmptyOption::None(Empty)),
            MaybeEmptyOption::Some(it) => MaybeEmpty(MaybeEmptyOption::Some(f(it))),
        }
    }

    /// Consume the iterator, if there is any, and return if so.
    pub fn take_iterator(&mut self) -> Option<I> {
        if let MaybeEmptyOption::Some(iterator) = std::mem::take(&mut self.0) {
            return Some(iterator);
        }
        None
    }
}

impl<'index, I> Default for MaybeEmpty<I>
where
    I: RQEIterator<'index>,
{
    #[inline(always)]
    fn default() -> Self {
        Self::new_empty()
    }
}

#[repr(C)]
enum MaybeEmptyOption<I> {
    None(Empty),
    Some(I),
}

impl<I> Default for MaybeEmptyOption<I> {
    fn default() -> Self {
        MaybeEmptyOption::None(Empty)
    }
}

impl<'index, I> RQEIterator<'index> for MaybeEmpty<I>
where
    I: RQEIterator<'index>,
{
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        match &mut self.0 {
            MaybeEmptyOption::None(empty) => empty.current(),
            MaybeEmptyOption::Some(it) => it.current(),
        }
    }

    #[inline(always)]
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        match &mut self.0 {
            MaybeEmptyOption::None(empty) => empty.read(),
            MaybeEmptyOption::Some(it) => it.read(),
        }
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        match &mut self.0 {
            MaybeEmptyOption::None(empty) => empty.skip_to(doc_id),
            MaybeEmptyOption::Some(it) => it.skip_to(doc_id),
        }
    }

    #[inline(always)]
    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        match &mut self.0 {
            MaybeEmptyOption::None(empty) => empty.revalidate(spec),
            MaybeEmptyOption::Some(it) => it.revalidate(spec),
        }
    }

    #[inline(always)]
    fn rewind(&mut self) {
        match &mut self.0 {
            MaybeEmptyOption::None(empty) => empty.rewind(),
            MaybeEmptyOption::Some(it) => it.rewind(),
        }
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        match &self.0 {
            // Disambiguated against `RQESuspendedIterator::num_estimated`
            // (Empty's suspended counterpart is itself).
            MaybeEmptyOption::None(empty) => RQEIterator::num_estimated(empty),
            MaybeEmptyOption::Some(it) => it.num_estimated(),
        }
    }

    #[inline(always)]
    fn last_doc_id(&self) -> DocId {
        match &self.0 {
            // Disambiguated against `RQESuspendedIterator::last_doc_id`
            // (Empty's suspended counterpart is itself).
            MaybeEmptyOption::None(empty) => RQEIterator::last_doc_id(empty),
            MaybeEmptyOption::Some(it) => it.last_doc_id(),
        }
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        match &self.0 {
            MaybeEmptyOption::None(empty) => empty.at_eof(),
            MaybeEmptyOption::Some(it) => it.at_eof(),
        }
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        match &self.0 {
            MaybeEmptyOption::None(empty) => empty.type_(),
            MaybeEmptyOption::Some(it) => it.type_(),
        }
    }

    fn intersection_sort_weight(&self, prioritize_union_children: bool) -> f64 {
        match &self.0 {
            MaybeEmptyOption::None(empty) => {
                empty.intersection_sort_weight(prioritize_union_children)
            }
            MaybeEmptyOption::Some(it) => it.intersection_sort_weight(prioritize_union_children),
        }
    }
}

impl<'index, I> RQEIteratorBoxed<'index> for MaybeEmpty<I>
where
    I: RQEIteratorBoxed<'index>,
{
    type Suspended = MaybeEmpty<I::Suspended>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // Walk the `Some(I)` arm if present — dispatches via the trait so
        // dyn-erased `I` correctly transitions its vtable. The `None(Empty)`
        // arm needs no suspend (Empty is a unit struct with no state).
        //
        // SAFETY: `raw` came from `Box::into_raw`, exclusively owned and
        // valid, so the inner enum slot is reachable.
        let inner: &mut MaybeEmptyOption<I> = unsafe { &mut (*raw).0 };
        if let MaybeEmptyOption::Some(it) = inner {
            // SAFETY: `it` is a valid `&mut I` aliased to nothing else;
            // the function leaves the slot in a valid `I::Suspended` state.
            unsafe { crate::boxed::suspend_child_slot_in_place(it as *mut I) };
        }
        // SAFETY: the `Some` child (if any) now holds `I::Suspended` and
        // `None(Empty)` is `I`-free, so the allocation is a valid
        // `MaybeEmpty<I::Suspended>` — layout-identical to the active form by
        // invariant 1 on `MaybeEmpty` (const proof above), with the child slot's
        // `I`/`I::Suspended` size/alignment match statically enforced by
        // `suspend_child_slot_in_place`. `Box::from_raw` reuses the allocation.
        unsafe { Box::from_raw(raw as *mut MaybeEmpty<I::Suspended>) }
    }
}

impl<'query, S> RQESuspendedIterator<'query> for MaybeEmpty<S>
where
    S: RQESuspendedIterator<'query>,
{
    type Resumed<'a>
        = MaybeEmpty<S::Resumed<'a>>
    where
        'query: 'a;

    fn resume<'a>(
        self: Box<Self>,
        guard: &IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        /// Outcome of resuming the `Some` child, captured so the `&mut` borrow of
        /// the inner enum is released before we touch the slot as a raw pointer.
        enum ChildResume {
            /// `None(Empty)` arm — no child to resume.
            NoneArm,
            /// Child resumed in place; `moved` mirrors `Moved`.
            Active { moved: bool },
            /// Child aborted and was consumed; forward `Aborted`.
            Aborted,
            /// Child resume failed; forward the error.
            Failed(RQEIteratorError),
        }

        let raw = Box::into_raw(self);
        // SAFETY: `raw` came from `Box::into_raw` (non-null, aligned, initialised,
        // exclusively owned). `&raw mut` forms a field pointer to the inner enum
        // without creating a reference.
        let inner_slot = unsafe { &raw mut (*raw).0 };

        // Resume the child in place. The `&mut` borrow of the enum is confined to
        // this block so the raw `inner_slot` writes below never alias it.
        // SAFETY: `inner_slot` is valid and exclusively owned; `&mut *inner_slot`
        // is a sound unique borrow of the inner enum.
        let step = match unsafe { &mut *inner_slot } {
            MaybeEmptyOption::None(_) => ChildResume::NoneArm,
            MaybeEmptyOption::Some(child) => {
                // SAFETY: `child` is the valid, exclusively-owned suspended child
                // payload. On Unchanged/Moved the helper rewrites the slot as a
                // valid `S::Resumed<'a>`; on Aborted/Err it consumes the child,
                // leaving the payload uninitialised (handled below).
                match unsafe { resume_child_slot_in_place(child as *mut S, guard) } {
                    Ok(ResumeSlotOutcome::Unchanged) => ChildResume::Active { moved: false },
                    Ok(ResumeSlotOutcome::Moved) => ChildResume::Active { moved: true },
                    Ok(ResumeSlotOutcome::Aborted) => ChildResume::Aborted,
                    Err(e) => ChildResume::Failed(e),
                }
            }
        };

        // Forward the child's outcome: an aborted child aborts the whole wrapper
        // (MaybeEmpty has no virtual fallback), mirroring the previous behaviour.
        let moved = match step {
            ChildResume::NoneArm => false,
            ChildResume::Active { moved } => moved,
            ChildResume::Aborted => {
                // Child consumed → the `Some` payload is uninitialised. Overwrite
                // the enum with `None(Empty)` (no `I` payload) so the box is a
                // valid owned value again, drop it, and forward `Aborted`.
                // SAFETY: `inner_slot` valid+owned; `ptr::write` does not drop the
                // moved-from payload.
                unsafe { inner_slot.write(MaybeEmptyOption::None(Empty)) };
                // SAFETY: `raw` is again a valid, exclusively-owned `MaybeEmpty<S>`;
                // reclaim and drop it (frees the allocation).
                drop(unsafe { Box::from_raw(raw) });
                return Ok(ResumeOutcome::Aborted);
            }
            ChildResume::Failed(e) => {
                // As `Aborted`, but forward the error.
                // SAFETY: `inner_slot` valid+owned; `ptr::write` does not drop the
                // moved-from payload.
                unsafe { inner_slot.write(MaybeEmptyOption::None(Empty)) };
                // SAFETY: `raw` is again a valid, exclusively-owned `MaybeEmpty<S>`;
                // reclaim and drop it.
                drop(unsafe { Box::from_raw(raw) });
                return Err(e);
            }
        };

        // `None`, or `Some` resumed in place: reinterpret the owning box, reusing
        // the allocation so the inline child — which `current()`/`read()` delegate
        // into, and whose own `resume` preserved its interior addresses — is not
        // moved (rebuilding via `Box::new` would undo that and dangle the FFI's
        // cached `header.current`).
        //
        // SAFETY: `MaybeEmpty<S>` and `MaybeEmpty<S::Resumed<'a>>` are
        // layout-identical by invariant 1 on `MaybeEmpty` (const proof above):
        // `None(Empty)` is `S`-free (and `Empty` is its own resumed counterpart),
        // and the `Some` payload's `S`/`S::Resumed` size/alignment match is
        // enforced by `resume_child_slot_in_place`. `Box::from_raw` reuses the
        // same allocation.
        let active = unsafe { Box::from_raw(raw.cast::<MaybeEmpty<S::Resumed<'a>>>()) };
        Ok(if moved {
            ResumeOutcome::Moved(active)
        } else {
            ResumeOutcome::Ok(active)
        })
    }

    fn last_doc_id(&self) -> DocId {
        match &self.0 {
            MaybeEmptyOption::None(_) => 0,
            MaybeEmptyOption::Some(child) => S::last_doc_id(child),
        }
    }

    fn num_estimated(&self) -> usize {
        match &self.0 {
            // Empty is its own suspended counterpart; disambiguate against
            // `RQEIterator::num_estimated`.
            MaybeEmptyOption::None(empty) => RQESuspendedIterator::num_estimated(empty),
            MaybeEmptyOption::Some(child) => S::num_estimated(child),
        }
    }
}

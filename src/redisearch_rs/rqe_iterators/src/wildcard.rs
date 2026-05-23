/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Supporting types for [`Wildcard`].

use std::ptr::NonNull;

use index_result::{RSIndexResult, RawIndexResult};
use index_spec::IndexSpecReadGuard;
use inverted_index::codec::{doc_ids_only::DocIdsOnly, raw_doc_ids_only::RawDocIdsOnly};
use inverted_index::{DocIdsDecoder, opaque};
use ref_mode::{Active, Ref, Suspended};

use rqe_core::{DocId, RS_FIELDMASK_ALL};

use crate::{
    Empty, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    RQEValidateStatus, ResumeOutcome, SEARCH_ENTERPRISE_ITERATORS, SkipToOutcome,
    profile_print::{ProfilePrint, ProfilePrintCtx},
};
use crate::{IteratorType, QueryError, RQEIteratorPrintable};

/// An iterator that yields all ids within a given range, from 1 to max id
/// (inclusive) in an index.
///
/// Parameterised over a [`Ref`] mode — see [`Wildcard`] for the [`Active`]
/// instantiation that implements [`RQEIterator`]. The struct owns no
/// references into the index (it's a pure counter); the only `Rf`-dependent
/// field is `result`.
#[repr(C)]
pub struct RawWildcard<'query, Rf: Ref> {
    // Supposed to be the max id in the index
    top_id: DocId,

    /// A reusable result object to avoid allocations on each `read` call.
    result: RawIndexResult<'query, Rf>,

    /// Whether the iterator has run *past* its last result, i.e. a
    /// `read`/`skip_to` found nothing.
    ///
    /// The state behind [`current`](RQEIterator::current) and
    /// [`at_eof`](RQEIterator::at_eof), and the *only* record of it: both
    /// entry points check it before anything else, so an exhausted iterator
    /// stays exhausted until [`rewind`](RQEIterator::rewind) whatever the
    /// position says.
    ///
    /// It cannot be folded into `result.doc_id`: that field *is*
    /// [`last_doc_id`](RQEIterator::last_doc_id), which parents use to choose
    /// skip targets, so moving it to record exhaustion — past `top_id`, or onto
    /// it when a skip overshoots — would report a position never yielded.
    /// [`InvIndIterator`](crate::inverted_index::InvIndIterator) splits the two
    /// the same way, with `at_eos` beside an untouched `last_doc_id`.
    past_end: bool,
}

/// Alias for an [`Active`] [`RawWildcard`] — the only instantiation with an
/// [`RQEIterator`] impl today.
pub type Wildcard<'index> = RawWildcard<'index, Active<'index>>;

// Compile-time proof that the `Active` and `Suspended` instantiations are
// layout-identical, which is what lets [`RawWildcard::suspend`] and its `resume`
// reinterpret the owning `Box` in place. `result: RawIndexResult<Rf>` is the only
// `Rf`-dependent field, and its own cross-`Rf` compatibility is proven in
// `index_result`; these asserts pin down that neither it nor the flag beside it
// shifts across modes. Mirrors the block in [`crate::optional`].
const _: () = {
    use std::mem::{align_of, offset_of, size_of};
    type A = RawWildcard<'static, Active<'static>>;
    type S = RawWildcard<'static, Suspended>;
    assert!(offset_of!(A, top_id) == offset_of!(S, top_id));
    assert!(offset_of!(A, result) == offset_of!(S, result));
    assert!(offset_of!(A, past_end) == offset_of!(S, past_end));
    assert!(size_of::<A>() == size_of::<S>());
    assert!(align_of::<A>() == align_of::<S>());
};

impl Wildcard<'_> {
    pub fn new(top_id: DocId, weight: f64) -> Self {
        Wildcard {
            top_id,
            result: RSIndexResult::build_virt()
                .frequency(1)
                .weight(weight)
                .field_mask(RS_FIELDMASK_ALL)
                .build(),
            past_end: false,
        }
    }

    /// Whether this iterator owes no further result, recording it if the
    /// position has just reached `top_id`.
    ///
    /// The single gate on both entry points, and the only predicate either of
    /// them needs: once [`past_end`](Self::past_end) is set, only a
    /// [`rewind`](RQEIterator::rewind) clears it. It is checked before the
    /// position, which is what lets an overshooting
    /// [`skip_to`](RQEIterator::skip_to) leave the position alone — the position
    /// then sits below `top_id` while the iterator owes nothing, so the
    /// comparison alone would answer that there is more to come.
    #[inline(always)]
    const fn exhausted(&mut self) -> bool {
        if self.past_end || self.result.doc_id >= self.top_id {
            self.past_end = true;
            return true;
        }
        false
    }
}

impl<'index> RQEIterator<'index> for Wildcard<'index> {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        if self.past_end {
            return None;
        }
        Some(&mut self.result)
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        if self.exhausted() {
            return Ok(None);
        }

        self.result.doc_id += 1;
        Ok(Some(&mut self.result))
    }

    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        if self.exhausted() {
            return Ok(None);
        }
        debug_assert!(self.last_doc_id() < doc_id);

        if doc_id > self.top_id {
            // Beyond the last document: this skip carries no result, so it may
            // not move the position. `past_end` is what records the step, and
            // the position stays where the last yield left it.
            self.past_end = true;
            return Ok(None);
        }

        self.result.doc_id = doc_id;
        Ok(Some(SkipToOutcome::Found(&mut self.result)))
    }

    fn rewind(&mut self) {
        self.result.doc_id = 0;
        self.past_end = false;
    }

    // This should always return total results from the iterator, even after some yields.
    fn num_estimated(&self) -> usize {
        self.top_id as usize
    }

    fn last_doc_id(&self) -> DocId {
        self.result.doc_id
    }

    fn at_eof(&self) -> bool {
        self.past_end
    }

    fn revalidate(
        &mut self,
        _spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        Ok(RQEValidateStatus::Ok)
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        IteratorType::Wildcard
    }

    fn intersection_sort_weight(&self, _prioritize_union_children: bool) -> f64 {
        1.0
    }
}

impl<'index> RQEIteratorBoxed<'index> for Wildcard<'index> {
    type Suspended = RawWildcard<'index, Suspended>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: `RawWildcard` is `#[repr(C)]` with the only `Rf`-dependent
        // field being `result: RawIndexResult<Rf>`, layout-compatible across
        // `Rf` (see [`crate::inverted_index::Wildcard::suspend`] for the
        // same argument). Box::from_raw reuses the same heap allocation.
        unsafe { Box::from_raw(raw as *mut RawWildcard<'index, Suspended>) }
    }
}

impl<'query> RQESuspendedIterator<'query> for RawWildcard<'query, Suspended> {
    type Resumed<'a>
        = Wildcard<'a>
    where
        'query: 'a;

    fn resume<'a>(
        self: Box<Self>,
        _guard: &IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        let raw = Box::into_raw(self);
        // SAFETY: layout-compatible — see `suspend`. The top-level wildcard
        // owns no references into the index (it's just a counter), so there
        // is no state to refresh.
        let active = unsafe { Box::from_raw(raw as *mut Wildcard<'a>) };
        Ok(ResumeOutcome::Ok(active))
    }

    fn last_doc_id(&self) -> DocId {
        self.result.doc_id
    }

    fn num_estimated(&self) -> usize {
        // Mode-independent — mirrors the active `num_estimated`.
        self.top_id as usize
    }
}
/// A marker trait for iterators that match all documents.
pub trait WildcardIterator<'index>: RQEIterator<'index> {}

/// [`Wildcard`] is obviously a wildcard iterator.
impl<'index> WildcardIterator<'index> for Wildcard<'index> {}

/// [`inverted_index::Wildcard`](crate::inverted_index::Wildcard) is used in the optimized version.
impl<'index, E> WildcardIterator<'index> for crate::inverted_index::Wildcard<'index, E>
where
    E: inverted_index::DecodedBy
        + inverted_index::opaque::OpaqueEncoding<Storage = inverted_index::InvertedIndex<E>>
        + 'index,
    <E as inverted_index::DecodedBy>::Decoder: DocIdsDecoder,
{
}

/// A [`Profile`](crate::profile::Profile) wrapper preserves the wildcard property of its child.
impl<'index, I: WildcardIterator<'index>> WildcardIterator<'index>
    for crate::profile::Profile<'index, I>
{
}

/// A [`CRQEIterator`](crate::c2rust::CRQEIterator) may wrap a wildcard iterator
/// at runtime, but this cannot be verified statically.
/// The caller is responsible for only using this impl when the underlying C
/// iterator is actually a wildcard—mirroring the C code's use of an untyped
/// `QueryIterator*` for the `wcii` field.
impl<'index> WildcardIterator<'index> for crate::c2rust::CRQEIterator {}

impl<'index> WildcardIterator<'index> for Box<dyn WildcardIterator<'index> + 'index> {}

/// A [`TypeErasedRQEIterator`](crate::TypeErasedRQEIterator) may wrap a wildcard
/// iterator, but — as with [`CRQEIterator`](crate::c2rust::CRQEIterator) — that
/// cannot be verified statically once the concrete type is erased.
///
/// The caller is responsible for only using this impl when the erased iterator
/// really is a wildcard. It exists so composites that take a wildcard base (e.g.
/// [`OptionalOptimized`](crate::optional_optimized::OptionalOptimized)) can hold
/// a type-erased one, which is what the suspend/resume path needs in order to
/// dispatch the base's own `suspend`/`resume` through its vtable.
impl<'index> WildcardIterator<'index> for crate::TypeErasedRQEIterator<'index> {}

/// The result of [`new_wildcard_iterator`], representing the different kinds of
/// wildcard iterators that can be created depending on the index configuration.
///
/// # Invariants
///
/// 1. **Layout compatibility with [`NewWildcardSuspended`].** The two are
///    layout-identical, so [`suspend`](RQEIteratorBoxed::suspend) /
///    [`resume`](RQESuspendedIterator::resume) can transition each payload in
///    place and then reinterpret the owning `Box`, preserving every interior
///    address across the cycle. `#[repr(C, u8)]` on **both** is what pins the
///    tag encoding and the payload offsets — under the default `repr(Rust)`
///    each enum picks its own, and the tag may even be niche-encoded into a
///    payload pointer. The `const _` proof beside `NewWildcardSuspended`
///    discharges the rest.
///
/// Unlike [`RawOptimizedWildcard`], this pair cannot collapse into a single
/// `Rf`-parametrized enum: the [`Disk`](Self::Disk) arm's active and suspended
/// forms are genuinely different types — a `Box<dyn …>` and the
/// [`DiskWildcardSuspended`] newtype — rather than one type at two modes.
#[repr(C, u8)]
pub enum NewWildcardIterator<'index> {
    /// Non-optimized wildcard: yields all document ids from 1 to `maxDocId`.
    NotOptimized(Wildcard<'index>),
    /// Optimized wildcard: reads from the `existingDocs` inverted index.
    Optimized(OptimizedWildcard<'index>),
    /// Empty wildcard: the index has no documents.
    Empty(Empty),
    /// Disk-backed wildcard: delegates to the enterprise disk index iterator.
    Disk(DiskWildcardIterator<'index>),
}

/// Payload of [`RawOptimizedWildcard::DocIdsOnly`]: an inverted-index wildcard
/// over a [`DocIdsOnly`]-encoded `existingDocs`. `Rf` flows into the reader,
/// which weakens on suspend; the reader-dispatch slot is left at its default,
/// which freezes it to the **active** reader in both modes — see
/// [`crate::inverted_index::RawWildcard`].
type DocIdsOnlyArm<'query, Rf> = crate::inverted_index::RawWildcard<'query, Rf, DocIdsOnly>;

/// Payload of [`RawOptimizedWildcard::RawDocIdsOnly`] — [`DocIdsOnlyArm`] over
/// the [`RawDocIdsOnly`] encoding instead.
type RawDocIdsOnlyArm<'query, Rf> = crate::inverted_index::RawWildcard<'query, Rf, RawDocIdsOnly>;

/// An optimized wildcard iterator over the `existingDocs` inverted index,
/// parameterised over a [`Ref`] mode.
///
/// The encoding may be either [`DocIdsOnly`] or [`RawDocIdsOnly`], depending on
/// the index configuration.
///
/// See [`OptimizedWildcard`] for the [`Active`] instantiation that implements
/// [`RQEIterator`], and [`OptimizedWildcardSuspended`] for its passive carrier
/// across a lock release/reacquire cycle.
///
/// # Invariants
///
/// 1. **Layout compatibility across modes.** `OptimizedWildcard<'query>` and
///    `OptimizedWildcardSuspended<'query>` are layout-identical, so
///    [`suspend`](RQEIteratorBoxed::suspend) /
///    [`resume`](RQESuspendedIterator::resume) can transition each payload in
///    place and then reinterpret the owning `Box` between the two. Being a
///    single `#[repr(C, u8)]` generic, the two share a tag encoding and variant
///    order by construction; the per-arm payload correspondence and layout
///    identity are enforced by the `const _` proof below.
///
///    `#[repr(C, u8)]` is load-bearing, not decorative: under the default
///    `repr(Rust)` both the tag encoding and the payload offsets are
///    unspecified, and the compiler is free to pick differently for the two
///    instantiations — the tag could even be niche-encoded into one of the
///    payload's [`NonNull`] fields.
#[repr(C, u8)]
pub enum RawOptimizedWildcard<'query, Rf: Ref> {
    /// Optimized wildcard with [`DocIdsOnly`] encoding.
    DocIdsOnly(DocIdsOnlyArm<'query, Rf>),
    /// Optimized wildcard with [`RawDocIdsOnly`] encoding.
    RawDocIdsOnly(RawDocIdsOnlyArm<'query, Rf>),
}

/// Alias for an [`Active`] [`RawOptimizedWildcard`] — the only instantiation
/// with an [`RQEIterator`] impl.
pub type OptimizedWildcard<'index> = RawOptimizedWildcard<'index, Active<'index>>;

/// [`Suspended`]-mode counterpart of [`OptimizedWildcard`], used as its
/// [`RQEIteratorBoxed::Suspended`] type. Retains the `'query` lifetime so
/// query-attached borrows stay valid across the suspend/resume cycle.
pub type OptimizedWildcardSuspended<'query> = RawOptimizedWildcard<'query, Suspended>;

/// Delegates each [`RQEIterator`] method to the active variant.
macro_rules! delegate_rqe_iterator {
    ($self:ident, $method:ident $(, $arg:ident)*) => {
        match $self {
            Self::DocIdsOnly(it) => it.$method($($arg),*),
            Self::RawDocIdsOnly(it) => it.$method($($arg),*),
        }
    };
}

impl<'index> RQEIterator<'index> for OptimizedWildcard<'index> {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        delegate_rqe_iterator!(self, current)
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        delegate_rqe_iterator!(self, read)
    }

    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        delegate_rqe_iterator!(self, skip_to, doc_id)
    }

    fn rewind(&mut self) {
        delegate_rqe_iterator!(self, rewind)
    }

    fn num_estimated(&self) -> usize {
        delegate_rqe_iterator!(self, num_estimated)
    }

    fn last_doc_id(&self) -> DocId {
        delegate_rqe_iterator!(self, last_doc_id)
    }

    fn at_eof(&self) -> bool {
        delegate_rqe_iterator!(self, at_eof)
    }

    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        delegate_rqe_iterator!(self, revalidate, spec)
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        delegate_rqe_iterator!(self, type_)
    }

    fn intersection_sort_weight(&self, prioritize_union_children: bool) -> f64 {
        delegate_rqe_iterator!(self, intersection_sort_weight, prioritize_union_children)
    }
}

impl<'index> WildcardIterator<'index> for OptimizedWildcard<'index> {}

impl crate::profile_print::ProfilePrint for OptimizedWildcard<'_> {
    fn print_profile(
        &self,
        map: &mut redis_reply::MapBuilder<'_>,
        ctx: &mut crate::profile_print::ProfilePrintCtx<'_>,
    ) {
        match self {
            Self::DocIdsOnly(it) => it.print_profile(map, ctx),
            Self::RawDocIdsOnly(it) => it.print_profile(map, ctx),
        }
    }
}

// Compile-time proof of invariant 1 on `RawOptimizedWildcard`: a
// `Box<OptimizedWildcard>` can be reinterpreted as a
// `Box<OptimizedWildcardSuspended>` and back, as `suspend`/`resume` below do.
// Because both are instantiations of the *same* `#[repr(C, u8)]` enum, the
// variant order and tag encoding agree by construction and need no assertion.
// What remains to be proven:
//
// (a) **Arm correspondence.** `suspend` fills each payload slot with
//     `<active arm as RQEIteratorBoxed>::Suspended` — see
//     [`crate::boxed::suspend_child_slot_in_place`] — and only then relabels the
//     owning box. That is sound only if the suspended enum's matching arm *is*
//     that projection. Asserted below as a type equality rather than a layout
//     comparison: it is the property the reinterpretation actually needs, and a
//     mismatch becomes a build error instead of a silently mistyped payload.
//
// (b) **Per-arm payload layout identity.** Implied by (a) together with
//     invariant 1 on `RawInvIndIterator` (proven in `inverted_index/core.rs`),
//     which `inverted_index::RawWildcard` is a `#[repr(C)]` newtype over.
//     Asserted per arm anyway so a regression names the arm that broke, and
//     because under `#[repr(C, u8)]` each payload sits at an offset derived
//     from *that payload's* alignment — per-arm alignment equality, not the
//     enum's, is what pins the payload offsets. (`offset_of!` into enum
//     variants is not yet stable, so the offsets cannot be asserted directly.)
//
// (c) **Enum size/alignment equality.** Needed on its own by `resume`'s
//     abort/error paths, which free an allocation created for the active enum
//     using `Layout::new::<OptimizedWildcardSuspended>()`.
//
// A module-level `const _` is evaluated even though neither type is ever
// instantiated here, which a `const {}` inside a generic function would not be.
const _: () = {
    use std::mem::{align_of, size_of};

    /// Witnesses that `Self` and `T` are the same type: the blanket impl is the
    /// only one, so a `A: IsSame<B>` bound holds exactly when `A` *is* `B`.
    trait IsSame<T> {}
    impl<T> IsSame<T> for T {}

    /// (a) — fails to compile unless suspending `A` yields exactly `S`.
    const fn assert_suspends_to<A, S>()
    where
        A: RQEIteratorBoxed<'static>,
        A::Suspended: IsSame<S>,
    {
    }

    assert_suspends_to::<DocIdsOnlyArm<'static, Active<'static>>, DocIdsOnlyArm<'static, Suspended>>(
    );
    assert_suspends_to::<
        RawDocIdsOnlyArm<'static, Active<'static>>,
        RawDocIdsOnlyArm<'static, Suspended>,
    >();

    // (b)
    assert!(
        size_of::<DocIdsOnlyArm<'static, Active<'static>>>()
            == size_of::<DocIdsOnlyArm<'static, Suspended>>()
    );
    assert!(
        align_of::<DocIdsOnlyArm<'static, Active<'static>>>()
            == align_of::<DocIdsOnlyArm<'static, Suspended>>()
    );
    assert!(
        size_of::<RawDocIdsOnlyArm<'static, Active<'static>>>()
            == size_of::<RawDocIdsOnlyArm<'static, Suspended>>()
    );
    assert!(
        align_of::<RawDocIdsOnlyArm<'static, Active<'static>>>()
            == align_of::<RawDocIdsOnlyArm<'static, Suspended>>()
    );

    // (c)
    assert!(
        size_of::<OptimizedWildcard<'static>>() == size_of::<OptimizedWildcardSuspended<'static>>()
    );
    assert!(
        align_of::<OptimizedWildcard<'static>>()
            == align_of::<OptimizedWildcardSuspended<'static>>()
    );
};

impl<'index> RQEIteratorBoxed<'index> for OptimizedWildcard<'index> {
    type Suspended = OptimizedWildcardSuspended<'index>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // Transition the payload in place, arm by arm. Both enums are the same
        // `#[repr(C, u8)]` generic at different `Rf`, so they share a tag
        // encoding and variant order; the helper writes exactly the arm's
        // `Suspended` projection, which invariant 1's proof (a) pins to the
        // matching suspended arm. Together those make the final whole-box cast
        // sound. The tag byte itself is untouched.
        //
        // SAFETY: `raw` came from `Box::into_raw` (non-null, aligned,
        // initialised, exclusively owned); the `&mut` borrow is confined to the
        // match.
        match unsafe { &mut *raw } {
            RawOptimizedWildcard::DocIdsOnly(it) => {
                // SAFETY: `it` is the valid, exclusively-owned payload; the
                // helper reinitialises the slot as its `Suspended` form in place.
                unsafe { crate::boxed::suspend_child_slot_in_place(it as *mut _) }
            }
            RawOptimizedWildcard::RawDocIdsOnly(it) => {
                // SAFETY: as above.
                unsafe { crate::boxed::suspend_child_slot_in_place(it as *mut _) }
            }
        }
        // SAFETY: the payload now holds its `Suspended` form at the same offset,
        // and the tag encodes the same variant in both enums. `Box::from_raw`
        // reuses the same allocation, so the box address is preserved.
        unsafe { Box::from_raw(raw.cast::<OptimizedWildcardSuspended<'index>>()) }
    }
}

impl<'query> RQESuspendedIterator<'query> for OptimizedWildcardSuspended<'query> {
    type Resumed<'a>
        = OptimizedWildcard<'a>
    where
        'query: 'a;

    fn resume<'a>(
        self: Box<Self>,
        spec: &IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        let raw = Box::into_raw(self);
        // Resume the payload in place, arm by arm — the tag byte is untouched,
        // so the whole-box cast below lands on the same variant of the active
        // enum (same `#[repr(C, u8)]` generic at a different `Rf`; arm
        // correspondence and per-arm layout identity are invariant 1's const
        // proof above). The arm's own `resume` owns the abort decision — the
        // garbage collector may have nulled out or replaced `existingDocs` — so
        // it is stated in exactly one place, shared with the arm's legacy
        // `revalidate`.
        //
        // SAFETY: `raw` came from `Box::into_raw` (non-null, aligned,
        // initialised, exclusively owned); the `&mut` borrow is confined to the
        // match. On `Unchanged`/`Moved` the helper rewrites the payload as its
        // resumed form; on `Aborted`/`Err` it consumes the payload, leaving it
        // uninitialised (handled below).
        let outcome = match unsafe { &mut *raw } {
            RawOptimizedWildcard::DocIdsOnly(it) => {
                // SAFETY: `it` is the valid, exclusively-owned payload.
                unsafe { crate::boxed::resume_child_slot_in_place(it as *mut _, spec) }
            }
            RawOptimizedWildcard::RawDocIdsOnly(it) => {
                // SAFETY: as above.
                unsafe { crate::boxed::resume_child_slot_in_place(it as *mut _, spec) }
            }
        };

        match outcome {
            Ok(crate::boxed::ResumeSlotOutcome::Unchanged) => {
                // SAFETY: the payload holds its resumed form at the same offset
                // and the tag is unchanged; `Box::from_raw` reuses the same
                // allocation, so the box address — and the FFI's cached
                // `header.current` into the payload's result — stay valid.
                let active = unsafe { Box::from_raw(raw.cast::<OptimizedWildcard<'a>>()) };
                Ok(ResumeOutcome::Ok(active))
            }
            Ok(crate::boxed::ResumeSlotOutcome::Moved) => {
                // SAFETY: as above.
                let active = unsafe { Box::from_raw(raw.cast::<OptimizedWildcard<'a>>()) };
                Ok(ResumeOutcome::Moved(active))
            }
            Ok(crate::boxed::ResumeSlotOutcome::Aborted) => {
                // The payload was consumed; the allocation holds only the tag
                // and an uninitialised payload, so it must be freed without
                // dropping anything.
                // SAFETY: `raw` was allocated by `Box` with exactly this layout;
                // nothing in it is live any more.
                unsafe {
                    std::alloc::dealloc(
                        raw.cast::<u8>(),
                        std::alloc::Layout::new::<OptimizedWildcardSuspended<'query>>(),
                    )
                };
                Ok(ResumeOutcome::Aborted)
            }
            Err(e) => {
                // As `Aborted`: the payload was consumed; free the allocation
                // without dropping it.
                // SAFETY: as above.
                unsafe {
                    std::alloc::dealloc(
                        raw.cast::<u8>(),
                        std::alloc::Layout::new::<OptimizedWildcardSuspended<'query>>(),
                    )
                };
                Err(e)
            }
        }
    }

    fn last_doc_id(&self) -> DocId {
        match self {
            RawOptimizedWildcard::DocIdsOnly(it) => RQESuspendedIterator::last_doc_id(it),
            RawOptimizedWildcard::RawDocIdsOnly(it) => RQESuspendedIterator::last_doc_id(it),
        }
    }

    fn num_estimated(&self) -> usize {
        match self {
            RawOptimizedWildcard::DocIdsOnly(it) => RQESuspendedIterator::num_estimated(it),
            RawOptimizedWildcard::RawDocIdsOnly(it) => RQESuspendedIterator::num_estimated(it),
        }
    }
}

/// Delegates each [`RQEIterator`] method to the active variant.
macro_rules! delegate_wildcard_iterator {
    ($self:ident, $method:ident $(, $arg:ident)*) => {
        match $self {
            Self::NotOptimized(it) => it.$method($($arg),*),
            Self::Optimized(it) => it.$method($($arg),*),
            Self::Empty(it) => it.$method($($arg),*),
            Self::Disk(it) => it.$method($($arg),*),
        }
    };
}

impl<'index> RQEIterator<'index> for NewWildcardIterator<'index> {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        delegate_wildcard_iterator!(self, current)
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        delegate_wildcard_iterator!(self, read)
    }

    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        delegate_wildcard_iterator!(self, skip_to, doc_id)
    }

    fn rewind(&mut self) {
        delegate_wildcard_iterator!(self, rewind)
    }

    fn num_estimated(&self) -> usize {
        // Disambiguated against `RQESuspendedIterator::num_estimated` for the
        // `Empty` variant (whose Suspended counterpart is `Empty` itself).
        match self {
            Self::NotOptimized(it) => RQEIterator::num_estimated(it),
            Self::Optimized(it) => RQEIterator::num_estimated(it),
            Self::Empty(it) => RQEIterator::num_estimated(it),
            Self::Disk(it) => RQEIterator::num_estimated(it),
        }
    }

    fn last_doc_id(&self) -> DocId {
        // Disambiguated against `RQESuspendedIterator::last_doc_id` for the
        // `Empty` variant (whose Suspended counterpart is `Empty` itself).
        match self {
            Self::NotOptimized(it) => RQEIterator::last_doc_id(it),
            Self::Optimized(it) => RQEIterator::last_doc_id(it),
            Self::Empty(it) => RQEIterator::last_doc_id(it),
            Self::Disk(it) => RQEIterator::last_doc_id(it),
        }
    }

    fn at_eof(&self) -> bool {
        delegate_wildcard_iterator!(self, at_eof)
    }

    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        delegate_wildcard_iterator!(self, revalidate, spec)
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        delegate_wildcard_iterator!(self, type_)
    }

    fn intersection_sort_weight(&self, prioritize_union_children: bool) -> f64 {
        delegate_wildcard_iterator!(self, intersection_sort_weight, prioritize_union_children)
    }
}

impl<'index> WildcardIterator<'index> for NewWildcardIterator<'index> {}

/// [`Suspended`]-mode counterpart of [`NewWildcardIterator`] used as
/// its [`RQEIteratorBoxed::Suspended`] type. Each variant holds the
/// `Suspended` form of the corresponding active variant, retaining the
/// `'query` lifetime so query-attached borrows stay valid across the
/// suspend/resume cycle.
///
/// `#[repr(C, u8)]` is required here for the same reason it is on
/// [`NewWildcardIterator`] — see invariant 1 there.
#[repr(C, u8)]
pub enum NewWildcardSuspended<'query> {
    /// Suspended counterpart of [`NewWildcardIterator::NotOptimized`].
    NotOptimized(RawWildcard<'query, Suspended>),
    /// Suspended counterpart of [`NewWildcardIterator::Optimized`].
    Optimized(OptimizedWildcardSuspended<'query>),
    /// Suspended counterpart of [`NewWildcardIterator::Empty`].
    Empty(Empty),
    /// Suspended counterpart of [`NewWildcardIterator::Disk`].
    Disk(DiskWildcardSuspended<'query>),
}

// Compile-time proof of invariant 1 on the
// `NewWildcardIterator`/`NewWildcardSuspended` pair, which the whole-box casts
// in `suspend` and `resume` below rely on.
//
// Both enums are `#[repr(C, u8)]` with the same four variants in the same
// order, which fixes the tag encoding (a `u8` counting from 0 in declaration
// order) and puts each payload at an offset derived from that payload's own
// alignment. That is a guarantee of the repr, so it needs stating rather than
// asserting. What remains:
//
// (a) **Arm correspondence.** `suspend` fills each payload slot with
//     `<active arm as RQEIteratorBoxed>::Suspended` — see
//     [`crate::boxed::suspend_child_slot_in_place`] — and only then relabels the
//     owning box, so the suspended enum's matching arm must *be* that
//     projection. Asserted as a type equality: that is the property the
//     reinterpretation needs, and a mismatch becomes a build error rather than
//     a silently mistyped payload.
//
// (b) **Per-arm payload layout identity.** Implied by (a) plus each arm's own
//     invariant (`RawWildcard`'s proof above, `RawOptimizedWildcard`'s proof in
//     this module, `Empty` being a ZST in both modes, and `DiskWildcardSuspended`
//     being a `#[repr(transparent)]` newtype over the same trait object).
//     Asserted per arm anyway so a regression names the arm that broke — under
//     `#[repr(C, u8)]` it is *per-arm* alignment, not the enum's, that pins the
//     payload offsets. (`offset_of!` into enum variants is not yet stable.)
//
// (c) **Enum size/alignment equality.** Needed on its own by `resume`'s
//     abort/error paths, which free an allocation created for the active enum
//     using `Layout::new::<NewWildcardSuspended>()`.
const _: () = {
    use std::mem::{align_of, size_of};

    /// Witnesses that `Self` and `T` are the same type: the blanket impl is the
    /// only one, so an `A: IsSame<B>` bound holds exactly when `A` *is* `B`.
    trait IsSame<T> {}
    impl<T> IsSame<T> for T {}

    /// (a) — fails to compile unless suspending `A` yields exactly `S`.
    const fn assert_suspends_to<A, S>()
    where
        A: RQEIteratorBoxed<'static>,
        A::Suspended: IsSame<S>,
    {
    }

    assert_suspends_to::<Wildcard<'static>, RawWildcard<'static, Suspended>>();
    assert_suspends_to::<OptimizedWildcard<'static>, OptimizedWildcardSuspended<'static>>();
    assert_suspends_to::<Empty, Empty>();
    assert_suspends_to::<DiskWildcardIterator<'static>, DiskWildcardSuspended<'static>>();

    // (b)
    assert!(size_of::<Wildcard<'static>>() == size_of::<RawWildcard<'static, Suspended>>());
    assert!(align_of::<Wildcard<'static>>() == align_of::<RawWildcard<'static, Suspended>>());
    assert!(
        size_of::<OptimizedWildcard<'static>>() == size_of::<OptimizedWildcardSuspended<'static>>()
    );
    assert!(
        align_of::<OptimizedWildcard<'static>>()
            == align_of::<OptimizedWildcardSuspended<'static>>()
    );
    assert!(
        size_of::<DiskWildcardIterator<'static>>() == size_of::<DiskWildcardSuspended<'static>>()
    );
    assert!(
        align_of::<DiskWildcardIterator<'static>>() == align_of::<DiskWildcardSuspended<'static>>()
    );

    // (c)
    assert!(
        size_of::<NewWildcardIterator<'static>>() == size_of::<NewWildcardSuspended<'static>>()
    );
    assert!(
        align_of::<NewWildcardIterator<'static>>() == align_of::<NewWildcardSuspended<'static>>()
    );
};

impl<'index> RQEIteratorBoxed<'index> for NewWildcardIterator<'index> {
    type Suspended = NewWildcardSuspended<'index>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // Transition the payload in place, arm by arm, then relabel the box.
        // Rebuilding instead — moving the arm out and `Box::new`-ing a fresh
        // enum — would relocate both the wrapper and the arm's payload, and
        // this is the type the FFI wrapper boxes, so the interior addresses it
        // hands to C (`header.current`) have to survive. The tag byte is
        // untouched, so the cast lands on the matching variant.
        //
        // SAFETY: `raw` came from `Box::into_raw` (non-null, aligned,
        // initialised, exclusively owned); the `&mut` borrow is confined to the
        // match.
        match unsafe { &mut *raw } {
            NewWildcardIterator::NotOptimized(it) => {
                // SAFETY: `it` is the valid, exclusively-owned payload; the
                // helper reinitialises the slot as its `Suspended` form in place.
                unsafe { crate::boxed::suspend_child_slot_in_place(it as *mut _) }
            }
            NewWildcardIterator::Optimized(it) => {
                // SAFETY: as above.
                unsafe { crate::boxed::suspend_child_slot_in_place(it as *mut _) }
            }
            NewWildcardIterator::Empty(it) => {
                // SAFETY: as above.
                unsafe { crate::boxed::suspend_child_slot_in_place(it as *mut _) }
            }
            NewWildcardIterator::Disk(it) => {
                // SAFETY: as above.
                unsafe { crate::boxed::suspend_child_slot_in_place(it as *mut _) }
            }
        }
        // SAFETY: every payload now holds its `Suspended` form at the same
        // offset and the tag encodes the same variant in both enums (invariant
        // 1, const proof above). `Box::from_raw` reuses the same allocation, so
        // the box address is preserved.
        unsafe { Box::from_raw(raw.cast::<NewWildcardSuspended<'index>>()) }
    }
}

impl<'query> RQESuspendedIterator<'query> for NewWildcardSuspended<'query> {
    type Resumed<'a>
        = NewWildcardIterator<'a>
    where
        'query: 'a;

    fn resume<'a>(
        self: Box<Self>,
        guard: &IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        let raw = Box::into_raw(self);
        // Resume the payload in place, arm by arm — mirroring `suspend`, so the
        // allocation and every interior address survive the full cycle. The tag
        // is untouched, so the cast below lands on the matching variant of the
        // active enum. An aborting arm aborts the whole wrapper: this enum owns
        // no result of its own to fall back to.
        //
        // SAFETY: `raw` came from `Box::into_raw` (non-null, aligned,
        // initialised, exclusively owned); the `&mut` borrow is confined to the
        // match. On `Unchanged`/`Moved` the helper rewrites the payload as its
        // resumed form; on `Aborted`/`Err` it consumes the payload, leaving it
        // uninitialised (handled below).
        let outcome = match unsafe { &mut *raw } {
            NewWildcardSuspended::NotOptimized(it) => {
                // SAFETY: `it` is the valid, exclusively-owned payload.
                unsafe { crate::boxed::resume_child_slot_in_place(it as *mut _, guard) }
            }
            NewWildcardSuspended::Optimized(it) => {
                // SAFETY: as above.
                unsafe { crate::boxed::resume_child_slot_in_place(it as *mut _, guard) }
            }
            NewWildcardSuspended::Empty(it) => {
                // SAFETY: as above.
                unsafe { crate::boxed::resume_child_slot_in_place(it as *mut _, guard) }
            }
            NewWildcardSuspended::Disk(it) => {
                // SAFETY: as above.
                unsafe { crate::boxed::resume_child_slot_in_place(it as *mut _, guard) }
            }
        };

        /// Frees the reused allocation after an arm consumed its payload,
        /// without dropping the uninitialised slot.
        ///
        /// # Safety
        ///
        /// `raw` must come from `Box::into_raw` on a `NewWildcardSuspended`
        /// whose payload has been consumed, leaving only the tag live.
        unsafe fn dealloc_after_arm_gone(raw: *mut NewWildcardSuspended<'_>) {
            // SAFETY: allocated by `Box` with exactly this layout, and nothing
            // in it is live any more.
            unsafe {
                std::alloc::dealloc(
                    raw.cast::<u8>(),
                    std::alloc::Layout::new::<NewWildcardSuspended<'_>>(),
                )
            };
        }

        match outcome {
            Ok(crate::boxed::ResumeSlotOutcome::Unchanged) => {
                // SAFETY: the payload holds its resumed form at the same offset
                // and the tag is unchanged; `Box::from_raw` reuses the same
                // allocation, so the box address — and any pointer the FFI
                // wrapper cached into the payload — stay valid.
                let active = unsafe { Box::from_raw(raw.cast::<NewWildcardIterator<'a>>()) };
                Ok(ResumeOutcome::Ok(active))
            }
            Ok(crate::boxed::ResumeSlotOutcome::Moved) => {
                // SAFETY: as above.
                let active = unsafe { Box::from_raw(raw.cast::<NewWildcardIterator<'a>>()) };
                Ok(ResumeOutcome::Moved(active))
            }
            Ok(crate::boxed::ResumeSlotOutcome::Aborted) => {
                // SAFETY: the arm consumed the payload; only the tag is left.
                unsafe { dealloc_after_arm_gone(raw) };
                Ok(ResumeOutcome::Aborted)
            }
            Err(e) => {
                // As `Aborted` — the arm consumed the payload.
                // SAFETY: as above.
                unsafe { dealloc_after_arm_gone(raw) };
                Err(e)
            }
        }
    }

    fn last_doc_id(&self) -> DocId {
        match self {
            NewWildcardSuspended::NotOptimized(it) => RQESuspendedIterator::last_doc_id(it),
            NewWildcardSuspended::Optimized(it) => RQESuspendedIterator::last_doc_id(it),
            NewWildcardSuspended::Empty(it) => RQESuspendedIterator::last_doc_id(it),
            NewWildcardSuspended::Disk(it) => RQESuspendedIterator::last_doc_id(it),
        }
    }

    fn num_estimated(&self) -> usize {
        match self {
            NewWildcardSuspended::NotOptimized(it) => RQESuspendedIterator::num_estimated(it),
            NewWildcardSuspended::Optimized(it) => RQESuspendedIterator::num_estimated(it),
            NewWildcardSuspended::Empty(it) => RQESuspendedIterator::num_estimated(it),
            NewWildcardSuspended::Disk(it) => RQESuspendedIterator::num_estimated(it),
        }
    }
}

/// Create a [`WildcardIterator`] for an index whose spec has
/// [`SchemaRule`](ffi::SchemaRule)`.index_all` set.
///
/// When [`spec.existingDocs`](ffi::IndexSpec::existingDocs) is non-null, the returned iterator
/// reads from the existing-documents inverted index (either
/// [`DocIdsOnly`] or [`RawDocIdsOnly`]
/// encoding). When it is null (no documents indexed yet), an [`Empty`] iterator
/// is returned instead.
///
/// # Safety
///
/// 1. `sctx` must point to a valid [`RedisSearchCtx`](ffi::RedisSearchCtx) that
///    remains valid for `'index`.
/// 2. `sctx.spec` must be a non-null pointer to a valid [`IndexSpec`](ffi::IndexSpec) that
///    remains valid for `'index`.
/// 3. `sctx.spec.rule` must be a non-null pointer to a valid [`SchemaRule`](ffi::SchemaRule) with
///    [`index_all`](ffi::SchemaRule::index_all) set to `true`.
/// 4. `sctx.spec.existingDocs`, when non-null, must point to a valid
///    [`opaque::InvertedIndex`] with either
///    [`DocIdsOnly`] or [`RawDocIdsOnly`]
///    encoding.
pub unsafe fn new_wildcard_iterator_optimized<'index>(
    sctx: NonNull<ffi::RedisSearchCtx>,
    weight: f64,
) -> NewWildcardIterator<'index> {
    // SAFETY: Caller guarantees `sctx` points to a valid `RedisSearchCtx` (1).
    let sctx_ref = unsafe { sctx.as_ref() };
    let spec = NonNull::new(sctx_ref.spec).expect("sctx.spec is null");
    // SAFETY: Caller guarantees `sctx.spec` is a valid, non-null pointer (2).
    let spec_ref = unsafe { spec.as_ref() };
    let rule = NonNull::new(spec_ref.rule).expect("sctx.spec.rule is null");
    // SAFETY: Caller guarantees `sctx.spec.rule` is a valid, non-null pointer (3).
    let rule_ref = unsafe { rule.as_ref() };
    debug_assert!(rule_ref.index_all);

    match NonNull::new(spec_ref.existingDocs) {
        Some(existing_docs) => {
            let ii = existing_docs.cast::<opaque::InvertedIndex>();
            // SAFETY: Caller guarantees `existingDocs` points to a valid
            // `opaque::InvertedIndex` with `DocIdsOnly` or `RawDocIdsOnly`
            // encoding (4).
            let ii_ref = unsafe { ii.as_ref() };
            let optimized = match ii_ref {
                opaque::InvertedIndex::DocIdsOnly(ii) => OptimizedWildcard::DocIdsOnly(
                    crate::inverted_index::Wildcard::new(ii.reader(), weight),
                ),
                opaque::InvertedIndex::RawDocIdsOnly(ii) => OptimizedWildcard::RawDocIdsOnly(
                    crate::inverted_index::Wildcard::new(ii.reader(), weight),
                ),
                _ => panic!("spec.existingDocs has the wrong inverted index type: {ii_ref:?}"),
            };
            NewWildcardIterator::Optimized(optimized)
        }
        None => NewWildcardIterator::Empty(Empty),
    }
}

/// Create a [`WildcardIterator`] backed by an on-disk index implementation.
///
/// This delegates to [`SEARCH_ENTERPRISE_ITERATORS`]'s
/// [`new_wildcard_on_disk`](crate::SearchEnterpriseIterators::new_wildcard_on_disk)
/// and wraps the resulting iterator in a [`DiskWildcardIterator`].
///
/// If the enterprise iterator cannot be created, this function populates
/// `status` (when non-null) with the cause and falls back to an empty iterator;
/// the query then aborts with an error rather than returning empty results.
///
/// # Safety
///
/// 1. `disk_spec` must reference a valid [`RedisSearchDiskIndexSpec`](ffi::RedisSearchDiskIndexSpec)
///    that remains valid for `'index`.
/// 2. [`SEARCH_ENTERPRISE_ITERATORS`] must be initialized before calling this function.
/// 3. `snapshot` must be a [`RedisSearchDiskSnapshot`](ffi::RedisSearchDiskSnapshot) handle
///    for `disk_spec` and must remain valid for `'index`.
/// 4. `status`, when non-null, must point to a valid [`QueryError`](ffi::QueryError).
pub unsafe fn new_wildcard_iterator_on_disk<'index>(
    disk_spec: &'index mut ffi::RedisSearchDiskIndexSpec,
    weight: f64,
    snapshot: std::ptr::NonNull<ffi::RedisSearchDiskSnapshot>,
    status: *mut ffi::QueryError,
) -> NewWildcardIterator<'index> {
    // SAFETY: Caller guarantees `SEARCH_ENTERPRISE_ITERATORS` is
    // initialized when `spec.diskSpec` is non-null (8).
    let enterprise_iters_api = SEARCH_ENTERPRISE_ITERATORS
        .get()
        .expect("SEARCH_ENTERPRISE_ITERATORS not initialized");
    // SAFETY: caller guarantees `status`, when non-null, points to a valid `QueryError` (4).
    let status = unsafe { QueryError::from_opaque_mut_ptr(status.cast()) };
    // On failure the enterprise implementation populates `status` with the
    // cause; we just fall back to an empty iterator so the query aborts via the
    // existing `QueryError_HasError` check rather than returning empty results.
    match enterprise_iters_api.new_wildcard_on_disk(disk_spec, weight, snapshot, status) {
        Ok(it) => NewWildcardIterator::Disk(it),
        Err(err) => {
            tracing::warn!(
                "Failed to create a disk wildcard iterator ({err}); falling back to empty iterator."
            );
            NewWildcardIterator::Empty(Empty)
        }
    }
}

/// Create a [`WildcardIterator`] from a query evaluation context.
///
/// There are three possible code paths:
///
/// 1. **Disk index** — when [`spec.diskSpec`](ffi::IndexSpec::diskSpec) is non-null, delegates to
///    [`SEARCH_ENTERPRISE_ITERATORS`]'s [`new_wildcard_on_disk`](crate::SearchEnterpriseIterators::new_wildcard_on_disk)
///    and wraps the result in a [`DiskWildcardIterator`].
/// 2. **[`index_all`](ffi::SchemaRule::index_all) optimized** — when
///    [`SchemaRule`](ffi::SchemaRule)`.index_all` is set, delegates to
///    [`new_wildcard_iterator_optimized`] which reads from the
///    [`existingDocs`](ffi::IndexSpec::existingDocs) inverted index.
/// 3. **Fallback** — creates a simple [`Wildcard`] iterator that yields all
///    document ids up to [`docTable.maxDocId`](ffi::DocTable::maxDocId).
///
/// # Safety
///
/// 1. `query` must point to a valid [`QueryEvalCtx`](ffi::QueryEvalCtx) that
///    remains valid for `'index`.
/// 2. `query.sctx` must be a non-null pointer to a valid
///    [`RedisSearchCtx`](ffi::RedisSearchCtx) that remains valid for `'index`.
/// 3. `query.sctx.spec` must be a non-null pointer to a valid [`IndexSpec`](ffi::IndexSpec) that
///    remains valid for `'index`.
/// 4. `query.sctx.spec.rule`, when non-null, must point to a valid [`SchemaRule`](ffi::SchemaRule).
/// 5. When [`SchemaRule`](ffi::SchemaRule)`.index_all` is true, the preconditions of
///    [`new_wildcard_iterator_optimized`] must also hold.
/// 6. `query.docTable` must be a non-null pointer to a valid [`DocTable`](ffi::DocTable) that
///    remains valid for `'index`.
/// 7. `query.sctx.spec.diskSpec`, when non-null, must point to a valid
///    [`RedisSearchDiskIndexSpec`](ffi::RedisSearchDiskIndexSpec) that remains valid for `'index`.
/// 8. When `query.sctx.spec.diskSpec` is non-null, [`SEARCH_ENTERPRISE_ITERATORS`] must be
///    initialized.
/// 9. When `query.sctx.spec.diskSpec` is non-null, `query.sctx.diskSnapshot` must be a
///    non-null [`RedisSearchDiskSnapshot`](ffi::RedisSearchDiskSnapshot) handle for
///    `query.sctx.spec.diskSpec` and must remain valid for `'index`.
pub unsafe fn new_wildcard_iterator<'index>(
    query: NonNull<ffi::QueryEvalCtx>,
    weight: f64,
) -> NewWildcardIterator<'index> {
    // SAFETY: Caller guarantees `query` points to a valid `QueryEvalCtx` (1).
    let query = unsafe { query.as_ref() };
    let sctx = NonNull::new(query.sctx).expect("query.sctx is null");
    // SAFETY: Caller guarantees `query.sctx` is a valid, non-null pointer (2).
    let sctx_ref = unsafe { sctx.as_ref() };
    // SAFETY: Caller guarantees `query.sctx.spec` is a valid, non-null pointer (3).
    let spec = unsafe { &*sctx_ref.spec };

    if !spec.diskSpec.is_null() {
        // SAFETY: Caller guarantees `spec.diskSpec` is a valid, non-null
        // pointer to a `RedisSearchDiskIndexSpec` that remains valid for
        // `'index` (7).
        let disk_spec = unsafe { &mut *spec.diskSpec };
        let snapshot = NonNull::new(sctx_ref.diskSnapshot)
            .expect("query.sctx.diskSnapshot is null for a disk-backed wildcard query");
        // SAFETY: Caller guarantees all preconditions of
        // `new_wildcard_iterator_on_disk` hold (7, 8, 9); `query.status` is the
        // valid `QueryError` of the evaluating query.
        return unsafe { new_wildcard_iterator_on_disk(disk_spec, weight, snapshot, query.status) };
    }

    let index_all = NonNull::new(spec.rule)
        .map(|rule| {
            // SAFETY: Caller guarantees `spec.rule`, when non-null, points to
            // a valid `SchemaRule` (4).
            let rule_ref = unsafe { rule.as_ref() };
            rule_ref.index_all
        })
        .unwrap_or_default();

    if index_all {
        // SAFETY: Caller guarantees the preconditions of
        // `new_wildcard_iterator_optimized` hold when `rule.index_all` is
        // true (5).
        unsafe { new_wildcard_iterator_optimized(sctx, weight) }
    } else {
        // SAFETY: Caller guarantees `query.docTable` is a valid, non-null
        // pointer (6).
        let doc_table = unsafe { &*query.docTable };
        NewWildcardIterator::NotOptimized(Wildcard::new(doc_table.maxDocId, weight))
    }
}

/// A wildcard iterator backed by an enterprise disk index iterator.
///
/// This is a thin wrapper around a [`Box<dyn RQEIterator>`] provided by
/// [`SEARCH_ENTERPRISE_ITERATORS`] that implements [`WildcardIterator`],
/// allowing disk-based wildcard queries to be used interchangeably with
/// in-memory ones.
pub type DiskWildcardIterator<'index> = Box<dyn RQEIteratorPrintable<'index> + 'index>;

/// [`DiskWildcardIterator`] matches all documents on the disk index.
impl<'index> WildcardIterator<'index> for DiskWildcardIterator<'index> {}

impl ProfilePrint for Wildcard<'_> {
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        ctx.print_leaf(c"WILDCARD", map);
    }
}

impl ProfilePrint for NewWildcardIterator<'_> {
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        match self {
            Self::NotOptimized(it) => it.print_profile(map, ctx),
            Self::Optimized(it) => it.print_profile(map, ctx),
            Self::Empty(it) => it.print_profile(map, ctx),
            Self::Disk(it) => it.print_profile(map, ctx),
        }
    }
}

/// Suspended counterpart of [`DiskWildcardIterator`], used as its
/// [`RQEIteratorBoxed::Suspended`] type.
///
/// Wraps the **same trait object** as [`DiskWildcardIterator`] — `dyn
/// RQEIteratorPrintable`, at the same lifetime. Two things about that are
/// load-bearing:
///
/// * **Same trait.** A `dyn Sub` and a `dyn Super` do *not* share a vtable —
///   vtable layout is unspecified, and upcasting is a coercion that may load a
///   different pointer — so a wrapper typed at the [`RQEIterator`] supertrait
///   would leave the value carrying metadata for the wrong trait.
/// * **Same lifetime.** The disk iterator is opaque: unlike every other
///   suspended type in this crate, there is no `Rf` mode to weaken its
///   index-derived state to raw pointers, so its borrow of the disk spec
///   necessarily stays live for the whole suspend/resume cycle. That is exactly
///   what `'query` denotes here (see [`RQESuspendedIterator`]) — borrows that
///   survive the cycle rather than being re-derived from the guard on resume.
///   Erasing it to `'static` instead would let safe code suspend an iterator,
///   drop the disk spec it borrows, and then drop the suspended box, running
///   the backend's destructor against dangling borrows.
///
/// This makes [`suspend`](RQEIteratorBoxed::suspend) a pure newtype wrap with
/// no lifetime change at all; only [`resume`](RQESuspendedIterator::resume)
/// adjusts one, shortening `'query` to the guard's lifetime.
#[repr(transparent)]
pub struct DiskWildcardSuspended<'query>(pub(crate) Box<dyn RQEIteratorPrintable<'query> + 'query>);

impl<'index> RQEIteratorBoxed<'index> for DiskWildcardIterator<'index> {
    type Suspended = DiskWildcardSuspended<'index>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // SAFETY: `DiskWildcardIterator<'index>` *is*
        // `Box<dyn RQEIteratorPrintable<'index> + 'index>` (a type alias), and
        // `DiskWildcardSuspended<'index>` is a `#[repr(transparent)]` newtype
        // over that very type — same trait, same lifetime — so this is a
        // newtype wrap rather than any reinterpretation of the value.
        // `Box::from_raw` reuses the same heap allocation.
        unsafe { Box::from_raw(raw as *mut DiskWildcardSuspended<'index>) }
    }
}

impl<'query> RQESuspendedIterator<'query> for DiskWildcardSuspended<'query> {
    type Resumed<'a>
        = DiskWildcardIterator<'a>
    where
        'query: 'a;

    fn resume<'a>(
        self: Box<Self>,
        spec: &IndexSpecReadGuard<'a>,
    ) -> Result<ResumeOutcome<Box<Self::Resumed<'a>>>, RQEIteratorError>
    where
        'query: 'a,
    {
        let raw = Box::into_raw(self);
        // SAFETY: unwraps the `#[repr(transparent)]` newtype and shortens the
        // inner trait object's lifetime from `'query` to the caller's `'a`,
        // which `'query: 'a` permits. The cast is needed because `'index`
        // appears behind `&mut` in [`RQEIterator::current`]'s return type, which
        // makes `dyn RQEIteratorPrintable<'index>` invariant and so blocks the
        // implicit coercion; shortening is nonetheless sound, since a value
        // valid for `'query` is valid for any shorter `'a`.
        // `Box::from_raw` reuses the same heap allocation.
        let mut active = unsafe { Box::from_raw(raw as *mut DiskWildcardIterator<'a>) };
        // Drive validity recovery through the inner trait object's
        // `revalidate` callback — the same path the legacy
        // `Suspendable::resume` used. Reduce the borrowing `RQEValidateStatus`
        // to a `Copy` status discriminant first so the mutable borrow of
        // `active` ends before we move it into the outcome; propagate a
        // revalidate error (e.g. timeout) rather than masking it.
        let status = match active.revalidate(spec)? {
            RQEValidateStatus::Ok => ffi::ValidateStatus_VALIDATE_OK,
            RQEValidateStatus::Moved { .. } => ffi::ValidateStatus_VALIDATE_MOVED,
            RQEValidateStatus::Aborted => ffi::ValidateStatus_VALIDATE_ABORTED,
        };
        Ok(match status {
            ffi::ValidateStatus_VALIDATE_OK => ResumeOutcome::Ok(active),
            ffi::ValidateStatus_VALIDATE_MOVED => ResumeOutcome::Moved(active),
            // `Aborted`: `active` is not moved into the outcome, so the inner
            // trait object drops here.
            _ => ResumeOutcome::Aborted,
        })
    }

    fn last_doc_id(&self) -> DocId {
        // Forwards into the backend iterator, which `'query` keeps borrow-checked
        // for the whole suspend window, so the dispatch and the receiver are both
        // sound. What is *not* type-enforced is that the backend answers from
        // cached state: `RQEIterator` does not require it, and a backend that
        // recomputed this from the disk spec would be reading it with the index
        // spec lock released. Every implementation in the workspace caches.
        RQEIterator::last_doc_id(&*self.0)
    }

    fn num_estimated(&self) -> usize {
        // Cached count — see `last_doc_id`, including the caveat.
        RQEIterator::num_estimated(&*self.0)
    }
}

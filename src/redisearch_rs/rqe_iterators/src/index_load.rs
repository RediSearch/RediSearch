/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Index loading: a decorator that hands the numeric value a hit was matched
//! on to the result pipeline, as a metric, so the pipeline reads it as a field
//! instead of loading it from the document.
//!
//! This is the iterator half of the `$as` query attribute, an unstable
//! feature. The value reaches the row the same way a vector range query's
//! distance does: the decorator pushes it into the hit's
//! [`metrics`](RSIndexResult::metrics_mut) under the [`RLookupKey`] the
//! pipeline resolved for the alias, and the metrics loader writes it into the
//! row.

use std::ptr::NonNull;

use ffi::QueryIterator;
use index_result::RSIndexResult;
use index_spec::IndexSpecReadGuard;
use ref_mode::{Active, Ref, Suspended};
use rlookup::{KeyHandleSlot, RLookupKey, RLookupKeyHandle};
use rqe_core::DocId;

use crate::{
    IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    RQEValidateStatus, ResumeOutcome, SkipToOutcome,
    boxed::{ResumeSlotOutcome, resume_child_slot_in_place, suspend_child_slot_in_place},
    c2rust::CRQEIterator,
    interop::{ProfileChildren, RQEIteratorWrapper},
    profile_print::{ProfilePrint, ProfilePrintCtx},
    score_bound::{BlockBound, MinCompetitiveScore},
};

/// The heap cell pipeline construction writes the alias's [`RLookupKey`]
/// into, through an [`RLookupKeyHandle`] that points at it.
///
/// It lives on the heap, apart from the decorator, because the decorator moves
/// after the handle is bound: profiling re-boxes every compound iterator, and
/// it runs before pipeline construction writes the key.
///
/// # Invariants
///
/// The pointer comes from [`Box::into_raw`] and is owned by this slot until it
/// is dropped.
struct KeySlot<'query>(NonNull<*mut RLookupKey<'query>>);

impl<'query> KeySlot<'query> {
    fn new() -> Self {
        Self(
            NonNull::new(Box::into_raw(Box::new(std::ptr::null_mut())))
                .expect("Box pointers are non-null"),
        )
    }

    /// The key pipeline construction bound, or null while it has bound none.
    fn key(&self) -> *mut RLookupKey<'query> {
        // SAFETY: the cell is live by the type invariant. Pipeline construction
        // writes it only while it builds the pipeline, never while the
        // iterator runs.
        unsafe { *self.0.as_ptr() }
    }
}

impl Drop for KeySlot<'_> {
    fn drop(&mut self) {
        // SAFETY: by the type invariant the pointer came from `Box::into_raw`
        // and nothing else owns it.
        drop(unsafe { Box::from_raw(self.0.as_ptr()) });
    }
}

/// A decorator that attaches the numeric value of each hit of its child to
/// the hit, as a metric under the alias's [`RLookupKey`].
///
/// Parameterised over a [`Ref`] mode — see [`IndexLoad`] for the [`Active`]
/// instantiation that implements [`RQEIterator`].
///
/// The value is the hit's own number when the hit is a numeric leaf, and
/// otherwise the first numeric record among its children: a numeric range
/// over several range-tree leaves is a union, which keeps the value in the
/// child that produced the document. Parents move a child's metrics into
/// their own result, so the metric is pushed afresh on every positioning,
/// replacing whatever the hit carried.
///
/// Until pipeline construction binds a key, the decorator pushes nothing: the
/// metrics loader writes every metric through its key, and a null one cannot
/// be written.
///
/// # Invariants
///
/// 1. **Layout compatibility across modes.** `RawIndexLoad` is `#[repr(C)]`
///    and carries no `Rf`-dependent field other than the zero-sized
///    `_marker`, so its `Active` and `Suspended` instantiations are
///    layout-identical given that the child `I` and its `I::Suspended` are,
///    which [`suspend_child_slot_in_place`] enforces. The `const` block below
///    proves the wrapper's own part.
#[repr(C)]
pub struct RawIndexLoad<'query, Rf: Ref, I> {
    child: I,
    key: KeySlot<'query>,
    /// The handle tracking [`key`](Self::key), invalidated when this iterator
    /// is dropped or the handle is replaced.
    key_handle: KeyHandleSlot<'query>,
    _marker: std::marker::PhantomData<Rf>,
}

/// Alias for an [`Active`] [`RawIndexLoad`] — the only instantiation with an
/// [`RQEIterator`] impl.
pub type IndexLoad<'index, I> = RawIndexLoad<'index, Active<'index>, I>;

// Compile-time proof of invariant 1 on `RawIndexLoad`, for a representative
// child whose `Active`/`Suspended` forms differ but stay layout-compatible.
const _: () = {
    use crate::Wildcard;
    use std::mem::{align_of, offset_of, size_of};
    type AChild = Wildcard<'static>;
    type SChild = <Wildcard<'static> as RQEIteratorBoxed<'static>>::Suspended;
    type A = RawIndexLoad<'static, Active<'static>, AChild>;
    type S = RawIndexLoad<'static, Suspended, SChild>;
    assert!(offset_of!(A, child) == offset_of!(S, child));
    assert!(offset_of!(A, key) == offset_of!(S, key));
    assert!(offset_of!(A, key_handle) == offset_of!(S, key_handle));
    assert!(size_of::<A>() == size_of::<S>());
    assert!(align_of::<A>() == align_of::<S>());
};

/// The value index loading attaches to `result`: its own number, or the first
/// one among its children, depth first.
fn hit_value(result: &RSIndexResult<'_>) -> Option<f64> {
    if let Some(value) = result.as_numeric() {
        return Some(value);
    }
    let aggregate = result.as_aggregate()?;
    (0..aggregate.len()).find_map(|i| aggregate.get(i).and_then(hit_value))
}

/// Replace `result`'s metrics with `(key, value of the hit)`, or leave them
/// untouched when `key` is null or the hit holds no number.
fn attach<'index>(result: &mut RSIndexResult<'index>, key: *mut RLookupKey<'index>) {
    if key.is_null() {
        return;
    }
    let Some(value) = hit_value(result) else {
        return;
    };
    let metrics = result.metrics_mut();
    metrics.reset();
    // The metrics vector types the key the way C sees it: the header
    // `RLookupKey` is exported under, which is this key's first field.
    //
    // SAFETY: `key` is non-null per the check above. It is the key pipeline
    // construction bound, owned by the pipeline's lookup, which outlives every
    // result the iterator tree yields.
    metrics.push_with_key(unsafe { &*key.cast::<ffi::RLookupKey>() }, value);
}

impl<'index, I: RQEIterator<'index>> IndexLoad<'index, I> {
    /// Decorate `child`, with no key bound yet.
    pub fn new(child: I) -> Self {
        Self {
            child,
            key: KeySlot::new(),
            key_handle: KeyHandleSlot::new(),
            _marker: std::marker::PhantomData,
        }
    }

    /// The decorated iterator.
    pub const fn child(&self) -> &I {
        &self.child
    }

    /// The slot pipeline construction writes the bound key into.
    ///
    /// The slot lives on the heap, so the pointer stays valid across moves of
    /// the decorator, until it is dropped.
    pub const fn key_slot(&mut self) -> NonNull<*mut RLookupKey<'index>> {
        self.key.0
    }

    /// Set the [`RLookupKeyHandle`] that tracks [`key_slot`](Self::key_slot),
    /// invalidating any different previously attached handle.
    ///
    /// # Safety
    ///
    /// The conditions of [`KeyHandleSlot::set`], with this iterator as the
    /// slot's owner.
    pub unsafe fn set_handle(&mut self, key_handle: Option<NonNull<RLookupKeyHandle<'index>>>) {
        // SAFETY: the caller upholds `KeyHandleSlot::set`'s conditions.
        unsafe { self.key_handle.set(key_handle) };
    }

    fn bound_key(&self) -> *mut RLookupKey<'index> {
        self.key.key()
    }
}

impl<'index, I: RQEIterator<'index>> RQEIterator<'index> for IndexLoad<'index, I> {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        self.child.current()
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        let key = self.bound_key();
        let mut result = self.child.read()?;
        if let Some(result) = result.as_deref_mut() {
            attach(result, key);
        }
        Ok(result)
    }

    fn skip_to(
        &mut self,
        doc_id: DocId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        let key = self.bound_key();
        // A `NotFound` hit is attached too: parents keep it as the next
        // candidate and consume it later without reading the child again.
        Ok(self.child.skip_to(doc_id)?.map(|outcome| match outcome {
            SkipToOutcome::Found(result) => {
                attach(result, key);
                SkipToOutcome::Found(result)
            }
            SkipToOutcome::NotFound(result) => {
                attach(result, key);
                SkipToOutcome::NotFound(result)
            }
        }))
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
        let key = self.bound_key();
        Ok(match self.child.revalidate(spec)? {
            RQEValidateStatus::Moved {
                current: Some(result),
            } => {
                attach(result, key);
                RQEValidateStatus::Moved {
                    current: Some(result),
                }
            }
            status => status,
        })
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        IteratorType::IndexLoad
    }

    fn intersection_sort_weight(&self, prioritize_union_children: bool) -> f64 {
        self.child
            .intersection_sort_weight(prioritize_union_children)
    }

    fn reports_score_bounds(&self) -> bool {
        self.child.reports_score_bounds()
    }

    fn current_max_score(&self) -> f64 {
        self.child.current_max_score()
    }

    fn current_block_bound(&self) -> Option<BlockBound> {
        self.child.current_block_bound()
    }

    fn read_with_threshold(
        &mut self,
        min_competitive_score: MinCompetitiveScore,
    ) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        let key = self.bound_key();
        let mut result = self.child.read_with_threshold(min_competitive_score)?;
        if let Some(result) = result.as_deref_mut() {
            attach(result, key);
        }
        Ok(result)
    }
}

/// Transparent: the profile shows the decorated iterator as it would without
/// index loading.
impl<'index, I> ProfilePrint for IndexLoad<'index, I>
where
    I: RQEIterator<'index> + ProfilePrint,
{
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        self.child.print_profile(map, ctx);
    }
}

impl<'index> ProfileChildren<'index> for IndexLoad<'index, CRQEIterator> {
    fn profile_children(self) -> Self {
        let Self {
            child,
            key,
            key_handle,
            _marker,
        } = self;
        Self {
            child: child.into_profiled(),
            key,
            key_handle,
            _marker,
        }
    }
}

impl<'index, I> RQEIteratorBoxed<'index> for IndexLoad<'index, I>
where
    I: RQEIteratorBoxed<'index>,
{
    type Suspended = RawIndexLoad<'index, Suspended, I::Suspended>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        // The child is suspended through its own `suspend`, in place, rather
        // than by reinterpreting the whole box: a type-erased child swaps its
        // vtable on the transition. Reusing the allocation keeps the child's
        // address, which the C wrapper's cached `current` points into.
        let raw = Box::into_raw(self);
        // SAFETY: `raw` came from `Box::into_raw` (non-null, aligned,
        // initialised, exclusively owned). `&raw mut` forms a field pointer to
        // the `child` slot without creating a reference.
        let child_slot = unsafe { &raw mut (*raw).child };
        // SAFETY: a field pointer derived from the non-null `raw` is non-null.
        let child_slot = unsafe { NonNull::new_unchecked(child_slot) };
        // SAFETY: `child_slot` points at the valid, exclusively-owned active
        // child; the helper leaves a valid `I::Suspended` in its place.
        unsafe { suspend_child_slot_in_place(child_slot) };
        // SAFETY: the child slot now holds a valid `I::Suspended`, and the
        // other fields carry no `Rf` (invariant 1), so the allocation is a
        // valid `RawIndexLoad<Suspended, I::Suspended>`. `Box::from_raw`
        // reuses the same allocation.
        unsafe { Box::from_raw(raw.cast::<RawIndexLoad<'index, Suspended, I::Suspended>>()) }
    }
}

/// Free a [`RawIndexLoad`] allocation whose `child` slot was consumed by an
/// aborted or failed child resume, without dropping the child.
///
/// # Safety
///
/// 1. `raw` came from `Box::into_raw` and is still exclusively owned by the
///    caller.
/// 2. `(*raw).child` is moved-from and must not be dropped.
unsafe fn dealloc_after_child_gone<S>(raw: *mut RawIndexLoad<'_, Suspended, S>) {
    // SAFETY: `raw` is valid (1); `&raw mut` forms a field pointer without a
    // reference.
    let key = unsafe { &raw mut (*raw).key };
    // SAFETY: `key` is still a valid, owned value; drop it in place.
    unsafe { std::ptr::drop_in_place(key) };
    // SAFETY: as for `key`.
    let key_handle = unsafe { &raw mut (*raw).key_handle };
    // SAFETY: as for `key`.
    unsafe { std::ptr::drop_in_place(key_handle) };
    // SAFETY: `raw` was allocated by `Box` with exactly this layout (1); the
    // `child` slot is moved-from (2), and every other field was dropped above
    // or is a ZST.
    unsafe {
        std::alloc::dealloc(
            raw.cast::<u8>(),
            std::alloc::Layout::new::<RawIndexLoad<'_, Suspended, S>>(),
        )
    };
}

impl<'query, S> RQESuspendedIterator<'query> for RawIndexLoad<'query, Suspended, S>
where
    S: RQESuspendedIterator<'query>,
{
    type Resumed<'a>
        = IndexLoad<'a, S::Resumed<'a>>
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
        // SAFETY: `raw` came from `Box::into_raw` (non-null, aligned,
        // initialised, exclusively owned). `&raw mut` forms a field pointer to
        // the `child` slot without creating a reference.
        let child_slot = unsafe { &raw mut (*raw).child };
        // SAFETY: a field pointer derived from the non-null `raw` is non-null.
        let child_slot = unsafe { NonNull::new_unchecked(child_slot) };
        // SAFETY: `child_slot` points at the valid, exclusively-owned suspended
        // child. On `Unchanged`/`Moved` the helper leaves a valid
        // `S::Resumed<'a>` in the slot; on `Aborted`/`Err` it consumed the
        // child, which the teardown arms below account for.
        match unsafe { resume_child_slot_in_place(child_slot, guard) } {
            Ok(outcome @ (ResumeSlotOutcome::Unchanged | ResumeSlotOutcome::Moved)) => {
                // SAFETY: the child slot now holds a valid `S::Resumed<'a>`,
                // and the other fields carry no `Rf` (invariant 1), so the
                // allocation is a valid `IndexLoad<'a, S::Resumed<'a>>`.
                // `Box::from_raw` reuses the same allocation.
                let mut active =
                    unsafe { Box::from_raw(raw.cast::<IndexLoad<'a, S::Resumed<'a>>>()) };
                if matches!(outcome, ResumeSlotOutcome::Moved) {
                    let key = active.bound_key();
                    if let Some(result) = active.child.current() {
                        attach(result, key);
                    }
                    Ok(ResumeOutcome::Moved(active))
                } else {
                    Ok(ResumeOutcome::Ok(active))
                }
            }
            Ok(ResumeSlotOutcome::Aborted) => {
                // SAFETY: the child was consumed by its `resume`.
                unsafe { dealloc_after_child_gone(raw) };
                Ok(ResumeOutcome::Aborted)
            }
            Err(e) => {
                // SAFETY: as the `Aborted` arm — the child is gone.
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

/// Decorate the C-ABI iterator `child` and lower the decorator to the C ABI.
pub fn boxed_new(child: CRQEIterator) -> NonNull<QueryIterator> {
    RQEIteratorWrapper::boxed_new_compound(IndexLoad::new(child))
}

/// The decorator behind `header`.
///
/// # Safety
///
/// 1. `header` was returned by [`boxed_new`] and is still live.
/// 2. The caller holds that iterator exclusively for as long as the returned
///    reference is used, and picks an `'index` its key slot may borrow for.
const unsafe fn decorator<'index>(
    header: NonNull<QueryIterator>,
) -> &'index mut IndexLoad<'index, CRQEIterator> {
    // SAFETY: (1): the header is live.
    let type_ = unsafe { header.as_ref() }.type_;
    debug_assert!(
        matches!(type_, IteratorType::IndexLoad),
        "expected an index-loading decorator"
    );
    // SAFETY: (1) and (2).
    unsafe {
        &mut RQEIteratorWrapper::<IndexLoad<'index, CRQEIterator>>::mut_ref_from_header_ptr(header)
            .inner
    }
}

/// The key slot of the decorator behind `header`, as [`IndexLoad::key_slot`].
///
/// # Safety
///
/// 1. `header` was returned by [`boxed_new`] and is still live.
/// 2. The caller holds that iterator exclusively for the duration of the call.
/// 3. `'index` must not outlive the storage the slot's key borrows, as for
///    [`metric::own_key_ref`](crate::metric::own_key_ref).
pub const unsafe fn own_key_ref<'index>(
    header: NonNull<QueryIterator>,
) -> NonNull<*mut RLookupKey<'index>> {
    // SAFETY: (1), (2) and (3).
    unsafe { decorator(header) }.key_slot()
}

/// Give the decorator behind `header` the [`RLookupKeyHandle`] it invalidates
/// when freed, as [`IndexLoad::set_handle`].
///
/// # Safety
///
/// 1. `header` was returned by [`boxed_new`] and is still live.
/// 2. The caller holds that iterator exclusively for the duration of the call.
/// 3. The conditions of [`KeyHandleSlot::set`], with the iterator as the
///    slot's owner.
pub unsafe fn set_key_handle(
    header: NonNull<QueryIterator>,
    handle: Option<NonNull<RLookupKeyHandle<'_>>>,
) {
    // SAFETY: (1) and (2).
    let decorator = unsafe { decorator(header) };
    // SAFETY: (3).
    unsafe { decorator.set_handle(handle) };
}

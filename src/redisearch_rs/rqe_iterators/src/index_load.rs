/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Index loading: a decorator that hands the numeric value each hit was
//! matched on to the result pipeline as a metric, so the pipeline reads it as a
//! field instead of loading it from the document.
//!
//! This is the iterator half of the `$as` query attribute, an unstable
//! feature. The value reaches the row the way a vector range query's distance
//! does: through the hit's [`metrics`](RSIndexResult::metrics_mut), under the
//! [`RLookupKey`] pipeline construction resolved for the alias.

use std::{cell::Cell, mem::offset_of, ptr::NonNull, rc::Rc};

use ffi::QueryIterator;
use index_result::RSIndexResult;
use index_spec::IndexSpecReadGuard;
use rlookup::{KeyHandleSlot, RLookupKey, RLookupKeyHandle};
use rqe_core::DocId;

use crate::{
    IteratorType, RQEIterator, RQEIteratorBoxed, RQEIteratorError, RQESuspendedIterator,
    RQEValidateStatus, ResumeOutcome, SkipToOutcome,
    boxed::{
        ResumeSlotOutcome, assert_layout_compatible, finish_resume_in_place,
        resume_child_slot_in_place, suspend_child_slot_in_place,
    },
    c2rust::CRQEIterator,
    interop::{ProfileChildren, RQEIteratorWrapper, rust_profile_children},
    profile_print::{ProfilePrint, ProfilePrintCtx},
    score_bound::{BlockBound, MinCompetitiveScore},
};

/// The key pipeline construction writes for the alias, and the handle it
/// writes it through.
struct KeyBinding<'query> {
    /// Null until pipeline construction writes it through
    /// [`key_slot`](IndexLoad::key_slot), after binding; `FT.PROFILE` then
    /// re-boxes (moves) the decorator, so the cell needs an address that
    /// survives the move. Moving a [`Box`] retags it as unique, invalidating
    /// the pointer C holds (Miri flags it); moving an [`Rc`] does not. Each hit
    /// costs one load, with no refcount traffic.
    key: Rc<Cell<*mut RLookupKey<'query>>>,
    handle: KeyHandleSlot<'query>,
}

/// A decorator that attaches the numeric value of each hit of its child to the
/// hit, as a metric under the alias's [`RLookupKey`].
///
/// One `#[repr(C)]` struct serves both modes: `I` is the active child in an
/// [`IndexLoad`] and its suspended form otherwise. The decorator's own field
/// does not depend on the mode, so the two forms are layout-identical when the
/// child's are, which [`suspend_child_slot_in_place`] enforces.
///
/// It reports its child's [`type_`](RQEIterator::type_), so it is invisible to
/// everything that dispatches on the type: loading is an option of the clause,
/// not a kind of iterator.
///
/// The value is the hit's own number, or else a numeric record among its
/// children: a range over several range-tree leaves is a union, which keeps the
/// value in the child that produced the document. When several values of one
/// document match, which one is loaded is unspecified. It is the value the index
/// stores, which a tree built with `search-_numeric-compress` may round.
///
/// Until pipeline construction writes the key, nothing is attached.
#[repr(C)]
pub struct RawIndexLoad<'query, I> {
    child: I,
    binding: KeyBinding<'query>,
}

/// The active form of [`RawIndexLoad`].
pub type IndexLoad<'index, I> = RawIndexLoad<'index, I>;

impl<'index, I: RQEIterator<'index>> IndexLoad<'index, I> {
    /// Decorate `child`, with no key bound yet.
    pub fn new(child: I) -> Self {
        // Reporting the child's type is sound only for types no code casts on by
        // tag. Of the types it wraps, only `Union` is cast by tag, by
        // `TrimUnionIterator`, whose only caller already skips loading trees.
        debug_assert!(
            matches!(
                child.type_(),
                IteratorType::InvIdxNumeric
                    | IteratorType::Union
                    | IteratorType::Empty
                    | IteratorType::Mock
            ),
            "index loading wraps a {} iterator",
            child.type_()
        );
        Self {
            child,
            binding: KeyBinding {
                key: Rc::new(Cell::new(std::ptr::null_mut())),
                handle: KeyHandleSlot::new(),
            },
        }
    }

    /// The slot pipeline construction writes the key into. It stays at this
    /// address until the decorator is dropped, however the decorator moves.
    pub fn key_slot(&self) -> *mut *mut RLookupKey<'index> {
        self.binding.key.as_ptr()
    }

    /// Set the [`RLookupKeyHandle`] that points at [`key_slot`](Self::key_slot),
    /// invalidating any different previously attached handle.
    ///
    /// # Safety
    ///
    /// 1. The conditions of [`KeyHandleSlot::set`], with this iterator as the
    ///    slot's owner.
    pub unsafe fn set_handle(&mut self, handle: Option<NonNull<RLookupKeyHandle<'index>>>) {
        // SAFETY: 1.
        unsafe { self.binding.handle.set(handle) };
    }
}

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
///
/// Parents move a child's metrics into their own result, so the metric is
/// pushed afresh on every positioning.
fn attach<'index>(result: &mut RSIndexResult<'index>, key: *mut RLookupKey<'index>) {
    // SAFETY: a non-null `key` was written by pipeline construction and is
    // owned by the pipeline's lookup, which outlives every result the iterator
    // tree yields. `RLookupKey` starts with the `#[repr(C)]` header the metrics
    // vector types it as.
    let Some(key) = (unsafe { key.cast::<ffi::RLookupKey>().as_ref() }) else {
        return;
    };
    let Some(value) = hit_value(result) else {
        return;
    };
    let metrics = result.metrics_mut();
    metrics.reset();
    metrics.push_with_key(key, value);
}

impl<'index, I: RQEIterator<'index>> RQEIterator<'index> for IndexLoad<'index, I> {
    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        self.child.current()
    }

    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        let key = self.binding.key.get();
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
        let key = self.binding.key.get();
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
        let key = self.binding.key.get();
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
        self.child.type_()
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
        let key = self.binding.key.get();
        let mut result = self.child.read_with_threshold(min_competitive_score)?;
        if let Some(result) = result.as_deref_mut() {
            attach(result, key);
        }
        Ok(result)
    }
}

/// Transparent: the profile shows the child as it would without index loading.
impl<I: ProfilePrint> ProfilePrint for RawIndexLoad<'_, I> {
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        self.child.print_profile(map, ctx);
    }
}

impl<'index> ProfileChildren<'index> for IndexLoad<'index, CRQEIterator> {
    fn profile_children(self) -> Self {
        Self {
            child: self.child.into_profiled(),
            binding: self.binding,
        }
    }
}

impl<'index, I: RQEIteratorBoxed<'index>> RQEIteratorBoxed<'index> for IndexLoad<'index, I> {
    type Suspended = RawIndexLoad<'index, I::Suspended>;

    /// Suspends the child in place and relabels the allocation, which must not
    /// move: the C wrapper resumes into the allocation it suspended.
    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        const { assert_layout_compatible::<Self, Self::Suspended>() };
        let raw = Box::into_raw(self);
        let child = raw.wrapping_byte_add(offset_of!(Self, child)).cast::<I>();
        // SAFETY: `child` is the child field of the live allocation `raw`
        // exclusively owns, which holds a valid `I`.
        unsafe { suspend_child_slot_in_place(NonNull::new(child).expect("non-null field")) };
        // SAFETY: `raw` came from `Box::into_raw`. With the child field holding
        // an `I::Suspended` the allocation is a valid `Self::Suspended`: the
        // other field does not depend on the mode, and the layout check above
        // with `#[repr(C)]` puts both fields at the same offsets.
        unsafe { Box::from_raw(raw.cast::<Self::Suspended>()) }
    }
}

impl<'query, S: RQESuspendedIterator<'query>> RQESuspendedIterator<'query>
    for RawIndexLoad<'query, S>
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
        let child = raw.wrapping_byte_add(offset_of!(Self, child)).cast::<S>();
        // SAFETY: `child` is the child field of the live allocation `raw`
        // exclusively owns, which holds a valid `S`.
        let slot = unsafe {
            resume_child_slot_in_place(NonNull::new(child).expect("non-null field"), guard)
        };
        if !matches!(
            slot,
            Ok(ResumeSlotOutcome::Unchanged | ResumeSlotOutcome::Moved)
        ) {
            let binding = raw
                .wrapping_byte_add(offset_of!(Self, binding))
                .cast::<KeyBinding<'query>>();
            // SAFETY: the child is gone, but the binding is still valid and
            // owned, and the allocation is freed below without reading it again.
            unsafe { std::ptr::drop_in_place(binding) };
        }
        // SAFETY: 1. `raw` came from `Box::into_raw` and nothing borrows into
        // it. 2. `slot` is its child's outcome, and on a teardown outcome the
        // binding was dropped above. 3. With the child resumed, the allocation is
        // a valid `Self::Resumed<'a>`, as in `suspend`; the binding narrows from
        // `'query` to `'a`, which the pipeline's key outlives.
        let outcome = unsafe { finish_resume_in_place::<Self, Self::Resumed<'a>>(raw, slot) }?;
        // A move repositions the child onto a hit whose value was never
        // attached, or was taken by a parent.
        Ok(match outcome {
            ResumeOutcome::Moved(mut active) => {
                let key = active.binding.key.get();
                if let Some(result) = active.child.current() {
                    attach(result, key);
                }
                ResumeOutcome::Moved(active)
            }
            outcome => outcome,
        })
    }

    fn last_doc_id(&self) -> DocId {
        self.child.last_doc_id()
    }

    fn num_estimated(&self) -> usize {
        self.child.num_estimated()
    }
}

/// Lower `decorator` to the C ABI, with the `ProfileChildren` callback that
/// profiles its child.
pub fn boxed_new(decorator: IndexLoad<'_, CRQEIterator>) -> NonNull<QueryIterator> {
    // Not `boxed_new_compound`, which asserts a compound type: the decorator
    // reports its child's, which may be a leaf's.
    RQEIteratorWrapper::boxed_new_inner(
        decorator,
        Some(rust_profile_children::<IndexLoad<'_, CRQEIterator>>),
    )
}

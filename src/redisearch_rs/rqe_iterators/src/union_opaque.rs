/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Dynamic dispatch wrapper over the concrete union variants.
//!
//! [`UnionOpaque`] is the type that sits behind every
//! [`RQEIteratorWrapper`](crate::interop::RQEIteratorWrapper) produced by the
//! FFI `NewUnionIterator` constructor. It holds one of the five concrete
//! union variants and forwards every [`RQEIterator`] call via match dispatch.
//!
//! This module lives in `rqe_iterators` (rather than in the FFI bridge crate)
//! so that [`c2rust::CRQEIterator`](crate::c2rust::CRQEIterator) can recover
//! the wrapper via
//! [`ref_from_header_ptr`](crate::interop::RQEIteratorWrapper::ref_from_header_ptr)
//! and call methods such as [`UnionOpaque::num_children_active`] directly,
//! without going through a C FFI trampoline.

use std::ffi::c_char;

use ffi::{QueryNodeType, ValidateStatus, t_docId};
use index_result::RSIndexResult;
use ref_mode::{Active, Ref, Suspended};

use crate::{
    IteratorType, RQEIterator, RQEIteratorError, RQESuspendedIterator, SkipToOutcome, UnionFullFlat, union_flat::RawUnionFlat,
    union_heap::RawUnionHeap, union_trimmed::RawUnionTrimmed,
};

use index_spec::IndexSpecReadGuard;

/// Enum holding all possible union iterator variants, parameterised over a
/// [`Ref`] mode. See [`UnionVariant`] for the [`Active`] instantiation.
#[repr(C)]
pub enum RawUnionVariant<Rf: Ref, I> {
    FlatFull(RawUnionFlat<Rf, I, false>),
    FlatQuick(RawUnionFlat<Rf, I, true>),
    HeapFull(RawUnionHeap<Rf, I, false>),
    HeapQuick(RawUnionHeap<Rf, I, true>),
    Trimmed(RawUnionTrimmed<Rf, I>),
}

/// Alias for an [`Active`] [`RawUnionVariant`] — the only instantiation
/// with a callable surface today.
pub type UnionVariant<'index, I> = RawUnionVariant<Active<'index>, I>;

impl<'index, I: RQEIterator<'index>> UnionVariant<'index, I> {
    /// Converts this variant in place to [`UnionVariant::Trimmed`], switching
    /// to unsorted sequential-read mode.
    ///
    /// # Panics
    ///
    /// Panics if the variant has fewer than 3 children.
    pub fn trim(&mut self, limit: usize, asc: bool) {
        // We need ownership of the inner value to call `into_trimmed`.
        // `FlatFull` with an empty Vec is a cheap, valid placeholder that is
        // immediately overwritten on success.
        let placeholder = Self::FlatFull(UnionFullFlat::new(Vec::new()));
        let prev = std::mem::replace(self, placeholder);
        let trimmed = match prev {
            Self::FlatFull(u) => u.into_trimmed(limit, asc),
            Self::FlatQuick(u) => u.into_trimmed(limit, asc),
            Self::HeapFull(u) => u.into_trimmed(limit, asc),
            Self::HeapQuick(u) => u.into_trimmed(limit, asc),
            Self::Trimmed(u) => u.into_trimmed(limit, asc),
        };
        match trimmed {
            Some(t) => *self = Self::Trimmed(t),
            // Should not happen — TrimUnionIterator guards on >= 3 children.
            None => unreachable!("trim called with fewer than 3 children"),
        }
    }
}

// Delegate to the inner variant by shared reference.
macro_rules! delegate_variant_ref {
    ($self:expr, $method:ident $(, $arg:expr)*) => {
        match &$self.variant {
            UnionVariant::FlatFull(it) => it.$method($($arg),*),
            UnionVariant::FlatQuick(it) => it.$method($($arg),*),
            UnionVariant::HeapFull(it) => it.$method($($arg),*),
            UnionVariant::HeapQuick(it) => it.$method($($arg),*),
            UnionVariant::Trimmed(it) => it.$method($($arg),*),
        }
    };
}

// Delegate to the inner variant by mutable reference.
macro_rules! delegate_variant_ref_mut {
    ($self:expr, $method:ident $(, $arg:expr)*) => {
        match &mut $self.variant {
            UnionVariant::FlatFull(it) => it.$method($($arg),*),
            UnionVariant::FlatQuick(it) => it.$method($($arg),*),
            UnionVariant::HeapFull(it) => it.$method($($arg),*),
            UnionVariant::HeapQuick(it) => it.$method($($arg),*),
            UnionVariant::Trimmed(it) => it.$method($($arg),*),
        }
    };
}

/// FFI-facing union iterator holding the Rust variant and C-visible metadata
/// (query node type, query string) used by profile printing.
///
/// Parameterised over a [`Ref`] mode — see [`UnionOpaque`] for the
/// [`Active`] instantiation that implements [`RQEIterator`].
#[repr(C)]
pub struct RawUnionOpaque<Rf: Ref, I> {
    pub variant: RawUnionVariant<Rf, I>,
    pub query_node_type: QueryNodeType,
    /// Non-owning pointer to a C string describing the query (e.g. the search
    /// term). May be null.
    ///
    /// The pointee is owned by the query AST and must outlive this iterator.
    /// In practice the AST is freed only after the entire query execution
    /// pipeline — including all iterators — has been torn down, so the
    /// pointer remains valid for the lifetime of this struct.
    pub query_string: *const c_char,
}

/// Alias for an [`Active`] [`RawUnionOpaque`] — the only instantiation
/// with an [`RQEIterator`] impl today.
pub type UnionOpaque<'index, I> = RawUnionOpaque<Active<'index>, I>;

impl<Rf: Ref, I> RawUnionOpaque<Rf, I> {
    /// Returns the total number of children (including exhausted ones).
    /// Mode-independent.
    pub const fn num_children_total(&self) -> usize {
        match &self.variant {
            RawUnionVariant::FlatFull(it) => it.num_children_total(),
            RawUnionVariant::FlatQuick(it) => it.num_children_total(),
            RawUnionVariant::HeapFull(it) => it.num_children_total(),
            RawUnionVariant::HeapQuick(it) => it.num_children_total(),
            RawUnionVariant::Trimmed(it) => it.num_children_total(),
        }
    }

    /// Returns the number of currently active (non-exhausted) children.
    /// Mode-independent.
    pub const fn num_children_active(&self) -> usize {
        match &self.variant {
            RawUnionVariant::FlatFull(it) => it.num_children_active(),
            RawUnionVariant::FlatQuick(it) => it.num_children_active(),
            RawUnionVariant::HeapFull(it) => it.num_children_active(),
            RawUnionVariant::HeapQuick(it) => it.num_children_active(),
            RawUnionVariant::Trimmed(it) => it.num_children_active(),
        }
    }

    /// Returns a shared reference to the child at `idx` (across all children).
    /// Returns [`None`] if the index is out of range. Mode-independent.
    pub fn child_at(&self, idx: usize) -> Option<&I> {
        match &self.variant {
            RawUnionVariant::FlatFull(it) => it.child_at(idx),
            RawUnionVariant::FlatQuick(it) => it.child_at(idx),
            RawUnionVariant::HeapFull(it) => it.child_at(idx),
            RawUnionVariant::HeapQuick(it) => it.child_at(idx),
            RawUnionVariant::Trimmed(it) => it.child_at(idx),
        }
    }
}

impl<'index, I: RQEIterator<'index>> UnionOpaque<'index, I> {
    /// Set the weight on the union's aggregate result.
    /// Must be called before the first read/skip.
    pub fn set_result_weight(&mut self, weight: f64) {
        if let Some(result) = self.current() {
            result.weight = weight;
        }
    }

    /// Returns a mutable iterator over all children (including exhausted ones).
    pub fn children_mut(&mut self) -> Box<dyn Iterator<Item = &mut I> + '_> {
        match &mut self.variant {
            UnionVariant::FlatFull(it) => Box::new(it.children_mut()),
            UnionVariant::FlatQuick(it) => Box::new(it.children_mut()),
            UnionVariant::HeapFull(it) => Box::new(it.children_mut()),
            UnionVariant::HeapQuick(it) => Box::new(it.children_mut()),
            UnionVariant::Trimmed(it) => Box::new(it.children_mut()),
        }
    }
}

impl<'index, I: RQEIterator<'index>> RQEIterator<'index> for UnionOpaque<'index, I> {
    type Suspended = RawUnionOpaque<Suspended, I::Suspended>;

    fn suspend(self: Box<Self>) -> Box<Self::Suspended> {
        let raw = Box::into_raw(self);
        // Per-variant dispatch: each inner Union variant (`UnionFlat`,
        // `UnionHeap`, `UnionTrimmed`) walks its own children during
        // `suspend`, correctly transitioning dyn-erased `I` via the vtable.
        // A whole-box cast at this level alone would skip those per-variant
        // walks and leave inner children's vtables stale.
        //
        // SAFETY: `raw` came from `Box::into_raw`, exclusively owned and
        // valid, so the variant field is reachable.
        let variant_slot = unsafe { std::ptr::addr_of_mut!((*raw).variant) };
        // SAFETY: `variant_slot` is a valid `*mut UnionVariant<...>`;
        // `ptr::read` moves the value out, leaving the slot logically
        // uninitialized until the matching `ptr::write` below.
        let active_variant = unsafe { std::ptr::read(variant_slot) };
        // The inner variant's own `RQEIterator::suspend` impl walks its
        // children and produces the suspended form. Per-variant dispatch
        // ensures dyn-erased `I` correctly transitions its vtable; a
        // whole-box cast at this outer level alone would skip those walks.
        let suspended_variant = match active_variant {
            UnionVariant::FlatFull(it) => RawUnionVariant::FlatFull(*Box::new(it).suspend()),
            UnionVariant::FlatQuick(it) => RawUnionVariant::FlatQuick(*Box::new(it).suspend()),
            UnionVariant::HeapFull(it) => RawUnionVariant::HeapFull(*Box::new(it).suspend()),
            UnionVariant::HeapQuick(it) => RawUnionVariant::HeapQuick(*Box::new(it).suspend()),
            UnionVariant::Trimmed(it) => RawUnionVariant::Trimmed(*Box::new(it).suspend()),
        };
        // SAFETY: `variant_slot` has the same size and alignment as the
        // suspended variant (both via `#[repr(C, u8)]` over layout-compatible
        // payloads). Writing reinitialises the slot moved-from above.
        unsafe {
            std::ptr::write(
                variant_slot as *mut RawUnionVariant<Suspended, I::Suspended>,
                suspended_variant,
            );
        }
        // SAFETY: `RawUnionOpaque` is `#[repr(C)]` over `variant`
        // (now byte-rewritten as Suspended form via the per-variant
        // dispatch above), `query_node_type`, and `query_string`.
        unsafe { Box::from_raw(raw as *mut RawUnionOpaque<Suspended, I::Suspended>) }
    }

    fn cascade_suspend(&mut self) {
        delegate_variant_ref_mut!(self, cascade_suspend);
    }

    #[inline(always)]
    fn current(&mut self) -> Option<&mut RSIndexResult<'index>> {
        delegate_variant_ref_mut!(self, current)
    }

    #[inline(always)]
    fn read(&mut self) -> Result<Option<&mut RSIndexResult<'index>>, RQEIteratorError> {
        delegate_variant_ref_mut!(self, read)
    }

    #[inline(always)]
    fn skip_to(
        &mut self,
        doc_id: t_docId,
    ) -> Result<Option<SkipToOutcome<'_, 'index>>, RQEIteratorError> {
        delegate_variant_ref_mut!(self, skip_to, doc_id)
    }

    #[inline(always)]
    fn rewind(&mut self) {
        delegate_variant_ref_mut!(self, rewind)
    }

    #[inline(always)]
    fn num_estimated(&self) -> usize {
        delegate_variant_ref!(self, num_estimated)
    }

    #[inline(always)]
    fn last_doc_id(&self) -> t_docId {
        delegate_variant_ref!(self, last_doc_id)
    }

    #[inline(always)]
    fn at_eof(&self) -> bool {
        delegate_variant_ref!(self, at_eof)
    }

    #[inline(always)]
    fn type_(&self) -> IteratorType {
        IteratorType::Union
    }

    fn intersection_sort_weight(&self, prioritize_union_children: bool) -> f64 {
        delegate_variant_ref!(self, intersection_sort_weight, prioritize_union_children)
    }
}

impl<S> RQESuspendedIterator for RawUnionOpaque<Suspended, S>
where
    S: RQESuspendedIterator,
{
    type Resumed<'a> = UnionOpaque<'a, S::Resumed<'a>>;

    fn resume<'a>(
        self: Box<Self>,
        guard: &'a IndexSpecReadGuard<'a>,
    ) -> (Box<Self::Resumed<'a>>, ValidateStatus) {
        let RawUnionOpaque {
            variant,
            query_node_type,
            query_string,
        } = *self;

        // Per-variant dispatch — `RawUnionVariant` itself doesn't impl
        // RQESuspendedIterator, so we match here and forward each arm to
        // the concrete variant's resume.
        let (variant, status) = match variant {
            RawUnionVariant::FlatFull(it) => {
                let (active, status) = Box::new(it).resume(guard);
                (UnionVariant::FlatFull(*active), status)
            }
            RawUnionVariant::FlatQuick(it) => {
                let (active, status) = Box::new(it).resume(guard);
                (UnionVariant::FlatQuick(*active), status)
            }
            RawUnionVariant::HeapFull(it) => {
                let (active, status) = Box::new(it).resume(guard);
                (UnionVariant::HeapFull(*active), status)
            }
            RawUnionVariant::HeapQuick(it) => {
                let (active, status) = Box::new(it).resume(guard);
                (UnionVariant::HeapQuick(*active), status)
            }
            RawUnionVariant::Trimmed(it) => {
                // In practice this branch is unreachable — trimmed unions
                // are not subject to GC. `UnionTrimmed::resume` panics if
                // reached.
                let (active, status) = Box::new(it).resume(guard);
                (UnionVariant::Trimmed(*active), status)
            }
        };

        let active = Box::new(UnionOpaque {
            variant,
            query_node_type,
            query_string,
        });
        (active, status)
    }

    fn last_doc_id(&self) -> t_docId {
        match &self.variant {
            RawUnionVariant::FlatFull(it) => {
                <RawUnionFlat<Suspended, S, false> as RQESuspendedIterator>::last_doc_id(it)
            }
            RawUnionVariant::FlatQuick(it) => {
                <RawUnionFlat<Suspended, S, true> as RQESuspendedIterator>::last_doc_id(it)
            }
            RawUnionVariant::HeapFull(it) => {
                <RawUnionHeap<Suspended, S, false> as RQESuspendedIterator>::last_doc_id(it)
            }
            RawUnionVariant::HeapQuick(it) => {
                <RawUnionHeap<Suspended, S, true> as RQESuspendedIterator>::last_doc_id(it)
            }
            RawUnionVariant::Trimmed(it) => {
                <RawUnionTrimmed<Suspended, S> as RQESuspendedIterator>::last_doc_id(it)
            }
        }
    }

    fn num_estimated(&self) -> usize {
        match &self.variant {
            RawUnionVariant::FlatFull(it) => {
                <RawUnionFlat<Suspended, S, false> as RQESuspendedIterator>::num_estimated(it)
            }
            RawUnionVariant::FlatQuick(it) => {
                <RawUnionFlat<Suspended, S, true> as RQESuspendedIterator>::num_estimated(it)
            }
            RawUnionVariant::HeapFull(it) => {
                <RawUnionHeap<Suspended, S, false> as RQESuspendedIterator>::num_estimated(it)
            }
            RawUnionVariant::HeapQuick(it) => {
                <RawUnionHeap<Suspended, S, true> as RQESuspendedIterator>::num_estimated(it)
            }
            RawUnionVariant::Trimmed(it) => {
                <RawUnionTrimmed<Suspended, S> as RQESuspendedIterator>::num_estimated(it)
            }
        }
    }
}

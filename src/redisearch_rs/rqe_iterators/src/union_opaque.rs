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

use ffi::{QueryNodeType, t_docId};
use inverted_index::RSIndexResult;

use crate::{
    IteratorType, RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome, UnionFullFlat,
    UnionFullHeap, UnionQuickFlat, UnionQuickHeap, UnionTrimmed,
};

/// Enum holding all possible union iterator variants.
pub enum UnionVariant<'index, I> {
    FlatFull(UnionFullFlat<'index, I>),
    FlatQuick(UnionQuickFlat<'index, I>),
    HeapFull(UnionFullHeap<'index, I>),
    HeapQuick(UnionQuickHeap<'index, I>),
    Trimmed(UnionTrimmed<'index, I>),
}

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
pub struct UnionOpaque<'index, I> {
    pub variant: UnionVariant<'index, I>,
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

impl<'index, I: RQEIterator<'index>> UnionOpaque<'index, I> {
    /// Set the weight on the union's aggregate result.
    /// Must be called before the first read/skip.
    pub fn set_result_weight(&mut self, weight: f64) {
        if let Some(result) = self.current() {
            result.weight = weight;
        }
    }

    /// Returns the total number of children (including exhausted ones).
    pub const fn num_children_total(&self) -> usize {
        delegate_variant_ref!(self, num_children_total)
    }

    /// Returns the number of currently active (non-exhausted) children.
    pub const fn num_children_active(&self) -> usize {
        delegate_variant_ref!(self, num_children_active)
    }

    /// Returns a shared reference to the child at `idx` (across all children).
    /// Returns [`None`] if the index is out of range.
    pub fn child_at(&self, idx: usize) -> Option<&I> {
        delegate_variant_ref!(self, child_at, idx)
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
    unsafe fn revalidate(
        &mut self,
        spec: std::ptr::NonNull<ffi::IndexSpec>,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        // SAFETY: Delegating to variant with the same `spec` passed by our caller.
        unsafe { delegate_variant_ref_mut!(self, revalidate, spec) }
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

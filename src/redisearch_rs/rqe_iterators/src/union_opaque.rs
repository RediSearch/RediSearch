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
use index_result::RSIndexResult;

use crate::{
    IteratorType, RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome, UnionFullFlat,
    UnionFullHeap, UnionQuickFlat, UnionQuickHeap, UnionTrimmed,
    profile_print::{ProfilePrint, ProfilePrintCtx},
};

use index_spec::IndexSpecReadGuard;
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
    fn revalidate(
        &mut self,
        spec: &IndexSpecReadGuard,
    ) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        delegate_variant_ref_mut!(self, revalidate, spec)
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

impl<'index, I> ProfilePrint for UnionOpaque<'index, I>
where
    I: RQEIterator<'index> + ProfilePrint,
{
    fn print_profile(&self, map: &mut redis_reply::MapBuilder<'_>, ctx: &mut ProfilePrintCtx<'_>) {
        use std::ffi::CStr;

        let node_type = self.query_node_type;
        // Union, Geo, and LexRange always print full children even in
        // limited mode — these types have few enough children that
        // collapsing them would lose useful information.
        let print_full = !ctx.limited
            || matches!(
                node_type,
                ffi::QueryNodeType::Union | ffi::QueryNodeType::Geo | ffi::QueryNodeType::LexRange
            );

        map.kv_simple_string(c"Type", c"UNION");

        let type_str = match node_type {
            ffi::QueryNodeType::Geo => "GEO",
            ffi::QueryNodeType::Tag => "TAG",
            ffi::QueryNodeType::Union => "UNION",
            ffi::QueryNodeType::Fuzzy => "FUZZY",
            ffi::QueryNodeType::Prefix => "PREFIX",
            ffi::QueryNodeType::Numeric => "NUMERIC",
            ffi::QueryNodeType::LexRange => "LEXRANGE",
            ffi::QueryNodeType::WildcardQuery => "WILDCARD",
            _ => unreachable!("Invalid type for union"),
        };

        let q_str_ptr = self.query_string;
        if q_str_ptr.is_null() {
            let value = std::ffi::CString::new(type_str).unwrap();
            map.kv_simple_string(c"Query type", &value);
        } else {
            // SAFETY: q_str_ptr is a valid null-terminated C string (checked
            // non-null). The pointee is owned by the query AST and outlives
            // this iterator.
            let q_str_rust = unsafe { CStr::from_ptr(q_str_ptr) }.to_string_lossy();
            let formatted = format!("{type_str} - {q_str_rust}");
            // Use string_buffer (bulk string) instead of simple_string: the
            // query string may contain \r\n which is invalid in RESP Simple
            // Strings.
            map.kv_string_buffer(c"Query type", formatted.as_bytes());
        }

        ctx.print_optional_counters(map);

        let num_children = self.num_children_total();

        if print_full {
            let mut arr = map.kv_array(c"Child iterators");
            for i in 0..num_children {
                if let Some(child) = self.child_at(i) {
                    let mut child_map = arr.map();
                    let mut child_ctx = ctx.child_ctx();
                    child.print_profile(&mut child_map, &mut child_ctx);
                }
            }
        } else {
            let msg = format!("The number of iterators in the union is {num_children}");
            let msg_cstr = std::ffi::CString::new(msg).unwrap();
            map.kv_simple_string(c"Child iterators", &msg_cstr);
        }
    }
}

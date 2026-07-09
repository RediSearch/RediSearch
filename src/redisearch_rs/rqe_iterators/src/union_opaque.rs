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
//! [`RQEIteratorWrapper`] produced by the
//! FFI `NewUnionIterator` constructor. It holds one of the five concrete
//! union variants and forwards every [`RQEIterator`] call via match dispatch.
//!
//! This module lives in `rqe_iterators` (rather than in the FFI bridge crate)
//! so that [`c2rust::CRQEIterator`](crate::c2rust::CRQEIterator) can recover
//! the wrapper via
//! [`ref_from_header_ptr`](crate::interop::RQEIteratorWrapper::ref_from_header_ptr)
//! and call methods such as [`UnionOpaque::num_children_active`] directly,
//! without going through a C FFI trampoline.

use std::ffi::CStr;

use ffi::{QueryIterator, QueryNodeType};
use index_result::RSIndexResult;
use ref_mode::{Active, Ref, SharedPtr};
use rqe_core::DocId;

use crate::{
    IteratorType, RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome, UnionFullFlat,
    c2rust::CRQEIterator,
    interop::RQEIteratorWrapper,
    profile_print::{ProfilePrint, ProfilePrintCtx},
    union_flat::RawUnionFlat,
    union_heap::RawUnionHeap,
    union_trimmed::RawUnionTrimmed,
};

use index_spec::IndexSpecReadGuard;

/// Enum holding all possible union iterator variants, parameterised over a
/// [`Ref`] mode. See [`UnionVariant`] for the [`Active`] instantiation.
#[repr(C)]
pub enum RawUnionVariant<'query, Rf: Ref, I> {
    FlatFull(RawUnionFlat<'query, Rf, I, false>),
    FlatQuick(RawUnionFlat<'query, Rf, I, true>),
    HeapFull(RawUnionHeap<'query, Rf, I, false>),
    HeapQuick(RawUnionHeap<'query, Rf, I, true>),
    Trimmed(RawUnionTrimmed<'query, Rf, I>),
}

/// Alias for an [`Active`] [`RawUnionVariant`] — the only instantiation
/// with a callable surface today.
pub type UnionVariant<'index, I> = RawUnionVariant<'index, Active<'index>, I>;

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
pub struct RawUnionOpaque<'query, Rf: Ref, I> {
    pub variant: RawUnionVariant<'query, Rf, I>,
    pub query_node_type: QueryNodeType,
    /// Borrowed C string describing the query (e.g. the search term), or
    /// [`None`] when the union has no associated query string.
    ///
    /// The string is owned by the query AST, not the index; its validity is
    /// tied to the [`Ref`] mode `Rf`, since both the index and the AST outlive
    /// the iterator. In practice the AST is freed only after the entire query
    /// execution pipeline — including all iterators — has been torn down, so
    /// the borrow remains valid for the lifetime of this struct.
    pub query_string: Option<SharedPtr<Rf, CStr>>,
}

/// Alias for an [`Active`] [`RawUnionOpaque`] — the only instantiation
/// with an [`RQEIterator`] impl today.
pub type UnionOpaque<'index, I> = RawUnionOpaque<'index, Active<'index>, I>;

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
        doc_id: DocId,
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
    fn last_doc_id(&self) -> DocId {
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

        match self.query_string {
            None => {
                let value = std::ffi::CString::new(type_str).unwrap();
                map.kv_simple_string(c"Query type", &value);
            }
            Some(q_str) => {
                let q_str_rust = q_str.get().to_string_lossy();
                let formatted = format!("{type_str} - {q_str_rust}");
                // Use string_buffer (bulk string) instead of simple_string: the
                // query string may contain \r\n which is invalid in RESP Simple
                // Strings.
                map.kv_string_buffer(c"Query type", formatted.as_bytes());
            }
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

/// Concrete [`RQEIteratorWrapper`] used to expose a [`UnionOpaque`] to C.
type UnionWrapper<'index> = RQEIteratorWrapper<UnionOpaque<'index, CRQEIterator>>;

/// `ProfileChildren` callback for union iterators.
///
/// Profiles each child in-place via
/// [`CRQEIterator::into_profiled`](crate::c2rust::CRQEIterator::into_profiled),
/// preserving the `UnionOpaque<CRQEIterator>` type so the C-side optimizer and
/// profiler keep seeing the same layout. Returns the same pointer (mutation is
/// in-place).
///
/// # Safety
///
/// `base` must be a valid, owning pointer to a `UnionWrapper` created via
/// [`build_union`].
unsafe extern "C" fn union_profile_children(base: *mut QueryIterator) -> *mut QueryIterator {
    debug_assert!(!base.is_null());
    // SAFETY: caller guarantees `base` is valid and points to a union wrapper.
    let wrapper = unsafe { UnionWrapper::mut_ref_from_header_ptr(base) };
    for child in wrapper.inner.children_mut() {
        // Read the child's owning pointer without consuming the slot; ownership
        // is moved out here and handed back in place below.
        let it = child.as_raw();
        // SAFETY: `it` is a valid, uniquely-owned C iterator; it is consumed
        // here and replaced below, so it is neither leaked nor double-freed.
        let profiled = unsafe { CRQEIterator::new(it) }.into_profiled();
        // `CRQEIterator` is `#[repr(transparent)]` over `NonNull<QueryIterator>`,
        // so a `&mut CRQEIterator` can be viewed as a `*mut *mut QueryIterator`
        // slot for in-place replacement.
        let slot = child as *mut CRQEIterator as *mut *mut QueryIterator;
        // SAFETY: `slot` is a valid, writable pointer; store the profiled
        // iterator back in place.
        unsafe { *slot = profiled.into_raw().as_ptr() };
    }
    base
}

/// Build a union iterator from a `Vec` of already-owned [`CRQEIterator`]
/// children, returning a C-ABI [`QueryIterator`] pointer.
///
/// Applies the union reduction and variant-selection logic of
/// [`new_union_iterator`](crate::union_reducer::new_union_iterator): empty
/// children are removed, a single surviving child
/// is returned directly, and multiple children are placed in a flat or heap
/// union depending on `min_union_iter_heap`. The resulting wrapper carries the
/// [`union_profile_children`] callback so the still-C-driven profiler can
/// recurse into the children.
///
/// Shared by the FFI `NewUnionIterator` constructor and the Rust `query_eval`
/// dispatcher so both build the identical C-boundary shape.
///
/// # Safety
///
/// `q_str`, when [`Some`], must stay live and unchanged for as long as the
/// returned iterator exists. The borrow is stored in the [`UnionOpaque`] and
/// read back when the C-driven profiler prints the iterator, but its
/// `'index` lifetime is erased once the iterator is leaked to a raw
/// `*mut QueryIterator`, so the borrow checker cannot enforce this — the
/// caller must guarantee the string outlives the returned iterator.
pub unsafe fn build_union(
    children: Vec<CRQEIterator>,
    quick_exit: bool,
    min_union_iter_heap: usize,
    type_: QueryNodeType,
    q_str: Option<&CStr>,
    weight: f64,
) -> *mut QueryIterator {
    use crate::union_reducer::{NewUnionIterator, new_union_iterator};

    let variant = match new_union_iterator(children, quick_exit, min_union_iter_heap) {
        NewUnionIterator::ReducedEmpty(empty) => return RQEIteratorWrapper::boxed_new(empty),
        NewUnionIterator::ReducedSingle(child) => return child.into_raw().as_ptr(),
        NewUnionIterator::Flat(flat) => UnionVariant::FlatFull(flat),
        NewUnionIterator::FlatQuick(flat) => UnionVariant::FlatQuick(flat),
        NewUnionIterator::Heap(heap) => UnionVariant::HeapFull(heap),
        NewUnionIterator::HeapQuick(heap) => UnionVariant::HeapQuick(heap),
    };

    let mut dispatch = UnionOpaque {
        variant,
        query_node_type: type_,
        query_string: q_str.map(SharedPtr::from_ref),
    };
    dispatch.set_result_weight(weight);
    RQEIteratorWrapper::boxed_new_inner(dispatch, Some(union_profile_children))
}

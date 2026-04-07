/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI bridge for the Rust union iterators.

use std::{ffi::c_char, ptr::NonNull};

use ffi::{IteratorsConfig, QueryIterator, QueryNodeType, t_docId};

use crate::profile::Profile_AddIters;
use inverted_index::RSIndexResult;
use rqe_iterator_type::IteratorType;
use rqe_iterators::{
    RQEIterator, RQEIteratorError, RQEValidateStatus, SkipToOutcome, UnionFullFlat, UnionFullHeap,
    UnionQuickFlat, UnionQuickHeap, UnionTrimmed, c2rust::CRQEIterator,
    interop::RQEIteratorWrapper, union_reducer::new_union_iterator,
};

/// Enum holding all possible union iterator variants with [`CRQEIterator`]
/// children, including the special "trimmed" mode used after
/// [`TrimUnionIterator`].
enum UnionVariant<'index> {
    FlatFull(UnionFullFlat<'index, CRQEIterator>),
    FlatQuick(UnionQuickFlat<'index, CRQEIterator>),
    HeapFull(UnionFullHeap<'index, CRQEIterator>),
    HeapQuick(UnionQuickHeap<'index, CRQEIterator>),
    Trimmed(UnionTrimmed<'index, CRQEIterator>),
}

impl<'index> UnionVariant<'index> {
    /// Converts this variant in place to [`UnionVariant::Trimmed`], switching
    /// to unsorted sequential-read mode.
    fn trim(&mut self, limit: usize, asc: bool) {
        // We need ownership of the inner value to call `into_trimmed`.
        // `FlatFull` with an empty Vec is a cheap, valid placeholder that is
        // immediately overwritten.
        let placeholder = Self::FlatFull(UnionFullFlat::new(Vec::new()));
        let trimmed = match std::mem::replace(self, placeholder) {
            Self::FlatFull(u) => u.into_trimmed(limit, asc),
            Self::FlatQuick(u) => u.into_trimmed(limit, asc),
            Self::HeapFull(u) => u.into_trimmed(limit, asc),
            Self::HeapQuick(u) => u.into_trimmed(limit, asc),
            Self::Trimmed(u) => u.into_trimmed(limit, asc),
        };
        *self = Self::Trimmed(trimmed);
    }
}

/// FFI-facing union iterator holding the Rust variant and C-visible metadata
/// (query node type, query string) used by profile printing.
struct UnionIteratorFfi<'index> {
    variant: UnionVariant<'index>,
    query_node_type: QueryNodeType,
    /// Non-owning pointer to a C string describing the query (e.g. the search
    /// term). May be null.
    ///
    /// The pointee is owned by the query AST and must outlive this iterator.
    /// In practice the AST is freed only after the entire query execution
    /// pipeline — including all iterators — has been torn down, so the
    /// pointer remains valid for the lifetime of this struct.
    query_string: *const c_char,
}

impl<'index> UnionIteratorFfi<'index> {
    /// Set the weight on the union's aggregate result.
    /// Must be called before the first read/skip.
    fn set_result_weight(&mut self, weight: f64) {
        if let Some(result) = self.current() {
            result.weight = weight;
        }
    }
}

// Delegate RQEIterator to the inner variant.
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

impl<'index> RQEIterator<'index> for UnionIteratorFfi<'index> {
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
    fn revalidate(&mut self) -> Result<RQEValidateStatus<'_, 'index>, RQEIteratorError> {
        delegate_variant_ref_mut!(self, revalidate)
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

// ============================================================================
// Helper: access to children across all variants
// ============================================================================

impl<'index> UnionIteratorFfi<'index> {
    const fn num_children_total(&self) -> usize {
        delegate_variant_ref!(self, num_children_total)
    }

    const fn num_children_active(&self) -> usize {
        delegate_variant_ref!(self, num_children_active)
    }

    fn child_at_ptr(&self, idx: usize) -> *const QueryIterator {
        let child: &CRQEIterator = delegate_variant_ref!(self, child_at, idx);
        child.as_ref() as *const QueryIterator
    }

    /// Apply `callback` to each child's raw pointer slot (e.g. profile wrapping).
    fn for_each_child_mut(&mut self, callback: unsafe extern "C" fn(*mut *mut QueryIterator)) {
        let iter: Box<dyn Iterator<Item = &mut CRQEIterator> + '_> = match &mut self.variant {
            UnionVariant::FlatFull(it) => Box::new(it.children_mut()),
            UnionVariant::FlatQuick(it) => Box::new(it.children_mut()),
            UnionVariant::HeapFull(it) => Box::new(it.children_mut()),
            UnionVariant::HeapQuick(it) => Box::new(it.children_mut()),
            UnionVariant::Trimmed(it) => Box::new(it.children_mut()),
        };
        for child in iter {
            // SAFETY: CRQEIterator is #[repr(transparent)] over NonNull<QueryIterator>,
            // which is layout-compatible with *mut QueryIterator (same size/alignment).
            // The cast to *mut *mut QueryIterator is therefore valid for in-place mutation.
            // The callback must write a valid non-null pointer, preserving the NonNull
            // invariant (documented in ForEachUnionChildMut's # Safety section).
            let slot = child as *mut CRQEIterator as *mut *mut QueryIterator;
            // SAFETY: callback is a valid function pointer per caller's contract.
            unsafe { callback(slot) };
        }
    }
}

/// Concrete [`RQEIteratorWrapper`] used to expose a [`UnionIteratorFfi`] to C.
///
/// The wrapper pairs a [`QueryIterator`] header (read by C code) with the Rust
/// [`UnionIteratorFfi`] payload. All `unsafe extern "C"` functions in this
/// module recover a reference to the wrapper from a raw `*mut QueryIterator`
/// via [`RQEIteratorWrapper::ref_from_header_ptr`] /
/// [`RQEIteratorWrapper::mut_ref_from_header_ptr`].
type UnionWrapper<'index> = RQEIteratorWrapper<UnionIteratorFfi<'index>>;

/// `ProfileChildren` callback for union iterators.
///
/// Profiles each child in-place via [`Profile_AddIters`].
/// Returns the same pointer (mutation is in-place).
///
/// # Safety
///
/// `base` must be a valid, owning pointer to a `UnionWrapper` created via
/// [`NewUnionIterator`].
unsafe extern "C" fn union_profile_children(base: *mut QueryIterator) -> *mut QueryIterator {
    debug_assert!(!base.is_null());
    // SAFETY: caller guarantees `base` is valid and points to a union wrapper.
    let wrapper = unsafe { UnionWrapper::mut_ref_from_header_ptr(base) };
    // SAFETY: `Profile_AddIters` is a valid function pointer (defined in profile.rs).
    wrapper.inner.for_each_child_mut(Profile_AddIters);
    base
}

// ============================================================================
// FFI: Constructor
// ============================================================================

/// Free the C-allocated `its` array using the Redis allocator.
///
/// # Safety
///
/// `its` must have been allocated with `rm_malloc` / `RedisModule_Alloc`.
unsafe fn free_iterators_array(its: *mut *mut QueryIterator) {
    // SAFETY: Redis allocator must be initialized before this is called.
    let free_fn = unsafe { ffi::RedisModule_Free.expect("Redis allocator not initialized") };
    // SAFETY: `its` was allocated via the Redis allocator; the caller guarantees this.
    unsafe { free_fn(its.cast::<std::ffi::c_void>()) };
}

/// Creates a new union iterator, applying reduction rules and choosing between
/// flat and heap variants based on the number of children.
///
/// Takes ownership of both the `its` array and all child iterators it contains.
///
/// # Safety
///
/// 1. `its` must be a valid non-null pointer to an array of `num`
///    `QueryIterator*` values, allocated with the Redis allocator (`rm_malloc`).
///    Ownership is transferred to this function.
/// 2. Every non-null pointer in `its` must be a valid `QueryIterator` whose
///    callbacks are set.
/// 3. Null entries in `its` are treated as empty iterators.
/// 4. `config` must be a valid non-null pointer to an [`IteratorsConfig`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewUnionIterator(
    its: *mut *mut QueryIterator,
    num: i32,
    quick_exit: bool,
    weight: f64,
    type_: QueryNodeType,
    q_str: *const c_char,
    config: *const IteratorsConfig,
) -> *mut QueryIterator {
    debug_assert!(num >= 0, "NewUnionIterator called with negative num: {num}");
    let num = num.max(0) as usize;
    // SAFETY: caller guarantees config is valid (4).
    let min_union_iter_heap = unsafe { (*config).minUnionIterHeap } as usize;

    // Build Vec<CRQEIterator> from the C array.
    let children: Vec<CRQEIterator> = (0..num)
        .filter_map(|i| {
            // SAFETY: caller guarantees `its` points to an array of `num` elements (1).
            let element_ptr = unsafe { its.add(i) };
            // SAFETY: the pointer is within bounds of the allocated array (1).
            let ptr = unsafe { *element_ptr };
            NonNull::new(ptr).map(|ptr| {
                // SAFETY: each pointer is valid, non-null, and uniquely owned (2).
                unsafe { CRQEIterator::new(ptr) }
            })
        })
        .collect();

    // Free the C-allocated array now that we've moved everything into the Vec.
    // SAFETY: its was allocated via rm_malloc per the function's safety contract (1).
    unsafe { free_iterators_array(its) };

    // Apply reduction and choose variant.
    let result = new_union_iterator(children, quick_exit, min_union_iter_heap);

    use rqe_iterators::union_reducer::NewUnionIterator as NewUI;
    match result {
        NewUI::ReducedEmpty(empty) => RQEIteratorWrapper::boxed_new(empty),
        NewUI::ReducedSingle(child) => {
            // Return the single child's raw pointer, unwrapping from CRQEIterator.
            child.into_raw().as_ptr()
        }
        NewUI::Flat(flat) => {
            let mut ffi = UnionIteratorFfi {
                variant: UnionVariant::FlatFull(flat),
                query_node_type: type_,
                query_string: q_str,
            };
            ffi.set_result_weight(weight);
            RQEIteratorWrapper::boxed_new_inner(ffi, Some(union_profile_children))
        }
        NewUI::FlatQuick(flat) => {
            let mut ffi = UnionIteratorFfi {
                variant: UnionVariant::FlatQuick(flat),
                query_node_type: type_,
                query_string: q_str,
            };
            ffi.set_result_weight(weight);
            RQEIteratorWrapper::boxed_new_inner(ffi, Some(union_profile_children))
        }
        NewUI::Heap(heap) => {
            let mut ffi = UnionIteratorFfi {
                variant: UnionVariant::HeapFull(heap),
                query_node_type: type_,
                query_string: q_str,
            };
            ffi.set_result_weight(weight);
            RQEIteratorWrapper::boxed_new_inner(ffi, Some(union_profile_children))
        }
        NewUI::HeapQuick(heap) => {
            let mut ffi = UnionIteratorFfi {
                variant: UnionVariant::HeapQuick(heap),
                query_node_type: type_,
                query_string: q_str,
            };
            ffi.set_result_weight(weight);
            RQEIteratorWrapper::boxed_new_inner(ffi, Some(union_profile_children))
        }
    }
}

// ============================================================================
// FFI: Profile accessors
// ============================================================================

/// Returns the number of child iterators (including exhausted ones).
///
/// # Safety
///
/// 1. `it` must be a valid non-null pointer to a non-reduced union iterator
///    created via [`NewUnionIterator`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GetUnionIteratorNumChildren(it: *const QueryIterator) -> usize {
    debug_assert!(!it.is_null());
    // SAFETY: caller guarantees `it` is valid and points to a union iterator (1).
    debug_assert_eq!(unsafe { (*it).type_ }, IteratorType::Union);
    // SAFETY: caller guarantees `it` is valid and points to a union iterator (1).
    let wrapper = unsafe { UnionWrapper::ref_from_header_ptr(it) };
    wrapper.inner.num_children_total()
}

/// Returns the number of currently active (non-exhausted) child iterators.
///
/// # Safety
///
/// 1. `it` must be a valid non-null pointer to a non-reduced union iterator
///    created via [`NewUnionIterator`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GetUnionIteratorNumActiveChildren(it: *const QueryIterator) -> usize {
    debug_assert!(!it.is_null());
    // SAFETY: caller guarantees `it` is valid and points to a union iterator (1).
    debug_assert_eq!(unsafe { (*it).type_ }, IteratorType::Union);
    // SAFETY: caller guarantees `it` is valid and points to a union iterator (1).
    let wrapper = unsafe { UnionWrapper::ref_from_header_ptr(it) };
    wrapper.inner.num_children_active()
}

/// Returns a non-owning raw pointer to the child at `idx`.
///
/// # Safety
///
/// 1. `it` must be a valid non-null pointer to a non-reduced union iterator
///    created via [`NewUnionIterator`].
/// 2. `idx` must be less than [`GetUnionIteratorNumChildren`]`(it)`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GetUnionIteratorChild(
    it: *const QueryIterator,
    idx: usize,
) -> *const QueryIterator {
    debug_assert!(!it.is_null());
    // SAFETY: caller guarantees `it` is valid and points to a union iterator (1).
    debug_assert_eq!(unsafe { (*it).type_ }, IteratorType::Union);
    // SAFETY: caller guarantees `it` is valid and points to a union iterator (1).
    let wrapper = unsafe { UnionWrapper::ref_from_header_ptr(it) };
    wrapper.inner.child_at_ptr(idx)
}

/// Returns the [`QueryNodeType`] stored in the union iterator.
///
/// # Safety
///
/// 1. `it` must be a valid non-null pointer to a non-reduced union iterator
///    created via [`NewUnionIterator`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GetUnionIteratorQueryNodeType(it: *const QueryIterator) -> QueryNodeType {
    debug_assert!(!it.is_null());
    // SAFETY: caller guarantees `it` is valid and points to a union iterator (1).
    debug_assert_eq!(unsafe { (*it).type_ }, IteratorType::Union);
    // SAFETY: caller guarantees `it` is valid and points to a union iterator (1).
    let wrapper = unsafe { UnionWrapper::ref_from_header_ptr(it) };
    wrapper.inner.query_node_type
}

/// Returns the query string pointer stored in the union iterator, or null.
///
/// # Safety
///
/// 1. `it` must be a valid non-null pointer to a non-reduced union iterator
///    created via [`NewUnionIterator`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn GetUnionIteratorQueryString(it: *const QueryIterator) -> *const c_char {
    debug_assert!(!it.is_null());
    // SAFETY: caller guarantees `it` is valid and points to a union iterator (1).
    debug_assert_eq!(unsafe { (*it).type_ }, IteratorType::Union);
    // SAFETY: caller guarantees `it` is valid and points to a union iterator (1).
    let wrapper = unsafe { UnionWrapper::ref_from_header_ptr(it) };
    wrapper.inner.query_string
}

/// Apply `callback` to each child iterator slot, passing a mutable
/// `QueryIterator**`. Used by `Profile_AddIters` to wrap each child.
///
/// # Safety
///
/// 1. `it` must be a valid non-null pointer to a non-reduced union iterator
///    created via [`NewUnionIterator`].
/// 2. `callback` must be a valid function pointer.
/// 3. The callback must replace `*slot` with a valid non-null `QueryIterator*`
///    that takes ownership of the original iterator.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ForEachUnionChildMut(
    it: *mut QueryIterator,
    callback: unsafe extern "C" fn(*mut *mut QueryIterator),
) {
    debug_assert!(!it.is_null());
    // SAFETY: caller guarantees `it` is valid and points to a union iterator (1).
    debug_assert_eq!(unsafe { (*it).type_ }, IteratorType::Union);
    // SAFETY: caller guarantees `it` is valid and points to a union iterator (1).
    let wrapper = unsafe { UnionWrapper::mut_ref_from_header_ptr(it) };
    wrapper.inner.for_each_child_mut(callback);
}

// ============================================================================
// FFI: Query optimizer support
// ============================================================================

/// Trims a union iterator for the LIMIT optimizer, then switches to unsorted
/// sequential read mode.
///
/// # Safety
///
/// 1. `it` must be a valid non-null pointer to a non-reduced union iterator
///    created via [`NewUnionIterator`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn TrimUnionIterator(it: *mut QueryIterator, limit: usize, asc: bool) {
    debug_assert!(!it.is_null());
    // SAFETY: caller guarantees `it` is valid and points to a union iterator (1).
    debug_assert_eq!(unsafe { (*it).type_ }, IteratorType::Union);
    // SAFETY: caller guarantees `it` is valid and points to a union iterator (1).
    let wrapper = unsafe { UnionWrapper::mut_ref_from_header_ptr(it) };
    let ffi = &mut wrapper.inner;

    // With ≤ 2 children, trimming is a no-op — keep the
    // current sorted union variant so skip_to and merge order are preserved.
    if ffi.num_children_total() <= 2 {
        return;
    }

    // Preserve the result weight before trimming — the new UnionTrimmed
    // creates a fresh RSIndexResult with weight 0.0.
    let weight = ffi.current().map_or(0.0, |r| r.weight);

    ffi.variant.trim(limit, asc);
    ffi.set_result_weight(weight);
}

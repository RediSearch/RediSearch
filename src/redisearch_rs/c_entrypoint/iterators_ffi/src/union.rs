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

use ffi::{IteratorsConfig, QueryIterator, QueryNodeType};

use crate::profile::Profile_AddIters;
use rqe_iterator_type::IteratorType;
use rqe_iterators::{
    RQEIterator, UnionVariant, c2rust::CRQEIterator, interop::RQEIteratorWrapper,
    union_opaque::UnionOpaque, union_reducer::new_union_iterator,
};

/// Concrete [`RQEIteratorWrapper`] used to expose a [`UnionOpaque`] to C.
///
/// The wrapper pairs a [`QueryIterator`] header (read by C code) with the Rust
/// [`UnionOpaque`] payload. All `unsafe extern "C"` functions in this
/// module recover a reference to the wrapper from a raw `*mut QueryIterator`
/// via [`RQEIteratorWrapper::ref_from_header_ptr`] /
/// [`RQEIteratorWrapper::mut_ref_from_header_ptr`].
type UnionWrapper<'index> = RQEIteratorWrapper<UnionOpaque<'index, CRQEIterator>>;

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
    for child in wrapper.inner.children_mut() {
        // SAFETY: CRQEIterator is #[repr(transparent)] over NonNull<QueryIterator>,
        // which is layout-compatible with *mut QueryIterator (same size/alignment).
        // The cast to *mut *mut QueryIterator is therefore valid for in-place mutation.
        // `Profile_AddIters` writes back a valid, non-null `QueryIterator*`,
        // preserving the `NonNull` invariant.
        let slot = child as *mut CRQEIterator as *mut *mut QueryIterator;
        // SAFETY: `Profile_AddIters` is a valid function pointer.
        unsafe { Profile_AddIters(slot) };
    }
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

/// Minimum child count below which the heap's init and log-n overhead is not
/// amortised: always pick the flat variant.
const UI_MIN_HEAP_N: usize = 8;
/// Crossover for [low-overlap](QueryNodeType) query types in full-aggregation
/// mode: prefer heap at or above this count.
const UI_LOW_OVERLAP_HEAP_N: usize = 24;
/// Crossover for query types with unknown overlap (`QN_UNION` and anything
/// else): prefer heap at or above this count. Set conservatively high to bound
/// the worst case on high-overlap children.
const UI_UNKNOWN_HEAP_N: usize = 32;

/// Decide whether the union iterator should dispatch to the heap- or flat-based
/// variant.
///
/// The [`QueryNodeType`] acts as a structural proxy for children overlap:
///
/// - **Disjoint** (`Numeric` / `Geo` / `Geometry`) — each doc lands in exactly
///   one child; heap wins once the fixed overhead is amortised.
/// - **Low overlap** (`Prefix` / `WildcardQuery` / `LexRange` / `Fuzzy` /
///   `Tag`) — term-expansion; heap only helps in full-aggregation mode at
///   large `num`.
/// - **Unknown** (`Union` and anything else) — arbitrary user `OR`; bias
///   toward flat to bound the worst case.
///
/// Thresholds are derived from the union sweep benchmarks (see the
/// `rqe_iterators_bencher` union sweep).
fn union_should_use_heap(num: usize, type_: QueryNodeType, quick_exit: bool) -> bool {
    if num < UI_MIN_HEAP_N {
        return false;
    }
    match type_ {
        QueryNodeType::Numeric | QueryNodeType::Geo | QueryNodeType::Geometry => true,
        QueryNodeType::Prefix
        | QueryNodeType::WildcardQuery
        | QueryNodeType::LexRange
        | QueryNodeType::Fuzzy
        | QueryNodeType::Tag => !quick_exit && num >= UI_LOW_OVERLAP_HEAP_N,
        _ => num >= UI_UNKNOWN_HEAP_N,
    }
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
    let _ = config;

    let use_heap = union_should_use_heap(num, type_, quick_exit);

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
    let result = new_union_iterator(children, quick_exit, use_heap);

    use rqe_iterators::union_reducer::NewUnionIterator as NewUI;
    match result {
        NewUI::ReducedEmpty(empty) => RQEIteratorWrapper::boxed_new(empty),
        NewUI::ReducedSingle(child) => {
            // Return the single child's raw pointer, unwrapping from CRQEIterator.
            child.into_raw().as_ptr()
        }
        NewUI::Flat(flat) => {
            let mut dispatch = UnionOpaque {
                variant: UnionVariant::FlatFull(flat),
                query_node_type: type_,
                query_string: q_str,
            };
            dispatch.set_result_weight(weight);
            RQEIteratorWrapper::boxed_new_inner(dispatch, Some(union_profile_children))
        }
        NewUI::FlatQuick(flat) => {
            let mut dispatch = UnionOpaque {
                variant: UnionVariant::FlatQuick(flat),
                query_node_type: type_,
                query_string: q_str,
            };
            dispatch.set_result_weight(weight);
            RQEIteratorWrapper::boxed_new_inner(dispatch, Some(union_profile_children))
        }
        NewUI::Heap(heap) => {
            let mut dispatch = UnionOpaque {
                variant: UnionVariant::HeapFull(heap),
                query_node_type: type_,
                query_string: q_str,
            };
            dispatch.set_result_weight(weight);
            RQEIteratorWrapper::boxed_new_inner(dispatch, Some(union_profile_children))
        }
        NewUI::HeapQuick(heap) => {
            let mut dispatch = UnionOpaque {
                variant: UnionVariant::HeapQuick(heap),
                query_node_type: type_,
                query_string: q_str,
            };
            dispatch.set_result_weight(weight);
            RQEIteratorWrapper::boxed_new_inner(dispatch, Some(union_profile_children))
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
    match wrapper.inner.child_at(idx) {
        Some(child) => child.as_ref() as *const QueryIterator,
        None => std::ptr::null(),
    }
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
    let dispatch = &mut wrapper.inner;

    // With fewer than 3 children, trimming is a no-op — keep the
    // current sorted union variant so skip_to and merge order are preserved.
    if dispatch.num_children_total() < 3 {
        return;
    }

    // Preserve the result weight before trimming — the new UnionTrimmed
    // creates a fresh RSIndexResult with weight 0.0.
    let weight = dispatch.current().map_or(0.0, |r| r.weight);

    dispatch.variant.trim(limit, asc);
    dispatch.set_result_weight(weight);

    // The old variant (and its RSIndexResult) was dropped by `trim()`, so
    // `header.current` is now dangling. Sync it to the new variant's result.
    wrapper.sync_current();
}

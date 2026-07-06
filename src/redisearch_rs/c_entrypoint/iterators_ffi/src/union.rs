/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! FFI bridge for the Rust union iterators.

use std::{
    ffi::{CStr, c_char},
    ptr::NonNull,
};

use ffi::{QueryIterator, QueryNodeType};

use rqe_iterator_type::IteratorType;
use rqe_iterators::{
    IteratorsConfig, RQEIterator,
    c2rust::CRQEIterator,
    interop::RQEIteratorWrapper,
    union_opaque::{UnionOpaque, build_union},
};

/// Concrete [`RQEIteratorWrapper`] used to expose a [`UnionOpaque`] to C.
///
/// The wrapper pairs a [`QueryIterator`] header (read by C code) with the Rust
/// [`UnionOpaque`] payload. All `unsafe extern "C"` functions in this
/// module recover a reference to the wrapper from a raw `*mut QueryIterator`
/// via [`RQEIteratorWrapper::ref_from_header_ptr`] /
/// [`RQEIteratorWrapper::mut_ref_from_header_ptr`].
type UnionWrapper<'index> = RQEIteratorWrapper<UnionOpaque<'index, CRQEIterator>>;

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
    let free_fn =
        unsafe { redis_module::RedisModule_Free.expect("Redis allocator not initialized") };
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
/// 5. `q_str` must be null or a valid, NUL-terminated C string that outlives
///    the returned iterator — the requirement of [`build_union`].
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
    let min_union_iter_heap = unsafe { (*config).min_union_iter_heap } as usize;

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

    let q_str = if q_str.is_null() {
        None
    } else {
        // SAFETY: by contract (5), a non-null `q_str` is a valid, NUL-terminated
        // C string that outlives the returned iterator.
        Some(unsafe { CStr::from_ptr(q_str) })
    };

    // SAFETY: by contract (5), `q_str` outlives the returned iterator.
    unsafe {
        build_union(
            children,
            quick_exit,
            min_union_iter_heap,
            type_,
            q_str,
            weight,
        )
    }
}

// ============================================================================
// FFI: Profile accessors
// ============================================================================

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

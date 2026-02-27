/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests that exercise C symbols linked into the test binary via `extern crate redisearch_rs`.
//!
//! These tests verify behaviour that requires calling C-side functions at runtime,
//! such as `NewIntersectionIterator` / `NewSortedIdListIterator`.

use ffi::{QueryIterator, RedisModule_Alloc, t_docId};
use rqe_iterators::{RQEIterator, c2rust::CRQEIterator};
use std::ptr::NonNull;

/// Allocate a Redis-managed id-list iterator containing `ids`.
///
/// The memory for `ids` is copied into a `RedisModule_Alloc`-owned buffer,
/// as required by `NewSortedIdListIterator`.
fn make_id_list_iterator(ids: &[t_docId]) -> *mut QueryIterator {
    let n = ids.len() as u64;
    let ptr = if n > 0 {
        let buf = unsafe {
            RedisModule_Alloc.unwrap()(n as usize * std::mem::size_of::<t_docId>()) as *mut t_docId
        };
        unsafe {
            std::ptr::copy_nonoverlapping(ids.as_ptr(), buf, n as usize);
        }
        buf
    } else {
        std::ptr::null_mut()
    };
    unsafe { redisearch_rs::iterators::id_list::NewSortedIdListIterator(ptr, n, 1.0) }
}

/// `CRQEIterator::sort_weight` must return `1.0 / num_children` when the
/// underlying iterator is an `INTERSECT_ITERATOR`.
///
/// This exercises the `else if self.type_ == IteratorType_INTERSECT_ITERATOR` branch
/// in `CRQEIterator::sort_weight`, which calls `GetIntersectionIteratorNumChildren`
/// — a C symbol resolved at link time.
///
/// The test creates `n` id-list iterators, wraps them in a Rust `Intersection`
/// via `NewIntersectionIterator`, then wraps the result in a `CRQEIterator` and
/// checks the returned weight.
#[rstest::rstest]
#[case(2)]
#[case(3)]
#[case(5)]
#[cfg_attr(miri, ignore)]
fn intersect_iterator_sort_weight_reflects_child_count(#[case] n: usize) {
    // Allocate a Redis-managed array to hold `n` child iterator pointers.
    // `NewIntersectionIterator` takes ownership of this array.
    let children_ptr = unsafe {
        RedisModule_Alloc.unwrap()(n * std::mem::size_of::<*mut QueryIterator>())
            as *mut *mut QueryIterator
    };

    // Populate with non-empty, non-wildcard id-list iterators so that
    // `IntersectionIteratorReducer` (called inside `NewIntersectionIterator`)
    // does not short-circuit and preserves all `n` children.
    for i in 0..n {
        let child = make_id_list_iterator(&[1, 2, 3]);
        unsafe { *children_ptr.add(i) = child };
    }

    // Build the intersection (takes ownership of `children_ptr` and all children).
    let raw = unsafe {
        redisearch_rs::iterators::intersection::NewIntersectionIterator(
            children_ptr,
            n,
            -1,
            false,
            1.0,
        )
    };

    // Wrap in `CRQEIterator` — this is the scenario that triggered the bug:
    // a Rust `Intersection` that re-enters Rust via the C iterator API.
    let c_it = unsafe { CRQEIterator::new(NonNull::new(raw).expect("non-null intersection")) };

    assert_eq!(
        c_it.sort_weight(),
        1.0 / n as f64,
        "sort_weight for an INTERSECT_ITERATOR with {n} children should be 1/{n}"
    );
}

/// `CRQEIterator::sort_weight` must return `1.0` when the underlying iterator is a
/// `UNION_ITERATOR` and `RSGlobalConfig.prioritizeIntersectUnionChildren` is `false`
/// (the default).
///
/// The `SortWeight` callback is set directly by the C `NewUnionIterator`, so this
/// exercises the generic callback path in `CRQEIterator::sort_weight`.
/// FIXME: The `prioritize=true` arm (returning `ui.num as f64`) is not tested here because
/// mutating the global `RSGlobalConfig` is not safe in a multi-threaded test binary.
///
/// The test creates `n` id-list iterators, wraps them in a C `UnionIterator`
/// via `NewUnionIterator`, then wraps the result in a `CRQEIterator` and checks
/// the returned weight.
#[rstest::rstest]
#[case(2)]
#[case(3)]
#[case(5)]
#[cfg_attr(miri, ignore)]
fn union_iterator_sort_weight_is_one_when_not_prioritized(#[case] n: usize) {
    // Allocate a Redis-managed array of `n` child iterator pointers.
    // `NewUnionIterator` takes ownership of this array.
    let children_ptr = unsafe {
        RedisModule_Alloc.unwrap()(n * std::mem::size_of::<*mut QueryIterator>())
            as *mut *mut QueryIterator
    };
    for i in 0..n {
        unsafe { *children_ptr.add(i) = make_id_list_iterator(&[1, 2, 3]) };
    }
    // Zero-initialise `IteratorsConfig`; no heap-threshold tuning needed here.
    let mut config: ffi::IteratorsConfig = unsafe { std::mem::zeroed() };
    let raw = unsafe {
        ffi::NewUnionIterator(
            children_ptr,
            n as i32,
            false,
            1.0,
            ffi::QueryNodeType_QN_UNION,
            std::ptr::null(),
            &mut config,
        )
    };
    let c_it = unsafe { CRQEIterator::new(NonNull::new(raw).expect("non-null union")) };

    // `prioritizeIntersectUnionChildren` defaults to `false`, so sort_weight should be 1.0.
    assert_eq!(
        c_it.sort_weight(),
        1.0,
        "sort_weight for a UNION_ITERATOR with prioritize=false should be 1.0"
    );
}

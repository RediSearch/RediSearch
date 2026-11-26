/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! [`TrimUnionIterator`] trims the unions [`build_union`] lowers, and only
//! those, whatever the type tag says.

use std::ptr::NonNull;

use ffi::QueryIterator;
use iterators_ffi::union::TrimUnionIterator;
use query_types::QueryNodeType;
use rqe_iterators::{
    IteratorType, RQEIterator,
    c2rust::CRQEIterator,
    id_list::IdListSorted,
    index_load::{self, IndexLoad},
    interop::RQEIteratorWrapper,
    union_opaque::{UnionOpaque, UnionVariant, build_union},
};

// Link both Rust-provided and C-provided symbols
extern crate redisearch_rs;
// Mock or stub the ones that aren't provided by the line above
redis_mock::mock_or_stub_missing_redis_c_symbols!();

/// A full union over three single-document leaves, enough for trimming not to
/// be skipped.
fn union() -> NonNull<QueryIterator> {
    let leaves = (1..=3)
        .map(|doc_id| CRQEIterator::from_rust_leaf(IdListSorted::new(vec![doc_id])))
        .collect();
    build_union(leaves, false, usize::MAX, QueryNodeType::Numeric, 1.0)
}

/// Whether the union behind `header` was trimmed.
///
/// # Safety
///
/// 1. `header` heads a live union [`build_union`] lowered, not otherwise
///    borrowed for the duration of the call.
const unsafe fn is_trimmed(header: NonNull<QueryIterator>) -> bool {
    // SAFETY: (1).
    let wrapper =
        unsafe { RQEIteratorWrapper::<UnionOpaque<'_, CRQEIterator>>::ref_from_header_ptr(header) };
    matches!(wrapper.inner.variant, UnionVariant::Trimmed(_))
}

#[test]
fn trims_a_union() {
    let header = union();
    // SAFETY: `header` is live, and nothing else holds it.
    unsafe { TrimUnionIterator(header.as_ptr(), 1, true) };
    // SAFETY: (1) `header` is the union `union` built, and nothing borrows it.
    assert!(unsafe { is_trimmed(header) });
    // SAFETY: (1)–(4) `build_union` returns a valid, owning iterator handle.
    drop(unsafe { CRQEIterator::new(header) });
}

#[test]
fn leaves_a_decorated_union_alone() {
    let union_header = union();
    // SAFETY: (1)–(4) `build_union` returns a valid, owning iterator handle.
    let decorator = IndexLoad::new(unsafe { CRQEIterator::new(union_header) }).unwrap();
    let header = index_load::boxed_new(decorator);
    // SAFETY: `header` is live.
    assert_eq!(unsafe { header.as_ref() }.type_, IteratorType::Union);

    // SAFETY: `header` is live, and nothing else holds it.
    unsafe { TrimUnionIterator(header.as_ptr(), 1, true) };

    // SAFETY: (1) the decorator owns the union and nothing borrows it.
    assert!(!unsafe { is_trimmed(union_header) });
    // SAFETY: (1)–(4) `boxed_new` returns a valid, owning iterator handle.
    let mut it = unsafe { CRQEIterator::new(header) };
    for doc_id in 1..=3 {
        assert_eq!(it.read().unwrap().unwrap().doc_id, doc_id);
    }
}

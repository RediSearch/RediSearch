/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests driving the Rust iterators `TagIndex::open_reader` and
//! `TagIndex::iterator_for_tag` build, through the `RQEIterator` trait
//! directly. Wrapping them for the C vtable is the FFI crate's job, not this
//! crate's, so these tests stop at the Rust boundary.
//!
//! `open_reader` returns a `NewTagIterator`, which is a plain enum: these tests
//! match out the `Mem` variant — the only one a memory-mode index yields — and
//! drive the iterator inside it.

use std::ptr::NonNull;

use rqe_iterators::{RQEIterator, RQEValidateStatus};
use rqe_iterators_test_utils::MockContext;
use tag_index::{NewTagIterator, TagIndex, TrieLookup};

use crate::util::index_mem;

/// Field index the reader filters on. These tests index a single field, so any
/// value works as long as it matches what `index_mem` writes.
const FIELD_INDEX: ffi::t_fieldIndex = 0;

/// Heap-allocate `index` and mint the revalidation lookup for it, returning both.
///
/// The pointer has to be raw from the start, and the lookup has to be minted from
/// that pointer rather than from a reference to it: the query layer mutates the
/// index (GC) while an iterator holds the lookup, which invalidates anything
/// derived above it. This is the same discipline the FFI entry points follow with
/// the pointer C hands them.
///
/// The caller owns the returned allocation and must free it with `Box::from_raw`
/// once every iterator built from it is gone.
fn allocate(index: TagIndex) -> (*mut TagIndex, TrieLookup) {
    let index = Box::into_raw(Box::new(index));
    let ptr = NonNull::new(index).expect("Box::into_raw is never null");
    // SAFETY: `ptr` is the owning pointer itself, and the allocation lives until
    // the caller frees it — which every test does only after freeing its
    // iterators, so nothing reads the lookup afterwards.
    let lookup = unsafe { TrieLookup::new(ptr) };
    (index, lookup)
}

/// Read every document id the iterator yields, in order, until it is exhausted.
fn drain<'i>(mut it: impl RQEIterator<'i>) -> Vec<ffi::t_docId> {
    let mut doc_ids = Vec::new();
    while it.read().expect("read must not error").is_some() {
        doc_ids.push(it.last_doc_id());
    }
    doc_ids
}

/// `open_reader` yields an iterator that reads every indexed document id in
/// ascending order, then reports EOF. A few hundred docs is enough to cross block
/// boundaries; the goal is ordering/EOF correctness, not throughput.
#[test]
fn open_reader_reads_all_ids_in_order() {
    const N: ffi::t_docId = 300;

    let mock = MockContext::new(N, N as usize);
    let (tag_index, lookup) = allocate(TagIndex::new_in_memory(1, false));
    let tags: &[&[u8]] = &[b"hello"];
    for doc_id in 1..=N {
        // SAFETY: `tag_index` was just allocated and is not yet aliased.
        index_mem(unsafe { &mut *tag_index }, tags, doc_id);
    }

    // SAFETY: `tag_index` and `mock` outlive the iterator, and `lookup` resolves
    // `tag_index`.
    let it = unsafe { (*tag_index).open_reader(mock.sctx(), b"hello", 1.0, FIELD_INDEX, lookup) }
        .expect("memory mode never errors")
        .expect("the tag is indexed");
    let NewTagIterator::Mem(it) = it else {
        panic!("a memory-mode index returns the Mem variant")
    };

    let doc_ids = drain(it);
    assert_eq!(doc_ids, (1..=N).collect::<Vec<_>>());

    // SAFETY: `allocate` allocated it; the iterator using it is gone.
    drop(unsafe { Box::from_raw(tag_index) });
}

/// After reading the only document, skipping past the last id reports EOF and
/// leaves `last_doc_id` at or beyond the last read id.
#[test]
fn skip_to_past_last_id_yields_eof() {
    let mock = MockContext::new(1, 1);
    let (tag_index, lookup) = allocate(TagIndex::new_in_memory(1, false));
    let doc_id: ffi::t_docId = 1;
    // SAFETY: `tag_index` was just allocated and is not yet aliased.
    index_mem(unsafe { &mut *tag_index }, &[b"hello"], doc_id);

    // SAFETY: `tag_index` and `mock` outlive the iterator, and `lookup` resolves
    // `tag_index`.
    let it = unsafe { (*tag_index).open_reader(mock.sctx(), b"hello", 1.0, FIELD_INDEX, lookup) }
        .expect("memory mode never errors")
        .expect("the tag is indexed");
    let NewTagIterator::Mem(mut it) = it else {
        panic!("a memory-mode index returns the Mem variant")
    };

    it.read().expect("read must not error");
    assert_eq!(it.last_doc_id(), doc_id);

    let outcome = it.skip_to(doc_id + 1).expect("skip_to must not error");
    assert!(
        outcome.is_none(),
        "skip_to past the last id must report EOF"
    );
    assert!(it.last_doc_id() >= doc_id);

    // SAFETY: `allocate` allocated it; the iterator using it is gone.
    drop(unsafe { Box::from_raw(tag_index) });
}

/// Opening a reader on a tag that was never indexed yields no iterator. The
/// NULL-index case is a C-ABI concern and stays in the C++ suite.
#[test]
fn open_reader_absent_tag_returns_none() {
    let (tag_index, lookup) = allocate(TagIndex::new_in_memory(1, false));
    // SAFETY: `tag_index` was just allocated and is not yet aliased.
    index_mem(unsafe { &mut *tag_index }, &[b"hello"], 1);

    let mock = MockContext::new(1, 1);
    // SAFETY: `tag_index` and `mock` outlive the (never created) iterator, and
    // `lookup` resolves `tag_index`.
    let it = unsafe { (*tag_index).open_reader(mock.sctx(), b"missing", 1.0, FIELD_INDEX, lookup) }
        .expect("memory mode never errors");
    assert!(it.is_none());

    // SAFETY: `allocate` allocated it; no iterator was built from it.
    drop(unsafe { Box::from_raw(tag_index) });
}

/// Driving the iterator built from an already-resolved inverted index
/// (`iterator_for_tag`) yields exactly the indexed document ids.
#[test]
fn value_path_reads_all_matching_docs() {
    let mock = MockContext::new(3, 3);
    let (tag_index, lookup) = allocate(TagIndex::new_in_memory(1, false));
    let tags: &[&[u8]] = &[b"team"];
    for doc_id in 1..=3 {
        // SAFETY: `tag_index` was just allocated and is not yet aliased.
        index_mem(unsafe { &mut *tag_index }, tags, doc_id);
    }

    // SAFETY: `tag_index` is valid and not mutated while `ii` is in use.
    let ii = unsafe { &*tag_index }
        .find_value(b"team")
        .expect("tag was indexed");
    // SAFETY: `tag_index` and `mock` outlive the iterator, `ii` is the trie's
    // current value for the tag, and `lookup` resolves `tag_index`.
    let it = unsafe {
        (*tag_index).iterator_for_tag(mock.sctx(), b"team", ii, 1.0, FIELD_INDEX, lookup)
    }
    .expect("ii holds documents");

    let doc_ids = drain(it);
    assert_eq!(doc_ids, vec![1, 2, 3]);

    // SAFETY: `allocate` allocated it; the iterator using it is gone.
    drop(unsafe { Box::from_raw(tag_index) });
}

/// Revalidating aborts once the garbage collector has dropped the tag's
/// postings — the lookup no longer resolves the tag, so the reader is stale.
///
/// The lib-level test of this covers a hand-built lookup; this one goes through
/// `iterator_for_tag`, so it exercises the lookup all the way through the
/// reader construction path.
#[test]
fn revalidate_aborts_after_gc() {
    let mock = MockContext::new(3, 3);
    let tags: &[&[u8]] = &[b"team"];
    // `allocate` reaches the index through a raw pointer, so it can be mutated while
    // the iterator holds its back-pointer — as the query layer does across GC cycles.
    let (tag_index, lookup) = allocate(TagIndex::new_in_memory(1, false));
    for doc_id in 1..=3 {
        // SAFETY: `tag_index` was just allocated and is not yet aliased.
        index_mem(unsafe { &mut *tag_index }, tags, doc_id);
    }

    // SAFETY: valid, and mutated below only between revalidations.
    let ii = unsafe { &*tag_index }
        .find_value(b"team")
        .expect("tag was indexed");
    // SAFETY: `tag_index` and `mock` outlive the iterator, `ii` is the trie's
    // current value for the tag, and `lookup` resolves `tag_index`.
    let mut it = unsafe {
        (*tag_index).iterator_for_tag(mock.sctx(), b"team", ii, 1.0, FIELD_INDEX, lookup)
    }
    .expect("ii holds documents");

    let status = it
        .revalidate(&mock.spec_read())
        .expect("revalidate must not error");
    assert_eq!(status, RQEValidateStatus::Ok);

    // Simulate the garbage collector dropping every document for the tag.
    // SAFETY: the iterator is untouched during the mutation, per the
    // revalidation protocol.
    unsafe { (*tag_index).delete_tag_value(b"team") };

    // SAFETY: as above; the protocol allows a read only after revalidating.
    let status = it
        .revalidate(&mock.spec_read())
        .expect("revalidate must not error");
    assert_eq!(
        status,
        RQEValidateStatus::Aborted,
        "a reader whose tag the GC removed must abort"
    );

    // SAFETY: `allocate` allocated it; the iterator using it is gone.
    drop(unsafe { Box::from_raw(tag_index) });
}

/// An inverted index with no documents yields no iterator: the value path returns
/// `None` rather than a reader that would immediately hit EOF.
#[test]
fn value_path_returns_none_for_empty_inverted_index() {
    let (tag_index, lookup) = allocate(TagIndex::new_in_memory(1, false));
    // Register the tag with a fresh, empty posting list (no documents indexed).
    // SAFETY: `tag_index` was just allocated and is not yet aliased.
    unsafe { &mut *tag_index }
        .open_index(b"empty", true)
        .expect("empty inverted index registered");

    let mock = MockContext::new(0, 0);
    // SAFETY: `tag_index` is valid and not mutated while `ii` is in use.
    let ii = unsafe { &*tag_index }
        .find_value(b"empty")
        .expect("tag was inserted");
    // SAFETY: `tag_index` and `mock` outlive the (never created) iterator, `ii` is
    // the trie's current value for the tag, and `lookup` resolves `tag_index`.
    let it = unsafe {
        (*tag_index).iterator_for_tag(mock.sctx(), b"empty", ii, 1.0, FIELD_INDEX, lookup)
    };
    assert!(it.is_none());

    // SAFETY: `allocate` allocated it; no iterator was built from it.
    drop(unsafe { Box::from_raw(tag_index) });
}

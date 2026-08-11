/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests driving the C `QueryIterator`s the index builds — both
//! `TagIndex::open_reader` and `TagIndex::query_iterator_for_value` — through
//! their vtable.

use std::ptr::{NonNull, null_mut};

use rqe_iterators_test_utils::MockContext;
use tag_index::{TagIndex, TrieLookup};

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

/// Read every document id the iterator yields, in order, until `ITERATOR_EOF`.
///
/// # Safety
///
/// `it` must be a valid `QueryIterator` whose `Read` callback is populated
/// (always the case for the `RQEIteratorWrapper` the index builds).
unsafe fn drain(it: *mut ffi::QueryIterator) -> Vec<ffi::t_docId> {
    let mut doc_ids = Vec::new();
    loop {
        // SAFETY: `it` is a valid `QueryIterator`.
        let read = unsafe { (*it).Read }.expect("Read is set");
        // SAFETY: `read` is `it`'s own vtable entry, so `it` is the argument it
        // expects.
        let status = unsafe { read(it) };
        if status != ffi::IteratorStatus_ITERATOR_OK {
            assert_eq!(status, ffi::IteratorStatus_ITERATOR_EOF);
            break;
        }
        // SAFETY: `it` is valid and positioned on a result.
        doc_ids.push(unsafe { (*it).lastDocId });
    }
    doc_ids
}

/// Hand ownership of `it` back through its `Free` callback.
///
/// # Safety
///
/// `it` must be a valid `QueryIterator` that nothing touches afterwards.
unsafe fn free(it: *mut ffi::QueryIterator) {
    // SAFETY: `it` is a valid `QueryIterator`.
    let free = unsafe { (*it).Free }.expect("Free is set");
    // SAFETY: `free` is `it`'s own vtable entry; the caller promises nothing
    // uses `it` after this.
    unsafe { free(it) };
}

/// `open_reader` yields an iterator that reads every indexed document id in
/// ascending order, then reports EOF. A few hundred docs is enough to cross block
/// boundaries; the goal is ordering/EOF correctness, not throughput.
#[test]
fn open_reader_reads_all_ids_in_order() {
    const N: ffi::t_docId = 300;

    let mock = MockContext::new(N, N as usize);
    let (tag_index, lookup) = allocate(TagIndex::new(1, None, false));
    let tags: &[&[u8]] = &[b"hello"];
    for doc_id in 1..=N {
        // SAFETY: `tag_index` was just allocated and is not yet aliased.
        index_mem(unsafe { &mut *tag_index }, tags, doc_id);
    }

    // SAFETY: `tag_index` and `mock` outlive the iterator, which is freed below,
    // and `lookup` resolves `tag_index`.
    let it = unsafe {
        (*tag_index).open_reader(mock.sctx(), b"hello", 1.0, FIELD_INDEX, lookup, null_mut())
    }
    .expect("the tag is indexed");

    // SAFETY: `it` is the valid iterator just built; freed below.
    let doc_ids = unsafe { drain(it.as_ptr()) };
    assert_eq!(doc_ids, (1..=N).collect::<Vec<_>>());

    // SAFETY: `it` is the valid iterator built above; nothing touches it after.
    unsafe { free(it.as_ptr()) };
    // SAFETY: `allocate` allocated it; the iterator using it is gone.
    drop(unsafe { Box::from_raw(tag_index) });
}

/// After reading the only document, skipping past the last id reports EOF and
/// leaves `lastDocId` at or beyond the last read id.
#[test]
fn skip_to_past_last_id_yields_eof() {
    let mock = MockContext::new(1, 1);
    let (tag_index, lookup) = allocate(TagIndex::new(1, None, false));
    let doc_id: ffi::t_docId = 1;
    // SAFETY: `tag_index` was just allocated and is not yet aliased.
    index_mem(unsafe { &mut *tag_index }, &[b"hello"], doc_id);

    // SAFETY: `tag_index` and `mock` outlive the iterator, which is freed below,
    // and `lookup` resolves `tag_index`.
    let it = unsafe {
        (*tag_index).open_reader(mock.sctx(), b"hello", 1.0, FIELD_INDEX, lookup, null_mut())
    }
    .expect("the tag is indexed");
    let it = it.as_ptr();

    // SAFETY: `it` is the valid iterator just built.
    let read = unsafe { (*it).Read }.expect("Read is set");
    // SAFETY: `read` is `it`'s own vtable entry.
    let status = unsafe { read(it) };
    assert_eq!(status, ffi::IteratorStatus_ITERATOR_OK);
    // SAFETY: `it` is valid and positioned on the first (only) result.
    assert_eq!(unsafe { (*it).lastDocId }, doc_id);

    // SAFETY: `it` is valid; `SkipTo` is populated by `RQEIteratorWrapper`.
    let skip_to = unsafe { (*it).SkipTo }.expect("SkipTo is set");
    // SAFETY: `skip_to` is `it`'s own vtable entry.
    let status = unsafe { skip_to(it, doc_id + 1) };
    assert_eq!(status, ffi::IteratorStatus_ITERATOR_EOF);
    // SAFETY: `it` is valid; the failed skip must not rewind `lastDocId`.
    assert!(unsafe { (*it).lastDocId } >= doc_id);

    // SAFETY: `it` is the valid iterator built above; nothing touches it after.
    unsafe { free(it) };
    // SAFETY: `allocate` allocated it; the iterator using it is gone.
    drop(unsafe { Box::from_raw(tag_index) });
}

/// Opening a reader on a tag that was never indexed yields no iterator. The
/// NULL-index case is a C-ABI concern and stays in the C++ suite.
#[test]
fn open_reader_absent_tag_returns_none() {
    let (tag_index, lookup) = allocate(TagIndex::new(1, None, false));
    // SAFETY: `tag_index` was just allocated and is not yet aliased.
    index_mem(unsafe { &mut *tag_index }, &[b"hello"], 1);

    let mock = MockContext::new(1, 1);
    // SAFETY: `tag_index` and `mock` outlive the (never created) iterator, and
    // `lookup` resolves `tag_index`.
    let it = unsafe {
        (*tag_index).open_reader(
            mock.sctx(),
            b"missing",
            1.0,
            FIELD_INDEX,
            lookup,
            null_mut(),
        )
    };
    assert!(it.is_none());

    // SAFETY: `allocate` allocated it; no iterator was built from it.
    drop(unsafe { Box::from_raw(tag_index) });
}

/// Drive the `QueryIterator` built from an already-resolved inverted index
/// (`query_iterator_for_value`) through its vtable, checking that it yields
/// exactly the indexed document ids. This exercises the whole bridge:
/// `query_iterator_for_value` → `Tag` iterator → `RQEIteratorWrapper`.
#[test]
fn value_path_reads_all_matching_docs_via_c_vtable() {
    let mock = MockContext::new(3, 3);
    let (tag_index, lookup) = allocate(TagIndex::new(1, None, false));
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
    // current value for the tag, `lookup` resolves `tag_index`, and the iterator
    // is freed below.
    let it = unsafe {
        (*tag_index).query_iterator_for_value(mock.sctx(), b"team", ii, 1.0, FIELD_INDEX, lookup)
    };
    assert!(!it.is_null());

    // SAFETY: `it` is the valid iterator just built; freed below.
    let doc_ids = unsafe { drain(it) };
    assert_eq!(doc_ids, vec![1, 2, 3]);

    // SAFETY: `it` is the valid iterator built above; nothing touches it after.
    unsafe { free(it) };
    // SAFETY: `allocate` allocated it; the iterator using it is gone.
    drop(unsafe { Box::from_raw(tag_index) });
}

/// Revalidating through the C vtable aborts once the garbage collector has
/// dropped the tag's postings — the lookup no longer resolves the tag, so the
/// reader is stale.
///
/// The lib-level test of this covers a hand-built lookup; this one goes through
/// `query_iterator_for_value`, so it exercises the lookup all the way through the
/// reader construction path, and it drives `Revalidate` the way the C query layer
/// does.
#[test]
fn revalidate_aborts_after_gc_via_c_vtable() {
    let mock = MockContext::new(3, 3);
    let tags: &[&[u8]] = &[b"team"];
    // `allocate` reaches the index through a raw pointer, so it can be mutated while
    // the iterator holds its back-pointer — as the query layer does across GC cycles.
    let (tag_index, lookup) = allocate(TagIndex::new(1, None, false));
    for doc_id in 1..=3 {
        // SAFETY: `tag_index` was just allocated and is not yet aliased.
        index_mem(unsafe { &mut *tag_index }, tags, doc_id);
    }

    // SAFETY: valid, and mutated below only between revalidations.
    let ii = unsafe { &*tag_index }
        .find_value(b"team")
        .expect("tag was indexed");
    // SAFETY: `tag_index` and `mock` outlive the iterator, `ii` is the trie's
    // current value for the tag, `lookup` resolves `tag_index`, and the iterator
    // is freed below.
    let it = unsafe {
        (*tag_index).query_iterator_for_value(mock.sctx(), b"team", ii, 1.0, FIELD_INDEX, lookup)
    };
    assert!(!it.is_null());

    // SAFETY: `mock` owns a valid `RedisSearchCtx` for the whole test.
    let spec = unsafe { (*mock.sctx().as_ptr()).spec };
    // SAFETY: `it` is the valid iterator built above.
    let revalidate = unsafe { (*it).Revalidate }.expect("Revalidate is set");

    // SAFETY: `it` is valid and positioned, and `spec` is the mock's live spec.
    let status = unsafe { revalidate(it, spec) };
    assert_eq!(status, ffi::ValidateStatus_VALIDATE_OK);

    // Simulate the garbage collector dropping every document for the tag.
    // SAFETY: the iterator is untouched during the mutation, per the
    // revalidation protocol.
    unsafe { (*tag_index).delete_tag_value(b"team") };

    // SAFETY: as above; the protocol allows a read only after revalidating.
    let status = unsafe { revalidate(it, spec) };
    assert_eq!(
        status,
        ffi::ValidateStatus_VALIDATE_ABORTED,
        "a reader whose tag the GC removed must abort"
    );

    // SAFETY: `it` is the valid iterator built above; nothing touches it after.
    unsafe { free(it) };
    // SAFETY: `allocate` allocated it; the iterator using it is gone.
    drop(unsafe { Box::from_raw(tag_index) });
}

/// An inverted index with no documents yields no iterator: the value path returns
/// a NULL pointer rather than a reader that would immediately hit EOF.
#[test]
fn value_path_returns_null_for_empty_inverted_index() {
    let (tag_index, lookup) = allocate(TagIndex::new(1, None, false));
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
        (*tag_index).query_iterator_for_value(mock.sctx(), b"empty", ii, 1.0, FIELD_INDEX, lookup)
    };
    assert!(it.is_null());

    // SAFETY: `allocate` allocated it; no iterator was built from it.
    drop(unsafe { Box::from_raw(tag_index) });
}

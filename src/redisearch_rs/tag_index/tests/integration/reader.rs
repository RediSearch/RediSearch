/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests driving the Rust iterators `TagIndex::open_reader` builds, through the
//! `RQEIterator` trait directly. Wrapping them for the C vtable is the FFI
//! crate's job, not this crate's, so these tests stop at the Rust boundary.
//!
//! `open_reader` on a memory-mode index returns the reader directly: the mode is
//! in the type, so there is no variant to match out.

use std::ptr::NonNull;

use rqe_iterators::{
    RQEIterator, RQEIteratorBoxed, RQESuspendedIterator, RQEValidateStatus, ResumeOutcome,
};
use rqe_iterators_test_utils::MockContext;
use tag_index::{InMemoryMode, Tag, TagIndex, TrieLookup};

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
fn allocate(index: TagIndex<InMemoryMode>) -> (*mut TagIndex<InMemoryMode>, TrieLookup) {
    let index = Box::into_raw(Box::new(index));
    let ptr = NonNull::new(index).expect("Box::into_raw is never null");
    // SAFETY: `ptr` is the owning pointer itself, and the allocation lives until
    // the caller frees it — which every test does only after freeing its
    // iterators, so nothing reads the lookup afterwards.
    let lookup = unsafe { TrieLookup::new(ptr) };
    (index, lookup)
}

/// Wrap a NUL-free test literal into a [`Tag`].
fn as_tag(bytes: &[u8]) -> Tag<'_> {
    Tag::new(bytes).expect("test literal is NUL-free")
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
    let (tag_index, lookup) = allocate(TagIndex::<InMemoryMode>::new(false));
    let tags: &[&[u8]] = &[b"hello"];
    for doc_id in 1..=N {
        // SAFETY: `tag_index` was just allocated and is not yet aliased.
        index_mem(unsafe { &mut *tag_index }, tags, doc_id);
    }

    // SAFETY: `tag_index` and `mock` outlive the iterator, and `lookup` resolves
    // `tag_index`.
    let it = unsafe {
        (*tag_index).open_reader(mock.sctx(), as_tag(b"hello"), 1.0, FIELD_INDEX, lookup)
    }
    .expect("the tag is indexed");

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
    let (tag_index, lookup) = allocate(TagIndex::<InMemoryMode>::new(false));
    let doc_id: ffi::t_docId = 1;
    // SAFETY: `tag_index` was just allocated and is not yet aliased.
    index_mem(unsafe { &mut *tag_index }, &[b"hello"], doc_id);

    // SAFETY: `tag_index` and `mock` outlive the iterator, and `lookup` resolves
    // `tag_index`.
    let mut it = unsafe {
        (*tag_index).open_reader(mock.sctx(), as_tag(b"hello"), 1.0, FIELD_INDEX, lookup)
    }
    .expect("the tag is indexed");

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
    let (tag_index, lookup) = allocate(TagIndex::<InMemoryMode>::new(false));
    // SAFETY: `tag_index` was just allocated and is not yet aliased.
    index_mem(unsafe { &mut *tag_index }, &[b"hello"], 1);

    let mock = MockContext::new(1, 1);
    // SAFETY: `tag_index` and `mock` outlive the (never created) iterator, and
    // `lookup` resolves `tag_index`.
    let it = unsafe {
        (*tag_index).open_reader(mock.sctx(), as_tag(b"missing"), 1.0, FIELD_INDEX, lookup)
    };
    assert!(it.is_none());

    // SAFETY: `allocate` allocated it; no iterator was built from it.
    drop(unsafe { Box::from_raw(tag_index) });
}

/// Revalidating aborts once the garbage collector has dropped the tag's
/// postings — the lookup no longer resolves the tag, so the reader is stale.
///
/// The lib-level test of this covers a hand-built lookup; this one goes through
/// `open_reader`, so it exercises the lookup all the way through the reader
/// construction path.
#[test]
fn revalidate_aborts_after_gc() {
    let mock = MockContext::new(3, 3);
    let tags: &[&[u8]] = &[b"team"];
    // `allocate` reaches the index through a raw pointer, so it can be mutated while
    // the iterator holds its back-pointer — as the query layer does across GC cycles.
    let (tag_index, lookup) = allocate(TagIndex::<InMemoryMode>::new(false));
    for doc_id in 1..=3 {
        // SAFETY: `tag_index` was just allocated and is not yet aliased.
        index_mem(unsafe { &mut *tag_index }, tags, doc_id);
    }

    // SAFETY: `tag_index` and `mock` outlive the iterator, `tag_index` is mutated
    // below only between revalidations, and `lookup` resolves `tag_index`.
    let mut it =
        unsafe { (*tag_index).open_reader(mock.sctx(), as_tag(b"team"), 1.0, FIELD_INDEX, lookup) }
            .expect("the tag is indexed");

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

/// A tag registered in the trie but holding no documents yields no iterator:
/// `open_reader` returns `None` rather than a reader that would immediately hit
/// EOF.
#[test]
fn open_reader_returns_none_for_empty_inverted_index() {
    let (tag_index, lookup) = allocate(TagIndex::<InMemoryMode>::new(false));
    // Register the tag with one document, then GC it away: the trie keeps the
    // now-empty inverted index registered, mirroring what removal leaves behind
    // once every posting under a tag has been removed.
    // SAFETY: `tag_index` was just allocated and is not yet aliased.
    index_mem(unsafe { &mut *tag_index }, &[b"empty"], 1);
    // SAFETY: `tag_index` was indexed into above and is not otherwise aliased.
    unsafe { &mut *tag_index }.gc_empty_value(b"empty");

    let mock = MockContext::new(0, 0);
    // SAFETY: `tag_index` and `mock` outlive the (never created) iterator, and
    // `lookup` resolves `tag_index`.
    let it = unsafe {
        (*tag_index).open_reader(mock.sctx(), as_tag(b"empty"), 1.0, FIELD_INDEX, lookup)
    };
    assert!(it.is_none());

    // SAFETY: `allocate` allocated it; no iterator was built from it.
    drop(unsafe { Box::from_raw(tag_index) });
}

/// The reason [`TrieLookup`] exists: a suspended reader whose tag the collector
/// dropped must abort on resume rather than come back reading the freed posting
/// list. The lookup re-resolves the tag through the owner's pointer, finds
/// nothing, and aborts.
///
/// The index is mutated only while the reader is suspended — that is the point of
/// suspension, and the only state in which a mutation through the owning pointer
/// is legal.
#[test]
fn resume_after_the_tag_was_collected_aborts() {
    let mock = MockContext::new(2, 2);
    let (tag_index, lookup) = allocate(TagIndex::<InMemoryMode>::new(false));
    for doc_id in 1..=2 {
        // SAFETY: `tag_index` was just allocated and is not yet aliased.
        index_mem(unsafe { &mut *tag_index }, &[b"hello"], doc_id);
    }

    // SAFETY: `tag_index` and `mock` outlive the iterator, and `lookup` resolves
    // `tag_index`.
    let mut it = unsafe {
        (*tag_index).open_reader(mock.sctx(), as_tag(b"hello"), 1.0, FIELD_INDEX, lookup)
    }
    .expect("the tag is indexed");
    it.read().expect("read must not error");

    let suspended = Box::new(it).suspend();

    // SAFETY: the reader is suspended, so it holds no reference into the index,
    // and `tag_index` is the owning pointer the lookup was minted from.
    unsafe { (*tag_index).delete_tag_value(b"hello") };

    let guard = mock.spec_read();
    let outcome = suspended.resume(&guard).expect("resume must not error");
    assert!(
        matches!(outcome, ResumeOutcome::Aborted),
        "a reader whose tag was collected must abort instead of resuming"
    );

    // SAFETY: `allocate` allocated it; the aborted resume consumed the iterator.
    drop(unsafe { Box::from_raw(tag_index) });
}

/// The counterpart: an untouched index re-resolves to the very inverted index the
/// reader holds, so the same suspend/resume cycle keeps reading where it left off.
/// Without it, an abort-everything lookup would pass the test above.
#[test]
fn resume_with_the_tag_untouched_reads_on() {
    const N: ffi::t_docId = 3;

    let mock = MockContext::new(N, N as usize);
    let (tag_index, lookup) = allocate(TagIndex::<InMemoryMode>::new(false));
    for doc_id in 1..=N {
        // SAFETY: `tag_index` was just allocated and is not yet aliased.
        index_mem(unsafe { &mut *tag_index }, &[b"hello"], doc_id);
    }

    // SAFETY: `tag_index` and `mock` outlive the iterator, and `lookup` resolves
    // `tag_index`.
    let mut it = unsafe {
        (*tag_index).open_reader(mock.sctx(), as_tag(b"hello"), 1.0, FIELD_INDEX, lookup)
    }
    .expect("the tag is indexed");
    it.read().expect("read must not error");

    let guard = mock.spec_read();
    let outcome = Box::new(it)
        .suspend()
        .resume(&guard)
        .expect("resume must not error");
    let it = match outcome {
        ResumeOutcome::Ok(it) | ResumeOutcome::Moved(it) => it,
        ResumeOutcome::Aborted => panic!("an untouched index cannot abort the reader"),
    };

    assert_eq!(drain(*it), (2..=N).collect::<Vec<_>>());

    // SAFETY: `allocate` allocated it; the iterator using it is gone.
    drop(unsafe { Box::from_raw(tag_index) });
}

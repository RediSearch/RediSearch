/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests driving the C `QueryIterator`s the index builds — both
//! `TagIndex::open_reader` (port of `TagIndex_OpenReader`) and
//! `TagIndex::query_iterator_for_value` (port of
//! `TagIndex_GetIteratorFromTrieMapValue`) — through their vtable.

use std::ptr::{null, null_mut};

use rqe_iterators_test_utils::MockContext;
use tag_index::TagIndex;

/// Read every document id the iterator yields, in order, until `ITERATOR_EOF`.
///
/// # Safety
///
/// `it` must be a valid `QueryIterator` whose `Read` callback is populated
/// (always the case for the `RQEIteratorWrapper` the index builds).
unsafe fn drain(it: *mut ffi::QueryIterator) -> Vec<ffi::t_docId> {
    let mut doc_ids = Vec::new();
    loop {
        // SAFETY: `it` is a valid `QueryIterator`; `Read` is always set.
        let status = unsafe { (*it).Read.expect("Read is set")(it) };
        if status != ffi::IteratorStatus_ITERATOR_OK {
            assert_eq!(status, ffi::IteratorStatus_ITERATOR_EOF);
            break;
        }
        // SAFETY: `it` is valid and positioned on a result.
        doc_ids.push(unsafe { (*it).lastDocId });
    }
    doc_ids
}

/// `open_reader` yields an iterator that reads every indexed document id in
/// ascending order, then reports EOF (C: `testCreate`, the `TagIndex_OpenReader`
/// read loop). A few hundred docs is enough to cross block boundaries; the goal
/// is ordering/EOF correctness, not the 100k-scale benchmark the C test ran.
#[test]
fn open_reader_reads_all_ids_in_order() {
    const N: ffi::t_docId = 300;

    let mut tag_index = TagIndex::new(1, None, false);
    let tags: &[&[u8]] = &[b"hello"];
    for doc_id in 1..=N {
        tag_index.index(null(), null(), tags, doc_id);
    }

    let mock = MockContext::new(N, N as usize);
    let it = unsafe { tag_index.open_reader(mock.sctx(), b"hello", 1.0, 0, null_mut()) }
        .expect("the tag is indexed");

    // SAFETY: `it` is the valid iterator just built; freed below.
    let doc_ids = unsafe { drain(it.as_ptr()) };
    assert_eq!(doc_ids, (1..=N).collect::<Vec<_>>());

    // SAFETY: `it` is a valid `QueryIterator`; this hands ownership back.
    unsafe { (*it.as_ptr()).Free.expect("Free is set")(it.as_ptr()) };
}

/// After reading the only document, skipping past the last id reports EOF and
/// leaves `lastDocId` at or beyond the last read id (C: `testSkipToLastId`).
#[test]
fn skip_to_past_last_id_yields_eof() {
    let mut tag_index = TagIndex::new(1, None, false);
    let doc_id: ffi::t_docId = 1;
    tag_index.index(null(), null(), &[b"hello"], doc_id);

    let mock = MockContext::new(1, 1);
    let it = unsafe { tag_index.open_reader(mock.sctx(), b"hello", 1.0, 0, null_mut()) }
        .expect("the tag is indexed");
    let it = it.as_ptr();

    // SAFETY: `it` is the valid iterator just built.
    let status = unsafe { (*it).Read.expect("Read is set")(it) };
    assert_eq!(status, ffi::IteratorStatus_ITERATOR_OK);
    // SAFETY: `it` is valid and positioned on the first (only) result.
    assert_eq!(unsafe { (*it).lastDocId }, doc_id);

    // SAFETY: `it` is valid; `SkipTo` is populated by `RQEIteratorWrapper`.
    let status = unsafe { (*it).SkipTo.expect("SkipTo is set")(it, doc_id + 1) };
    assert_eq!(status, ffi::IteratorStatus_ITERATOR_EOF);
    // SAFETY: `it` is valid; the failed skip must not rewind `lastDocId`.
    assert!(unsafe { (*it).lastDocId } >= doc_id);

    // SAFETY: `it` is a valid `QueryIterator`; this hands ownership back.
    unsafe { (*it).Free.expect("Free is set")(it) };
}

/// Opening a reader on a tag that was never indexed yields no iterator (C:
/// `testOpenReaderEdgeCases`, absent-tag case). The NULL-index case is a C-ABI
/// concern and stays in the C++ suite.
#[test]
fn open_reader_absent_tag_returns_none() {
    let mut tag_index = TagIndex::new(1, None, false);
    tag_index.index(null(), null(), &[b"hello"], 1);

    let mock = MockContext::new(1, 1);
    assert!(
        unsafe { tag_index.open_reader(mock.sctx(), b"missing", 1.0, 0, null_mut()) }.is_none()
    );
}

/// Drive the `QueryIterator` built from an already-resolved inverted index
/// (`query_iterator_for_value`) through its vtable, checking that it yields
/// exactly the indexed document ids. This exercises the whole bridge:
/// `query_iterator_for_value` → `Tag` iterator → `RQEIteratorWrapper`.
#[test]
fn value_path_reads_all_matching_docs_via_c_vtable() {
    let mut tag_index = TagIndex::new(1, None, false);
    let tags: &[&[u8]] = &[b"team"];
    for doc_id in 1..=3 {
        tag_index.index(null(), null(), tags, doc_id);
    }

    let mock = MockContext::new(3, 3);
    let ii = tag_index.find_value(b"team").expect("tag was indexed");
    // SAFETY: `tag_index` and `mock` outlive the iterator, `ii` is the trie's
    // current value for the tag, and the iterator is freed below.
    let it = unsafe { tag_index.query_iterator_for_value(mock.sctx(), b"team", ii, 1.0, 0) };
    assert!(!it.is_null());

    // SAFETY: `it` is the valid iterator just built; freed below.
    let doc_ids = unsafe { drain(it) };
    assert_eq!(doc_ids, vec![1, 2, 3]);

    // SAFETY: `it` is a valid `QueryIterator`; this hands ownership back.
    unsafe { (*it).Free.expect("Free is set")(it) };
}

/// An inverted index with no documents yields no iterator, matching the C
/// `TagIndex_GetIteratorFromTrieMapValue` returning NULL for an empty value.
#[test]
fn value_path_returns_null_for_empty_inverted_index() {
    let mut tag_index = TagIndex::new(1, None, false);
    // Register the tag with a fresh, empty posting list (no documents indexed).
    tag_index
        .open_index(b"empty", true)
        .expect("empty inverted index registered");

    let mock = MockContext::new(0, 0);
    let ii = tag_index.find_value(b"empty").expect("tag was inserted");
    // SAFETY: `tag_index` and `mock` outlive the (never created) iterator.
    let it = unsafe { tag_index.query_iterator_for_value(mock.sctx(), b"empty", ii, 1.0, 0) };
    assert!(it.is_null());
}

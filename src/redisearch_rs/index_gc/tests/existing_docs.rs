/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{io::Cursor, mem};

use ffi::IndexFlags_Index_DocIdsOnly;
use index_gc::{
    DocExistence, GcError,
    scanners::existing_docs::{
        ExistingDocsEntry, apply_existing_docs, apply_existing_docs_with, collect_existing_docs,
        scan_existing_docs, snapshot_existing_docs,
    },
};
use index_result::RSIndexResult;
use index_spec::{IndexSpecReadGuard, IndexSpecWriteGuard};
use inverted_index::opaque::InvertedIndex as OpaqueInvertedIndex;
use inverted_index::{DocId, DocLiveness, GcScanDelta, InvertedIndex, doc_ids_only::DocIdsOnly};

// Link both Rust-provided and C-provided symbols
extern crate redisearch_rs;
use index_gc::fork::wire::deserialize;
// Provide Redis allocator shims so the C dict functions can allocate memory.
redis_mock::mock_or_stub_missing_redis_c_symbols!();

const fn make_spec(existing_docs: *mut ffi::InvertedIndex) -> ffi::IndexSpec {
    // SAFETY: zeroed IndexSpec is valid for read-only access; existingDocs is
    // either null or a caller-managed pointer that outlives this struct.
    let mut spec: ffi::IndexSpec = unsafe { mem::zeroed() };
    spec.existingDocs = existing_docs;
    spec
}

/// An existence snapshot for tests that never look a document up.
/// [`DocExistence::snapshot`] calls C functions that miri cannot run.
fn unqueried_existence<'a>(guard: &'a IndexSpecReadGuard<'a>) -> DocExistence<'a> {
    DocExistence::with_bitmap(guard, Box::default(), 0)
}

/// When the spec has no `existingDocs` index, the child writes `None`.
#[test]
fn no_existing_docs_writes_only_terminator() {
    let spec = make_spec(std::ptr::null_mut());
    // SAFETY: spec is valid and lives for the duration of the test.
    let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };

    let mut buf = Vec::new();
    collect_existing_docs(&mut buf, &guard, &unqueried_existence(&guard)).unwrap();

    let mut cursor = Cursor::new(&buf);
    assert!(
        deserialize::<Option<ExistingDocsEntry>>(&mut cursor, "test message")
            .unwrap()
            .is_none()
    );
}

/// When `existingDocs` is present but empty, `scan_gc` returns no delta and
/// only `None` is written.
#[test]
fn empty_existing_docs_writes_only_terminator() {
    let ii = Box::new(OpaqueInvertedIndex::DocIdsOnly(
        InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly),
    ));
    let ii_ptr = Box::into_raw(ii);
    let spec = make_spec(ii_ptr.cast());
    // SAFETY: spec and ii_ptr are valid and live for the duration of the test.
    let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };

    let mut buf = Vec::new();
    collect_existing_docs(&mut buf, &guard, &unqueried_existence(&guard)).unwrap();

    let mut cursor = Cursor::new(&buf);
    assert!(
        deserialize::<Option<ExistingDocsEntry>>(&mut cursor, "test message")
            .unwrap()
            .is_none()
    );

    // SAFETY: ii_ptr was created via Box::into_raw above.
    unsafe { drop(Box::from_raw(ii_ptr)) };
}

/// When `existingDocs` has entries and all docs are absent from the DocTable
/// (stub returns false), `scan_gc` produces a serialised `Some` [`GcScanDelta`]
/// followed by the stream terminator.
#[test]
#[cfg_attr(miri, ignore = "calls FFI function `DocTable_Exists`")]
fn existing_docs_with_deleted_entries_writes_delta() {
    let mut ii = InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
    ii.add_record(&RSIndexResult::build_virt().doc_id(1).build())
        .unwrap();
    ii.add_record(&RSIndexResult::build_virt().doc_id(2).build())
        .unwrap();
    let ii = Box::new(OpaqueInvertedIndex::DocIdsOnly(ii));
    let ii_ptr = Box::into_raw(ii);
    let spec = make_spec(ii_ptr.cast());
    // SAFETY: spec and ii_ptr are valid and live for the duration of the test.
    let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };

    let mut buf = Vec::new();
    collect_existing_docs(&mut buf, &guard, &DocExistence::snapshot(&guard)).unwrap();

    let mut cursor = Cursor::new(&buf);
    let entry = deserialize::<Option<ExistingDocsEntry>>(&mut cursor, "test message")
        .unwrap()
        .unwrap();
    assert_eq!(entry.delta.last_block_idx(), 0);
    // SAFETY: ii_ptr is live and nothing mutates it here.
    assert_eq!(entry.unique_id, u32::from(unsafe { &*ii_ptr }.unique_id()));
    assert!(
        deserialize::<Option<ExistingDocsEntry>>(&mut cursor, "test message")
            .unwrap()
            .is_none()
    );
    assert_eq!(cursor.position() as usize, buf.len());

    // SAFETY: ii_ptr was created via Box::into_raw above.
    unsafe { drop(Box::from_raw(ii_ptr)) };
}

#[test]
fn apply_returns_err_when_existing_docs_absent() {
    let entry = ExistingDocsEntry {
        unique_id: 0,
        delta: GcScanDelta::empty_for_testing(),
    };

    let mut apply_spec = make_spec(std::ptr::null_mut());
    // SAFETY: the test owns the initialized spec; no concurrent access occurs.
    let mut guard = unsafe { IndexSpecWriteGuard::from_locked_mut(&mut apply_spec) };
    assert!(matches!(
        apply_existing_docs(entry, &mut guard),
        Err(GcError::ApplyError(
            "the existingDocs inverted index was removed before the delta could be applied"
        ))
    ));
}

#[test]
#[cfg_attr(miri, ignore = "calls FFI function `DocTable_Exists`")]
fn roundtrip_all_docs_deleted_clears_index() {
    let mut ii = InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
    ii.add_record(&RSIndexResult::build_virt().doc_id(1).build())
        .unwrap();
    ii.add_record(&RSIndexResult::build_virt().doc_id(2).build())
        .unwrap();
    let ii = Box::new(OpaqueInvertedIndex::DocIdsOnly(ii));
    let ii_ptr = Box::into_raw(ii);
    let mut spec = make_spec(ii_ptr.cast());

    let mut buf = Vec::new();
    {
        // SAFETY: the test owns the initialized spec; no concurrent access occurs.
        let read_guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };
        collect_existing_docs(&mut buf, &read_guard, &DocExistence::snapshot(&read_guard)).unwrap();
    }

    let mut cursor = Cursor::new(&buf);
    let entry = deserialize::<Option<ExistingDocsEntry>>(&mut cursor, "test message")
        .unwrap()
        .unwrap();
    assert!(
        deserialize::<Option<ExistingDocsEntry>>(&mut cursor, "test message")
            .unwrap()
            .is_none()
    );
    assert_eq!(cursor.position() as usize, buf.len());

    // SAFETY: the test owns the initialized spec; no concurrent access occurs.
    let mut write_guard = unsafe { IndexSpecWriteGuard::from_locked_mut(&mut spec) };
    let stats = apply_existing_docs(entry, &mut write_guard).unwrap();

    assert!(write_guard.existing_docs_mut().is_none());
    assert!(stats.bytes_collected > 0);
}

/// `scan_existing_docs` finds the same delta `collect_existing_docs` sends.
#[test]
#[cfg_attr(miri, ignore = "calls FFI function `DocTable_Exists`")]
fn scan_existing_docs_matches_the_collected_delta() {
    let mut ii = InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
    ii.add_record(&RSIndexResult::build_virt().doc_id(1).build())
        .unwrap();
    let ii_ptr = Box::into_raw(Box::new(OpaqueInvertedIndex::DocIdsOnly(ii)));
    let spec = make_spec(ii_ptr.cast());
    // SAFETY: spec and ii_ptr are valid and live for the duration of the test.
    let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };

    let mut buf = Vec::new();
    collect_existing_docs(&mut buf, &guard, &DocExistence::snapshot(&guard)).unwrap();
    let collected =
        deserialize::<Option<ExistingDocsEntry>>(&mut Cursor::new(&buf), "test message")
            .unwrap()
            .expect("the index has docs to collect");
    assert_eq!(
        scan_existing_docs(&guard, |id| guard.doc_exists(id)),
        Some(collected)
    );

    // SAFETY: ii_ptr was created via Box::into_raw above.
    unsafe { drop(Box::from_raw(ii_ptr)) };
}

/// A spec without an existing-docs index has nothing to scan.
#[test]
fn scan_existing_docs_returns_none_without_an_index() {
    let spec = make_spec(std::ptr::null_mut());
    // SAFETY: spec is valid and live for the duration of the test.
    let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };
    assert_eq!(scan_existing_docs(&guard, |id| guard.doc_exists(id)), None);
}

/// A snapshot of the index scans to the same delta as the live index.
#[test]
#[cfg_attr(miri, ignore = "calls FFI function `DocTable_Exists`")]
fn a_snapshot_scans_like_the_live_index() {
    let mut ii = InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
    for doc_id in 1..=3 {
        ii.add_record(&RSIndexResult::build_virt().doc_id(doc_id).build())
            .unwrap();
    }
    let ii_ptr = Box::into_raw(Box::new(OpaqueInvertedIndex::DocIdsOnly(ii)));
    let spec = make_spec(ii_ptr.cast());
    // SAFETY: spec and ii_ptr are valid and live for the duration of the test.
    let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };
    let exists = |id: DocId| id != 2;

    let live = scan_existing_docs(&guard, exists).expect("doc 2 is gone");
    let snapshot = snapshot_existing_docs(&guard, &exists).expect("doc 2 is gone");
    assert_eq!(u32::from(snapshot.unique_id()), live.unique_id);
    assert_eq!(snapshot.scan_gc(exists).unwrap(), Some(live.delta));

    // SAFETY: ii_ptr was created via Box::into_raw above.
    unsafe { drop(Box::from_raw(ii_ptr)) };
}

/// A delta whose last block grew since the scan is rescanned when the apply has a
/// liveness source, and denied when it has none.
#[test]
#[cfg_attr(miri, ignore = "calls FFI function `DocTable_Exists`")]
fn apply_with_a_liveness_source_rescans_a_grown_last_block() {
    // Docs 1 and 2 are gone; the one the writer appends is live.
    let exists = |id: DocId| id > 2;
    for (live, denied, collected) in [
        (Some(&exists as &dyn DocLiveness), 0, true),
        (None, 1, false),
    ] {
        let mut ii = InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
        for doc_id in 1..=2 {
            ii.add_record(&RSIndexResult::build_virt().doc_id(doc_id).build())
                .unwrap();
        }
        let ii_ptr = Box::into_raw(Box::new(OpaqueInvertedIndex::DocIdsOnly(ii)));
        let mut spec = make_spec(ii_ptr.cast());
        let entry = {
            // SAFETY: spec and ii_ptr are valid and live for the duration of the test.
            let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };
            scan_existing_docs(&guard, exists).expect("both docs are gone")
        };
        // SAFETY: ii_ptr is live and nothing else accesses it here.
        let OpaqueInvertedIndex::DocIdsOnly(ii) = (unsafe { &mut *ii_ptr }) else {
            panic!("the index is a doc-ids-only index");
        };
        ii.add_record(&RSIndexResult::build_virt().doc_id(3).build())
            .unwrap();

        // SAFETY: spec is valid and exclusively accessed for the duration of the test.
        let mut guard = unsafe { IndexSpecWriteGuard::from_locked_mut(&mut spec) };
        let stats = apply_existing_docs_with(entry, live, &mut guard).unwrap();

        assert_eq!(stats.blocks_denied, denied);
        assert_eq!(
            stats.bytes_collected > 0,
            collected,
            "the gone docs' bytes are freed"
        );
        assert!(
            guard.existing_docs_mut().is_some(),
            "doc 3 is live, so the index stays"
        );
        // SAFETY: ii_ptr was created via Box::into_raw above and the index was kept.
        unsafe { drop(Box::from_raw(ii_ptr)) };
    }
}

/// An `existingDocs` index dropped and recreated after the scan keeps its new
/// postings: the stale delta names the old index and is rejected.
#[test]
#[cfg_attr(miri, ignore = "calls FFI function `DocTable_Exists`")]
fn a_delta_for_a_replaced_index_is_rejected() {
    let docs = |count: DocId| {
        let mut ii = InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
        for doc_id in 1..=count {
            ii.add_record(&RSIndexResult::build_virt().doc_id(doc_id).build())
                .unwrap();
        }
        Box::into_raw(Box::new(OpaqueInvertedIndex::DocIdsOnly(ii)))
    };
    let scanned = docs(2);
    let mut spec = make_spec(scanned.cast());
    let entry = {
        // SAFETY: spec and `scanned` are valid and live here.
        let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };
        scan_existing_docs(&guard, |_| false).expect("both docs are gone")
    };
    let replacement = docs(2);
    spec.existingDocs = replacement.cast();

    // SAFETY: spec is valid and exclusively accessed for the duration of the test.
    let mut guard = unsafe { IndexSpecWriteGuard::from_locked_mut(&mut spec) };
    assert!(matches!(
        apply_existing_docs(entry, &mut guard),
        Err(GcError::ApplyError(
            "the existingDocs inverted index is not the one scanned"
        ))
    ));
    assert_eq!(
        guard.existing_docs_mut().map(|ii| ii.unique_docs()),
        Some(2),
        "the replacement keeps its postings"
    );
    // SAFETY: both indexes were created via Box::into_raw above and are dropped once.
    unsafe {
        drop(Box::from_raw(scanned));
        drop(Box::from_raw(replacement));
    }
}

/// Reports every doc id the fixtures use as present, whatever the spec's
/// document table says, as a snapshot taken before those docs were deleted.
fn all_live<'a>(guard: &'a IndexSpecReadGuard<'a>) -> DocExistence<'a> {
    const WORDS: usize = 1 << 14;
    DocExistence::with_bitmap(
        guard,
        vec![u64::MAX; WORDS].into_boxed_slice(),
        (WORDS * 64 - 1) as u64,
    )
}

/// The scanner must answer existence from `docs`: the fixture's document table
/// is empty, so a scanner that looked docs up itself would collect them all.
#[test]
fn docs_live_in_the_snapshot_are_kept() {
    let mut ii = InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
    for id in 1..=2 {
        ii.add_record(&RSIndexResult::build_virt().doc_id(id).build())
            .unwrap();
    }
    let ii_ptr = Box::into_raw(Box::new(OpaqueInvertedIndex::DocIdsOnly(ii)));
    let spec = make_spec(ii_ptr.cast());
    // SAFETY: spec and ii_ptr are valid and live for the duration of the test.
    let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };

    let mut buf = Vec::new();
    collect_existing_docs(&mut buf, &guard, &all_live(&guard)).unwrap();

    let mut cursor = Cursor::new(&buf);
    assert!(
        deserialize::<Option<GcScanDelta>>(&mut cursor, "test message")
            .unwrap()
            .is_none()
    );
    assert_eq!(cursor.position() as usize, buf.len());

    // SAFETY: ii_ptr was created via Box::into_raw above.
    unsafe { drop(Box::from_raw(ii_ptr)) };
}

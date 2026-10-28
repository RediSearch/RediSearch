/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{ffi::CString, io::Cursor, mem};

use dict::MissingFieldDictType;
use dict::OwnedDict;
use ffi::IndexFlags_Index_DocIdsOnly;
use fork_gc::{
    HandleError,
    missing_docs::{
        MissingDocsEntry, apply_missing_docs, apply_missing_docs_with, collect_missing_docs,
        missing_field_names, receive_missing_docs, scan_missing_field, snapshot_missing_field,
    },
};
use hidden_string::OwnedHiddenString;
use index_result::RSIndexResult;
use index_spec::{IndexSpecReadGuard, IndexSpecWriteGuard};
use inverted_index::opaque::InvertedIndex as OpaqueInvertedIndex;
use inverted_index::{DocId, DocLiveness, GcScanDelta, InvertedIndex, doc_ids_only::DocIdsOnly};
use serde::Serialize as _;

// Link both Rust-provided and C-provided symbols
extern crate redisearch_rs;
// Provide Redis allocator shims so the C dict functions can allocate memory.
redis_mock::mock_or_stub_missing_redis_c_symbols!();

/// Creates an inverted index containing `count` documents.
fn index(count: u64) -> Box<OpaqueInvertedIndex> {
    let mut ii = InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
    for doc_id in 1..=count {
        ii.add_record(&RSIndexResult::build_virt().doc_id(doc_id).build())
            .unwrap();
    }
    Box::new(OpaqueInvertedIndex::DocIdsOnly(ii))
}

/// Builds an `IndexSpec` with a `missing.indexes` with the provided entries.
fn make_spec(
    entries: impl IntoIterator<Item = (&'static [u8], Box<OpaqueInvertedIndex>)>,
) -> (ffi::IndexSpec, OwnedDict<MissingFieldDictType>) {
    let mut dict: OwnedDict<MissingFieldDictType> = OwnedDict::create();
    for (field_name, ii) in entries {
        let field_name = CString::new(field_name).unwrap();
        let hidden = OwnedHiddenString::new(&field_name);
        dict.try_insert(&hidden, ii).unwrap();
    }

    // SAFETY: zeroed IndexSpec is valid for read-only field access through the guard.
    let mut spec: ffi::IndexSpec = unsafe { mem::zeroed() };
    spec.missing.indexes = dict.as_mut_ptr();
    (spec, dict)
}

/// When the dict has no entries, the child writes `None`.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `missingFieldDictType`")]
fn empty_dict_writes_only_terminator() {
    let (spec, _dict) = make_spec([]);
    let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };

    let mut buf = Vec::new();
    collect_missing_docs(&mut buf, &*guard).unwrap();

    let mut cursor = Cursor::new(&buf);
    assert!(
        rmp_serde::from_read::<_, Option<MissingDocsEntry>>(&mut cursor)
            .unwrap()
            .is_none()
    );
    assert_eq!(cursor.position(), buf.len() as u64);
}

/// An entry whose inverted index is empty produces no delta, so it is skipped.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `missingFieldDictType`")]
fn empty_inverted_index_is_skipped() {
    let (spec, _dict) = make_spec([(&b"empty_field"[..], index(0))]);
    let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };

    let mut buf = Vec::new();
    collect_missing_docs(&mut buf, &*guard).unwrap();

    let mut cursor = Cursor::new(&buf);
    assert!(
        rmp_serde::from_read::<_, Option<MissingDocsEntry>>(&mut cursor)
            .unwrap()
            .is_none()
    );
    assert_eq!(cursor.position(), buf.len() as u64);
}

/// When an entry has docs absent from the DocTable, `scan_gc` produces a delta.
/// The output is a serialised [`MissingDocsEntry`].
///
/// The spec's `DocTable` is zeroed, so `DocTable_Exists` returns `false` for
/// every doc ID — every recorded doc is treated as deleted.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `missingFieldDictType`")]
fn entry_with_deleted_docs_writes_delta_frame() {
    let (spec, _dict) = make_spec([(&b"age"[..], index(2))]);
    let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };

    let mut buf = Vec::new();
    collect_missing_docs(&mut buf, &*guard).unwrap();

    let mut cursor = Cursor::new(&buf);

    let entry = rmp_serde::from_read::<_, Option<MissingDocsEntry>>(&mut cursor)
        .unwrap()
        .unwrap();
    assert_eq!(&*entry.field_name, b"age\0");
    assert!(
        rmp_serde::from_read::<_, Option<MissingDocsEntry>>(&mut cursor)
            .unwrap()
            .is_none()
    );
    assert_eq!(cursor.position(), buf.len() as u64);
}

/// Multiple entries each produce their own serialised message, followed by
/// `None`.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `missingFieldDictType`")]
fn multiple_entries_write_multiple_delta_frames() {
    let (spec, _dict) = make_spec([(&b"field_a"[..], index(1)), (&b"field_b"[..], index(1))]);
    let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };

    let mut buf = Vec::new();
    collect_missing_docs(&mut buf, &*guard).unwrap();

    let mut cursor = Cursor::new(&buf);

    let first = rmp_serde::from_read::<_, Option<MissingDocsEntry>>(&mut cursor)
        .unwrap()
        .unwrap();
    let second = rmp_serde::from_read::<_, Option<MissingDocsEntry>>(&mut cursor)
        .unwrap()
        .unwrap();

    let mut names = [first.field_name, second.field_name];
    names.sort(); // Entries can come in any order because dict iteration order is undefined.

    assert_eq!(names, [b"field_a\0".to_vec(), b"field_b\0".to_vec()]);
    assert!(
        rmp_serde::from_read::<_, Option<MissingDocsEntry>>(&mut cursor)
            .unwrap()
            .is_none()
    );
    assert_eq!(cursor.position(), buf.len() as u64);
}

#[test]
fn receive_none_returns_none() {
    let mut buf = Vec::new();
    Option::<MissingDocsEntry<&[u8]>>::None
        .serialize(&mut rmp_serde::Serializer::new(&mut buf))
        .unwrap();
    let mut cursor = Cursor::new(&buf);
    assert!(matches!(receive_missing_docs(&mut cursor).unwrap(), None));
}

#[test]
fn receive_malformed_entry_returns_codec_error() {
    let mut cursor = Cursor::new(b"garbage");
    assert!(matches!(
        receive_missing_docs(&mut cursor),
        Err(HandleError::Codec {
            msg: "decoding missing-docs entry",
            ..
        })
    ));
}

#[test]
fn receive_entry_returns_field_name_and_delta() {
    let mut buf = Vec::new();
    Some(MissingDocsEntry {
        field_name: b"age\0".as_slice(),
        delta: GcScanDelta::empty_for_testing(),
    })
    .serialize(&mut rmp_serde::Serializer::new(&mut buf))
    .unwrap();

    let mut cursor = Cursor::new(&buf);
    let (field_name, _delta) = receive_missing_docs(&mut cursor).unwrap().unwrap();
    assert_eq!(field_name, c"age");
}

#[test]
fn receive_entry_rejects_a_field_name_without_a_trailing_nul() {
    let mut buf = Vec::new();
    Some(MissingDocsEntry {
        field_name: b"age".as_slice(),
        delta: GcScanDelta::empty_for_testing(),
    })
    .serialize(&mut rmp_serde::Serializer::new(&mut buf))
    .unwrap();

    let mut cursor = Cursor::new(&buf);
    assert!(matches!(
        receive_missing_docs(&mut cursor),
        Err(HandleError::Codec {
            msg: "decoding missing-docs field name",
            ..
        })
    ));
}

#[test]
#[cfg_attr(miri, ignore = "accesses extern static `missingFieldDictType`")]
fn apply_returns_err_when_field_not_found() {
    let (mut spec, _dict) = make_spec([]);
    let delta = GcScanDelta::empty_for_testing();

    let mut write_guard = unsafe { IndexSpecWriteGuard::from_locked_mut(&mut spec) };
    assert!(matches!(
        apply_missing_docs(c"nonexistent", delta, &mut *write_guard),
        Err(HandleError::ApplyError(
            "the field was removed from missing.indexes before the delta could be applied"
        ))
    ));
}

/// A successful apply with a no-op delta leaves the dict entry in place.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `missingFieldDictType`")]
fn apply_succeeds_and_keeps_entry_when_docs_remain() {
    let (mut spec, _dict) = make_spec([(&b"age"[..], index(2))]);
    let delta = GcScanDelta::empty_for_testing();

    let mut write_guard = unsafe { IndexSpecWriteGuard::from_locked_mut(&mut spec) };
    let stats = apply_missing_docs(c"age", delta, &mut *write_guard).unwrap();

    assert_eq!(stats.records_removed, 0);
    let hidden = OwnedHiddenString::new(c"age");
    assert!(
        write_guard
            .missing_field_dict_mut()
            .fetch_mut(&hidden)
            .is_some()
    );
}

/// Full child-to-parent roundtrip: when all docs are deleted the dict entry is removed.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `missingFieldDictType`")]
fn roundtrip_all_docs_deleted_removes_entry() {
    let (mut spec, _dict) = make_spec([(&b"age"[..], index(2))]);

    // Child side: collect.
    let mut buf = Vec::new();
    {
        let read_guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };
        collect_missing_docs(&mut buf, &*read_guard).unwrap();
    }

    // Parent side: receive.
    let mut cursor = Cursor::new(&buf);
    let (field_name, delta) = receive_missing_docs(&mut cursor).unwrap().unwrap();
    assert_eq!(field_name, c"age");

    // Parent side: apply.
    let mut write_guard = unsafe { IndexSpecWriteGuard::from_locked_mut(&mut spec) };
    let stats = apply_missing_docs(&field_name, delta, &mut *write_guard).unwrap();

    let hidden = OwnedHiddenString::new(c"age");
    assert!(
        write_guard
            .missing_field_dict_mut()
            .fetch_mut(&hidden)
            .is_none()
    );
    assert!(stats.bytes_collected > 0);
    assert_eq!(stats.records_removed, 0);
}

/// `missing_field_names` lists every field in the dict, including those with
/// nothing to collect, so a thread cycle sees the same fields the child does.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `missingFieldDictType`")]
fn missing_field_names_lists_every_field() {
    let (spec, _dict) = make_spec([(&b"field_a"[..], index(1)), (&b"field_b"[..], index(0))]);
    // SAFETY: `spec` and its dict are live for the duration of the test.
    let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };

    let mut names = missing_field_names(&guard);
    names.sort();
    assert_eq!(names, [c"field_a".to_owned(), c"field_b".to_owned()]);
}

/// `scan_missing_field` finds the same delta `collect_missing_docs` sends, and
/// nothing for an unknown or empty field.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `missingFieldDictType`")]
fn scan_missing_field_matches_the_collected_entry() {
    let (spec, _dict) = make_spec([(&b"age"[..], index(2)), (&b"empty"[..], index(0))]);
    // SAFETY: `spec` and its dict are live for the duration of the test.
    let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };

    let mut buf = Vec::new();
    collect_missing_docs(&mut buf, &*guard).unwrap();
    let collected = rmp_serde::from_read::<_, Option<MissingDocsEntry>>(&mut Cursor::new(&buf))
        .unwrap()
        .expect("the field has docs to collect");

    assert_eq!(
        scan_missing_field(&guard, c"age", |id| guard.doc_exists(id)),
        Some(collected.delta)
    );
    assert_eq!(
        scan_missing_field(&guard, c"empty", |id| guard.doc_exists(id)),
        None
    );
    assert_eq!(
        scan_missing_field(&guard, c"unknown", |id| guard.doc_exists(id)),
        None
    );
}

/// A snapshot of a field's index scans to the same delta as the live index.
#[test]
#[cfg_attr(miri, ignore = "calls FFI function `DocTable_Exists`")]
fn a_snapshot_scans_like_the_live_index() {
    let (spec, _dict) = make_spec([(&b"f"[..], index(3))]);
    // SAFETY: spec is valid and live for the duration of the test.
    let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };
    let field = CString::new("f").unwrap();
    let exists = |id: DocId| id != 2;

    let live_delta = scan_missing_field(&guard, &field, exists).expect("doc 2 is gone");
    let snapshot = snapshot_missing_field(&guard, &field, &exists).expect("doc 2 is gone");
    assert_eq!(snapshot.scan_gc(exists).unwrap(), Some(live_delta));
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
        let (mut spec, mut dict) = make_spec([(&b"f"[..], index(2))]);
        let field = CString::new("f").unwrap();
        let delta = {
            // SAFETY: spec is valid and live for the duration of the test.
            let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };
            scan_missing_field(&guard, &field, exists).expect("both docs are gone")
        };
        let Some(OpaqueInvertedIndex::DocIdsOnly(ii)) =
            dict.fetch_mut(&OwnedHiddenString::new(&field))
        else {
            panic!("the field is indexed as a doc-ids-only index");
        };
        ii.add_record(&RSIndexResult::build_virt().doc_id(3).build())
            .unwrap();

        // SAFETY: spec is valid and exclusively accessed for the duration of the test.
        let mut guard = unsafe { IndexSpecWriteGuard::from_locked_mut(&mut spec) };
        let stats = apply_missing_docs_with(&field, delta, live, &mut guard).unwrap();

        assert_eq!(stats.blocks_denied, denied);
        assert_eq!(
            stats.bytes_collected > 0,
            collected,
            "the gone docs' bytes are freed"
        );
        assert!(
            guard
                .missing_field_dict_mut()
                .fetch(&OwnedHiddenString::new(&field))
                .is_some(),
            "doc 3 is live, so the field's index stays"
        );
    }
}

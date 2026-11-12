/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{ffi::CString, io::Cursor, mem};

use field_spec::{FieldSpecBuilder, FieldSpecType};

use dict::MissingFieldDictType;
use dict::OwnedDict;
use ffi::IndexFlags_Index_DocIdsOnly;
use hidden_string::OwnedHiddenString;
use index_gc::{
    GcError,
    scanners::missing_docs::{
        MissingDocsEntry, apply_missing_docs, apply_missing_docs_with, collect_missing_docs,
        missing_field_indexes, receive_missing_docs, scan_missing_field, snapshot_missing_field,
    },
};
use index_result::RSIndexResult;
use index_spec::{IndexSpecReadGuard, IndexSpecWriteGuard};
use inverted_index::opaque::InvertedIndex as OpaqueInvertedIndex;
use inverted_index::{DocId, DocLiveness, GcScanDelta, InvertedIndex, doc_ids_only::DocIdsOnly};

// Link both Rust-provided and C-provided symbols
extern crate redisearch_rs;
use index_gc::fork::wire::{deserialize, serialize};
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

/// The unique id of `field`'s missing-docs index in `spec`.
fn unique_id_of(spec: &ffi::IndexSpec, field: &std::ffi::CStr) -> u32 {
    // SAFETY: the test owns `spec` and nothing mutates it during this read.
    let guard = unsafe { IndexSpecReadGuard::from_locked(spec) };
    let ii = guard
        .missing_field_dict()
        .fetch(&OwnedHiddenString::new(field))
        .expect("the field is indexed");
    u32::from(ii.unique_id())
}

/// The field array a [`make_spec`] spec points at, freeing each field's name on drop.
struct Fields(Vec<ffi::FieldSpec>);

impl Drop for Fields {
    fn drop(&mut self) {
        for fs in &self.0 {
            // SAFETY: `FieldSpecBuilder::new` makes `fieldName` and `fieldPath` one
            // owned allocation, so this frees it exactly once.
            unsafe { ffi::HiddenString_Free(fs.fieldName, true) };
        }
    }
}

/// Builds an `IndexSpec` with one field per entry, in order, and a
/// `missing.indexes` holding each entry's inverted index under its field's name.
///
/// The spec's document table is empty, so every doc id in those entries counts
/// as deleted and a scan collects it.
fn make_spec(
    entries: impl IntoIterator<Item = (&'static [u8], Box<OpaqueInvertedIndex>)>,
) -> (ffi::IndexSpec, OwnedDict<MissingFieldDictType>, Fields) {
    let mut dict: OwnedDict<MissingFieldDictType> = OwnedDict::create();
    let mut fields = Vec::new();
    for (field_name, ii) in entries {
        let field_name = CString::new(field_name).unwrap();
        fields.push(
            FieldSpecBuilder::new(&field_name)
                .with_types(FieldSpecType::Numeric.into())
                .finish(),
        );
        let hidden = OwnedHiddenString::new(&field_name);
        dict.try_insert(&hidden, ii).unwrap();
    }

    // SAFETY: zeroed IndexSpec is valid for read-only field access through the guard.
    let mut spec: ffi::IndexSpec = unsafe { mem::zeroed() };
    spec.missing.indexes = dict.as_mut_ptr();
    // The Vec's heap buffer stays put when it moves into `Fields`.
    spec.fields = fields.as_mut_ptr();
    spec.numFields = fields.len() as u16;
    (spec, dict, Fields(fields))
}

/// When the dict has no entries, the child writes `None`.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `missingFieldDictType`")]
fn empty_dict_writes_only_terminator() {
    let (spec, _dict, _fields) = make_spec([]);
    // SAFETY: the test owns the initialized spec; no concurrent access occurs.
    let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };

    let mut buf = Vec::new();
    collect_missing_docs(&mut buf, &guard, |id| guard.doc_exists(id)).unwrap();

    let mut cursor = Cursor::new(&buf);
    assert!(
        deserialize::<Option<MissingDocsEntry>>(&mut cursor, "test message")
            .unwrap()
            .is_none()
    );
    assert_eq!(cursor.position(), buf.len() as u64);
}

/// An entry whose inverted index is empty produces no delta, so it is skipped.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `missingFieldDictType`")]
fn empty_inverted_index_is_skipped() {
    let (spec, _dict, _fields) = make_spec([(&b"empty_field"[..], index(0))]);
    // SAFETY: the test owns the initialized spec; no concurrent access occurs.
    let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };

    let mut buf = Vec::new();
    collect_missing_docs(&mut buf, &guard, |id| guard.doc_exists(id)).unwrap();

    let mut cursor = Cursor::new(&buf);
    assert!(
        deserialize::<Option<MissingDocsEntry>>(&mut cursor, "test message")
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
    let (spec, _dict, _fields) = make_spec([(&b"age"[..], index(2))]);
    // SAFETY: the test owns the initialized spec; no concurrent access occurs.
    let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };

    let mut buf = Vec::new();
    collect_missing_docs(&mut buf, &guard, |id| guard.doc_exists(id)).unwrap();

    let mut cursor = Cursor::new(&buf);

    let entry = deserialize::<Option<MissingDocsEntry>>(&mut cursor, "test message")
        .unwrap()
        .unwrap();
    assert_eq!(entry.field_index, 0);
    assert!(
        deserialize::<Option<MissingDocsEntry>>(&mut cursor, "test message")
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
    let (spec, _dict, _fields) =
        make_spec([(&b"field_a"[..], index(1)), (&b"field_b"[..], index(1))]);
    // SAFETY: the test owns the initialized spec; no concurrent access occurs.
    let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };

    let mut buf = Vec::new();
    collect_missing_docs(&mut buf, &guard, |id| guard.doc_exists(id)).unwrap();

    let mut cursor = Cursor::new(&buf);

    let first = deserialize::<Option<MissingDocsEntry>>(&mut cursor, "test message")
        .unwrap()
        .unwrap();
    let second = deserialize::<Option<MissingDocsEntry>>(&mut cursor, "test message")
        .unwrap()
        .unwrap();

    assert_eq!([first.field_index, second.field_index], [0, 1]);
    assert!(
        deserialize::<Option<MissingDocsEntry>>(&mut cursor, "test message")
            .unwrap()
            .is_none()
    );
    assert_eq!(cursor.position(), buf.len() as u64);
}

#[test]
fn receive_none_returns_none() {
    let mut buf = Vec::new();
    serialize(&mut buf, Option::<MissingDocsEntry>::None).unwrap();
    let mut cursor = Cursor::new(&buf);
    assert!(receive_missing_docs(&mut cursor).unwrap().is_none());
}

#[test]
fn receive_malformed_entry_returns_codec_error() {
    let mut cursor = Cursor::new(b"garbage");
    assert!(matches!(
        receive_missing_docs(&mut cursor),
        Err(GcError::Codec {
            msg: "decoding missing-docs entry",
            ..
        })
    ));
}

#[test]
fn receive_entry_returns_field_index_and_delta() {
    let mut buf = Vec::new();
    serialize(
        &mut buf,
        Some(MissingDocsEntry {
            field_index: 3,
            unique_id: 7,
            delta: GcScanDelta::empty_for_testing(),
        }),
    )
    .unwrap();

    let mut cursor = Cursor::new(&buf);
    let entry = receive_missing_docs(&mut cursor).unwrap().unwrap();
    assert_eq!(entry.field_index, 3);
    assert_eq!(entry.unique_id, 7);
}

#[test]
#[cfg_attr(miri, ignore = "accesses extern static `missingFieldDictType`")]
fn apply_returns_err_when_field_not_found() {
    let (mut spec, _dict, _fields) = make_spec([]);
    let delta = GcScanDelta::empty_for_testing();

    // SAFETY: the test owns the initialized spec; no concurrent access occurs.
    let mut write_guard = unsafe { IndexSpecWriteGuard::from_locked_mut(&mut spec) };
    assert!(matches!(
        apply_missing_docs(0, 0, delta, &mut write_guard),
        Err(GcError::TargetGone(
            "no field in the spec is at the scanned field index"
        ))
    ));
}

/// A successful apply with a no-op delta leaves the dict entry in place.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `missingFieldDictType`")]
fn apply_succeeds_and_keeps_entry_when_docs_remain() {
    let (mut spec, _dict, _fields) = make_spec([(&b"age"[..], index(2))]);
    let delta = GcScanDelta::empty_for_testing();
    let unique_id = unique_id_of(&spec, c"age");

    // SAFETY: the test owns the initialized spec; no concurrent access occurs.
    let mut write_guard = unsafe { IndexSpecWriteGuard::from_locked_mut(&mut spec) };
    let stats = apply_missing_docs(0, unique_id, delta, &mut write_guard).unwrap();

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
    let (mut spec, _dict, _fields) = make_spec([(&b"age"[..], index(2))]);

    // Child side: collect.
    let mut buf = Vec::new();
    {
        // SAFETY: the test owns the initialized spec; no concurrent access occurs.
        let read_guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };
        collect_missing_docs(&mut buf, &read_guard, |id| read_guard.doc_exists(id)).unwrap();
    }

    // Parent side: receive.
    let mut cursor = Cursor::new(&buf);
    let entry = receive_missing_docs(&mut cursor).unwrap().unwrap();
    assert_eq!(entry.field_index, 0);
    assert_eq!(entry.unique_id, unique_id_of(&spec, c"age"));

    // Parent side: apply.
    // SAFETY: the test owns the initialized spec; no concurrent access occurs.
    let mut write_guard = unsafe { IndexSpecWriteGuard::from_locked_mut(&mut spec) };
    let stats = apply_missing_docs(
        entry.field_index,
        entry.unique_id,
        entry.delta,
        &mut write_guard,
    )
    .unwrap();

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

/// `missing_field_indexes` lists every field in the dict, including those with
/// nothing to collect, so a thread cycle sees the same fields the child does,
/// and skips fields the dict has no index for.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `missingFieldDictType`")]
fn missing_field_indexes_lists_every_field_in_the_dict() {
    let (spec, mut dict, _fields) = make_spec([
        (&b"field_a"[..], index(1)),
        (&b"field_b"[..], index(0)),
        (&b"field_c"[..], index(1)),
    ]);
    assert!(dict.remove(&OwnedHiddenString::new(c"field_b")));
    // SAFETY: `spec` and its dict are live for the duration of the test.
    let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };

    assert_eq!(missing_field_indexes(&guard), [0, 2]);
}

/// `scan_missing_field` finds the same delta `collect_missing_docs` sends, and
/// nothing for an unknown or empty field.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `missingFieldDictType`")]
fn scan_missing_field_matches_the_collected_entry() {
    let (spec, _dict, _fields) = make_spec([(&b"age"[..], index(2)), (&b"empty"[..], index(0))]);
    // SAFETY: `spec` and its dict are live for the duration of the test.
    let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };

    let mut buf = Vec::new();
    collect_missing_docs(&mut buf, &guard, |id| guard.doc_exists(id)).unwrap();
    let collected = deserialize::<Option<MissingDocsEntry>>(&mut Cursor::new(&buf), "test message")
        .unwrap()
        .expect("the field has docs to collect");

    assert_eq!(
        scan_missing_field(&guard, 0, |id| guard.doc_exists(id)),
        Some((collected.unique_id, collected.delta))
    );
    assert_eq!(
        scan_missing_field(&guard, 1, |id| guard.doc_exists(id)),
        None
    );
    assert_eq!(
        scan_missing_field(&guard, 2, |id| guard.doc_exists(id)),
        None
    );
}

/// A snapshot of a field's index scans to the same delta as the live index.
#[test]
#[cfg_attr(miri, ignore = "calls FFI function `DocTable_Exists`")]
fn a_snapshot_scans_like_the_live_index() {
    let (spec, _dict, _fields) = make_spec([(&b"f"[..], index(3))]);
    // SAFETY: spec is valid and live for the duration of the test.
    let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };
    let exists = |id: DocId| id != 2;

    let (unique_id, live_delta) = scan_missing_field(&guard, 0, exists).expect("doc 2 is gone");
    let snapshot = snapshot_missing_field(&guard, 0, &exists).expect("doc 2 is gone");
    assert_eq!(u32::from(snapshot.unique_id()), unique_id);
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
        let (mut spec, mut dict, _fields) = make_spec([(&b"f"[..], index(2))]);
        let field = CString::new("f").unwrap();
        let (unique_id, delta) = {
            // SAFETY: spec is valid and live for the duration of the test.
            let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };
            scan_missing_field(&guard, 0, exists).expect("both docs are gone")
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
        let stats = apply_missing_docs_with(0, unique_id, delta, live, &mut guard).unwrap();

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

/// A field whose index was dropped and recreated after the scan keeps its new
/// postings: the stale delta names the old index and is rejected.
#[test]
#[cfg_attr(miri, ignore = "calls FFI function `DocTable_Exists`")]
fn a_delta_for_a_replaced_index_is_rejected() {
    let (mut spec, mut dict, _fields) = make_spec([(&b"f"[..], index(2))]);
    let field = CString::new("f").unwrap();
    let (unique_id, delta) = {
        // SAFETY: spec is valid and live for the duration of the test.
        let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };
        scan_missing_field(&guard, 0, |_| false).expect("both docs are gone")
    };
    let hidden = OwnedHiddenString::new(&field);
    assert!(dict.remove(&hidden));
    dict.try_insert(&hidden, index(2)).unwrap();
    assert_ne!(unique_id_of(&spec, &field), unique_id);

    // SAFETY: spec is valid and exclusively accessed for the duration of the test.
    let mut guard = unsafe { IndexSpecWriteGuard::from_locked_mut(&mut spec) };
    assert!(matches!(
        apply_missing_docs(0, unique_id, delta, &mut guard),
        Err(GcError::TargetGone(
            "the field's missing-docs inverted index is not the one scanned"
        ))
    ));
    let kept = guard
        .missing_field_dict_mut()
        .fetch(&hidden)
        .expect("the replacement stays");
    assert_eq!(kept.unique_docs(), 2);
}

/// The scanner must answer existence from `docs`: the fixture's document table
/// is empty, so a scanner that looked docs up itself would collect them all.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `missingFieldDictType`")]
fn docs_live_in_the_snapshot_are_kept() {
    let (spec, _dict, _fields) = make_spec([(&b"age"[..], index(3))]);
    // SAFETY: the test owns the initialized spec; no concurrent access occurs.
    let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };
    let mut buf = Vec::new();
    collect_missing_docs(&mut buf, &guard, |_| true).unwrap();

    let mut cursor = Cursor::new(&buf);
    assert!(
        deserialize::<Option<MissingDocsEntry>>(&mut cursor, "test message")
            .unwrap()
            .is_none()
    );
    assert_eq!(cursor.position(), buf.len() as u64);
}

/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{io::Cursor, mem};

use dict::MissingFieldDictType;
use dict::OwnedDict;
use ffi::IndexFlags_Index_DocIdsOnly;
use fork_gc::{
    Frame,
    missing_docs::{HandleError, apply_missing_docs, collect_missing_docs, receive_missing_docs},
    util::with_hidden_string_ref,
};
use index_result::RSIndexResult;
use index_spec::{IndexSpecReadGuard, IndexSpecWriteGuard};
use inverted_index::opaque::InvertedIndex as OpaqueInvertedIndex;
use inverted_index::{GcScanDelta, InvertedIndex, doc_ids_only::DocIdsOnly};
use serde::Serialize as _;

// Link both Rust-provided and C-provided symbols
extern crate redisearch_rs;
// Provide Redis allocator shims so the C dict functions can allocate memory.
redis_mock::mock_or_stub_missing_redis_c_symbols!();

/// Add one entry to `dict` keyed by `field_name`, with value `ii`.
///
/// `MissingFieldDictType` uses `missingFieldDictType`, which copies the key via
/// `keyDup` and owns the value via `valDestructor` (→ `InvertedIndex_Free`).
/// Ownership of `ii` is transferred to the dict.
fn add_entry(
    dict: &mut OwnedDict<MissingFieldDictType>,
    field_name: &[u8],
    ii: Box<OpaqueInvertedIndex>,
) {
    with_hidden_string_ref(field_name, |key| dict.try_insert(key, ii).unwrap());
}

fn make_spec(dict: &OwnedDict<MissingFieldDictType>) -> ffi::IndexSpec {
    // SAFETY: zeroed IndexSpec is valid for read-only field access through the guard.
    let mut spec: ffi::IndexSpec = unsafe { mem::zeroed() };
    spec.missingFieldDict = dict.as_mut_ptr();
    spec
}

/// When the dict has no entries, only a Terminator is written.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `missingFieldDictType`")]
fn empty_dict_writes_only_terminator() {
    let dict = OwnedDict::create();
    let spec = make_spec(&dict);
    let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };

    let mut buf = Vec::new();
    collect_missing_docs(&mut buf, &*guard).unwrap();

    let mut cursor = Cursor::new(&buf);
    assert!(matches!(
        Frame::decode(&mut cursor).unwrap(),
        Frame::Terminator
    ));
    assert_eq!(cursor.position(), buf.len() as u64);
}

/// An entry whose inverted index is empty produces no delta, so it is skipped.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `missingFieldDictType`")]
fn empty_inverted_index_is_skipped() {
    let ii = Box::new(OpaqueInvertedIndex::DocIdsOnly(
        InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly),
    ));

    let mut dict = OwnedDict::create();
    add_entry(&mut dict, b"empty_field", ii);
    let spec = make_spec(&dict);
    let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };

    let mut buf = Vec::new();
    collect_missing_docs(&mut buf, &*guard).unwrap();

    let mut cursor = Cursor::new(&buf);
    assert!(matches!(
        Frame::decode(&mut cursor).unwrap(),
        Frame::Terminator
    ));
    assert_eq!(cursor.position(), buf.len() as u64);
}

/// When an entry has docs absent from the DocTable, `scan_gc` produces a delta.
/// The output is a Data frame carrying the field name, the serialised
/// [`GcScanDelta`], then a Terminator.
///
/// The spec's `DocTable` is zeroed, so `DocTable_Exists` returns `false` for
/// every doc ID — every recorded doc is treated as deleted.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `missingFieldDictType`")]
fn entry_with_deleted_docs_writes_delta_frame() {
    let mut ii = InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
    ii.add_record(&RSIndexResult::build_virt().doc_id(1).build())
        .unwrap();
    ii.add_record(&RSIndexResult::build_virt().doc_id(2).build())
        .unwrap();
    let ii = Box::new(OpaqueInvertedIndex::DocIdsOnly(ii));

    let mut dict = OwnedDict::create();
    add_entry(&mut dict, b"age", ii);
    let spec = make_spec(&dict);
    let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };

    let mut buf = Vec::new();
    collect_missing_docs(&mut buf, &*guard).unwrap();

    let mut cursor = Cursor::new(&buf);

    let frame = Frame::decode(&mut cursor).unwrap();
    let Frame::Data(name) = frame else {
        panic!("expected Data frame, got {frame:?}");
    };
    assert_eq!(&*name, b"age");

    let _delta: GcScanDelta = rmp_serde::from_read(&mut cursor).unwrap();

    assert!(matches!(
        Frame::decode(&mut cursor).unwrap(),
        Frame::Terminator
    ));
    assert_eq!(cursor.position(), buf.len() as u64);
}

/// Multiple entries each produce their own Data frame + delta, followed by a
/// single Terminator.
#[test]
#[cfg_attr(miri, ignore = "accesses extern static `missingFieldDictType`")]
fn multiple_entries_write_multiple_delta_frames() {
    let make_ii = |doc_id| {
        let mut ii = InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
        ii.add_record(&RSIndexResult::build_virt().doc_id(doc_id).build())
            .unwrap();
        Box::new(OpaqueInvertedIndex::DocIdsOnly(ii))
    };

    let mut dict = OwnedDict::create();
    add_entry(&mut dict, b"field_a", make_ii(1));
    add_entry(&mut dict, b"field_b", make_ii(2));
    let spec = make_spec(&dict);
    let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };

    let mut buf = Vec::new();
    collect_missing_docs(&mut buf, &*guard).unwrap();

    let mut cursor = Cursor::new(&buf);

    let Frame::Data(name1) = Frame::decode(&mut cursor).unwrap() else {
        panic!("expected first Data frame");
    };
    let _: GcScanDelta = rmp_serde::from_read(&mut cursor).unwrap();

    let Frame::Data(name2) = Frame::decode(&mut cursor).unwrap() else {
        panic!("expected second Data frame");
    };
    let _: GcScanDelta = rmp_serde::from_read(&mut cursor).unwrap();

    let mut names = [name1.into_inner().into_vec(), name2.into_inner().into_vec()];
    names.sort(); // Frames can come in any order because dict iteration order is undefined.

    assert_eq!(names, [b"field_a".to_vec(), b"field_b".to_vec()]);
    assert!(matches!(
        Frame::decode(&mut cursor).unwrap(),
        Frame::Terminator
    ));
    assert_eq!(cursor.position(), buf.len() as u64);
}

#[test]
fn receive_terminator_returns_none() {
    let mut buf = Vec::new();
    Frame::Terminator.encode(&mut buf).unwrap();
    let mut cursor = Cursor::new(&buf);
    assert!(matches!(receive_missing_docs(&mut cursor).unwrap(), None));
}

#[test]
fn receive_malformed_frame_returns_child_error() {
    let mut cursor = Cursor::new(b"garbage");
    assert!(matches!(
        receive_missing_docs(&mut cursor),
        Err(HandleError::ChildError)
    ));
}

#[test]
fn receive_data_frame_returns_field_name_and_delta() {
    let mut buf = Vec::new();
    Frame::data(b"age").encode(&mut buf).unwrap();
    GcScanDelta::empty_for_testing()
        .serialize(&mut rmp_serde::Serializer::new(&mut buf))
        .unwrap();

    let mut cursor = Cursor::new(&buf);
    let (field_name, _delta) = receive_missing_docs(&mut cursor).unwrap().unwrap();
    assert_eq!(&*field_name, b"age");
}

#[test]
fn apply_returns_err_when_field_not_found() {
    let dict = OwnedDict::<MissingFieldDictType>::create();
    let mut spec = make_spec(&dict);
    let delta = GcScanDelta::empty_for_testing();

    let mut write_guard = unsafe { IndexSpecWriteGuard::from_locked_mut(&mut spec) };
    assert!(matches!(
        apply_missing_docs(b"nonexistent", delta, &mut *write_guard),
        Err(HandleError::FieldNotFound)
    ));
}

/// Full child-to-parent roundtrip: when all docs are deleted the dict entry is removed.
#[test]
fn roundtrip_all_docs_deleted_removes_entry() {
    let mut ii = InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
    ii.add_record(&RSIndexResult::build_virt().doc_id(1).build())
        .unwrap();
    ii.add_record(&RSIndexResult::build_virt().doc_id(2).build())
        .unwrap();
    let ii = Box::new(OpaqueInvertedIndex::DocIdsOnly(ii));
    let ii_ptr = Box::into_raw(ii);

    let mut dict = OwnedDict::create();
    add_entry(&mut dict, b"age", Some(unsafe { &*ii_ptr }));
    let mut spec = make_spec(&dict);

    // Child side: collect.
    let mut buf = Vec::new();
    {
        let read_guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };
        collect_missing_docs(&mut buf, &*read_guard).unwrap();
    }

    // Parent side: receive.
    let mut cursor = Cursor::new(&buf);
    let (field_name, delta) = receive_missing_docs(&mut cursor).unwrap().unwrap();
    assert_eq!(&*field_name, b"age");

    // Parent side: apply.
    let mut write_guard = unsafe { IndexSpecWriteGuard::from_locked_mut(&mut spec) };
    let info = apply_missing_docs(&field_name, delta, &mut *write_guard).unwrap();

    assert!(
        with_hidden_string_ref(b"age", |key| write_guard
            .missing_field_dict_mut()
            .fetch_mut(key))
        .is_none()
    );
    assert!(info.bytes_freed > 0);
    assert!(info.entries_removed > 0);

    // The test dict (dictTypeHeapHiddenStrings) has no valDestructor, so the II
    // is not freed on RS_dictDelete — free it manually here.
    unsafe { drop(Box::from_raw(ii_ptr)) };
}

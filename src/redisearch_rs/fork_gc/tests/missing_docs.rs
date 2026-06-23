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
use fork_gc::{Frame, missing_docs::collect_missing_docs};
use hidden_string::HiddenStringRef;
use index_result::RSIndexResult;
use index_spec::IndexSpecReadGuard;
use inverted_index::opaque::InvertedIndex as OpaqueInvertedIndex;
use inverted_index::{GcScanDelta, InvertedIndex, doc_ids_only::DocIdsOnly};

// Provide Redis allocator shims so the C dict functions can allocate memory.
redis_mock::mock_or_stub_missing_redis_c_symbols!();

/// Add one entry to `dict` keyed by `field_name`, with value `ii`.
///
/// `MissingFieldDictType` uses `dictTypeHeapHiddenStrings`, which copies the
/// key via `keyDup`, so the temporary `HiddenString` can be freed immediately
/// after insertion.
fn add_entry(
    dict: &mut OwnedDict<MissingFieldDictType>,
    field_name: &[u8],
    ii: Option<&OpaqueInvertedIndex>,
) {
    // SAFETY: NewHiddenString copies `field_name`; the dict's keyDup copies
    // the HiddenString itself, so we free the original immediately after insert.
    let hs = unsafe { ffi::NewHiddenString(field_name.as_ptr().cast(), field_name.len(), false) };
    let hs_ref = unsafe { HiddenStringRef::from_raw(hs) };
    dict.insert(hs_ref, ii);
    unsafe { ffi::HiddenString_Free(hs, false) };
}

fn make_spec(dict: &OwnedDict<MissingFieldDictType>) -> ffi::IndexSpec {
    // SAFETY: zeroed IndexSpec is valid for read-only field access through the guard.
    let mut spec: ffi::IndexSpec = unsafe { mem::zeroed() };
    spec.missingFieldDict = dict.as_ptr();
    spec
}

/// When the dict has no entries, only a Terminator is written.
#[test]
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
}

/// Entries whose value is `None` (no inverted index) are skipped silently.
#[test]
fn null_value_entry_is_skipped() {
    let mut dict = OwnedDict::create();
    add_entry(&mut dict, b"no_index_field", None);
    let spec = make_spec(&dict);
    let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };

    let mut buf = Vec::new();
    collect_missing_docs(&mut buf, &*guard).unwrap();

    let mut cursor = Cursor::new(&buf);
    assert!(matches!(
        Frame::decode(&mut cursor).unwrap(),
        Frame::Terminator
    ));
}

/// An entry whose inverted index is empty produces no delta, so it is skipped.
#[test]
fn empty_inverted_index_is_skipped() {
    let ii = Box::new(OpaqueInvertedIndex::DocIdsOnly(
        InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly),
    ));
    let ii_ptr = Box::into_raw(ii);

    let mut dict = OwnedDict::create();
    add_entry(&mut dict, b"empty_field", Some(unsafe { &*ii_ptr }));
    let spec = make_spec(&dict);
    let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };

    let mut buf = Vec::new();
    collect_missing_docs(&mut buf, &*guard).unwrap();

    let mut cursor = Cursor::new(&buf);
    assert!(matches!(
        Frame::decode(&mut cursor).unwrap(),
        Frame::Terminator
    ));

    // SAFETY: dict does not free values; ii_ptr was created via Box::into_raw.
    unsafe { drop(Box::from_raw(ii_ptr)) };
}

/// When an entry has docs absent from the DocTable, `scan_gc` produces a delta.
/// The output is a Data frame carrying the field name, the serialised
/// [`GcScanDelta`], then a Terminator.
///
/// The spec's `DocTable` is zeroed, so `DocTable_Exists` returns `false` for
/// every doc ID — every recorded doc is treated as deleted.
#[test]
fn entry_with_deleted_docs_writes_delta_frame() {
    let mut ii = InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
    ii.add_record(&RSIndexResult::build_virt().doc_id(1).build())
        .unwrap();
    ii.add_record(&RSIndexResult::build_virt().doc_id(2).build())
        .unwrap();
    let ii = Box::new(OpaqueInvertedIndex::DocIdsOnly(ii));
    let ii_ptr = Box::into_raw(ii);

    let mut dict = OwnedDict::create();
    add_entry(&mut dict, b"age", Some(unsafe { &*ii_ptr }));
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

    // SAFETY: dict does not free values; ii_ptr was created via Box::into_raw.
    unsafe { drop(Box::from_raw(ii_ptr)) };
}

/// Multiple entries each produce their own Data frame + delta, followed by a
/// single Terminator.
#[test]
fn multiple_entries_write_multiple_delta_frames() {
    let make_ii = |doc_id| {
        let mut ii = InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
        ii.add_record(&RSIndexResult::build_virt().doc_id(doc_id).build())
            .unwrap();
        Box::into_raw(Box::new(OpaqueInvertedIndex::DocIdsOnly(ii)))
    };

    let ii_a = make_ii(1);
    let ii_b = make_ii(2);

    let mut dict = OwnedDict::create();
    add_entry(&mut dict, b"field_a", Some(unsafe { &*ii_a }));
    add_entry(&mut dict, b"field_b", Some(unsafe { &*ii_b }));
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
    names.sort();
    assert_eq!(names, [b"field_a".to_vec(), b"field_b".to_vec()]);
    assert!(matches!(
        Frame::decode(&mut cursor).unwrap(),
        Frame::Terminator
    ));

    // SAFETY: dict does not free values; both ii pointers were created via Box::into_raw.
    unsafe {
        drop(Box::from_raw(ii_a));
        drop(Box::from_raw(ii_b));
    }
}

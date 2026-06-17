/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{io::Cursor, mem, ptr};

use dict::OwnedDict;
use ffi::IndexFlags_Index_DocIdsOnly;
use fork_gc::{Frame, missing_docs::collect_missing_docs};
use index_result::RSIndexResult;
use index_spec::IndexSpecReadGuard;
use inverted_index::opaque::InvertedIndex as OpaqueInvertedIndex;
use inverted_index::{GcScanDelta, InvertedIndex, doc_ids_only::DocIdsOnly};
use serde::Serialize as _;

// Provide Redis allocator shims so the C dict functions can allocate memory.
redis_mock::mock_or_stub_missing_redis_c_symbols!();

// ── Test helpers ─────────────────────────────────────────────────────────────

/// Create an [`OwnedDict`] using `dictTypeHeapHiddenStrings` — the same key
/// type as a real `missingFieldDict`. The dict is released automatically when
/// it goes out of scope. Values are NOT freed on drop because
/// `dictTypeHeapHiddenStrings.valDestructor` is null.
fn make_dict() -> OwnedDict {
    // SAFETY: dictTypeHeapHiddenStrings is a valid static dictType.
    unsafe { OwnedDict::create(ptr::addr_of_mut!(ffi::dictTypeHeapHiddenStrings)) }
}

/// Add one entry to `dict` keyed by `field_name`, with value `ii`.
///
/// `dictTypeHeapHiddenStrings` duplicates the key internally (via `keyDup`),
/// so the temporary `HiddenString` is freed immediately after insertion.
fn add_entry(dict: &mut OwnedDict, field_name: &[u8], ii: *mut std::ffi::c_void) {
    // SAFETY: NewHiddenString copies `field_name` into a heap allocation; the
    // dict's keyDup makes its own copy, so we free the original immediately.
    let hs = unsafe { ffi::NewHiddenString(field_name.as_ptr().cast(), field_name.len(), false) };
    dict.insert(hs.cast(), ii);
    unsafe { ffi::HiddenString_Free(hs, false) };
}

fn make_spec(dict: &OwnedDict) -> ffi::IndexSpec {
    // SAFETY: zeroed IndexSpec is valid for read-only field access through the guard.
    let mut spec: ffi::IndexSpec = unsafe { mem::zeroed() };
    spec.missingFieldDict = dict.as_ptr();
    spec
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// When the dict has no entries, only a Terminator is written.
#[test]
fn empty_dict_writes_only_terminator() {
    let dict = make_dict();
    let spec = make_spec(&dict);
    // SAFETY: spec borrows dict, which lives for the duration of this test.
    let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };

    let mut buf = Vec::new();
    collect_missing_docs(&mut buf, &*guard).unwrap();

    let mut cursor = Cursor::new(&buf);
    assert!(matches!(
        Frame::decode(&mut cursor).unwrap(),
        Frame::Terminator
    ));
}

/// Entries whose value is null (no inverted index) are skipped silently.
#[test]
fn null_value_entry_is_skipped() {
    let mut dict = make_dict();
    add_entry(&mut dict, b"no_index_field", ptr::null_mut());
    let spec = make_spec(&dict);
    // SAFETY: spec borrows dict, which lives for the duration of this test.
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

    let mut dict = make_dict();
    add_entry(&mut dict, b"empty_field", ii_ptr.cast());
    let spec = make_spec(&dict);
    // SAFETY: spec borrows dict; ii_ptr is valid for the duration of this test.
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

    let mut dict = make_dict();
    add_entry(&mut dict, b"age", ii_ptr.cast());
    let spec = make_spec(&dict);
    // SAFETY: spec borrows dict; ii_ptr is valid for the duration of this test.
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

    let mut dict = make_dict();
    add_entry(&mut dict, b"field_a", ii_a.cast());
    add_entry(&mut dict, b"field_b", ii_b.cast());
    let spec = make_spec(&dict);
    // SAFETY: spec borrows dict; both ii pointers are valid for the duration of this test.
    let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };

    let mut buf = Vec::new();
    collect_missing_docs(&mut buf, &*guard).unwrap();

    let mut cursor = Cursor::new(&buf);
    let mut field_names_seen = Vec::new();
    loop {
        match Frame::decode(&mut cursor).unwrap() {
            Frame::Terminator => break,
            Frame::Data(name) => {
                field_names_seen.push(name.into_inner().into_vec());
                let _delta: GcScanDelta = rmp_serde::from_read(&mut cursor).unwrap();
            }
            Frame::Empty => panic!("unexpected Empty frame"),
        }
    }

    field_names_seen.sort();
    assert_eq!(field_names_seen, [b"field_a".to_vec(), b"field_b".to_vec()]);

    // SAFETY: dict does not free values; both ii pointers were created via Box::into_raw.
    unsafe {
        drop(Box::from_raw(ii_a));
        drop(Box::from_raw(ii_b));
    }
}

/// A roundtrip test: the wire output of `collect_missing_docs` can be decoded
/// frame-by-frame as the parent side would.
#[test]
fn roundtrip_protocol_is_decodable() {
    let mut ii = InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
    ii.add_record(&RSIndexResult::build_virt().doc_id(5).build())
        .unwrap();
    let ii = Box::new(OpaqueInvertedIndex::DocIdsOnly(ii));
    let ii_ptr = Box::into_raw(ii);

    let mut dict = make_dict();
    add_entry(&mut dict, b"title", ii_ptr.cast());
    let spec = make_spec(&dict);
    // SAFETY: spec borrows dict; ii_ptr is valid for the duration of this test.
    let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };

    let mut buf = Vec::new();
    collect_missing_docs(&mut buf, &*guard).unwrap();

    let mut cursor = Cursor::new(&buf);
    let mut entries_received = 0usize;
    loop {
        match Frame::decode(&mut cursor).unwrap() {
            Frame::Terminator => break,
            Frame::Data(name) => {
                assert_eq!(&*name, b"title");
                let delta: GcScanDelta = rmp_serde::from_read(&mut cursor).unwrap();
                assert_eq!(delta.last_block_idx(), 0);
                entries_received += 1;
            }
            Frame::Empty => panic!("unexpected Empty frame"),
        }
    }
    assert_eq!(entries_received, 1);

    // SAFETY: dict does not free values; ii_ptr was created via Box::into_raw.
    unsafe { drop(Box::from_raw(ii_ptr)) };
}

/// Verify the serialised bytes written after a Data frame are a valid
/// msgpack-encoded [`GcScanDelta`] that round-trips cleanly.
#[test]
fn delta_frame_serialisation_roundtrips_via_msgpack() {
    let mut ii = InvertedIndex::<DocIdsOnly>::new(IndexFlags_Index_DocIdsOnly);
    ii.add_record(&RSIndexResult::build_virt().doc_id(3).build())
        .unwrap();
    let ii = Box::new(OpaqueInvertedIndex::DocIdsOnly(ii));
    let ii_ptr = Box::into_raw(ii);

    let mut dict = make_dict();
    add_entry(&mut dict, b"score", ii_ptr.cast());
    let spec = make_spec(&dict);
    // SAFETY: spec borrows dict; ii_ptr is valid for the duration of this test.
    let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };

    let mut buf = Vec::new();
    collect_missing_docs(&mut buf, &*guard).unwrap();

    let mut cursor = Cursor::new(&buf);
    let Frame::Data(_) = Frame::decode(&mut cursor).unwrap() else {
        panic!("expected Data frame");
    };
    let delta: GcScanDelta = rmp_serde::from_read(&mut cursor).unwrap();

    let mut re_encoded = Vec::new();
    delta
        .serialize(&mut rmp_serde::Serializer::new(&mut re_encoded))
        .unwrap();

    let frame_end = cursor.position() as usize;
    let frame_start = size_of::<usize>() + b"score".len();
    assert_eq!(&buf[frame_start..frame_end], re_encoded.as_slice());

    // SAFETY: dict does not free values; ii_ptr was created via Box::into_raw.
    unsafe { drop(Box::from_raw(ii_ptr)) };
}

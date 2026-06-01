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
use fork_gc::{
    Frame,
    existing_docs::{
        HandleError, apply_existing_docs, collect_existing_docs, receive_existing_docs,
    },
};
use index_result::RSIndexResult;
use index_spec::{IndexSpecReadGuard, IndexSpecWriteGuard};
use inverted_index::opaque::InvertedIndex as OpaqueInvertedIndex;
use inverted_index::{GcScanDelta, InvertedIndex, doc_ids_only::DocIdsOnly};
use serde::Serialize as _;

// `collect_existing_docs` calls `spec.doc_exists()`, which calls `DocTable_Exists`
// from the C library. That symbol is unavailable in pure-Rust test binaries, so we
// provide a stub that always returns `false` (every doc treated as absent).
#[unsafe(no_mangle)]
unsafe extern "C" fn DocTable_Exists(_: *const ffi::DocTable, _: ffi::t_docId) -> bool {
    false
}

fn make_spec(existing_docs: *mut ffi::InvertedIndex) -> ffi::IndexSpec {
    // SAFETY: zeroed IndexSpec is valid for read-only access; existingDocs is
    // either null or a caller-managed pointer that outlives this struct.
    let mut spec: ffi::IndexSpec = unsafe { mem::zeroed() };
    spec.existingDocs = existing_docs;
    spec
}

/// When the spec has no `existingDocs` index, only a Terminator is written.
#[test]
fn no_existing_docs_writes_only_terminator() {
    let spec = make_spec(std::ptr::null_mut());
    // SAFETY: spec is valid and lives for the duration of the test.
    let guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };

    let mut buf = Vec::new();
    collect_existing_docs(&mut buf, &*guard).unwrap();

    let mut cursor = Cursor::new(&buf);
    assert!(matches!(
        Frame::decode(&mut cursor).unwrap(),
        Frame::Terminator
    ));
}

/// When `existingDocs` is present but empty, `scan_gc` returns no delta and
/// only a Terminator is written.
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
    collect_existing_docs(&mut buf, &*guard).unwrap();

    let mut cursor = Cursor::new(&buf);
    assert!(matches!(
        Frame::decode(&mut cursor).unwrap(),
        Frame::Terminator
    ));

    // SAFETY: ii_ptr was created via Box::into_raw above.
    unsafe { drop(Box::from_raw(ii_ptr)) };
}

/// When `existingDocs` has entries and all docs are absent from the DocTable
/// (stub returns false), `scan_gc` produces a delta. The output is an Empty
/// frame, the serialised [`GcScanDelta`], then a Terminator.
#[test]
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
    collect_existing_docs(&mut buf, &*guard).unwrap();

    let mut cursor = Cursor::new(&buf);
    assert!(matches!(Frame::decode(&mut cursor).unwrap(), Frame::Empty));
    let delta: GcScanDelta = rmp_serde::from_read(&mut cursor).unwrap();
    assert_eq!(delta.last_block_idx(), 0);
    assert!(matches!(
        Frame::decode(&mut cursor).unwrap(),
        Frame::Terminator
    ));

    // SAFETY: ii_ptr was created via Box::into_raw above.
    unsafe { drop(Box::from_raw(ii_ptr)) };
}

#[test]
fn receive_terminator_returns_none() {
    let mut buf = Vec::new();
    Frame::Terminator.encode(&mut buf).unwrap();
    let mut cursor = Cursor::new(&buf);
    assert!(matches!(receive_existing_docs(&mut cursor).unwrap(), None));
}

#[test]
fn receive_malformed_frame_returns_child_error() {
    let mut cursor = Cursor::new(b"garbage");
    assert!(matches!(
        receive_existing_docs(&mut cursor),
        Err(HandleError::ChildError)
    ));
}

#[test]
fn receive_valid_delta_returns_some() {
    let mut buf = Vec::new();
    Frame::Empty.encode(&mut buf).unwrap();
    GcScanDelta::empty_for_testing()
        .serialize(&mut rmp_serde::Serializer::new(&mut buf))
        .unwrap();

    let mut cursor = Cursor::new(&buf);
    assert!(matches!(
        receive_existing_docs(&mut cursor).unwrap(),
        Some(_)
    ));
}

#[test]
fn apply_returns_err_when_existing_docs_absent() {
    let delta = GcScanDelta::empty_for_testing();

    let mut apply_spec = make_spec(std::ptr::null_mut());
    let mut guard = unsafe { IndexSpecWriteGuard::from_locked_mut(&mut apply_spec) };
    assert!(matches!(
        apply_existing_docs(delta, &mut *guard),
        Err(HandleError::ExistingDocsDeleted)
    ));
}

#[test]
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
        let read_guard = unsafe { IndexSpecReadGuard::from_locked(&spec) };
        collect_existing_docs(&mut buf, &*read_guard).unwrap();
    }

    let mut cursor = Cursor::new(&buf);
    let delta = receive_existing_docs(&mut cursor).unwrap().unwrap();

    let mut write_guard = unsafe { IndexSpecWriteGuard::from_locked_mut(&mut spec) };
    let info = apply_existing_docs(delta, &mut *write_guard).unwrap();

    assert!(write_guard.existing_docs_mut().is_none());
    assert!(info.bytes_freed > 0);
}

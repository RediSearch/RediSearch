/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The claim the mode-erased handle rests on: a revalidation lookup minted by
//! projecting through the union keeps the provenance C owns, so it stays usable
//! after the index has been mutated through that same handle.
//!
//! These tests are only meaningful under `miri` — a plain run cannot observe an
//! invalid retag. Run them with:
//!
//! ```text
//! cargo +nightly miri nextest run --manifest-path src/redisearch_rs/Cargo.toml -p tag_index_ffi
//! ```

use ffi::{IteratorStatus_ITERATOR_OK, QueryIterator, ValidateStatus_VALIDATE_OK, t_docId};
use rqe_iterators_test_utils::MockContext;

use crate::handle::{CValues, index_and_commit, new_in_memory};

/// Read one posting, returning the document id, or `None` at EOF.
///
/// # Safety
///
/// `it` must be a live iterator from `Rust_TagIndex_OpenReader`.
unsafe fn read(it: *mut QueryIterator) -> Option<t_docId> {
    // SAFETY: the caller guarantees `it` is live, so its vtable is populated.
    let read_fn = unsafe { (*it).Read }.expect("Read is always set");
    // SAFETY: as above.
    if unsafe { read_fn(it) } != IteratorStatus_ITERATOR_OK {
        return None;
    }
    // SAFETY: a successful read updates `lastDocId`.
    Some(unsafe { (*it).lastDocId })
}

/// A reader survives the index being written through the same handle, and
/// re-resolves its tag on revalidation.
///
/// This is the whole point of projecting through the union with a raw place
/// expression: `Rust_TagIndex_Index` takes a `&mut` through the handle while the
/// iterator holds a lookup minted from it. Had the lookup been derived from a
/// reference to the handle — as an `enum` + `match` would force — that `&mut`
/// would revoke it, and the `Revalidate` below would be undefined behaviour.
#[test]
fn a_lookup_survives_a_write_through_the_same_handle() {
    const N: t_docId = 8;

    let mock = MockContext::new(N * 2, (N * 2) as usize);
    let idx = new_in_memory(false);
    for doc_id in 1..=N {
        index_and_commit(idx, &["hello"], doc_id);
    }

    // SAFETY: `idx` is live and is the owning pointer, exactly as C's
    // `tagOpts.tagIndex` is; `mock` outlives the iterator.
    let it = unsafe {
        tag_index_ffi::Rust_TagIndex_OpenReader(
            idx,
            mock.sctx().as_ptr(),
            c"hello".as_ptr(),
            5,
            1.0,
            std::ptr::null_mut(),
        )
    };
    assert!(!it.is_null(), "the tag has postings");

    // SAFETY: `it` is live.
    assert_eq!(unsafe { read(it) }, Some(1));

    // Mutate the index through the handle, which is where the `&mut` that would
    // revoke a badly-derived lookup is taken.
    //
    // It has to be a *different* tag: writing into the posting list this reader
    // is walking would invalidate the reader's own pointer into it, which the
    // revalidation protocol forbids independently of anything the handle does.
    index_and_commit(idx, &["world"], N + 1);

    // SAFETY: `it` is live and `spec` is the mock's, which outlives it.
    let spec = unsafe { (*mock.sctx().as_ptr()).spec };
    // SAFETY: `it` is live, so its vtable is populated.
    let revalidate = unsafe { (*it).Revalidate }.expect("Revalidate is always set");
    // SAFETY: `it` and `spec` are both live; this is the call that reads back
    // through the minted lookup.
    let status = unsafe { revalidate(it, spec) };
    assert_eq!(
        status, ValidateStatus_VALIDATE_OK,
        "the tag still resolves to the same posting list"
    );

    // The reader keeps going afterwards, so the lookup was not merely tolerated
    // but usable. Whether the document appended above becomes visible to an
    // already-open reader is not this test's concern — only that the reader is
    // still sound and still reading its own postings.
    let mut rest = Vec::new();
    // SAFETY: `it` is live.
    while let Some(doc_id) = unsafe { read(it) } {
        rest.push(doc_id);
    }
    assert_eq!(rest, (2..=N).collect::<Vec<_>>());

    // SAFETY: `it` is live and owned here.
    let free = unsafe { (*it).Free }.expect("Free is always set");
    // SAFETY: as above; nothing uses `it` afterwards.
    unsafe { free(it) };

    let mut slot = idx;
    // SAFETY: every iterator over the index is gone.
    unsafe { tag_index_ffi::Rust_TagIndex_Free(&raw mut slot) };
}

/// Opening a reader for a tag that was never indexed yields NULL rather than an
/// empty iterator, which is what `query.c` branches on.
#[test]
fn opening_an_absent_tag_yields_null() {
    let mock = MockContext::new(4, 4);
    let idx = new_in_memory(false);
    index_and_commit(idx, &["hello"], 1);

    // SAFETY: `idx` and `mock` are live.
    let it = unsafe {
        tag_index_ffi::Rust_TagIndex_OpenReader(
            idx,
            mock.sctx().as_ptr(),
            c"goodbye".as_ptr(),
            7,
            1.0,
            std::ptr::null_mut(),
        )
    };

    assert!(it.is_null());

    let mut slot = idx;
    // SAFETY: no iterator was created.
    unsafe { tag_index_ffi::Rust_TagIndex_Free(&raw mut slot) };
}

/// The empty tag `INDEXEMPTY` writes arrives as a NULL pointer with length 0,
/// which `slice::from_raw_parts` would reject.
#[test]
fn a_null_zero_length_tag_is_the_empty_tag() {
    let mock = MockContext::new(4, 4);
    let idx = new_in_memory(false);

    let values = CValues::new(&[""]);
    // SAFETY: `idx` is live and `values` holds one valid, empty C string.
    unsafe {
        tag_index_ffi::Rust_TagIndex_Index(
            idx,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            values.as_ptr(),
            values.len(),
            1,
            false,
        )
    };
    // SAFETY: as above.
    unsafe { tag_index_ffi::Rust_TagIndex_Commit(idx, values.as_ptr(), values.len()) };

    // SAFETY: `idx` and `mock` are live; a NULL `value` with `len` 0 is allowed.
    let it = unsafe {
        tag_index_ffi::Rust_TagIndex_OpenReader(
            idx,
            mock.sctx().as_ptr(),
            std::ptr::null(),
            0,
            1.0,
            std::ptr::null_mut(),
        )
    };
    assert!(!it.is_null(), "the empty tag was indexed, so it reads back");

    // SAFETY: `it` is live.
    assert_eq!(unsafe { read(it) }, Some(1));

    // SAFETY: `it` is live and owned here.
    let free = unsafe { (*it).Free }.expect("Free is always set");
    // SAFETY: as above.
    unsafe { free(it) };

    let mut slot = idx;
    // SAFETY: every iterator is gone.
    unsafe { tag_index_ffi::Rust_TagIndex_Free(&raw mut slot) };
}

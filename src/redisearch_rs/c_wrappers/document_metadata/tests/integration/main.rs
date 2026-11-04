/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use document_metadata::OwnedDocumentMetadata;
use std::{cell::Cell, ptr::NonNull, rc::Rc};

// The mock destructor recovers this allocation through its first member.
#[repr(C)]
struct TestMetadata {
    metadata: ffi::RSDocumentMetadata,
    free_calls: Rc<Cell<usize>>,
}

// Only this integration binary supplies DMD_Free. It tests the wrapper's ownership
// handoff, not the production C destructor's handling of metadata subfields.
/// # Safety
///
/// `ptr` must point to the first member of a live [`TestMetadata`] allocated with
/// [`Box::into_raw`], retaining that allocation's provenance. Call exactly once,
/// on the fixture's owning thread, after releasing its last metadata reference.
#[unsafe(export_name = "DMD_Free")]
unsafe extern "C" fn free_metadata(ptr: *const ffi::RSDocumentMetadata) {
    debug_assert!(!ptr.is_null());
    // SAFETY: every wrapper in this binary owns a reference to a Box<TestMetadata>.
    // Its first member has the same address and the allocation is reclaimed only
    // when the wrapper releases the last reference.
    let allocation = unsafe { Box::from_raw(ptr.cast_mut().cast::<TestMetadata>()) };
    allocation.free_calls.set(allocation.free_calls.get() + 1);
}

#[test]
fn dropping_metadata_frees_only_after_the_last_owner() {
    let free_calls = Rc::new(Cell::new(0));
    // SAFETY: the bindgen struct contains only scalar values and raw pointers,
    // for which zero is valid. This test never borrows its document subfields;
    // only the reference count and the mock destructor are exercised.
    let mut metadata: ffi::RSDocumentMetadata = unsafe { std::mem::zeroed() };
    metadata.ref_count = 1;
    let allocation = Box::new(TestMetadata {
        metadata,
        free_calls: Rc::clone(&free_calls),
    });
    let ptr = NonNull::new(Box::into_raw(allocation).cast::<ffi::RSDocumentMetadata>()).unwrap();
    // SAFETY: the pointer is aligned and initialized, and its single reference
    // is transferred to the wrapper. The mock destructor owns its final release.
    let first = unsafe { OwnedDocumentMetadata::from_raw(ptr) };
    let last = first.clone();
    // SAFETY: both owners keep the allocation alive; no other thread accesses it.
    assert_eq!(unsafe { (*ptr.as_ptr()).ref_count }, 2);

    drop(first);
    assert_eq!(free_calls.get(), 0);
    // SAFETY: the remaining owner keeps the allocation alive, with no concurrent access.
    assert_eq!(unsafe { (*ptr.as_ptr()).ref_count }, 1);

    drop(last);
    assert_eq!(free_calls.get(), 1);
    assert_eq!(Rc::strong_count(&free_calls), 1);
}

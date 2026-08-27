/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for [`ErasedTagIndex`], the mode-erased handle.
//!
//! What these pin down is the union bookkeeping: the discriminant survives
//! erasure, the projections reach the payload the discriminant advertises, and
//! `Drop` releases the live field. The *provenance* claim the erasure exists for —
//! that a projection outlives a write through the same handle — needs C's calling
//! pattern and `miri`, so it is covered by `tag_index_ffi`'s `provenance` tests.
//!
//! Run these under `miri` too: it is what turns the drop test from "did not crash"
//! into "dropped the live field, exactly once, and leaked nothing".

use std::ptr::NonNull;

use tag_index::{ErasedTagIndex, InMemoryMode, Mode, OnDiskMode, TagIndex};

use crate::util::{as_tag, index_mem};

/// A disk-mode index over a dangling spec pointer. Nothing here crosses into the
/// backend, so the pointer is never read — same placeholder as `disk`'s.
fn erased_on_disk() -> ErasedTagIndex {
    // SAFETY: no disk path is exercised, so the spec is never dereferenced.
    let index = unsafe { TagIndex::<OnDiskMode>::new(NonNull::dangling(), 0, false) };
    ErasedTagIndex::new_on_disk(index)
}

#[test]
fn mode_reports_the_constructing_mode() {
    let in_memory = ErasedTagIndex::new_in_memory(TagIndex::<InMemoryMode>::new(0, false));
    // SAFETY: the handle is live for the whole borrow.
    assert_eq!(unsafe { ErasedTagIndex::mode(&in_memory) }, Mode::InMemory);

    let on_disk = erased_on_disk();
    // SAFETY: as above.
    assert_eq!(unsafe { ErasedTagIndex::mode(&on_disk) }, Mode::OnDisk);
}

/// Erasure preserves the payload: the projection reaches the very index that was
/// erased, identified by the unique id it was created with.
#[test]
fn projection_reaches_the_erased_index() {
    let index = TagIndex::<InMemoryMode>::new(0, true);
    let id = index.id();

    let handle = ErasedTagIndex::new_in_memory(index);
    // SAFETY: the handle is live and its mode is `InMemory`.
    let index = unsafe { ErasedTagIndex::in_memory(&handle) };

    assert_eq!(index.id(), id);
    assert!(
        index.has_suffix(),
        "the erased index was created WITHSUFFIXTRIE"
    );
}

/// The mutable projection writes through to the erased index, and the shared one
/// then observes the write.
#[test]
fn the_erased_index_is_writable_through_the_handle() {
    let mut handle = ErasedTagIndex::new_in_memory(TagIndex::<InMemoryMode>::new(0, false));

    // SAFETY: the handle is live, its mode is `InMemory`, and no other borrow of the
    // payload exists across either call.
    let index = unsafe { ErasedTagIndex::in_memory_mut(&mut handle) };
    index_mem(index, &[b"hello"], 1);
    index.commit(&[as_tag(b"hello")]);

    // SAFETY: as above; the `&mut` above is dead.
    let index = unsafe { ErasedTagIndex::in_memory(&handle) };
    assert_eq!(index.n_tags(), 1);
}

/// Dropping the handle drops the live union field and only that one. Under `miri`
/// this catches both a leak (the live field never dropped) and the double-free a
/// mismatched discriminant would cause.
#[test]
fn drop_releases_the_live_field() {
    let mut handle = ErasedTagIndex::new_in_memory(TagIndex::<InMemoryMode>::new(0, true));
    // Give the in-memory field something to own, so a missed drop is a real leak
    // rather than a no-op on empty tries.
    // SAFETY: the handle is live and its mode is `InMemory`.
    let index = unsafe { ErasedTagIndex::in_memory_mut(&mut handle) };
    index_mem(index, &[b"hello", b"world"], 1);
    index.commit(&[as_tag(b"hello"), as_tag(b"world")]);
    drop(handle);

    drop(erased_on_disk());
}

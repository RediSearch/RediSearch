/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for the `TagIndex` constructors and for `TagIndex::open_index`, the
//! path creating per-tag posting lists.
//!
//! Ids come from a counter global to the process, whose starting point these
//! tests can't pin down, so they assert that ids *differ*, never what they are.

use std::ptr::NonNull;

use tag_index::{InMemoryMode, OnDiskMode, TagIndex};

/// Two indexes never share an id, so the fork GC can tell a recreated index from
/// the one its child scanned.
#[test]
fn each_index_gets_its_own_id() {
    let first = TagIndex::<InMemoryMode>::new(false);
    let second = TagIndex::<InMemoryMode>::new(false);

    assert_ne!(first.id(), second.id());
}

/// Both storage modes draw from the same counter, so an in-memory index and an
/// on-disk one can't collide either.
#[test]
fn ids_are_unique_across_storage_modes() {
    let in_memory = TagIndex::<InMemoryMode>::new(false);
    // SAFETY: this test only drives paths that never dereference the spec, so a
    // dangling pointer satisfies `new_on_disk` here (see the `disk` module docs).
    let on_disk = unsafe { TagIndex::<OnDiskMode>::new(NonNull::dangling(), 0, false) };

    assert_ne!(in_memory.id(), on_disk.id());
}

/// `with_suffix` toggles suffix support.
#[test]
fn suffix_support_follows_the_creation_flag() {
    let tag_index = TagIndex::<InMemoryMode>::new(false);
    assert!(!tag_index.has_suffix());

    let tag_index = TagIndex::<InMemoryMode>::new(true);
    assert!(tag_index.has_suffix());
}

#[test]
fn new_in_memory_means_memory_mode() {
    // Type checked
    let _: TagIndex<InMemoryMode> = TagIndex::<InMemoryMode>::new(false);
}

/// A newly created index holds no tags: lookups miss, iteration yields
/// nothing, and a read-only open does not create the posting list.
#[test]
fn new_index_holds_no_tags() {
    let mut tag_index = TagIndex::<InMemoryMode>::new(false);

    assert!(tag_index.find_value(b"missing").is_none());

    assert!(tag_index.open_index(b"missing", false).is_none());
    // The read-only open must not have registered the tag on the way.
    assert!(tag_index.find_value(b"missing").is_none());
}

/// `open_index` with `create_if_missing` registers an empty posting list on
/// the first call, and later calls return that same posting list instead of
/// replacing it.
#[test]
fn open_index_creates_the_posting_list_once() {
    let mut tag_index = TagIndex::<InMemoryMode>::new(false);

    let created: *const _ = tag_index
        .open_index(b"team", true)
        .expect("first open creates the posting list");

    let found = tag_index.find_value(b"team").expect("tag is registered");
    assert_eq!(found.unique_docs(), 0, "the posting list starts empty");
    assert!(std::ptr::eq(created, found));

    let reopened: *const _ = tag_index
        .open_index(b"team", true)
        .expect("tag is registered");
    assert!(
        std::ptr::eq(created, reopened),
        "an existing posting list must be returned, not replaced"
    );
}

#[test]
fn new_on_disk_means_disk_mode() {
    // Type checked
    // SAFETY: this test only drives paths that never dereference the spec, so a
    // dangling pointer satisfies `new_on_disk` here (see the `disk` module docs).
    let _: TagIndex<OnDiskMode> =
        unsafe { TagIndex::<OnDiskMode>::new(NonNull::dangling(), 0, false) };
}

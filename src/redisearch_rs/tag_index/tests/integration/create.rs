/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for the `TagIndex` constructors.

use std::ptr::NonNull;

use tag_index::{InMemoryMode, OnDiskMode, TagIndex};

/// A tag index reports the id it was created with.
#[test]
fn reports_the_creation_id() {
    let tag_index = TagIndex::<InMemoryMode>::new(1, false);
    assert_eq!(tag_index.id(), 1);
}

/// `with_suffix` toggles suffix support.
#[test]
fn suffix_support_follows_the_creation_flag() {
    let tag_index = TagIndex::<InMemoryMode>::new(1, false);
    assert!(!tag_index.has_suffix());

    let tag_index = TagIndex::<InMemoryMode>::new(1, true);
    assert!(tag_index.has_suffix());
}

#[test]
fn new_in_memory_means_memory_mode() {
    // Type checked
    let _: TagIndex<InMemoryMode> = TagIndex::<InMemoryMode>::new(1, false);
}

#[test]
fn new_on_disk_means_disk_mode() {
    // Type checked
    // SAFETY: this test only drives paths that never dereference the spec, so a
    // dangling pointer satisfies `new_on_disk` here (see the `disk` module docs).
    let _: TagIndex<OnDiskMode> =
        unsafe { TagIndex::<OnDiskMode>::new(1, NonNull::dangling(), 0, false) };
}

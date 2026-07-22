/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for the `TagIndex` constructors.
//!
//! Ids come from a counter global to the process, whose starting point these
//! tests can't pin down, so they assert that ids *differ*, never what they are.

use std::ptr::NonNull;

use tag_index::{InMemoryMode, OnDiskMode, TagIndex};

use crate::util::{commit_mem, index_mem};

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

/// A newly created index holds no tags: lookups miss.
#[test]
fn new_index_holds_no_tags() {
    let tag_index = TagIndex::<InMemoryMode>::new(false);

    assert!(tag_index.find_value(b"missing").is_none());
    assert!(
        tag_index.value_iter().advance().is_none(),
        "a new index yields no tags"
    );
}

/// `mem_usage` accounts for the suffix trie: an index built
/// `WITHSUFFIXTRIE` reports strictly more overhead than one without, once the
/// suffix trie has been populated. Asserted as a comparison so it does not depend
/// on the trie's absolute byte size, which varies with the target.
#[test]
fn mem_usage_accounts_for_the_suffix_trie() {
    let tags: &[&[u8]] = &[b"hello", b"world"];

    // A fresh index already reports its tries' stack footprint, so growth has to be
    // measured against that baseline rather than against zero.
    let empty = TagIndex::<InMemoryMode>::new(true).mem_usage();

    let mut with_suffix = TagIndex::<InMemoryMode>::new(true);
    index_mem(&mut with_suffix, tags, 1);
    commit_mem(&mut with_suffix, tags);
    assert!(
        with_suffix.mem_usage() > empty,
        "populating the tries must grow the reported overhead"
    );

    let mut without_suffix = TagIndex::<InMemoryMode>::new(false);
    index_mem(&mut without_suffix, tags, 1);
    commit_mem(&mut without_suffix, tags);

    assert!(
        with_suffix.mem_usage() > without_suffix.mem_usage(),
        "the suffix trie must add to the reported overhead"
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

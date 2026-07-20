/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for `TagIndex::new` (the port of the C `NewTagIndex`) and for
//! `TagIndex::open_index`, the path creating per-tag posting lists.

use std::ptr::NonNull;

use tag_index::TagIndex;

/// A tag index reports the id it was created with (C: `TagIndex_GetId`).
#[test]
fn reports_the_creation_id() {
    let tag_index = TagIndex::new(1, None, false);
    assert_eq!(tag_index.id(), 1);
}

/// `with_suffix` toggles suffix support (C: `TagIndex_HasSuffix`).
#[test]
fn suffix_support_follows_the_creation_flag() {
    let tag_index = TagIndex::new(1, None, false);
    assert!(!tag_index.has_suffix());

    let tag_index = TagIndex::new(1, None, true);
    assert!(tag_index.has_suffix());
}

/// Without a disk spec the index is in memory mode.
#[test]
fn no_disk_spec_means_memory_mode() {
    let tag_index = TagIndex::new(1, None, false);
    assert!(!tag_index.disk_mode());
}

/// A newly created index holds no tags: lookups miss, iteration yields
/// nothing, and a read-only open does not create the posting list.
#[test]
fn new_index_holds_no_tags() {
    let mut tag_index = TagIndex::new(1, None, false);

    assert!(tag_index.find_value(b"missing").is_none());
    assert!(
        tag_index.value_iter().advance().is_none(),
        "a new index yields no tags"
    );

    assert!(tag_index.open_index(b"missing", false).is_none());
    // The read-only open must not have registered the tag on the way.
    assert!(tag_index.find_value(b"missing").is_none());
}

/// `open_index` with `create_if_missing` registers an empty posting list on
/// the first call, and later calls return that same posting list instead of
/// replacing it.
#[test]
fn open_index_creates_the_posting_list_once() {
    let mut tag_index = TagIndex::new(1, None, false);

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

/// `commit` registers metadata (suffixes) but indexes no documents: on a
/// memory-mode index it leaves the values trie empty, so no posting list — and
/// hence no record — is created (C: `testCommitAndOverheadWithSuffix`, the
/// `numRecords == 0` assertion after a bare `Commit`).
#[test]
fn commit_indexes_no_documents() {
    let mut tag_index = TagIndex::new(1, None, true);
    // `commit` requires NUL-terminated tags for the suffix trie.
    tag_index.commit(&[b"hello\0", b"world\0"]);

    assert_eq!(tag_index.unique_values(), 0, "commit creates no postings");
    assert!(tag_index.find_value(b"hello\0").is_none());
}

/// `get_overhead` accounts for the suffix trie: an index built
/// `WITHSUFFIXTRIE` reports strictly more overhead than one without, once the
/// suffix trie has been populated (C: `testCommitAndOverheadWithSuffix`, the
/// `GetOverhead > 0` assertion — expressed here as a comparison so it does not
/// depend on the trie's absolute byte size).
#[test]
fn get_overhead_accounts_for_the_suffix_trie() {
    let tags: &[&[u8]] = &[b"hello\0", b"world\0"];

    let mut with_suffix = TagIndex::new(1, None, true);
    with_suffix.commit(tags);
    assert!(
        with_suffix.get_overhead() > 0,
        "a populated suffix trie contributes overhead"
    );

    let mut without_suffix = TagIndex::new(1, None, false);
    without_suffix.commit(tags);

    assert!(
        with_suffix.get_overhead() > without_suffix.get_overhead(),
        "the suffix trie must add to the reported overhead"
    );
}

/// A disk spec selects the disk-backed mode, which is still a stub: the
/// memory-mode accessors abort with `unimplemented!` instead of silently
/// operating on in-memory postings that don't exist.
#[test]
#[should_panic(expected = "not implemented")]
fn disk_spec_selects_the_stubbed_disk_mode() {
    // The spec pointer is only stored, never dereferenced — no disk code path
    // is implemented yet — so a dangling pointer is enough for the test.
    let tag_index = TagIndex::new(1, Some((NonNull::dangling(), 0)), false);

    let _ = tag_index.find_value(b"team");
}

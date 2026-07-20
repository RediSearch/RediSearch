/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for indexing documents under tags (`TagIndex::index`) and iterating
//! the indexed tags.

use std::ptr::null;

use tag_index::{TagIndex, ValueIterator};

/// Drain a [`ValueIterator`] into its yielded keys, in iteration order.
fn value_iter_keys(mut it: ValueIterator<'_>) -> Vec<Vec<u8>> {
    let mut keys: Vec<Vec<u8>> = Vec::new();
    while let Some((key, _)) = it.advance() {
        keys.push(key.to_vec());
    }
    keys
}

/// Indexing a document registers each of its tags.
#[test]
fn indexing_registers_every_tag() {
    let mut tag_index = TagIndex::new(1, None, false);

    let tags: &[&[u8]] = &[b"tag-1", b"tag-2"];

    tag_index.index(null(), null(), tags, 1);

    let values = value_iter_keys(tag_index.value_iter());

    assert_eq!(tags, values.as_slice());
}

/// Tags are yielded in lexicographical order, whatever the insertion order.
#[test]
fn iterate_values_is_lexicographically_ordered() {
    let mut tag_index = TagIndex::new(1, None, false);

    let tags: &mut [&[u8]] = &mut [b"z", b"r", b"t", b"d", b"m", b"a"];

    tag_index.index(null(), null(), tags, 1);

    let values = value_iter_keys(tag_index.value_iter());

    tags.sort();
    assert_eq!(tags, values.as_slice());
}

/// `value_iter` yields `(tag, inverted index)` entries in lexicographical tag
/// order, and each yielded index is the one stored in the trie.
#[test]
fn iter_values_yields_the_stored_entries_in_order() {
    let mut tag_index = TagIndex::new(1, None, false);

    let tags: &mut [&[u8]] = &mut [b"z", b"r", b"t", b"d", b"m", b"a"];

    tag_index.index(null(), null(), tags, 1);

    tags.sort();
    let mut iter = tag_index.value_iter();
    let mut seen = 0;
    while let Some((tag, ii)) = iter.advance() {
        assert_eq!(
            tag, tags[seen],
            "entries should be yielded in lexicographical tag order"
        );
        let ii = ii.expect("memory-mode iteration yields the stored inverted index");
        let found = tag_index.find_value(tag).expect("yielded tag is indexed");
        assert!(
            std::ptr::eq(ii, found),
            "the yielded reference should be the inverted index stored in the trie"
        );
        seen += 1;
    }
    assert_eq!(seen, tags.len());
}

/// An empty index yields no entries.
#[test]
fn iter_values_on_empty_index_yields_nothing() {
    let tag_index = TagIndex::new(1, None, false);
    assert!(tag_index.value_iter().advance().is_none());
}

/// Re-indexing the same document under the same tags is a no-op: the second
/// write adds no records, allocates no blocks and grows the index by zero
/// bytes (C: `testCreate`, the repeated-push idempotency check).
#[test]
fn reindexing_the_same_document_is_a_no_op() {
    let mut tag_index = TagIndex::new(1, None, false);
    let tags: &[&[u8]] = &[b"hello", b"world", b"foo"];

    let first = tag_index
        .index(null(), null(), tags, 1)
        .expect("memory-mode indexing is infallible");
    assert!(first.size_delta > 0, "the first insert allocates postings");
    assert_eq!(first.num_records, tags.len() as u32);

    let second = tag_index
        .index(null(), null(), tags, 1)
        .expect("memory-mode indexing is infallible");
    assert_eq!(second.size_delta, 0, "re-indexing must not grow the index");
    assert_eq!(second.num_records, 0, "no new records for a duplicate doc");
    assert_eq!(second.blocks_added, 0, "no new blocks for a duplicate doc");
}

/// Indexing N documents over a fixed tag set leaves one posting list per
/// distinct tag and accumulates one record per (tag, doc) pair (C: `testCreate`,
/// the `NUniqueValues` / `numRecords` assertions). A few hundred docs is enough
/// to cross block boundaries without the 100k scale the C test benchmarked.
#[test]
fn unique_values_and_record_count_track_the_writes() {
    const N: u64 = 500;

    let mut tag_index = TagIndex::new(1, None, false);
    let tags: &[&[u8]] = &[b"hello", b"world", b"foo"];

    let mut total_records = 0u32;
    for doc_id in 1..=N {
        total_records += tag_index
            .index(null(), null(), tags, doc_id)
            .expect("memory-mode indexing is infallible")
            .num_records;
    }

    assert_eq!(tag_index.unique_values(), tags.len());
    assert_eq!(total_records, N as u32 * tags.len() as u32);
}

/// A tag value repeated within a single document is counted once (C:
/// `testDuplicateTagValuesCountOnce`): `["foo", "foo", "bar"]` yields two
/// records and two unique values.
#[test]
fn intra_document_duplicate_tag_counted_once() {
    let mut tag_index = TagIndex::new(1, None, false);
    let tags: &[&[u8]] = &[b"foo", b"foo", b"bar"];

    let delta = tag_index
        .index(null(), null(), tags, 1)
        .expect("memory-mode indexing is infallible");
    tag_index.commit(tags);

    assert_eq!(delta.num_records, 2, "the duplicate `foo` is counted once");
    assert_eq!(tag_index.unique_values(), 2);
}

/// The accumulated `WritePostingsDelta` accounting matches the crate's own
/// memory model — the sum of every `size_delta` equals the memory the per-tag
/// inverted indexes report, and blocks accumulate (one per tag on the first
/// write). This is the portable form of `testCreate`'s memory assertions, which
/// hardcoded C `InvertedIndex` byte constants that do not describe the Rust
/// `InvertedIndex<DocIdsOnly>` layout.
#[test]
fn size_and_block_accounting_matches_reported_memory() {
    // A `DocIdsOnly` block holds up to 1000 entries, so index past that to make
    // each tag's posting list spill into more than one block.
    const N: u64 = 2500;

    let mut tag_index = TagIndex::new(1, None, false);
    let tags: &[&[u8]] = &[b"hello", b"world", b"foo"];

    let first = tag_index
        .index(null(), null(), tags, 1)
        .expect("memory-mode indexing is infallible");
    assert!(
        first.size_delta > 0,
        "the first insert allocates the index and its first block"
    );
    assert_eq!(
        first.blocks_added,
        tags.len() as u32,
        "each new tag gets exactly one block on its first document"
    );

    let mut total_size = first.size_delta;
    let mut total_blocks = first.blocks_added;
    for doc_id in 2..=N {
        let delta = tag_index
            .index(null(), null(), tags, doc_id)
            .expect("memory-mode indexing is infallible");
        total_size += delta.size_delta;
        total_blocks += delta.blocks_added;
    }

    let reported: usize = tags
        .iter()
        .map(|tag| {
            tag_index
                .find_value(tag)
                .expect("tag is indexed")
                .memory_usage()
        })
        .sum();
    assert_eq!(
        total_size, reported,
        "accumulated size_delta must equal the reported inverted-index memory"
    );
    assert!(
        total_blocks > tags.len() as u32,
        "blocks accumulate beyond the first per-tag block as postings fill up"
    );
}

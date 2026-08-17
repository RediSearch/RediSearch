/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for indexing documents under tags (`TagIndex::index`).

use index_result::RSIndexResult;
use inverted_index::IndexReader;
use tag_index::{InMemoryMode, TagIndex, Tag};

use crate::util::{commit_mem, index_mem};

/// Indexing a document registers each of its tags.
#[test]
fn indexing_registers_every_tag() {
    let mut tag_index = TagIndex::<InMemoryMode>::new(false);

    let tags: &[&[u8]] = &[b"tag-1", b"tag-2"];

    index_mem(&mut tag_index, tags, 1);

    for tag in tags {
        assert!(tag_index.find_value(tag).is_some());
    }
}

#[test]
fn field_expiration_flag_round_trips() {
    let mut tag_index = TagIndex::<InMemoryMode>::new(false);
    let tags = &[Tag::new(b"team").unwrap()];

    // Doc 1 has no TTL on this field; doc 2 does.
    for (doc_id, has_field_expiration) in [(1, false), (2, true)] {
        tag_index.index(tags, doc_id, has_field_expiration);
    }

    let ii = tag_index.find_value(b"team").expect("tag was indexed");
    let mut reader = ii.reader();
    let mut record = RSIndexResult::build_virt().doc_id(0).build();

    let mut seen = Vec::new();
    while reader
        .next_record(&mut record)
        .expect("read must not error")
    {
        seen.push((record.doc_id, record.has_field_expiration));
    }

    assert_eq!(seen, [(1, false), (2, true)]);
}

/// Re-indexing the same document under the same tags is a no-op: the second
/// write adds no records, allocates no blocks and grows the index by zero
/// bytes.
#[test]
fn reindexing_the_same_document_is_a_no_op() {
    let mut tag_index = TagIndex::<InMemoryMode>::new(false);
    let tags: &[&[u8]] = &[b"hello", b"world", b"foo"];

    let first = index_mem(&mut tag_index, tags, 1);
    assert!(first.size_delta > 0, "the first insert allocates postings");
    assert_eq!(first.num_records, tags.len() as u32);

    let second = index_mem(&mut tag_index, tags, 1);
    assert_eq!(second.size_delta, 0, "re-indexing must not grow the index");
    assert_eq!(second.num_records, 0, "no new records for a duplicate doc");
    assert_eq!(second.blocks_added, 0, "no new blocks for a duplicate doc");
}

/// Indexing N documents over a fixed tag set leaves one posting list per
/// distinct tag and accumulates one record per (tag, doc) pair.
#[test]
fn n_tags_and_record_count_track_the_writes() {
    // Both counts are linear in N, so the assertions hold at any size; a few hundred
    // documents just exercises the accumulation over many writes.
    #[cfg(not(miri))]
    const N: u64 = 500;
    // Miri interprets every write, and 500 of them exceed `nextest`'s slow-test budget.
    #[cfg(miri)]
    const N: u64 = 20;

    let mut tag_index = TagIndex::<InMemoryMode>::new(false);
    let tags: &[&[u8]] = &[b"hello", b"world", b"foo"];

    let mut total_records = 0u32;
    for doc_id in 1..=N {
        total_records += index_mem(&mut tag_index, tags, doc_id).num_records;
    }

    assert_eq!(tag_index.n_tags(), tags.len());
    assert_eq!(total_records, N as u32 * tags.len() as u32);
}

/// A tag value repeated within a single document is counted once:
/// `["foo", "foo", "bar"]` yields two records and two unique values.
#[test]
fn intra_document_duplicate_tag_counted_once() {
    let mut tag_index = TagIndex::<InMemoryMode>::new(false);
    let tags: &[&[u8]] = &[b"foo", b"foo", b"bar"];

    let delta = index_mem(&mut tag_index, tags, 1);
    commit_mem(&mut tag_index, tags);

    assert_eq!(delta.num_records, 2, "the duplicate `foo` is counted once");
    assert_eq!(tag_index.n_tags(), 2);
}

/// The accumulated `WritePostingsDelta` accounting matches the crate's own
/// memory model — the sum of every `size_delta` equals the memory the per-tag
/// inverted indexes report, and blocks accumulate (one per tag on the first
/// write). Asserted against the reported memory rather than absolute byte
/// constants, which would pin the `InvertedIndex<DocIdsOnly>` layout.
// Ignored under Miri: the block accounting is only interesting past
// `DocIdsOnly::RECOMMENDED_BLOCK_ENTRIES`, and interpreting that many writes
// exceeds `nextest`'s slow-test budget.
#[test]
#[cfg_attr(miri, ignore)]
fn size_and_block_accounting_matches_reported_memory() {
    // A `DocIdsOnly` block holds up to 1000 entries, so index past that to make
    // each tag's posting list spill into more than one block.
    const N: u64 = 2500;

    let mut tag_index = TagIndex::<InMemoryMode>::new(false);
    let tags: &[&[u8]] = &[b"hello", b"world", b"foo"];

    let first = index_mem(&mut tag_index, tags, 1);
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
        let delta = index_mem(&mut tag_index, tags, doc_id);
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

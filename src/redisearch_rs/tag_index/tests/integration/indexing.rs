/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for indexing documents under tags (`TagIndex::index`) and
//! iterating the indexed tags.

use index_result::RSIndexResult;
use tag_index::{InMemoryMode, SuffixQuery, Tag, TagIndex, TagValueReader};

use crate::util::{commit_mem, index_mem, value_iter_keys};

/// Indexing a document registers each of its tags.
#[test]
fn indexing_registers_every_tag() {
    let mut tag_index = TagIndex::<InMemoryMode>::new(false, 0);

    let tags: &[&[u8]] = &[b"tag-1", b"tag-2"];

    index_mem(&mut tag_index, tags, 1);

    let values = value_iter_keys(tag_index.value_iter());

    assert_eq!(tags, values.as_slice());
}

/// A document write drives `index` and `commit` from the same tag buffers, so
/// both must key on those bytes verbatim: the tag stays resolvable afterwards and
/// the values trie, the suffix trie and iteration all agree on the key.
///
/// Nothing else exercises the two phases together, which is how a mismatch
/// between them could go unnoticed.
#[test]
fn index_and_commit_agree_on_the_key() {
    let mut tag_index = TagIndex::<InMemoryMode>::new(true, 0);
    let tags: &[&[u8]] = &[b"foo"];

    index_mem(&mut tag_index, tags, 1);
    commit_mem(&mut tag_index, tags);

    assert!(
        tag_index.find_value(b"foo").is_some(),
        "the indexed tag must still resolve under the bytes it was written with"
    );
    assert_eq!(value_iter_keys(tag_index.value_iter()), [b"foo".to_vec()]);
    assert!(
        tag_index
            .suffix_expand(SuffixQuery::Suffix(Tag::new(b"oo").unwrap()), None)
            .next()
            .is_some(),
        "the suffix trie must resolve the same tag through one of its suffixes"
    );
}

/// `TagValueReader` walks a tag's postings in ascending document order and
/// reports the end of the list, staying there once reached.
// Crossing a block boundary needs more documents than
// `DocIdsOnly::RECOMMENDED_BLOCK_ENTRIES`, which is a fixed trait constant — there is no
// smaller corpus that keeps the property, and interpreting that many writes exceeds
// `nextest`'s slow-test budget. `inverted_index`'s own Miri tests cover the multi-block
// path by overriding the block capacity instead.
#[test]
#[cfg_attr(miri, ignore)]
fn tag_value_reader_reads_every_posting_in_order() {
    // A `DocIdsOnly` block holds up to 1000 entries, so index past that to make
    // the reader cross a block boundary.
    const N: u64 = 1500;

    let mut tag_index = TagIndex::<InMemoryMode>::new(false, 0);
    for doc_id in 1..=N {
        index_mem(&mut tag_index, &[b"team"], doc_id);
    }

    let ii = tag_index.find_value(b"team").expect("tag was indexed");
    let mut reader = TagValueReader::new(ii);
    let mut record = RSIndexResult::build_virt().doc_id(0).build();

    let mut doc_ids = Vec::new();
    while reader
        .next_record(&mut record)
        .expect("postings written by `index` decode cleanly")
    {
        doc_ids.push(record.doc_id);
    }

    assert_eq!(doc_ids, (1..=N).collect::<Vec<_>>());
    assert!(
        !reader
            .next_record(&mut record)
            .expect("postings written by `index` decode cleanly"),
        "a reader at the end of the postings stays there"
    );
}

/// `commit` forwards tags to the suffix index when suffix indexing is
/// enabled: every tag and every one of its suffixes becomes a lookup key,
/// including when a tag is itself a suffix of another indexed tag (`"oo"` is
/// a suffix of `"foo"` and also indexed as a tag on its own).
#[test]
fn commit_registers_tags_in_the_suffix_index_when_enabled() {
    let mut tag_index = TagIndex::<InMemoryMode>::new(true, 0);
    let tags: &[&[u8]] = &[b"foo", b"oo"];

    index_mem(&mut tag_index, tags, 1);
    commit_mem(&mut tag_index, tags);

    for key in [b"foo".as_slice(), b"oo", b"o"] {
        assert!(
            tag_index.suffix_contains(key),
            "`{}` must be registered in the suffix index",
            String::from_utf8_lossy(key)
        );
    }
    assert!(
        !tag_index.suffix_contains(b"bar"),
        "an unrelated key must not be registered"
    );
}

/// With suffix indexing disabled, `commit` is a no-op on the suffix index.
#[test]
fn commit_does_not_touch_the_suffix_index_when_disabled() {
    let mut tag_index = TagIndex::<InMemoryMode>::new(false, 0);
    let tags: &[&[u8]] = &[b"foo"];

    index_mem(&mut tag_index, tags, 1);
    commit_mem(&mut tag_index, tags);

    assert!(!tag_index.suffix_contains(b"foo"));
}

#[test]
fn field_expiration_flag_round_trips() {
    let mut tag_index = TagIndex::<InMemoryMode>::new(false, 0);
    let tags = &[Tag::new(b"team").unwrap()];

    // Doc 1 has no TTL on this field; doc 2 does.
    for (doc_id, has_field_expiration) in [(1, false), (2, true)] {
        tag_index.index(tags, doc_id, has_field_expiration);
    }

    let ii = tag_index.find_value(b"team").expect("tag was indexed");
    let mut reader = TagValueReader::new(ii);
    let mut record = RSIndexResult::build_virt().doc_id(0).build();

    let mut seen = Vec::new();
    while reader
        .next_record(&mut record)
        .expect("postings written by `index` decode cleanly")
    {
        seen.push((record.doc_id, record.has_field_expiration));
    }

    assert_eq!(seen, [(1, false), (2, true)]);
}

/// Tags are yielded in lexicographical order, whatever the insertion order.
#[test]
fn iterate_values_is_lexicographically_ordered() {
    let mut tag_index = TagIndex::<InMemoryMode>::new(false, 0);

    let tags: &mut [&[u8]] = &mut [b"z", b"r", b"t", b"d", b"m", b"a"];

    index_mem(&mut tag_index, tags, 1);

    let values = value_iter_keys(tag_index.value_iter());

    tags.sort();
    assert_eq!(tags, values.as_slice());
}

/// `value_iter` yields `(tag, inverted index)` entries in lexicographical tag
/// order, and each yielded index is the one stored in the trie.
#[test]
fn iter_values_yields_the_stored_entries_in_order() {
    let mut tag_index = TagIndex::<InMemoryMode>::new(false, 0);

    let tags: &mut [&[u8]] = &mut [b"z", b"r", b"t", b"d", b"m", b"a"];

    index_mem(&mut tag_index, tags, 1);

    tags.sort();
    let mut iter = tag_index.value_iter();
    let mut seen = 0;
    while let Some((tag, ii)) = iter.advance() {
        assert_eq!(
            tag.as_bytes(),
            tags[seen],
            "entries should be yielded in lexicographical tag order"
        );
        let found = tag_index
            .find_value(tag.as_bytes())
            .expect("yielded tag is indexed");
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
    let tag_index = TagIndex::<InMemoryMode>::new(false, 0);
    assert!(tag_index.value_iter().advance().is_none());
}

/// Re-indexing the same document under the same tags is a no-op: the second
/// write adds no records, allocates no blocks and grows the index by zero
/// bytes.
#[test]
fn reindexing_the_same_document_is_a_no_op() {
    let mut tag_index = TagIndex::<InMemoryMode>::new(false, 0);
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

    let mut tag_index = TagIndex::<InMemoryMode>::new(false, 0);
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
    let mut tag_index = TagIndex::<InMemoryMode>::new(false, 0);
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
#[test]
#[cfg_attr(miri, ignore)]
fn size_and_block_accounting_matches_reported_memory() {
    // A `DocIdsOnly` block holds up to 1000 entries, so index past that to make
    // each tag's posting list spill into more than one block.
    const N: u64 = 2500;

    let mut tag_index = TagIndex::<InMemoryMode>::new(false, 0);
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

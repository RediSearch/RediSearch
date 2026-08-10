/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for `TagIndex::gc`, the parent-process half of a fork-GC cycle.
//!
//! The child scans a tag's postings against the doc table and ships a
//! `GcScanDelta`; the parent applies it. These tests cover what is specific to
//! the *tag* index: the identity check that rejects a stale delta, dropping a tag
//! that lost its last document from both the values trie and the suffix trie, and
//! folding the discarded posting list into the reported totals.
//!
//! The block-level repair mechanics the delta describes — deleting a whole block,
//! repairing one partially, and ignoring a last block that changed since the scan
//! — belong to `InvertedIndex::apply_gc` and are covered by `inverted_index`'s own
//! `tests/gc.rs`. They are not repeated here.

use index_result::RSIndexResult;
use inverted_index::{DocId, GcScanDelta, InvertedIndex, RepairContext, doc_ids_only::DocIdsOnly};
use tag_index::TagIndex;

use crate::util::index_mem;

/// Build a memory-mode index holding `tags`, each carrying documents `1..=n`.
/// `with_suffix` mirrors `WITHSUFFIXTRIE`, and commits the tags so the suffix
/// trie is populated too.
fn indexed(tags: &[&[u8]], n: DocId, with_suffix: bool) -> TagIndex {
    let mut idx = TagIndex::new(1, None, with_suffix);
    for doc_id in 1..=n {
        index_mem(&mut idx, tags, doc_id);
    }
    if with_suffix {
        idx.commit(tags);
    }
    idx
}

/// The heap address of `tag`'s posting list — what the GC child ships back and
/// [`TagIndex::gc`] checks the delta against.
fn value_ptr(idx: &TagIndex, tag: &[u8]) -> *const InvertedIndex<DocIdsOnly> {
    idx.find_value(tag).expect("tag is indexed")
}

/// Scan `tag`'s postings the way the GC child does, keeping only the documents
/// `doc_exists` accepts. `None` when nothing needs repairing.
fn scan(idx: &TagIndex, tag: &[u8], doc_exists: impl Fn(DocId) -> bool) -> Option<GcScanDelta> {
    idx.find_value(tag)
        .expect("tag is indexed")
        .scan_gc(
            doc_exists,
            None::<for<'i> fn(&RSIndexResult<'i>, &RepairContext<'i>)>,
        )
        .expect("scanning a tag's postings should not fail")
}

/// Collect the keys currently in the suffix trie.
fn suffix_keys(idx: &TagIndex) -> Vec<Vec<u8>> {
    let mut it = idx
        .suffix_value_iter()
        .expect("index was created with a suffix trie");
    let mut keys = Vec::new();
    while let Some((key, _)) = it.advance() {
        keys.push(key.to_vec());
    }
    keys
}

/// Applying a delta drops the deleted documents and reports how many entries
/// went, leaving the tag in place because documents remain.
#[test]
fn gc_removes_deleted_documents() {
    let mut idx = indexed(&[b"team"], 5, false);

    // Documents 2 and 4 are gone from the doc table.
    let delta = scan(&idx, b"team", |doc_id| doc_id != 2 && doc_id != 4)
        .expect("two deleted documents need repairing");
    let ptr = value_ptr(&idx, b"team");

    let info = idx.gc(b"team", ptr, delta).expect("the delta is current");

    assert_eq!(info.entries_removed, 2);
    assert_eq!(
        idx.find_value(b"team")
            .expect("the tag still has documents")
            .unique_docs(),
        3
    );
}

/// When the last document goes, the tag is dropped from the values trie *and*
/// from the suffix trie, and the whole discarded posting list is folded into the
/// reported totals.
#[test]
fn gc_drops_a_tag_that_lost_every_document() {
    let mut idx = indexed(&[b"team"], 3, true);
    assert_eq!(
        suffix_keys(&idx),
        [
            b"am".to_vec(),
            b"eam".to_vec(),
            b"m".to_vec(),
            b"team".to_vec()
        ],
        "the tag and each of its suffixes are indexed"
    );

    let delta = scan(&idx, b"team", |_| false).expect("every document is gone");
    let ptr = value_ptr(&idx, b"team");
    let memory_before = idx
        .find_value(b"team")
        .expect("tag is indexed")
        .memory_usage();

    let info = idx.gc(b"team", ptr, delta).expect("the delta is current");

    assert!(idx.find_value(b"team").is_none(), "the tag is dropped");
    assert_eq!(idx.unique_values(), 0);
    assert!(
        suffix_keys(&idx).is_empty(),
        "the suffix trie drops the tag and all of its suffixes"
    );
    assert!(
        info.bytes_freed >= memory_before,
        "the whole posting list must be accounted as freed, not just the repaired blocks"
    );
    assert!(
        info.block_count_delta < 0,
        "the dropped list's blocks must be subtracted from the spec's block count"
    );
}

/// The empty tag (INDEXEMPTY) never entered the suffix trie, so dropping it must
/// not try to delete it from there — `TagSuffixIndex::delete` would assert.
#[test]
fn gc_drops_the_empty_tag_without_touching_the_suffix_trie() {
    let mut idx = indexed(&[b""], 2, true);
    assert!(
        suffix_keys(&idx).is_empty(),
        "the empty tag is never indexed in the suffix trie"
    );

    let delta = scan(&idx, b"", |_| false).expect("every document is gone");
    let ptr = value_ptr(&idx, b"");

    idx.gc(b"", ptr, delta).expect("the delta is current");

    assert!(idx.find_value(b"").is_none(), "the empty tag is dropped");
}

/// A delta scanned against a different posting list than the tag currently holds
/// is stale: the GC ran while the tag was being rewritten, so applying it would
/// corrupt the live list. Port of the identity check C did with
/// `idx != value` (`testDeleteDuringGCCleanup`).
#[test]
fn gc_rejects_a_delta_scanned_against_another_index() {
    let mut idx = indexed(&[b"team", b"other"], 3, false);

    let delta = scan(&idx, b"team", |doc_id| doc_id != 2).expect("one document is gone");
    // Stand in for the tag's index having been replaced since the scan.
    let other = value_ptr(&idx, b"other");

    assert!(
        idx.gc(b"team", other, delta).is_none(),
        "a delta that does not match the tag's current index must not be applied"
    );
    assert_eq!(
        idx.find_value(b"team")
            .expect("the tag is untouched")
            .unique_docs(),
        3,
        "rejecting the delta must leave the postings alone"
    );
}

/// A tag the GC removed in an earlier pass is simply not there any more, so its
/// delta is dropped rather than resurrecting the tag.
#[test]
fn gc_rejects_a_delta_for_a_tag_that_is_gone() {
    let mut idx = indexed(&[b"team"], 3, false);

    let delta = scan(&idx, b"team", |doc_id| doc_id != 2).expect("one document is gone");
    let ptr = value_ptr(&idx, b"team");
    idx.delete_tag_value(b"team");

    assert!(idx.gc(b"team", ptr, delta).is_none());
    assert_eq!(idx.unique_values(), 0);
}

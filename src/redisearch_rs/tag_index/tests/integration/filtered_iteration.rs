/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for the public filtered-iteration API (`value_iter_filtered` with the
//! prefix / contains / wildcard modes, `range_iter_values`) and the
//! suffix-index iteration (`suffix_value_iter`).
//!
//! The traversal logic itself is tested in `trie_rs`; these tests verify that
//! each mode drives the right traversal over the index's values trie.

use std::ptr::null;

use lending_iterator::LendingIterator;
use tag_index::{IterMode, TagIndex, ValueIterator};
use trie_rs::iter::{RangeBoundary, RangeFilter};

/// Collect the keys yielded by a lending iterator over `(key, value)` pairs.
macro_rules! collect_keys {
    ($iter:expr) => {{
        let mut iter = $iter;
        let mut keys: Vec<Vec<u8>> = Vec::new();
        while let Some((key, _)) = iter.next() {
            keys.push(key.to_vec());
        }
        keys
    }};
}

/// Drain a [`ValueIterator`] into its yielded keys, in iteration order.
fn value_iter_keys(mut it: ValueIterator<'_>) -> Vec<Vec<u8>> {
    let mut keys: Vec<Vec<u8>> = Vec::new();
    while let Some((key, _)) = it.advance() {
        keys.push(key.to_vec());
    }
    keys
}

/// Build an in-memory index holding `tags`, each with one document.
fn index_with_tags(tags: &[&[u8]]) -> TagIndex {
    let mut tag_index = TagIndex::new(1, None, false);
    tag_index.index(null(), null(), tags, 1);
    tag_index
}

/// The `Prefix` mode yields only the tags starting with the prefix, and each
/// yielded inverted index is the one stored in the trie.
#[test]
fn prefixed_iter_values_yields_only_matching_tags() {
    let tag_index = index_with_tags(&[b"bar", b"foo", b"foobar", b"foz"]);

    let mut iter = tag_index.value_iter_filtered(b"foo", IterMode::Prefix);
    let mut keys: Vec<Vec<u8>> = Vec::new();
    while let Some((tag, ii)) = iter.advance() {
        let ii = ii.expect("memory-mode iteration yields the stored inverted index");
        let found = tag_index.find_value(tag).expect("yielded tag is indexed");
        assert!(
            std::ptr::eq(ii, found),
            "the yielded reference should be the inverted index stored in the trie"
        );
        keys.push(tag.to_vec());
    }

    assert_eq!(keys, [b"foo".to_vec(), b"foobar".to_vec()]);
}

/// The `Contains` mode yields only the tags containing the fragment.
#[test]
fn contains_iter_values_yields_only_matching_tags() {
    let tag_index = index_with_tags(&[b"bar", b"foo", b"oof", b"xooy"]);

    let keys = value_iter_keys(tag_index.value_iter_filtered(b"oo", IterMode::Contains));

    assert_eq!(keys, [b"foo".to_vec(), b"oof".to_vec(), b"xooy".to_vec()]);
}

/// The `Wildcard` mode supports the `*` and `?` metacharacters.
#[test]
fn wildcard_iter_values_matches_metacharacters() {
    let tag_index = index_with_tags(&[b"bar", b"fao", b"fo", b"foo", b"fooo"]);

    let keys = value_iter_keys(tag_index.value_iter_filtered(b"f?o", IterMode::Wildcard));
    assert_eq!(keys, [b"fao".to_vec(), b"foo".to_vec()]);

    let keys = value_iter_keys(tag_index.value_iter_filtered(b"f*o", IterMode::Wildcard));
    assert_eq!(
        keys,
        [
            b"fao".to_vec(),
            b"fo".to_vec(),
            b"foo".to_vec(),
            b"fooo".to_vec()
        ]
    );
}

/// `range_iter_values` honors the boundaries' inclusiveness.
#[test]
fn range_iter_values_respects_boundaries() {
    let tag_index = index_with_tags(&[b"a", b"b", b"c", b"d"]);

    let keys = collect_keys!(tag_index.range_iter_values(RangeFilter {
        min: Some(RangeBoundary {
            value: b"b",
            is_included: true,
        }),
        max: Some(RangeBoundary {
            value: b"d",
            is_included: false,
        }),
    }));

    assert_eq!(keys, [b"b".to_vec(), b"c".to_vec()]);
}

/// Without `WITHSUFFIXTRIE` there is no suffix index to iterate.
#[test]
fn iter_suffix_entries_is_none_without_suffix_trie() {
    let tag_index = TagIndex::new(1, None, false);
    assert!(tag_index.suffix_value_iter().is_none());
}

/// With `WITHSUFFIXTRIE`, committing a tag registers every suffix of the tag
/// in the suffix index. The keys are NUL-free — the tag and each of its
/// suffixes without the trailing NUL — matching the C `addSuffixTrieMap`.
#[test]
fn iter_suffix_entries_lists_every_suffix() {
    let mut tag_index = TagIndex::new(1, None, true);
    tag_index.index(null(), null(), &[b"foo\0"], 1);
    tag_index.commit(&[b"foo\0"]);

    let keys = value_iter_keys(
        tag_index
            .suffix_value_iter()
            .expect("index was created with a suffix trie"),
    );

    let mut expected: Vec<Vec<u8>> = [b"foo".as_slice(), b"oo", b"o"]
        .iter()
        .map(|s| s.to_vec())
        .collect();
    expected.sort();
    assert_eq!(keys, expected);
}

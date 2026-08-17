/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for the public filtered-iteration API (`value_iter_filtered` in each of
//! its four modes, `range_iter_values`, and the iteration deadline).
//!
//! The traversal logic itself is tested in `trie_rs`; these tests verify that
//! each mode drives the right traversal over the index's values trie.

use ffi::timespec;
use lending_iterator::LendingIterator;
use tag_index::{InMemoryMode, IterMode, MemTagIndexIterator, TagIndex};
use trie_rs::iter::{RangeBoundary, RangeFilter};

use crate::util::index_mem;

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

/// Drain a [`MemTagIndexIterator`] into its yielded keys, in iteration order.
fn value_iter_keys(mut it: MemTagIndexIterator<'_>) -> Vec<Vec<u8>> {
    let mut keys: Vec<Vec<u8>> = Vec::new();
    while let Some((key, _)) = it.advance() {
        keys.push(key.to_vec());
    }
    keys
}

/// Build an in-memory index holding `tags`, each with one document.
fn index_with_tags(tags: &[&[u8]]) -> TagIndex<InMemoryMode> {
    let mut tag_index = TagIndex::<InMemoryMode>::new(false);
    index_mem(&mut tag_index, tags, 1);
    tag_index
}

/// A deadline that has already passed. Any `CLOCK_MONOTONIC_RAW` value one second
/// after boot is in the past on a running system.
const fn elapsed_deadline() -> timespec {
    timespec {
        tv_sec: 1,
        tv_nsec: 0,
    }
}

/// The `Prefix` mode yields only the tags starting with the prefix, and each
/// yielded inverted index is the one stored in the trie.
#[test]
fn prefixed_iter_values_yields_only_matching_tags() {
    let tag_index = index_with_tags(&[b"bar", b"foo", b"foobar", b"foz"]);

    let mut iter = tag_index.value_iter_filtered(b"foo", IterMode::Prefix);
    let mut keys: Vec<Vec<u8>> = Vec::new();
    while let Some((tag, ii)) = iter.advance() {
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

/// The `Suffix` mode yields only the tags ending with the pattern. Unlike the
/// other modes it filters a full trie walk, because a trie cannot seek by suffix.
#[test]
fn suffix_iter_values_yields_only_matching_tags() {
    let tag_index = index_with_tags(&[b"bar", b"foo", b"oof", b"xoo"]);

    let keys = value_iter_keys(tag_index.value_iter_filtered(b"oo", IterMode::Suffix));

    assert_eq!(
        keys,
        [b"foo".to_vec(), b"xoo".to_vec()],
        "`oof` contains the pattern but does not end with it"
    );
}

/// An elapsed deadline stops iteration early. The check is amortized over a fixed
/// number of entries, so the first ones still come through — what matters is that
/// iteration ends instead of walking the whole trie.
#[test]
#[cfg_attr(miri, ignore)] // probes CLOCK_MONOTONIC_RAW, unimplemented under miri
fn set_timeout_cuts_iteration_short() {
    // Comfortably more tags than the check granularity, so the deadline is
    // guaranteed to be probed before the walk finishes.
    let owned: Vec<Vec<u8>> = (0..400)
        .map(|i| format!("tag{i:04}").into_bytes())
        .collect();
    let tags: Vec<&[u8]> = owned.iter().map(|t| t.as_slice()).collect();
    let mut tag_index = TagIndex::<InMemoryMode>::new(false);
    index_mem(&mut tag_index, &tags, 1);

    let mut it = tag_index.value_iter();
    it.set_timeout(Some(elapsed_deadline()));
    let seen = value_iter_keys(it).len();

    assert!(seen > 0, "the entries before the first probe are yielded");
    assert!(
        seen < tags.len(),
        "an elapsed deadline must stop the walk before the end"
    );
}

/// The deadline also bounds the entries a suffix walk *skips*, not just the ones
/// it yields. A trie cannot seek by suffix, so the walk here has to visit hundreds
/// of non-matching tags before reaching the single match; probing the deadline only
/// per yielded match would let that whole scan run to completion first.
#[test]
#[cfg_attr(miri, ignore)] // probes CLOCK_MONOTONIC_RAW, unimplemented under miri
fn set_timeout_bounds_the_nonmatches_a_suffix_walk_skips() {
    // The only match sorts last, behind comfortably more non-matching tags than
    // the check granularity.
    let owned: Vec<Vec<u8>> = (0..400)
        .map(|i| format!("tag{i:04}").into_bytes())
        .chain(std::iter::once(b"zzzoo".to_vec()))
        .collect();
    let tags: Vec<&[u8]> = owned.iter().map(|t| t.as_slice()).collect();
    let mut tag_index = TagIndex::<InMemoryMode>::new(false);
    index_mem(&mut tag_index, &tags, 1);

    let mut it = tag_index.value_iter_filtered(b"oo", IterMode::Suffix);
    it.set_timeout(Some(elapsed_deadline()));

    assert!(
        it.advance().is_none(),
        "an elapsed deadline must stop the walk before it reaches the match"
    );
}

/// `None` means "no deadline", so the walk completes.
#[test]
fn no_timeout_lets_iteration_complete() {
    let tag_index = index_with_tags(&[b"a", b"b", b"c"]);

    let mut it = tag_index.value_iter();
    it.set_timeout(None);

    assert_eq!(value_iter_keys(it).len(), 3);
}

/// The `Wildcard` mode supports the `*` and `?` metacharacters.
#[test]
fn wildcard_iter_values_matches_metacharacters() {
    let tag_index = index_with_tags(&[b"bar", b"gao", b"go", b"goo", b"gooo"]);

    let keys = value_iter_keys(tag_index.value_iter_filtered(b"g?o", IterMode::Wildcard));
    assert_eq!(keys, [b"gao".to_vec(), b"goo".to_vec()]);

    let keys = value_iter_keys(tag_index.value_iter_filtered(b"g*o", IterMode::Wildcard));
    assert_eq!(
        keys,
        [
            b"gao".to_vec(),
            b"go".to_vec(),
            b"goo".to_vec(),
            b"gooo".to_vec()
        ],
        "`g*o` also matches `go`, where the `*` expands to nothing"
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

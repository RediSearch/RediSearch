/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for disk (bigredis/Flex) mode.
//!
//! Disk mode keeps the postings on disk behind the `SearchDisk_*` API; the
//! values trie holds only tag *presence* sentinels. These tests exercise every
//! disk path that does **not** dereference the disk index spec — `commit`
//! (presence + record accounting), the value-trie iterators used by query
//! expansion and `FT.TAGVALS`, `get_overhead`, and mode detection. The paths
//! that call into `SearchDisk_*` (`index`, `open_reader`) need a real disk
//! backend and are covered end-to-end elsewhere, so here the spec pointer is a
//! dangling-but-non-null placeholder that is never read.

use std::ptr::NonNull;

use ffi::RedisSearchDiskIndexSpec;
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

/// A dangling but non-null, well-aligned disk-spec pointer. Sound to store
/// because these tests only drive paths that never dereference it.
fn fake_disk_spec() -> NonNull<RedisSearchDiskIndexSpec> {
    NonNull::dangling()
}

/// Build a disk-mode index and register `tags` through `commit` (the phase-3
/// path). `commit` expects NUL-terminated tags, as the FFI boundary hands it.
fn disk_index_with_tags(tags: &[&[u8]], with_suffix: bool) -> TagIndex {
    let mut idx = TagIndex::new(1, Some((fake_disk_spec(), 0)), with_suffix);
    let owned: Vec<Vec<u8>> = tags
        .iter()
        .map(|t| {
            let mut v = t.to_vec();
            v.push(0);
            v
        })
        .collect();
    let refs: Vec<&[u8]> = owned.iter().map(|v| v.as_slice()).collect();
    idx.commit(&refs);
    idx
}

/// A disk spec selects disk mode (C: `TagIndex_HasDiskSpec`).
#[test]
fn disk_spec_means_disk_mode() {
    let idx = TagIndex::new(1, Some((fake_disk_spec(), 0)), false);
    assert!(idx.disk_mode());
}

/// `commit` counts every committed tag value (disk postings are written during
/// this phase) and registers each tag's presence in the values trie, keyed
/// NUL-free like the tags queries look up (C: `TagIndex_Commit` disk branch).
#[test]
fn commit_counts_records_and_registers_presence() {
    let mut idx = TagIndex::new(1, Some((fake_disk_spec(), 0)), false);

    let n = idx.commit(&[b"foo\0", b"bar\0", b"foo\0"]);
    assert_eq!(n, 3, "disk commit counts every committed tag value");

    // The duplicate `foo` collapses to a single trie entry.
    assert_eq!(idx.unique_values(), 2);

    let mut values = value_iter_keys(idx.value_iter());
    values.sort();
    assert_eq!(
        values,
        vec![b"bar".to_vec(), b"foo".to_vec()],
        "values trie is keyed by the NUL-free tag bytes"
    );
}

/// The empty tag (INDEXEMPTY) commits to an empty NUL-free key.
#[test]
fn commit_indexes_the_empty_tag() {
    let mut idx = TagIndex::new(1, Some((fake_disk_spec(), 0)), false);
    let n = idx.commit(&[b"\0"]);
    assert_eq!(n, 1);
    let values = value_iter_keys(idx.value_iter());
    assert_eq!(values, vec![Vec::<u8>::new()]);
}

/// In memory mode `commit` reports no records — they are counted at index time.
#[test]
fn memory_commit_reports_no_records() {
    let mut idx = TagIndex::new(1, None, false);
    assert_eq!(idx.commit(&[b"foo\0", b"bar\0"]), 0);
}

/// Disk-mode prefix iteration yields only the matching tag keys.
#[test]
fn disk_prefix_iteration_yields_matching_keys() {
    let idx = disk_index_with_tags(&[b"bar", b"foo", b"foobar", b"foz"], false);
    let keys = value_iter_keys(idx.value_iter_filtered(b"foo", IterMode::Prefix));
    assert_eq!(keys, [b"foo".to_vec(), b"foobar".to_vec()]);
}

/// Disk-mode contains (infix) iteration yields only the matching tag keys.
#[test]
fn disk_contains_iteration_yields_matching_keys() {
    let idx = disk_index_with_tags(&[b"bar", b"foo", b"oof", b"xooy"], false);
    let keys = value_iter_keys(idx.value_iter_filtered(b"oo", IterMode::Contains));
    assert_eq!(keys, [b"foo".to_vec(), b"oof".to_vec(), b"xooy".to_vec()]);
}

/// Disk-mode wildcard iteration supports the `*` and `?` metacharacters.
#[test]
fn disk_wildcard_iteration_matches_metacharacters() {
    let idx = disk_index_with_tags(&[b"bar", b"fao", b"fo", b"foo", b"fooo"], false);

    let keys = value_iter_keys(idx.value_iter_filtered(b"f?o", IterMode::Wildcard));
    assert_eq!(keys, [b"fao".to_vec(), b"foo".to_vec()]);

    let keys = value_iter_keys(idx.value_iter_filtered(b"f*o", IterMode::Wildcard));
    assert_eq!(
        keys,
        [b"fao".to_vec(), b"fo".to_vec(), b"foo".to_vec(), b"fooo".to_vec()]
    );
}

/// Disk-mode lexical range iteration yields the keys within the boundaries.
#[test]
fn disk_range_iteration_yields_keys_in_range() {
    let idx = disk_index_with_tags(&[b"a", b"b", b"c", b"d"], false);
    let filter = RangeFilter {
        min: Some(RangeBoundary {
            value: b"b",
            is_included: true,
        }),
        max: Some(RangeBoundary {
            value: b"c",
            is_included: true,
        }),
    };
    let keys = collect_keys!(idx.disk_range_iter_values(filter));
    assert_eq!(keys, [b"b".to_vec(), b"c".to_vec()]);
}

/// Full iteration visits every key in lexicographical order.
#[test]
fn disk_iter_values_yields_every_key_in_order() {
    let idx = disk_index_with_tags(&[b"z", b"a", b"m"], false);
    let keys = value_iter_keys(idx.value_iter());
    assert_eq!(keys, [b"a".to_vec(), b"m".to_vec(), b"z".to_vec()]);
}

/// `get_overhead` works in disk mode (C: `TagIndex_GetOverhead`, both modes).
#[test]
fn get_overhead_accounts_for_the_values_trie() {
    let idx = disk_index_with_tags(&[b"foo", b"bar", b"baz"], false);
    assert!(
        idx.get_overhead() > 0,
        "the values trie holding the tag keys is counted"
    );
}

/// Disk mode still supports the suffix trie (`WITHSUFFIXTRIE`): `commit`
/// populates it, so it holds entries afterwards.
#[test]
fn disk_mode_supports_suffix_trie() {
    let idx = disk_index_with_tags(&[b"hello", b"world"], true);
    assert!(idx.has_suffix());

    let mut suffix = idx
        .suffix_value_iter()
        .expect("suffix trie is enabled in disk mode");
    let mut count = 0;
    while suffix.advance().is_some() {
        count += 1;
    }
    assert!(count > 0, "commit populated the suffix trie in disk mode");
}

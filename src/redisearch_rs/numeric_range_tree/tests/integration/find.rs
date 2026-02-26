/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Tests for `NumericRangeTree::find` — range query functionality.

use inverted_index::NumericFilter;
use numeric_range_tree::NumericRangeTree;
use rstest::rstest;

/// Build a filter with full control over ascending, offset and limit.
fn make_filter_full(
    min: f64,
    max: f64,
    ascending: bool,
    offset: usize,
    limit: usize,
) -> NumericFilter {
    NumericFilter {
        min,
        max,
        ascending,
        offset,
        limit,
        ..Default::default()
    }
}

/// Build a large tree with 50k entries whose values cycle through 1..=5000.
/// This mirrors the C `testRangeTree` setup.
fn build_large_tree(compress_floats: bool) -> NumericRangeTree {
    let mut tree = NumericRangeTree::new(compress_floats);
    for i in 1..=50_000u64 {
        let value = ((i - 1) % 5000 + 1) as f64;
        tree.add(i, value, false, 0);
    }
    tree
}

#[rstest]
#[cfg_attr(miri, ignore = "Too slow to run under miri")]
fn test_find_large_tree(#[values(false, true)] compress_floats: bool) {
    let tree = build_large_tree(compress_floats);
    assert_eq!(tree.num_entries(), 50_000);

    let test_ranges: &[(f64, f64)] = &[
        (0.0, 100.0),
        (10.0, 1000.0),
        (2500.0, 3500.0),
        (0.0, 5000.0),
        (4999.0, 4999.0),
        (0.0, 0.0),
    ];

    for &(min, max) in test_ranges {
        let filter = NumericFilter {
            min,
            max,
            ..Default::default()
        };
        let ranges = tree.find(&filter);

        // Every returned range must overlap the query bounds.
        for range in &ranges {
            assert!(
                range.overlaps(min, max),
                "range [{}, {}] does not overlap query [{min}, {max}]",
                range.min_val(),
                range.max_val(),
            );
        }
    }
}

#[test]
#[cfg_attr(miri, ignore = "Too slow to run under miri")]
fn test_find_full_range() {
    let tree = build_large_tree(false);

    let filter = NumericFilter {
        min: f64::NEG_INFINITY,
        max: f64::INFINITY,
        ..Default::default()
    };
    let ranges = tree.find(&filter);

    // At minimum one range must be returned.
    assert!(
        !ranges.is_empty(),
        "full-range find must return at least one range"
    );

    // Non-overlapping ranges partition the entries exactly — each doc appears
    // in exactly one range, so the total must equal num_entries.
    let total_docs: u64 = ranges.iter().map(|r| r.num_docs() as u64).sum();
    assert_eq!(
        total_docs,
        tree.num_entries() as u64,
        "total docs across non-overlapping ranges should equal num_entries"
    );
}

#[test]
#[cfg_attr(miri, ignore = "Too slow to run under miri")]
fn test_find_no_overlap() {
    let tree = build_large_tree(false);

    // All values are in 1..=5000, so querying below 0 should find nothing.
    let filter = NumericFilter {
        min: -1000.0,
        max: -1.0,
        ..Default::default()
    };
    let ranges = tree.find(&filter);
    assert!(
        ranges.is_empty(),
        "expected no ranges for query outside stored values, got {}",
        ranges.len()
    );
}

#[test]
#[cfg_attr(miri, ignore = "Too slow to run under miri")]
fn test_find_single_point() {
    let tree = build_large_tree(false);

    let filter = NumericFilter {
        min: 42.0,
        max: 42.0,
        ..Default::default()
    };
    let ranges = tree.find(&filter);

    // At least one range must overlap the point.
    assert!(
        !ranges.is_empty(),
        "single-point query must find at least one range"
    );

    for range in &ranges {
        assert!(
            range.overlaps(42.0, 42.0),
            "range [{}, {}] does not contain 42.0",
            range.min_val(),
            range.max_val(),
        );
    }
}

#[test]
#[cfg_attr(miri, ignore = "Too slow to run under miri")]
fn test_find_with_offset_and_limit() {
    let tree = build_large_tree(false);

    // First get all ranges without offset/limit.
    let filter_all = NumericFilter {
        min: 0.0,
        max: 5000.0,
        ..Default::default()
    };
    let all_ranges = tree.find(&filter_all);
    // Now use a limit.
    let filter_limited = make_filter_full(0.0, 5000.0, true, 0, 10);
    let limited_ranges = tree.find(&filter_limited);
    // Limited should return fewer or equal ranges.
    assert!(
        limited_ranges.len() <= all_ranges.len(),
        "limited find ({}) should return <= all ranges ({})",
        limited_ranges.len(),
        all_ranges.len()
    );

    // With offset, we should also get fewer or equal ranges.
    let filter_offset = make_filter_full(0.0, 5000.0, true, 100, 10);
    let offset_ranges = tree.find(&filter_offset);
    assert!(
        offset_ranges.len() <= all_ranges.len(),
        "offset find ({}) should return <= all ranges ({})",
        offset_ranges.len(),
        all_ranges.len()
    );
}

#[test]
fn test_find_on_empty_tree() {
    let tree = NumericRangeTree::new(false);

    let filter = NumericFilter {
        min: 0.0,
        max: 100.0,
        ..Default::default()
    };
    let ranges = tree.find(&filter);

    // Empty ranges (num_docs == 0) are pruned, so an empty tree returns nothing.
    assert!(
        ranges.is_empty(),
        "empty tree find should return no ranges, got {}",
        ranges.len(),
    );
}

#[test]
fn test_find_on_empty_tree_infinite_bounds() {
    let tree = NumericRangeTree::new(false);

    let filter = NumericFilter {
        min: f64::NEG_INFINITY,
        max: f64::INFINITY,
        ..Default::default()
    };
    let ranges = tree.find(&filter);

    // Empty ranges (num_docs == 0) are pruned, so an empty tree returns nothing.
    assert!(
        ranges.is_empty(),
        "empty tree with infinite filter should return no ranges, got {}",
        ranges.len(),
    );
}

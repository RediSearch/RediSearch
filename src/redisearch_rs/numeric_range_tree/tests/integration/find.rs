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
use numeric_range_tree::{NumericRangeTree, RangeWindow};
use rstest::rstest;

/// Build a filter with full control over the bounds and traversal order.
fn make_filter_full(min: f64, max: f64, ascending: bool) -> NumericFilter {
    NumericFilter {
        min,
        max,
        ascending,
        ..Default::default()
    }
}

/// Build a large tree with 50k entries whose values cycle through 1..=5000.
/// This mirrors the C `testRangeTree` setup.
fn build_large_tree(compress_floats: bool) -> NumericRangeTree {
    let mut tree = NumericRangeTree::new(compress_floats);
    for i in 1..=50_000u64 {
        let value = ((i - 1) % 5000 + 1) as f64;
        tree.add(i, value, false, false, 0);
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
    let filter = make_filter_full(0.0, 5000.0, true);

    // Now use a limit.
    let limited_ranges = tree.find_windowed(
        &filter,
        RangeWindow {
            offset: 0,
            limit: 10,
        },
    );
    // Limited should return fewer or equal ranges.
    assert!(
        limited_ranges.len() <= all_ranges.len(),
        "limited find ({}) should return <= all ranges ({})",
        limited_ranges.len(),
        all_ranges.len()
    );

    // With offset, we should also get fewer or equal ranges.
    let offset_ranges = tree.find_windowed(
        &filter,
        RangeWindow {
            offset: 100,
            limit: 10,
        },
    );
    assert!(
        offset_ranges.len() <= all_ranges.len(),
        "offset find ({}) should return <= all ranges ({})",
        offset_ranges.len(),
        all_ranges.len()
    );
}

#[test]
#[cfg_attr(miri, ignore = "Too slow to run under miri")]
fn test_find_unbounded_window_matches_find() {
    let tree = build_large_tree(false);
    let filter = make_filter_full(0.0, 5000.0, true);

    let windowed = tree.find_windowed(&filter, RangeWindow::UNBOUNDED);
    let all = tree.find(&filter);

    assert_eq!(
        windowed.len(),
        all.len(),
        "an unbounded window must restrict nothing"
    );
    for (windowed, all) in windowed.iter().zip(all.iter()) {
        assert_eq!(windowed.min_val(), all.min_val());
        assert_eq!(windowed.max_val(), all.max_val());
    }
}

/// A window whose `limit` the tree cannot fill still returns every matching
/// range: a window may overshoot, but it may not drop a match.
#[test]
fn test_find_windowed_with_fewer_matches_than_limit() {
    // Two value clusters, each with enough cardinality to split the root, and a
    // gap between them wide enough that the split value lands inside it.
    let mut tree = NumericRangeTree::new(false);
    let mut doc: u64 = 1;
    for i in 0..500u64 {
        tree.add(doc, 5.0 + (i % 50) as f64 * 0.08, false, false, 0);
        doc += 1;
        tree.add(doc, 21.0 + (i % 50) as f64 * 0.08, false, false, 0);
        doc += 1;
    }
    assert_eq!(
        tree.num_leaves(),
        2,
        "fixture must split into exactly the two cluster leaves"
    );

    // The filter slices through both clusters, so neither leaf is contained in
    // it: the traversal cannot know how many of their documents match without
    // reading them. Fewer documents match than the limit asks for.
    let filter = make_filter_full(8.0, 22.0, true);
    let windowed = tree.find_windowed(
        &filter,
        RangeWindow {
            offset: 0,
            limit: 300,
        },
    );

    let all = tree.find(&filter);
    assert_eq!(
        windowed.len(),
        all.len(),
        "a window the tree cannot fill must not restrict the result"
    );
    for (windowed, all) in windowed.iter().zip(&all) {
        assert!(std::ptr::eq(*windowed, *all));
    }
}

/// Consecutive windows must tile the unbounded result: advancing `offset` by
/// the consumed ranges' whole-`num_docs` sum yields the next ranges in order,
/// with no gap, no overlap, and nothing left over.
///
/// This is the contract the optimizer's expand-and-retry loop relies on
/// (`OPT_Rewind` advances the filter's offset by the drained iterator's
/// estimate; `NumericScoreSource::expand_window` does the same with its
/// `RangeWindow`). It is also where the first window's head-leaf-counted-as-1
/// rule and the full-count offset advance must agree with each other.
#[rstest]
#[cfg_attr(miri, ignore = "Too slow to run under miri")]
fn test_find_windowed_windows_tile(#[values(true, false)] ascending: bool) {
    let tree = build_large_tree(false);
    // Fractional bounds so both boundary leaves overlap the filter without
    // being contained in it, exercising the head-leaf special case.
    let filter = make_filter_full(2.5, 4321.5, ascending);

    let all = tree.find(&filter);
    assert!(all.len() > 3, "fixture must produce several ranges");

    // Walk the tree window by window, advancing `offset` the way the
    // consumers do: by the whole-range document count of what was consumed.
    let mut window = RangeWindow {
        offset: 0,
        limit: 100,
    };
    let mut tiled = Vec::new();
    loop {
        let ranges = tree.find_windowed(&filter, window);
        if ranges.is_empty() {
            break;
        }
        window.offset += ranges.iter().map(|r| r.num_docs() as usize).sum::<usize>();
        tiled.extend(ranges);
        assert!(
            tiled.len() <= all.len(),
            "windows returned more ranges than the unbounded find: overlap or \
             non-termination (offset={}, limit={})",
            window.offset,
            window.limit,
        );
    }

    // Both sequences borrow ranges from the same tree, so correct tiling means
    // they hold literally the same ranges in the same order — which pointer
    // identity states directly, proving completeness, order, and disjointedness
    // at once. Comparing bounds instead would assume `(min_val, max_val)`
    // uniquely identifies a range, and the tree does not promise that: with
    // `max_depth_range > 0`, an internal node retains a range indexing the
    // same documents as its subtree, whose bounds can coincide with a child
    // leaf's. The containment shortcut in the traversal picks between exactly
    // those two, so a bounds comparison could miss a divergence there.
    assert_eq!(
        tiled.len(),
        all.len(),
        "windows must cover every range exactly once"
    );
    for (i, (tiled, all)) in tiled.iter().zip(all.iter()).enumerate() {
        assert!(
            std::ptr::eq(*tiled, *all),
            "window tiling diverged from the unbounded result at position {i}: \
             got [{}, {}], expected [{}, {}]",
            tiled.min_val(),
            tiled.max_val(),
            all.min_val(),
            all.max_val(),
        );
    }
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

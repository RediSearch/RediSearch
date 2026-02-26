/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// Link both Rust-provided and C-provided symbols
extern crate redisearch_rs;
// Mock or stub the ones that aren't provided by the line above
redis_mock::mock_or_stub_missing_redis_c_symbols!();

// Benchmarks for `NumericRangeTree::find`.

use std::time::Duration;

use criterion::measurement::WallTime;
use criterion::{BenchmarkGroup, Criterion, criterion_group, criterion_main};
use inverted_index::NumericFilter;
use numeric_range_tree::NumericRangeTree;
use numeric_range_tree::test_utils::{build_large_tree, build_tree};

fn bench_leaf_only(group: &mut BenchmarkGroup<'_, WallTime>, tree: &NumericRangeTree) {
    let filter = NumericFilter {
        min: 42.0,
        max: 42.0,
        ..Default::default()
    };
    assert_eq!(
        tree.find(&filter).len(),
        1,
        "Leaf Only: point query should hit exactly one range"
    );
    group.bench_function("Leaf Only", |b| b.iter(|| tree.find(&filter)));
}

fn bench_multiple_leaves(group: &mut BenchmarkGroup<'_, WallTime>, tree: &NumericRangeTree) {
    let filter = NumericFilter {
        min: 100.0,
        max: 1000.0,
        ..Default::default()
    };
    assert!(
        tree.find(&filter).len() > 1,
        "Multiple Leaves: range query should hit multiple ranges, got {}",
        tree.find(&filter).len()
    );
    group.bench_function("Multiple Leaves", |b| b.iter(|| tree.find(&filter)));
}

fn bench_full_tree_scan(group: &mut BenchmarkGroup<'_, WallTime>, tree: &NumericRangeTree) {
    let filter = NumericFilter {
        min: f64::NEG_INFINITY,
        max: f64::INFINITY,
        ..Default::default()
    };
    assert_eq!(
        tree.find(&filter).len(),
        tree.num_leaves(),
        "Full Tree Scan: should return all leaves"
    );
    group.bench_function("Full Tree Scan", |b| b.iter(|| tree.find(&filter)));
}

fn bench_no_match(group: &mut BenchmarkGroup<'_, WallTime>, tree: &NumericRangeTree) {
    let filter = NumericFilter {
        min: -1000.0,
        max: -1.0,
        ..Default::default()
    };
    assert!(
        tree.find(&filter).is_empty(),
        "No Match: should return zero ranges"
    );
    group.bench_function("No Match", |b| b.iter(|| tree.find(&filter)));
}

fn bench_contained_internal(group: &mut BenchmarkGroup<'_, WallTime>) {
    let retained_tree = build_tree(50_000, false, 2);
    let baseline_tree = build_tree(50_000, false, 0);
    let filter = NumericFilter {
        min: 1.0,
        max: 50000.0,
        ..Default::default()
    };

    let n_retained_ranges = retained_tree.find(&filter).len();
    let n_baseline_ranges = baseline_tree.find(&filter).len();
    assert!(
        n_retained_ranges < n_baseline_ranges,
        "Contained Internal: the tree with internal ranges should return \
        fewer ranges ({n_retained_ranges}) than the one without retention ({n_baseline_ranges})"
    );

    group.bench_function("Contained Internal", |b| {
        b.iter(|| retained_tree.find(&filter))
    });
}

fn bench_with_offset_limit(group: &mut BenchmarkGroup<'_, WallTime>, tree: &NumericRangeTree) {
    let filter = NumericFilter {
        min: 0.0,
        max: 5000.0,
        offset: 100,
        limit: 10,
        ..Default::default()
    };
    assert!(
        !tree.find(&filter).is_empty(),
        "With Offset/Limit: should return some ranges"
    );
    group.bench_function("With Offset/Limit", |b| b.iter(|| tree.find(&filter)));
}

fn benchmark_find(c: &mut Criterion) {
    let mut group = c.benchmark_group("Find");
    group.measurement_time(Duration::from_secs(3));
    group.warm_up_time(Duration::from_millis(500));

    let large_tree = build_large_tree();
    assert!(
        large_tree.num_leaves() > 1,
        "precondition: large tree must have multiple leaves"
    );

    bench_leaf_only(&mut group, &large_tree);
    bench_multiple_leaves(&mut group, &large_tree);
    bench_full_tree_scan(&mut group, &large_tree);
    bench_no_match(&mut group, &large_tree);
    bench_contained_internal(&mut group);
    bench_with_offset_limit(&mut group, &large_tree);

    group.finish();
}

criterion_group!(benches, benchmark_find);
criterion_main!(benches);

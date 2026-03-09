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

// Benchmarks for `NumericRangeTree::add`.

use std::time::Duration;

use criterion::measurement::WallTime;
use criterion::{
    BatchSize, BenchmarkGroup, BenchmarkId, Criterion, criterion_group, criterion_main,
};
use numeric_range_tree::NumericRangeTree;
use numeric_range_tree::test_utils::{
    DEEP_TREE_ENTRIES, build_single_leaf_tree, build_tree, build_tree_at_split_edge,
};

fn bench_no_split_small(group: &mut BenchmarkGroup<'_, WallTime>) {
    let setup = || build_single_leaf_tree(10);
    let measure = |mut tree: NumericRangeTree| tree.add(11, 5.0, false, 0);
    let result = measure(setup());
    assert!(
        !result.changed,
        "No Split/small: add must not trigger a split"
    );
    group.bench_function("No Split/small", |b| {
        b.iter_batched(setup, measure, BatchSize::SmallInput)
    });
}

fn bench_no_split_large(group: &mut BenchmarkGroup<'_, WallTime>) {
    let setup = || build_single_leaf_tree(1000);
    let measure = |mut tree: NumericRangeTree| tree.add(1001, 5.0, false, 0);
    let result = measure(setup());
    assert!(
        !result.changed,
        "No Split/large: add must not trigger a split"
    );
    group.bench_function("No Split/large", |b| {
        b.iter_batched(setup, measure, BatchSize::SmallInput)
    });
}

fn bench_splits_single(group: &mut BenchmarkGroup<'_, WallTime>) {
    let (_edge_tree, split_doc_id) = build_tree_at_split_edge();
    let setup = move || build_tree(split_doc_id - 1, false, 0);
    let measure = move |mut tree: NumericRangeTree| {
        let result = tree.add(split_doc_id, split_doc_id as f64, false, 0);
        (result, tree)
    };
    let (result, tree) = measure(setup());
    assert!(
        result.changed,
        "With Splits/single: add must trigger a split"
    );
    assert!(
        tree.num_leaves() > 1,
        "With Splits/single: tree must have multiple leaves"
    );
    group.bench_function("With Splits/single", |b| {
        b.iter_batched(setup, measure, BatchSize::SmallInput)
    });
}

fn bench_retained_ranges(group: &mut BenchmarkGroup<'_, WallTime>) {
    for n in [100, DEEP_TREE_ENTRIES] {
        let setup = NumericRangeTree::default;
        let measure = move |mut tree: NumericRangeTree| {
            for i in 1..=n {
                tree.add(i, i as f64, false, 2);
            }
            tree
        };
        let tree = measure(setup());
        assert!(
            tree.num_ranges() > tree.num_leaves(),
            "With Retained Ranges/batch/{n}: internal nodes must retain ranges \
             (num_ranges={}, num_leaves={})",
            tree.num_ranges(),
            tree.num_leaves(),
        );
        group.bench_function(BenchmarkId::new("With Retained Ranges/batch", n), |b| {
            b.iter_batched(setup, measure, BatchSize::SmallInput)
        });
    }
}

fn benchmark_add(c: &mut Criterion) {
    let mut group = c.benchmark_group("Add");
    group.measurement_time(Duration::from_secs(3));
    group.warm_up_time(Duration::from_millis(500));

    bench_no_split_small(&mut group);
    bench_no_split_large(&mut group);
    bench_splits_single(&mut group);
    bench_retained_ranges(&mut group);

    group.finish();
}

criterion_group!(benches, benchmark_add);
criterion_main!(benches);

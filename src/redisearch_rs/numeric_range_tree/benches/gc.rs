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

// Benchmarks for `NumericRangeTree::compact_if_sparse`.

use std::time::Duration;

use criterion::measurement::WallTime;
use criterion::{
    BatchSize, BenchmarkGroup, BenchmarkId, Criterion, criterion_group, criterion_main,
};
use numeric_range_tree::test_utils::{DEEP_TREE_ENTRIES, SPLIT_TRIGGER, build_tree, gc_all_ranges};

fn bench_compact(group: &mut BenchmarkGroup<'_, WallTime>) {
    for n in [SPLIT_TRIGGER * 2, DEEP_TREE_ENTRIES, 5000] {
        let setup = move || {
            let mut tree = build_tree(n, false, 0);
            let half = n / 2;
            gc_all_ranges(&mut tree, &|doc_id| doc_id > half);
            tree
        };
        // Sanity check: compact_if_sparse actually trims and compacts.
        {
            let mut tree = setup();
            assert!(
                tree.is_sparse(),
                "Compact/{n}: tree should be sparse after GC"
            );
            let leaves_before = tree.num_leaves();
            let result = tree.compact_if_sparse();
            assert!(
                result.inverted_index_size_delta != 0 || result.node_size_delta != 0,
                "Compact/{n}: compact_if_sparse must do actual work"
            );
            assert!(
                tree.num_leaves() < leaves_before,
                "Compact/{n}: should have fewer leaves after compaction"
            );
        }
        group.bench_function(BenchmarkId::new("Compact", n), |b| {
            b.iter_batched(
                setup,
                |mut tree| tree.compact_if_sparse(),
                BatchSize::SmallInput,
            )
        });
    }
}

fn benchmark_gc(c: &mut Criterion) {
    let mut group = c.benchmark_group("GC");
    group.measurement_time(Duration::from_secs(10));
    group.warm_up_time(Duration::from_millis(500));

    bench_compact(&mut group);

    group.finish();
}

criterion_group!(benches, benchmark_gc);
criterion_main!(benches);

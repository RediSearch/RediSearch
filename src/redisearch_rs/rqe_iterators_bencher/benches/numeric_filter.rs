/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Benchmark numeric filter construction and traversal using the pre-estimation API.
//!
//! Keep this harness usable on both sides of the advisory-estimate change so its
//! constructor cost can be compared without changing the measured operations.

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use field::{FieldExpirationPredicate, FieldFilterContext, FieldMaskOrIndex};
use index_result::RSIndexResult;
use inverted_index::{IndexReader, NumericFilter};
use rqe_iterators::{FieldExpirationChecker, NumericIteratorVariant, RQEIterator};
use rqe_iterators_bencher as _;
use rqe_iterators_test_utils::TestContext;

/// Two distinct values keep the population in a single numeric range.
const NUM_DOCS: u64 = 10_000;

/// Benchmark the direct reader and range-tree construction paths on the same data.
fn benchmark_numeric_filter(c: &mut Criterion) {
    let context = TestContext::numeric(
        (1..=NUM_DOCS).map(|id| {
            RSIndexResult::build_numeric(if id % 2 == 0 { 0.0 } else { 100.0 })
                .doc_id(id)
                .build()
        }),
        false,
    );
    let tree = context.numeric_range_tree_ref();
    let ranges = tree
        .indexed_iter()
        .filter_map(|(_, node)| node.range())
        .collect::<Vec<_>>();
    assert_eq!(ranges.len(), 1);
    let range = ranges[0];
    let field_context = FieldFilterContext {
        field: FieldMaskOrIndex::Index(context.field_spec().index),
        predicate: FieldExpirationPredicate::Default,
    };

    for (label, min, max) in [("partial", 0.0, 50.0), ("equality", 100.0, 100.0)] {
        let filter = NumericFilter {
            min,
            max,
            min_inclusive: true,
            max_inclusive: true,
            ..Default::default()
        };
        let direct = || {
            let reader = range.entries().reader();
            // SAFETY: (1) context owns the live search context and spec for this
            // benchmark. (2) field_context identifies its numeric field by index.
            let checker =
                unsafe { FieldExpirationChecker::new(context.sctx, field_context, reader.flags()) };
            // SAFETY: the context and its index remain alive and immutable for
            // the iterator's lifetime; no range source is supplied.
            unsafe {
                NumericIteratorVariant::new(
                    reader,
                    Some(black_box(&filter)),
                    checker,
                    None,
                    black_box(range.min_val()),
                    black_box(range.max_val()),
                )
            }
        };
        let from_tree = || {
            // SAFETY: (1) context owns the live search context and spec.
            // (2) field_context selects its numeric field by index. (3) The tree
            // is immutable and retained by context throughout every iteration.
            unsafe {
                NumericIteratorVariant::from_tree(
                    context.numeric_range_tree(),
                    context.sctx,
                    black_box(&filter),
                    &field_context,
                )
            }
        };
        let mut group = c.benchmark_group(format!("Iterator - NumericFilter - {label}"));
        group.bench_function("construct_direct", |b| b.iter(|| black_box(direct())));
        group.bench_function("construct_tree", |b| b.iter(|| black_box(from_tree())));
        group.bench_function("read_direct", |b| {
            b.iter(|| {
                let mut iterator = black_box(direct());
                while let Some(result) = iterator.read().expect("numeric read failed") {
                    black_box(result);
                }
            });
        });
        group.bench_function("read_tree", |b| {
            b.iter(|| {
                for mut iterator in black_box(from_tree()) {
                    while let Some(result) = iterator.read().expect("numeric read failed") {
                        black_box(result);
                    }
                }
            });
        });
        group.finish();
    }
}

criterion_group!(benches, benchmark_numeric_filter);
criterion_main!(benches);

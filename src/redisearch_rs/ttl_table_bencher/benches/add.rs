/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Bulk-insert benchmarks for `TimeToLiveTable::add`.

use std::{hint::black_box, num::NonZeroUsize};

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ttl_table::TimeToLiveTable;
use ttl_table_bencher::{
    DocsInput, FieldExpirationInput, create_and_populate_c, create_docs, create_docs_c,
};

fn doc_inputs() -> Vec<DocsInput> {
    let mut doc_inputs = Vec::new();
    for doc_count in [1_000_usize, 100_000, 1_000_000] {
        for doc_fill_probability in [0.9_f32, 0.5, 0.1] {
            doc_inputs.push(DocsInput {
                count: doc_count,
                start_doc_id_from: 0,
                fill_probability: doc_fill_probability,
                field_expiration_input: FieldExpirationInput {
                    count_mean: 5,
                    count_variation: 2,
                    fill_probability: 1.0,
                    expired_probability: 0.,
                    far_future_percentage: 0.
                }
            });
        }
    }
    doc_inputs
}

fn add_sequential(c: &mut Criterion) {
    let mut group = c.benchmark_group("add/sequential");

    let doc_inputs = doc_inputs();

    for max_size in [1_000, 100_000, 1_000_000] {
        let max_size = NonZeroUsize::new(max_size as usize).unwrap();

        for doc_input in &doc_inputs {

            group.throughput(Throughput::Elements(doc_input.count as u64));
            group.bench_function(BenchmarkId::from_parameter(format!("slot_size={}/pop_count={}/doc_filled_at={}/lang=Rust", max_size, doc_input.count, doc_input.fill_probability)), |b| {
                b.iter_batched(
                    || {
                        create_docs(*doc_input, rand::rng())
                    },
                    |inputs| {
                        let mut t = TimeToLiveTable::new(max_size);
                        for (doc_id, fields) in inputs {
                            unsafe { black_box(t.add(black_box(doc_id), black_box(fields))) };
                        }
                        t
                    },
                    BatchSize::LargeInput,
                )
            });

            group.bench_function(BenchmarkId::from_parameter(format!("slot_size={}/pop_count={}/doc_filled_at={}/lang=C", max_size, doc_input.count, doc_input.fill_probability)), |b| {
                b.iter_batched(
                    || {
                        create_docs_c(*doc_input, rand::rng())
                    },
                    |inputs| {
                        // SAFETY: `create_docs_c` builds each payload in `0..real_count`
                        // order and guarantees it is non-empty, satisfying
                        // `TimeToLiveTable_Add`'s precondition.
                        unsafe { create_and_populate_c(black_box(max_size), black_box(inputs)) }
                    },
                    BatchSize::LargeInput,
                )
            });
        }
    }

    group.finish();
}

criterion_group!(benches, add_sequential);
criterion_main!(benches);

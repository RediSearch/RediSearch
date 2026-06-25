/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Read-side benchmarks for `TimeToLiveTable::field_satisfies_predicate`.

use std::hint::black_box;
use std::num::NonZeroUsize;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ffi::t_docId;
use ttl_table::{FieldExpirationPredicate, test_utils::NOW};
use ttl_table_bencher::{DocsInput, FieldExpirationInput, create_and_populate, create_docs};

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
                    far_future_percentage: 0.,
                },
            });
        }
    }
    doc_inputs
}

fn field_satisfies_predicate_doc_default(c: &mut Criterion) {
    let mut group = c.benchmark_group("field_satisfies_predicate/doc_default");

    let doc_inputs = doc_inputs();

    for max_size in [1_000, 100_000, 1_000_000] {
        let max_size = NonZeroUsize::new(max_size as usize).unwrap();

        for doc_input in &doc_inputs {
            let inputs = create_docs(*doc_input, rand::rng());
            let table = create_and_populate(max_size, inputs);

            group.throughput(Throughput::Elements(doc_input.count as u64));
            group.bench_function(
                BenchmarkId::from_parameter(format!(
                    "slot_size={}/pop_count={}/doc_filled_at={}/lang=Rust",
                    max_size, doc_input.count, doc_input.fill_probability
                )),
                |b| {
                    b.iter_batched(
                        || (0..doc_input.count).collect::<Vec<_>>(),
                        |doc_ids| {
                            let mut acc = 0u64;
                            for doc_id in doc_ids {
                                let ok = table.field_satisfies_predicate(
                                    black_box(doc_id as t_docId),
                                    0,
                                    FieldExpirationPredicate::Default,
                                    &NOW,
                                );
                                acc += ok as u64;
                            }
                            black_box(acc)
                        },
                        BatchSize::LargeInput,
                    );
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, field_satisfies_predicate_doc_default);
criterion_main!(benches);

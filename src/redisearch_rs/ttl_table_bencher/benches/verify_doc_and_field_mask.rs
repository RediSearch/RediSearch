/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Read-side benchmarks for `TimeToLiveTable::verify_doc_and_field_mask`.

use std::hint::black_box;
use std::num::NonZeroUsize;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ffi::t_docId;
use ttl_table::{FieldExpirationPredicate, test_utils::{NOW, identity_ft_id}};
use ttl_table_bencher::{DocsInput, FieldExpirationInput, create_and_populate, create_docs, random_mask};

fn verify_doc_and_field_mask_doc_default(c: &mut Criterion) {
    let mut group = c.benchmark_group("verify_doc_and_field_mask/doc_default");

    let mut doc_inputs = Vec::new();
    for doc_count in [1_000_usize, 100_000, 1_000_000] {
        for doc_fill_probability in [0.9_f32, 0.5, 0.1] {
            for field_fill_probability in [0.9, 0.5, 0.1] {
                for field_expiration_probability in [0.9, 0.5, 0.1] {
                    doc_inputs.push(DocsInput {
                        count: doc_count,
                        start_doc_id_from: 0,
                        fill_probability: doc_fill_probability,
                        field_expiration_input: FieldExpirationInput {
                            count_mean: 10,
                            count_variation: 2,
                            fill_probability: field_fill_probability,
                            expired_probability: field_expiration_probability,
                            far_future_percentage: 0.
                        }
                    });
                }
            }
        }
    }

    let ft_id_to_field_index = identity_ft_id();

    for max_size in [1_000, 100_000, 1_000_000] {
        let max_size = NonZeroUsize::new(max_size as usize).unwrap();

        for doc_input in &doc_inputs {

            let mut rng = rand::rng();
            let inputs = create_docs(*doc_input, rng.clone());
            // SAFETY: `create_docs` returns FieldExpiration vectors sorted by `index`
            // (constructed monotonically in `0..real_count`), satisfying `add`'s precondition.
            let table = unsafe { create_and_populate(max_size, inputs) };

            let mask = random_mask(&mut rng);

            group.throughput(Throughput::Elements(1));
            group.bench_function(BenchmarkId::from_parameter(format!("slot_size={}/pop_count={}/doc_filled_at={}/field_filled_at={}/expired={}/mask={:#010b}", max_size, doc_input.count, doc_input.fill_probability, doc_input.field_expiration_input.fill_probability, doc_input.field_expiration_input.expired_probability, mask)), |b| {
                b.iter_batched(|| {
                    (0..doc_input.count).collect::<Vec<_>>()
                },
                    |doc_ids| {
                    for doc_id in doc_ids {
                        black_box(table.verify_doc_and_field_mask(
                            black_box(doc_id as t_docId),
                            black_box(mask),
                            FieldExpirationPredicate::Default,
                            &NOW,
                            &ft_id_to_field_index,
                        ));
                    }
                }, BatchSize::LargeInput);
            });
        }
    }

    group.finish();
}

criterion_group!(benches, verify_doc_and_field_mask_doc_default);
criterion_main!(benches);

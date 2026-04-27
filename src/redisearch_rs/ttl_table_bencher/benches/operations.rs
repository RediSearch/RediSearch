/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Head-to-head benchmarks of the TTL-table implementations.
//!
//! - `Rust (Buckets)` — [`ttl_table_2`], faithful direct-modulo port of
//!   the C module at `src/ttl_table/`.
//! - `C` — the C `TimeToLiveTable_*` API, accessed through the `ffi`
//!   crate and wrapped by [`ttl_table_bencher::c_adapter`].
//!
//! Only the operations exposed by the C public API are benchmarked.

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use ttl_table_bencher::{
    FieldExpirationPredicate, Workload, build_ttl2, c_adapter, fields_c, fields_v2, ftid_table,
    mask_u32, mask_u128, query_now_ts,
};

const N_DOCS: u64 = 10_000;
const MAX_SIZE: usize = 16_384;

const fn workload(k_fields: u16) -> Workload {
    Workload {
        max_size: MAX_SIZE,
        n_docs: N_DOCS,
        k_fields,
    }
}

/// Pre-populates a C `TimeToLiveTable` with the same monotonic doc-id
/// pattern used by [`build_ttl2`]. Caller owns the returned pointer and
/// must pass it to [`c_adapter::c_destroy`].
fn build_ttl_c(w: &Workload) -> *mut ffi::TimeToLiveTable {
    let table = c_adapter::c_init(w.max_size);
    for doc_id in 1..=w.n_docs {
        c_adapter::c_add(table, doc_id, &fields_c(w.k_fields));
    }
    table
}

fn bench_add(c: &mut Criterion) {
    let w = workload(8);
    let mut group = c.benchmark_group("TTLTable - Add");
    group.throughput(criterion::Throughput::Elements(w.n_docs));

    group.bench_function("Rust (Buckets)", |b| {
        b.iter_batched(
            || ttl_table_2::TimeToLiveTable::new(w.max_size),
            |mut t| {
                for doc_id in 1..=w.n_docs {
                    t.add(doc_id, fields_v2(w.k_fields));
                }
                black_box(t)
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("C", |b| {
        b.iter_batched(
            || c_adapter::c_init(w.max_size),
            |t| {
                for doc_id in 1..=w.n_docs {
                    c_adapter::c_add(t, doc_id, &fields_c(w.k_fields));
                }
                // Free between iterations so memory does not grow without
                // bound across batches.
                c_adapter::c_destroy(t);
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

fn bench_remove(c: &mut Criterion) {
    let w = workload(8);
    let mut group = c.benchmark_group("TTLTable - Remove");
    group.throughput(criterion::Throughput::Elements(w.n_docs));

    group.bench_function("Rust (Buckets)", |b| {
        b.iter_batched_ref(
            || build_ttl2(&w),
            |t| {
                for doc_id in 1..=w.n_docs {
                    t.remove(doc_id);
                }
            },
            BatchSize::LargeInput,
        );
    });

    group.bench_function("C", |b| {
        b.iter_batched(
            || build_ttl_c(&w),
            |t| {
                for doc_id in 1..=w.n_docs {
                    c_adapter::c_remove(t, doc_id);
                }
                // The table is now empty; tear down the wrapper.
                c_adapter::c_destroy(t);
            },
            BatchSize::LargeInput,
        );
    });

    group.finish();
}

fn bench_verify_doc_and_field(c: &mut Criterion) {
    let w = workload(8);
    let now = query_now_ts();
    let predicate = FieldExpirationPredicate::Missing;
    let probed_field: u16 = 0;

    {
        let mut group = c.benchmark_group("TTLTable - VerifyDocAndField - Hit");
        group.bench_function("Rust (Buckets)", |b| {
            let table = build_ttl2(&w);
            let mut i: u64 = 0;
            b.iter(|| {
                i = i.wrapping_add(1);
                let doc_id = (i % w.n_docs) + 1;
                black_box(table.verify_doc_and_field(doc_id, probed_field, predicate, &now))
            });
        });
        group.bench_function("C", |b| {
            let table = build_ttl_c(&w);
            let mut i: u64 = 0;
            b.iter(|| {
                i = i.wrapping_add(1);
                let doc_id = (i % w.n_docs) + 1;
                black_box(c_adapter::c_verify_doc_and_field(
                    table,
                    doc_id,
                    probed_field,
                    predicate,
                    &now,
                ))
            });
            c_adapter::c_destroy(table);
        });
        group.finish();
    }

    {
        let mut group = c.benchmark_group("TTLTable - VerifyDocAndField - Miss");
        let miss_doc_id = w.n_docs + 1;
        group.bench_function("Rust (Buckets)", |b| {
            let table = build_ttl2(&w);
            b.iter(|| {
                black_box(table.verify_doc_and_field(miss_doc_id, probed_field, predicate, &now))
            });
        });
        group.bench_function("C", |b| {
            let table = build_ttl_c(&w);
            b.iter(|| {
                black_box(c_adapter::c_verify_doc_and_field(
                    table,
                    miss_doc_id,
                    probed_field,
                    predicate,
                    &now,
                ))
            });
            c_adapter::c_destroy(table);
        });
        group.finish();
    }
}

fn bench_verify_doc_and_field_mask(c: &mut Criterion) {
    // `K = 32` keeps every popcount we test ≤ K, so the verify loop
    // does not short-circuit through the `field_with_expiration_count <
    // field_count` early-exit branch and we measure the actual mask
    // walk on every iteration.
    let w = workload(32);
    let now = query_now_ts();
    let predicate = FieldExpirationPredicate::Missing;
    let translation = ftid_table(u32::BITS);

    let mut group = c.benchmark_group("TTLTable - VerifyDocAndFieldMask u32");

    for &popcount in &[1u32, 4, 16, 32] {
        let mask = mask_u32(popcount);

        group.bench_with_input(
            BenchmarkId::new("Rust (Buckets)", format!("popcount={popcount}")),
            &mask,
            |b, &mask| {
                let table = build_ttl2(&w);
                let mut i: u64 = 0;
                b.iter(|| {
                    i = i.wrapping_add(1);
                    let doc_id = (i % w.n_docs) + 1;
                    black_box(table.verify_doc_and_field_mask(
                        doc_id,
                        mask,
                        predicate,
                        &now,
                        &translation,
                    ))
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("C", format!("popcount={popcount}")),
            &mask,
            |b, &mask| {
                let table = build_ttl_c(&w);
                let mut i: u64 = 0;
                b.iter(|| {
                    i = i.wrapping_add(1);
                    let doc_id = (i % w.n_docs) + 1;
                    black_box(c_adapter::c_verify_doc_and_field_mask(
                        table,
                        doc_id,
                        mask,
                        predicate,
                        &now,
                        &translation,
                    ))
                });
                c_adapter::c_destroy(table);
            },
        );
    }

    group.finish();
}

fn bench_verify_doc_and_wide_field_mask(c: &mut Criterion) {
    // `K = 128` keeps every popcount we test ≤ K — see the u32 group
    // above for the rationale.
    let w = workload(128);
    let now = query_now_ts();
    let predicate = FieldExpirationPredicate::Missing;
    let translation = ftid_table(u128::BITS);

    let mut group = c.benchmark_group("TTLTable - VerifyDocAndWideFieldMask u128");

    for &popcount in &[1u32, 8, 64, 128] {
        let mask = mask_u128(popcount);

        group.bench_with_input(
            BenchmarkId::new("Rust (Buckets)", format!("popcount={popcount}")),
            &mask,
            |b, &mask| {
                let table = build_ttl2(&w);
                let mut i: u64 = 0;
                b.iter(|| {
                    i = i.wrapping_add(1);
                    let doc_id = (i % w.n_docs) + 1;
                    black_box(table.verify_doc_and_wide_field_mask(
                        doc_id,
                        mask,
                        predicate,
                        &now,
                        &translation,
                    ))
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("C", format!("popcount={popcount}")),
            &mask,
            |b, &mask| {
                let table = build_ttl_c(&w);
                let mut i: u64 = 0;
                b.iter(|| {
                    i = i.wrapping_add(1);
                    let doc_id = (i % w.n_docs) + 1;
                    black_box(c_adapter::c_verify_doc_and_wide_field_mask(
                        table,
                        doc_id,
                        mask,
                        predicate,
                        &now,
                        &translation,
                    ))
                });
                c_adapter::c_destroy(table);
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_add,
    bench_remove,
    bench_verify_doc_and_field,
    bench_verify_doc_and_field_mask,
    bench_verify_doc_and_wide_field_mask,
);
criterion_main!(benches);

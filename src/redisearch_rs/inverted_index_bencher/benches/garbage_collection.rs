/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::time::Duration;

use criterion::{
    BatchSize, BenchmarkGroup, BenchmarkId, Criterion, criterion_group, criterion_main,
    measurement::WallTime,
};
use ffi::IndexFlags_Index_DocIdsOnly;
use inverted_index::{IndexBlock, InvertedIndex, RSIndexResult, numeric};

#[allow(unused_imports)] // We need this symbol for C binding
use inverted_index_bencher::ResultMetrics_Free;

fn benchmark_garbage_collection(c: &mut Criterion) {
    let mut group = c.benchmark_group("GC");
    group.measurement_time(Duration::from_millis(500));
    group.warm_up_time(Duration::from_millis(200));

    for total_records in [1_000, 100_000, 10_000_000] {
        // Random deletion pattern - 30% of documents deleted based on hash
        benchmark_gc_pattern(&mut group, total_records, "Random 30%", |doc_id| {
            // Simple hash to get pseudo-random but deterministic behavior
            let hash = doc_id.wrapping_mul(2654435761); // golden ratio prime
            hash % 100 >= 30 // 30% deletion rate
        });

        // Age-based deletion - delete the oldest 30% of documents
        benchmark_gc_pattern(&mut group, total_records, "First 30%", |doc_id| {
            doc_id >= (total_records * 30 / 100)
        });

        // Block deletion - delete every 3rd block of 100 documents
        benchmark_gc_pattern(&mut group, total_records, "Every 3rd block", |doc_id| {
            (doc_id / 100) % 3 != 0
        });
    }

    benchmark_large_delta_pattern(&mut group);

    group.finish();
}

fn benchmark_gc_pattern(
    group: &mut BenchmarkGroup<'_, WallTime>,
    total_records: u64,
    pattern_name: &str,
    doc_exist: impl Fn(u64) -> bool,
) {
    group.bench_function(
        BenchmarkId::new("Scan", format!("{pattern_name}/{total_records}")),
        |b| {
            let mut ii = InvertedIndex::<Numeric>::new(IndexFlags_Index_DocIdsOnly);

            for doc_id in 0..total_records {
                ii.add_record(&RSIndexResult::numeric(doc_id as f64 / 10.0).doc_id(doc_id))
                    .unwrap();
            }

            b.iter(|| {
                ii.scan_gc(&doc_exist, None::<fn(&RSIndexResult, &IndexBlock)>)
                    .unwrap();
            })
        },
    );

    group.bench_function(
        BenchmarkId::new("Apply", format!("{pattern_name}/{total_records}")),
        |b| {
            b.iter_batched(
                || {
                    let mut ii = InvertedIndex::<Numeric>::new(IndexFlags_Index_DocIdsOnly);

                    for doc_id in 0..total_records {
                        ii.add_record(&RSIndexResult::numeric(doc_id as f64 / 10.0).doc_id(doc_id))
                            .unwrap();
                    }
                    let scan_deltas = ii
                        .scan_gc(&doc_exist, None::<fn(&RSIndexResult, &IndexBlock)>)
                        .unwrap()
                        .unwrap();

                    (ii, scan_deltas)
                },
                |(mut ii, scan_deltas)| {
                    ii.apply_gc(scan_deltas);
                },
                BatchSize::SmallInput,
            )
        },
    );
}

fn benchmark_large_delta_pattern(group: &mut BenchmarkGroup<'_, WallTime>) {
    // We want the deltas to be 7 bytes long, but 8 bytes after one deletion.
    // To get a 7-byte delta on u64 (8-byte) numbers means we can have a minimum of 1 byte (8 - 7)
    // worth of entries. This is 256 entries. This gives 7-byte deltas between entries, but 8-bytes
    // delta after one entry has been deleted.
    //
    // But we want the maximum number of entries instead, so we double it to 512. This is the
    // minimum number of entries to get a 8-byte delta, but only after two deletes. So we subtract
    // one from it to get 511 entries, which is the maximum number of entries to get a 7-byte delta
    // between entries, but 8-byte delta after one delete.
    let total_records = 511u64;
    let spacing = u64::MAX / total_records;
    let pattern_name = "Large deltas - Random 30%";

    let doc_exist = |doc_id: u64| {
        // Simple hash to get pseudo-random but deterministic behavior
        let hash = doc_id.wrapping_mul(2654435761); // golden ratio prime
        hash % 100 >= 30 // 30% deletion rate
    };

    group.bench_function(
        BenchmarkId::new("Scan", format!("{pattern_name}/{total_records}")),
        |b| {
            let mut ii = InvertedIndex::<Numeric>::new(IndexFlags_Index_DocIdsOnly);

            for i in 0..total_records {
                let doc_id = i * spacing;
                ii.add_record(&RSIndexResult::numeric(doc_id as f64 / 10.0).doc_id(doc_id))
                    .unwrap();
            }

            b.iter(|| {
                ii.scan_gc(&doc_exist, None::<fn(&RSIndexResult, &IndexBlock)>)
                    .unwrap();
            })
        },
    );

    group.bench_function(
        BenchmarkId::new("Apply", format!("{pattern_name}/{total_records}")),
        |b| {
            b.iter_batched(
                || {
                    let mut ii = InvertedIndex::<Numeric>::new(IndexFlags_Index_DocIdsOnly);

                    for i in 0..total_records {
                        let doc_id = i * spacing;
                        ii.add_record(&RSIndexResult::numeric(doc_id as f64 / 10.0).doc_id(doc_id))
                            .unwrap();
                    }
                    let scan_deltas = ii
                        .scan_gc(&doc_exist, None::<fn(&RSIndexResult, &IndexBlock)>)
                        .unwrap()
                        .unwrap();

                    (ii, scan_deltas)
                },
                |(mut ii, scan_deltas)| {
                    ii.apply_gc(scan_deltas);
                },
                BatchSize::SmallInput,
            )
        },
    );
}

criterion_group!(benches, benchmark_garbage_collection);

criterion_main!(benches);

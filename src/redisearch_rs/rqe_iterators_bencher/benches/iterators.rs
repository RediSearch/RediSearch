/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Benchmark iterators

use criterion::{Criterion, criterion_group, criterion_main};
/*
use inverted_index::{
    doc_ids_only::DocIdsOnly,
    fields_offsets::{FieldsOffsets, FieldsOffsetsWide},
    fields_only::{FieldsOnly, FieldsOnlyWide},
    freqs_fields::{FreqsFields, FreqsFieldsWide},
    freqs_offsets::FreqsOffsets,
    full::{Full, FullWide},
    offsets_only::OffsetsOnly,
};
 */
use rqe_iterators_bencher::benchers;

fn benchmark_empty(c: &mut Criterion) {
    let bencher = benchers::empty::Bencher::default();
    bencher.bench(c);
}

fn benchmark_id_list(c: &mut Criterion) {
    let bencher = benchers::id_list::Bencher::default();
    bencher.bench(c);
}

fn benchmark_metric(c: &mut Criterion) {
    let bencher = benchers::metric::Bencher::default();
    bencher.bench(c);
}

fn benchmark_wildcard(c: &mut Criterion) {
    let bencher = benchers::wildcard::Bencher::default();
    bencher.bench(c);
}

fn benchmark_intersection(c: &mut Criterion) {
    let bencher = benchers::intersection::Bencher::default();
    bencher.bench(c);
}

/*
fn benchmark_inverted_index_numeric_full(c: &mut Criterion) {
    let bencher = benchers::inverted_index::NumericFullBencher::default();
    bencher.bench(c);
}

fn benchmark_inverted_index_term_full(c: &mut Criterion) {
    // Run bench with each decoder producing term results.
    benchers::inverted_index::TermFullBencher::<Full>::new(
        "Full",
        ffi::IndexFlags_Index_StoreFreqs
            | ffi::IndexFlags_Index_StoreTermOffsets
            | ffi::IndexFlags_Index_StoreFieldFlags
            | ffi::IndexFlags_Index_StoreByteOffsets,
    )
    .bench(c);
    benchers::inverted_index::TermFullBencher::<FullWide>::new(
        "FullWide",
        ffi::IndexFlags_Index_StoreFreqs
            | ffi::IndexFlags_Index_StoreTermOffsets
            | ffi::IndexFlags_Index_StoreFieldFlags
            | ffi::IndexFlags_Index_StoreByteOffsets
            | ffi::IndexFlags_Index_WideSchema,
    )
    .bench(c);

    benchers::inverted_index::TermFullBencher::<FreqsFields>::new(
        "FreqsFields",
        ffi::IndexFlags_Index_StoreFreqs | ffi::IndexFlags_Index_StoreFieldFlags,
    )
    .bench(c);
    benchers::inverted_index::TermFullBencher::<FreqsFieldsWide>::new(
        "FreqsFieldsWide",
        ffi::IndexFlags_Index_StoreFreqs
            | ffi::IndexFlags_Index_StoreFieldFlags
            | ffi::IndexFlags_Index_WideSchema,
    )
    .bench(c);

    benchers::inverted_index::TermFullBencher::<FieldsOnly>::new(
        "FieldsOnly",
        ffi::IndexFlags_Index_StoreFieldFlags,
    )
    .bench(c);
    benchers::inverted_index::TermFullBencher::<FieldsOnlyWide>::new(
        "FieldsOnlyWide",
        ffi::IndexFlags_Index_StoreFieldFlags | ffi::IndexFlags_Index_WideSchema,
    )
    .bench(c);

    benchers::inverted_index::TermFullBencher::<FieldsOffsets>::new(
        "FieldsOffsets",
        ffi::IndexFlags_Index_StoreTermOffsets | ffi::IndexFlags_Index_StoreFieldFlags,
    )
    .bench(c);
    benchers::inverted_index::TermFullBencher::<FieldsOffsetsWide>::new(
        "FieldsOffsetsWide",
        ffi::IndexFlags_Index_StoreTermOffsets
            | ffi::IndexFlags_Index_StoreFieldFlags
            | ffi::IndexFlags_Index_WideSchema,
    )
    .bench(c);

    benchers::inverted_index::TermFullBencher::<OffsetsOnly>::new(
        "OffsetsOnly",
        ffi::IndexFlags_Index_StoreTermOffsets,
    )
    .bench(c);

    benchers::inverted_index::TermFullBencher::<FreqsOffsets>::new(
        "FreqsOffsets",
        ffi::IndexFlags_Index_StoreFreqs | ffi::IndexFlags_Index_StoreTermOffsets,
    )
    .bench(c);

    benchers::inverted_index::TermFullBencher::<DocIdsOnly>::new(
        "DocIdsOnly",
        ffi::IndexFlags_Index_DocIdsOnly,
    )
    .bench(c);
}
*/

criterion_group!(
    benches,
    benchmark_empty,
    benchmark_id_list,
    benchmark_metric,
    benchmark_wildcard,
    benchmark_intersection,
);

criterion_main!(benches);

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

fn benchmark_optional(c: &mut Criterion) {
    let bencher = benchers::optional::Bencher::default();
    bencher.bench(c);
}

fn benchmark_not_iterator(c: &mut Criterion) {
    let bencher = benchers::not::Bencher::default();
    bencher.bench(c);
}

fn benchmark_inverted_index_numeric(c: &mut Criterion) {
    let bencher = benchers::inverted_index::NumericBencher::default();
    bencher.bench(c);
}

fn benchmark_inverted_index_wildcard(c: &mut Criterion) {
    let bencher = benchers::inverted_index::WildcardBencher::default();
    bencher.bench(c);
}

/*
fn benchmark_inverted_index_term(c: &mut Criterion) {
    // Run bench with each decoder producing term results.
    benchers::inverted_index::TermBencher::<Full>::new(
        "Full",
        ffi::IndexFlags_Index_StoreFreqs
            | ffi::IndexFlags_Index_StoreTermOffsets
            | ffi::IndexFlags_Index_StoreFieldFlags
            | ffi::IndexFlags_Index_StoreByteOffsets,
    )
    .bench(c);
    benchers::inverted_index::TermBencher::<FullWide>::new(
        "FullWide",
        ffi::IndexFlags_Index_StoreFreqs
            | ffi::IndexFlags_Index_StoreTermOffsets
            | ffi::IndexFlags_Index_StoreFieldFlags
            | ffi::IndexFlags_Index_StoreByteOffsets
            | ffi::IndexFlags_Index_WideSchema,
    )
    .bench(c);

    benchers::inverted_index::TermBencher::<FreqsFields>::new(
        "FreqsFields",
        ffi::IndexFlags_Index_StoreFreqs | ffi::IndexFlags_Index_StoreFieldFlags,
    )
    .bench(c);
    benchers::inverted_index::TermBencher::<FreqsFieldsWide>::new(
        "FreqsFieldsWide",
        ffi::IndexFlags_Index_StoreFreqs
            | ffi::IndexFlags_Index_StoreFieldFlags
            | ffi::IndexFlags_Index_WideSchema,
    )
    .bench(c);

    benchers::inverted_index::TermBencher::<FieldsOnly>::new(
        "FieldsOnly",
        ffi::IndexFlags_Index_StoreFieldFlags,
    )
    .bench(c);
    benchers::inverted_index::TermBencher::<FieldsOnlyWide>::new(
        "FieldsOnlyWide",
        ffi::IndexFlags_Index_StoreFieldFlags | ffi::IndexFlags_Index_WideSchema,
    )
    .bench(c);

    benchers::inverted_index::TermBencher::<FieldsOffsets>::new(
        "FieldsOffsets",
        ffi::IndexFlags_Index_StoreTermOffsets | ffi::IndexFlags_Index_StoreFieldFlags,
    )
    .bench(c);
    benchers::inverted_index::TermBencher::<FieldsOffsetsWide>::new(
        "FieldsOffsetsWide",
        ffi::IndexFlags_Index_StoreTermOffsets
            | ffi::IndexFlags_Index_StoreFieldFlags
            | ffi::IndexFlags_Index_WideSchema,
    )
    .bench(c);

    benchers::inverted_index::TermBencher::<OffsetsOnly>::new(
        "OffsetsOnly",
        ffi::IndexFlags_Index_StoreTermOffsets,
    )
    .bench(c);

    benchers::inverted_index::TermBencher::<FreqsOffsets>::new(
        "FreqsOffsets",
        ffi::IndexFlags_Index_StoreFreqs | ffi::IndexFlags_Index_StoreTermOffsets,
    )
    .bench(c);

    benchers::inverted_index::TermBencher::<DocIdsOnly>::new(
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
    benchmark_not_iterator,
    benchmark_wildcard,
    benchmark_intersection,
    benchmark_optional,
    benchmark_inverted_index_numeric,
    benchmark_inverted_index_wildcard,
    //benchmark_inverted_index_term,
);

criterion_main!(benches);

/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{hint::black_box, marker::PhantomData};

use criterion::{BenchmarkGroup, Criterion, measurement::Measurement};
use inverted_index::{
    DecodedBy, Encoder, HasInnerIndex, InvertedIndex, RSIndexResult, RSOffsetSlice, TermDecoder,
    opaque::OpaqueEncoding,
};
use query_term::RSQueryTerm;
use rqe_iterators::{NoOpChecker, RQEIterator, SkipToOutcome, inverted_index::Term};
use rqe_iterators_test_utils::MockContext;

use crate::ffi as bench_ffi;

use super::{INDEX_SIZE, SKIP_TO_STEP, SPARSE_DELTA, benchmark_group};

pub struct TermBencher<E> {
    /// Name of the benchmark group
    group_name: String,
    /// Inverted index flags to use for the benchmark
    ii_flags: u32,
    /// The offsets used in records, need to be kept alive for the duration of the benchmark
    offsets: Vec<u8>,
    /// context used to create iterators. We do not actually use it so its `max_doc_id` and `num_docs` params are irrelevant.
    mock_ctx: MockContext,
    /// Needed to carry the encoder used by the benchmark.
    _phantom_enc: PhantomData<E>,
}

impl<E> TermBencher<E>
where
    E: Encoder + DecodedBy + OpaqueEncoding,
    E::Decoder: TermDecoder,
    E::Storage: HasInnerIndex<E>,
{
    pub fn new(decoder_name: &str, ii_flags: u32) -> Self {
        Self {
            group_name: format!("Term - {decoder_name}"),
            ii_flags,
            offsets: vec![0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
            mock_ctx: MockContext::new(0, 0),
            _phantom_enc: PhantomData,
        }
    }

    pub fn bench(&self, c: &mut Criterion) {
        self.read_dense(c);
        self.skip_to_dense(c);
        self.skip_to_sparse(c);
    }

    fn read_dense(&self, c: &mut Criterion) {
        let mut group = benchmark_group(c, &self.group_name, "Read Dense");
        self.c_read_dense(&mut group);
        self.rust_read_dense(&mut group);
        group.finish();
    }

    fn skip_to_dense(&self, c: &mut Criterion) {
        let mut group = benchmark_group(c, &self.group_name, "SkipTo Dense");
        self.c_skip_to_dense(&mut group);
        self.rust_skip_to_dense(&mut group);
        group.finish();
    }

    fn skip_to_sparse(&self, c: &mut Criterion) {
        let mut group = benchmark_group(c, &self.group_name, "SkipTo Sparse");
        self.c_skip_to_sparse(&mut group);
        self.rust_skip_to_sparse(&mut group);
        group.finish();
    }

    fn c_index(&self, sparse: bool) -> bench_ffi::InvertedIndex {
        let ii = bench_ffi::InvertedIndex::new(self.ii_flags);
        let delta = if sparse { SPARSE_DELTA } else { 1 };
        for doc_id in 1..INDEX_SIZE {
            let actual_doc_id = doc_id * delta;
            ii.write_term_entry(actual_doc_id, 1, 1, None, &self.offsets);
        }
        ii
    }

    fn rust_index(&self, sparse: bool) -> InvertedIndex<E> {
        let mut ii = InvertedIndex::<E>::new(self.ii_flags);
        let delta = if sparse { SPARSE_DELTA } else { 1 };
        for doc_id in 1..INDEX_SIZE {
            let actual_doc_id = doc_id * delta;
            let record = RSIndexResult::with_term(
                None,
                RSOffsetSlice::from_slice(&self.offsets),
                actual_doc_id,
                1,
                1,
            );
            ii.add_record(&record).expect("failed to add record");
        }
        ii
    }

    fn c_read_dense<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        group.bench_function("C", |b| {
            b.iter_batched_ref(
                || self.c_index(false),
                |ii| {
                    let it = ii.iterator_term();
                    while it.read() == ::ffi::IteratorStatus_ITERATOR_OK {
                        black_box(it.current());
                    }
                    it.free();
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    fn rust_read_dense<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || self.rust_index(false),
                |ii| {
                    // SAFETY: sctx points to a valid, zeroed RedisSearchCtx with a valid spec.
                    let mut it = unsafe {
                        Term::new(
                            ii.reader(),
                            self.mock_ctx.sctx(),
                            RSQueryTerm::new("term", 1, 0),
                            1.0,
                            NoOpChecker,
                        )
                    };
                    while let Ok(Some(current)) = it.read() {
                        black_box(current);
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    fn c_skip_to_dense<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        group.bench_function("C", |b| {
            b.iter_batched_ref(
                || self.c_index(false),
                |ii| {
                    let it = ii.iterator_term();
                    while it.skip_to(it.last_doc_id() + SKIP_TO_STEP)
                        != ::ffi::IteratorStatus_ITERATOR_EOF
                    {
                        black_box(it.current());
                    }
                    it.free();
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    fn c_skip_to_sparse<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        group.bench_function("C", |b| {
            b.iter_batched_ref(
                || self.c_index(true),
                |ii| {
                    let it = ii.iterator_term();
                    while it.skip_to(it.last_doc_id() + SKIP_TO_STEP)
                        != ::ffi::IteratorStatus_ITERATOR_EOF
                    {
                        black_box(it.current());
                    }
                    it.free();
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    fn rust_skip_to_dense<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || self.rust_index(false),
                |ii| {
                    // SAFETY: sctx points to a valid, zeroed RedisSearchCtx with a valid spec.
                    let mut it = unsafe {
                        Term::new(
                            ii.reader(),
                            self.mock_ctx.sctx(),
                            RSQueryTerm::new("term", 1, 0),
                            1.0,
                            NoOpChecker,
                        )
                    };
                    while let Ok(Some(outcome)) = it.skip_to(it.last_doc_id() + SKIP_TO_STEP) {
                        match outcome {
                            SkipToOutcome::Found(current) | SkipToOutcome::NotFound(current) => {
                                black_box(current);
                            }
                        }
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }

    fn rust_skip_to_sparse<M: Measurement>(&self, group: &mut BenchmarkGroup<'_, M>) {
        group.bench_function("Rust", |b| {
            b.iter_batched_ref(
                || self.rust_index(true),
                |ii| {
                    // SAFETY: sctx points to a valid, zeroed RedisSearchCtx with a valid spec.
                    let mut it = unsafe {
                        Term::new(
                            ii.reader(),
                            self.mock_ctx.sctx(),
                            RSQueryTerm::new("term", 1, 0),
                            1.0,
                            NoOpChecker,
                        )
                    };
                    while let Ok(Some(outcome)) = it.skip_to(it.last_doc_id() + SKIP_TO_STEP) {
                        match outcome {
                            SkipToOutcome::Found(current) | SkipToOutcome::NotFound(current) => {
                                black_box(current);
                            }
                        }
                    }
                },
                criterion::BatchSize::SmallInput,
            );
        });
    }
}

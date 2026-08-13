/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Commit-phase benchmarks: `TagIndex::commit` against C's `TagIndex_Commit`.
//!
//! In memory mode the commit phase *is* the suffix-trie add. Both
//! implementations insert into the values trie only in disk mode — C gates it on
//! `idx->diskSpec`, Rust on its `TagIndexMode` — so with the suffix trie disabled
//! this benchmark would time an empty loop. Every configuration therefore runs
//! `with_suffix = true`.
//!
//! This is where the two implementations differ most. Per tag, both insert one
//! entry per suffix, but C's `addSuffixTrieMap` does a `TrieMap_Find` and then a
//! `TrieMap_Add` on a miss — two descents per suffix — plus an `rm_calloc` for
//! the node payload and a separate allocation for its term array. Rust's
//! `TagSuffixIndex::add` resolves each suffix with a single `insert_with` and
//! keeps the term references in an inline `ThinVec`. The cost is proportional to
//! the tag length, which makes `tag_len` the axis to watch.
//!
//! Every timed commit inserts terms the suffix trie does not yet hold, so the
//! full per-suffix insert runs. Re-committing terms already present is not
//! benchmarked: both sides then bail out after one lookup, and C's lookup is
//! `triemap_ffi`, so that path compares `trie_rs` against `trie_rs` across an FFI
//! boundary rather than comparing the two insert algorithms.

use std::hint::black_box;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rand::{SeedableRng as _, rngs::StdRng};
use tag_index_bencher::{
    CTagIndex, TagCorpus, TagCorpusInput, build_c, build_rust, commit_c, commit_rust, zeroed_stats,
};

/// Number of terms committed per timed iteration, and the suffix trie's width.
const UNIQUE_TAGS: &[usize] = &[1_000, 100_000];
/// Suffixes inserted per term, so the dominant axis here.
const TAG_LENS: &[usize] = &[8, 48];

/// Seed fixed so every run commits the same terms.
const SEED: u64 = 42;

fn commit_suffix(c: &mut Criterion) {
    let mut group = c.benchmark_group("commit/suffix");

    for &unique_tags in UNIQUE_TAGS {
        for &tag_len in TAG_LENS {
            let mut rng = StdRng::seed_from_u64(SEED);
            let corpus = TagCorpus::generate(
                TagCorpusInput {
                    unique_tags,
                    tag_len_mean: tag_len,
                    tag_len_variation: 2,
                    shared_prefix_depth: 4,
                    prefix_pool: 16,
                    alphabet: 26,
                },
                &mut rng,
            );

            // The same allocations feed both arms.
            let rust_tags = corpus.rust_tags();
            let c_tags = corpus.c_tags();

            let parameters = format!("unique_tags={unique_tags}/tag_len={tag_len}");
            group.throughput(Throughput::Elements(unique_tags as u64));

            group.bench_function(
                BenchmarkId::from_parameter(format!("{parameters}/lang=Rust")),
                |b| {
                    b.iter_batched(
                        // The fresh index is the point, not boilerplate: reusing
                        // one across iterations would make every iteration after
                        // the first hit the dedup early-exit, measuring a lookup
                        // instead of the per-suffix insert.
                        || build_rust(true),
                        |mut idx| {
                            black_box(commit_rust(&mut idx, black_box(&rust_tags)));
                            idx
                        },
                        BatchSize::LargeInput,
                    );
                },
            );

            group.bench_function(
                BenchmarkId::from_parameter(format!("{parameters}/lang=C")),
                |b| {
                    b.iter_batched(
                        // Fresh per iteration, for the reason given above.
                        || build_c(true),
                        |idx: CTagIndex| {
                            let mut stats = zeroed_stats();
                            // SAFETY: `c_tags` holds NUL-terminated tags owned by
                            // `corpus`, which outlives the call, and `stats` is a
                            // live local.
                            unsafe { commit_c(&idx, black_box(&c_tags), &mut stats) };
                            black_box(stats);
                            idx
                        },
                        BatchSize::LargeInput,
                    );
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, commit_suffix);
criterion_main!(benches);

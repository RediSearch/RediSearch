/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Write-path benchmarks: `TagIndex::index` against C's `TagIndex_Index`.
//!
//! Each iteration writes the whole document set into an index that starts empty,
//! which is where the two implementations diverge: on a tag the values trie does
//! not hold yet, C's `tagIndex_Put` does a `TrieMap_Find` and then a
//! `TrieMap_Add` — two descents — where Rust's `write_postings` resolves the slot
//! with a single `insert_with`.
//!
//! `unique_tags` is therefore the miss-rate axis, not just the trie's width: with
//! 1 000 tags the 10 000–80 000 writes are nearly all appends to a tag that is
//! already there, while with 100 000 they are mostly first writes. A separate
//! pre-populated "all appends" group used to sit alongside this one and was
//! removed: on the narrow corpus it reproduced these numbers, and on the wide one
//! its per-iteration repopulation — untimed, but hard on the cache and the
//! allocator — widened the confidence intervals about twentyfold.
//!
//! Neither implementation touches the suffix trie here — that is `commit`'s job,
//! benchmarked in `commit.rs` — so there is no suffix axis.

use std::hint::black_box;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rand::{SeedableRng as _, rngs::StdRng};
use tag_index_bencher::{
    CTagIndex, DocsInput, TagCorpus, TagCorpusInput, build_c, build_rust, index_c, index_rust,
    zeroed_stats,
};

/// Trie width, and with it the share of writes that create a tag rather than
/// append to one.
const UNIQUE_TAGS: &[usize] = &[1_000, 100_000];
/// Trie depth.
const TAG_LENS: &[usize] = &[8, 48];
/// Single- versus multi-value tag fields.
const TAGS_PER_DOC: &[usize] = &[1, 8];
/// Documents written per timed iteration.
const DOC_COUNT: usize = 10_000;

/// Seed fixed so every configuration benchmarks the same corpus on every run,
/// and so a regression hunt compares like with like.
const SEED: u64 = 42;

struct Config {
    corpus: TagCorpus,
    docs_input: DocsInput,
    unique_tags: usize,
    tag_len: usize,
    tags_per_doc: usize,
}

fn configs() -> Vec<Config> {
    let mut configs = Vec::new();
    for &unique_tags in UNIQUE_TAGS {
        for &tag_len in TAG_LENS {
            for &tags_per_doc in TAGS_PER_DOC {
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
                configs.push(Config {
                    corpus,
                    docs_input: DocsInput {
                        count: DOC_COUNT,
                        start_doc_id_from: 1,
                        tags_per_doc_mean: tags_per_doc,
                        tags_per_doc_variation: 0,
                    },
                    unique_tags,
                    tag_len,
                    tags_per_doc,
                });
            }
        }
    }
    configs
}

impl Config {
    /// The benchmark id, minus the `lang=` pivot the comparison is paired on.
    fn parameters(&self) -> String {
        format!(
            "unique_tags={}/tag_len={}/tags_per_doc={}",
            self.unique_tags, self.tag_len, self.tags_per_doc
        )
    }
}

fn index_from_scratch(c: &mut Criterion) {
    let mut group = c.benchmark_group("index/from_scratch");

    for config in configs() {
        let mut rng = StdRng::seed_from_u64(SEED);
        let docs = config.corpus.docs(config.docs_input, &mut rng);

        // Both projections point at the corpus' own NUL-terminated allocations,
        // so the two arms write byte-identical tags. Built here, outside the
        // timed loop.
        let rust_docs = config.corpus.rust_docs(&docs);
        let c_docs = config.corpus.c_docs(&docs);

        let tag_writes: u64 = docs.iter().map(|doc| doc.tags.len() as u64).sum();
        group.throughput(Throughput::Elements(tag_writes));

        let parameters = config.parameters();

        group.bench_function(
            BenchmarkId::from_parameter(format!("{parameters}/lang=Rust")),
            |b| {
                b.iter_batched(
                    || build_rust(false),
                    |mut idx| {
                        for (doc_id, tags) in &rust_docs {
                            black_box(index_rust(&mut idx, tags, black_box(*doc_id)));
                        }
                        // Returned so the drop lands in criterion's teardown
                        // rather than inside the measurement.
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
                    || build_c(false),
                    |idx: CTagIndex| {
                        // The C write path reports its accounting through this
                        // out-parameter, where Rust returns a
                        // `WritePostingsDelta`; both are black-boxed so neither
                        // is optimized away.
                        let mut stats = zeroed_stats();
                        for (doc_id, tags) in &c_docs {
                            // SAFETY: the tags are NUL-terminated and owned by
                            // `config.corpus`.
                            let ok = unsafe { index_c(&idx, tags, black_box(*doc_id), &mut stats) };
                            black_box(ok);
                        }
                        black_box(stats);
                        idx
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group!(benches, index_from_scratch);
criterion_main!(benches);

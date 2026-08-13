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
//! Both groups exercise the same call; they differ in whether the tag being
//! written is already in the values trie, which is where the two implementations
//! diverge. C's `tagIndex_Put` does a `TrieMap_Find` and then, on a miss, a
//! `TrieMap_Add` — two descents — where Rust's `write_postings` resolves the slot
//! with a single `insert_with`.
//!
//! - `index/fresh` starts from an empty index, so `unique_tags` sets the miss
//!   rate: a wide corpus means most writes create a tag, a narrow one means most
//!   append to an existing one.
//! - `index/append` starts from an index already holding every tag, so every
//!   write is a hit.
//!
//! Neither implementation touches the suffix trie here — that is `commit`'s job,
//! benchmarked in `commit.rs` — so there is no suffix axis.

use std::hint::black_box;

use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use rand::{SeedableRng as _, rngs::StdRng};
use rqe_core::DocId;
use tag_index_bencher::{
    CTagIndex, DocsInput, TagCorpus, TagCorpusInput, build_c, build_rust, index_c, index_rust,
    populate_c, populate_rust, zeroed_stats,
};

/// Trie width, and in the `fresh` group the miss rate.
const UNIQUE_TAGS: &[usize] = &[1_000, 100_000];
/// Trie depth.
const TAG_LENS: &[usize] = &[8, 48];
/// Single- versus multi-value tag fields.
const TAGS_PER_DOC: &[usize] = &[1, 8];
/// Documents written per timed iteration.
const DOC_COUNT: usize = 10_000;
/// Reserved for the `append` group's pre-population, so the workload's ascending
/// document ids all sit above it.
const WARMUP_DOC_ID: DocId = 1;

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
                        start_doc_id_from: WARMUP_DOC_ID + 1,
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

/// Whether the timed writes land on tags the index already holds.
#[derive(Clone, Copy)]
enum Regime {
    /// Empty index: whether a write is a miss depends on the corpus width.
    Fresh,
    /// Every tag pre-inserted, so every write appends.
    Append,
}

impl Regime {
    const fn group(self) -> &'static str {
        match self {
            Self::Fresh => "index/fresh",
            Self::Append => "index/append",
        }
    }
}

/// Below criterion's default of 100. The `append` group rebuilds a fully
/// populated index in every `iter_batched` setup call: untimed, but real
/// wall-clock, and at the widest corpus it costs about as much as the iteration
/// it prepares. Both groups use the same count so neither gets a statistically
/// kinder treatment than the other.
const SAMPLE_SIZE: usize = 20;

fn bench(c: &mut Criterion, regime: Regime) {
    let mut group = c.benchmark_group(regime.group());
    group.sample_size(SAMPLE_SIZE);

    for config in configs() {
        let mut rng = StdRng::seed_from_u64(SEED);
        let docs = config.corpus.docs(config.docs_input, &mut rng);

        // Both projections point at the corpus' own NUL-terminated allocations,
        // so the two arms write byte-identical tags. Built here, outside the
        // timed loop.
        let rust_docs = config.corpus.rust_docs(&docs);
        let c_docs = config.corpus.c_docs(&docs);

        // The `append` group's pre-population: one document carrying every tag,
        // below the workload's id range.
        let rust_warmup = vec![(WARMUP_DOC_ID, config.corpus.rust_tags())];
        let c_warmup = vec![(WARMUP_DOC_ID, config.corpus.c_tags())];

        let tag_writes: u64 = docs.iter().map(|doc| doc.tags.len() as u64).sum();
        group.throughput(Throughput::Elements(tag_writes));

        let parameters = config.parameters();

        group.bench_function(
            BenchmarkId::from_parameter(format!("{parameters}/lang=Rust")),
            |b| {
                b.iter_batched(
                    || match regime {
                        Regime::Fresh => build_rust(false),
                        Regime::Append => populate_rust(false, &rust_warmup),
                    },
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
                    || match regime {
                        Regime::Fresh => build_c(false),
                        // SAFETY: every pointer in `c_warmup` addresses a
                        // NUL-terminated tag owned by `config.corpus`, which
                        // outlives the index built here.
                        Regime::Append => unsafe { populate_c(false, &c_warmup) },
                    },
                    |idx: CTagIndex| {
                        // The C write path reports its accounting through this
                        // out-parameter, where Rust returns a
                        // `WritePostingsDelta`; both are black-boxed so neither
                        // is optimized away.
                        let mut stats = zeroed_stats();
                        for (doc_id, tags) in &c_docs {
                            // SAFETY: the tags are NUL-terminated and owned by
                            // `config.corpus`, which outlives this call, and
                            // `stats` is a live local.
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

fn index_fresh(c: &mut Criterion) {
    bench(c, Regime::Fresh);
}

fn index_append(c: &mut Criterion) {
    bench(c, Regime::Append);
}

criterion_group!(benches, index_fresh, index_append);
criterion_main!(benches);

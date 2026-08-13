/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Affix-expansion benchmarks: `TagIndex::suffix_expand` against C's
//! `TagIndex_GetSuffixMatches` and `TagIndex_GetSuffixWildcardMatches`.
//!
//! The two arms return results in different shapes, so the measurement covers
//! expansion *and* term enumeration on both sides. C builds an eager `arrayof`
//! and leaves its caller in `src/query.c` to walk it — `arr[i][j]`, with a
//! `strlen` per term — while Rust yields each term lazily, already
//! NUL-terminated. Timing C's array construction against an unconsumed Rust
//! iterator would compare the two halves of different jobs, so
//! [`consume_suffix_matches`] mirrors query.c's walk and the Rust arm is driven to
//! exhaustion.
//!
//! Three more fairness details:
//!
//! - The wildcard arm builds its [`SuffixWildcardPattern`] *inside* the timed
//!   closure, because C picks its anchor token inside
//!   `GetList_SuffixTrieMap_Wildcard` (via `Suffix_ChooseToken`). Hoisting it out
//!   would hand Rust a free head start.
//! - Both arms run without a deadline — `skipTimeoutChecks = true` for C, `None`
//!   for Rust — so C's `TIMEOUT_COUNTER_LIMIT` probe cadence stays out of the
//!   numbers.
//! - The suffix and contains arms are **uncapped**, so both walk the whole match
//!   set. Capping the consumption while C had already materialised every match
//!   made the two arms do wildly different amounts of work — C completing the
//!   trie walk, Rust's lazy iterator abandoning it after
//!   `MAX_PREFIX_EXPANSIONS` terms — and turned a per-term measurement into a
//!   two-orders-of-magnitude measurement of laziness. Production does cap, and
//!   there the port's laziness is a genuine advantage; it just isn't what these
//!   numbers are for. The wildcard arm keeps its cap because both
//!   implementations apply it during expansion.
//!
//! Only the commit phase is needed to set up: expansion reads the suffix trie,
//! which `commit` alone fills.

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use rand::{SeedableRng as _, rngs::StdRng};
use tag_index::{SuffixQuery, SuffixWildcardPattern};
use tag_index_bencher::{
    BAD_POINTER, CTagIndex, ExpandMode, MAX_PREFIX_EXPANSIONS, NO_TIMEOUT, Selectivity, TagCorpus,
    TagCorpusInput, build_c, build_rust, commit_c, commit_rust, consume_suffix_matches,
    consume_wildcard_matches, zeroed_stats,
};

/// Suffix-trie width.
const UNIQUE_TAGS: &[usize] = &[1_000, 100_000];
/// Suffix-trie depth, and how many suffix keys each term contributes.
const TAG_LENS: &[usize] = &[8, 48];

/// Seed fixed so every run expands against the same trie.
const SEED: u64 = 42;

/// Build the suffix-trie-backed pair of indexes for a corpus, plus the pattern
/// each mode/selectivity combination expands.
struct Fixture {
    corpus: TagCorpus,
    rust_index: tag_index::TagIndex,
    c_index: CTagIndex,
}

impl Fixture {
    fn new(unique_tags: usize, tag_len: usize) -> Self {
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

        let mut rust_index = build_rust(true);
        commit_rust(&mut rust_index, &corpus.rust_tags());

        let c_index = build_c(true);
        let mut stats = zeroed_stats();
        // SAFETY: `c_tags` holds NUL-terminated tags owned by `corpus`, which
        // outlives this call, and `stats` is a live local. (The suffix trie keeps
        // its own copies of the terms, so nothing borrows from `corpus`
        // afterwards.)
        unsafe { commit_c(&c_index, &corpus.c_tags(), &mut stats) };

        Self {
            corpus,
            rust_index,
            c_index,
        }
    }
}

fn bench_mode(c: &mut Criterion, mode: ExpandMode) {
    let mut group = c.benchmark_group(format!("suffix_expand/{}", mode.as_str()));

    for &unique_tags in UNIQUE_TAGS {
        for &tag_len in TAG_LENS {
            let fixture = Fixture::new(unique_tags, tag_len);

            for selectivity in [Selectivity::Few, Selectivity::Many] {
                let pattern = fixture.corpus.pattern_for(mode, selectivity);
                let parameters = format!(
                    "unique_tags={unique_tags}/tag_len={tag_len}/matches={}",
                    selectivity.as_str()
                );

                group.bench_function(
                    BenchmarkId::from_parameter(format!("{parameters}/lang=Rust")),
                    |b| {
                        b.iter(|| match mode {
                            ExpandMode::Suffix => consume_rust(
                                &fixture.rust_index,
                                SuffixQuery::Suffix(black_box(&pattern)),
                            ),
                            ExpandMode::Contains => consume_rust(
                                &fixture.rust_index,
                                SuffixQuery::Contains(black_box(&pattern)),
                            ),
                            ExpandMode::Wildcard => {
                                // Inside the timed closure on purpose: C chooses
                                // its anchor token inside the call below.
                                let prepared = SuffixWildcardPattern::new(black_box(&pattern))
                                    .expect("benched patterns always carry an anchor token");
                                let query = SuffixQuery::Wildcard {
                                    pattern: &prepared,
                                    max_prefix_expansions: MAX_PREFIX_EXPANSIONS as u64,
                                };
                                // Already capped during expansion, as C is.
                                let mut visited = 0usize;
                                for term in fixture.rust_index.suffix_expand(query, None) {
                                    black_box(term);
                                    visited += 1;
                                }
                                visited
                            }
                        });
                    },
                );

                group.bench_function(
                    BenchmarkId::from_parameter(format!("{parameters}/lang=C")),
                    |b| {
                        b.iter(|| match mode {
                            ExpandMode::Suffix | ExpandMode::Contains => {
                                // `prefix = true` is C's contains form: it
                                // prefix-iterates the suffix trie instead of
                                // looking up one node.
                                let prefix = matches!(mode, ExpandMode::Contains);
                                // SAFETY: the index is alive for the whole
                                // benchmark, `pattern` outlives the call, and the
                                // returned array is handed straight to
                                // `consume_suffix_matches`, which frees it.
                                let arr = unsafe {
                                    ffi::TagIndex_GetSuffixMatches(
                                        fixture.c_index.as_ptr(),
                                        black_box(pattern.as_ptr()).cast(),
                                        pattern.len() as u32,
                                        prefix,
                                        NO_TIMEOUT,
                                        true,
                                    )
                                };
                                // SAFETY: `arr` is what the call just returned,
                                // not yet freed, and its inner arrays borrow from
                                // a suffix trie that is still alive.
                                unsafe { consume_suffix_matches(arr) }
                            }
                            ExpandMode::Wildcard => {
                                // SAFETY: as above; the anchor-token choice
                                // happens inside this call, which is why the Rust
                                // arm prepares its pattern inside the closure.
                                let arr = unsafe {
                                    ffi::TagIndex_GetSuffixWildcardMatches(
                                        fixture.c_index.as_ptr(),
                                        black_box(pattern.as_ptr()).cast(),
                                        pattern.len() as u32,
                                        NO_TIMEOUT,
                                        MAX_PREFIX_EXPANSIONS as i64,
                                        true,
                                    )
                                };
                                assert_ne!(
                                    arr as usize, BAD_POINTER,
                                    "benched patterns always carry an anchor token"
                                );
                                // SAFETY: `arr` is the array just returned and is
                                // not the sentinel, as asserted above.
                                unsafe { consume_wildcard_matches(arr) }
                            }
                        });
                    },
                );
            }
        }
    }

    group.finish();
}

/// Drive a suffix or contains expansion to exhaustion, matching the full walk
/// `GetList_SuffixTrieMap` has already completed by the time it returns.
fn consume_rust(index: &tag_index::TagIndex, query: SuffixQuery<'_>) -> usize {
    let mut visited = 0usize;
    for term in index.suffix_expand(query, None) {
        black_box(term);
        visited += 1;
    }
    visited
}

fn suffix(c: &mut Criterion) {
    bench_mode(c, ExpandMode::Suffix);
}

fn contains(c: &mut Criterion) {
    bench_mode(c, ExpandMode::Contains);
}

fn wildcard(c: &mut Criterion) {
    bench_mode(c, ExpandMode::Wildcard);
}

criterion_group!(benches, suffix, contains, wildcard);
criterion_main!(benches);

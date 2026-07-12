/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Case-fold cost of the two case-insensitive query paths, [`SpellCheckDictionary::contains`]
//! and [`SpellCheckDictionary::fuzzy_matches`].
//!
//! Each is run twice over the same dictionary: once with an already-lowercase
//! needle, once with an upper-cased needle that lowers to the identical bytes.
//! The folded needle drives the same streaming automaton either way, so the
//! trie descent — and thus all work past folding — is unchanged. The
//! lower-vs-mixed delta is therefore purely the cost of case-folding the query.
//!
//! CAVEAT — unlike the suffix trie's `_Mixed` bench rows, this is NOT a
//! copy-on-write (COW) allocation signal. Both query paths fold the needle
//! with `string_utils::unicode_tolower_capped`, which *always* allocates a
//! folded `String`; there is no borrow-when-already-lowercase fast path to
//! bypass. So the lower/mixed delta here reflects `char::to_lowercase`
//! mapping CPU only, not an avoided allocation, and is expected to be small.
//! Should a borrowing `unicode_tolower_cow` ever be adopted here, this same
//! bench would begin to capture the real COW delta.
//!
//! Stored terms are folded codepoint-by-codepoint during the automaton walk
//! itself (with non-matching subtrees pruned), so per-query cost tracks the
//! visited portion of the trie, not the stored-term count.

use std::hint::black_box;

use criterion::{Criterion, criterion_group, criterion_main};
use spellcheck_dictionary::SpellCheckDictionary;

/// Stored-term count. Matches the suffix bench corpus size so the two are
/// roughly comparable in scale.
const NUM_WORDS: usize = 5000;

/// Deterministic, dependency-free PRNG (SplitMix64) so the corpus is stable
/// across runs without pulling in `rand`.
struct SplitMix64(u64);

impl SplitMix64 {
    const fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform `u32` in `[0, bound)`.
    fn below(&mut self, bound: u32) -> u32 {
        (self.next_u64() % u64::from(bound)) as u32
    }
}

/// Deterministic lowercase-ASCII corpus of unique words, lengths 3..=12.
fn make_corpus() -> Vec<String> {
    let mut rng = SplitMix64(42);
    let mut seen = std::collections::HashSet::new();
    let mut out = Vec::with_capacity(NUM_WORDS);
    while out.len() < NUM_WORDS {
        let n = 3 + rng.below(10) as usize; // 3..=12
        let word: String = (0..n)
            .map(|_| (b'a' + rng.below(26) as u8) as char)
            .collect();
        if seen.insert(word.clone()) {
            out.push(word);
        }
    }
    out
}

/// Full corpus words used as query needles, sampled sparsely. `contains`
/// matches whole terms case-insensitively, so a needle must be an entire stored
/// word (not a substring) to actually hit.
fn make_needles(corpus: &[String]) -> Vec<String> {
    corpus.iter().step_by(313).take(16).cloned().collect()
}

/// Upper-cased copies. These lower to the exact same bytes as the originals, so
/// the match set is identical — only the folding work differs.
fn upper_all(words: &[String]) -> Vec<String> {
    words.iter().map(|w| w.to_ascii_uppercase()).collect()
}

fn build(corpus: &[String]) -> SpellCheckDictionary {
    let mut dict = SpellCheckDictionary::new();
    for w in corpus {
        dict.add(w);
    }
    dict
}

fn bench_contains(c: &mut Criterion) {
    let corpus = make_corpus();
    let dict = build(&corpus);
    let lower = make_needles(&corpus);
    let mixed = upper_all(&lower);

    let mut group = c.benchmark_group("contains");
    group.bench_function("lower", |b| {
        b.iter(|| {
            let mut hits = 0usize;
            for nd in &lower {
                hits += usize::from(dict.contains(black_box(nd)));
            }
            black_box(hits)
        })
    });
    group.bench_function("mixed", |b| {
        b.iter(|| {
            let mut hits = 0usize;
            for nd in &mixed {
                hits += usize::from(dict.contains(black_box(nd)));
            }
            black_box(hits)
        })
    });
    group.finish();
}

fn bench_fuzzy(c: &mut Criterion) {
    let corpus = make_corpus();
    let dict = build(&corpus);
    let lower = make_needles(&corpus);
    let mixed = upper_all(&lower);
    let max_dist = 1;

    let mut group = c.benchmark_group("fuzzy_matches");
    group.bench_function("lower", |b| {
        b.iter(|| {
            let mut matches = 0usize;
            for nd in &lower {
                matches += dict.fuzzy_matches(black_box(nd), max_dist).count();
            }
            black_box(matches)
        })
    });
    group.bench_function("mixed", |b| {
        b.iter(|| {
            let mut matches = 0usize;
            for nd in &mixed {
                matches += dict.fuzzy_matches(black_box(nd), max_dist).count();
            }
            black_box(matches)
        })
    });
    group.finish();
}

criterion_group!(case_fold, bench_contains, bench_fuzzy);
criterion_main!(case_fold);

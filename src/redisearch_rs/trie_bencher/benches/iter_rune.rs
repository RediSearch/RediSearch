/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Iteration benches for [`trie_rs::RuneTrieMap`].
//!
//! Mirrors [`benches/iter.rs`] but the type under test is the rune-keyed
//! wrapper used by the suffix index after the phase 1 migration. The C
//! baseline (rune-keyed `Trie`) is intentionally not wired here yet; the
//! Rust-only numbers establish a stable reference point for any later
//! C comparison.

use criterion::{Criterion, criterion_group, criterion_main};
use trie_bencher::RuneOperationBencher;
use trie_bencher::corpus::CorpusType;

fn iter_rune_benches_wiki1k(c: &mut Criterion) {
    let corpus = CorpusType::RedisBench1kWiki;
    let terms = corpus.create_terms(true);

    let bencher = RuneOperationBencher::new("Wiki-1K-rune".to_owned(), terms, None);
    bencher.prefixed_iter_group(c, "Aba", "Prefixed iter");
    bencher.wildcard_group(c, "Ab*");
    // Fixed length pattern ("Apollo " + two arbitrary runes).
    bencher.wildcard_group(c, "Apollo ??");
}

fn iter_rune_benches_gutenberg(c: &mut Criterion) {
    let corpus = CorpusType::GutenbergEbook(true);
    let terms = corpus.create_terms(true);

    let bencher = RuneOperationBencher::new("Gutenberg-rune".to_owned(), terms, None);

    // Wide prefix scan — exercises sibling branching at the root level.
    bencher.prefixed_iter_group(c, "ever", "Prefixed iter");

    // Trailing star — backtracking-driven, de facto suffix matching.
    bencher.wildcard_group(c, "*ly");

    // Range across distant lex bounds.
    bencher.range_inclusive_group(c, "enemies", "syllable");
    // Range whose min is a prefix of the max — triggers the early-exit
    // optimisation in the underlying byte trie.
    bencher.range_inclusive_group(c, "en", "enemies");
    // Range whose endpoints share a long prefix — different optimisation
    // path again.
    bencher.range_inclusive_group(c, "aback", "abyss");

    // High-frequency contains target — short prefix, lots of children.
    bencher.contains_group(c, "An");
    // Low-frequency contains target — needle that rarely lines up.
    bencher.contains_group(c, "of");
}

criterion_group!(wiki_1k_rune_iter, iter_rune_benches_wiki1k);
criterion_group!(gutenberg_rune_iter, iter_rune_benches_gutenberg);
criterion_main!(wiki_1k_rune_iter, gutenberg_rune_iter);

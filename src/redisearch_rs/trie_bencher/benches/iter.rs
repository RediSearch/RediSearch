/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Side-by-side Rust `TermDictionary` vs C `Trie` (`Trie_Sort_Lex`) iteration
//! benches:
//!
//! - Full lex walk (`Trie_IterateAll`) — fork-GC hot path
//!   (`src/fork_gc/terms.c:20`)
//! - DFA-filtered walk (`Trie_Iterate`) — FT.SEARCH prefix/fuzzy hot path
//!   (`src/query.c:617`); the running-min distance is load-bearing per
//!   memory `project_dfa_filter_dist_semantics`, so the benches sum it.
//! - Lex-range walk (`Trie_IterateRange`) — `[lex foo bar]` query nodes
//!   (`src/query.c:912`)
//! - Wildcard (`Trie_IterateWildcard`)
//! - Contains-anywhere (`Trie_IterateContains(prefix=false, suffix=false)`)
//!
//! Each corpus runs both folding modes — see [`trie_bencher::bencher`]
//! for the [`FoldMode`] contract.

use criterion::{Criterion, criterion_group, criterion_main};
use trie_bencher::{FoldMode, OperationBencher, corpus::CorpusType};

fn iter_benches_wiki1k(c: &mut Criterion) {
    let corpus = CorpusType::RedisBench1kWiki;
    let terms = corpus.create_terms(true);

    for fold_mode in [FoldMode::Raw, FoldMode::PreFolded] {
        let bencher = OperationBencher::new("Wiki-1K".to_owned(), terms.clone(), fold_mode, None);

        bencher.iter_all_group(c);
        bencher.wildcard_group(c, "Ab*");
        // Fixed length.
        bencher.wildcard_group(c, "Apollo ??");
        // DFA is intentionally NOT exercised against Wiki-1K — see memory
        // `project_str_dfa_mid_codepoint_panic`. The corpus contains
        // multi-byte UTF-8 titles which trip the DFA's
        // "trie label is UTF-8" assertion. DFA benches run on the ASCII
        // corpora (Gutenberg, Wiki-10K) instead.
    }
}

fn iter_benches_wiki10k(c: &mut Criterion) {
    let corpus = CorpusType::RedisBench10kNumerics;
    let terms = corpus.create_terms(true);

    for fold_mode in [FoldMode::Raw, FoldMode::PreFolded] {
        let bencher = OperationBencher::new(
            "Wiki-10K".to_owned(),
            terms.clone(),
            fold_mode,
            None,
        );

        bencher.iter_all_group(c);
        // Wiki-10K is hex-and-colon GUIDs; '0' is a frequent leading byte.
        bencher.dfa_group(c, "0", 0, true);
        bencher.dfa_group(c, "0", 1, true);
        bencher.dfa_group(c, "0", 2, true);
    }
}

fn iter_benches_gutenberg(c: &mut Criterion) {
    let corpus = CorpusType::GutenbergEbook(true);
    let terms = corpus.create_terms(true);

    for fold_mode in [FoldMode::Raw, FoldMode::PreFolded] {
        let bencher = OperationBencher::new(
            "Gutenberg".to_owned(),
            terms.clone(),
            fold_mode,
            None,
        );

        bencher.iter_all_group(c);

        // Requires backtracking — de facto suffix matching.
        bencher.wildcard_group(c, "*ly");

        // Bounded lex range.
        bencher.range_group(c, Some("enemies"), false, Some("syllable"), true);
        // Min is a prefix of max — known optimization path.
        bencher.range_group(c, Some("en"), false, Some("enemies"), true);
        // Min and max share a long prefix.
        bencher.range_group(c, Some("aback"), true, Some("abyss"), true);

        // Contains-anywhere of a frequent prefix.
        bencher.contains_group(c, "An");
        // Rarely a prefix — scans ~all nodes.
        bencher.contains_group(c, "of");

        // DFA over a common English prefix.
        bencher.dfa_group(c, "ever", 0, true);
        bencher.dfa_group(c, "ever", 1, true);
    }
}

criterion_group!(wiki_1k_iter, iter_benches_wiki1k);
criterion_group!(wiki_10k_iter, iter_benches_wiki10k);
criterion_group!(gutenberg_iter, iter_benches_gutenberg);
criterion_main!(wiki_1k_iter, wiki_10k_iter, gutenberg_iter);

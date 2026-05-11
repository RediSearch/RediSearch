/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Benchmark the iterators exposed by a trie map.

use criterion::{Criterion, criterion_group, criterion_main};
use trie_bencher::OperationBencher;

use trie_bencher::corpus::CorpusType;
use trie_rs::iter::{RangeBoundary, RangeFilter};

fn iter_benches_wiki1k(c: &mut Criterion) {
    let corpus = CorpusType::RedisBench1kWiki;
    let terms = corpus.create_terms(true);

    let bencher = OperationBencher::new("Wiki-1K".to_owned(), terms, None);
    // Matches "A" and "Abacus"
    bencher.find_prefixes_group(c, "Abacuses", "Find prefixes");
    bencher.wildcard_group(c, "Ab*");
    // Fixed length.
    bencher.wildcard_group(c, "Apollo ??");

    bencher.into_values_group(c, "IntoValues iterator");
}

fn iter_benches_gutenberg(c: &mut Criterion) {
    let corpus = CorpusType::GutenbergEbook(true);
    let terms = corpus.create_terms(true);

    let bencher = OperationBencher::new("Gutenberg".to_owned(), terms, None);
    // Matches "ever", "everlasting" and "everlastingly".
    bencher.find_prefixes_group(c, "everlastingly", "Find prefixes");
    // Requires backtracking to perform, de facto, suffix matching
    bencher.wildcard_group(c, "*ly");

    // Patterns that exercise the two surviving NFA classes (`u64` ≤ 63
    // atoms, `u128` ≤ 127). Anything larger routes through the filter
    // fallback under `wildcard_specialized_iter` and would compare
    // identical timings against `wildcard_iter`, so we don't bench it.
    //
    // For the `u128` class we run two shapes back-to-back so the contrast
    // is visible:
    //
    // - High self-overlap (`*a×70*`): every position in the literal is a
    //   valid match-start, so the NFA's active set fills the entire bitset
    //   during traversal and `union_in_place` does maximum work per byte.
    // - Low self-overlap (`*Zabc…×70*`): the literal starts with a
    //   character (`Z`) that doesn't appear in the rest of the body, so
    //   the active set stays narrow (≈ 3 positions) throughout. This is
    //   the typical shape for real long patterns — e.g., codepoint-lifted
    //   UTF-8 queries.
    //
    // The leading `*` defeats the literal-prefix shortcut, so the iterator
    // walks the entire trie.
    let high_overlap_70 = format!("*{}*", "a".repeat(70));
    bencher.wildcard_group_labeled(
        c,
        "*a×70* (72 atoms, u128, high self-overlap)",
        &high_overlap_70,
    );
    let low_overlap_70: String = std::iter::once('Z')
        .chain((b'a'..=b'z').map(char::from).cycle().take(69))
        .collect();
    bencher.wildcard_group_labeled(
        c,
        "*Zabc…×70* (72 atoms, u128, low self-overlap)",
        &format!("*{low_overlap_70}*"),
    );

    bencher.range_group(
        c,
        RangeFilter {
            min: Some(RangeBoundary::excluded("enemies".as_bytes())),
            max: Some(RangeBoundary::included("syllable".as_bytes())),
        },
    );

    // The minimum is a prefix of the maximum, allowing for an optimization.
    bencher.range_group(
        c,
        RangeFilter {
            min: Some(RangeBoundary::excluded("en".as_bytes())),
            max: Some(RangeBoundary::included("enemies".as_bytes())),
        },
    );

    // The minimum and the maximum share a prefix, allowing for an optimization.
    bencher.range_group(
        c,
        RangeFilter {
            min: Some(RangeBoundary::included("aback".as_bytes())),
            max: Some(RangeBoundary::included("abyss".as_bytes())),
        },
    );

    // It's a prefix of many titles, we will therefore be able to skip
    // the check for all children of the prefix node.
    bencher.contains_group(c, "An");

    // Rarely a prefix, we have to scan ~all nodes.
    bencher.contains_group(c, "of");

    bencher.into_values_group(c, "IntoValues iterator");
}

criterion_group!(wiki_1k_iter, iter_benches_wiki1k);
criterion_group!(gutenberg_iter, iter_benches_gutenberg);
criterion_main!(wiki_1k_iter, gutenberg_iter);

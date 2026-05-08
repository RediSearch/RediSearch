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

    // Cross-corpus sanity check that the wide-state dispatch routes
    // correctly on a smaller trie too. One representative pattern at the
    // sparse class is enough — the per-class scaling story is exercised on
    // Gutenberg.
    let low_overlap_260: String = std::iter::once('Z')
        .chain((b'a'..=b'z').map(char::from).cycle().take(259))
        .collect();
    bencher.wildcard_group_labeled(
        c,
        "*Zabc…×260* (262 atoms, sparse)",
        &format!("*{low_overlap_260}*"),
    );

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

    // Patterns that exercise the wide-state paths in the wildcard NFA.
    // The dispatcher picks the most efficient state representation that
    // fits a pattern's atom count: `u64` up to 63 atoms, `u128` up to 127,
    // `InlineStateSet` (`[u64; 4]` on the stack) up to 255, and
    // `WildcardSparseNfa` (a sparse-set automaton with recycled scratches)
    // for anything larger.
    //
    // One catch-all bench per non-`u64` size class. Atom-count scaling
    // within each class moves the timings within noise once the class is
    // fixed, so the largest representative is enough.
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
    let low_overlap_130: String = std::iter::once('Z')
        .chain((b'a'..=b'z').map(char::from).cycle().take(129))
        .collect();
    bencher.wildcard_group_labeled(
        c,
        "*Zabc…×130* (132 atoms, inline)",
        &format!("*{low_overlap_130}*"),
    );
    let low_overlap_1100: String = std::iter::once('Z')
        .chain((b'a'..=b'z').map(char::from).cycle().take(1099))
        .collect();
    bencher.wildcard_group_labeled(
        c,
        "*Zabc…×1100* (1102 atoms, sparse, single literal)",
        &format!("*{low_overlap_1100}*"),
    );

    // Multi-literal variant at the same atom count: `*the*and*of*` repeated
    // 100 times (with consecutive `**` collapsed by the parser, each
    // repetition adds 11 atoms for a total of 1101). Same `BitSetClass` as
    // the previous bench, different shape — every `*` is a backtrack
    // point on the filter side, so this measures whether shape changes the
    // gap meaningfully on a non-`*L*` pattern.
    //
    // The literals are short common English fragments and may match real
    // Gutenberg keys in places, so this also exercises the yield path.
    let multi_literal = "*the*and*of*".repeat(100);
    bencher.wildcard_group_labeled(
        c,
        "*the*and*of*×100 (1101 atoms, sparse, multi-literal)",
        &multi_literal,
    );

    // Prefix-anchored variant. The literal-prefix shortcut sends the
    // iterator straight to the `ev` subtree, so the trie traversal is
    // bounded; what dominates is `WildcardSparseNfa::compile` itself. The
    // 1100-atom case is the most demanding for construction.
    bencher.wildcard_group_labeled(
        c,
        "ev*Zabc…×1100* (1104 atoms, sparse, prefix-anchored)",
        &format!("ev*{low_overlap_1100}*"),
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

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

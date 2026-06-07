/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Side-by-side Rust `TermDictionary` vs C `Trie` (`Trie_Sort_Lex`) benches
//! for the core mutating + point-lookup surface: bulk load, insert, find,
//! remove. Iteration benches live in `iter.rs`.
//!
//! Every corpus is run twice: once with [`FoldMode::Raw`] and once with
//! [`FoldMode::PreFolded`]. See [`trie_bencher::bencher`] for the
//! folding-mode contract.

use std::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};
use trie_bencher::{FoldMode, OperationBencher, corpus::CorpusType};

fn criterion_benchmark_gutenberg(c: &mut Criterion) {
    let corpus = CorpusType::GutenbergEbook(false);
    let terms = corpus.create_terms(true);
    for fold_mode in [FoldMode::Raw, FoldMode::PreFolded] {
        let bencher =
            OperationBencher::new("Gutenberg".to_owned(), terms.clone(), fold_mode, None);
        bencher.load_group(c);
        bencher.insert_group(c, "colder", "Insert (leaf)");
        bencher.insert_group(c, "fan", "Insert (split with 2 children)");
        bencher.insert_group(c, "effo", "Insert (split with no children)");
        bencher.find_group(c, "form", "Find no match");
        bencher.find_group(c, "April,", "Find match (depth 2)");
        bencher.find_group(c, "enormous", "Find match (depth 4)");
        bencher.remove_group(c, "bright", "Remove leaf (with merge)");
        bencher.remove_group(c, "along", "Remove leaf (no merge)");
    }
}

fn criterion_benchmark_redis_wiki_1k(c: &mut Criterion) {
    let corpus = CorpusType::RedisBench1kWiki;
    let terms = corpus.create_terms(true);
    for fold_mode in [FoldMode::Raw, FoldMode::PreFolded] {
        let bencher = OperationBencher::new("Wiki-1K".to_owned(), terms.clone(), fold_mode, None);
        bencher.load_group(c);

        // parent at line 95 in redis_wiki1k_titles_bench.txt
        bencher.insert_group(c, "Abigail", "Insert (split with 2 children)");
        // parent at line 1 (root) in redis_wiki1k_titles_bench.txt
        bencher.insert_group(c, "Zoo", "Insert (split with 18 children)");

        // line 209 in redis_wiki1k_titles_bench.txt
        bencher.find_group(c, "Alabama River", "Find match (depth 5)");
        // line 156 in redis_wiki1k_titles_bench.txt
        bencher.find_group(c, "Afrikaans History", "Find no match (depth 5)");
        // line 893 in redis_wiki1k_titles_bench.txt
        bencher.find_group(c, "Zoo", "Find no match (depth 1)");

        // line 208 in redis_wiki1k_titles_bench.txt
        bencher.remove_group(c, "Alabama", "Remove internal (with merge)");
    }
}

fn criterion_benchmark_redis_wiki_10k(c: &mut Criterion) {
    let corpus = CorpusType::RedisBench10kNumerics;
    let terms = corpus.create_terms(true);
    for fold_mode in [FoldMode::Raw, FoldMode::PreFolded] {
        let bencher = OperationBencher::new(
            "Wiki-10K".to_owned(),
            terms.clone(),
            fold_mode,
            Some(Duration::from_secs(20)),
        );
        bencher.load_group(c);

        // Parent at line 45 in redis_wiki10k_guids_bench.txt
        let word = "00f20a-new-idx-no-format-check";
        bencher.insert_group(c, word, "Insert (leaf)");

        // At line 6430 in redis_wiki10K_guids_bench.txt
        let word = "767eae1ed0ed43d2b2da05cc3be32aa2:4999";
        bencher.find_group(c, word, "Find match (depth 5)");

        // At line 11708 in redis_wiki10K_guids_bench.txt
        let word = "d9756b1211744e85b3580d939b131dfd:6000";
        bencher.remove_group(c, word, "Remove leaf (no merge)");
    }
}

criterion_group!(wiki_10k, criterion_benchmark_redis_wiki_10k);
criterion_group!(wiki_1k, criterion_benchmark_redis_wiki_1k);
criterion_group!(benches, criterion_benchmark_gutenberg);
criterion_main!(benches, wiki_1k, wiki_10k);

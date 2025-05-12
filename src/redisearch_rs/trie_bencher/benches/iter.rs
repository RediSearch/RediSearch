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

fn iter_benches(c: &mut Criterion) {
    let corpus = CorpusType::RedisBench1kWiki;
    let terms = corpus.create_terms(true);

    let bencher = OperationBencher::new("Wiki-1K".to_owned(), terms, None);
    bencher.find_prefixes_group(c, "Abacuses", "Find prefixes");
}

criterion_group!(wiki_1k_iter, iter_benches);
criterion_main!(wiki_1k_iter);

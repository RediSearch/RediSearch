/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;
use trie_bencher::{RustTrieMap, corpus::CorpusType};

fn main() {
    compute_and_report_memory_usage();
}

/// Download a text corpus and build a trie from it, using
/// both the Rust and the C implementations.
///
/// Report to stdout the memory usage of both tries, alongside
/// the memory size of the original raw corpus.
fn compute_and_report_memory_usage() {
    let mut map = RustTrieMap::new();
    let mut raw_size = 0;
    let unique_words = CorpusType::GutenbergEbook(true).create_terms(false);

    for string in unique_words.iter() {
        raw_size += string.len();
        // Use a zero-sized type by passing a null pointer for `value`
        let value = NonNull::dangling();
        map.insert(string.as_bytes(), value);
    }

    let n_unique_words = unique_words.len();
    println!(
        r#"Statistics:
- Raw text size: {:.3} MBs
- Number of unique words: {n_unique_words}
- Memory {:.3} MBs
- {} nodes"#,
        raw_size as f64 / 1024. / 1024.,
        map.mem_usage() as f64 / 1024. / 1024.,
        map.n_nodes(),
    );
}

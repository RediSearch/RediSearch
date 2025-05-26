/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;
use trie_bencher::corpus::CorpusType;
use trie_bencher::{
    CTrieMap, RustTrieMap,
    c_map::{AsTrieTermView as _, IntoCString as _},
};

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
    let mut cmap = CTrieMap::new();

    let mut raw_size = 0;
    let unique_words = CorpusType::GutenbergEbook(true).create_terms(false);
    let unique_words_cstrings = unique_words
        .iter()
        .map(|e| e.into_cstring())
        .collect::<Vec<_>>();

    for (string, c_string) in unique_words.iter().zip(unique_words_cstrings.iter()) {
        raw_size += string.len();

        // Use a zero-sized type by passing a null pointer for `value`
        let value = NonNull::dangling();

        // Rust insertion
        map.insert(string.as_bytes(), value);

        // C insertion
        cmap.insert(c_string.as_view());
    }

    // Sanity check
    for (string, c_string) in unique_words.iter().zip(unique_words_cstrings.iter()) {
        assert!(
            map.find(string.as_bytes()).is_some(),
            "{string} not found in Rust map"
        );
        assert!(
            // Safety: TRIEMAP_NOTFOUND is a constant defined in the C code
            cmap.find(c_string.as_view()) != unsafe { trie_bencher::ffi::TRIEMAP_NOTFOUND },
            "{string} not found in C map"
        )
    }
    let n_stored_words = map.iter().count();
    let n_unique_words = unique_words.len();
    assert_eq!(n_stored_words, n_unique_words);
    println!(
        r#"Statistics:
- Raw text size: {:.3} MBs
- Number of unique words: {n_unique_words}
- Rust -> {:.3} MBs
          {} nodes
- C    -> {:.3} MBs
          {} nodes"#,
        raw_size as f64 / 1024. / 1024.,
        map.mem_usage() as f64 / 1024. / 1024.,
        map.n_nodes(),
        cmap.mem_usage() as f64 / 1024. / 1024.,
        cmap.n_nodes()
    );
}

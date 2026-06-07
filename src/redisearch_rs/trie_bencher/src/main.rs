/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use trie_bencher::{RustTrieMap, corpus::CorpusType};

fn main() {
    compute_and_report_memory_usage();
}

/// Download a text corpus and build a [`RustTrieMap`] from it, then
/// report raw size, unique-word count, and trie memory footprint.
fn compute_and_report_memory_usage() {
    let mut dict = RustTrieMap::new();
    let mut raw_size = 0;
    let unique_words = CorpusType::GutenbergEbook(true).create_terms(false);

    for term in unique_words.iter() {
        raw_size += term.len();
        dict.add_term(term, 1.0, 1);
    }

    let n_unique_terms = dict.len();
    println!(
        r#"Statistics:
- Raw text size: {:.3} MBs
- Number of unique terms: {n_unique_terms}
- Memory {:.3} MBs"#,
        raw_size as f64 / 1024. / 1024.,
        dict.mem_usage() as f64 / 1024. / 1024.,
    );
}

use std::{collections::BTreeSet, ptr::NonNull};
use trie_bencher::corpus::CorpusType;
use trie_bencher::{str2c_char, AsCstr as _, AsTrieInput as _, CTrieMap, RustTrieMap};

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
    let contents = CorpusType::GutenbergEbook.download_or_read_corpus();
    let mut raw_size = 0;
    let mut n_words = 0;
    let mut unique_words = BTreeSet::new();
    for line in contents.lines() {
        for word in line.split_whitespace() {
            let converted = str2c_char(word);
            raw_size += converted.len();
            n_words += 1;
            unique_words.insert(word.to_owned());

            // Use a zero-sized type by passing a null pointer for `value`
            let value = NonNull::dangling();

            // Rust insertion
            map.insert(&converted, value);

            // C insertion
            let word = word.as_cstr();
            let view = word.as_trie_input();
            cmap.insert(view.ptr(), view.len());
        }
    }

    // Sanity check
    for unique_word in &unique_words {
        let converted = str2c_char(unique_word);
        assert!(
            map.find(&converted).is_some(),
            "{unique_word} not found in Rust map"
        );
        let unique_word_c = unique_word.as_cstr();
        let view = unique_word_c.as_trie_input();
        assert!(
            cmap.find(view.ptr(), view.len()) != unsafe { trie_bencher::ffi::TRIEMAP_NOTFOUND },
            "{unique_word} not found in C map"
        )
    }
    let n_stored_words = map.iter().count();
    let n_unique_words = unique_words.len();
    assert_eq!(n_stored_words, n_unique_words);
    println!(
        r#"Statistics:
- Raw text size: {:.3} MBs
- Number of words (with duplicates): {n_words}
- Number of unique words: {n_unique_words}
- Rust -> {:.3} MBs
          {} nodes
- C    -> {:.3} MBs
          {} nodes"#,
        raw_size as f64 / 1024. / 1024.,
        map.mem_usage() as f64 / 1024. / 1024.,
        map.num_nodes(),
        cmap.mem_usage() as f64 / 1024. / 1024.,
        cmap.n_nodes()
    );
}

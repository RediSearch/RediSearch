//! Benchmark the iterators provided by a trie map
//!
//! Refer to `data/bench_trie.txt` to visualize the structure of the trie that's being benchmarked.

use criterion::{Criterion, criterion_group, criterion_main};
use trie_bencher::{OperationBencher, corpus::CorpusType};

fn criterion_benchmark_gutenberg(c: &mut Criterion) {
    let corpus = CorpusType::GutenbergEbook;
    let keys = corpus.create_terms(true);

    let bencher = OperationBencher::new("Gutenberg".to_owned(), keys, None);
    bencher.iterate_prefix_group(c, "a", r#"Iterate nodes with 1 letter vowel prefix"#);
    bencher.iterate_contains_group(c, "e", r#"Iterate nodes containing 1 letter pattern"#);
}

criterion_group!(benches, criterion_benchmark_gutenberg);
criterion_main!(benches);

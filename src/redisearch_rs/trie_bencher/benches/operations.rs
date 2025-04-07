//! Benchmark the core operations provided by a trie map—insertions, deletions, and lookups.
//!
//! Refer to `data/bench_trie.txt` to visualize the structure of the trie that's being benchmarked.

use criterion::{Criterion, criterion_group, criterion_main};
use trie_bencher::{strvec2_raw_words, OperationBencher};

use trie_bencher::corpus::CorpusType;

fn criterion_benchmark_legacy(c: &mut Criterion) {
    let corpus = CorpusType::GutenbergEbook;
    let keys = corpus.create_keys(true);
    let (c_char_words, words) = strvec2_raw_words(&keys);

    let mut bencher = OperationBencher::new(c, c_char_words, words);
    bencher.load_group("Gutenberg");
    bencher.insert_group("colder", "Insert (leaf)");
    bencher.insert_group("fan", "Insert (split with 2 children)");
    bencher.insert_group("effo", "Insert (split with no children)");
    bencher.find_group("form", "Find no match");
    bencher.find_group("April,", "Find match (depth 2)");
    bencher.find_group("enormous", "Find match (depth 4)");
    bencher.remove_group("bright", "Remove leaf (with merge)");
    bencher.remove_group("along", "Remove leaf (no merge)");
}

fn criterion_benchmark_redis_wiki_1k(c: &mut Criterion) {
    let corpus = CorpusType::RedisBench1kWiki;
    let keys = corpus.create_keys(true);
    let (c_char_words, words) = strvec2_raw_words(&keys);

    let mut bencher = OperationBencher::new(c, c_char_words, words);
    
    bencher.load_group("wiki_1k");
    
    bencher.insert_group("Abigail", "Insert Abigail");
    bencher.insert_group("Zoo", "Insert (Zoo)");

    bencher.find_group("Alabama", "Find Alambama");
    bencher.find_group("Zoo", "Find Zoo (not there)");

    bencher.remove_group("Alabama", "Remove Alabama");
}

criterion_group!(wiki_1k, criterion_benchmark_redis_wiki_1k);
criterion_group!(benches, criterion_benchmark_legacy);
criterion_main!(benches, wiki_1k);


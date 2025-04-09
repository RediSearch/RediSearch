//! Benchmark the core operations provided by a trie map—insertions, deletions, and lookups.
//!
//! Refer to `data/bench_trie.txt` to visualize the structure of the trie that's being benchmarked.

use std::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};
use trie_bencher::OperationBencher;

use trie_bencher::corpus::CorpusType;

fn criterion_benchmark_gutenberg(c: &mut Criterion) {
    let corpus = CorpusType::GutenbergEbook;
    let keys = corpus.create_keys(true);

    let bencher = OperationBencher::new("Gutenberg".to_owned(), keys, None);
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

fn criterion_benchmark_redis_wiki_1k(c: &mut Criterion) {
    let corpus = CorpusType::RedisBench1kWiki;
    let keys = corpus.create_keys(true);

    let bencher = OperationBencher::new("Wiki-1K".to_owned(), keys, None);
    bencher.load_group(c);

    // parent at line 95 in redis_wiki1k_titles_bench.txt
    bencher.insert_group(c, "Abigail", "Insert (split with 2 children)");
    // parent at line 1 (root) in redis_wiki1k_titles_bench.txt
    bencher.insert_group(c, "Zoo", "Insert (split with 18 children)");

    // line 209 in redis_wiki1k_titles_bench.txt
    bencher.find_group(c, "Alabama River", "Find match (depth 5");
    bencher.find_group(c, "Zoo", "Find no match");

    // line 208 in redis_wiki1k_titles_bench.txt
    bencher.remove_group(c, "Alabama", "Remove internal (with merge)");
}

fn criterion_benchmark_redis_wiki_10k(c: &mut Criterion) {
    let corpus = CorpusType::RedisBench10kNumerics;
    let keys = corpus.create_keys(true);
    let bencher = OperationBencher::new("Wiki-10K".to_owned(), keys, Some(Duration::from_secs(10)));

    bencher.load_group(c);

    // Parent at line 45 in reis_wiki10k_guids_bench.txt
    let word = "00f20a-new-idx-no-format-check";
    bencher.insert_group(c, word, "Insert (leaf)");

    // At line 6430 in redis_wiki10K_guids_bench.txt
    let word = "767eae1ed0ed43d2b2da05cc3be32aa2:4999";
    bencher.find_group(c, word, "Find match (depth 5)");

    // At line 11708 in redis_wiki10K_guids_bench.txt
    let word = "d9756b1211744e85b3580d939b131dfd:6000";
    bencher.remove_group(c, word, "Remove leaf (no merge)");
}

criterion_group!(wiki_10k, criterion_benchmark_redis_wiki_10k);
criterion_group!(wiki_1k, criterion_benchmark_redis_wiki_1k);
criterion_group!(benches, criterion_benchmark_gutenberg);
criterion_main!(benches, wiki_1k, wiki_10k);

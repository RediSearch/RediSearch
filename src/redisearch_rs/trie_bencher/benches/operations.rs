//! Benchmark the core operations provided by a trie map—insertions, deletions, and lookups.
//!
//! Refer to `data/bench_trie.txt` to visualize the structure of the trie that's being benchmarked.
use std::path::PathBuf;

use criterion::{Criterion, criterion_group, criterion_main};
use trie_bencher::OperationBencher;

fn criterion_benchmark(c: &mut Criterion) {
    let file_path = PathBuf::from("data").join("bench.txt");
    let contents = fs_err::read_to_string(file_path).unwrap();

    let mut bencher = OperationBencher::new(c, contents.clone());
    bencher.load_group(contents);
    bencher.insert_group("colder", "Insert (leaf)");
    bencher.insert_group("fan", "Insert (split with 2 children)");
    bencher.insert_group("effo", "Insert (split with no children)");
    bencher.find_group("form", "Find no match");
    bencher.find_group("April,", "Find match (depth 2)");
    bencher.find_group("enormous", "Find match (depth 4)");
    // Causes a node merge when removed
    bencher.remove_group("bright", "Remove leaf with one sibling");
    // No merges when it gets removed
    bencher.remove_group("along", "Remove leaf with more than one sibling");
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

//! Benchmark the iterators provided by a trie map
//!
//! Refer to `data/bench_trie.txt` to visualize the structure of the trie that's being benchmarked.
use std::path::PathBuf;

use criterion::{Criterion, criterion_group, criterion_main};
use trie_bencher::OperationBencher;

fn criterion_benchmark(c: &mut Criterion) {
    let file_path = PathBuf::from("data").join("bench.txt");
    let contents = fs_err::read_to_string(file_path).unwrap();

    let mut bencher = OperationBencher::new(c, contents.clone());
    bencher.iterate_prefix_group();
    bencher.iterate_contains_group();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);

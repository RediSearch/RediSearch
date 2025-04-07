use crate::{CTrieMap, RustRadixTrie, RustTrieMap, str2c_char, str2c_input};
use criterion::{BatchSize, BenchmarkGroup, Criterion, measurement::Measurement};
use std::{
    ffi::{c_char, c_void},
    hint::black_box,
    ptr::NonNull,
};

/// A helper struct for benchmarking operations on different trie map implementations.
pub struct OperationBencher<'a> {
    c: &'a mut Criterion,
    rust_map: RustTrieMap,
    radix: RustRadixTrie,
    contents: String,
}

impl<'a> OperationBencher<'a> {
    /* 
    pub fn new(
        c: &'a mut Criterion,
        contents: String,
    ) -> Self {
        let c_char_words = contents
            .split_whitespace()
            .map(str2c_char)
            .collect::<Vec<_>>();
        let rust_map = rust_load(&c_char_words);

        let u8_words = contents
            .split_whitespace()
            .map(crate::str2u8)
            .collect::<Vec<_>>();
        let radix = radix_load(u8_words);

        OperationBencher {
            c,
            rust_map,
            radix,
            contents,
        }
    }
    */

    pub fn new(
        c: &'a mut Criterion,
        c_char_words: Vec<Box<[i8]>>,
        words: Vec<(u16, *mut c_char)>,
    ) -> Self {
        let rust_map = rust_load(&c_char_words);
        assert!(rust_map.n_nodes() > c_char_words.len(), "{} > {} failed", rust_map.n_nodes(), c_char_words.len());
        Self { c, rust_map, raw_contents_c: words, raw_contents_rust: c_char_words }
    }

    /// Benchmark the find operation.
    ///
    /// The benchmark group will be marked with the given label.
    pub fn find_group(&mut self, word: &str, label: &str) {
        let mut group = self.c.benchmark_group(label);
        find_rust_benchmark(&mut group, &self.rust_map, word);
        find_c_benchmark(&mut group, &self.raw_contents_c, word);
        group.finish();
    }

    /// Benchmark the insert operation.
    ///
    /// The benchmark group will be marked with the given label.
    pub fn insert_group(&mut self, word: &str, label: &str) {
        let mut group = self.c.benchmark_group(label);
        insert_rust_benchmark(&mut group, self.rust_map.clone(), word);
        insert_c_benchmark(&mut group, &self.raw_contents_c, word);
        group.finish();
    }

    /// Benchmark the removal operation.
    ///
    /// The benchmark group will be marked with the given label.
    pub fn remove_group(&mut self, word: &str, label: &str) {
        let mut group = self.c.benchmark_group(label);
        remove_rust_benchmark(&mut group, self.rust_map.clone(), word);
        remove_c_benchmark(&mut group, &self.raw_contents_c, word);
        group.finish();
    }

    /// Benchmark loading a corpus of words.
    pub fn load_group(&mut self, postfix: &str) {
        let mut group = self.c.benchmark_group(format!("Load ({})", postfix));
        load_rust_benchmark(&mut group, &self.raw_contents_rust);
        load_c_benchmark(&mut group, &self.raw_contents_c);
        group.finish();
    }
}

fn find_rust_benchmark<M: Measurement>(
    c: &mut BenchmarkGroup<'_, M>,
    map: &RustTrieMap,
    word: &str,
) {
    let rust_word = str2c_char(word);
    c.bench_function("Rust", |b| {
        b.iter(|| map.find(black_box(&rust_word)).is_some())
    });
}

fn insert_rust_benchmark<M: Measurement>(
    c: &mut BenchmarkGroup<'_, M>,
    map: RustTrieMap,
    word: &str,
) {
    let word = crate::str2c_char(word);
    c.bench_function("Rust", |b| {
        b.iter_batched_ref(
            || map.clone(),
            |data| {
                data.insert(black_box(&word), black_box(NonNull::<c_void>::dangling()))
                    .is_some()
            },
            BatchSize::LargeInput,
        )
    });
}

fn insert_c_benchmark<M: Measurement>(c: &mut BenchmarkGroup<'_, M>, contents: &Vec<(u16, *mut c_char)>, word: &str) {
    let (c_word, c_len) = str2c_input(word);
    c.bench_function("C", |b| {
        b.iter_batched_ref(
            || c_load(contents),
            |data| data.insert(black_box(c_word), black_box(c_len)),
            BatchSize::LargeInput,
        )
    });
}

fn find_c_benchmark<M: Measurement>(c: &mut BenchmarkGroup<'_, M>, contents: &Vec<(u16, *mut c_char)>, word: &str) {
    let (c_word, c_len) = str2c_input(word);
    let map = c_load(contents);
    c.bench_function("C", |b| {
        b.iter(|| map.find(black_box(c_word), black_box(c_len)))
    });
}

fn remove_rust_benchmark<M: Measurement>(
    c: &mut BenchmarkGroup<'_, M>,
    map: RustTrieMap,
    word: &str,
) {
    let rust_word = str2c_char(word);
    c.bench_function("Rust", |b| {
        b.iter_batched_ref(
            || map.clone(),
            |data| data.remove(black_box(&rust_word)).is_some(),
            BatchSize::LargeInput,
        )
    });
}

fn remove_c_benchmark<M: Measurement>(c: &mut BenchmarkGroup<'_, M>, contents: &Vec<(u16, *mut c_char)>, word: &str) {
    let (c_word, c_len) = str2c_input(word);
    c.bench_function("C", |b| {
        b.iter_batched_ref(
            || c_load(contents),
            |data| data.remove(black_box(c_word), black_box(c_len)),
            BatchSize::LargeInput,
        )
    });
}

fn load_rust_benchmark<M: Measurement>(group: &mut BenchmarkGroup<'_, M>, words: &Vec<Box<[i8]>>) {
    group.bench_function("Rust", |b| {
        b.iter_batched(
            || words.clone(),
            |data| rust_load(black_box(&data)),
            BatchSize::LargeInput,
        )
    });
}

fn load_c_benchmark<M: Measurement>(group: &mut BenchmarkGroup<'_, M>, c_words: &Vec<(u16, * mut c_char)>) {
    group.bench_function("C", |b| {
        b.iter_batched(
            || c_words.clone(),
            |data| c_load(black_box(&data)),
            BatchSize::LargeInput,
        )
    });
}

pub fn rust_load(words: &[Box<[c_char]>]) -> RustTrieMap {
    let mut map = trie_rs::TrieMap::new();
    for word in words {
        map.insert(word, NonNull::<c_void>::dangling());
    }
    map
}

fn c_load(words: &Vec<(u16, *mut c_char)>) -> CTrieMap {
    let mut map = CTrieMap::new();
    for (len, word) in words {
        map.insert(word.clone(), *len);
    }
    map
}

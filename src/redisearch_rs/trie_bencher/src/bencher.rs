use crate::{CTrieMap, RustTrieMap, str2c_char, str2c_input};
use criterion::{BatchSize, BenchmarkGroup, Criterion, measurement::Measurement};
use std::{
    ffi::{CString, c_char, c_void},
    hint::black_box,
    ptr::NonNull,
};

/// A helper struct for benchmarking operations on different trie map implementations.
pub struct OperationBencher<'a> {
    c: &'a mut Criterion,
    rust_map: RustTrieMap,
    contents: String,
}

impl<'a> OperationBencher<'a> {
    /// Initialize a new bencher by creating a new instance of each trie map implementation
    /// for the same corpus of words.
    pub fn new(c: &'a mut Criterion, contents: String) -> Self {
        let c_char_words = contents
            .split_whitespace()
            .map(str2c_char)
            .collect::<Vec<_>>();
        let rust_map = rust_load(&c_char_words);

        OperationBencher {
            c,
            rust_map,
            contents,
        }
    }

    /// Benchmark the find operation.
    ///
    /// The benchmark group will be marked with the given label.
    pub fn find_group(&mut self, word: &str, label: &str) {
        let mut group = self.c.benchmark_group(label);
        find_rust_benchmark(&mut group, &self.rust_map, word);
        find_c_benchmark(&mut group, &self.contents, word);
        group.finish();
    }

    /// Benchmark the insert operation.
    ///
    /// The benchmark group will be marked with the given label.
    pub fn insert_group(&mut self, word: &str, label: &str) {
        let mut group = self.c.benchmark_group(label);
        insert_rust_benchmark(&mut group, self.rust_map.clone(), word);
        insert_c_benchmark(&mut group, &self.contents, word);
        group.finish();
    }

    /// Benchmark the removal operation.
    ///
    /// The benchmark group will be marked with the given label.
    pub fn remove_group(&mut self, word: &str, label: &str) {
        let mut group = self.c.benchmark_group(label);
        remove_rust_benchmark(&mut group, self.rust_map.clone(), word);
        remove_c_benchmark(&mut group, &self.contents, word);
        group.finish();
    }

    /// Benchmark loading a corpus of words.
    pub fn load_group(&mut self) {
        let mut group = self.c.benchmark_group("Load");
        load_rust_benchmark(&mut group, &self.contents);
        load_c_benchmark(&mut group, &self.contents);
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

fn insert_c_benchmark<M: Measurement>(c: &mut BenchmarkGroup<'_, M>, contents: &str, word: &str) {
    let (c_word, c_len) = str2c_input(word);
    c.bench_function("C", |b| {
        b.iter_batched_ref(
            || c_load_from_str(contents),
            |data| data.insert(black_box(c_word), black_box(c_len)),
            BatchSize::LargeInput,
        )
    });
}

fn find_c_benchmark<M: Measurement>(c: &mut BenchmarkGroup<'_, M>, contents: &str, word: &str) {
    let (c_word, c_len) = str2c_input(word);
    let map = c_load_from_str(&contents);
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

fn remove_c_benchmark<M: Measurement>(c: &mut BenchmarkGroup<'_, M>, contents: &str, word: &str) {
    let (c_word, c_len) = str2c_input(word);
    c.bench_function("C", |b| {
        b.iter_batched_ref(
            || c_load_from_str(contents),
            |data| data.remove(black_box(c_word), black_box(c_len)),
            BatchSize::LargeInput,
        )
    });
}

fn load_rust_benchmark<M: Measurement>(group: &mut BenchmarkGroup<'_, M>, contents: &str) {
    let words = contents
        .split_whitespace()
        .map(str2c_char)
        .collect::<Vec<_>>();
    group.bench_function("Rust", |b| {
        b.iter_batched(
            || words.clone(),
            |data| rust_load(black_box(&data)),
            BatchSize::LargeInput,
        )
    });
}

fn load_c_benchmark<M: Measurement>(group: &mut BenchmarkGroup<'_, M>, contents: &str) {
    let c_words = contents
        .split_whitespace()
        .map(|s| {
            let (c_word, c_len) = str2c_input(&s);
            (c_len, c_word)
        })
        .collect::<Vec<_>>();
    group.bench_function("C", |b| {
        b.iter_batched(
            || c_words.clone(),
            |data| c_load(black_box(data)),
            BatchSize::LargeInput,
        )
    });
}

fn rust_load(words: &[Box<[c_char]>]) -> RustTrieMap {
    let mut map = trie_rs::TrieMap::new();
    for word in words {
        map.insert(word, NonNull::<c_void>::dangling());
    }
    map
}

fn c_load_from_str(contents: &str) -> CTrieMap {
    let words = contents
        .split_whitespace()
        .map(|word| CString::new(word).expect("CString conversion failed"))
        .map(|s| {
            let len: u16 = s.as_bytes_with_nul().len().try_into().unwrap();
            let converted = s.into_raw();
            (len, converted)
        })
        .collect::<Vec<_>>();
    c_load(words)
}

fn c_load(words: Vec<(u16, *mut c_char)>) -> CTrieMap {
    let mut map = CTrieMap::new();
    for (len, word) in words {
        map.insert(word, len);
    }
    map
}

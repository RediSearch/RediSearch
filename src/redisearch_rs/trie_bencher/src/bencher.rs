use crate::{str2c_char, CTrieMap, RustNewTrie, RustRadixTrie, RustTrieMap};
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
    rust_new: RustNewTrie,
    _radix: RustRadixTrie,
    c_map: *mut CTrieMap,
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

        let rust_new = rust_new_load(&c_char_words);

        let u8_words = contents
            .split_whitespace()
            .map(crate::str2u8)
            .collect::<Vec<_>>();
        let radix = radix_load(u8_words);

        let c_words = contents
            .split_whitespace()
            .map(|word| CString::new(word).expect("CString conversion failed"))
            .map(|s| {
                let len: u16 = s.as_bytes_with_nul().len().try_into().unwrap();
                let converted = s.into_raw();
                (len, converted)
            })
            .collect::<Vec<_>>();
        let c_map = c_load(c_words);

        OperationBencher {
            c,
            rust_map,
            rust_new,
            _radix: radix,
            c_map,
        }
    }

    /// Benchmark the find operation.
    ///
    /// The benchmark group will be marked with the given label.
    pub fn find_group(&mut self, word: &str, label: &str) {
        let mut group = self.c.benchmark_group(label);
        find_rust_benchmark(&mut group, self.rust_map.clone(), word);
        find_rust_new_benchmark(&mut group, self.rust_new.clone(), word);
        //find_radix_benchmark(&mut group, self.radix.clone(), word);
        find_c_benchmark(&mut group, self.c_map, word);
        group.finish();
    }

    /// Benchmark the insert operation.
    ///
    /// The benchmark group will be marked with the given label.
    pub fn insert_group(&mut self, word: &str, label: &str) {
        let mut group = self.c.benchmark_group(label);
        insert_rust_benchmark(&mut group, self.rust_map.clone(), word);
        insert_rust_new_benchmark(&mut group, self.rust_new.clone(), word);
        //insert_radix_benchmark(&mut group, self.radix.clone(), word);
        insert_c_benchmark(&mut group, self.c_map, word);
        group.finish();
    }

    /// Benchmark the removal operation.
    ///
    /// The benchmark group will be marked with the given label.
    pub fn remove_group(&mut self, word: &str, label: &str) {
        let mut group = self.c.benchmark_group(label);
        remove_rust_benchmark(&mut group, self.rust_map.clone(), word);
        remove_rust_new_benchmark(&mut group, self.rust_new.clone(), word);
        //remove_radix_benchmark(&mut group, self.radix.clone(), word);
        remove_c_benchmark(&mut group, self.c_map, word);
        group.finish();
    }

    /// Benchmark loading a corpus of words.
    pub fn load_group(&mut self, contents: String) {
        let mut group = self.c.benchmark_group("Load");
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
        group.bench_function("Rust New", |b| {
            b.iter_batched(
                || words.clone(),
                |data| rust_new_load(black_box(&data)),
                BatchSize::LargeInput,
            )
        });
        /*
        let vwords = contents
            .split_whitespace()
            .map(crate::str2u8)
            .collect::<Vec<_>>();
        group.bench_function("Rust Radix", |b| {
            b.iter_batched(
                || vwords.clone(),
                |data| radix_load(black_box(data)),
                BatchSize::LargeInput,
            )
        });
        */
        let c_words = contents
            .split_whitespace()
            .map(|word| CString::new(word).expect("CString conversion failed"))
            .map(|s| {
                let len: u16 = s.as_bytes_with_nul().len().try_into().unwrap();
                let converted = s.into_raw();
                (len, converted)
            })
            .collect::<Vec<_>>();
        group.bench_function("C", |b| {
            b.iter_batched(
                || c_words.clone(),
                |data| c_insertion(black_box(data)),
                BatchSize::LargeInput,
            )
        });
        group.finish();
    }
}

fn find_rust_benchmark<M: Measurement>(
    c: &mut BenchmarkGroup<'_, M>,
    map: RustTrieMap,
    word: &str,
) {
    let rust_word = str2c_char(word);
    c.bench_function("Rust", |b| {
        b.iter_batched_ref(
            || map.clone(),
            |data| data.find(black_box(&rust_word)).is_some(),
            BatchSize::LargeInput,
        )
    });
}

fn find_rust_new_benchmark<M: Measurement>(
    c: &mut BenchmarkGroup<'_, M>,
    map: RustNewTrie,
    word: &str,
) {
    let rust_word = str2c_char(word);
    c.bench_function("Rust New", |b| {
        b.iter_batched_ref(
            || map.clone(),
            |data| data.find(black_box(&rust_word)).is_some(),
            BatchSize::LargeInput,
        )
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

fn insert_rust_new_benchmark<M: Measurement>(
    c: &mut BenchmarkGroup<'_, M>,
    map: RustNewTrie,
    word: &str,
) {
    let word = crate::str2c_char(word);
    c.bench_function("Rust New", |b| {
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


fn insert_radix_benchmark<M: Measurement>(
    c: &mut BenchmarkGroup<'_, M>,
    map: RustRadixTrie,
    word: &str,
) {
    let word = crate::str2u8(word);
    c.bench_function("Rust Radix", |b| {
        b.iter_batched_ref(
            || map.clone(),
            |data| {
                data.insert(
                    black_box(word.clone()),
                    black_box(NonNull::<c_void>::dangling()),
                )
                .is_some()
            },
            BatchSize::LargeInput,
        )
    });
}

fn insert_c_benchmark<M: Measurement>(
    c: &mut BenchmarkGroup<'_, M>,
    map: *mut CTrieMap,
    word: &str,
) {
    let (c_word, c_len) = {
        let converted = CString::new(word).expect("CString conversion failed");
        let len: u16 = converted.as_bytes_with_nul().len().try_into().unwrap();
        (converted.into_raw(), len)
    };
    c.bench_function("C", |b| {
        b.iter_batched_ref(
            || map.clone(),
            |data| unsafe {
                crate::ffi::TrieMap_Add(
                    *data,
                    black_box(c_word),
                    black_box(c_len),
                    black_box(std::ptr::null_mut()),
                    Some(do_nothing),
                )
            },
            BatchSize::LargeInput,
        )
    });
}

fn find_radix_benchmark<M: Measurement>(
    c: &mut BenchmarkGroup<'_, M>,
    map: RustRadixTrie,
    word: &str,
) {
    let rust_word = crate::str2u8(word);
    c.bench_function("Rust Radix", |b| {
        b.iter_batched_ref(
            || map.clone(),
            |data| data.get(black_box(&rust_word)).is_some(),
            BatchSize::LargeInput,
        )
    });
}

fn find_c_benchmark<M: Measurement>(c: &mut BenchmarkGroup<'_, M>, map: *mut CTrieMap, word: &str) {
    let (c_word, c_len) = {
        let converted = CString::new(word).expect("CString conversion failed");
        let len: u16 = converted.as_bytes_with_nul().len().try_into().unwrap();
        (converted.into_raw(), len)
    };
    c.bench_function("C", |b| {
        b.iter_batched_ref(
            || map,
            |data| unsafe { crate::ffi::TrieMap_Find(*data, black_box(c_word), black_box(c_len)) },
            BatchSize::LargeInput,
        )
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

fn remove_rust_new_benchmark<M: Measurement>(
    c: &mut BenchmarkGroup<'_, M>,
    map: RustNewTrie,
    word: &str,
) {
    let rust_word = str2c_char(word);
    c.bench_function("Rust New", |b| {
        b.iter_batched_ref(
            || map.clone(),
            |data| data.remove(black_box(&rust_word)).is_some(),
            BatchSize::LargeInput,
        )
    });
}

fn remove_radix_benchmark<M: Measurement>(
    c: &mut BenchmarkGroup<'_, M>,
    map: RustRadixTrie,
    word: &str,
) {
    let radix_word = crate::str2u8(word);
    c.bench_function("Rust Radix", |b| {
        b.iter_batched_ref(
            || map.clone(),
            |data| data.remove(black_box(&radix_word)).is_some(),
            BatchSize::LargeInput,
        )
    });
}

fn remove_c_benchmark<M: Measurement>(
    c: &mut BenchmarkGroup<'_, M>,
    map: *mut CTrieMap,
    word: &str,
) {
    let (c_word, c_len) = {
        let converted = CString::new(word).expect("CString conversion failed");
        let len: u16 = converted.as_bytes_with_nul().len().try_into().unwrap();
        (converted.into_raw(), len)
    };
    c.bench_function("C", |b| {
        b.iter_batched_ref(
            || map,
            |data| unsafe {
                crate::ffi::TrieMap_Delete(
                    *data,
                    black_box(c_word),
                    black_box(c_len),
                    Some(do_not_free),
                )
            },
            BatchSize::LargeInput,
        )
    });
}

fn rust_load(words: &[Box<[c_char]>]) -> RustTrieMap {
    let mut map = trie_rs::trie::TrieMap::new();
    for word in words {
        map.insert(word, NonNull::<c_void>::dangling());
    }
    map
}

fn rust_new_load(words: &[Box<[c_char]>]) -> RustNewTrie {
    let mut map = RustNewTrie::new();
    for word in words {
        map.insert(word, NonNull::<c_void>::dangling());
    }
    map
}

fn radix_load(words: Vec<Vec<u8>>) -> RustRadixTrie {
    let mut map = radix_trie::Trie::new();
    for word in words {
        map.insert(word, NonNull::<c_void>::dangling());
    }
    map
}

fn c_insertion(words: Vec<(u16, *mut c_char)>) -> usize {
    let map = c_load(words);
    let n_nodes = unsafe { (*map).size };
    unsafe { crate::ffi::TrieMap_Free(map, Some(do_not_free)) }
    n_nodes
}

fn c_load(words: Vec<(u16, *mut c_char)>) -> *mut CTrieMap {
    let map = unsafe { crate::ffi::NewTrieMap() };
    let value = std::ptr::null_mut();
    for (len, word) in words {
        unsafe { crate::ffi::TrieMap_Add(map, word, len, value, Some(do_nothing)) };
    }
    map
}

unsafe extern "C" fn do_nothing(oldval: *mut c_void, _newval: *mut c_void) -> *mut c_void {
    // Just return the old value, since it's a null pointer and we don't want
    // the C map implementation to try to free it.
    oldval
}

unsafe extern "C" fn do_not_free(_val: *mut c_void) {
    // We're using the null pointer as value, so we don't want to free it.
}

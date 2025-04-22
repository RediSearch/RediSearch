use crate::{CTrieMap, RustTrieMap, str2c_char, str2c_input};
use criterion::{
    BatchSize, BenchmarkGroup, Criterion,
    measurement::{Measurement, WallTime},
};
use std::{
    ffi::{c_char, c_void},
    hint::black_box,
    ptr::NonNull,
    time::Duration,
};

/// A helper struct for benchmarking operations on different trie map implementations.
pub struct OperationBencher {
    rust_map: RustTrieMap,

    /// A vector of strings that will be inserted into the trie map.
    keys: Vec<String>,

    /// How long to run benchmarks overall, this differs significantly for benching immutable vs mutable operations because of the setup.
    ///
    /// We need to customize this parameter when using large datasets that require a time-consuming set up
    /// (e.g. an expensive clone of the triemap we are benching, for the example with 10k entries).
    measurement_times: CorpusMeasurementTime,

    /// The prefix added to the label of each benchmark group to identify which corpus was used.
    prefix: String,
}

/// A struct to hold the best `overall measurement times` in a group.
///
/// The benchmarking tool [Criterion] uses this to determine how long to run the benchmarks overall.
/// The `measurement_time_immutable` is used for immutable operations (e.g. find),
/// while the `measurement_time_mutable` is used for mutable operations (e.g. insert, remove).
/// The `measurement_time_immutable` is set to 20% of the `measurement_time_mutable`.
/// This is an approximation based on the assumption that immutable operations are 5 times faster than mutable operations.
pub struct CorpusMeasurementTime {
    /// Measurement time for immutable operations, e.g. find
    measurement_time_immutable: Duration,

    /// Measurement time for mutable operations, e.g. insert, remove
    measurement_time_mutable: Duration,
}

impl CorpusMeasurementTime {
    /// Creates a new [CorpusMeasurementTime] instance based on a mutable measurement time, assuming immutable operations are 5 times faster.
    ///
    /// This holds for the trie operations, but may not hold for other operations.
    ///
    /// This is an approximation based on the assumption that immutable operations are 5 times faster than mutable operations which has been seen
    /// for find vs insert/remove in the benchmarks.
    pub fn from_mutable_trie(mutable_measurement_time: Duration) -> Self {
        Self {
            measurement_time_immutable: mutable_measurement_time.mul_f32(0.2),
            measurement_time_mutable: mutable_measurement_time,
        }
    }
}

impl Default for CorpusMeasurementTime {
    fn default() -> Self {
        Self {
            measurement_time_immutable: Duration::from_secs(5),
            measurement_time_mutable: Duration::from_secs(5),
        }
    }
}

impl OperationBencher {
    /// Creates a new `OperationBencher` instance with the given prefix and terms.
    ///
    /// - `prefix` is used to identify the corpus in the benchmark groups.
    /// - `terms` are used to create a trie map in the setup routine of criterion.
    /// - `mutable_measurement_time` is used to set the measurement time for mutable operations (insert, remove), for now it's also used to approximate the immutable measurement time.
    ///
    /// Use the provided mutable measurement time or default to 5 seconds which is the default in criterion.
    /// For benching other operations than the trie, ensure to check the assumption from [CorpusMeasurementTime::from_mutable_trie].
    pub fn new(
        prefix: String,
        terms: Vec<String>,
        mutable_measurement_time: Option<Duration>,
    ) -> Self {
        let rust_map = rust_load_from_terms(&terms);

        let measurement_time = mutable_measurement_time.unwrap_or(Duration::from_secs(5));
        // approximate the immutable measurement time based on the mutable measurement time, only liable for trie operations.
        let measurement_times = CorpusMeasurementTime::from_mutable_trie(measurement_time);
        Self {
            prefix,
            rust_map,
            keys: terms,
            measurement_times: measurement_times,
        }
    }

    fn benchmark_group_mutable<'a>(
        &self,
        c: &'a mut Criterion,
        label: &str,
    ) -> BenchmarkGroup<'a, WallTime> {
        let mut group = c.benchmark_group(format!("{}|{}", self.prefix, label));
        group.measurement_time(self.measurement_times.measurement_time_mutable);
        group
    }

    fn benchmark_group_immutable<'a>(
        &self,
        c: &'a mut Criterion,
        label: &str,
    ) -> BenchmarkGroup<'a, WallTime> {
        let mut group = c.benchmark_group(format!("{}|{}", self.prefix, label));
        group.measurement_time(self.measurement_times.measurement_time_immutable);
        group
    }

    /// Benchmark the find operation.
    ///
    /// The benchmark group will be marked with the given label.
    pub fn find_group(&self, c: &mut Criterion, word: &str, label: &str) {
        let mut group = self.benchmark_group_immutable(c, label);
        find_rust_benchmark(&mut group, &self.rust_map, word);
        find_c_benchmark(&mut group, &self.keys, word);
        group.finish();
    }

    /// Benchmark the insert operation.
    ///
    /// The benchmark group will be marked with the given label.
    pub fn insert_group(&self, c: &mut Criterion, word: &str, label: &str) {
        let mut group = self.benchmark_group_mutable(c, label);
        insert_rust_benchmark(&mut group, self.rust_map.clone(), word);
        insert_c_benchmark(&mut group, &self.keys, word);
        group.finish();
    }

    /// Benchmark the removal operation.
    ///
    /// The benchmark group will be marked with the given label.
    pub fn remove_group(&self, c: &mut Criterion, word: &str, label: &str) {
        let mut group = self.benchmark_group_mutable(c, label);
        remove_rust_benchmark(&mut group, self.rust_map.clone(), word);
        remove_c_benchmark(&mut group, &self.keys, word);
        group.finish();
    }

    /// Benchmark loading a corpus of words.
    pub fn load_group(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group_mutable(c, "Load");
        load_rust_benchmark(&mut group, &self.keys);
        load_c_benchmark(&mut group, &self.keys);
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

fn insert_c_benchmark<M: Measurement>(c: &mut BenchmarkGroup<'_, M>, keys: &[String], word: &str) {
    let (c_word, c_len) = str2c_input(word);
    c.bench_function("C", |b| {
        b.iter_batched_ref(
            || c_load_from_terms(keys),
            |data| data.insert(black_box(c_word), black_box(c_len)),
            BatchSize::LargeInput,
        )
    });
}

fn find_c_benchmark<M: Measurement>(c: &mut BenchmarkGroup<'_, M>, keys: &[String], word: &str) {
    let (c_word, c_len) = str2c_input(word);
    let map = c_load_from_terms(keys);
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

fn remove_c_benchmark<M: Measurement>(c: &mut BenchmarkGroup<'_, M>, keys: &[String], word: &str) {
    let (c_word, c_len) = str2c_input(word);
    c.bench_function("C", |b| {
        b.iter_batched_ref(
            || c_load_from_terms(keys),
            |data| data.remove(black_box(c_word), black_box(c_len)),
            BatchSize::LargeInput,
        )
    });
}

fn load_rust_benchmark<M: Measurement>(group: &mut BenchmarkGroup<'_, M>, keys: &[String]) {
    group.bench_function("Rust", |b| {
        b.iter_batched(
            || keys.iter().map(|s| str2c_char(s)).collect::<Vec<_>>(),
            |data| rust_load(black_box(&data)),
            BatchSize::LargeInput,
        )
    });
}

fn load_c_benchmark<M: Measurement>(group: &mut BenchmarkGroup<'_, M>, contents: &[String]) {
    group.bench_function("C", |b| {
        b.iter_batched(
            || contents.iter().map(|s| str2c_input(&s)).collect::<Vec<_>>(),
            |data| c_load(black_box(data)),
            BatchSize::LargeInput,
        )
    });
}

pub fn rust_load_from_terms(keys: &[String]) -> RustTrieMap {
    let words = keys.iter().map(|s| str2c_char(s)).collect::<Vec<_>>();
    rust_load(&words)
}

fn rust_load(words: &[Box<[c_char]>]) -> RustTrieMap {
    let mut map = trie_rs::TrieMap::new();
    for word in words {
        map.insert(word, NonNull::<c_void>::dangling());
    }
    map
}

fn c_load(words: Vec<(*mut c_char, u16)>) -> CTrieMap {
    let mut map = CTrieMap::new();
    for (word, len) in words {
        map.insert(word, len);
    }
    map
}

fn c_load_from_terms(contents: &[String]) -> CTrieMap {
    let words = contents.iter().map(|s| str2c_input(&s)).collect::<Vec<_>>();
    c_load(words)
}

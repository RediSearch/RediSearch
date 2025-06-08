/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use criterion::{
    BatchSize, BenchmarkGroup, Criterion,
    measurement::{Measurement, WallTime},
};
use lending_iterator::LendingIterator;
use std::{ffi::c_void, hint::black_box, ptr::NonNull, time::Duration};
use trie_rs::iter::{ContainsLendingIter, LendingIter};
use trie_rs::iter::{RangeFilter, RangeLendingIter};
use wildcard::WildcardPattern;

use crate::RustTrieMap;

/// A helper struct for benchmarking operations on different trie map implementations.
pub struct OperationBencher {
    map: RustTrieMap,

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
    immutable: Duration,

    /// Measurement time for mutable operations, e.g. insert, remove
    mutable: Duration,
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
            immutable: mutable_measurement_time.mul_f32(0.2),
            mutable: mutable_measurement_time,
        }
    }
}

impl Default for CorpusMeasurementTime {
    fn default() -> Self {
        Self {
            immutable: Duration::from_secs(5),
            mutable: Duration::from_secs(5),
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
            map: rust_map,
            keys: terms,
            measurement_times,
        }
    }

    fn benchmark_group_mutable<'a>(
        &self,
        c: &'a mut Criterion,
        label: &str,
    ) -> BenchmarkGroup<'a, WallTime> {
        let mut group = c.benchmark_group(format!("{}|{}", self.prefix, label));
        group.measurement_time(self.measurement_times.mutable);
        group
    }

    fn benchmark_group_immutable<'a>(
        &self,
        c: &'a mut Criterion,
        label: &str,
    ) -> BenchmarkGroup<'a, WallTime> {
        let mut group = c.benchmark_group(format!("{}|{}", self.prefix, label));
        group.measurement_time(self.measurement_times.immutable);
        group
    }

    /// Benchmark the find operation.
    ///
    /// The benchmark group will be marked with the given label.
    pub fn find_group(&self, c: &mut Criterion, word: &str, label: &str) {
        let mut group = self.benchmark_group_immutable(c, label);
        find_rust_benchmark(&mut group, &self.map, word);
        group.finish();
    }

    /// Benchmark the insert operation.
    ///
    /// The benchmark group will be marked with the given label.
    pub fn insert_group(&self, c: &mut Criterion, word: &str, label: &str) {
        let mut group = self.benchmark_group_mutable(c, label);
        insert_rust_benchmark(&mut group, self.map.clone(), word);
        group.finish();
    }

    /// Benchmark the removal operation.
    ///
    /// The benchmark group will be marked with the given label.
    pub fn remove_group(&self, c: &mut Criterion, word: &str, label: &str) {
        let mut group = self.benchmark_group_mutable(c, label);
        remove_rust_benchmark(&mut group, self.map.clone(), word);
        group.finish();
    }

    /// Benchmark loading a corpus of words.
    pub fn load_group(&self, c: &mut Criterion) {
        let mut group = self.benchmark_group_mutable(c, "Load");
        load_rust_benchmark(&mut group, &self.keys);
        group.finish();
    }

    /// Benchmark the find prefixes iterator.
    ///
    /// The benchmark group will be marked with the given label.
    pub fn find_prefixes_group(&self, c: &mut Criterion, target: &str, label: &str) {
        let mut group = self.benchmark_group_immutable(c, label);
        find_prefixes_rust_benchmark(&mut group, &self.map, target);
        group.finish();
    }

    /// Benchmark the wildcard iterator.
    ///
    /// The benchmark group will be marked with the given label.
    pub fn wildcard_group(&self, c: &mut Criterion, target: &str) {
        let label = format!("Wildcard [{target}]");
        let mut group = self.benchmark_group_immutable(c, &label);
        wildcard_rust_benchmark(&mut group, &self.map, target);
        group.finish();
    }

    /// Benchmark the range iterator.
    ///
    /// The benchmark group will be marked with the given label.
    pub fn range_group(&self, c: &mut Criterion, range: RangeFilter) {
        let label = format!("Range [{range}]");
        let mut group = self.benchmark_group_immutable(c, &label);
        range_rust_benchmark(&mut group, &self.map, range);
        group.finish();
    }

    /// Benchmark the `IntoValues` iterator.
    ///
    /// The benchmark group will be marked with the given label.
    pub fn into_values_group(&self, c: &mut Criterion, label: &str) {
        let mut group = self.benchmark_group_mutable(c, label);
        into_values_benchmark(&mut group, &self.map);
        group.finish();
    }

    /// Benchmark the `ContainsIter` iterator.
    ///
    /// The benchmark group will be marked with the given label.
    pub fn contains_group(&self, c: &mut Criterion, target: &str) {
        let label = format!("Contains [{target}]");
        let mut group = self.benchmark_group_mutable(c, &label);
        contains_rust_benchmark(&mut group, &self.map, target);
        group.finish();
    }
}

fn contains_rust_benchmark<M: Measurement>(
    c: &mut BenchmarkGroup<'_, M>,
    map: &RustTrieMap,
    target: &str,
) {
    c.bench_function("Rust", |b| {
        b.iter(|| {
            let mut iter: ContainsLendingIter<_> =
                map.contains_iter(black_box(target.as_bytes())).into();
            while let Some(entry) = LendingIterator::next(&mut iter) {
                black_box(entry);
            }
        })
    });
}

fn into_values_benchmark<M: Measurement>(c: &mut BenchmarkGroup<'_, M>, map: &RustTrieMap) {
    c.bench_function("Rust", |b| {
        b.iter_batched(
            || map.clone(),
            |map| {
                for value in map.into_values() {
                    black_box(value);
                }
            },
            BatchSize::LargeInput,
        )
    });
}

fn range_rust_benchmark<M: Measurement>(
    c: &mut BenchmarkGroup<'_, M>,
    map: &RustTrieMap,
    range: RangeFilter,
) {
    c.bench_function("Rust", |b| {
        b.iter(|| {
            let mut iter: RangeLendingIter<_> = map.range_iter(black_box(range)).into();
            while let Some(entry) = LendingIterator::next(&mut iter) {
                black_box(entry);
            }
        })
    });
}

fn wildcard_rust_benchmark<M: Measurement>(
    c: &mut BenchmarkGroup<'_, M>,
    map: &RustTrieMap,
    pattern: &str,
) {
    c.bench_function("Rust", |b| {
        b.iter(|| {
            let filter = WildcardPattern::parse(black_box(pattern.as_bytes()));
            let mut iter: LendingIter<'_, _, _> = map.wildcard_iter(filter).into();
            while let Some(entry) = LendingIterator::next(&mut iter) {
                black_box(entry);
            }
        })
    });
}

fn find_prefixes_rust_benchmark<M: Measurement>(
    c: &mut BenchmarkGroup<'_, M>,
    map: &RustTrieMap,
    target: &str,
) {
    let target = target.as_bytes();
    c.bench_function("Rust", |b| {
        b.iter(|| map.prefixes_iter(black_box(target)).collect::<Vec<_>>())
    });
}

fn find_rust_benchmark<M: Measurement>(
    c: &mut BenchmarkGroup<'_, M>,
    map: &RustTrieMap,
    word: &str,
) {
    let word = word.as_bytes();
    c.bench_function("Rust", |b| b.iter(|| map.find(black_box(word)).is_some()));
}

fn insert_rust_benchmark<M: Measurement>(
    c: &mut BenchmarkGroup<'_, M>,
    map: RustTrieMap,
    word: &str,
) {
    let word = word.as_bytes();
    c.bench_function("Rust", |b| {
        b.iter_batched_ref(
            || map.clone(),
            |data| {
                data.insert(black_box(word), black_box(NonNull::<c_void>::dangling()))
                    .is_some()
            },
            BatchSize::LargeInput,
        )
    });
}

fn remove_rust_benchmark<M: Measurement>(
    c: &mut BenchmarkGroup<'_, M>,
    map: RustTrieMap,
    word: &str,
) {
    let bytes = word.as_bytes();
    c.bench_function("Rust", |b| {
        b.iter_batched_ref(
            || map.clone(),
            |data| data.remove(black_box(&bytes)).is_some(),
            BatchSize::LargeInput,
        )
    });
}

fn load_rust_benchmark<M: Measurement>(group: &mut BenchmarkGroup<'_, M>, keys: &[String]) {
    group.bench_function("Rust", |b| {
        b.iter_batched(
            || keys.iter().map(|s| s.as_bytes()).collect::<Vec<_>>(),
            |data| rust_load(black_box(&data)),
            BatchSize::LargeInput,
        )
    });
}

pub fn rust_load_from_terms(keys: &[String]) -> RustTrieMap {
    let words = keys.iter().map(|s| s.as_bytes()).collect::<Vec<_>>();
    rust_load(&words)
}

fn rust_load(words: &[&[u8]]) -> RustTrieMap {
    let mut map = trie_rs::TrieMap::new();
    for word in words {
        map.insert(word, NonNull::<c_void>::dangling());
    }
    map
}

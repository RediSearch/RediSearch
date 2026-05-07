/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Benchmark harness mirroring [`crate::OperationBencher`] but targeting
//! [`trie_rs::RuneTrieMap`] — the rune-keyed wrapper that replaced the
//! C `Trie *` for the suffix index in phase 1.
//!
//! The C-side baseline (rune-keyed `Trie`) is intentionally not wired up
//! yet; this layer establishes the Rust-only numbers first so any later
//! C comparison has a stable reference point.

use std::{ffi::c_void, hint::black_box, ptr::NonNull, time::Duration};

use criterion::{
    BenchmarkGroup, Criterion,
    measurement::{Measurement, WallTime},
};
use trie_rs::{RuneBound, RuneTrieMap};

use crate::bencher::CorpusMeasurementTime;

/// Convenient alias for the rune-keyed trie under benchmark.
pub type RustRuneTrieMap = RuneTrieMap<NonNull<c_void>>;

/// A bencher that drives [`RustRuneTrieMap`] operations against a fixed
/// corpus of rune-converted terms.
pub struct RuneOperationBencher {
    map: RustRuneTrieMap,
    /// Pre-converted UTF-16 form of every input term, kept around so each
    /// bench iteration can reuse the runes without re-encoding cost
    /// dominating the measurement.
    rune_keys: Vec<Vec<u16>>,
    measurement_times: CorpusMeasurementTime,
    prefix: String,
}

impl RuneOperationBencher {
    /// Build a bencher from the same `Vec<String>` corpus the byte-keyed
    /// [`crate::OperationBencher`] consumes.
    pub fn new(
        prefix: String,
        terms: Vec<String>,
        mutable_measurement_time: Option<Duration>,
    ) -> Self {
        let rune_keys: Vec<Vec<u16>> = terms.iter().map(|s| s.encode_utf16().collect()).collect();
        let map = rune_load(&rune_keys);
        let measurement_time = mutable_measurement_time.unwrap_or(Duration::from_secs(5));
        let measurement_times = CorpusMeasurementTime::from_mutable_trie(measurement_time);
        Self {
            map,
            rune_keys,
            measurement_times,
            prefix,
        }
    }

    fn group_immutable<'a>(
        &self,
        c: &'a mut Criterion,
        label: &str,
    ) -> BenchmarkGroup<'a, WallTime> {
        let mut group = c.benchmark_group(format!("{}|{}", self.prefix, label));
        group.measurement_time(self.measurement_times.immutable());
        group
    }

    /// Visits every entry whose key starts with `prefix`.
    pub fn prefixed_iter_group(&self, c: &mut Criterion, prefix: &str, label: &str) {
        let mut group = self.group_immutable(c, label);
        let prefix_runes: Vec<u16> = prefix.encode_utf16().collect();
        prefixed_iter_rune_benchmark(&mut group, &self.map, &prefix_runes);
        group.finish();
    }

    /// Visits every entry whose key matches the rune-level wildcard
    /// pattern.
    pub fn wildcard_group(&self, c: &mut Criterion, pattern: &str) {
        let label = format!("Wildcard [{pattern}]");
        let mut group = self.group_immutable(c, &label);
        let pattern_runes: Vec<u16> = pattern.encode_utf16().collect();
        wildcard_rune_benchmark(&mut group, &self.map, &pattern_runes);
        group.finish();
    }

    /// Visits every entry within the inclusive rune range `[min, max]`.
    pub fn range_inclusive_group(&self, c: &mut Criterion, min: &str, max: &str) {
        let label = format!("Range [{min}..={max}]");
        let mut group = self.group_immutable(c, &label);
        let min_runes: Vec<u16> = min.encode_utf16().collect();
        let max_runes: Vec<u16> = max.encode_utf16().collect();
        range_rune_benchmark(&mut group, &self.map, &min_runes, &max_runes);
        group.finish();
    }

    /// Visits every entry whose key contains `target` as a rune-aligned
    /// substring (anywhere in the key).
    pub fn contains_group(&self, c: &mut Criterion, target: &str) {
        let label = format!("Contains [{target}]");
        let mut group = self.group_immutable(c, &label);
        let target_runes: Vec<u16> = target.encode_utf16().collect();
        contains_rune_benchmark(&mut group, &self.map, &target_runes);
        group.finish();
    }

    /// Number of rune-converted terms backing the benched map.
    pub const fn key_count(&self) -> usize {
        self.rune_keys.len()
    }
}

fn prefixed_iter_rune_benchmark<M: Measurement>(
    g: &mut BenchmarkGroup<'_, M>,
    map: &RustRuneTrieMap,
    prefix: &[u16],
) {
    g.bench_function("Rust", |b| {
        b.iter(|| {
            for entry in map.prefixed_iter(black_box(prefix)) {
                black_box(entry);
            }
        })
    });
}

fn wildcard_rune_benchmark<M: Measurement>(
    g: &mut BenchmarkGroup<'_, M>,
    map: &RustRuneTrieMap,
    pattern: &[u16],
) {
    g.bench_function("Rust", |b| {
        b.iter(|| {
            map.wildcard_iter(black_box(pattern), |k, v| {
                black_box((k, v));
            });
        })
    });
}

fn range_rune_benchmark<M: Measurement>(
    g: &mut BenchmarkGroup<'_, M>,
    map: &RustRuneTrieMap,
    min: &[u16],
    max: &[u16],
) {
    g.bench_function("Rust", |b| {
        b.iter(|| {
            map.range_iter(
                Some(RuneBound::included(black_box(min))),
                Some(RuneBound::included(black_box(max))),
                |k, v| {
                    black_box((k, v));
                },
            );
        })
    });
}

fn contains_rune_benchmark<M: Measurement>(
    g: &mut BenchmarkGroup<'_, M>,
    map: &RustRuneTrieMap,
    target: &[u16],
) {
    g.bench_function("Rust", |b| {
        b.iter(|| {
            map.contains_iter(black_box(target), false, false, |k, v| {
                black_box((k, v));
            });
        })
    });
}

fn rune_load(keys: &[Vec<u16>]) -> RustRuneTrieMap {
    let mut map = RuneTrieMap::new();
    for k in keys {
        map.insert_replace(k, NonNull::<c_void>::dangling());
    }
    map
}

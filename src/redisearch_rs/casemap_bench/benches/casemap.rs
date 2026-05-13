/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Side-by-side perf benchmarks for libnu vs `icu_casemap`.
//!
//! Pairs each corpus (`casemap_bench::ALL`) under both operations (fold,
//! lowercase) for both backends. Criterion groups put libnu and ICU
//! adjacent in the report so per-corpus ratios are easy to read.
//!
//! - `casemap_bench::fold_libnu` calls `casemap_compare::fold_libnu`,
//!   which exercises libnu's `nu_tofold` table (used by RediSearch's
//!   `runeFold` in `src/trie/rune_util.c`) but re-encodes the multi-
//!   codepoint output through Rust's native UTF-8 — so it does *not*
//!   pay the `nu_utf8_write` cost. Use it as a lower bound for libnu
//!   fold cost, not a faithful reproduction of the C call site.
//! - `casemap_bench::lower_libnu` calls `casemap_compare::lower_libnu`,
//!   the same per-codepoint loop pattern as the C `unicode_tolower`
//!   hot path in `src/util/strconv.h`.
//! - ICU sides call `CaseMapper::fold_string` and
//!   `CaseMapper::lowercase_to_string` (root locale, `und`).

use std::hint::black_box;

use casemap_bench::ALL;
use casemap_compare::{fold_icu, fold_libnu, lower_icu, lower_libnu};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

fn bench_fold(c: &mut Criterion) {
    let mut group = c.benchmark_group("fold");
    for (name, input) in ALL {
        group.throughput(Throughput::Bytes(input.len() as u64));
        group.bench_with_input(BenchmarkId::new("libnu", name), input, |b, &s| {
            b.iter(|| fold_libnu(black_box(s)));
        });
        group.bench_with_input(BenchmarkId::new("icu", name), input, |b, &s| {
            b.iter(|| fold_icu(black_box(s)));
        });
    }
    group.finish();
}

fn bench_lower(c: &mut Criterion) {
    let mut group = c.benchmark_group("lower");
    for (name, input) in ALL {
        group.throughput(Throughput::Bytes(input.len() as u64));
        group.bench_with_input(BenchmarkId::new("libnu", name), input, |b, &s| {
            b.iter(|| lower_libnu(black_box(s)));
        });
        group.bench_with_input(BenchmarkId::new("icu", name), input, |b, &s| {
            b.iter(|| lower_icu(black_box(s)));
        });
    }
    group.finish();
}

criterion_group!(benches, bench_fold, bench_lower);
criterion_main!(benches);

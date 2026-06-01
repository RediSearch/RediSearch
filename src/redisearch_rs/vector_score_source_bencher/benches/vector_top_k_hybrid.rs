/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Apples-to-apples hybrid benchmark: Rust `VectorTopKIterator` vs the *real* C
//! `HybridIterator` (`iterators/hybrid_reader.c`).
//!
//! Both sides:
//! - drive the same HNSW index and the same sorted-id child filter,
//! - maintain a real bounded top-k heap,
//! - are forced into the same execution mode (batches / adhoc-BF).
//!
//! The C side builds a minimal mock `RedisSearchCtx` and wraps the child in
//! `NewSortedIdListIterator` (the C twin of the Rust `IdList`). See
//! `hybrid_shim.c` for details.
//!
//! Two groups × two sides (rust / c):
//!
//! - `vector_top_k_hybrid/batches` — hybrid, batches mode forced.
//! - `vector_top_k_hybrid/adhoc`   — hybrid, adhoc-BF mode forced.
//!
//! Parameters swept: `n ∈ [10_000, 100_000]`, `k ∈ [10, 100]`, `dim = 128`.

// Pull in the lib to ensure FFI stubs and mock allocator symbols are linked.
use vector_score_source_bencher as _;

use std::hint::black_box;
use std::{
    ffi::{c_int, c_void},
    num::NonZeroUsize,
};

use std::cmp::Ordering;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use ffi::{
    HNSWParams, RedisModule_Alloc, VecSimAlgo_VecSimAlgo_HNSWLIB, VecSimIndex,
    VecSimIndex_AddVector, VecSimIndex_Free, VecSimIndex_New, VecSimMetric_VecSimMetric_L2,
    VecSimParams, VecSimQueryParams, VecSimType_VecSimType_FLOAT32, timespec,
};
use rqe_iterators::{IdList, RQEIterator};
use top_k::{TopKIterator, TopKMode};
use vector_score_source::VectorScoreSource;

/// Score order for vector distance: ascending (lower distance = better).
/// Matches the comparator `vector_score_source` uses internally.
fn asc_cmp(a: f64, b: f64) -> Ordering {
    a.partial_cmp(&b).unwrap_or(Ordering::Equal)
}

// ── C shim (from hybrid_shim.c, compiled by build.rs) ────────────────────────

unsafe extern "C" {
    /// Drive the real C `HybridIterator` over a sorted-id child and count
    /// results. `force_adhoc != 0` forces adhoc-BF, otherwise batches mode.
    fn bench_c_hybrid(
        index: *mut VecSimIndex,
        query_vec: *const c_void,
        dim: usize,
        k: usize,
        ids: *mut u64,
        child_count: usize,
        force_adhoc: c_int,
    ) -> usize;
}

/// Allocate a `RedisModule_Alloc`-owned copy of `ids`, so the child iterator's
/// `OwnedSlice` can free it via the matching `RedisModule_Free` (same mock
/// allocator pair). Ownership is transferred to `bench_c_hybrid`.
fn alloc_owned_ids(ids: &[u64]) -> *mut u64 {
    // SAFETY: `RedisModule_Alloc` is the mock allocator, initialised by linking
    // redis_mock. We copy `ids.len()` u64s into the freshly allocated buffer.
    unsafe {
        let ptr = RedisModule_Alloc.unwrap()(std::mem::size_of_val(ids)) as *mut u64;
        std::ptr::copy_nonoverlapping(ids.as_ptr(), ptr, ids.len());
        ptr
    }
}

const DIM: usize = 128;

/// Create an HNSW index, populate it with `n` random FLOAT32 vectors, and
/// return the raw pointer. Caller frees via `VecSimIndex_Free`.
unsafe fn make_index(n: usize) -> *mut VecSimIndex {
    let params = VecSimParams {
        algo: VecSimAlgo_VecSimAlgo_HNSWLIB,
        algoParams: ffi::AlgoParams {
            hnswParams: HNSWParams {
                type_: VecSimType_VecSimType_FLOAT32,
                dim: DIM,
                metric: VecSimMetric_VecSimMetric_L2,
                multi: false,
                initialCapacity: n,
                blockSize: 1024,
                M: 16,
                efConstruction: 200,
                efRuntime: 10,
                epsilon: 0.01,
            },
        },
        logCtx: std::ptr::null_mut(),
    };
    // SAFETY: `params` is fully initialised.
    let index = unsafe { VecSimIndex_New(&params) };
    assert!(!index.is_null(), "VecSimIndex_New failed");

    let mut rng_state: u64 = 0xDEAD_BEEF_1234_5678;
    for label in 1..=n {
        let vec: Vec<f32> = (0..DIM)
            .map(|_| {
                rng_state ^= rng_state << 13;
                rng_state ^= rng_state >> 7;
                rng_state ^= rng_state << 17;
                (rng_state as f32) / (u64::MAX as f32)
            })
            .collect();
        // SAFETY: `index` is valid; `vec` is `DIM` f32s.
        unsafe {
            VecSimIndex_AddVector(index, vec.as_ptr() as *const c_void, label);
        }
    }
    index
}

/// Generate a single random query vector of `DIM` f32s.
fn random_query(seed: u64) -> Vec<f32> {
    let mut s = seed;
    (0..DIM)
        .map(|_| {
            s ^= s << 13;
            s ^= s >> 7;
            s ^= s << 17;
            (s as f32) / (u64::MAX as f32)
        })
        .collect()
}

/// Generate `count` sorted child IDs drawn uniformly from `[1, n]`.
fn child_ids(n: usize, count: usize) -> Vec<usize> {
    let step = (n / count).max(1);
    (0..count).map(|i| (i * step + 1).min(n)).collect()
}

/// Reinterpret a `Vec<f32>` as `Vec<u8>` (byte blob for VectorScoreSource).
fn bytemuck_cast_vec(v: Vec<f32>) -> Vec<u8> {
    let mut bytes = vec![0u8; v.len() * 4];
    for (i, f) in v.iter().enumerate() {
        bytes[i * 4..i * 4 + 4].copy_from_slice(&f.to_ne_bytes());
    }
    bytes
}

/// Run one Rust hybrid scan to depletion, returning the result count.
// ── Comparability of the two sides ───────────────────────────────────────────
//
// Both `run_rust` and `run_c` receive the same index, query, k, and sorted
// child-id slice. The execution mode (batches / adhoc-BF) is passed explicitly
// to both rather than inferred from child-size heuristics, so each group is
// guaranteed to exercise the intended code path regardless of index state.
//
// The only structural difference is the child-iterator type:
//   Rust — `IdList::<true>` (sorted), which also drives batch-size estimation
//           via `child_num_estimated = ids.len()`.
//   C    — `NewSortedIdListIterator` over a `RedisModule_Alloc`-owned copy of
//           the same ids. `NumEstimated` returns `ids.len()`, so estimates match.

/// Run one Rust hybrid scan with the given explicit mode, returning result count.
fn run_rust(
    index: *mut VecSimIndex,
    query: &[f32],
    k: NonZeroUsize,
    ids: &[u64],
    mode: TopKMode,
) -> usize {
    // SAFETY: `index` lives for the duration of this benchmark.
    let source: VectorScoreSource = unsafe {
        VectorScoreSource::new(
            std::ptr::NonNull::new(index).unwrap(),
            bytemuck_cast_vec(query.to_vec()),
            std::mem::zeroed::<VecSimQueryParams>(),
            k,
            timespec {
                tv_sec: 0,
                tv_nsec: 0,
            },
            true,
            false,
            false,
            ids.len(),
            0,
            None,
        )
    };
    let child: Box<dyn RQEIterator> = Box::new(IdList::<true>::new(ids.to_vec()));
    let mut it = TopKIterator::new_with_mode(source, Some(child), k, asc_cmp, mode);
    let mut count = 0usize;
    while it.read().unwrap().is_some() {
        count += 1;
    }
    count
}

/// Run one C HybridIterator scan to depletion, returning result count.
/// `force_adhoc` must match the `TopKMode` passed to `run_rust`.
fn run_c(
    index: *mut VecSimIndex,
    query: &[f32],
    k: NonZeroUsize,
    ids: &[u64],
    force_adhoc: bool,
) -> usize {
    // Fresh owned copy each call: the child iterator frees it via RedisModule_Free.
    let ids_ptr = alloc_owned_ids(ids);
    // SAFETY: `index` is valid; `ids_ptr` is a sorted, owned array of `ids.len()`
    // ids whose ownership transfers to the call.
    unsafe {
        bench_c_hybrid(
            index,
            query.as_ptr() as *const c_void,
            DIM,
            k.get(),
            ids_ptr,
            ids.len(),
            force_adhoc as c_int,
        )
    }
}

/// Validate that both sides produce the expected result count before the timed
/// loop starts.  Catches iterator bugs, shim error paths, and mode mismatches
/// that would otherwise silently corrupt the Rust/C timing comparison.
fn preflight(
    index: *mut VecSimIndex,
    query: &[f32],
    k: NonZeroUsize,
    ids: &[u64],
    rust_mode: TopKMode,
    force_adhoc: bool,
) {
    let expected = k.get().min(ids.len());

    let rust_count = run_rust(index, query, k, ids, rust_mode);
    assert_eq!(
        rust_count,
        expected,
        "Rust iterator produced {rust_count} results (expected {expected}) \
         for k={k}, ids.len()={len}, mode={rust_mode:?}",
        len = ids.len()
    );

    let c_count = run_c(index, query, k, ids, force_adhoc);
    assert_eq!(
        c_count,
        expected,
        "C shim produced {c_count} results (expected {expected}) \
         for k={k}, ids.len()={len}, force_adhoc={force_adhoc}",
        len = ids.len()
    );
}

// ── Batches ────────────────────────────────────────────────────────────────

fn bench_batches(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector_top_k_hybrid/batches");

    for n in [10_000usize, 100_000] {
        // SAFETY: make_index + VecSimIndex_Free are paired.
        let index = unsafe { make_index(n) };

        for k in [10usize, 100].map(|k| NonZeroUsize::new(k).unwrap()) {
            // ~10% child; mode is forced to Batches regardless of heuristics.
            let child_count = (n / 10).max(k.get());
            let ids: Vec<u64> = child_ids(n, child_count)
                .iter()
                .map(|&id| id as u64)
                .collect();
            let query = random_query(99);
            let param_str = format!("n{n}_k{k}");

            preflight(index, &query, k, &ids, TopKMode::ForcedBatches, false);

            group.bench_with_input(BenchmarkId::new("rust", &param_str), &(n, k), |b, _| {
                b.iter(|| black_box(run_rust(index, &query, k, &ids, TopKMode::ForcedBatches)))
            });

            group.bench_with_input(BenchmarkId::new("c", &param_str), &(n, k), |b, _| {
                b.iter(|| black_box(run_c(index, &query, k, &ids, false)))
            });
        }

        // SAFETY: `index` was created with make_index.
        unsafe { VecSimIndex_Free(index) };
    }

    group.finish();
}

// ── Adhoc BF ─────────────────────────────────────────────────────────────────

fn bench_adhoc(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector_top_k_hybrid/adhoc");

    for n in [10_000usize, 100_000] {
        let index = unsafe { make_index(n) };

        for k in [10usize, 100].map(|k| NonZeroUsize::new(k).unwrap()) {
            // k*5 child; mode is forced to AdhocBF regardless of heuristics.
            let child_count = k.get() * 5;
            let ids: Vec<u64> = child_ids(n, child_count)
                .iter()
                .map(|&id| id as u64)
                .collect();
            let query = random_query(7);
            let param_str = format!("n{n}_k{k}");

            preflight(index, &query, k, &ids, TopKMode::AdhocBF, true);

            group.bench_with_input(BenchmarkId::new("rust", &param_str), &(n, k), |b, _| {
                b.iter(|| black_box(run_rust(index, &query, k, &ids, TopKMode::AdhocBF)))
            });

            group.bench_with_input(BenchmarkId::new("c", &param_str), &(n, k), |b, _| {
                b.iter(|| black_box(run_c(index, &query, k, &ids, true)))
            });
        }

        unsafe { VecSimIndex_Free(index) };
    }

    group.finish();
}

// ── Adhoc vs Batches mode comparison ─────────────────────────────────────────
//
// Three child-size configurations, all with n=100_000 and k=100:
//
//   adhoc_favored   — child_count=500  (0.5% of n, ~5×k):
//       AdhocBF does 500 vector lookups; Batches must run many HNSW rounds to
//       accumulate 100 results from a 0.5% pass filter → adhoc should win.
//
//   balanced        — child_count=8_000 (8% of n, ~80×k):
//       Both modes have comparable cost near the natural crossover point.
//
//   batches_favored — child_count=70_000 (70% of n, 700×k):
//       AdhocBF must scan 70 000 docs; Batches finds 100 HNSW candidates and
//       most pass the loose filter → batches should win.
//
// Each configuration benchmarks rust/adhoc, rust/batches, c/adhoc, c/batches.

struct ModeCase {
    label: &'static str,
    child_count: usize,
}

const MODE_CASES: &[ModeCase] = &[
    ModeCase {
        label: "adhoc_favored",
        child_count: 500,
    },
    ModeCase {
        label: "balanced",
        child_count: 8_000,
    },
    ModeCase {
        label: "batches_favored",
        child_count: 70_000,
    },
];

fn bench_mode_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector_top_k_hybrid/mode_comparison");

    const N: usize = 100_000;
    let k = NonZeroUsize::new(100).unwrap();

    // Build a single index for all cases (same n).
    // SAFETY: make_index + VecSimIndex_Free are paired.
    let index = unsafe { make_index(N) };
    let query = random_query(42);

    for case in MODE_CASES {
        let ids: Vec<u64> = child_ids(N, case.child_count)
            .iter()
            .map(|&id| id as u64)
            .collect();

        preflight(index, &query, k, &ids, TopKMode::AdhocBF, true);
        preflight(index, &query, k, &ids, TopKMode::ForcedBatches, false);

        group.bench_with_input(BenchmarkId::new("rust_adhoc", case.label), &(), |b, _| {
            b.iter(|| black_box(run_rust(index, &query, k, &ids, TopKMode::AdhocBF)));
        });

        group.bench_with_input(BenchmarkId::new("rust_batches", case.label), &(), |b, _| {
            b.iter(|| black_box(run_rust(index, &query, k, &ids, TopKMode::ForcedBatches)));
        });

        group.bench_with_input(BenchmarkId::new("c_adhoc", case.label), &(), |b, _| {
            b.iter(|| black_box(run_c(index, &query, k, &ids, true)));
        });

        group.bench_with_input(BenchmarkId::new("c_batches", case.label), &(), |b, _| {
            b.iter(|| black_box(run_c(index, &query, k, &ids, false)));
        });
    }

    // SAFETY: `index` was created with make_index.
    unsafe { VecSimIndex_Free(index) };

    group.finish();
}

criterion_group!(benches, bench_batches, bench_adhoc, bench_mode_comparison);
criterion_main!(benches);

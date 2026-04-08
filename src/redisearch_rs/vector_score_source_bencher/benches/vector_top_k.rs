/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! M2b benchmarks: Rust VectorTopKIterator vs C VecSim direct API.
//!
//! Three groups × two sides (rust / c):
//!
//! - `vector_top_k/unfiltered` — pure KNN, no filter child.
//! - `vector_top_k/batches`    — hybrid, batch mode forced.
//! - `vector_top_k/adhoc`      — hybrid, adhoc-BF mode forced.
//!
//! Parameters swept: `n ∈ [10_000, 100_000]`, `k ∈ [10, 100]`, `dim = 128`.

// Pull in the lib to ensure FFI stubs and mock allocator symbols are linked.
use vector_score_source_bencher as _;

use std::hint::black_box;
use std::{ffi::c_void, num::NonZeroUsize};

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use ffi::{
    HNSWParams, VecSimAlgo_VecSimAlgo_HNSWLIB, VecSimIndex, VecSimIndex_AddVector,
    VecSimIndex_Free, VecSimIndex_New, VecSimMetric_VecSimMetric_L2, VecSimParams,
    VecSimQueryParams, VecSimType_VecSimType_FLOAT32,
};
use rqe_iterators::{IdList, RQEIterator};
use vector_score_source::{
    VectorScoreSource, new_vector_top_k_filtered_boxed, new_vector_top_k_unfiltered,
};

// ── C shims (from bench_shim.c, compiled by build.rs) ───────────────────────

unsafe extern "C" {
    fn bench_c_unfiltered(index: *mut VecSimIndex, query_vec: *const c_void, k: usize) -> usize;

    fn bench_c_batches(
        index: *mut VecSimIndex,
        query_vec: *const c_void,
        k: usize,
        child_ids: *const usize,
        child_count: usize,
    ) -> usize;

    fn bench_c_adhoc(
        index: *mut VecSimIndex,
        query_vec: *const c_void,
        child_ids: *const usize,
        child_count: usize,
    ) -> usize;
}

const DIM: usize = 128;

/// Create an HNSW index, populate it with `n` random FLOAT32 vectors, and
/// return the raw pointer.  The caller is responsible for freeing via
/// `VecSimIndex_Free`.
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
    // Simple deterministic subset: every `step`-th ID.
    let step = (n / count).max(1);
    (0..count).map(|i| (i * step + 1).min(n)).collect()
}

// ── Unfiltered ───────────────────────────────────────────────────────────────

fn bench_unfiltered(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector_top_k/unfiltered");

    for n in [10_000usize, 100_000] {
        // SAFETY: make_index + VecSimIndex_Free are paired.
        let index = unsafe { make_index(n) };

        for k in [10usize, 100].map(|k| NonZeroUsize::new(k).unwrap()) {
            let query = random_query(42);
            let param_str = format!("n{n}_k{k}");

            // ── Rust ──────────────────────────────────────────────────────
            group.bench_with_input(BenchmarkId::new("rust", &param_str), &(n, k), |b, _| {
                b.iter(|| {
                    // SAFETY: `index` lives for the duration of this benchmark.
                    let source = unsafe {
                        VectorScoreSource::new(
                            index,
                            bytemuck_cast_vec(query.clone()),
                            std::mem::zeroed::<VecSimQueryParams>(),
                            k,
                            std::ptr::null_mut(),
                            false, // RAM index
                            0,     // child_num_estimated (0 = index_size)
                            0,     // fixed_batch_size (0 = dynamic)
                        )
                    };
                    let mut it = new_vector_top_k_unfiltered(source, k);
                    let mut count = 0usize;
                    while it.read().unwrap().is_some() {
                        count += 1;
                    }
                    black_box(count)
                })
            });

            // ── C ─────────────────────────────────────────────────────────
            group.bench_with_input(BenchmarkId::new("c", &param_str), &(n, k), |b, _| {
                b.iter(|| {
                    // SAFETY: `index` is valid.
                    let count = unsafe {
                        bench_c_unfiltered(index, query.as_ptr() as *const c_void, k.get())
                    };
                    black_box(count)
                })
            });
        }

        // SAFETY: `index` was created with make_index.
        unsafe { VecSimIndex_Free(index) };
    }

    group.finish();
}

// ── Batches ──────────────────────────────────────────────────────────────────

fn bench_batches(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector_top_k/batches");

    for n in [10_000usize, 100_000] {
        let index = unsafe { make_index(n) };

        for k in [10usize, 100].map(|k| NonZeroUsize::new(k).unwrap()) {
            // Child covers ~10% of index to force multiple batches.
            let child_count = (n / 10).max(k.get());
            let ids = child_ids(n, child_count);
            let query = random_query(99);
            let param_str = format!("n{n}_k{k}");

            // ── Rust ──────────────────────────────────────────────────────
            group.bench_with_input(BenchmarkId::new("rust", &param_str), &(n, k), |b, _| {
                b.iter(|| {
                    let source = unsafe {
                        VectorScoreSource::new(
                            index,
                            bytemuck_cast_vec(query.clone()),
                            std::mem::zeroed::<VecSimQueryParams>(),
                            k,
                            std::ptr::null_mut(),
                            false,
                            child_count,
                            0,
                        )
                    };
                    // Force Batches mode by using new_filtered_boxed; VecSim heuristics
                    // will pick the mode but we also force via large child_est.
                    let child: Box<dyn RQEIterator> = Box::new(IdList::<true>::new(
                        ids.iter().map(|&id| id as u64).collect::<Vec<_>>(),
                    ));
                    let mut it = new_vector_top_k_filtered_boxed(source, child, k);
                    let mut count = 0usize;
                    while it.read().unwrap().is_some() {
                        count += 1;
                    }
                    black_box(count)
                })
            });

            // ── C ─────────────────────────────────────────────────────────
            group.bench_with_input(BenchmarkId::new("c", &param_str), &(n, k), |b, _| {
                b.iter(|| {
                    let count = unsafe {
                        bench_c_batches(
                            index,
                            query.as_ptr() as *const c_void,
                            k.get(),
                            ids.as_ptr(),
                            ids.len(),
                        )
                    };
                    black_box(count)
                })
            });
        }

        unsafe { VecSimIndex_Free(index) };
    }

    group.finish();
}

// ── Adhoc BF ─────────────────────────────────────────────────────────────────

fn bench_adhoc(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector_top_k/adhoc");

    for n in [10_000usize, 100_000] {
        let index = unsafe { make_index(n) };

        for k in [10usize, 100].map(|k| NonZeroUsize::new(k).unwrap()) {
            // Small child to force adhoc path naturally.
            let child_count = k.get() * 5;
            let ids = child_ids(n, child_count);
            let query = random_query(7);
            let param_str = format!("n{n}_k{k}");

            // ── Rust ──────────────────────────────────────────────────────
            group.bench_with_input(BenchmarkId::new("rust", &param_str), &(n, k), |b, _| {
                b.iter(|| {
                    let source = unsafe {
                        VectorScoreSource::new(
                            index,
                            bytemuck_cast_vec(query.clone()),
                            std::mem::zeroed::<VecSimQueryParams>(),
                            k,
                            std::ptr::null_mut(),
                            false,
                            child_count,
                            0,
                        )
                    };
                    let child: Box<dyn RQEIterator> = Box::new(IdList::<true>::new(
                        ids.iter().map(|&id| id as u64).collect::<Vec<_>>(),
                    ));
                    // Force AdhocBF mode: small child_count → prefer_adhoc = true.
                    let mut it = new_vector_top_k_filtered_boxed(source, child, k);
                    let mut count = 0usize;
                    while it.read().unwrap().is_some() {
                        count += 1;
                    }
                    black_box(count)
                })
            });

            // ── C ─────────────────────────────────────────────────────────
            group.bench_with_input(BenchmarkId::new("c", &param_str), &(n, k), |b, _| {
                b.iter(|| {
                    let count = unsafe {
                        bench_c_adhoc(
                            index,
                            query.as_ptr() as *const c_void,
                            ids.as_ptr(),
                            ids.len(),
                        )
                    };
                    black_box(count)
                })
            });
        }

        unsafe { VecSimIndex_Free(index) };
    }

    group.finish();
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Reinterpret a `Vec<f32>` as `Vec<u8>` (byte blob for VectorScoreSource).
fn bytemuck_cast_vec(v: Vec<f32>) -> Vec<u8> {
    let mut bytes = vec![0u8; v.len() * 4];
    for (i, f) in v.iter().enumerate() {
        bytes[i * 4..i * 4 + 4].copy_from_slice(&f.to_ne_bytes());
    }
    bytes
}

criterion_group!(benches, bench_unfiltered, bench_batches, bench_adhoc);
criterion_main!(benches);

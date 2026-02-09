/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

/**************************************
  Define and register disk index benchmarks for fp32.
  NOTE: benchmarks' tests order can affect their results. Please add new benchmarks at the end of
  the file.
***************************************/

// =============================================================================
// Phase 1: Index Creation & Resource Usage Benchmarks
// =============================================================================

/**
 * Benchmark index creation across different dimensions.
 * Measures time to create and destroy a disk HNSW index.
 */
BENCHMARK_DEFINE_F(BM_DiskIndex, CreateIndex)(benchmark::State& state) {
    for (auto _ : state) {
        auto params_holder = CreateDiskParams();
        index_ = VecSimDisk_CreateIndex(&params_holder->params_disk);
        benchmark::DoNotOptimize(index_);
    }

    // Report metrics (index_ is still alive after single iteration)
    bm_common::ReportMemory(state, index_);
    bm_common::ReportDiskUsage(state, temp_db_->db());
}

BENCHMARK_REGISTER_F(BM_DiskIndex, CreateIndex)->Unit(benchmark::kMicrosecond)->Iterations(1);

// Phase 2: AddVector (stub for now)
// BENCHMARK_DEFINE_F(BM_DiskIndex, AddVector)(benchmark::State& state) { ... }

// Phase 3: TopK (stub for now)
// BENCHMARK_DEFINE_F(BM_DiskIndex, TopK)(benchmark::State& state) { ... }

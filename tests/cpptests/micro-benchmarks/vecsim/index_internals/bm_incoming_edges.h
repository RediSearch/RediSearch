/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include <benchmark/benchmark.h>

#include "VecSim/vec_sim.h"
#include "VecSim/vec_sim_common.h"
#include "VecSim/algorithms/hnsw/hnsw.h"
#include "VecSim/algorithms/hnsw/hnsw_tiered.h"
#include "VecSim/index_factories/tiered_factory.h"
#include "utils/mock_thread_pool.h"

#include <random>
#include <vector>
#include <iostream>
#include <algorithm>
#include <cstring>

// =============================================================================
// Constants for the stress scenario (MOD-13761 reproduction)
// =============================================================================
static constexpr size_t BM_DIM = 128;
static constexpr size_t BM_N_BASELINE = 40000; // Random baseline vectors
static constexpr size_t BM_N_ZERO = 50000;     // Zero vectors (stress case)
static constexpr size_t BM_M = 16;             // HNSW M parameter
static constexpr size_t BM_EF_C = 200;         // HNSW efConstruction
static constexpr size_t BM_INITIAL_CAP = BM_N_BASELINE + BM_N_ZERO;
static constexpr unsigned int BM_NUM_THREADS = 1; // Single bg thread for deterministic timing

// =============================================================================
// Base fixture class for incoming edges benchmarks (MOD-13761)
// =============================================================================
// Holds the tiered HNSW index, mock thread pool, and shared helpers for
// measuring ghost memory, shrinking incoming edges, and inserting vectors.
// Derived fixtures (Async, InPlace) implement the benchmark body.
class BM_IncomingEdgesBase : public benchmark::Fixture {
protected:
    tieredIndexMock *mock_tp_ = nullptr;
    TieredHNSWIndex<float, float> *tiered_index_ = nullptr;
    HNSWIndex<float, float> *hnsw_ = nullptr;
    VecSimWriteMode original_write_mode_;

    // --- Index lifecycle ---

    // Creates the tiered HNSW index with the stress scenario parameters.
    // swapJobThreshold=0 so swap jobs accumulate and we control when they run.
    // flatBufferLimit=SIZE_MAX so vectors go directly to HNSW via addVector.
    void create_tiered_index() {
        mock_tp_ = new tieredIndexMock(BM_NUM_THREADS);
        HNSWParams hnsw_params = {
            .type = VecSimType_FLOAT32,
            .dim = BM_DIM,
            .metric = VecSimMetric_Cosine,
            .initialCapacity = BM_INITIAL_CAP,
            .M = BM_M,
            .efConstruction = BM_EF_C,
        };
        VecSimParams vecsim_params = {.algo = VecSimAlgo_HNSWLIB,
                                      .algoParams = {.hnswParams = HNSWParams{hnsw_params}}};
        TieredIndexParams tiered_params = {
            .jobQueue = &mock_tp_->jobQ,
            .jobQueueCtx = mock_tp_->ctx,
            .submitCb = tieredIndexMock::submit_callback,
            .flatBufferLimit = SIZE_MAX,
            .primaryIndexParams = &vecsim_params,
            .specificParams = {TieredHNSWParams{.swapJobThreshold = 0}},
        };
        tiered_index_ = reinterpret_cast<TieredHNSWIndex<float, float> *>(
            TieredFactory::NewIndex(&tiered_params));
        mock_tp_->ctx->index_strong_ref.reset(tiered_index_);
        mock_tp_->init_threads();
        hnsw_ = tiered_index_->getHNSWIndex();
    }

    // --- Vector insertion helpers ---

    // Inserts BM_N_BASELINE random vectors (labels 0..BM_N_BASELINE-1).
    // Called once in SetUp. Waits for background jobs to complete.
    void insert_baseline_vectors() {
        std::mt19937 rng(42); // Fixed seed for reproducibility
        std::uniform_real_distribution<float> dist(-1.0f, 1.0f);

        std::vector<float> vec(BM_DIM);
        for (size_t i = 0; i < BM_N_BASELINE; i++) {
            for (size_t d = 0; d < BM_DIM; d++) {
                vec[d] = dist(rng);
            }
            VecSimIndex_AddVector(tiered_index_, vec.data(), i);
        }
        mock_tp_->thread_pool_wait();
    }

    // Inserts BM_N_ZERO zero vectors (labels BM_N_BASELINE..BM_N_BASELINE+BM_N_ZERO-1).
    // Zero vectors with COSINE metric create dense hub nodes that stress the
    // incoming edges bookkeeping — reproducing the ghost memory growth from MOD-13761.
    // Called in SetUp and again during reset between iterations.
    void insert_zero_vectors() {
        std::vector<float> vec(BM_DIM, 0.0f);
        for (size_t i = 0; i < BM_N_ZERO; i++) {
            VecSimIndex_AddVector(tiered_index_, vec.data(), BM_N_BASELINE + i);
        }
        mock_tp_->thread_pool_wait();
    }

    // --- Measurement helpers ---

    // Measures ghost memory across all incoming edges vectors in the HNSW graph.
    // Ghost memory = sum of (capacity - size) * sizeof(idType) for every
    // incomingUnidirectionalEdges vector across all nodes and all levels.
    // Reports scalar stats via benchmark state counters and prints distribution
    // details (top-10, percentiles) to stdout.
    void measure_ghost_memory(benchmark::State &state, int iteration = -1,
                              bool report_counters = false) {
        size_t total_used_bytes = 0;
        size_t total_alloc_bytes = 0;

        // Collect per-vector stats for distribution analysis
        std::vector<size_t> all_sizes;
        std::vector<size_t> all_caps;

        size_t num_elements = hnsw_->indexSize();
        size_t non_empty_count = 0;

        for (size_t id = 0; id < num_elements; id++) {
            auto *graph_data = hnsw_->getGraphDataByInternalId(id);
            for (size_t level = 0; level <= graph_data->toplevel; level++) {
                auto &level_data = hnsw_->getElementLevelData(graph_data, level);
                auto &incoming = level_data.getIncomingEdges();
                size_t sz = incoming.size();
                size_t cap = incoming.capacity();

                total_used_bytes += sz * sizeof(idType);
                total_alloc_bytes += cap * sizeof(idType);

                all_sizes.push_back(sz);
                all_caps.push_back(cap);
                if (sz > 0)
                    non_empty_count++;
            }
        }

        size_t wasted_bytes = total_alloc_bytes - total_used_bytes;
        size_t total_vectors = all_sizes.size();

        // Sort for top-10
        std::vector<size_t> sorted_sizes(all_sizes);
        std::vector<size_t> sorted_caps(all_caps);
        std::sort(sorted_sizes.begin(), sorted_sizes.end());
        std::sort(sorted_caps.begin(), sorted_caps.end());

        // --- Report metrics via benchmark counters (only for the "before shrink" call) ---
        if (report_counters) {
            state.counters["index_memory"] = hnsw_->getAllocationSize();
            state.counters["wasted_bytes"] = static_cast<double>(wasted_bytes);
        }

        // --- Print diagnostic summary to stdout ---
        std::cout << "\n=== Incoming Edges Stats"
                  << (iteration >= 0 ? " (iter=" + std::to_string(iteration) + ")" : "")
                  << " ===" << std::endl;
        std::cout << "  Nodes: " << num_elements << "  Level entries: " << total_vectors
                  << "  Non-empty: " << non_empty_count << std::endl;
        std::cout << "  Wasted bytes: " << wasted_bytes << "  (used=" << total_used_bytes
                  << ", alloc=" << total_alloc_bytes << ")" << std::endl;

        // Print top-10 by size (descending)
        std::cout << "  Top-10 by size: [";
        size_t top_n = std::min<size_t>(10, sorted_sizes.size());
        for (size_t i = 0; i < top_n; i++) {
            if (i > 0)
                std::cout << ", ";
            std::cout << sorted_sizes[sorted_sizes.size() - 1 - i];
        }
        std::cout << "]" << std::endl;

        // Print top-10 by capacity (descending)
        std::cout << "  Top-10 by cap:  [";
        top_n = std::min<size_t>(10, sorted_caps.size());
        for (size_t i = 0; i < top_n; i++) {
            if (i > 0)
                std::cout << ", ";
            std::cout << sorted_caps[sorted_caps.size() - 1 - i];
        }
        std::cout << "]" << std::endl;
    }

    // Shrinks all incoming edges vectors to reclaim ghost memory.
    // Used to reset state between benchmark iterations so each iteration
    // starts from a clean baseline.
    void shrink_all_incoming_edges() {
        size_t num_elements = hnsw_->indexSize();
        for (size_t id = 0; id < num_elements; id++) {
            auto *graph_data = hnsw_->getGraphDataByInternalId(id);
            for (size_t level = 0; level <= graph_data->toplevel; level++) {
                auto &level_data = hnsw_->getElementLevelData(graph_data, level);
                level_data.incomingUnidirectionalEdges->shrink_to_fit();
            }
        }
    }

public:
    // Common SetUp: save write mode, suppress logs, create index, insert baseline vectors.
    // Each benchmark method is responsible for inserting/deleting zero vectors as needed.
    void SetUp(benchmark::State &state) override {
        original_write_mode_ = VecSimIndexInterface::asyncWriteMode;
        VecSim_SetLogCallbackFunction(nullptr); // Suppress verbose resize/capacity logs
        VecSim_SetWriteMode(VecSim_WriteAsync);
        create_tiered_index();
        insert_baseline_vectors();
    }

    // Common TearDown: restore write mode, clean up mock thread pool.
    // The mock thread pool destructor handles joining threads and
    // releasing the index via index_strong_ref.
    void TearDown(benchmark::State &state) override {
        VecSim_SetWriteMode(original_write_mode_);
        delete mock_tp_;
        mock_tp_ = nullptr;
        tiered_index_ = nullptr;
        hnsw_ = nullptr;
    }

    // --- Benchmark methods (called from run files via BENCHMARK_DEFINE_F) ---

    // Async deletion path (production default).
    // In production, deleteVector() on a TieredHNSWIndex does:
    //   1. Main thread: markDelete() + create repair/swap jobs
    //   2. Background threads: executeRepairJob() → repairNodeConnections()
    //   3. executeReadySwapJobs() → removeAndSwap()
    // This benchmark measures the full async deletion path including ghost memory.
    void DeleteZeroVectorsAsync(benchmark::State &state) {
        // Insert zero vectors before the first iteration
        insert_zero_vectors();

        int iteration = 0;
        for (auto _ : state) {
            // TIMED: delete all 50K zero vectors through the tiered async path
            for (size_t i = 0; i < BM_N_ZERO; i++) {
                VecSimIndex_DeleteVector(tiered_index_, BM_N_BASELINE + i);
            }
            // Wait for all background repair jobs to complete
            mock_tp_->thread_pool_wait();
            // Execute all accumulated swap jobs (removes marked-deleted nodes)
            tiered_index_->executeReadySwapJobs();

            state.PauseTiming();

            // Measure ghost memory after deletion, before shrink (the "problem" state)
            std::cout << "\n--- Async iteration " << iteration
                      << ": After deletion (before shrink) ---";
            measure_ghost_memory(state, iteration, true);

            // Shrink all incoming edges to reclaim ghost memory
            shrink_all_incoming_edges();

            // Measure ghost memory after shrink (should be near-zero)
            std::cout << "\n--- Async iteration " << iteration << ": After shrink (baseline) ---";
            measure_ghost_memory(state, iteration);

            // Re-insert zero vectors for the next iteration
            insert_zero_vectors();

            iteration++;
            state.ResumeTiming();
        }
    }

    // Insertion path benchmark.
    // Measures the cost of inserting 50K zero vectors into the index.
    // During insertion, the HNSW heuristic prunes neighbors, which calls
    // removeIncomingUnidirectionalEdgeIfExists(). After the fix, shrink_to_fit
    // fires here too, so this benchmark captures the insertion latency impact.
    void InsertZeroVectorsTimed(benchmark::State &state) {
        int iteration = 0;
        for (auto _ : state) {
            // TIMED: insert 50K zero vectors (triggers heuristic pruning)
            insert_zero_vectors();

            state.PauseTiming();

            // Measure state after insertion (90K nodes)
            std::cout << "\n--- Insert iteration " << iteration << ": After insertion ---";
            measure_ghost_memory(state, iteration);

            // Delete zero vectors
            for (size_t i = 0; i < BM_N_ZERO; i++) {
                VecSimIndex_DeleteVector(tiered_index_, BM_N_BASELINE + i);
            }
            mock_tp_->thread_pool_wait();
            tiered_index_->executeReadySwapJobs();

            // Measure ghost memory after deletion, before shrink
            std::cout << "\n--- Insert iteration " << iteration
                      << ": After deletion (before shrink) ---";
            measure_ghost_memory(state, iteration, true);

            // Shrink to reclaim ghost memory
            shrink_all_incoming_edges();

            // Measure after shrink (should be near-zero)
            std::cout << "\n--- Insert iteration " << iteration << ": After shrink (baseline) ---";
            measure_ghost_memory(state, iteration);

            iteration++;
            state.ResumeTiming();
        }
    }

    // In-place deletion path (synchronous, worst-case latency).
    // Used during RDB loading, AOF rewrite, and certain overwrite scenarios.
    // Unlike async, all repair work happens on the calling thread — no bg threads.
    // This gives the worst-case latency impact of the shrink_to_fit fix.
    void DeleteZeroVectorsInPlace(benchmark::State &state) {
        // Insert zero vectors using async mode before switching to in-place
        insert_zero_vectors();

        // Switch to in-place mode for the timed deletion phase
        VecSim_SetWriteMode(VecSim_WriteInPlace);

        int iteration = 0;
        for (auto _ : state) {
            // TIMED: delete all 50K zero vectors through the in-place path
            // All repair + swap happens synchronously on this thread
            for (size_t i = 0; i < BM_N_ZERO; i++) {
                VecSimIndex_DeleteVector(tiered_index_, BM_N_BASELINE + i);
            }
            // No thread_pool_wait() or executeReadySwapJobs() needed — fully synchronous

            state.PauseTiming();

            // Measure ghost memory after deletion, before shrink (the "problem" state)
            std::cout << "\n--- InPlace iteration " << iteration
                      << ": After deletion (before shrink) ---";
            measure_ghost_memory(state, iteration, true);

            // Shrink all incoming edges to reclaim ghost memory
            shrink_all_incoming_edges();

            // Measure ghost memory after shrink (should be near-zero)
            std::cout << "\n--- InPlace iteration " << iteration << ": After shrink (baseline) ---";
            measure_ghost_memory(state, iteration);

            // Re-insert zero vectors using async mode for next iteration
            VecSim_SetWriteMode(VecSim_WriteAsync);
            insert_zero_vectors();
            VecSim_SetWriteMode(VecSim_WriteInPlace);

            iteration++;
            state.ResumeTiming();
        }

        // Restore async mode for clean teardown
        VecSim_SetWriteMode(VecSim_WriteAsync);
    }
};

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
#include <random>
#include <unistd.h>
#include "VecSim/vec_sim.h"
#include "VecSim/query_results.h"
#include "bm_vecsim_index.h"

template <typename index_type_t>
class BM_BatchIterator : public BM_VecSimIndex<index_type_t> {
protected:
    using data_t = typename index_type_t::data_t;
    using dist_t = typename index_type_t::dist_t;

    BM_BatchIterator() = default;
    ~BM_BatchIterator() = default;

    static void BF_FixedBatchSize(benchmark::State &st);
    static void BF_VariableBatchSize(benchmark::State &st);
    static void BF_BatchesToAdhocBF(benchmark::State &st);

    static void HNSW_FixedBatchSize(benchmark::State &st);
    static void HNSW_VariableBatchSize(benchmark::State &st);
    static void HNSW_BatchesToAdhocBF(benchmark::State &st);

    static void RunBatchedSearch_HNSW(benchmark::State &st, std::atomic_int &correct, size_t iter,
                                      size_t num_batches, size_t batch_size, size_t &total_res_num,
                                      size_t batch_increase_factor, size_t index_memory,
                                      double &memory_delta);
};

template <typename index_type_t>
void BM_BatchIterator<index_type_t>::RunBatchedSearch_HNSW(
    benchmark::State &st, std::atomic_int &correct, size_t iter, size_t num_batches,
    size_t batch_size, size_t &total_res_num, size_t batch_increase_factor, size_t index_memory,
    double &memory_delta) {
    VecSimBatchIterator *batchIterator =
        VecSimBatchIterator_New(GET_INDEX(INDEX_HNSW), QUERIES[iter % N_QUERIES].data(), nullptr);
    VecSimQueryReply *accumulated_results[num_batches];
    size_t batch_num = 0;
    total_res_num = 0;

    // Run search in batches, collect the accumulated results.
    while (VecSimBatchIterator_HasNext(batchIterator)) {
        if (batch_num == num_batches) {
            break;
        }
        VecSimQueryReply *res = VecSimBatchIterator_Next(batchIterator, batch_size, BY_ID);
        total_res_num += VecSimQueryReply_Len(res);
        accumulated_results[batch_num++] = res;
        batch_size *= batch_increase_factor;
    }
    st.PauseTiming();
    // Update the memory delta as a result of using the batch iterator.
    size_t curr_memory = VecSimIndex_StatsInfo(GET_INDEX(INDEX_HNSW)).memory;
    memory_delta += (double)(curr_memory - index_memory);
    VecSimBatchIterator_Free(batchIterator);

    // Measure recall - compare every result that was collected in some batch to the BF results.
    auto bf_results = VecSimIndex_TopKQuery(GET_INDEX(INDEX_BF), QUERIES[iter % N_QUERIES].data(),
                                            total_res_num, nullptr, BY_SCORE);
    for (size_t i = 0; i < batch_num; i++) {
        auto hnsw_results = accumulated_results[i];
        BM_VecSimGeneral::MeasureRecall(hnsw_results, bf_results, correct);
        VecSimQueryReply_Free(hnsw_results);
    }
    VecSimQueryReply_Free(bf_results);
    st.ResumeTiming();
}

template <typename index_type_t>
void BM_BatchIterator<index_type_t>::BF_FixedBatchSize(benchmark::State &st) {
    size_t batch_size = st.range(0);
    size_t num_batches = st.range(1);
    size_t iter = 0;
    size_t index_memory = VecSimIndex_StatsInfo(GET_INDEX(INDEX_BF)).memory;
    double memory_delta = 0.0;

    for (auto _ : st) {
        VecSimBatchIterator *batchIterator =
            VecSimBatchIterator_New(GET_INDEX(INDEX_BF), QUERIES[iter % N_QUERIES].data(), nullptr);
        size_t batches_counter = 0;
        while (VecSimBatchIterator_HasNext(batchIterator)) {
            VecSimQueryReply *res = VecSimBatchIterator_Next(batchIterator, batch_size, BY_ID);
            VecSimQueryReply_Free(res);
            batches_counter++;
            if (batches_counter == num_batches) {
                break;
            }
        }
        size_t curr_memory = VecSimIndex_StatsInfo(GET_INDEX(INDEX_BF)).memory;
        memory_delta += (double)(curr_memory - index_memory);
        VecSimBatchIterator_Free(batchIterator);
        iter++;
    }
    st.counters["memory"] = memory_delta / (double)iter;
}

template <typename index_type_t>
void BM_BatchIterator<index_type_t>::BF_VariableBatchSize(benchmark::State &st) {
    size_t batch_size = st.range(0);
    size_t num_batches = st.range(1);
    size_t iter = 0;
    for (auto _ : st) {
        VecSimBatchIterator *batchIterator =
            VecSimBatchIterator_New(GET_INDEX(INDEX_BF), QUERIES[iter % N_QUERIES].data(), nullptr);
        size_t batches_counter = 0;
        while (VecSimBatchIterator_HasNext(batchIterator)) {
            VecSimQueryReply *res = VecSimBatchIterator_Next(batchIterator, batch_size, BY_ID);
            VecSimQueryReply_Free(res);
            batches_counter++;
            batch_size *= 2;
            if (batches_counter == num_batches) {
                break;
            }
        }
        VecSimBatchIterator_Free(batchIterator);
        iter++;
    }
}

template <typename index_type_t>
void BM_BatchIterator<index_type_t>::BF_BatchesToAdhocBF(benchmark::State &st) {
    size_t step = st.range(0);
    size_t num_batches = st.range(1);
    size_t batch_size = 10;
    size_t iter = 0;
    for (auto _ : st) {
        VecSimBatchIterator *batchIterator =
            VecSimBatchIterator_New(GET_INDEX(INDEX_BF), QUERIES[iter % N_QUERIES].data(), nullptr);
        size_t batches_counter = 0;
        while (VecSimBatchIterator_HasNext(batchIterator)) {
            if (batches_counter == num_batches) {
                break;
            }
            VecSimQueryReply *res = VecSimBatchIterator_Next(batchIterator, batch_size, BY_ID);
            VecSimQueryReply_Free(res);
            batches_counter++;
            batch_size *= 2;
        }
        VecSimBatchIterator_Free(batchIterator);
        // Switch to ad-hoc BF
        for (size_t i = 0; i < N_VECTORS; i += step) {
            VecSimIndex_GetDistanceFrom_Unsafe(GET_INDEX(INDEX_BF), i,
                                               QUERIES[iter % N_QUERIES].data());
        }
        iter++;
    }
}

template <typename index_type_t>
void BM_BatchIterator<index_type_t>::HNSW_FixedBatchSize(benchmark::State &st) {

    size_t batch_size = st.range(0);
    size_t num_batches = st.range(1);
    size_t total_res_num = num_batches * batch_size;
    size_t iter = 0;
    std::atomic_int correct = 0;
    size_t index_memory = VecSimIndex_StatsInfo(GET_INDEX(INDEX_HNSW)).memory;
    double memory_delta = 0.0;

    for (auto _ : st) {
        RunBatchedSearch_HNSW(st, correct, iter, num_batches, batch_size, total_res_num, 1,
                              index_memory, memory_delta);
        iter++;
    }
    st.counters["Recall"] = (float)correct / (total_res_num * iter);
    st.counters["memory"] = memory_delta / (double)iter;
}

template <typename index_type_t>
void BM_BatchIterator<index_type_t>::HNSW_VariableBatchSize(benchmark::State &st) {
    size_t initial_batch_size = st.range(0);
    size_t num_batches = st.range(1);
    size_t total_res_num;
    size_t iter = 0;
    std::atomic_int correct = 0;
    size_t index_memory = VecSimIndex_StatsInfo(GET_INDEX(INDEX_HNSW)).memory;
    double memory_delta = 0.0;

    for (auto _ : st) {
        RunBatchedSearch_HNSW(st, correct, iter, num_batches, initial_batch_size, total_res_num, 2,
                              index_memory, memory_delta);
        iter++;
    }
    st.counters["Recall"] = (float)correct / (float)(total_res_num * iter);
    st.counters["memory"] = memory_delta / (double)iter;
}

template <typename index_type_t>
void BM_BatchIterator<index_type_t>::HNSW_BatchesToAdhocBF(benchmark::State &st) {
    size_t step = st.range(0);
    size_t num_batches = st.range(1);
    size_t total_res_num;
    size_t iter = 0;
    std::atomic_int correct = 0;
    size_t index_memory = VecSimIndex_StatsInfo(GET_INDEX(INDEX_HNSW)).memory;
    double memory_delta = 0.0;

    for (auto _ : st) {
        RunBatchedSearch_HNSW(st, correct, iter, num_batches, 10, total_res_num, 2, index_memory,
                              memory_delta);
        // Switch to ad-hoc BF
        for (size_t i = 0; i < N_VECTORS; i += step) {
            VecSimIndex_GetDistanceFrom_Unsafe(GET_INDEX(INDEX_HNSW), i,
                                               QUERIES[iter % N_QUERIES].data());
        }
        iter++;
    }
    st.counters["memory"] = memory_delta / (double)iter;
}

// Register the functions as a benchmark
// {batch_size, num_batches}
#define REGISTER_FixedBatchSize(BM_FUNC)                                                           \
    BENCHMARK_REGISTER_F(BM_BatchIterator, BM_FUNC)                                                \
        ->Args({10, 1})                                                                            \
        ->Args({10, 3})                                                                            \
        ->Args({10, 5})                                                                            \
        ->Args({100, 1})                                                                           \
        ->Args({100, 3})                                                                           \
        ->Args({100, 5})                                                                           \
        ->Args({1000, 1})                                                                          \
        ->Args({1000, 3})                                                                          \
        ->Args({1000, 5})                                                                          \
        ->ArgNames({"batch size", "number of batches"})                                            \
        ->Unit(benchmark::kMillisecond)                                                            \
        ->Iterations(10)

// {initial_batch_size, num_batches}
// batch size is increased by factor of 2 from iteration to the next one.
#define REGISTER_VariableBatchSize(BM_FUNC)                                                        \
    BENCHMARK_REGISTER_F(BM_BatchIterator, BM_FUNC)                                                \
        ->Args({10, 2})                                                                            \
        ->Args({10, 4})                                                                            \
        ->Args({100, 2})                                                                           \
        ->Args({100, 4})                                                                           \
        ->Args({1000, 2})                                                                          \
        ->Args({1000, 4})                                                                          \
        ->ArgNames({"batch initial size", "number of batches"})                                    \
        ->Unit(benchmark::kMillisecond)                                                            \
        ->Iterations(10)

// {step, num_batches} - where step is the ratio between the index size to the number of vectors to
// go over in ad-hoc BF.
// batch size is increased by factor of 2 from iteration to the next one, and initial batch size
// is 10.
#define REGISTER_BatchesToAdhocBF(BM_FUNC)                                                         \
    BENCHMARK_REGISTER_F(BM_BatchIterator, BM_FUNC)                                                \
        ->Args({5, 0})                                                                             \
        ->Args({5, 2})                                                                             \
        ->Args({5, 5})                                                                             \
        ->Args({10, 0})                                                                            \
        ->Args({10, 2})                                                                            \
        ->Args({10, 5})                                                                            \
        ->Args({20, 0})                                                                            \
        ->Args({20, 2})                                                                            \
        ->Args({20, 5})                                                                            \
        ->ArgNames({"step", "number of batches"})                                                  \
        ->Unit(benchmark::kMillisecond)                                                            \
        ->Iterations(10)

// {step, num_batches} - where step is the ratio between the index size to the number of vectors to
// go over in ad-hoc BF.
// batch size is increased by factor of 2 from iteration to the next one, and initial batch size
// is 10.
#define REGISTER_HNSW_BatchesToAdhocBF(BM_FUNC)                                                    \
    REGISTER_BatchesToAdhocBF(BM_FUNC)->Args({50, 0})->Args({50, 2})->Args({50, 5})->Args({100, 0})

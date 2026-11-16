/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 * SPDX-FileCopyrightText: Copyright 2026 Arm Limited and/or its affiliates
 * <open-source-office@arm.com>
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once
#include <atomic>
#include "bm_common.h"
#include <chrono>
#include "types_ranges.h"

using namespace std::chrono;

template <typename index_type_t>
class BM_VecSimBasics : public BM_VecSimCommon<index_type_t> {
public:
    using data_t = typename index_type_t::data_t;
    using dist_t = typename index_type_t::dist_t;

    BM_VecSimBasics() = default;
    ~BM_VecSimBasics() = default;

    // Add one label in each iteration. For multi index adds multiple vectors under the same label
    // per iteration, for single index adds one vector per iteration.
    static void AddLabel(benchmark::State &st);

    static void AddLabel_AsyncIngest(benchmark::State &st);

    static void DeleteLabel_AsyncRepair(benchmark::State &st,
                                        IndexTypeIndex index_type = INDEX_TIERED_HNSW);

    // We pass a specific index pointer instead of VecSimIndex * so we can use GetDataByLabel
    // which is not known to VecSimIndex class.
    // We delete one label in each iteration. For multi index deletes multiple vectors per
    // iteration, for single index deletes one vector per iteration.
    template <typename algo_t>
    static void DeleteLabel(algo_t *index, benchmark::State &st,
                            const VecSimIndexAbstract<data_t, dist_t> *data_source = nullptr);

    // Move one label to a fresh label in each iteration. This only rewrites label bookkeeping -
    // no vector data is copied and, for HNSW, the graph is untouched - so it is expected to be
    // orders of magnitude cheaper than the AddLabel + DeleteLabel pair it replaces for callers
    // whose external id changed while the vector did not. Reported in microseconds for that
    // reason, whereas the add/delete benchmarks above are reported in milliseconds.
    static void RelabelLabel(benchmark::State &st);

    // Replace one label's vectors in each iteration with VecSimIndex_UpdateVectors, re-inserting
    // the label's original vectors taken from the flat index. Re-insertion keeps the data but
    // rebuilds the label's graph links. Recall on held-out queries is measured before and after,
    // and every updated label is queried with its own vector to check it is still reachable.
    static void UpdateLabel(benchmark::State &st);

    static void Range_BF(benchmark::State &st);
    static void Range_HNSW(benchmark::State &st, IndexTypeIndex index_type = INDEX_HNSW);

    // Reproduces allocation/deallocation oscillation issue at block size boundaries.
    // Sets up index at blockSize+1 capacity, then repeatedly deletes and re-adds the same vector,
    // triggering constant grow-shrink cycles.
    // This behavior was fixed by PR #753 with a conservative resize strategy that only
    // shrinks containers when there are 2+ free blocks, preventing oscillation cycles.
    // Expected: High allocation overhead before fix, stable performance after fix.
    static void UpdateAtBlockSize(benchmark::State &st);

private:
    // Vectors of vector to store deleted labels' data.
    using LabelData = std::vector<std::vector<data_t>>;
};

template <typename index_type_t>
void BM_VecSimBasics<index_type_t>::AddLabel(benchmark::State &st) {

    auto index = GET_INDEX(st.range(0));
    size_t index_size = N_VECTORS;
    size_t initial_label_count = index->indexLabelCount();

    // In a single vector per label index, index size should equal label count.
    size_t vec_per_label = index_size % initial_label_count == 0
                               ? index_size / initial_label_count
                               : index_size / initial_label_count + 1;
    labelType label = initial_label_count;
    size_t added_vec_count = 0;

    index->fitMemory();
    size_t memory_delta = index->getAllocationSize();
    // Add a new label from the test set in every iteration.
    for (auto _ : st) {
        // Add one label
        for (labelType vec = 0; vec < vec_per_label; ++vec) {
            VecSimIndex_AddVector(index, QUERIES[added_vec_count % N_QUERIES].data(), label);
        }
        added_vec_count += vec_per_label;
        label++;
    }
    memory_delta = index->getAllocationSize() - memory_delta;
    // For tiered index, wait for all threads to finish indexing
    BM_VecSimGeneral::mock_thread_pool->thread_pool_wait();

    st.counters["memory_per_vector"] =
        benchmark::Counter((double)memory_delta / (double)added_vec_count,
                           benchmark::Counter::kDefaults, benchmark::Counter::OneK::kIs1024);
    st.counters["vectors_per_label"] = vec_per_label;

    assert(VecSimIndex_IndexSize(index) == N_VECTORS + added_vec_count);

    // Clean-up all the new vectors to restore the index size to its original value.
    // Note we loop over the new labels and not the internal ids. This way in multi indices BM all
    // the new vectors added under the same label will be removed in one call.
    size_t new_label_count = index->indexLabelCount();
    auto cleanup_index = static_cast<IndexTypeIndex>(st.range(0));
    if (cleanup_index == INDEX_TIERED_HNSW) {
        cleanup_index = INDEX_HNSW;
    } else if (cleanup_index == INDEX_TIERED_HNSW_SQ8) {
        cleanup_index = INDEX_HNSW_SQ8;
    }
    for (size_t label = initial_label_count; label < new_label_count; label++) {
        VecSimIndex_DeleteVector(GET_INDEX(cleanup_index), label);
    }
    assert(VecSimIndex_IndexSize(index) == N_VECTORS);
}

template <typename index_type_t>
void BM_VecSimBasics<index_type_t>::AddLabel_AsyncIngest(benchmark::State &st) {
    auto index = GET_INDEX(st.range(0));
    size_t index_size = N_VECTORS;
    size_t initial_label_count = index->indexLabelCount();

    // In a single vector per label index, index size should equal label count.
    size_t vec_per_label = index_size % initial_label_count == 0
                               ? index_size / initial_label_count
                               : index_size / initial_label_count + 1;

    index->fitMemory();
    size_t memory_before = index->getAllocationSize();
    labelType label = initial_label_count;
    size_t added_vec_count = 0;

    // Add a new label from the test set in every iteration.
    for (auto _ : st) {
        // Add one label
        for (labelType vec = 0; vec < vec_per_label; ++vec) {
            VecSimIndex_AddVector(index, QUERIES[added_vec_count % N_QUERIES].data(), label);
        }
        added_vec_count += vec_per_label;
        label++;
        if (label == initial_label_count + BM_VecSimGeneral::block_size) {
            BM_VecSimGeneral::mock_thread_pool->thread_pool_wait();
        }
    }

    size_t memory_delta = index->getAllocationSize() - memory_before;
    st.counters["memory_per_vector"] =
        benchmark::Counter((double)memory_delta / (double)added_vec_count,
                           benchmark::Counter::kDefaults, benchmark::Counter::OneK::kIs1024);
    st.counters["vectors_per_label"] = vec_per_label;
    st.counters["num_threads"] = BM_VecSimGeneral::mock_thread_pool->thread_pool_size;

    size_t index_size_after = VecSimIndex_IndexSize(index);
    assert(index_size_after == N_VECTORS + added_vec_count);

    // Clean-up all the new vectors to restore the index size to its original value.
    // Note we loop over the new labels and not the internal ids. This way in multi indices BM all
    // the new vectors added under the same label will be removed in one call.
    size_t new_label_count = index->indexLabelCount();
    // Remove directly from the tiered index's HNSW backend.
    auto backend_type = st.range(0) == INDEX_TIERED_HNSW_SQ8 ? INDEX_HNSW_SQ8 : INDEX_HNSW;
    for (size_t label_ = initial_label_count; label_ < new_label_count; label_++) {
        VecSimIndex_DeleteVector(GET_INDEX(backend_type), label_);
    }

    assert(VecSimIndex_IndexSize(index) == N_VECTORS);
}

template <typename index_type_t>
template <typename algo_t>
void BM_VecSimBasics<index_type_t>::DeleteLabel(
    algo_t *index, benchmark::State &st, const VecSimIndexAbstract<data_t, dist_t> *data_source) {
    // Remove a different vector in every execution.
    size_t label_to_remove = 0;
    index->fitMemory();
    double memory_delta, memory_before = index->getAllocationSize();
    size_t removed_vectors_count = 0;
    std::vector<LabelData> removed_labels_data;

    for (auto _ : st) {
        st.PauseTiming();
        LabelData data(0);
        // Get label id(s) data.
        if (data_source) {
            data_source->getDataByLabel(label_to_remove, data);
        } else {
            index->getDataByLabel(label_to_remove, data);
        }

        removed_labels_data.push_back(data);

        removed_vectors_count += data.size();
        st.ResumeTiming();

        // Delete label
        VecSimIndex_DeleteVector(index, label_to_remove++);
    }
    memory_delta = index->getAllocationSize() - memory_before;

    BM_VecSimGeneral::mock_thread_pool->thread_pool_wait();
    // Remove the rest of the vectors that hadn't been swapped yet for tiered index.
    if (VecSimIndex_BasicInfo(index).algo == VecSimAlgo_TIERED) {
        dynamic_cast<TieredHNSWIndex<data_t, dist_t> *>(index)->executeReadySwapJobs();
    }
    assert(removed_vectors_count > 0);
    st.counters["memory_per_vector"] =
        benchmark::Counter((double)memory_delta / (double)removed_vectors_count,
                           benchmark::Counter::kDefaults, benchmark::Counter::OneK::kIs1024);

    // Restore index state.
    // For each label in removed_labels_data
    for (size_t label_idx = 0; label_idx < removed_labels_data.size(); label_idx++) {
        size_t vec_count = removed_labels_data[label_idx].size();
        // Reinsert all the deleted vectors under this label.
        for (size_t vec_idx = 0; vec_idx < vec_count; ++vec_idx) {
            VecSimIndex_AddVector(index, removed_labels_data[label_idx][vec_idx].data(), label_idx);
        }
    }
    BM_VecSimGeneral::mock_thread_pool->thread_pool_wait();
    size_t cur_index_size = VecSimIndex_IndexSize(index);
    assert(VecSimIndex_IndexSize(index) == N_VECTORS);
}

template <typename index_type_t>
void BM_VecSimBasics<index_type_t>::DeleteLabel_AsyncRepair(benchmark::State &st,
                                                            IndexTypeIndex index_type) {
    // Remove a different vector in every execution.
    size_t label_to_remove = 0;
    auto *tiered_index = dynamic_cast<TieredHNSWIndex<data_t, dist_t> *>(GET_INDEX(index_type));
    auto *data_source =
        index_type == INDEX_TIERED_HNSW_SQ8
            ? dynamic_cast<VecSimIndexAbstract<data_t, dist_t> *>(GET_INDEX(INDEX_BF))
            : nullptr;

    tiered_index->fitMemory();
    double memory_before = tiered_index->getAllocationSize();
    size_t removed_vectors_count = 0;
    std::vector<LabelData> removed_labels_data;
    tiered_index->pendingSwapJobsThreshold = st.range(0);

    for (auto _ : st) {
        st.PauseTiming();
        LabelData data(0);
        // Get label id(s) data.
        if (data_source) {
            data_source->getDataByLabel(label_to_remove, data);
        } else {
            tiered_index->getDataByLabel(label_to_remove, data);
        }

        removed_labels_data.push_back(data);

        removed_vectors_count += data.size();
        st.ResumeTiming();

        // Delete label
        VecSimIndex_DeleteVector(tiered_index, label_to_remove++);
        if (label_to_remove == BM_VecSimGeneral::block_size) {
            BM_VecSimGeneral::mock_thread_pool->thread_pool_wait();
        }
    }

    // Avg. memory delta per vector equals the total memory delta divided by the number
    // of deleted vectors.
    double memory_delta = tiered_index->getAllocationSize() - memory_before;

    st.counters["memory_per_vector"] =
        benchmark::Counter((double)memory_delta / (double)removed_vectors_count,
                           benchmark::Counter::kDefaults, benchmark::Counter::OneK::kIs1024);
    st.counters["num_threads"] = (double)BM_VecSimGeneral::mock_thread_pool->thread_pool_size;
    st.counters["num_zombies"] = tiered_index->idToSwapJob.size();

    // Remove the rest of the vectors that hadn't been swapped yet.
    auto start = high_resolution_clock::now();
    tiered_index->pendingSwapJobsThreshold = 1;
    tiered_index->executeReadySwapJobs();
    tiered_index->pendingSwapJobsThreshold = DEFAULT_PENDING_SWAP_JOBS_THRESHOLD;
    auto end = high_resolution_clock::now();
    st.counters["cleanup_time"] = (double)duration_cast<milliseconds>(end - start).count();

    // Restore index state.
    // For each label in removed_labels_data
    for (size_t label_idx = 0; label_idx < removed_labels_data.size(); label_idx++) {
        size_t vec_count = removed_labels_data[label_idx].size();
        // Reinsert all the deleted vectors under this label.
        for (size_t vec_idx = 0; vec_idx < vec_count; ++vec_idx) {
            VecSimIndex_AddVector(tiered_index, removed_labels_data[label_idx][vec_idx].data(),
                                  label_idx);
        }
    }
    BM_VecSimGeneral::mock_thread_pool->thread_pool_wait();
    assert(VecSimIndex_IndexSize(tiered_index) == N_VECTORS);
}

template <typename index_type_t>
void BM_VecSimBasics<index_type_t>::RelabelLabel(benchmark::State &st) {
    auto index = GET_INDEX(st.range(0));
    const size_t initial_label_count = index->indexLabelCount();
    const size_t initial_index_size = VecSimIndex_IndexSize(index);

    // The loaded index labels occupy [0, initial_label_count), so every target taken from
    // initial_label_count upwards is free. That matters because relabelVector rejects an occupied
    // target, and a rejected call would time a no-op instead of the real work.
    const labelType target_base = initial_label_count;

    size_t attempted = 0;
    size_t moved = 0;
    for (auto _ : st) {
        moved += VecSimIndex_RelabelVector(index, attempted, target_base + attempted) ==
                 VecSimRelabel_OK;
        attempted++;
    }

    // Restore the original labels. Benchmark order affects results in this suite, so the following
    // benchmarks must see the label range they were written against. Note that for the tiered index
    // this also restores what INDEX_HNSW sees, since the tiered index wraps that same HNSW index.
    // Restoring an attempt that was rejected above is itself a harmless no-op.
    for (size_t i = 0; i < attempted; i++) {
        VecSimIndex_RelabelVector(index, target_base + i, i);
    }

    // A ratio below 1 means some timed calls were rejected and the reported time is not a
    // measurement of the relabel work - most likely the iteration count outgrew the label range.
    st.counters["relabeled_ratio"] = (double)moved / (double)attempted;
    st.counters["vectors_per_label"] = (double)initial_index_size / (double)initial_label_count;

    assert(index->indexLabelCount() == initial_label_count);
    assert(VecSimIndex_IndexSize(index) == initial_index_size);
}

template <typename index_type_t>
void BM_VecSimBasics<index_type_t>::UpdateLabel(benchmark::State &st) {
    auto *index = GET_INDEX(st.range(0));
    const auto *source = dynamic_cast<VecSimIndexAbstract<data_t, dist_t> *>(GET_INDEX(INDEX_BF));
    assert(source);
    constexpr size_t k = 10;
    constexpr size_t n_recall_queries = 50;
    HNSWRuntimeParams hnsw_runtime_params = {.efRuntime = 200};
    auto query_params = BM_VecSimGeneral::CreateQueryParams(hnsw_runtime_params);

    // Exact results do not change, since every label keeps the same vectors.
    std::vector<VecSimQueryReply *> exact_results;
    for (size_t q = 0; q < n_recall_queries; q++) {
        exact_results.push_back(
            VecSimIndex_TopKQuery(GET_INDEX(INDEX_BF), QUERIES[q].data(), k, nullptr, BY_SCORE));
    }
    auto measure_recall = [&]() {
        std::atomic_int correct = 0;
        for (size_t q = 0; q < n_recall_queries; q++) {
            auto results =
                VecSimIndex_TopKQuery(index, QUERIES[q].data(), k, &query_params, BY_SCORE);
            BM_VecSimGeneral::MeasureRecall(results, exact_results[q], correct);
            VecSimQueryReply_Free(results);
        }
        return (double)correct / (double)(k * n_recall_queries);
    };
    const double recall_before = measure_recall();

    labelType label = 0;
    size_t updated = 0;
    std::vector<data_t> blobs;
    std::vector<std::vector<data_t>> first_vectors;
    // Reused across iterations so the previous label's copy is freed while timing is paused.
    LabelData data(0);
    for (auto _ : st) {
        st.PauseTiming();
        data.clear();
        source->getDataByLabel(label, data);
        if (data.empty()) {
            st.SkipWithError("UpdateLabel ran past the loaded labels");
            break;
        }
        blobs.clear();
        for (const auto &vector : data) {
            blobs.insert(blobs.end(), vector.begin(), vector.end());
        }
        first_vectors.push_back(data.front());
        st.ResumeTiming();

        updated +=
            VecSimIndex_UpdateVectors(index, label, blobs.data(), data.size()) == VecSimUpdate_OK;
        label++;
    }

    st.counters["recall_before"] = recall_before;
    st.counters["recall_after"] = measure_recall();
    // Each updated label's own vector should still return that label first.
    size_t found_self = 0;
    for (labelType l = 0; l < label; l++) {
        auto results =
            VecSimIndex_TopKQuery(index, first_vectors[l].data(), 1, &query_params, BY_SCORE);
        auto *it = VecSimQueryReply_GetIterator(results);
        if (VecSimQueryReply_IteratorHasNext(it) &&
            VecSimQueryResult_GetId(VecSimQueryReply_IteratorNext(it)) == l) {
            found_self++;
        }
        VecSimQueryReply_IteratorFree(it);
        VecSimQueryReply_Free(results);
    }
    st.counters["self_recall_after"] = (double)found_self / (double)label;
    // A ratio below 1 means some timed calls were rejected, so the time does not measure updates.
    st.counters["updated_ratio"] = (double)updated / (double)label;
    for (auto *results : exact_results) {
        VecSimQueryReply_Free(results);
    }
}

template <typename index_type_t>
void BM_VecSimBasics<index_type_t>::Range_BF(benchmark::State &st) {
    double radius = (1.0 / 100.0) * (double)st.range(0);
    size_t iter = 0;
    size_t total_res = 0;

    for (auto _ : st) {
        auto res = VecSimIndex_RangeQuery(GET_INDEX(INDEX_BF), QUERIES[iter % N_QUERIES].data(),
                                          radius, nullptr, BY_ID);
        total_res += VecSimQueryReply_Len(res);
        iter++;
    }
    st.counters["Avg. results number"] = (double)total_res / iter;
}

template <typename index_type_t>
void BM_VecSimBasics<index_type_t>::Range_HNSW(benchmark::State &st, IndexTypeIndex index_type) {
    double radius = (1.0 / 100.0) * (double)st.range(0);
    double epsilon = (1.0 / 1000.0) * (double)st.range(1);
    size_t iter = 0;
    size_t total_res = 0;
    size_t total_res_bf = 0;
    std::atomic_int correct = 0;
    HNSWRuntimeParams hnswRuntimeParams = {.epsilon = epsilon};
    auto query_params = BM_VecSimGeneral::CreateQueryParams(hnswRuntimeParams);

    for (auto _ : st) {
        auto hnsw_results = VecSimIndex_RangeQuery(
            GET_INDEX(index_type), QUERIES[iter % N_QUERIES].data(), radius, &query_params, BY_ID);
        st.PauseTiming();
        total_res += VecSimQueryReply_Len(hnsw_results);

        // Measure recall:
        auto bf_results = VecSimIndex_RangeQuery(
            GET_INDEX(INDEX_BF), QUERIES[iter % N_QUERIES].data(), radius, nullptr, BY_ID);
        total_res_bf += VecSimQueryReply_Len(bf_results);
        BM_VecSimGeneral::MeasureRecall(hnsw_results, bf_results, correct);

        VecSimQueryReply_Free(bf_results);
        VecSimQueryReply_Free(hnsw_results);
        iter++;
        st.ResumeTiming();
    }
    st.counters["Avg. results number"] = (double)total_res / iter;
    // When the exact result set is empty, recall is undefined; retain the result count.
    if (total_res_bf != 0) {
        st.counters["Recall"] = (double)correct / total_res_bf;
    }
}

template <typename index_type_t>
void BM_VecSimBasics<index_type_t>::UpdateAtBlockSize(benchmark::State &st) {
    auto index = GET_INDEX(st.range(0));
    size_t initial_index_size = VecSimIndex_IndexSize(index);
    // Calculate vectors needed to reach next block boundary
    size_t vecs_to_blocksize =
        BM_VecSimGeneral::block_size - (initial_index_size % BM_VecSimGeneral::block_size);
    size_t initial_index_cap = index->indexMetaDataCapacity();
    assert(initial_index_cap == N_VECTORS + vecs_to_blocksize);

    assert(vecs_to_blocksize < BM_VecSimGeneral::block_size);
    labelType initial_label_count = index->indexLabelCount();
    labelType curr_label = initial_label_count;

    // Set up index at blockSize+1 to trigger oscillation issue
    // Make sure we have enough queries to add a new label.
    assert(N_QUERIES > BM_VecSimGeneral::block_size);
    size_t overhead = 1;
    size_t added_vec_count = vecs_to_blocksize + overhead;
    for (size_t i = 0; i < added_vec_count; ++i) {
        VecSimIndex_AddVector(index, QUERIES[added_vec_count % N_QUERIES].data(), curr_label++);
    }
    // For tiered index, wait for all threads to finish indexing
    BM_VecSimGeneral::mock_thread_pool->thread_pool_wait();
    assert(VecSimIndex_IndexSize(index) % BM_VecSimGeneral::block_size == overhead);
    assert(VecSimIndex_IndexSize(index) == N_VECTORS + added_vec_count);

    std::cout << "Added " << added_vec_count << " vectors to reach block size boundary."
              << std::endl;
    std::cout << "Index size is now " << VecSimIndex_IndexSize(index) << std::endl;
    std::cout << "Last label is " << curr_label - 1 << std::endl;

    // Benchmark loop: repeatedly delete/add same vector to trigger grow-shrink cycles
    labelType label_to_update = curr_label - 1;
    size_t index_cap = index->indexMetaDataCapacity();
    std::cout << "index_cap after adding vectors " << index_cap << std::endl;
    assert(index_cap == initial_index_cap + BM_VecSimGeneral::block_size);

    for (auto _ : st) {
        // Remove the vector directly from hnsw
        size_t ret = VecSimIndex_DeleteVector(
            GET_INDEX(st.range(0) == INDEX_TIERED_HNSW ? INDEX_HNSW : st.range(0)),
            label_to_update);
        assert(ret == 1);

        // Capacity should not change
        size_t curr_cap = index->indexMetaDataCapacity();
        assert(curr_cap == index_cap);
        ret = VecSimIndex_AddVector(index, QUERIES[(added_vec_count - 1) % N_QUERIES].data(),
                                    label_to_update);
        assert(ret == 1);
        BM_VecSimGeneral::mock_thread_pool->thread_pool_wait();
        assert(VecSimIndex_IndexSize(
                   GET_INDEX(st.range(0) == INDEX_TIERED_HNSW ? INDEX_HNSW : st.range(0))) ==
               N_VECTORS + added_vec_count);
        // Capacity should not change
        assert(index->indexMetaDataCapacity() == index_cap);
    }
    assert(VecSimIndex_IndexSize(index) == N_VECTORS + added_vec_count);

    // Clean-up all the new vectors to restore the index size to its original value.

    size_t new_label_count = index->indexLabelCount();
    for (size_t label = initial_label_count; label < new_label_count; label++) {
        // If index is tiered HNSW, remove directly from the underline HNSW.
        VecSimIndex_DeleteVector(
            GET_INDEX(st.range(0) == INDEX_TIERED_HNSW ? INDEX_HNSW : st.range(0)), label);
    }
    assert(VecSimIndex_IndexSize(index) == N_VECTORS);
}

#define UNIT_AND_ITERATIONS Unit(benchmark::kMillisecond)->Iterations(BM_VecSimGeneral::block_size)

// These macros are used to make sure the expansion of other macros happens when needed
#define MACRO_EXPAND_AND_CONCATENATE(a, b) a##b
#define MACRO_CONCATENATE(a, b)            MACRO_EXPAND_AND_CONCATENATE(a, b)

// The actual radius will be the given arg divided by 100, since arg must be an integer.
#define REGISTER_Range_BF(BM_FUNC, TYPENAME)                                                       \
    static void MACRO_CONCATENATE(BM_FUNC, _Args)(benchmark::internal::Benchmark * b) {            \
        for (int radius : benchmark_range<TYPENAME>::get_radii()) {                                \
            b->Args({radius});                                                                     \
        }                                                                                          \
    }                                                                                              \
    BENCHMARK_REGISTER_F(BM_VecSimBasics, BM_FUNC)                                                 \
        ->Apply(MACRO_CONCATENATE(BM_FUNC, _Args))                                                 \
        ->ArgNames({"radiusX100"})                                                                 \
        ->Iterations(10)                                                                           \
        ->Unit(benchmark::kMillisecond);

// {radius*100, epsilon*1000}
// The actual radius will be the given arg divided by 100, and the actual epsilon values
// will be the given arg divided by 1000.
#define REGISTER_Range_HNSW(BM_FUNC, TYPENAME)                                                     \
    static void MACRO_CONCATENATE(BM_FUNC, _Args)(benchmark::internal::Benchmark * b) {            \
        for (int radius : benchmark_range<TYPENAME>::get_radii()) {                                \
            for (int epsilon : benchmark_range<TYPENAME>::get_epsilons()) {                        \
                b->Args({radius, epsilon});                                                        \
            }                                                                                      \
        }                                                                                          \
    }                                                                                              \
    BENCHMARK_REGISTER_F(BM_VecSimBasics, BM_FUNC)                                                 \
        ->Apply(MACRO_CONCATENATE(BM_FUNC, _Args))                                                 \
        ->ArgNames({"radiusX100", "epsilonX1000"})                                                 \
        ->Iterations(10)                                                                           \
        ->Unit(benchmark::kMillisecond)

#define REGISTER_AddLabel(BM_FUNC, VecSimAlgo)                                                     \
    BENCHMARK_REGISTER_F(BM_VecSimBasics, BM_FUNC)                                                 \
        ->UNIT_AND_ITERATIONS->Arg(VecSimAlgo)                                                     \
        ->ArgName(#VecSimAlgo)

// DeleteLabel define and register macros
#define DEFINE_DELETE_LABEL(BM_FUNC, INDEX_TYPE, INDEX_NAME, DATA_TYPE, DIST_TYPE, VecSimAlgo)     \
    BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimBasics, BM_FUNC, INDEX_TYPE)(benchmark::State & st) {     \
        DeleteLabel<INDEX_NAME<DATA_TYPE, DIST_TYPE>>(                                             \
            BM_VecSimIndex<INDEX_TYPE>::get_typed_index<INDEX_NAME<DATA_TYPE, DIST_TYPE>>(         \
                VecSimAlgo),                                                                       \
            st);                                                                                   \
    }
#define DEFINE_DELETE_LABEL_WITH_DATA_SOURCE(BM_FUNC, INDEX_TYPE, INDEX_NAME, DATA_SOURCE_NAME,    \
                                             DATA_TYPE, DIST_TYPE, VecSimAlgo, DataSourceAlgo)     \
    BENCHMARK_TEMPLATE_DEFINE_F(BM_VecSimBasics, BM_FUNC, INDEX_TYPE)(benchmark::State & st) {     \
        DeleteLabel<INDEX_NAME<DATA_TYPE, DIST_TYPE>>(                                             \
            BM_VecSimIndex<INDEX_TYPE>::get_typed_index<INDEX_NAME<DATA_TYPE, DIST_TYPE>>(         \
                VecSimAlgo),                                                                       \
            st,                                                                                    \
            BM_VecSimIndex<INDEX_TYPE>::get_typed_index<DATA_SOURCE_NAME<DATA_TYPE, DIST_TYPE>>(   \
                DataSourceAlgo));                                                                  \
    }
#define REGISTER_DeleteLabel(BM_FUNC)                                                              \
    BENCHMARK_REGISTER_F(BM_VecSimBasics, BM_FUNC)->UNIT_AND_ITERATIONS

#define REGISTER_UpdateAtBlockSize(BM_FUNC, VecSimAlgo)                                            \
    BENCHMARK_REGISTER_F(BM_VecSimBasics, BM_FUNC)                                                 \
        ->UNIT_AND_ITERATIONS->Arg(VecSimAlgo)                                                     \
        ->ArgName(#VecSimAlgo)

// UpdateLabel rebuilds graph links of the updated labels, so register it after the cases that
// should see the loaded graph.
#define REGISTER_UpdateLabel(BM_FUNC, VecSimAlgo)                                                  \
    BENCHMARK_REGISTER_F(BM_VecSimBasics, BM_FUNC)                                                 \
        ->UNIT_AND_ITERATIONS->Arg(VecSimAlgo)                                                     \
        ->ArgName(#VecSimAlgo)

// A relabel is label bookkeeping only, so it lands orders of magnitude below the add/delete
// benchmarks - hence microseconds rather than the shared UNIT_AND_ITERATIONS milliseconds. The
// iteration count stays fixed (rather than letting the harness choose) because each iteration
// consumes one source label, and overrunning the label range would silently turn timed work into
// rejected no-ops.
#define REGISTER_RelabelLabel(BM_FUNC, VecSimAlgo)                                                 \
    BENCHMARK_REGISTER_F(BM_VecSimBasics, BM_FUNC)                                                 \
        ->Unit(benchmark::kMicrosecond)                                                            \
        ->Iterations(BM_VecSimGeneral::block_size)                                                 \
        ->Arg(VecSimAlgo)                                                                          \
        ->ArgName(#VecSimAlgo)

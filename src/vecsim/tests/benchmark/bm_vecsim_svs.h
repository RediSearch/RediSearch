/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once
#include "bm_vecsim_general.h"
#include "bm_training_utils.h"
#include "bm_macros.h"
#include "VecSim/algorithms/svs/svs_tiered.h"
#include "VecSim/index_factories/tiered_factory.h"
#include "gtest/gtest.h"
#include "VecSim/types/float16.h"
#include "bm_utils.h"

template <typename index_type_t>
class BM_VecSimSVS : public BM_VecSimGeneral {
public:
    using data_t = typename index_type_t::data_t;
    using dist_t = typename index_type_t::dist_t;

    BM_VecSimSVS() : data_type(index_type_t::get_index_type()) {
        if (!is_initialized) {
            VecSim_SetLogCallbackFunction(nullptr);
            test_vectors = benchmark_utils::LoadTrainingVectors<data_t>(
                AttachRootPath(test_queries_file), N_QUERIES, dim);
            if (svs_index_tar_file) {
                base_path = AttachRootPath("tests/benchmark/data");
                extractTarGz(BM_VecSimGeneral::AttachRootPath(svs_index_tar_file), base_path);
            }
            is_initialized = true;
        }
    }
    ~BM_VecSimSVS() = default;

    // Training benchmarks
    void Train(benchmark::State &st);
    void TrainAsync(benchmark::State &st);

    // Loaded index benchmarks

    // Add label directly to svs index one by one
    void AddLabel(benchmark::State &st);
    // Add vector to reach update threshold in tiered index. Measure time to move vectors to svs
    // index.
    void TriggerUpdateTiered(benchmark::State &st);

    // Add vectors to reach training threshold in tiered index, and then add more vectors in
    // parallel to backend training job. Measure time to add new vectors in this scenario.
    void AddVectorsDuringTraining(benchmark::State &st);

    // Deletes an amount of labels from the index that triggers inplace consolidation.
    void RunGC(benchmark::State &st);

    // TopK search benchmark. The search window size and k are provided by the benchmark
    // registration.
    void TopK_SVS(benchmark::State &st);
    void Memory(benchmark::State &st);

private:
    static const char *svs_index_tar_file;
    static const char *search_queries_file;
    inline static IndexPtr reference_index;
    inline static std::vector<std::vector<data_t>> search_queries;
    static void InitializeSearchReference();
    static std::string base_path;

    // Quant bits can be controlled by the benchmark framework, or by changed by a running benchmark
    // if it explicitly sets it.
    static VecSimSvsQuantBits quantBits;
    VecSimType data_type;

    // Initialize test vectors once
    static bool is_initialized;
    static std::vector<std::vector<data_t>> test_vectors;

    static void extractTarGz(const std::string &filename, const std::string &destination) {

        // Extract tar.gz
        std::string command = "tar -xzf " + filename + " -C " + destination;
        int result = system(command.c_str());
        if (result != 0) {
            throw std::runtime_error("Failed to extract tar.gz file");
        }
    }

    template <bool is_async>
    bool runTrainBMIteration(benchmark::State &st, tieredIndexMock &mock_thread_pool,
                             size_t training_threshold);

    TieredSVSIndex<data_t> *
    CreateTieredSVSIndex(tieredIndexMock &mock_thread_pool, size_t training_threshold,
                         size_t update_threshold = SVS_VAMANA_DEFAULT_UPDATE_THRESHOLD) {
        SVSParams svs_params = {
            .type = this->data_type,
            .dim = BM_VecSimGeneral::dim,
            .metric = VecSimMetric_Cosine,
            .quantBits = this->quantBits,
            .graph_max_degree = BM_VecSimGeneral::M,
            .construction_window_size = BM_VecSimGeneral::EF_C,
        };
        VecSimParams params{.algo = VecSimAlgo_SVS, .algoParams = {.svsParams = svs_params}};

        TieredIndexParams tiered_params = test_utils::CreateTieredSVSParams(
            params, mock_thread_pool, training_threshold, update_threshold);
        auto *tiered_index =
            reinterpret_cast<TieredSVSIndex<data_t> *>(TieredFactory::NewIndex(&tiered_params));
        assert(tiered_index);
        verifyQuantBits(tiered_index, "CreateTieredSVSIndex");

        // Set the created tiered index in the index external context (it will take ownership over
        // the index, and we'll need to release the ctx at the end of the test.
        mock_thread_pool.ctx->index_strong_ref.reset(tiered_index);
        VecSim_UpdateThreadPoolSize(mock_thread_pool.thread_pool_size);
        test_utils::verifyNumThreads(tiered_index, 1,
                                     std::max(mock_thread_pool.thread_pool_size, size_t{1}),
                                     std::string("CreateTieredSVSIndex"));

        return tiered_index;
    }

    VecSimIndexAbstract<data_t, float> *CreateSVSIndexFromFile(size_t update_threshold) {
        SVSParams svs_params = {.type = this->data_type,
                                .dim = BM_VecSimGeneral::dim,
                                .metric = VecSimMetric_Cosine,
                                .quantBits = this->quantBits,
                                .graph_max_degree = BM_VecSimGeneral::M,
                                .construction_window_size = BM_VecSimGeneral::EF_C};
        VecSimParams params{.algo = VecSimAlgo_SVS, .algoParams = {.svsParams = svs_params}};

        // Load svs index - construct path based on quantBits
        std::string index_path = base_path;
        if (this->quantBits == VecSimSvsQuant_NONE) {
            index_path += "/dbpedia_svs_none";
        } else if (this->quantBits == VecSimSvsQuant_8) {
            index_path += "/dbpedia_svs_8";
        }
        auto *svs_index = reinterpret_cast<VecSimIndexAbstract<data_t, float> *>(
            SVSFactory::NewIndex(index_path, &params));

        return svs_index;
    }
    // training_threshold is not relevant in this case, as the loaded index is already trained.
    TieredSVSIndex<data_t> *CreateTieredSVSIndexFromFile(tieredIndexMock &mock_thread_pool,
                                                         size_t update_threshold) {
        SVSParams svs_params = {.type = this->data_type,
                                .dim = BM_VecSimGeneral::dim,
                                .metric = VecSimMetric_Cosine,
                                .quantBits = this->quantBits,
                                .graph_max_degree = BM_VecSimGeneral::M,
                                .construction_window_size = BM_VecSimGeneral::EF_C};
        VecSimParams params{.algo = VecSimAlgo_SVS, .algoParams = {.svsParams = svs_params}};

        // Load svs index
        auto *svs_index = CreateSVSIndexFromFile(update_threshold);
        TieredIndexParams tiered_params = test_utils::CreateTieredSVSParams(
            params, mock_thread_pool, update_threshold, update_threshold);

        auto *tiered_index = reinterpret_cast<TieredSVSIndex<data_t> *>(
            TieredFactory::TieredSVSFactory::NewIndex<data_t>(&tiered_params, svs_index));
        assert(tiered_index);
        verifyQuantBits(tiered_index, "CreateTieredSVSIndexFromFile");
        // Set the created tiered index in the index external context (it will take ownership over
        // the index, and we'll need to release the ctx at the end of the test.
        mock_thread_pool.ctx->index_strong_ref.reset(tiered_index);
        size_t num_threads = mock_thread_pool.thread_pool_size;
        VecSim_UpdateThreadPoolSize(num_threads);
        test_utils::verifyNumThreads(tiered_index, 1, std::max(num_threads, size_t{1}),
                                     std::string("CreateTieredSVSIndexFromFile"));

        return tiered_index;
    }

    void verifyQuantBits(VecSimIndex *index, std::string msg) {
        VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(index);
#if HAVE_SVS_LVQ
        ASSERT_EQ(info.svsInfo.quantBits, this->quantBits) << msg;
#else
        if (this->quantBits == VecSimSvsQuant_NONE) {
            ASSERT_EQ(info.svsInfo.quantBits, this->quantBits) << msg;
        } else {
            ASSERT_EQ(info.svsInfo.quantBits, VecSimSvsQuant_Scalar) << msg;
        }
#endif
    }
};

template <typename index_type_t>
const char *BM_VecSimSVS<index_type_t>::search_queries_file = nullptr;

template <typename index_type_t>
bool BM_VecSimSVS<index_type_t>::is_initialized = false;
template <typename index_type_t>
VecSimSvsQuantBits BM_VecSimSVS<index_type_t>::quantBits = VecSimSvsQuant_NONE;
template <typename index_type_t>
std::string BM_VecSimSVS<index_type_t>::base_path = "tests/benchmark/data";

// Needs to be explicitly initalized
template <>
std::vector<std::vector<float>> BM_VecSimSVS<fp32_index_t>::test_vectors{};
template <>
std::vector<std::vector<vecsim_types::float16>> BM_VecSimSVS<fp16_index_t>::test_vectors{};

template <typename index_type_t>
template <bool is_async>
bool BM_VecSimSVS<index_type_t>::runTrainBMIteration(benchmark::State &st,
                                                     tieredIndexMock &mock_thread_pool,
                                                     size_t training_threshold) {
    this->quantBits = static_cast<VecSimSvsQuantBits>(st.range(0));
    auto *tiered_index = CreateTieredSVSIndex(mock_thread_pool, training_threshold);

    const auto expected_mode = is_async ? VecSim_WriteAsync : VecSim_WriteInPlace;
    if (VecSimIndexInterface::asyncWriteMode != expected_mode) {
        st.SkipWithError("Unexpected SVS training write mode");
        return false;
    }
    if (!benchmark_utils::RunTrainingIteration<is_async>(st, tiered_index, mock_thread_pool,
                                                         test_vectors, training_threshold)) {
        return false;
    }
    if constexpr (is_async)
        test_utils::verifyNumThreads(tiered_index, mock_thread_pool.thread_pool_size,
                                     mock_thread_pool.thread_pool_size,
                                     std::string("runTrainBMIteration"));

    return true;
}

template <typename index_type_t>
void BM_VecSimSVS<index_type_t>::Train(benchmark::State &st) {
    auto training_threshold = st.range(1);

    // Ensure we have enough vectors to train.
    ASSERT_GE(N_QUERIES, training_threshold);
    for (auto _ : st) {
        st.PauseTiming();
        // In each iteration create a new index with 0 threads (in-place mode).
        // VecSim_UpdateThreadPoolSize(0) sets write mode to VecSim_WriteInPlace.
        {
            auto mock_thread_pool = tieredIndexMock(0);
            if (!runTrainBMIteration<false>(st, mock_thread_pool, training_threshold)) {
                return;
            }
        }
        st.ResumeTiming();
    }
    ASSERT_EQ(VecSimIndexInterface::asyncWriteMode, VecSim_WriteInPlace);
}

template <typename index_type_t>
void BM_VecSimSVS<index_type_t>::TrainAsync(benchmark::State &st) {
    auto training_threshold = st.range(1);
    int unsigned num_threads = st.range(2);

    if (num_threads > std::thread::hardware_concurrency()) {
        GTEST_SKIP() << "Not enough threads available, skipping test...";
    }

    // Ensure we have enough vectors to train.
    ASSERT_GE(N_QUERIES, training_threshold);
    size_t iter = 0;
    for (auto _ : st) {
        st.PauseTiming();
        // In each iteration create a new index
        {
            auto mock_thread_pool = tieredIndexMock(num_threads);
            ASSERT_EQ(mock_thread_pool.thread_pool_size, num_threads);
            if (!runTrainBMIteration<true>(st, mock_thread_pool, training_threshold)) {
                return;
            }
        }
        st.ResumeTiming();
    }
}

template <typename index_type_t>
void BM_VecSimSVS<index_type_t>::AddLabel(benchmark::State &st) {

    size_t label = 0;
    auto index = CreateSVSIndexFromFile(1);
    size_t memory_delta = index->getAllocationSize();
    for (auto _ : st) {
        VecSimIndex_AddVector(index, test_vectors[label].data(), label + N_VECTORS);
        label++;
    };
    memory_delta = index->getAllocationSize() - memory_delta;

    ASSERT_EQ(VecSimIndex_IndexSize(index), N_VECTORS + this->block_size);
    st.counters["memory_per_vector"] =
        benchmark::Counter((double)memory_delta / (double)(this->block_size),
                           benchmark::Counter::kDefaults, benchmark::Counter::OneK::kIs1024);
}

template <typename index_type_t>
void BM_VecSimSVS<index_type_t>::TriggerUpdateTiered(benchmark::State &st) {
    // ensure mode is async
    ASSERT_EQ(VecSimIndexInterface::asyncWriteMode, VecSim_WriteAsync);

    auto update_threshold = st.range(0);
    int unsigned num_threads = st.range(1);

    if (num_threads > std::thread::hardware_concurrency()) {
        GTEST_SKIP() << "Not enough threads available, skipping test...";
    }

    // Ensure we have enough vectors to update.
    ASSERT_GE(N_QUERIES, update_threshold);

    // In each iteration create a new index
    auto mock_thread_pool = tieredIndexMock(num_threads);
    ASSERT_EQ(mock_thread_pool.thread_pool_size, num_threads);
    auto *tiered_index = CreateTieredSVSIndexFromFile(mock_thread_pool, update_threshold);
    VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(tiered_index);

    auto verify_index_size = [&](size_t expected_tiered_index_size, size_t expected_frontend_size,
                                 size_t expected_backend_size, std::string msg = "") {
        VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(tiered_index);
        auto backend_info = info.tieredInfo.backendCommonInfo;
        auto frontend_info = info.tieredInfo.frontendCommonInfo;
        ASSERT_EQ(info.commonInfo.indexSize, expected_tiered_index_size) << msg;
        ASSERT_EQ(backend_info.indexSize, expected_backend_size) << msg;
        ASSERT_EQ(frontend_info.indexSize, expected_frontend_size) << msg;
    };
    // insert just below training threshold vectors
    verify_index_size(
        N_VECTORS, 0, N_VECTORS,
        (std::ostringstream() << "Loaded svs index from file with " << N_VECTORS << " vectors")
            .str());
    for (size_t i = 0; i < update_threshold - 1; ++i) {
        int ret = VecSimIndex_AddVector(tiered_index, test_vectors[i].data(), i + N_VECTORS);
        ASSERT_EQ(ret, 1);
    }
    mock_thread_pool.init_threads();
    for (auto _ : st) {
        // add one more vector to trigger the update
        int ret = VecSimIndex_AddVector(tiered_index, test_vectors[update_threshold - 1].data(),
                                        update_threshold - 1 + N_VECTORS);
        ASSERT_EQ(ret, 1);

        mock_thread_pool.thread_pool_wait();
    }
    verify_index_size(N_VECTORS + update_threshold, 0, N_VECTORS + update_threshold,
                      (std::ostringstream()
                       << "added the update_threshold'th (" << update_threshold << ") vector")
                          .str());
}

template <typename index_type_t>
inline void BM_VecSimSVS<index_type_t>::AddVectorsDuringTraining(benchmark::State &st) {
    // ensure mode is async
    ASSERT_EQ(VecSimIndexInterface::asyncWriteMode, VecSim_WriteAsync);

    auto training_threshold = st.range(0);
    int unsigned num_threads = st.range(1);

    if (num_threads > std::thread::hardware_concurrency()) {
        GTEST_SKIP() << "Not enough threads available, skipping test...";
    }

    // Ensure we have enough vectors to train.
    ASSERT_GE(N_QUERIES, training_threshold + 1000); // need extra vectors to add during training

    // In each iteration create a new index
    auto mock_thread_pool = tieredIndexMock(num_threads);
    ASSERT_EQ(mock_thread_pool.thread_pool_size, num_threads);
    auto *tiered_index = CreateTieredSVSIndex(
        mock_thread_pool, training_threshold,
        1 << 30); // set very high update threshold to avoid updates during this test

    // Add vectors to reach training threshold and trigger training.
    for (size_t i = 0; i < training_threshold; ++i) {
        VecSimIndex_AddVector(tiered_index, test_vectors[i].data(), i);
    }
    mock_thread_pool.init_threads();
    size_t label = training_threshold;

    for (auto _ : st) {
        // While the backend is training, keep adding vectors in parallel to the training job.
        for (size_t i = 0; i < 1000 && label < N_QUERIES; i++, label++) {
            VecSimIndex_AddVector(tiered_index, test_vectors[label].data(), label);
        }
    }
    mock_thread_pool.thread_pool_join();
}

template <typename index_type_t>
void BM_VecSimSVS<index_type_t>::RunGC(benchmark::State &st) {

    size_t num_deletions = st.range(0);
    auto mock_thread_pool = tieredIndexMock(st.range(1));
    ASSERT_EQ(mock_thread_pool.thread_pool_size, st.range(1));
    auto *tiered_index = CreateTieredSVSIndexFromFile(mock_thread_pool, 1);

    // start threadpool
    mock_thread_pool.init_threads();

    // Delete vectors, not yet triggering consolidation.
    for (size_t i = 0; i < num_deletions; ++i) {
        int ret = VecSimIndex_DeleteVector(tiered_index, i);
        ASSERT_EQ(ret, 1);
    }
    // Num deleted should equal num_deletions.
    VecSimIndexDebugInfo info = VecSimIndex_DebugInfo(tiered_index);
    ASSERT_EQ(info.svsInfo.numberOfMarkedDeletedNodes, num_deletions);
    for (auto _ : st) {
        VecSimTieredIndex_GC(tiered_index);
        mock_thread_pool.thread_pool_wait();
    };
    // num deleted should be 0
    info = VecSimIndex_DebugInfo(tiered_index);
    ASSERT_EQ(info.svsInfo.numberOfMarkedDeletedNodes, 0);

    ASSERT_EQ(VecSimIndex_IndexSize(tiered_index), N_VECTORS - num_deletions);
}

template <typename index_type_t>
void BM_VecSimSVS<index_type_t>::InitializeSearchReference() {
    if (!search_queries.empty()) {
        return;
    }
    // Reuse the HNSW benchmark's exact vectors and labels, independently of SVS compression.
    IndexPtr source(HNSWFactory::NewIndex(AttachRootPath(hnsw_index_file)));
    auto *hnsw = dynamic_cast<HNSWIndex<data_t, dist_t> *>(static_cast<VecSimIndex *>(source));
    if (!hnsw || hnsw->indexSize() != N_VECTORS) {
        throw std::runtime_error("Search reference does not match the benchmark dataset");
    }
    BFParams params = {.type = index_type_t::get_index_type(),
                       .dim = dim,
                       .metric = VecSimMetric_Cosine,
                       .multi = is_multi,
                       .blockSize = block_size};
    reference_index = IndexPtr(CreateNewIndex(params));
    for (size_t i = 0; i < N_VECTORS; ++i) {
        VecSimIndex_AddVector(reference_index, hnsw->getDataByInternalId(i),
                              hnsw->getExternalLabel(i));
    }
    std::ifstream input(AttachRootPath(search_queries_file), std::ios::binary);
    if (!input) {
        throw std::runtime_error("Search queries file was not found");
    }
    std::vector<data_t> query(dim);
    while (input.read(reinterpret_cast<char *>(query.data()), dim * sizeof(data_t))) {
        // Match the shared normalized query inputs used by the SQ8 basics fixture.
        VecSim_Normalize(query.data(), dim, index_type_t::get_index_type());
        search_queries.push_back(query);
    }
    if (search_queries.empty() || input.gcount() != 0 || !input.eof()) {
        throw std::runtime_error("Invalid search queries file");
    }
}

template <typename index_type_t>
void BM_VecSimSVS<index_type_t>::Memory(benchmark::State &st) {
    IndexPtr index(CreateSVSIndexFromFile(1));
    for (auto _ : st) {
    }
    st.counters["memory"] =
        benchmark::Counter(static_cast<double>(VecSimIndex_StatsInfo(index).memory),
                           benchmark::Counter::kDefaults, benchmark::Counter::OneK::kIs1024);
}

template <typename index_type_t>
void BM_VecSimSVS<index_type_t>::TopK_SVS(benchmark::State &st) {
    size_t window_size = st.range(0);
    size_t k = st.range(1);
    InitializeSearchReference();
    IndexPtr index(CreateSVSIndexFromFile(1));
    if (VecSimIndex_IndexSize(index) != N_VECTORS) {
        st.SkipWithError("SVS snapshot size does not match the search reference");
        return;
    }
    SVSRuntimeParams svs_params = {.windowSize = window_size};
    VecSimQueryParams query_params = {.svsRuntimeParams = svs_params};
    size_t iter = 0;
    size_t total_exact_results = 0;
    std::atomic_int correct = 0;
    for (auto _ : st) {
        const auto &query = search_queries[iter % search_queries.size()];
        auto results = VecSimIndex_TopKQuery(index, query.data(), k, &query_params, BY_SCORE);
        st.PauseTiming();
        auto exact = VecSimIndex_TopKQuery(reference_index, query.data(), k, nullptr, BY_SCORE);
        total_exact_results += VecSimQueryReply_Len(exact);
        MeasureRecall(results, exact, correct);
        VecSimQueryReply_Free(exact);
        VecSimQueryReply_Free(results);
        ++iter;
        st.ResumeTiming();
    }
    if (total_exact_results) {
        st.counters["Recall"] = static_cast<double>(correct) / total_exact_results;
    }
}

#define UNIT_AND_ITERATIONS Unit(benchmark::kMillisecond)->Iterations(2)

#if HAVE_SVS_LVQ
#define QUANT_BITS_ARGS {VecSimSvsQuant_8, VecSimSvsQuant_4x8_LeanVec}
#define COMPRESSED_TRAINING_THRESHOLD_ARGS                                                         \
    {static_cast<long int>(BM_VecSimGeneral::block_size), 5000, 10000}
#define COMPRESSED_ASYNC_TRAINING_THRESHOLD_ARGS                                                   \
    {static_cast<long int>(BM_VecSimGeneral::block_size), 5000, 10000, 50000}
#else
#define QUANT_BITS_ARGS {VecSimSvsQuant_8}
// Using smaller training TH to avoid long test times without LVQ
#define COMPRESSED_TRAINING_THRESHOLD_ARGS                                                         \
    {static_cast<long int>(BM_VecSimGeneral::block_size), 5000}
#define COMPRESSED_ASYNC_TRAINING_THRESHOLD_ARGS                                                   \
    {static_cast<long int>(BM_VecSimGeneral::block_size), 5000}

#endif

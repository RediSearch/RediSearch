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
#include <fstream>
#include <stdexcept>
#include <string>
#include <vector>
#include "VecSim/vec_sim.h"
#include "utils/mock_thread_pool.h"

namespace benchmark_utils {

template <typename data_t>
std::vector<std::vector<data_t>> LoadTrainingVectors(const std::string &path, size_t count,
                                                     size_t dim) {
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        throw std::runtime_error("Training vector file was not found: " + path);
    }
    std::vector<std::vector<data_t>> vectors(count, std::vector<data_t>(dim));
    for (auto &vector : vectors) {
        if (!input.read(reinterpret_cast<char *>(vector.data()), dim * sizeof(data_t))) {
            throw std::runtime_error("Training vector file is too short: " + path);
        }
    }
    return vectors;
}

inline bool CheckTieredSizes(benchmark::State &st, VecSimIndex *index, size_t total,
                             size_t frontend, size_t backend) {
    const auto info = VecSimIndex_DebugInfo(index);
    if (info.commonInfo.indexSize != total ||
        info.tieredInfo.frontendCommonInfo.indexSize != frontend ||
        info.tieredInfo.backendCommonInfo.indexSize != backend) {
        st.SkipWithError("Unexpected tiered index sizes");
        return false;
    }
    return true;
}

template <typename data_t>
bool AddTrainingVectors(VecSimIndex *index, const std::vector<std::vector<data_t>> &vectors,
                        size_t first, size_t last) {
    for (size_t i = first; i < last; ++i) {
        if (VecSimIndex_AddVector(index, vectors[i].data(), i) != 1) {
            return false;
        }
    }
    return true;
}

// Enter and leave with timing paused. The caller owns index/pool setup and teardown.
template <bool is_async, typename data_t>
bool RunTrainingIteration(benchmark::State &st, VecSimIndex *index, tieredIndexMock &pool,
                          const std::vector<std::vector<data_t>> &vectors, size_t threshold) {
    if (threshold == 0 || threshold > vectors.size()) {
        st.SkipWithError("Insufficient vectors for training threshold");
        return false;
    }
    if (!AddTrainingVectors(index, vectors, 0, threshold - 1)) {
        st.SkipWithError("Training vector insertion failed");
        return false;
    }
    if (!CheckTieredSizes(st, index, threshold - 1, threshold - 1, 0)) {
        return false;
    }
    if constexpr (is_async) {
        pool.init_threads();
    }
    st.ResumeTiming();
    const int added = VecSimIndex_AddVector(index, vectors[threshold - 1].data(), threshold - 1);
    if constexpr (is_async) {
        pool.thread_pool_wait();
    }
    st.PauseTiming();
    if (added != 1) {
        st.SkipWithError("Training threshold insertion failed");
        return false;
    }
    return CheckTieredSizes(st, index, threshold, 0, threshold);
}

} // namespace benchmark_utils

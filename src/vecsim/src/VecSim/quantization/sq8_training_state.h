/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include <algorithm>
#include <cassert>
#include <span>

#include "VecSim/spaces/computer/preprocessors.h"
#include "VecSim/utils/vecsim_stl.h"

static const size_t MAX_QUANT_NORMALIZATION_SET_SIZE = 100 * DEFAULT_BLOCK_SIZE;

/** Writer-owned SQ8 training state. Inputs must be the stored, preprocessed vectors.
 * Callers synchronize mutations and subtract a vector before overwriting or deleting it.
 */
class SQ8TrainingState {
    size_t normalizationSetSize;
    vecsim_stl::vector<double> runningSumVec;
    size_t count{0};

public:
    SQ8TrainingState(const std::shared_ptr<VecSimAllocator> &allocator, size_t dim,
                     size_t normalization_set_size)
        : normalizationSetSize(std::min(normalization_set_size, MAX_QUANT_NORMALIZATION_SET_SIZE)),
          runningSumVec(dim, 0.0, allocator) {}

    template <QuantInput T>
    void add(std::span<const T> vector) {
        assert(vector.size() == runningSumVec.size());
        for (size_t i = 0; i < vector.size(); ++i) {
            runningSumVec[i] += to_fp32(vector[i]);
        }
        ++count;
    }

    template <QuantInput T>
    void subtract(std::span<const T> vector) {
        assert(count > 0);
        assert(vector.size() == runningSumVec.size());
        for (size_t i = 0; i < vector.size(); ++i) {
            runningSumVec[i] -= to_fp32(vector[i]);
        }
        --count;
    }

    bool shouldFinalize() const { return count > 0 && count >= normalizationSetSize; }

    /// Requires at least one live training vector.
    [[nodiscard]] vecsim_stl::vector<float> calculateMean() const {
        assert(count > 0);
        vecsim_stl::vector<float> mean(runningSumVec.size(), runningSumVec.getAllocator());
        for (size_t i = 0; i < runningSumVec.size(); ++i) {
            mean[i] = static_cast<float>(runningSumVec[i] / static_cast<double>(count));
        }
        return mean;
    }

    size_t getCount() const { return count; }
    size_t getNormalizationSetSize() const { return normalizationSetSize; }
    const vecsim_stl::vector<double> &getRunningSum() const { return runningSumVec; }
};

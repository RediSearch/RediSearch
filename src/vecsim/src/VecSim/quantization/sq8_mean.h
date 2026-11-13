/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include <cassert>
#include <span>

#include "VecSim/spaces/computer/calculator.h"
#include "VecSim/spaces/computer/preprocessor_container.h"

// Install before storing vectors, with queries excluded by the caller. Requires exactly one
// SQ8 WithNorm preprocessor in slot 0; cosine normalization must be performed by the frontend.
template <QuantInput DataType, VecSimMetric Metric>
void applySQ8Mean(PreprocessorsContainerAbstract &preprocessors,
                  IndexCalculatorInterface<float> &index_calculator,
                  std::span<const float> mean) noexcept {
    auto &container = static_cast<MultiPreprocessorsContainer<DataType, 1> &>(preprocessors);
    auto *preprocessor =
        dynamic_cast<QuantPreprocessor<DataType, Metric, true> *>(container.getPreprocessors()[0]);
    auto *calculator =
        dynamic_cast<DistanceCalculatorWithNorm<DataType, float, Metric> *>(&index_calculator);
    assert(preprocessor && calculator);
    preprocessor->setMean(mean);
    calculator->setMeanSumSquares(mean);
}

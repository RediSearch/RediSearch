/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include "VecSim/spaces/spaces.h"
#include "VecSim/vec_sim_common.h"
#include "VecSim/vec_sim_index.h"
#include "VecSim/index_factories/components/preprocessors_factory.h"
#include "VecSim/spaces/computer/calculator.h"

template <typename DataType, typename DistType>
IndexComponents<DataType, DistType>
CreateIndexComponents(std::shared_ptr<VecSimAllocator> allocator, VecSimMetric metric, size_t dim,
                      bool is_normalized) {
    unsigned char alignment = 0;
    spaces::dist_func_t<DistType> distFunc =
        spaces::GetDistFunc<DataType, DistType>(metric, dim, &alignment);
    // Currently we have only one distance calculator implementation
    auto indexCalculator = new (allocator) DistanceCalculatorCommon<DistType>(allocator, distFunc);

    // TODO: take into account quantization
    auto preprocessors =
        CreatePreprocessorsContainer<DataType>(allocator, metric, dim, is_normalized, alignment);

    return {indexCalculator, preprocessors};
}

// Asymmetric dispatch reports alignment for the stored operand only. Ask the query type's
// dispatcher for the query allocation alignment.
template <typename DataType>
[[nodiscard]] unsigned char GetQueryAlignment(VecSimMetric metric, size_t dim) {
    unsigned char alignment = 0;
    spaces::GetDistFunc<DataType, float>(metric, dim, &alignment);
    return alignment;
}

template <typename DataType, VecSimMetric Metric, bool WithMean>
IndexComponents<DataType, float>
CreateSQ8IndexComponents(const std::shared_ptr<VecSimAllocator> &allocator, size_t dim,
                         const float *mean_ptr) {
    using Preprocessor = QuantPreprocessor<DataType, Metric, WithMean>;
    using QueryType = typename Preprocessor::QueryType;
    unsigned char storage_alignment = 0, asym_storage_alignment = 0;

    // Graph construction compares two stored SQ8 blobs; search compares a stored blob with a
    // QueryType query. Both dispatchers report alignment for the stored operand.
    auto sym_func = spaces::GetDistFunc<vecsim_types::sq8, float>(Metric, dim, &storage_alignment);
    auto asym_func = spaces::GetDistFunc<vecsim_types::sq8, float, QueryType>(
        Metric, dim, &asym_storage_alignment);
    storage_alignment = spaces::combineAlignments(storage_alignment, asym_storage_alignment);
    const unsigned char query_alignment = GetQueryAlignment<QueryType>(Metric, dim);

    PreprocessorInterface *pp = nullptr;
    IndexCalculatorInterface<float> *calc = nullptr;

    if constexpr (WithMean) {
        vecsim_stl::vector<float> mean_vec(allocator);
        mean_vec.assign(mean_ptr, mean_ptr + dim);

        float mean_sum_squares = 0.0f;
        for (float v : mean_vec) {
            mean_sum_squares += v * v;
        }

        pp = new (allocator) Preprocessor(allocator, dim, mean_vec);
        calc = new (allocator) DistanceCalculatorWithNorm<DataType, float, Metric>(
            allocator, asym_func, sym_func, mean_sum_squares);
    } else {
        pp = new (allocator) Preprocessor(allocator, dim);
        calc = new (allocator) DistanceCalculatorCommon<float>(allocator, sym_func, asym_func);
    }

    auto *container = new (allocator)
        MultiPreprocessorsContainer<DataType, 1>(allocator, query_alignment, storage_alignment);
    [[maybe_unused]] const int ret = container->addPreprocessor(pp);
    assert(ret != -1 && "SQ8 preprocessor was not added correctly");

    return {calc, container};
}

template <typename DataType, VecSimMetric Metric>
IndexComponents<DataType, float>
CreateSQ8IndexComponents(const std::shared_ptr<VecSimAllocator> &allocator, size_t dim,
                         const float *mean_ptr) {
    if (mean_ptr) {
        return CreateSQ8IndexComponents<DataType, Metric, true>(allocator, dim, mean_ptr);
    }
    return CreateSQ8IndexComponents<DataType, Metric, false>(allocator, dim, mean_ptr);
}

template <typename DataType, typename DistType>
size_t EstimateComponentsMemory(VecSimMetric metric, bool is_normalized) {
    size_t allocations_overhead = VecSimAllocator::getAllocationOverheadSize();

    // Currently we have only one distance calculator implementation
    size_t est = allocations_overhead + sizeof(DistanceCalculatorCommon<DistType>);

    est += EstimatePreprocessorsContainerMemory<DataType>(metric, is_normalized);

    return est;
}

/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include <stdexcept>
#include "vecsim/spaces/spaces.h"
#include "vecsim/vec_sim_common.h"
#include "vecsim/vec_sim_index.h"
#include "vecsim/index_factories/components/preprocessors_factory.h"
#include "vecsim/spaces/computer/calculator.h"
#include "vecsim/spaces/computer/sq8_fp64_preprocessor.h"

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
// dispatcher for the query allocation alignment. FP64 quantized queries require no alignment.
template <typename DataType, typename DistType = float>
[[nodiscard]] unsigned char GetQueryAlignment(VecSimMetric metric, size_t dim) {
    if constexpr (std::same_as<DistType, double>) {
        return 0;
    }

    unsigned char alignment = 0;
    spaces::GetDistFunc<DataType, DistType>(metric, dim, &alignment);
    return alignment;
}

template <typename DataType, VecSimMetric Metric, bool WithMean, typename StorageType>
    requires QuantPreprocessorInput<DataType, StorageType, WithMean>
IndexComponents<DataType, typename StorageType::metadata_type>
CreateQuantizedIndexComponents(const std::shared_ptr<VecSimAllocator> &allocator, size_t dim,
                               const float *mean_ptr) {
    using DistType = typename StorageType::metadata_type;
    using Preprocessor = QuantPreprocessor<DataType, Metric, WithMean, StorageType>;
    using QueryType = typename Preprocessor::QueryType;
    if constexpr (std::same_as<DistType, double>) {
        if (mean_ptr) {
            throw std::invalid_argument("FP64 quantization does not support mean training");
        }
    }
    unsigned char storage_alignment = 0;
    unsigned char asym_storage_alignment = 0;

    // Graph construction compares two stored quantized blobs; search compares a stored blob with a
    // QueryType query. Both dispatchers report alignment for the stored operand.
    const auto sym_func =
        spaces::GetDistFunc<StorageType, DistType>(Metric, dim, &storage_alignment);
    const auto asym_func =
        spaces::GetDistFunc<StorageType, DistType, QueryType>(Metric, dim, &asym_storage_alignment);
    storage_alignment = spaces::combineAlignments(storage_alignment, asym_storage_alignment);
    const unsigned char query_alignment = GetQueryAlignment<QueryType, DistType>(Metric, dim);

    PreprocessorInterface *pp = nullptr;
    IndexCalculatorInterface<DistType> *calc = nullptr;

    if constexpr (WithMean) {
        vecsim_stl::vector<float> mean_vec(allocator);
        mean_vec.assign(mean_ptr, mean_ptr + dim);

        float mean_sum_squares = 0.0f;
        for (float v : mean_vec) {
            mean_sum_squares += v * v;
        }

        pp = new (allocator) Preprocessor(allocator, dim, mean_vec);
        calc = new (allocator) DistanceCalculatorWithNorm<DataType, float, Metric, StorageType>(
            allocator, asym_func, sym_func, mean_sum_squares);
    } else {
        pp = new (allocator) Preprocessor(allocator, dim);
        calc = new (allocator) DistanceCalculatorCommon<DistType>(allocator, sym_func, asym_func);
    }

    auto *container = new (allocator)
        MultiPreprocessorsContainer<DataType, 1>(allocator, query_alignment, storage_alignment);
    [[maybe_unused]] const int ret = container->addPreprocessor(pp);
    assert(ret != -1 && "Quantization preprocessor was not added correctly");

    return {calc, container};
}

// Run-time overload: selects WithMean from whether mean_ptr is set.
template <typename DataType, VecSimMetric Metric, typename StorageType>
    requires QuantPreprocessorInput<DataType, StorageType, false>
auto CreateQuantizedIndexComponents(const std::shared_ptr<VecSimAllocator> &allocator, size_t dim,
                                    const float *mean_ptr) {
    constexpr bool supports_training = QuantPreprocessorInput<DataType, StorageType, true>;
    if constexpr (supports_training) {
        if (mean_ptr) {
            return CreateQuantizedIndexComponents<DataType, Metric, true, StorageType>(
                allocator, dim, mean_ptr);
        }
    }
    return CreateQuantizedIndexComponents<DataType, Metric, false, StorageType>(allocator, dim,
                                                                                mean_ptr);
}

template <typename DataType, VecSimMetric Metric, bool WithMean>
IndexComponents<DataType, float>
CreateSQ8IndexComponents(const std::shared_ptr<VecSimAllocator> &allocator, size_t dim,
                         const float *mean_ptr) {
    return CreateQuantizedIndexComponents<DataType, Metric, WithMean, vecsim_types::sq8>(
        allocator, dim, mean_ptr);
}

template <typename DataType, VecSimMetric Metric>
IndexComponents<DataType, float>
CreateSQ8IndexComponents(const std::shared_ptr<VecSimAllocator> &allocator, size_t dim,
                         const float *mean_ptr) {
    return CreateQuantizedIndexComponents<DataType, Metric, vecsim_types::sq8>(allocator, dim,
                                                                               mean_ptr);
}

// Cosine storage and query inputs must already be normalized by the caller.
template <VecSimMetric Metric>
IndexComponents<double, double>
CreateSQ8FP64IndexComponents(const std::shared_ptr<VecSimAllocator> &allocator, size_t dim) {
    return CreateQuantizedIndexComponents<double, Metric, false, vecsim_types::sq8_fp64>(
        allocator, dim, nullptr);
}

template <typename DataType, typename DistType>
size_t EstimateComponentsMemory(VecSimMetric metric, bool is_normalized) {
    size_t allocations_overhead = VecSimAllocator::getAllocationOverheadSize();

    // Currently we have only one distance calculator implementation
    size_t est = allocations_overhead + sizeof(DistanceCalculatorCommon<DistType>);

    est += EstimatePreprocessorsContainerMemory<DataType>(metric, is_normalized);

    return est;
}

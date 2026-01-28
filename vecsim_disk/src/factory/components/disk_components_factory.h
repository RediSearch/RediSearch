/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include "VecSim/types/sq8.h"
#include "VecSim/vec_sim_index.h"
#include "VecSim/index_factories/components/preprocessors_factory.h"
#include "VecSim/spaces/computer/preprocessors.h"
#include "disk_calculator.h"

/**
 * Disk-specific index components with typed DiskDistanceCalculator.
 *
 * This struct provides compile-time type safety by holding a DiskDistanceCalculator*
 * instead of the generic IndexCalculatorInterface*. It implicitly converts to
 * IndexComponents for compatibility with VecSimIndexAbstract.
 */
template <typename DataType, typename DistType>
struct DiskIndexComponents {
    DiskDistanceCalculator<DistType>* diskCalculator;
    PreprocessorsContainerAbstract* preprocessors;

    // Implicit conversion to IndexComponents for base class compatibility
    operator IndexComponents<DataType, DistType>() const { return {diskCalculator, preprocessors}; }
};

/**
 * Factory functions for creating disk-specific index components.
 *
 * The disk HNSW index uses scalar quantization (SQ8) for storage efficiency.
 * This requires three different distance calculators:
 * - Full (FP32-FP32): For exact distance calculations and reranking
 * - QuantizedVsFull (SQ8-FP32): For query vs stored vectors during search
 * - Quantized (SQ8-SQ8): For graph operations (neighbor selection)
 */
namespace DiskComponentsFactory {

/**
 * Creates a multi-mode distance calculator for disk-based indexes.
 *
 * The calculator provides three distance functions with compile-time mode selection:
 * - Full: spaces::GetDistFunc<float, DistType>(metric, dim)
 * - QuantizedVsFull: spaces::GetDistFunc<sq8, DistType, float>(metric, dim)
 * - Quantized: spaces::GetDistFunc<sq8, DistType>(metric, dim)
 *
 * @tparam DistType The distance return type (typically float)
 * @param allocator Memory allocator
 * @param metric Distance metric (L2, IP, or Cosine)
 * @param dim Vector dimension
 * @return Pointer to the created calculator (owned by caller)
 */
template <typename DistType>
DiskDistanceCalculator<DistType>* CreateDiskCalculator(std::shared_ptr<VecSimAllocator> allocator, VecSimMetric metric,
                                                       size_t dim) {
    using sq8 = vecsim_types::sq8;

    // TODO: need to extand the alignmet to support query alignmet and
    // storage alignment separately.
    // ATM, because there's only one alignment variable, we can't support different
    // alignments for query and storage.
    // Full precision: FP32 ↔ FP32 (exact distance / reranking)
    auto fullFunc = spaces::GetDistFunc<float, DistType>(metric, dim, nullptr);

    // Asymmetric: SQ8 ↔ FP32 (quantized storage vs full precision query)
    auto quantizedVsFullFunc = spaces::GetDistFunc<sq8, DistType, float>(metric, dim, nullptr);

    // Symmetric quantized: SQ8 ↔ SQ8 (both quantized, for graph operations)
    auto quantizedFunc = spaces::GetDistFunc<sq8, DistType>(metric, dim, nullptr);

    return new (allocator) DiskDistanceCalculator<DistType>(allocator, fullFunc, quantizedVsFullFunc, quantizedFunc);
}

/**
 * Creates a QuantPreprocessor-based container for disk indexes with SQ8 quantization.
 *
 * The disk index uses SQ8 quantization for storage, requiring:
 * - Storage preprocessing: Quantize FP32 to SQ8 with metadata (min, delta, sum, sum_squares)
 * - Query preprocessing: Append sum and sum_squares to FP32 query
 *
 * Note: Currently, the disk index is only used within a tiered index context, where the
 * frontend index (e.g., BruteForce) normalizes vectors for Cosine/IP metrics before passing
 * them to the disk backend. Therefore, is_normalized is expected to always be true.
 * If standalone disk index usage is needed in the future, normalization handling would
 * need to be implemented here.
 *
 * @tparam DataType The data type (for now only float is supported)
 * @tparam Metric The distance metric (compile-time constant)
 * @param allocator Memory allocator
 * @param dim Vector dimension
 * @param is_normalized Whether input vectors are already normalized (must be true)
 * @param alignment Memory alignment for query blobs
 * @return Preprocessor container with QuantPreprocessor for SQ8 quantization
 */
template <typename DataType, VecSimMetric Metric>
PreprocessorsContainerAbstract* CreateDiskPreprocessorsContainer(std::shared_ptr<VecSimAllocator> allocator, size_t dim,
                                                                 [[maybe_unused]] bool is_normalized,
                                                                 unsigned char alignment) {
    // Only float is supported for now, remove when other types are supported
    static_assert(std::is_same_v<DataType, float>, "Disk index with SQ8 quantization only supports float");

    // Disk index is currently only used within tiered context where frontend normalizes vectors.
    // If standalone usage is needed, normalization handling must be added here.
    // Support in the preprocessor also needs to be added, in order to support this.

    assert(is_normalized && "Disk index expects pre-normalized vectors from tiered frontend");

    constexpr size_t n_preprocessors = 1;
    auto* multiPPContainer =
        new (allocator) MultiPreprocessorsContainer<DataType, n_preprocessors>(allocator, alignment);
    auto* quant_preprocessor = new (allocator) QuantPreprocessor<DataType, Metric>(allocator, dim);
    multiPPContainer->addPreprocessor(quant_preprocessor);
    return multiPPContainer;
}

template <typename DataType>
PreprocessorsContainerAbstract* CreatePreprocessorsForMetric(std::shared_ptr<VecSimAllocator> allocator,
                                                             VecSimMetric metric, size_t dim, bool is_normalized,
                                                             unsigned char alignment) {
    assert(metric == VecSimMetric_L2 || metric == VecSimMetric_IP || metric == VecSimMetric_Cosine);
    switch (metric) {
    case VecSimMetric_L2:
        return CreateDiskPreprocessorsContainer<DataType, VecSimMetric_L2>(allocator, dim, is_normalized, alignment);
    case VecSimMetric_IP:
        return CreateDiskPreprocessorsContainer<DataType, VecSimMetric_IP>(allocator, dim, is_normalized, alignment);
    case VecSimMetric_Cosine:
        return CreateDiskPreprocessorsContainer<DataType, VecSimMetric_Cosine>(allocator, dim, is_normalized,
                                                                               alignment);
    }
    throw std::invalid_argument("Unsupported metric for disk index preprocessors");
}

/**
 * Creates complete index components for a disk-based HNSW index.
 *
 * This creates:
 * - A DiskDistanceCalculator with all three distance modes
 * - A preprocessors container with QuantPreprocessor for SQ8 quantization
 *
 * @tparam DataType The input data type (typically float)
 * @tparam DistType The distance return type (typically float)
 * @param allocator Memory allocator
 * @param metric Distance metric (L2, IP, or Cosine)
 * @param dim Vector dimension
 * @param is_normalized Whether input vectors are already normalized
 * @return DiskIndexComponents containing the typed calculator and preprocessors
 */
template <typename DataType, typename DistType>
DiskIndexComponents<DataType, DistType> CreateDiskIndexComponents(std::shared_ptr<VecSimAllocator> allocator,
                                                                  VecSimMetric metric, size_t dim, bool is_normalized) {
    unsigned char alignment = 0;

    // Create the multi-mode disk calculator
    auto* diskCalculator = CreateDiskCalculator<DistType>(allocator, metric, dim);

    // Create preprocessors with QuantPreprocessor for SQ8 quantization
    PreprocessorsContainerAbstract* preprocessors =
        CreatePreprocessorsForMetric<DataType>(allocator, metric, dim, is_normalized, alignment);

    return {diskCalculator, preprocessors};
}

} // namespace DiskComponentsFactory

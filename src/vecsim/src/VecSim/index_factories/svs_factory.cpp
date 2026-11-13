/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "VecSim/index_factories/svs_factory.h"

#if HAVE_SVS
#include "VecSim/memory/vecsim_malloc.h"
#include "VecSim/vec_sim_index.h"
#include "VecSim/algorithms/svs/svs.h"
#include "VecSim/algorithms/svs/svs_utils.h"
#include "VecSim/index_factories/components/components_factory.h"
#include "VecSim/index_factories/factory_utils.h"
#include <atomic>

namespace SVSFactory {

namespace {

// Bytes consumed by the per-index VecSimSVSThreadPool::parallelism_ allocation
// (a std::allocate_shared<std::atomic<size_t>> through the index's allocator).
// The exact size of the inplace shared_ptr control block is implementation-defined,
// so measure the allocation delta once at startup against a throwaway VecSimAllocator.
size_t getThreadPoolParallelismAllocationSize() {
    static const size_t size = []() {
        auto allocator = VecSimAllocator::newVecsimAllocator();
        size_t before = allocator->getAllocationSize();
        [[maybe_unused]] auto p = std::allocate_shared<std::atomic<size_t>>(
            VecsimSTLAllocator<std::atomic<size_t>>(allocator), size_t{1});
        return allocator->getAllocationSize() - before;
    }();
    return size;
}

// NewVectorsImpl() is the chain of a template helper functions to create a new SVS index.
template <typename MetricType, typename DataType, size_t QuantBits, size_t ResidualBits,
          bool IsLeanVec>
VecSimIndex *NewIndexImpl(const VecSimParams *params, bool is_normalized) {
    auto &svsParams = params->algoParams.svsParams;
    auto abstractInitParams =
        VecSimFactory::NewAbstractInitParams(&svsParams, params->logCtx, is_normalized);
    auto preprocessors = CreatePreprocessorsContainer<svs_details::vecsim_dt<DataType>>(
        abstractInitParams.allocator, svsParams.metric, svsParams.dim, is_normalized, 0);
    IndexComponents<svs_details::vecsim_dt<DataType>, float> components = {
        nullptr, preprocessors}; // calculator is not in use in svs.
    bool forcePreprocessing = !is_normalized && svsParams.metric == VecSimMetric_Cosine;
    if (svsParams.multi) {
        return new (abstractInitParams.allocator)
            SVSIndex<MetricType, DataType, true, QuantBits, ResidualBits, IsLeanVec>(
                svsParams, abstractInitParams, components, forcePreprocessing);
    } else {
        return new (abstractInitParams.allocator)
            SVSIndex<MetricType, DataType, false, QuantBits, ResidualBits, IsLeanVec>(
                svsParams, abstractInitParams, components, forcePreprocessing);
    }
}

template <typename MetricType, typename DataType>
VecSimIndex *NewIndexImpl(const VecSimParams *params, bool is_normalized) {
    // Ignore the 'supported' flag because we always fallback at least to the non-quantized mode
    // elsewhere we got code coverage failure for the `supported==false` case
    auto quantBits =
        std::get<0>(svs_details::isSVSQuantBitsSupported(params->algoParams.svsParams.quantBits));

    switch (quantBits) {
    case VecSimSvsQuant_NONE:
        return NewIndexImpl<MetricType, DataType, 0, 0, false>(params, is_normalized);
    case VecSimSvsQuant_Scalar:
        return NewIndexImpl<MetricType, DataType, 1, 0, false>(params, is_normalized);
    case VecSimSvsQuant_8:
        return NewIndexImpl<MetricType, DataType, 8, 0, false>(params, is_normalized);
    case VecSimSvsQuant_4:
        return NewIndexImpl<MetricType, DataType, 4, 0, false>(params, is_normalized);
    case VecSimSvsQuant_4x4:
        return NewIndexImpl<MetricType, DataType, 4, 4, false>(params, is_normalized);
    case VecSimSvsQuant_4x8:
        return NewIndexImpl<MetricType, DataType, 4, 8, false>(params, is_normalized);
    case VecSimSvsQuant_4x8_LeanVec:
        return NewIndexImpl<MetricType, DataType, 4, 8, true>(params, is_normalized);
    case VecSimSvsQuant_8x8_LeanVec:
        return NewIndexImpl<MetricType, DataType, 8, 8, true>(params, is_normalized);
    default:
        // If we got here something is wrong.
        assert(false && "Unsupported quantization mode");
        return NULL;
    }
}

template <typename MetricType>
VecSimIndex *NewIndexImpl(const VecSimParams *params, bool is_normalized) {
    assert(params && params->algo == VecSimAlgo_SVS);
    switch (params->algoParams.svsParams.type) {
    case VecSimType_FLOAT32:
        return NewIndexImpl<MetricType, float>(params, is_normalized);
    case VecSimType_FLOAT16:
        return NewIndexImpl<MetricType, svs::Float16>(params, is_normalized);
    default:
        // If we got here something is wrong.
        assert(false && "Unsupported data type");
        return NULL;
    }
}

VecSimIndex *NewIndexImpl(const VecSimParams *params, bool is_normalized) {
    assert(params && params->algo == VecSimAlgo_SVS);
    switch (params->algoParams.svsParams.metric) {
    case VecSimMetric_L2:
        return NewIndexImpl<svs::distance::DistanceL2>(params, is_normalized);
    case VecSimMetric_IP:
    case VecSimMetric_Cosine:
        return NewIndexImpl<svs::distance::DistanceIP>(params, is_normalized);
    default:
        // If we got here something is wrong.
        assert(false && "Unknown distance metric type");
        return NULL;
    }
}

// QuantizedVectorSize() is the chain of template functions to estimate vector DataSize.
template <typename DataType, size_t QuantBits, size_t ResidualBits, bool IsLeanVec>
constexpr size_t QuantizedVectorSize(size_t dims, size_t alignment = 0, size_t leanvec_dim = 0) {
    return SVSStorageTraits<DataType, QuantBits, ResidualBits, IsLeanVec>::element_size(
        dims, alignment, leanvec_dim);
}

template <typename DataType>
size_t QuantizedVectorSize(VecSimSvsQuantBits quant_bits, size_t dims, size_t alignment = 0,
                           size_t leanvec_dim = 0) {
    // Ignore the 'supported' flag because we always fallback at least to the non-quantized mode
    // elsewhere we got code coverage failure for the `supported==false` case
    auto quantBits = std::get<0>(svs_details::isSVSQuantBitsSupported(quant_bits));

    switch (quantBits) {
    case VecSimSvsQuant_NONE:
        return QuantizedVectorSize<DataType, 0, 0, false>(dims, alignment);
    case VecSimSvsQuant_Scalar:
        return QuantizedVectorSize<DataType, 1, 0, false>(dims, alignment);
    case VecSimSvsQuant_8:
        return QuantizedVectorSize<DataType, 8, 0, false>(dims, alignment);
    case VecSimSvsQuant_4:
        return QuantizedVectorSize<DataType, 4, 0, false>(dims, alignment);
    case VecSimSvsQuant_4x4:
        return QuantizedVectorSize<DataType, 4, 4, false>(dims, alignment);
    case VecSimSvsQuant_4x8:
        return QuantizedVectorSize<DataType, 4, 8, false>(dims, alignment);
    case VecSimSvsQuant_4x8_LeanVec:
        return QuantizedVectorSize<DataType, 4, 8, true>(dims, alignment, leanvec_dim);
    case VecSimSvsQuant_8x8_LeanVec:
        return QuantizedVectorSize<DataType, 8, 8, true>(dims, alignment, leanvec_dim);
    default:
        // If we got here something is wrong.
        assert(false && "Unsupported quantization mode");
        return 0;
    }
}

size_t QuantizedVectorSize(VecSimType data_type, VecSimSvsQuantBits quant_bits, size_t dims,
                           size_t alignment = 0, size_t leanvec_dim = 0) {
    switch (data_type) {
    case VecSimType_FLOAT32:
        return QuantizedVectorSize<float>(quant_bits, dims, alignment, leanvec_dim);
    case VecSimType_FLOAT16:
        return QuantizedVectorSize<svs::Float16>(quant_bits, dims, alignment, leanvec_dim);
    default:
        // If we got here something is wrong.
        assert(false && "Unsupported data type");
        return 0;
    }
}

size_t EstimateSVSIndexSize(const SVSParams *params) {
    // SVSindex class has no fields which size depend on template specialization
    // when VecSimIndexAbstract may depend on DataType template parameter
    switch (params->type) {
    case VecSimType_FLOAT32:
        return sizeof(SVSIndex<svs::distance::DistanceL2, float, false, 0, 0, false>);
    case VecSimType_FLOAT16:
        return sizeof(SVSIndex<svs::distance::DistanceL2, svs::Float16, false, 0, 0, false>);
    default:
        // If we got here something is wrong.
        assert(false && "Unsupported data type");
        return 0;
    }
}

size_t EstimateComponentsMemorySVS(VecSimType type, VecSimMetric metric, bool is_normalized) {
    // SVS index only includes a preprocessor container.
    switch (type) {
    case VecSimType_FLOAT32:
        return EstimatePreprocessorsContainerMemory<float>(metric, is_normalized);
    case VecSimType_FLOAT16:
        return EstimatePreprocessorsContainerMemory<svs_details::vecsim_dt<svs::Float16>>(
            metric, is_normalized);
    default:
        // If we got here something is wrong.
        assert(false && "Unsupported data type");
        return 0;
    }
}
} // namespace

VecSimIndex *NewIndex(const VecSimParams *params, bool is_normalized) {
    return NewIndexImpl(params, is_normalized);
}

#ifdef BUILD_TESTS
VecSimIndex *NewIndex(const std::string &location, const VecSimParams *params, bool is_normalized) {
    auto index = NewIndexImpl(params, is_normalized);
    // Side-cast to SVSIndexBase to call loadIndex
    SVSIndexBase *svs_index = dynamic_cast<SVSIndexBase *>(index);
    if (svs_index != nullptr) {
        try {
            svs_index->loadIndex(location);
        } catch (const std::exception &e) {
            VecSimIndex_Free(index);
            throw;
        }
    } else {
        VecSimIndex_Free(index);
        throw std::runtime_error(
            "Cannot load index: Error in index creation before loading serialization");
    }
    return index;
}
#endif

size_t EstimateElementSize(const SVSParams *params) {
    using graph_idx_type = uint32_t;
    // Assuming that the graph_max_degree can be unset in params.
    const auto graph_max_degree = svs_details::makeVamanaBuildParameters(*params).graph_max_degree;
    const auto graph_node_size = SVSGraphBuilder<graph_idx_type>::element_size(graph_max_degree);
    const auto reverse_edges_node_size =
        SVSGraphBuilder<graph_idx_type>::reverse_edges_element_size();
    const auto vector_size =
        QuantizedVectorSize(params->type, params->quantBits, params->dim, 0, params->leanvec_dim);

    return vector_size + graph_node_size + reverse_edges_node_size;
}

size_t EstimateInitialSize(const SVSParams *params, bool is_normalized) {
    size_t allocations_overhead = VecSimAllocator::getAllocationOverheadSize();
    size_t est = sizeof(VecSimAllocator) + allocations_overhead;

    est += EstimateSVSIndexSize(params);
    est += EstimateComponentsMemorySVS(params->type, params->metric, is_normalized);
    est += sizeof(DataBlocksContainer) + allocations_overhead;
    // Per-index VecSimSVSThreadPool::parallelism_ allocation tracked through
    // the index's VecSimAllocator (see VecSimSVSThreadPool ctor).
    est += getThreadPoolParallelismAllocationSize();
    return est;
}

} // namespace SVSFactory

// This is a temporary solution to avoid breaking the build when SVS is not available
// and to allow the code to compile without SVS support.
// TODO: remove HAVE_SVS when SVS will support all Redis platforms and compilers
#else // HAVE_SVS
namespace SVSFactory {
VecSimIndex *NewIndex(const VecSimParams *params, bool is_normalized) { return NULL; }
#ifdef BUILD_TESTS
VecSimIndex *NewIndex(const std::string &location, const VecSimParams *params, bool is_normalized) {
    return NULL;
}
#endif
size_t EstimateInitialSize(const SVSParams *params, bool is_normalized) { return -1; }
size_t EstimateElementSize(const SVSParams *params) { return -1; }
}; // namespace SVSFactory
#endif // HAVE_SVS

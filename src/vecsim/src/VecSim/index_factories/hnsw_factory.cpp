/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "VecSim/algorithms/hnsw/hnsw_single.h"
#include "VecSim/algorithms/hnsw/hnsw_multi.h"
#include "VecSim/index_factories/hnsw_factory.h"
#include "VecSim/index_factories/components/components_factory.h"
#include "VecSim/index_factories/factory_utils.h"
#include "VecSim/algorithms/hnsw/hnsw.h"
#include "VecSim/types/bfloat16.h"
#include "VecSim/types/float16.h"

#include <type_traits>

using bfloat16 = vecsim_types::bfloat16;
using float16 = vecsim_types::float16;
using sq8 = vecsim_types::sq8;

namespace HNSWFactory {

template <typename DataType, typename DistType = DataType>
inline HNSWIndex<DataType, DistType> *
NewIndex_ChooseMultiOrSingle(const HNSWParams *params,
                             const AbstractIndexInitParams &abstractInitParams,
                             IndexComponents<DataType, DistType> &components) {
    // check if single and return new hnsw_index
    if (params->multi)
        return new (abstractInitParams.allocator)
            HNSWIndex_Multi<DataType, DistType>(params, abstractInitParams, components);
    else
        return new (abstractInitParams.allocator)
            HNSWIndex_Single<DataType, DistType>(params, abstractInitParams, components);
}

template <VecSimMetric Metric>
[[nodiscard]] constexpr size_t GetSQ8StoredDataSize(size_t dim, bool with_norm) {
    static_assert(Metric == VecSimMetric_L2 || Metric == VecSimMetric_IP);

    return with_norm ? sq8::storage_bytes_count<Metric, true>(dim)
                     : sq8::storage_bytes_count<Metric, false>(dim);
}

// Cosine over pre-normalized vectors is computed as inner product.
[[nodiscard]] constexpr VecSimMetric ResolveSQ8Metric(VecSimMetric metric, bool is_normalized) {
    return (is_normalized && metric == VecSimMetric_Cosine) ? VecSimMetric_IP : metric;
}

// Keep construction and initial-size validation in sync.
[[nodiscard]] constexpr bool SQ8ParamsSupported(VecSimType type, VecSimMetric resolved_metric) {
    // SQ8 kernels accept only FLOAT32 and FLOAT16 input.
    if (type != VecSimType_FLOAT32 && type != VecSimType_FLOAT16) {
        return false;
    }
    // Only L2 and inner product have SQ8 kernels.
    if (resolved_metric != VecSimMetric_L2 && resolved_metric != VecSimMetric_IP) {
        return false;
    }
    return true;
}

template <typename DataType, VecSimMetric Metric>
VecSimIndex *NewIndex_SQ8(const HNSWParams *hnswParams, AbstractIndexInitParams abstractInitParams,
                          const float *mean_ptr) {
    const bool with_norm = mean_ptr != nullptr;
    abstractInitParams.storedDataSize =
        GetSQ8StoredDataSize<Metric>(abstractInitParams.dim, with_norm);
    abstractInitParams.isQuantized = true;

    IndexComponents<DataType, float> components = CreateSQ8IndexComponents<DataType, Metric>(
        abstractInitParams.allocator, abstractInitParams.dim, mean_ptr);
    auto index =
        NewIndex_ChooseMultiOrSingle<DataType, float>(hnswParams, abstractInitParams, components);
#ifdef BUILD_TESTS
    // Store quantization metadata for re-serialization.
    index->quantType = hnswParams->quantType;
    if (mean_ptr != nullptr) {
        index->serializedMeanVector.assign(mean_ptr, mean_ptr + abstractInitParams.dim);
    }
#endif
    return index;
}

VecSimIndex *NewIndex(const VecSimParams *params, bool is_normalized) {
    const HNSWParams *hnswParams = &params->algoParams.hnswParams;
    AbstractIndexInitParams abstractInitParams =
        VecSimFactory::NewAbstractInitParams(hnswParams, params->logCtx, is_normalized);

    if (hnswParams->quantType != VecSimQuant_NONE) {
        // Reject unknown quantizers instead of silently creating an unquantized index.
        if (hnswParams->quantType != VecSimQuant_SQ8) {
            return NULL;
        }

        const VecSimMetric metric = ResolveSQ8Metric(hnswParams->metric, is_normalized);
        const float *mean_ptr = static_cast<const float *>(hnswParams->quantParams);

        if (!SQ8ParamsSupported(hnswParams->type, metric)) {
            return NULL;
        }

        if (hnswParams->type == VecSimType_FLOAT32) {
            if (metric == VecSimMetric_L2) {
                return NewIndex_SQ8<float, VecSimMetric_L2>(hnswParams, abstractInitParams,
                                                            mean_ptr);
            } else if (metric == VecSimMetric_IP) {
                return NewIndex_SQ8<float, VecSimMetric_IP>(hnswParams, abstractInitParams,
                                                            mean_ptr);
            }
        } else if (hnswParams->type == VecSimType_FLOAT16) {
            if (metric == VecSimMetric_L2) {
                return NewIndex_SQ8<float16, VecSimMetric_L2>(hnswParams, abstractInitParams,
                                                              mean_ptr);
            } else if (metric == VecSimMetric_IP) {
                return NewIndex_SQ8<float16, VecSimMetric_IP>(hnswParams, abstractInitParams,
                                                              mean_ptr);
            }
        }

        // Keep the return for release builds so future dispatch changes cannot fall through.
        assert(false && "unhandled SQ8 data type and metric combination");
        return NULL;
    }

    if (hnswParams->type == VecSimType_FLOAT32) {
        IndexComponents<float, float> indexComponents = CreateIndexComponents<float, float>(
            abstractInitParams.allocator, hnswParams->metric, hnswParams->dim, is_normalized);
        return NewIndex_ChooseMultiOrSingle<float>(hnswParams, abstractInitParams, indexComponents);

    } else if (hnswParams->type == VecSimType_FLOAT64) {
        IndexComponents<double, double> indexComponents = CreateIndexComponents<double, double>(
            abstractInitParams.allocator, hnswParams->metric, hnswParams->dim, is_normalized);
        return NewIndex_ChooseMultiOrSingle<double>(hnswParams, abstractInitParams,
                                                    indexComponents);

    } else if (hnswParams->type == VecSimType_BFLOAT16) {
        IndexComponents<bfloat16, float> indexComponents = CreateIndexComponents<bfloat16, float>(
            abstractInitParams.allocator, hnswParams->metric, hnswParams->dim, is_normalized);
        return NewIndex_ChooseMultiOrSingle<bfloat16, float>(hnswParams, abstractInitParams,
                                                             indexComponents);
    } else if (hnswParams->type == VecSimType_FLOAT16) {
        IndexComponents<float16, float> indexComponents = CreateIndexComponents<float16, float>(
            abstractInitParams.allocator, hnswParams->metric, hnswParams->dim, is_normalized);
        return NewIndex_ChooseMultiOrSingle<float16, float>(hnswParams, abstractInitParams,
                                                            indexComponents);
    } else if (hnswParams->type == VecSimType_INT8) {
        IndexComponents<int8_t, float> indexComponents = CreateIndexComponents<int8_t, float>(
            abstractInitParams.allocator, hnswParams->metric, hnswParams->dim, is_normalized);
        return NewIndex_ChooseMultiOrSingle<int8_t, float>(hnswParams, abstractInitParams,
                                                           indexComponents);
    } else if (hnswParams->type == VecSimType_UINT8) {
        IndexComponents<uint8_t, float> indexComponents = CreateIndexComponents<uint8_t, float>(
            abstractInitParams.allocator, hnswParams->metric, hnswParams->dim, is_normalized);
        return NewIndex_ChooseMultiOrSingle<uint8_t, float>(hnswParams, abstractInitParams,
                                                            indexComponents);
    }

    // If we got here something is wrong.
    return NULL;
}

VecSimIndex *NewIndex(const HNSWParams *params, bool is_normalized) {
    VecSimParams vecSimParams = {.algoParams = {.hnswParams = HNSWParams{*params}}};
    return NewIndex(&vecSimParams);
}

template <typename DataType, typename DistType = DataType>
inline size_t EstimateInitialSize_ChooseMultiOrSingle(bool is_multi) {
    // check if single or multi and return the size of the matching class struct.
    if (is_multi)
        return sizeof(HNSWIndex_Multi<DataType, DistType>);
    else
        return sizeof(HNSWIndex_Single<DataType, DistType>);
}

size_t EstimateInitialSize(const HNSWParams *params, bool is_normalized) {
    return EstimateInitialSize(params, is_normalized,
                               /* with_mean = */ params->quantParams != nullptr);
}

size_t EstimateInitialSize(const HNSWParams *params, bool is_normalized, bool with_mean) {
    size_t allocations_overhead = VecSimAllocator::getAllocationOverheadSize();

    size_t est = sizeof(VecSimAllocator) + allocations_overhead;

    if (params->quantType != VecSimQuant_NONE) {
        // Keep construction and initial-size validation in sync.
        if (params->quantType != VecSimQuant_SQ8 ||
            !SQ8ParamsSupported(params->type, ResolveSQ8Metric(params->metric, is_normalized))) {
            throw std::invalid_argument("Unsupported quantization params for HNSW index");
        }
        // Template arguments do not affect these component sizes.
        if (with_mean) {
            est += allocations_overhead +
                   sizeof(DistanceCalculatorWithNorm<float, float, VecSimMetric_L2>);
            est += allocations_overhead + sizeof(MultiPreprocessorsContainer<float, 1>);
            est += allocations_overhead + sizeof(QuantPreprocessor<float, VecSimMetric_L2, true>);
            est += allocations_overhead + params->dim * sizeof(float);
        } else {
            est += allocations_overhead + sizeof(DistanceCalculatorCommon<float>);
            est += allocations_overhead + sizeof(MultiPreprocessorsContainer<float, 1>);
            est += allocations_overhead + sizeof(QuantPreprocessor<float, VecSimMetric_L2>);
        }
        est += EstimateInitialSize_ChooseMultiOrSingle<float>(params->multi);
    } else if (params->type == VecSimType_FLOAT32) {
        est += EstimateComponentsMemory<float, float>(params->metric, is_normalized);
        est += EstimateInitialSize_ChooseMultiOrSingle<float>(params->multi);
    } else if (params->type == VecSimType_FLOAT64) {
        est += EstimateComponentsMemory<double, double>(params->metric, is_normalized);
        est += EstimateInitialSize_ChooseMultiOrSingle<double>(params->multi);
    } else if (params->type == VecSimType_BFLOAT16) {
        est += EstimateComponentsMemory<bfloat16, float>(params->metric, is_normalized);
        est += EstimateInitialSize_ChooseMultiOrSingle<bfloat16, float>(params->multi);
    } else if (params->type == VecSimType_FLOAT16) {
        est += EstimateComponentsMemory<float16, float>(params->metric, is_normalized);
        est += EstimateInitialSize_ChooseMultiOrSingle<float16, float>(params->multi);
    } else if (params->type == VecSimType_INT8) {
        est += EstimateComponentsMemory<int8_t, float>(params->metric, is_normalized);
        est += EstimateInitialSize_ChooseMultiOrSingle<int8_t, float>(params->multi);
    } else if (params->type == VecSimType_UINT8) {
        est += EstimateComponentsMemory<uint8_t, float>(params->metric, is_normalized);
        est += EstimateInitialSize_ChooseMultiOrSingle<uint8_t, float>(params->multi);
    } else {
        throw std::invalid_argument("Invalid params->type");
    }
    est += sizeof(DataBlocksContainer) + allocations_overhead;

    return est;
}

size_t EstimateElementSize(const HNSWParams *params) {
    return EstimateElementSize(params, /* with_mean = */ params->quantParams != nullptr);
}

size_t EstimateElementSize(const HNSWParams *params, bool with_mean) {
    size_t M = (params->M) ? params->M : HNSW_DEFAULT_M;
    size_t elementGraphDataSize = sizeof(ElementGraphData) + sizeof(idType) * M * 2;

    size_t stored_data_size;
    // Preserve the element estimator's existing contract: return a size and validate in NewIndex.
    if (params->quantType == VecSimQuant_SQ8) {
        if (params->metric == VecSimMetric_L2) {
            stored_data_size = GetSQ8StoredDataSize<VecSimMetric_L2>(params->dim, with_mean);
        } else {
            stored_data_size = GetSQ8StoredDataSize<VecSimMetric_IP>(params->dim, with_mean);
        }
    } else {
        stored_data_size =
            VecSimParams_GetStoredDataSize(params->type, params->dim, params->metric);
    }

    size_t size_total_data_per_element = elementGraphDataSize + stored_data_size;

    // when reserving space for new labels in the lookup hash table, each entry is a pointer to a
    // label node (bucket).
    size_t size_label_lookup_entry = sizeof(void *);

    // 1 entry in visited nodes + 1 entry in element metadata map + 1 node lock + (approximately)
    // 1 bucket in labels lookup hash map.
    size_t size_meta_data = sizeof(tag_t) + sizeof(ElementMetaData) +
                            sizeof(vecsim_stl::one_byte_mutex) + size_label_lookup_entry;

    /* Disclaimer: we are neglecting two additional factors that consume memory:
     * 1. The overall bucket size in labels_lookup hash table is usually higher than the number of
     * requested buckets (which is the index capacity), and it is auto selected according to the
     * hashing policy and the max load factor.
     * 2. The incoming edges that aren't bidirectional are stored in a dynamic array
     * (vecsim_stl::vector) Those edges' memory *is omitted completely* from this estimation.
     */
    return size_meta_data + size_total_data_per_element;
}

#ifdef BUILD_TESTS

template <typename DataType, typename DistType = DataType>
inline VecSimIndex *NewIndex_ChooseMultiOrSingle(std::ifstream &input, const HNSWParams *params,
                                                 const AbstractIndexInitParams &abstractInitParams,
                                                 IndexComponents<DataType, DistType> &components,
                                                 HNSWSerializer::EncodingVersion version,
                                                 const float *mean_ptr = nullptr) {
    HNSWIndex<DataType, DistType> *index = nullptr;
    // check if single and call the ctor that loads index information from file.
    if (params->multi)
        index = new (abstractInitParams.allocator) HNSWIndex_Multi<DataType, DistType>(
            input, params, abstractInitParams, components, version);
    else
        index = new (abstractInitParams.allocator) HNSWIndex_Single<DataType, DistType>(
            input, params, abstractInitParams, components, version);

    index->restoreGraph(input, version);

    // Store quantization metadata for re-serialization.
    index->quantType = params->quantType;
    if (mean_ptr != nullptr) {
        index->serializedMeanVector.assign(mean_ptr, mean_ptr + abstractInitParams.dim);
    }

    return index;
}

// Initialize @params from file for V3+. For V5+ also reads quantType and mean vector.
// Returns false if the file holds a quantizer value this version does not know.
static bool InitializeParams(std::ifstream &source_params, HNSWParams &params,
                             HNSWSerializer::EncodingVersion version,
                             std::vector<float> &meanVector) {
    Serializer::readBinaryPOD(source_params, params.dim);
    Serializer::readBinaryPOD(source_params, params.type);
    Serializer::readBinaryPOD(source_params, params.metric);
    Serializer::readBinaryPOD(source_params, params.blockSize);
    Serializer::readBinaryPOD(source_params, params.multi);
    Serializer::readBinaryPOD(source_params, params.initialCapacity);

    // V5: read quantization fields
    if (version >= HNSWSerializer::EncodingVersion::V5) {
        // Validate the raw value before converting it: an out-of-range value is not a valid
        // VecSimQuantType, so it must never be loaded into the enum.
        std::underlying_type_t<VecSimQuantType> raw_quant_type;
        Serializer::readBinaryPOD(source_params, raw_quant_type);
        if (raw_quant_type != VecSimQuant_NONE && raw_quant_type != VecSimQuant_SQ8) {
            return false;
        }
        params.quantType = static_cast<VecSimQuantType>(raw_quant_type);
        if (params.quantType == VecSimQuant_SQ8) {
            bool hasMean = false;
            Serializer::readBinaryPOD(source_params, hasMean);
            if (hasMean) {
                meanVector.resize(params.dim);
                source_params.read(reinterpret_cast<char *>(meanVector.data()),
                                   params.dim * sizeof(float));
            }
        }
    }
    return true;
}

VecSimIndex *NewIndex(const std::string &location, bool is_normalized) {

    std::ifstream input(location, std::ios::binary);
    if (!input.is_open()) {
        throw std::runtime_error("Cannot open file");
    }

    HNSWSerializer::EncodingVersion version = HNSWSerializer::ReadVersion(input);

    VecSimAlgo algo = VecSimAlgo_BF;
    Serializer::readBinaryPOD(input, algo);
    if (algo != VecSimAlgo_HNSWLIB) {
        input.close();
        auto bad_name = VecSimAlgo_ToString(algo);
        if (bad_name == nullptr) {
            bad_name = "Unknown (corrupted file?)";
        }
        throw std::runtime_error(
            std::string("Cannot load index: Expected HNSW file but got algorithm type: ") +
            bad_name);
    }

    HNSWParams params = {};
    std::vector<float> meanVector;
    if (!InitializeParams(input, params, version, meanVector)) {
        return NULL;
    }

    AbstractIndexInitParams abstractInitParams =
        VecSimFactory::NewAbstractInitParams(&params, nullptr, is_normalized);

    if (params.quantType != VecSimQuant_NONE) {
        const float *mean_ptr = meanVector.empty() ? nullptr : meanVector.data();
        const VecSimMetric metric = ResolveSQ8Metric(params.metric, is_normalized);

        if (!SQ8ParamsSupported(params.type, metric)) {
            return NULL;
        }

        // Override blob sizes for SQ8 storage layout.
        size_t dim = params.dim;
        abstractInitParams.isQuantized = true;
        if (metric == VecSimMetric_L2) {
            abstractInitParams.storedDataSize =
                GetSQ8StoredDataSize<VecSimMetric_L2>(dim, mean_ptr != nullptr);
        } else {
            abstractInitParams.storedDataSize =
                GetSQ8StoredDataSize<VecSimMetric_IP>(dim, mean_ptr != nullptr);
        }

        if (params.type == VecSimType_FLOAT32) {
            if (metric == VecSimMetric_L2) {
                auto components = CreateSQ8IndexComponents<float, VecSimMetric_L2>(
                    abstractInitParams.allocator, dim, mean_ptr);
                return NewIndex_ChooseMultiOrSingle<float>(input, &params, abstractInitParams,
                                                           components, version, mean_ptr);
            } else {
                auto components = CreateSQ8IndexComponents<float, VecSimMetric_IP>(
                    abstractInitParams.allocator, dim, mean_ptr);
                return NewIndex_ChooseMultiOrSingle<float>(input, &params, abstractInitParams,
                                                           components, version, mean_ptr);
            }
        } else if (params.type == VecSimType_FLOAT16) {
            if (metric == VecSimMetric_L2) {
                auto components = CreateSQ8IndexComponents<float16, VecSimMetric_L2>(
                    abstractInitParams.allocator, dim, mean_ptr);
                return NewIndex_ChooseMultiOrSingle<float16, float>(
                    input, &params, abstractInitParams, components, version, mean_ptr);
            } else {
                auto components = CreateSQ8IndexComponents<float16, VecSimMetric_IP>(
                    abstractInitParams.allocator, dim, mean_ptr);
                return NewIndex_ChooseMultiOrSingle<float16, float>(
                    input, &params, abstractInitParams, components, version, mean_ptr);
            }
        }
    }

    if (params.type == VecSimType_FLOAT32) {
        IndexComponents<float, float> indexComponents = CreateIndexComponents<float, float>(
            abstractInitParams.allocator, params.metric, abstractInitParams.dim, is_normalized);
        return NewIndex_ChooseMultiOrSingle<float>(input, &params, abstractInitParams,
                                                   indexComponents, version);
    } else if (params.type == VecSimType_FLOAT64) {
        IndexComponents<double, double> indexComponents = CreateIndexComponents<double, double>(
            abstractInitParams.allocator, params.metric, abstractInitParams.dim, is_normalized);
        return NewIndex_ChooseMultiOrSingle<double>(input, &params, abstractInitParams,
                                                    indexComponents, version);
    } else if (params.type == VecSimType_BFLOAT16) {
        IndexComponents<bfloat16, float> indexComponents = CreateIndexComponents<bfloat16, float>(
            abstractInitParams.allocator, params.metric, abstractInitParams.dim, is_normalized);
        return NewIndex_ChooseMultiOrSingle<bfloat16, float>(input, &params, abstractInitParams,
                                                             indexComponents, version);
    } else if (params.type == VecSimType_FLOAT16) {
        IndexComponents<float16, float> indexComponents = CreateIndexComponents<float16, float>(
            abstractInitParams.allocator, params.metric, abstractInitParams.dim, is_normalized);
        return NewIndex_ChooseMultiOrSingle<float16, float>(input, &params, abstractInitParams,
                                                            indexComponents, version);
    } else if (params.type == VecSimType_INT8) {
        IndexComponents<int8_t, float> indexComponents = CreateIndexComponents<int8_t, float>(
            abstractInitParams.allocator, params.metric, abstractInitParams.dim, is_normalized);
        return NewIndex_ChooseMultiOrSingle<int8_t, float>(input, &params, abstractInitParams,
                                                           indexComponents, version);
    } else if (params.type == VecSimType_UINT8) {
        IndexComponents<uint8_t, float> indexComponents = CreateIndexComponents<uint8_t, float>(
            abstractInitParams.allocator, params.metric, abstractInitParams.dim, is_normalized);
        return NewIndex_ChooseMultiOrSingle<uint8_t, float>(input, &params, abstractInitParams,
                                                            indexComponents, version);
    } else {
        auto bad_name = VecSimType_ToString(params.type);
        if (bad_name == nullptr) {
            bad_name = "Unknown (corrupted file?)";
        }
        throw std::runtime_error(std::string("Cannot load index: bad index data type: ") +
                                 bad_name);
    }
}
#endif

}; // namespace HNSWFactory

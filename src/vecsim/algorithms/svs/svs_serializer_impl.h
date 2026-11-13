/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include "svs_serializer.h"
#include "svs/index/vamana/dynamic_index.h"
#include "svs/index/vamana/multi.h"

// Saves all relevant fields of SVSIndex to the output stream
// This function saves all template parameters and instance fields needed to reconstruct
// an SVSIndex<MetricType, DataType, isMulti, QuantBits, ResidualBits, IsLeanVec>
template <typename MetricType, typename DataType, bool isMulti, size_t QuantBits,
          size_t ResidualBits, bool IsLeanVec>
void SVSIndex<MetricType, DataType, isMulti, QuantBits, ResidualBits, IsLeanVec>::saveIndexFields(
    std::ofstream &output) const {
    // Save base class fields from VecSimIndexAbstract
    // Note: this->vecType corresponds to DataType template parameter
    // Note: this->metric corresponds to MetricType template parameter
    writeBinaryPOD(output, this->dim);
    writeBinaryPOD(output, this->vecType); // DataType template parameter (as VecSimType enum)
    writeBinaryPOD(output, this->getStoredDataSize());
    writeBinaryPOD(output, this->metric); // MetricType template parameter (as VecSimMetric enum)
    writeBinaryPOD(output, this->blockSize);
    writeBinaryPOD(output, this->isMulti);

    // Save SVS-specific configuration fields
    writeBinaryPOD(output, this->forcePreprocessing);

    // Save build parameters
    writeBinaryPOD(output, this->buildParams.alpha);
    writeBinaryPOD(output, this->buildParams.graph_max_degree);
    writeBinaryPOD(output, this->buildParams.window_size);
    writeBinaryPOD(output, this->buildParams.max_candidate_pool_size);
    writeBinaryPOD(output, this->buildParams.prune_to);
    writeBinaryPOD(output, this->buildParams.use_full_search_history);

    // Save search parameters
    writeBinaryPOD(output, this->search_window_size);
    writeBinaryPOD(output, this->epsilon);

    // Save template parameters as metadata for validation during loading
    writeBinaryPOD(output, getCompressionMode());

    // QuantBits, ResidualBits, and IsLeanVec information

    // Save additional template parameter constants for complete reconstruction
    writeBinaryPOD(output, static_cast<size_t>(QuantBits));    // Template parameter QuantBits
    writeBinaryPOD(output, static_cast<size_t>(ResidualBits)); // Template parameter ResidualBits
    writeBinaryPOD(output, static_cast<bool>(IsLeanVec));      // Template parameter IsLeanVec
    writeBinaryPOD(output, static_cast<bool>(isMulti));        // Template parameter isMulti

    // Save additional metadata for validation during loading
    writeBinaryPOD(output, this->lastMode); // Last search mode
}

// Saves metadata (e.g., encoding version) to satisfy Serializer interface.
// Full index is saved separately in saveIndex() using file paths.
template <typename MetricType, typename DataType, bool isMulti, size_t QuantBits,
          size_t ResidualBits, bool IsLeanVec>
void SVSIndex<MetricType, DataType, isMulti, QuantBits, ResidualBits, IsLeanVec>::saveIndexIMP(
    std::ofstream &output) {

    // Save all index fields using the dedicated function
    saveIndexFields(output);
}

// Saves metadata (e.g., encoding version) to satisfy Serializer interface.
// Full index is saved separately in saveIndex() using file paths.
template <typename MetricType, typename DataType, bool isMulti, size_t QuantBits,
          size_t ResidualBits, bool IsLeanVec>
void SVSIndex<MetricType, DataType, isMulti, QuantBits, ResidualBits, IsLeanVec>::impl_save(
    const std::string &location) {
    auto impl = getImpl();
    if (!impl) {
        throw std::runtime_error("SVSIndex::impl_save called on an empty index");
    }
    impl->save(location + "/config", location + "/graph", location + "/data");
}

// This function will load the serialized svs index from the given folder path
// This function should be called after the index is created with the same parameters as the
// original index. The index fields and template parameters will be validated before loading. After
// sucssessful loading, the graph can be validated with checkIntegrity.
template <typename MetricType, typename DataType, bool isMulti, size_t QuantBits,
          size_t ResidualBits, bool IsLeanVec>
void SVSIndex<MetricType, DataType, isMulti, QuantBits, ResidualBits, IsLeanVec>::loadIndex(
    const std::string &folder_path) {
    svs::threads::ThreadPoolHandle threadpool_handle{VecSimSVSThreadPool{threadpool_}};

    // Verify metadata compatibility, will throw runtime exception if not compatible
    compareMetadataFile(folder_path + "/metadata");

    std::unique_ptr<impl_type> loaded_impl;
    if constexpr (isMulti) {
        auto loaded = svs::concurrent::auto_multi_dynamic_assemble(
            folder_path + "/config",
            SVS_LAZY(graph_builder_t::load(folder_path + "/graph", this->blockSize,
                                           this->buildParams, this->getAllocator())),
            SVS_LAZY(storage_traits_t::load(folder_path + "/data", this->blockSize, this->dim,
                                            this->getAllocator())),
            distance_f(), std::move(threadpool_handle),
            svs::concurrent::MultiMutableVamanaLoad::FROM_MULTI, logger_);
        loaded_impl = std::make_unique<impl_type>(std::move(loaded));
    } else {
        auto loaded = svs::concurrent::auto_dynamic_assemble(
            folder_path + "/config",
            SVS_LAZY(graph_builder_t::load(folder_path + "/graph", this->blockSize,
                                           this->buildParams, this->getAllocator())),
            SVS_LAZY(storage_traits_t::load(folder_path + "/data", this->blockSize, this->dim,
                                            this->getAllocator())),
            distance_f(), std::move(threadpool_handle), false, logger_);
        loaded_impl = std::make_unique<impl_type>(std::move(loaded));
    }

    delete impl_.exchange(loaded_impl.release(), std::memory_order_release);
}

template <typename MetricType, typename DataType, bool isMulti, size_t QuantBits,
          size_t ResidualBits, bool IsLeanVec>
bool SVSIndex<MetricType, DataType, isMulti, QuantBits, ResidualBits,
              IsLeanVec>::compareMetadataFile(const std::string &metadataFilePath) const {
    std::ifstream input(metadataFilePath, std::ios::binary);
    if (!input.is_open()) {
        throw std::runtime_error("Failed to open metadata file: " + metadataFilePath);
    }

    // To check version, use ReadVersion
    SVSSerializer::ReadVersion(input);

    compareField(input, this->dim, "dim");
    compareField(input, this->vecType, "vecType");
    compareField(input, this->getStoredDataSize(), "dataSize");
    compareField(input, this->metric, "metric");
    compareField(input, this->blockSize, "blockSize");
    compareField(input, this->isMulti, "isMulti");

    compareField(input, this->forcePreprocessing, "forcePreprocessing");

    compareField(input, this->buildParams.alpha, "buildParams.alpha");
    compareField(input, this->buildParams.graph_max_degree, "buildParams.graph_max_degree");
    compareField(input, this->buildParams.window_size, "buildParams.window_size");
    compareField(input, this->buildParams.max_candidate_pool_size,
                 "buildParams.max_candidate_pool_size");
    compareField(input, this->buildParams.prune_to, "buildParams.prune_to");
    compareField(input, this->buildParams.use_full_search_history,
                 "buildParams.use_full_search_history");

    compareField(input, this->search_window_size, "search_window_size");
    compareField(input, this->epsilon, "epsilon");

    auto compressionMode = getCompressionMode();
    compareField(input, compressionMode, "compression_mode");

    compareField(input, static_cast<size_t>(QuantBits), "QuantBits");
    compareField(input, static_cast<size_t>(ResidualBits), "ResidualBits");
    compareField(input, static_cast<bool>(IsLeanVec), "IsLeanVec");
    compareField(input, static_cast<bool>(isMulti), "isMulti (template param)");

    return true;
}

template <typename MetricType, typename DataType, bool isMulti, size_t QuantBits,
          size_t ResidualBits, bool IsLeanVec>
bool SVSIndex<MetricType, DataType, isMulti, QuantBits, ResidualBits, IsLeanVec>::checkIntegrity()
    const {
    auto impl = getImpl();
    if (!impl) {
        throw std::runtime_error(
            "SVSIndex integrity check failed: index implementation (impl_) is null.");
    }

    try {
        // SVS internal index integrity validation
        if constexpr (isMulti) {
            impl->get_parent_index().debug_check_invariants(true);
        } else {
            impl->debug_check_invariants(true);
        }
    }
    // debug_check_invariants throws svs::lib::ANNException : public std::runtime_error in case of
    // fail.
    catch (...) {
        throw;
    }

    try {
        size_t index_size = impl->size();
        size_t storage_size = impl->view_data().size();
        size_t capacity = storage_traits_t::storage_capacity(impl->view_data());
        size_t label_count = labelCountOf(*impl);

        // Storage size must match index size
        if (storage_size != index_size) {
            throw std::runtime_error(
                "SVSIndex integrity check failed: storage_size != index_size.");
        }

        // Capacity must be at least index size
        if (capacity < index_size) {
            throw std::runtime_error("SVSIndex integrity check failed: capacity < index_size.");
        }

        // Binary label validation: verify label iteration and count consistency
        size_t labels_counted = 0;
        bool label_validation_passed = true;

        try {
            impl->on_ids([&](size_t label) { labels_counted++; });

            // Validate label count consistency
            label_validation_passed = (labels_counted == label_count);

            // For multi-index, also ensure label count doesn't exceed index size
            if constexpr (isMulti) {
                label_validation_passed = label_validation_passed && (label_count <= index_size);
            }
        } catch (...) {
            label_validation_passed = false;
        }

        if (!label_validation_passed) {
            throw std::runtime_error("SVSIndex integrity check failed: label validation failed.");
        }

        return true;

    } catch (const std::exception &e) {
        throw std::runtime_error(std::string("SVSIndex integrity check failed with exception: ") +
                                 e.what());
    } catch (...) {
        throw std::runtime_error("SVSIndex integrity check failed with unknown exception.");
    }
}

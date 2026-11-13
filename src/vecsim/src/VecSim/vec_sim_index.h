/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once
#include "vec_sim_common.h"
#include "vec_sim_interface.h"
#include "query_results.h"
#include <stddef.h>
#include "VecSim/memory/vecsim_base.h"
#include "VecSim/utils/vec_utils.h"
#include "VecSim/spaces/spaces.h"
#include "VecSim/spaces/computer/calculator.h"
#include "VecSim/spaces/computer/preprocessor_container.h"
#include "info_iterator_struct.h"
#include "containers/data_blocks_container.h"
#include "containers/raw_data_container_interface.h"

#include <cassert>
#include <functional>

/**
 * @brief Struct for initializing an abstract index class.
 *
 * @param allocator The allocator to use for the index.
 * @param dim The dimension of the vectors in the index.
 * @param vecType The type of the vectors in the index.
 * @param storedDataSize The size of stored vectors (possibly after pre-processing) in bytes.
 * @param metric The metric to use in the index.
 * @param blockSize The block size to use in the index.
 * @param multi Determines if the index should multi-index or not.
 * @param logCtx The context to use for logging.
 * @param inputBlobSize The size of input vectors/queries blob in bytes. May differ from dim *
 * sizeof(vecType) when vectors have been externally preprocessed (e.g., cosine normalization adds
 * extra bytes). For example, in tiered indexes, the backend receives preprocessed blobs, not raw
 * input vectors.
 */
struct AbstractIndexInitParams {
    std::shared_ptr<VecSimAllocator> allocator;
    size_t dim;
    VecSimType vecType;
    size_t storedDataSize;
    VecSimMetric metric;
    size_t blockSize;
    bool multi;
    bool isDisk;      // Whether the index stores vectors on disk
    bool isQuantized; // Whether the index uses quantized storage.
    void *logCtx;
    size_t inputBlobSize;
};

/**
 * @brief Struct for initializing the components of the abstract index.
 * The index takes ownership of the components allocations' and is responsible for freeing
 * them when the index is destroyed.
 *
 * @param indexCalculator The distance calculator for the index.
 * @param preprocessors The preprocessing pipeline for ingesting user data before storage and
 * querying.
 */
template <typename DataType, typename DistType>
struct IndexComponents {
    IndexCalculatorInterface<DistType> *indexCalculator;
    PreprocessorsContainerAbstract *preprocessors;
};

/**
 * @brief Abstract C++ class for vector index, delete and lookup
 *
 */
template <typename DataType, typename DistType>
struct VecSimIndexAbstract : public VecSimIndexInterface {
protected:
    size_t dim;          // Vector's dimension.
    VecSimType vecType;  // Datatype to index.
    VecSimMetric metric; // Distance metric to use in the index.
    size_t blockSize;    // Index's vector block size (determines by how many vectors to resize when
                         // resizing)
    mutable VecSearchMode lastMode; // The last search mode in RediSearch (used for debug/testing).
    bool isMulti;                   // Determines if the index should multi-index or not.
    bool isDisk;                    // Whether the index stores vectors on disk.
    bool isQuantized;               // Whether stored vectors are quantized.
    void *logCallbackCtx;           // Context for the log callback.
    RawDataContainer *vectors;      // The raw vectors data container.
private:
    IndexCalculatorInterface<DistType> *indexCalculator; // Distance calculator.
    DistanceDispatch<DistType> storedDistanceDispatch;
    DistanceDispatch<DistType> queryDistanceDispatch;
    PreprocessorsContainerAbstract *preprocessors; // Storage and query preprocessors.

    size_t inputBlobSize; // The size of input vectors/queries blob in bytes. May differ from dim *
                          // sizeof(vecType) when vectors have been externally preprocessed (e.g.,
                          // cosine normalization adds extra bytes). For example, in tiered indexes,
                          // the backend receives preprocessed blobs, not raw input vectors.
    size_t storedDataSize; // Vector element data size in bytes to be stored
                           // (possibly after pre-processing and may differ from inputBlobSize if
                           // NOT externally preprocessed).
protected:
    /**
     * @brief Get the common info object
     *
     * @return CommonInfo
     */
    CommonInfo getCommonInfo() const {
        CommonInfo info;
        info.basicInfo = this->getBasicInfo();
        info.lastMode = this->lastMode;
        info.memory = this->getAllocationSize();
        info.indexSize = this->indexSize();
        info.indexLabelCount = this->indexLabelCount();
        return info;
    }

    IndexCalculatorInterface<DistType> *getIndexCalculator() const { return indexCalculator; }
    PreprocessorsContainerAbstract *getPreprocessorsContainer() const { return preprocessors; }

public:
    /**
     * @brief Construct a new Vec Sim Index object
     *
     */
    VecSimIndexAbstract(const AbstractIndexInitParams &params,
                        const IndexComponents<DataType, DistType> &components)
        : VecSimIndexInterface(params.allocator), dim(params.dim), vecType(params.vecType),
          metric(params.metric),
          blockSize(params.blockSize ? params.blockSize : DEFAULT_BLOCK_SIZE), lastMode(EMPTY_MODE),
          isMulti(params.multi), isDisk(params.isDisk), isQuantized(params.isQuantized),
          logCallbackCtx(params.logCtx), indexCalculator(components.indexCalculator),
          storedDistanceDispatch(
              components.indexCalculator
                  ? components.indexCalculator->getDistanceDispatch(DistanceMode::StoredToStored)
                  : DistanceDispatch<DistType>{}),
          queryDistanceDispatch(
              components.indexCalculator
                  ? components.indexCalculator->getDistanceDispatch(DistanceMode::StoredToQuery)
                  : DistanceDispatch<DistType>{}),
          preprocessors(components.preprocessors), inputBlobSize(params.inputBlobSize),
          storedDataSize(params.storedDataSize) {
        assert(VecSimType_sizeof(vecType));
        assert(storedDataSize);
        assert(inputBlobSize);
        assert(indexCalculator == nullptr || storedDistanceDispatch.isValid());
        assert(indexCalculator == nullptr || queryDistanceDispatch.isValid());
        // DataBlocksContainer holds the persistent storage vectors, so it must honor the storage
        // alignment hint (not the query alignment). Note: this only aligns the base address of
        // each block; vectors inside a block are packed back-to-back at stride `storedDataSize`,
        // so only the first vector in every block is guaranteed to be aligned. Aligning every
        // vector would require padding `storedDataSize` up to a multiple of the alignment, which
        // is a separate change.
        this->vectors = new (this->allocator) DataBlocksContainer(
            this->blockSize, this->storedDataSize, this->allocator, this->getStorageAlignment());
    }

    /**
     * @brief Destroy the Vec Sim Index object
     *
     */
    virtual ~VecSimIndexAbstract() noexcept {
        delete this->vectors;
        delete indexCalculator;
        delete preprocessors;
    }

    /**
     * @brief Calculate the distance between two vectors based on index parameters.
     *
     * Uses the cached dispatch to avoid the indexCalculator vtable on the hot path.
     *
     * @note Subclasses that construct this index with a null @c indexCalculator (e.g. SVS, which
     * uses its own internal distance kernels) must not call this method.
     *
     * @return the distance between the vectors.
     */
    DistType calcDistance(const void *vector_data1, const void *vector_data2) const {
        return storedDistanceDispatch(vector_data1, vector_data2, this->dim);
    }

    /**
     * @brief Calculate the distance between a stored candidate vector and a query vector.
     * Allows asymmetric distance computation (e.g., for quantized stored vectors).
     *
     * @note Subclasses that construct this index with a null @c indexCalculator (e.g. SVS, which
     * uses its own internal distance kernels) must not call this method.
     *
     * @return the distance between the candidate and the query.
     */
    DistType calcDistanceForQuery(const void *candidate_vector, const void *query_vector) const {
        return queryDistanceDispatch(candidate_vector, query_vector, this->dim);
    }

    /**
     * @brief Preprocess a blob for both storage and query.
     *
     * @param original_blob will be copied.
     * @return two unique_ptr of the processed blobs.
     */
    ProcessedBlobs preprocess(const void *original_blob) const;

    /**
     * @brief Preprocess a blob for query.
     *
     * @param queryBlob will be copied if preprocessing is required, or if force_copy is set to
     * true.
     * @return unique_ptr of the processed blob.
     */
    MemoryUtils::unique_blob preprocessQuery(const void *queryBlob, bool force_copy = false) const;

    /**
     * @brief Preprocess a blob for storage.
     *
     * @param original_blob will be copied.
     * @return unique_ptr of the processed blob.
     */
    MemoryUtils::unique_blob preprocessForStorage(const void *original_blob) const;

    /**
     * @brief Preprocess a blob for storage in place.
     *
     * @param blob will be directly modified, not copied.
     */
    void preprocessStorageInPlace(void *blob) const;

    inline size_t getDim() const { return dim; }
    inline void setLastSearchMode(VecSearchMode mode) override { this->lastMode = mode; }
    inline bool isMultiValue() const { return isMulti; }
    inline bool usesQuantizedStorage() const { return isQuantized; }
    inline VecSimType getType() const { return vecType; }
    inline VecSimMetric getMetric() const { return metric; }
    inline size_t getStoredDataSize() const { return storedDataSize; }
    inline size_t getInputBlobSize() const { return inputBlobSize; }
    inline size_t getBlockSize() const { return blockSize; }
    inline auto getQueryAlignment() const { return this->preprocessors->getQueryAlignment(); }
    inline auto getStorageAlignment() const { return this->preprocessors->getStorageAlignment(); }

    virtual inline VecSimIndexStatsInfo statisticInfo() const override {
        return VecSimIndexStatsInfo{
            .memory = this->getAllocationSize(),
            .numberOfMarkedDeleted = 0,
            .directHNSWInsertions = 0,
            .flatBufferSize = 0,
        };
    }

    virtual VecSimQueryReply *rangeQuery(const void *queryBlob, double radius,
                                         VecSimQueryParams *queryParams) const = 0;
    VecSimQueryReply *rangeQuery(const void *queryBlob, double radius,
                                 VecSimQueryParams *queryParams,
                                 VecSimQueryReply_Order order) const override {
        auto results = rangeQuery(queryBlob, radius, queryParams);
        sort_results(results, order);
        return results;
    }

    void log(const char *level, const char *fmt, ...) const {
        if (VecSimIndexInterface::logCallback) {
            // Format the message and call the callback
            va_list args;
            va_start(args, fmt);
            int len = vsnprintf(NULL, 0, fmt, args);
            va_end(args);
            char *buf = new char[len + 1];
            va_start(args, fmt);
            vsnprintf(buf, len + 1, fmt, args);
            va_end(args);
            logCallback(this->logCallbackCtx, level, buf);
            delete[] buf;
        }
    }

    // Adds all common info to the info iterator, besides the block size (currently 8 fields).
    void addCommonInfoToIterator(VecSimDebugInfoIterator *infoIterator,
                                 const CommonInfo &info) const {
        infoIterator->addInfoField(VecSim_InfoField{
            .fieldName = VecSimCommonStrings::TYPE_STRING,
            .fieldType = INFOFIELD_STRING,
            .fieldValue = {FieldValue{.stringValue = VecSimType_ToString(info.basicInfo.type)}}});
        infoIterator->addInfoField(
            VecSim_InfoField{.fieldName = VecSimCommonStrings::DIMENSION_STRING,
                             .fieldType = INFOFIELD_UINT64,
                             .fieldValue = {FieldValue{.uintegerValue = info.basicInfo.dim}}});
        infoIterator->addInfoField(
            VecSim_InfoField{.fieldName = VecSimCommonStrings::METRIC_STRING,
                             .fieldType = INFOFIELD_STRING,
                             .fieldValue = {FieldValue{
                                 .stringValue = VecSimMetric_ToString(info.basicInfo.metric)}}});
        infoIterator->addInfoField(
            VecSim_InfoField{.fieldName = VecSimCommonStrings::IS_MULTI_STRING,
                             .fieldType = INFOFIELD_UINT64,
                             .fieldValue = {FieldValue{.uintegerValue = info.basicInfo.isMulti}}});
        infoIterator->addInfoField(
            VecSim_InfoField{.fieldName = VecSimCommonStrings::IS_DISK_STRING,
                             .fieldType = INFOFIELD_UINT64,
                             .fieldValue = {FieldValue{.uintegerValue = info.basicInfo.isDisk}}});
        infoIterator->addInfoField(
            VecSim_InfoField{.fieldName = VecSimCommonStrings::INDEX_SIZE_STRING,
                             .fieldType = INFOFIELD_UINT64,
                             .fieldValue = {FieldValue{.uintegerValue = info.indexSize}}});
        infoIterator->addInfoField(
            VecSim_InfoField{.fieldName = VecSimCommonStrings::INDEX_LABEL_COUNT_STRING,
                             .fieldType = INFOFIELD_UINT64,
                             .fieldValue = {FieldValue{.uintegerValue = info.indexLabelCount}}});
        infoIterator->addInfoField(
            VecSim_InfoField{.fieldName = VecSimCommonStrings::MEMORY_STRING,
                             .fieldType = INFOFIELD_UINT64,
                             .fieldValue = {FieldValue{.uintegerValue = info.memory}}});
        infoIterator->addInfoField(VecSim_InfoField{
            .fieldName = VecSimCommonStrings::SEARCH_MODE_STRING,
            .fieldType = INFOFIELD_STRING,
            .fieldValue = {FieldValue{.stringValue = VecSimSearchMode_ToString(info.lastMode)}}});
    }

    /**
     * @brief Get the basic static info object
     *
     * @return basicInfo
     */
    VecSimIndexBasicInfo getBasicInfo() const {
        VecSimIndexBasicInfo info{
            .metric = this->metric,
            .type = this->vecType,
            .isMulti = this->isMulti,
            .isDisk = this->isDisk,
            .blockSize = this->blockSize,
            .dim = this->dim,
        };
        return info;
    }
    /**
     * @brief Get the vector elements stored under a given label, in insertion order.
     *
     * A tiered index cannot honour the ordering across its tiers; see
     * `VecSimTieredIndex::getDataByLabel`.
     *
     * Appends nothing when the label is absent, so the output size doubles as the answer to
     * whether the index holds it -- and likewise for an index type that cannot read its stored
     * vectors back at all, which is how `SVSIndex` answers.
     *
     * Returns ONLY the vector elements, even where the stored vector carries extra
     * metadata -- with int8_t/uint8_t under cosine, the trailing norm is not included.
     * Use getStoredVectorDataByLabel() when the complete stored form is wanted.
     *
     * The elements are the *stored* ones, i.e. after any insert-time preprocessing —
     * normalized under cosine, for instance — and not the blob originally handed to
     * addVector.
     *
     * Nothing here dequantizes. A quantized index therefore appends nothing rather than
     * reinterpreting compression and metadata as values, which reads as "cannot tell". The test
     * is the index's own `isQuantized`, not a size comparison: an SQ8 element is smaller than the
     * raw elements only above a certain dimension, and larger below it.
     *
     * @param label The label to retrieve vector(s) elements for
     * @param vectors_output Empty vector to be filled with vector(s)
     */
    virtual void getDataByLabel(labelType label,
                                std::vector<std::vector<DataType>> &vectors_output) const = 0;

#ifdef BUILD_TESTS
    void replacePPContainer(PreprocessorsContainerAbstract *newPPContainer) {
        delete this->preprocessors;
        this->preprocessors = newPPContainer;
    }

    IndexComponents<DataType, DistType> get_components() const {
        return {.indexCalculator = this->indexCalculator, .preprocessors = this->preprocessors};
    }

    // Test-only: override the disk flag to exercise disk-specific param resolution
    // without requiring an actual disk-based index implementation.
    void setIsDiskForTesting(bool v) { this->isDisk = v; }

    /**
     * @brief Used for testing - get the complete raw data associated with a given label.
     * This function returns the ENTIRE vector(s) data as stored in the index, including any
     * additional metadata that might be stored alongside the vector elements.
     *
     * For example:
     * - For int8_t/uint8_t vectors with cosine similarity, this includes the norm stored at the end
     * - For other vector types or future implementations, this will include any additional data
     *   that might be stored with the vector
     *
     * Use this method when you need access to the complete vector data as it is stored internally.
     *
     * @param label The label to retrieve data for
     * @return A vector containing the complete vector data (elements + metadata) for the given
     * label
     */
    virtual std::vector<std::vector<char>> getStoredVectorDataByLabel(labelType label) const = 0;
#endif

    /**
     * Virtual functions that access the label lookup which is implemented in the derived classes
     * Return all the labels in the index - this should be used for computing the number of distinct
     * labels in a tiered index, and caller should hold the appropriate guards.
     */
    virtual vecsim_stl::set<labelType> getLabelsSet() const = 0;

protected:
    void runGC() override {}              // Do nothing, relevant for tiered index only.
    void acquireSharedLocks() override {} // Do nothing, relevant for tiered index only.
    void releaseSharedLocks() override {} // Do nothing, relevant for tiered index only.
};

template <typename DataType, typename DistType>
ProcessedBlobs VecSimIndexAbstract<DataType, DistType>::preprocess(const void *blob) const {
    return this->preprocessors->preprocess(blob, inputBlobSize);
}

template <typename DataType, typename DistType>
MemoryUtils::unique_blob
VecSimIndexAbstract<DataType, DistType>::preprocessQuery(const void *queryBlob,
                                                         bool force_copy) const {
    return this->preprocessors->preprocessQuery(queryBlob, inputBlobSize, force_copy);
}

template <typename DataType, typename DistType>
MemoryUtils::unique_blob
VecSimIndexAbstract<DataType, DistType>::preprocessForStorage(const void *original_blob) const {
    return this->preprocessors->preprocessForStorage(original_blob, inputBlobSize);
}

template <typename DataType, typename DistType>
void VecSimIndexAbstract<DataType, DistType>::preprocessStorageInPlace(void *blob) const {
    this->preprocessors->preprocessStorageInPlace(blob, inputBlobSize);
}

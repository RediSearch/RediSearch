/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include "hnsw.h"
#include "hnsw_multi_batch_iterator.h"
#include "VecSim/utils/updatable_heap.h"

template <typename DataType, typename DistType>
class HNSWIndex_Multi : public HNSWIndex<DataType, DistType> {
private:
    // Index global state - this should be guarded by the indexDataGuard lock in
    // multithreaded scenario.
    vecsim_stl::unordered_map<labelType, vecsim_stl::vector<idType>> labelLookup;

#ifdef BUILD_TESTS
#include "VecSim/algorithms/hnsw/hnsw_multi_tests_friends.h"
#endif

    inline void replaceIdOfLabel(labelType label, idType new_id, idType old_id) override;
    inline void setVectorId(labelType label, idType id) override {
        // Checking if an element with the given label already exists.
        // if not, add an empty vector under the new label.
        if (labelLookup.find(label) == labelLookup.end()) {
            labelLookup.emplace(label, vecsim_stl::vector<idType>{this->allocator});
        }
        labelLookup.at(label).push_back(id);
    }
    inline vecsim_stl::vector<idType> getElementIds(size_t label) override {
        auto it = labelLookup.find(label);
        if (it == labelLookup.end()) {
            return vecsim_stl::vector<idType>{this->allocator}; // return an empty collection
        }
        return it->second;
    }
    inline size_t getLabelSize(labelType label) const override {
        auto it = labelLookup.find(label);
        return it == labelLookup.end() ? 0 : it->second.size();
    }
    inline void resizeLabelLookup(size_t new_max_elements) override;

    // Return all the labels in the index - this should be used for computing the number of distinct
    // labels in a tiered index.
    inline vecsim_stl::set<labelType> getLabelsSet() const override {
        std::shared_lock<std::shared_mutex> index_data_lock(this->indexDataGuard);
        vecsim_stl::set<labelType> keys(this->allocator);
        for (auto &it : labelLookup) {
            keys.insert(it.first);
        }
        return keys;
    };

    inline double getDistanceFromInternal(labelType label, const void *vector_data) const;

public:
    HNSWIndex_Multi(const HNSWParams *params, const AbstractIndexInitParams &abstractInitParams,
                    const IndexComponents<DataType, DistType> &components, size_t random_seed = 100)
        : HNSWIndex<DataType, DistType>(params, abstractInitParams, components, random_seed),
          labelLookup(this->allocator) {}
#ifdef BUILD_TESTS
    // Ctor to be used before loading a serialized index. Can be used from v2 and up.
    HNSWIndex_Multi(std::ifstream &input, const HNSWParams *params,
                    const AbstractIndexInitParams &abstractInitParams,
                    const IndexComponents<DataType, DistType> &components,
                    HNSWSerializer::EncodingVersion version)
        : HNSWIndex<DataType, DistType>(input, params, abstractInitParams, components, version),
          labelLookup(this->maxElements, this->allocator) {}

#endif
    void getDataByLabel(labelType label,
                        std::vector<std::vector<DataType>> &vectors_output) const override {
        // See the single-value implementation: the guard is the accessor's to take, because
        // `indexDataGuard` -- not the main lock -- is what ingest and `markDelete` hold while
        // mutating `labelLookup` and the data blocks.
        // A quantized element is not the elements: SQ8 keeps one byte per dimension plus FP32
        // metadata, so copying `dim * sizeof(DataType)` out of it would reinterpret compression
        // and metadata as values. Nothing here dequantizes, so the honest answer is none -- per
        // the contract an empty output reads as "cannot tell".
        //
        // Asking the index rather than comparing sizes: the quantized element is only *smaller*
        // than the raw elements above a certain dimension. At dim 4 with FP32/L2 it is larger
        // (4 + 4*4 = 20 bytes against 16), so a size test concludes "not quantized" exactly where
        // it matters most.
        if (this->isQuantized) {
            return;
        }
        std::shared_lock<std::shared_mutex> index_data_lock(this->indexDataGuard);
        auto ids = labelLookup.find(label);
        if (ids == labelLookup.end()) {
            return;
        }

        for (idType id : ids->second) {
            auto vec = std::vector<DataType>(this->dim);
            // Only copy the vector data (dim * sizeof(DataType)), not any additional metadata like
            // the norm
            memcpy(vec.data(), this->getDataByInternalId(id), this->dim * sizeof(DataType));
            vectors_output.push_back(std::move(vec));
        }
    }

#ifdef BUILD_TESTS
    std::vector<std::vector<char>> getStoredVectorDataByLabel(labelType label) const override {
        std::vector<std::vector<char>> vectors_output;
        auto ids = labelLookup.find(label);

        for (idType id : ids->second) {
            const char *data = this->getDataByInternalId(id);

            // Create a vector with the full data (including any metadata like norms)
            std::vector<char> vec(this->getStoredDataSize());
            memcpy(vec.data(), data, this->getStoredDataSize());
            vectors_output.push_back(std::move(vec));
        }

        return vectors_output;
    }
#endif
    ~HNSWIndex_Multi() = default;

    inline candidatesLabelsMaxHeap<DistType> *getNewMaxPriorityQueue() const override {
        return new (this->allocator)
            vecsim_stl::updatable_max_heap<DistType, labelType>(this->allocator);
    }
    inline std::unique_ptr<vecsim_stl::abstract_results_container>
    getNewResultsContainer(size_t cap) const override {
        return std::unique_ptr<vecsim_stl::abstract_results_container>(
            new (this->allocator) vecsim_stl::unique_results_container(cap, this->allocator));
    }

    inline size_t indexLabelCount() const override;
    VecSimBatchIterator *newBatchIterator(const void *queryBlob,
                                          VecSimQueryParams *queryParams) const override;

    int deleteVector(labelType label) override;
    int addVector(const void *vector_data, labelType label) override;
    vecsim_stl::vector<idType> markDelete(labelType label) override;
    double getDistanceFrom_Unsafe(labelType label, const void *vector_data) const override {
        if (this->isQuantized) {
            auto processed_query = this->preprocessQuery(vector_data);
            return getDistanceFromInternal(label, processed_query.get());
        }
        return getDistanceFromInternal(label, vector_data);
    }
    int removeLabel(labelType label) override { return labelLookup.erase(label); }
    idType popLastIdFromLabel(labelType label) override {
        auto it = labelLookup.find(label);
        assert(it != labelLookup.end());

        auto &ids = it->second;
        assert(!ids.empty());

        const idType id = ids.back();
        ids.pop_back();
        if (ids.empty()) {
            labelLookup.erase(it);
        }
        return id;
    }
    bool isLabelExists(labelType label) override {
        return labelLookup.find(label) != labelLookup.end();
    }
};

/**
 * getters and setters of index data
 */

template <typename DataType, typename DistType>
size_t HNSWIndex_Multi<DataType, DistType>::indexLabelCount() const {
    return labelLookup.size();
}

/**
 * helper functions
 */

template <typename DataType, typename DistType>
double HNSWIndex_Multi<DataType, DistType>::getDistanceFromInternal(labelType label,
                                                                    const void *vector_data) const {
    DistType dist = INVALID_SCORE;

    // Check if the label exists in the index, return invalid score if not.
    auto it = this->labelLookup.find(label);
    if (it == this->labelLookup.end()) {
        return dist;
    }

    // Get the vector of ids associated with the label.
    // Get a copy if `Safe` is true, otherwise get a reference.
    auto &IDs = it->second;

    // Iterate over the ids and find the minimum distance.
    for (auto id : IDs) {
        DistType d = this->calcDistanceForQuery(this->getDataByInternalId(id), vector_data);
        dist = std::fmin(dist, d);
    }

    return dist;
}

template <typename DataType, typename DistType>
void HNSWIndex_Multi<DataType, DistType>::replaceIdOfLabel(labelType label, idType new_id,
                                                           idType old_id) {
    assert(labelLookup.find(label) != labelLookup.end());
    // *Non-trivial code here* - in every iteration we replace the internal id of the previous last
    // id that has been swapped with the deleted id. Note that if the old and the new replaced ids
    // both belong to the same label, then we are going to delete the new id later on as well, since
    // we are currently iterating on this exact array of ids in 'deleteVector'. Hence, the relevant
    // part of the vector that should be updated is the "tail" that comes after the position of
    // old_id, while the "head" may contain old occurrences of old_id that are irrelevant for the
    // future deletions. Therefore, we iterate from end to beginning. For example, assuming we are
    // deleting a label that contains the only 3 ids that exist in the index. Hence, we would
    // expect the following scenario w.r.t. the ids array:
    // [|1, 0, 2] -> [1, |0, 1] -> [1, 0, |0] (where | marks the current position)
    auto &ids = labelLookup.at(label);
    for (int i = ids.size() - 1; i >= 0; i--) {
        if (ids[i] == old_id) {
            ids[i] = new_id;
            return;
        }
    }
    assert(!"should have found the old id");
}

template <typename DataType, typename DistType>
void HNSWIndex_Multi<DataType, DistType>::resizeLabelLookup(size_t new_max_elements) {
    labelLookup.reserve(new_max_elements);
}

/**
 * Index API functions
 */

template <typename DataType, typename DistType>
int HNSWIndex_Multi<DataType, DistType>::deleteVector(const labelType label) {
    int ret = 0;
    // check that the label actually exists in the graph, and update the number of elements.
    auto ids_it = labelLookup.find(label);
    if (ids_it == labelLookup.end()) {
        return ret;
    }
    for (auto &ids = ids_it->second; idType id : ids) {
        this->removeVectorInPlace(id);
        ret++;
    }
    labelLookup.erase(label);
    return ret;
}

template <typename DataType, typename DistType>
int HNSWIndex_Multi<DataType, DistType>::addVector(const void *vector_data, const labelType label) {

    this->appendVector(vector_data, label);
    return 1; // We always add the vector, no overrides in multi.
}

template <typename DataType, typename DistType>
VecSimBatchIterator *
HNSWIndex_Multi<DataType, DistType>::newBatchIterator(const void *queryBlob,
                                                      VecSimQueryParams *queryParams) const {
    // force_copy == true.
    auto queryBlobCopy = this->preprocessQuery(queryBlob, true);

    // take ownership of the blob copy and pass it to the batch iterator.
    auto *queryBlobCopyPtr = queryBlobCopy.release();
    // Ownership of queryBlobCopy moves to HNSW_BatchIterator that will free it at the end.
    return new (this->allocator) HNSWMulti_BatchIterator<DataType, DistType>(
        queryBlobCopyPtr, this, queryParams, this->allocator);
}

/**
 * Marks an element with the given label deleted, does NOT really change the current graph.
 * @param label
 */
template <typename DataType, typename DistType>
vecsim_stl::vector<idType> HNSWIndex_Multi<DataType, DistType>::markDelete(labelType label) {
    std::unique_lock<std::shared_mutex> index_data_lock(this->indexDataGuard);

    auto ids = this->getElementIds(label);
    for (idType id : ids) {
        this->markDeletedInternal(id);
    }
    labelLookup.erase(label);
    return ids;
}

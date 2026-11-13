/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include "brute_force.h"
#include "bfs_batch_iterator.h"
#include "VecSim/utils/vec_utils.h"

template <typename DataType, typename DistType>
class BruteForceIndex_Single : public BruteForceIndex<DataType, DistType> {

protected:
    vecsim_stl::unordered_map<labelType, idType> labelToIdLookup;

public:
    BruteForceIndex_Single(const BFParams *params,
                           const AbstractIndexInitParams &abstractInitParams,
                           const IndexComponents<DataType, DistType> &components);
    ~BruteForceIndex_Single() = default;

    int addVector(const void *vector_data, labelType label) override;
    int deleteVector(labelType label) override;
    int deleteVectorById(labelType label, idType id) override;
    VecSimRelabelCode relabelVector(labelType old_label, labelType new_label) override;
    double getDistanceFrom_Unsafe(labelType label, const void *vector_data) const override;

    std::unique_ptr<vecsim_stl::abstract_results_container>
    getNewResultsContainer(size_t cap) const override {
        return std::unique_ptr<vecsim_stl::abstract_results_container>(
            new (this->allocator) vecsim_stl::default_results_container(cap, this->allocator));
    }

    size_t indexLabelCount() const override { return this->count; }
    std::unordered_map<idType, std::pair<idType, labelType>>
    deleteVectorAndGetUpdatedIds(labelType label) override;

    // We call this when we KNOW that the label exists in the index.
    idType getIdOfLabel(labelType label) const { return labelToIdLookup.find(label)->second; }

    void getDataByLabel(labelType label,
                        std::vector<std::vector<DataType>> &vectors_output) const override {
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
        auto it = labelToIdLookup.find(label);
        if (it == labelToIdLookup.end()) {
            return;
        }

        auto vec = std::vector<DataType>(this->dim);
        // Only copy the vector data (dim * sizeof(DataType)), not any additional metadata like the
        // norm
        memcpy(vec.data(), this->getDataByInternalId(it->second), this->dim * sizeof(DataType));
        vectors_output.push_back(std::move(vec));
    }

#ifdef BUILD_TESTS

    std::vector<std::vector<char>> getStoredVectorDataByLabel(labelType label) const override {
        std::vector<std::vector<char>> vectors_output;
        auto id = labelToIdLookup.at(label);

        // Get the data pointer - need to cast to char* for memcpy
        const char *data = reinterpret_cast<const char *>(this->getDataByInternalId(id));

        // Create a vector with the full data (including any metadata like norms)
        std::vector<char> vec(this->getStoredDataSize());
        memcpy(vec.data(), data, this->getStoredDataSize());
        vectors_output.push_back(std::move(vec));

        return vectors_output;
    }
#endif
protected:
    // inline definitions
    void setVectorId(labelType label, idType id) override { labelToIdLookup.emplace(label, id); }

    void replaceIdOfLabel(labelType label, idType new_id, idType old_id) override {
        labelToIdLookup.at(label) = new_id;
    }

    void resizeLabelLookup(size_t new_max_elements) override {
        labelToIdLookup.reserve(new_max_elements);
    }

    bool isLabelExists(labelType label) override {
        return labelToIdLookup.find(label) != labelToIdLookup.end();
    }
    // Return a set of all labels that are stored in the index (helper for computing label count
    // without duplicates in tiered index). Caller should hold the flat buffer lock for read.
    vecsim_stl::set<labelType> getLabelsSet() const override {
        vecsim_stl::set<labelType> keys(this->allocator);
        for (auto &it : labelToIdLookup) {
            keys.insert(it.first);
        }
        return keys;
    }

    vecsim_stl::vector<idType> getElementIds(size_t label) const override {
        vecsim_stl::vector<idType> ids(this->allocator);
        auto it = labelToIdLookup.find(label);
        if (it != labelToIdLookup.end()) {
            ids.push_back(it->second);
        }
        return ids;
    }

    vecsim_stl::abstract_priority_queue<DistType, labelType> *
    getNewMaxPriorityQueue() const override {
        return new (this->allocator)
            vecsim_stl::max_priority_queue<DistType, labelType>(this->allocator);
    }

    BF_BatchIterator<DataType, DistType> *
    newBatchIterator_Instance(void *queryBlob, VecSimQueryParams *queryParams) const override {
        return new (this->allocator)
            BFS_BatchIterator<DataType, DistType>(queryBlob, this, queryParams, this->allocator);
    }

#ifdef BUILD_TESTS
#include "VecSim/algorithms/brute_force/brute_force_friend_tests.h"

#endif
};

/******************************* Implementation **********************************/

template <typename DataType, typename DistType>
BruteForceIndex_Single<DataType, DistType>::BruteForceIndex_Single(
    const BFParams *params, const AbstractIndexInitParams &abstractInitParams,
    const IndexComponents<DataType, DistType> &components)
    : BruteForceIndex<DataType, DistType>(params, abstractInitParams, components),
      labelToIdLookup(this->allocator) {}

template <typename DataType, typename DistType>
int BruteForceIndex_Single<DataType, DistType>::addVector(const void *vector_data,
                                                          labelType label) {

    auto optionalID = this->labelToIdLookup.find(label);
    // Check if label already exists, so it is an update operation.
    if (optionalID != this->labelToIdLookup.end()) {
        idType id = optionalID->second;
        auto processed_blob = this->preprocessForStorage(vector_data);
        this->vectors->updateElement(id, processed_blob.get());
        return 0;
    }

    this->appendVector(vector_data, label);
    return 1;
}

template <typename DataType, typename DistType>
int BruteForceIndex_Single<DataType, DistType>::deleteVector(labelType label) {

    // Find the id to delete.
    auto deleted_label_id_pair = this->labelToIdLookup.find(label);
    if (deleted_label_id_pair == this->labelToIdLookup.end()) {
        // Nothing to delete.
        return 0;
    }

    // Get deleted vector id.
    idType id_to_delete = deleted_label_id_pair->second;

    // Remove the pair of the deleted vector.
    labelToIdLookup.erase(label);

    this->removeVector(id_to_delete);
    return 1;
}

template <typename DataType, typename DistType>
std::unordered_map<idType, std::pair<idType, labelType>>
BruteForceIndex_Single<DataType, DistType>::deleteVectorAndGetUpdatedIds(labelType label) {

    std::unordered_map<idType, std::pair<idType, labelType>> updated_ids;
    // Find the id to delete.
    auto deleted_label_id_pair = this->labelToIdLookup.find(label);
    if (deleted_label_id_pair == this->labelToIdLookup.end()) {
        // Nothing to delete.
        return updated_ids;
    }

    // Get deleted vector id.
    idType id_to_delete = deleted_label_id_pair->second;

    // Remove the pair of the deleted vector.
    labelToIdLookup.erase(label);
    labelType last_id_label = this->idToLabelMapping[this->count - 1];
    this->removeVector(id_to_delete); // this will decrease this->count and make the swap
    if (id_to_delete != this->count) {
        updated_ids[id_to_delete] = {this->count, last_id_label};
    }
    return updated_ids;
}

template <typename DataType, typename DistType>
int BruteForceIndex_Single<DataType, DistType>::deleteVectorById(labelType label, idType id) {
    return deleteVector(label);
}

template <typename DataType, typename DistType>
VecSimRelabelCode BruteForceIndex_Single<DataType, DistType>::relabelVector(labelType old_label,
                                                                            labelType new_label) {
    if (old_label == new_label) {
        return VecSimRelabel_SameLabel;
    }
    auto old_it = labelToIdLookup.find(old_label);
    if (old_it == labelToIdLookup.end()) {
        return VecSimRelabel_OldLabelMissing;
    }
    if (labelToIdLookup.find(new_label) != labelToIdLookup.end()) {
        return VecSimRelabel_NewLabelTaken;
    }

    const idType id = old_it->second;
    labelToIdLookup.erase(old_it);
    labelToIdLookup.emplace(new_label, id);
    // Keep the id->label direction in sync; `topKQuery` reports results through it.
    this->setVectorLabel(id, new_label);
    return VecSimRelabel_OK;
}

template <typename DataType, typename DistType>
double
BruteForceIndex_Single<DataType, DistType>::getDistanceFrom_Unsafe(labelType label,
                                                                   const void *vector_data) const {

    auto optionalId = this->labelToIdLookup.find(label);
    if (optionalId == this->labelToIdLookup.end()) {
        return INVALID_SCORE;
    }
    idType id = optionalId->second;

    return this->calcDistance(this->getDataByInternalId(id), vector_data);
}

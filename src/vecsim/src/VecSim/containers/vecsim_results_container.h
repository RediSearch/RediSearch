/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include "VecSim/query_result_definitions.h"
#include "VecSim/utils/vecsim_stl.h"

namespace vecsim_stl {

// An abstract API for query result container, used by RANGE queries.
struct abstract_results_container : public VecsimBaseObject {
public:
    abstract_results_container(const std::shared_ptr<VecSimAllocator> &alloc)
        : VecsimBaseObject(alloc) {}
    ~abstract_results_container() = default;

    // Inserts (or updates) a new result to the container.
    virtual inline void emplace(size_t id, double score) = 0;

    // Returns the size of the container
    virtual inline size_t size() const = 0;

    // Returns a vector containing all current data, and passes its ownership
    virtual inline VecSimQueryResultContainer get_results() = 0;
};

struct unique_results_container : public abstract_results_container {
private:
    vecsim_stl::unordered_map<size_t, double> idToScore;

public:
    explicit unique_results_container(const std::shared_ptr<VecSimAllocator> &alloc)
        : abstract_results_container(alloc), idToScore(alloc) {}
    explicit unique_results_container(size_t cap, const std::shared_ptr<VecSimAllocator> &alloc)
        : abstract_results_container(alloc), idToScore(cap, alloc) {}

    inline void emplace(size_t id, double score) override {
        auto existing = idToScore.find(id);
        if (existing == idToScore.end()) {
            idToScore.emplace(id, score);
        } else if (existing->second > score) {
            existing->second = score;
        }
    }

    inline size_t size() const override { return idToScore.size(); }

    inline VecSimQueryResultContainer get_results() override {
        VecSimQueryResultContainer results(this->allocator);
        results.reserve(idToScore.size());
        for (auto res : idToScore) {
            results.push_back(VecSimQueryResult{res.first, res.second});
        }
        return results;
    }
};

struct default_results_container : public abstract_results_container {
private:
    VecSimQueryResultContainer _data;

public:
    explicit default_results_container(const std::shared_ptr<VecSimAllocator> &alloc)
        : abstract_results_container(alloc), _data(alloc) {}
    explicit default_results_container(size_t cap, const std::shared_ptr<VecSimAllocator> &alloc)
        : abstract_results_container(alloc), _data(alloc) {
        _data.reserve(cap);
    }
    ~default_results_container() = default;

    inline void emplace(size_t id, double score) override {
        _data.push_back(VecSimQueryResult{id, score});
    }
    inline size_t size() const override { return _data.size(); }
    inline VecSimQueryResultContainer get_results() override { return std::move(_data); }
};
} // namespace vecsim_stl

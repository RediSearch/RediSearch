/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once
#include <cstddef>
#include <span>

#include "VecSim/memory/vecsim_base.h"
#include "VecSim/spaces/spaces.h"
#include "VecSim/types/sq8.h"
#include "VecSim/utils/alignment.h"

enum class DistanceMode {
    StoredToStored,
    StoredToQuery,
};

/**
 * A distance callback selected once when an index is constructed.
 *
 * Stateless calculators populate `stateless_func`, preserving the existing hot path of one
 * indirect call to the selected distance kernel. Stateful calculators populate `stateful_func`
 * and `context`; this avoids a vtable lookup in the hot path without forcing stateless
 * calculators through an adapter.
 */
template <typename DistType>
struct DistanceDispatch {
    using stateful_dist_func_t = DistType (*)(const void *context, const void *lhs, const void *rhs,
                                              size_t dim);

    spaces::dist_func_t<DistType> stateless_func = nullptr;
    stateful_dist_func_t stateful_func = nullptr;
    const void *context = nullptr;

    static DistanceDispatch stateless(spaces::dist_func_t<DistType> func) {
        return {.stateless_func = func};
    }

    static DistanceDispatch stateful(const void *context, stateful_dist_func_t func) {
        return {.stateful_func = func, .context = context};
    }

    bool isValid() const { return (stateless_func != nullptr) != (stateful_func != nullptr); }

    DistType operator()(const void *lhs, const void *rhs, size_t dim) const {
        if (stateless_func) {
            return stateless_func(lhs, rhs, dim);
        }
        return stateful_func(context, lhs, rhs, dim);
    }
};

// We need this "wrapper" class to hold the DistanceCalculatorInterface in the index, that is not
// templated according to the distance function signature.
template <typename DistType>
class IndexCalculatorInterface : public VecsimBaseObject {
public:
    explicit IndexCalculatorInterface(std::shared_ptr<VecSimAllocator> allocator)
        : VecsimBaseObject(allocator) {}

    virtual ~IndexCalculatorInterface() = default;

    virtual DistType calcDistance(const void *v1, const void *v2, size_t dim) const = 0;

    virtual DistType calcDistanceForQuery(const void *candidate_vector, const void *query_vector,
                                          size_t dim) const = 0;

    // Called once per mode when constructing the index. The returned dispatch is cached so
    // distance calculations in the hot path do not perform virtual calls.
    virtual DistanceDispatch<DistType> getDistanceDispatch(DistanceMode mode) const = 0;
};

/**
 * This object purpose is to calculate the distance between two vectors.
 * It extends the IndexCalculatorInterface class' type to hold the distance function.
 * Every specific implementation of the distance calculator should hold by reference or by value the
 * parameters required for the calculation. The distance calculation API of all DistanceCalculator
 * classes is: calc_dist(v1,v2,dim). Internally it calls the distance function according the
 * template signature, allowing flexibility in the distance function arguments.
 */
template <typename DistType, typename DistFuncType>
class DistanceCalculatorInterface : public IndexCalculatorInterface<DistType> {
public:
    DistanceCalculatorInterface(std::shared_ptr<VecSimAllocator> allocator, DistFuncType dist_func,
                                DistFuncType query_dist_func = nullptr)
        : IndexCalculatorInterface<DistType>(allocator), dist_func(dist_func),
          query_dist_func(query_dist_func ? query_dist_func : dist_func) {}
    virtual DistType calcDistance(const void *v1, const void *v2, size_t dim) const = 0;
    virtual DistType calcDistanceForQuery(const void *candidate_vector, const void *query_vector,
                                          size_t dim) const = 0;

protected:
    DistFuncType dist_func;
    DistFuncType query_dist_func;
};

template <typename DistType>
class DistanceCalculatorCommon
    : public DistanceCalculatorInterface<DistType, spaces::dist_func_t<DistType>> {
public:
    DistanceCalculatorCommon(std::shared_ptr<VecSimAllocator> allocator,
                             spaces::dist_func_t<DistType> dist_func,
                             spaces::dist_func_t<DistType> query_dist_func = nullptr)
        : DistanceCalculatorInterface<DistType, spaces::dist_func_t<DistType>>(allocator, dist_func,
                                                                               query_dist_func) {}

    DistType calcDistance(const void *v1, const void *v2, size_t dim) const override {
        return this->dist_func(v1, v2, dim);
    }

    DistType calcDistanceForQuery(const void *candidate_vector, const void *query_vector,
                                  size_t dim) const override {
        return this->query_dist_func(candidate_vector, query_vector, dim);
    }

    DistanceDispatch<DistType> getDistanceDispatch(DistanceMode mode) const override {
        auto func = mode == DistanceMode::StoredToStored ? this->dist_func : this->query_dist_func;
        return DistanceDispatch<DistType>::stateless(func);
    }
};

/**
 * Distance calculator for mean-normalized SQ8 indices.
 *
 * Stored vectors are SQ8-quantized from x' = x - mean. IP queries remain unshifted and use
 * analytical correction terms derived from the per-vector x_mean_ip / y_mean_ip metadata.
 * L2 queries are centered once during preprocessing, allowing the base kernel to compute the
 * original distance directly without cancellation-prone correction terms.
 *
 * Correction formulas (expressed in terms of distance semantics):
 *
 * Asymmetric (stored vector x as SQ8 of x'):
 *   IP distance:  dist(x,y) = asym_func(x_blob, y_blob) - y_mean_ip
 *   L2 distance:  dist(x,y) = asym_func(x_blob, centered_y_blob)
 *                              where centered_y = y - mean
 *
 * Symmetric (both x and y as SQ8 blobs):
 *   IP distance:  dist(x,y) = sym_func(x_blob, y_blob) - x_mean_ip - y_mean_ip
 *                              + mean_sum_squares
 *   L2 distance:  dist(x,y) = sym_func(x_blob, y_blob)   (mean terms cancel exactly)
 *
 * Note: IP dist functions return 1 - IP(x',y'), not raw inner product. L2 dist functions
 * return ||x'-y'||² directly.
 *
 * DataType is float or float16
 * DistType is float
 */
template <typename DataType, typename DistType, VecSimMetric Metric>
class DistanceCalculatorWithNorm : public IndexCalculatorInterface<DistType> {
    static_assert(Metric == VecSimMetric_L2 || Metric == VecSimMetric_IP,
                  "DistanceCalculatorWithNorm only supports L2 and IP metrics");

private:
    using sq8 = vecsim_types::sq8;

    struct WithNormDistanceContext {
        spaces::dist_func_t<DistType> stored_func;
        spaces::dist_func_t<DistType> query_func;
        float mean_sum_squares;
    };

    // HNSW may install the accumulated mean before storing the first vector.
    WithNormDistanceContext context_;

    static DistType calcStoredWithContext(const void *opaque_context, const void *v1,
                                          const void *v2, size_t dim) {
        const auto *context = static_cast<const WithNormDistanceContext *>(opaque_context);
        DistType base = context->stored_func(v1, v2, dim);
        if constexpr (Metric == VecSimMetric_IP) {
            // base = 1 - IP(x', y'). We want 1 - IP(x, y).
            // IP(x, y) = IP(x', y') + x_mean_ip + y_mean_ip - mean_sum_squares
            // => 1 - IP(x, y) = base - x_mean_ip - y_mean_ip + mean_sum_squares
            float x_mean_ip =
                load_unaligned<float>(static_cast<const uint8_t *>(v1) + dim +
                                      sq8::template mean_ip_index<Metric>() * sizeof(float));
            float y_mean_ip =
                load_unaligned<float>(static_cast<const uint8_t *>(v2) + dim +
                                      sq8::template mean_ip_index<Metric>() * sizeof(float));
            return base - x_mean_ip - y_mean_ip + context->mean_sum_squares;
        } else { // L2: ||x - y||² = ||x' - y'||² (mean terms cancel exactly)
            return base;
        }
    }

    static DistType calcQueryWithContext(const void *opaque_context, const void *candidate,
                                         const void *query, size_t dim) {
        const auto *context = static_cast<const WithNormDistanceContext *>(opaque_context);
        DistType base = context->query_func(candidate, query, dim);
        if constexpr (Metric == VecSimMetric_IP) {
            float y_mean_ip =
                load_unaligned<float>(static_cast<const uint8_t *>(query) + dim * sizeof(DataType) +
                                      sq8::template query_mean_ip_index<Metric>() * sizeof(float));
            // base = 1 - IP(x', y). We want 1 - IP(x, y).
            // IP(x, y) = IP(x', y) + y_mean_ip
            // => 1 - IP(x, y) = base - y_mean_ip
            return base - y_mean_ip;
        }
        // L2 query preprocessing centers y, so base = ||(x - mean) - (y - mean)||².
        return base;
    }

public:
    DistanceCalculatorWithNorm(std::shared_ptr<VecSimAllocator> allocator,
                               spaces::dist_func_t<DistType> asym_func,
                               spaces::dist_func_t<DistType> sym_func, float mean_sum_squares)
        : IndexCalculatorInterface<DistType>(allocator), context_(WithNormDistanceContext{
                                                             .stored_func = sym_func,
                                                             .query_func = asym_func,
                                                             .mean_sum_squares = mean_sum_squares,
                                                         }) {}

    // Written once, before any stored vector, under the exclusive tiered main lock. The caller
    // also updates the preprocessor's mean; cached distance dispatches keep this context address.
    void setMeanSumSquares(std::span<const float> mean) noexcept {
        if constexpr (Metric == VecSimMetric_IP) {
            float mean_sum_squares = 0.0f;
            for (float value : mean) {
                mean_sum_squares += value * value;
            }
            context_.mean_sum_squares = mean_sum_squares;
        }
    }

    // Symmetric: both v1 and v2 are stored SQ8-of-x' blobs.
    DistType calcDistance(const void *v1, const void *v2, size_t dim) const override {
        return calcStoredWithContext(&context_, v1, v2, dim);
    }

    // Asymmetric: IP queries are raw; L2 queries are centered during preprocessing. The candidate
    // is a stored SQ8-of-x' blob.
    // Note: query_dist_func expects (storage, query) argument order.
    DistType calcDistanceForQuery(const void *candidate, const void *query,
                                  size_t dim) const override {
        return calcQueryWithContext(&context_, candidate, query, dim);
    }

    DistanceDispatch<DistType> getDistanceDispatch(DistanceMode mode) const override {
        if constexpr (Metric == VecSimMetric_L2) {
            auto func =
                mode == DistanceMode::StoredToStored ? context_.stored_func : context_.query_func;
            return DistanceDispatch<DistType>::stateless(func);
        }
        if (mode == DistanceMode::StoredToStored) {
            return DistanceDispatch<DistType>::stateful(&context_, calcStoredWithContext);
        }
        return DistanceDispatch<DistType>::stateful(&context_, calcQueryWithContext);
    }
};

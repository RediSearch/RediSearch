/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include "VecSim/spaces/computer/calculator.h"
#include "VecSim/spaces/spaces.h"

#include <stdexcept>

/**
 * Distance calculation modes for disk-based HNSW index.
 *
 * The disk index uses scalar quantization (SQ8) for storage efficiency.
 * Different operations require different precision levels:
 *
 * - Full:            FP32 ↔ FP32 - Both vectors are full precision (exact distance / reranking)
 * - QuantizedVsFull: SQ8 ↔ FP32  - Quantized storage vs full precision query (search operations)
 * - Quantized:       SQ8 ↔ SQ8   - Both vectors are quantized (graph operations)
 */
enum class DistanceMode {
    Full,            // FP32 ↔ FP32 (both full precision)
    QuantizedVsFull, // SQ8 ↔ FP32 (quantized vs full)
    Quantized        // SQ8 ↔ SQ8 (both quantized)
};

/**
 * Multi-mode distance calculator for disk-based vector indexes.
 *
 * This calculator holds three distance functions for different precision levels
 * and provides a templated interface for zero-overhead mode selection at compile time.
 *
 * The base class calcDistance() method is intentionally disabled - disk indexes must
 * use the templated calcDistance<Mode>() to explicitly specify the distance mode.
 * This prevents accidental misuse where the wrong precision mode could be silently used.
 */
template <typename DistType>
class DiskDistanceCalculator : public IndexCalculatorInterface<DistType> {
public:
    DiskDistanceCalculator(std::shared_ptr<VecSimAllocator> allocator, spaces::dist_func_t<DistType> fullFunc,
                           spaces::dist_func_t<DistType> quantizedVsFullFunc,
                           spaces::dist_func_t<DistType> quantizedFunc)
        : IndexCalculatorInterface<DistType>(allocator), fullFunc_(fullFunc), quantizedVsFullFunc_(quantizedVsFullFunc),
          quantizedFunc_(quantizedFunc) {}

    /**
     * Base class interface - intentionally disabled for disk calculators.
     *
     * Disk indexes must use the templated calcDistance<Mode>() to explicitly specify
     * the distance mode (Full, QuantizedVsFull, or Quantized). Calling this method
     * indicates a bug where code is using the wrong interface.
     *
     * @throws std::logic_error Always throws to catch accidental misuse.
     */
    DistType calcDistance(const void* /*&v1*/, const void* /*&v2*/, size_t /*dim*/) const override {
        throw std::logic_error("DiskDistanceCalculator::calcDistance() called without explicit mode. "
                               "Use calcDistance<DistanceMode>() instead.");
    }

    /**
     * Templated distance calculation with compile-time mode selection.
     *
     * Uses if constexpr for zero runtime overhead - the compiler generates
     * a direct function pointer call for each mode, identical to calling
     * the distance function directly.
     *
     * @tparam Mode The distance mode (Full, QuantizedVsFull, or Quantized)
     * @param v1 First vector
     * @param v2 Second vector
     * @param dim Vector dimension
     * @return Distance between the vectors
     */
    template <DistanceMode Mode>
    DistType calcDistance(const void* v1, const void* v2, size_t dim) const {
        return getDistFunc<Mode>()(v1, v2, dim);
    }

    /**
     * Get the distance function pointer for a specific mode.
     * @tparam Mode The distance mode to get the function for
     * @return The distance function pointer for the specified mode
     */
    template <DistanceMode Mode>
    spaces::dist_func_t<DistType> getDistFunc() const {
        if constexpr (Mode == DistanceMode::Full) {
            return fullFunc_;
        } else if constexpr (Mode == DistanceMode::QuantizedVsFull) {
            return quantizedVsFullFunc_;
        } else if constexpr (Mode == DistanceMode::Quantized) {
            return quantizedFunc_;
        }
        // throw at compile time for invalid mode (security for enum misuse or future extensions)
        throw std::invalid_argument("Invalid DistanceMode");
    }

private:
    spaces::dist_func_t<DistType> fullFunc_;            // FP32-FP32
    spaces::dist_func_t<DistType> quantizedVsFullFunc_; // SQ8-FP32
    spaces::dist_func_t<DistType> quantizedFunc_;       // SQ8-SQ8
};

/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include "vecsim/utils/vecsim_stl.h"
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <random>
#include <span>
#include <utility>

namespace vecsim::quantization {

// The orthogonal transform R applied to stored residuals, queries and the fitted mean: three
// rounds of
//   [round > 0: permute coordinates] -> multiply by random signs -> normalized FWHT on each
//   power-of-two block,
// with block = the largest power of two dividing dim, capped at kMaxBlock. There is no
// permutation after the last round: it would only relabel output coordinates, and the scalar
// codebook is shared by all coordinates. State is 3 sign vectors and 2 permutations (about
// 15 KB at dim 768) instead of a dim x dim matrix (2.36 MB at dim 768).
class TurboQuantRotation {
public:
    static constexpr unsigned kRounds = 3;
    // apply() ping-pongs between out and scratch, so an odd round count ends in out.
    static_assert(kRounds % 2 == 1);
    // Smaller blocks mix too locally, so TurboQuant requires dim to be a multiple of kMinBlock.
    // Recall was validated only for dim 768 (three 256 blocks).
    static constexpr size_t kMinBlock = 64;
    static constexpr size_t kMaxBlock = 1024;

    // Block size for dim, or 0 when dim is not a multiple of kMinBlock.
    static size_t hadamardBlock(size_t dim) {
        if (dim == 0)
            return 0;
        size_t block = dim & (~dim + 1);
        if (block > kMaxBlock)
            block = kMaxBlock;
        return block >= kMinBlock ? block : 0;
    }
    // Permanent payload bytes of the rotation for this dimension, in two allocations.
    static size_t sharedBytesFor(size_t dim) {
        return kRounds * dim * sizeof(float) + (kRounds - 1) * dim * sizeof(uint32_t);
    }

    explicit TurboQuantRotation(const std::shared_ptr<VecSimAllocator> &allocator)
        : signs_(allocator), permutations_(allocator) {}

    size_t dim() const { return dim_; }
    size_t block() const { return block_; }
    std::span<const float> scaledSigns() const { return {signs_.data(), signs_.size()}; }
    std::span<const uint32_t> permutations() const {
        return {permutations_.data(), permutations_.size()};
    }
    size_t sharedBytes() const {
        return signs_.size() * sizeof(float) + permutations_.size() * sizeof(uint32_t);
    }

    // Draws signs then permutations from rng. Only mt19937_64 raw output is used (its sequence
    // is fixed by the C++ standard), not std::shuffle or std distributions, whose results are
    // implementation defined. The same seed therefore gives the same rotation everywhere.
    void generate(size_t dim, std::mt19937_64 &rng) { // NOSONAR(S2245)
        const size_t block = hadamardBlock(dim);
        assert(block && signs_.empty()); // Callers check hadamardBlock(dim) first.
        dim_ = dim;
        block_ = block;
        signs_.resize(kRounds * dim);
        permutations_.resize((kRounds - 1) * dim);
        const float magnitude = 1.0f / std::sqrt(static_cast<float>(block));
        for (unsigned round = 0; round < kRounds; ++round) {
            float *signs = signs_.data() + round * dim;
            for (size_t i = 0; i < dim; i += 64) {
                const uint64_t bits = rng();
                for (size_t b = 0; b < 64 && i + b < dim; ++b)
                    signs[i + b] = ((bits >> b) & 1) ? -magnitude : magnitude;
            }
        }
        for (unsigned round = 1; round < kRounds; ++round) {
            uint32_t *permutation = permutations_.data() + (round - 1) * dim;
            for (size_t i = 0; i < dim; ++i)
                permutation[i] = static_cast<uint32_t>(i);
            for (size_t i = dim - 1; i > 0; --i)
                std::swap(permutation[i], permutation[boundedDraw(rng, i + 1)]);
        }
    }

    // out = R * in. out may alias in; scratch (dim floats) must not alias out, and may alias
    // in, which is fully consumed before scratch is written.
    void apply(const float *in, float *out, float *scratch) const {
        assert(block_ && out != scratch);
        if (in != out)
            std::memcpy(out, in, dim_ * sizeof(float));
        transformBlocks(out, signs_.data());
        float *current = out, *next = scratch;
        for (unsigned round = 1; round < kRounds; ++round) {
            const uint32_t *permutation = permutations_.data() + (round - 1) * dim_;
            for (size_t i = 0; i < dim_; ++i)
                next[i] = current[permutation[i]];
            transformBlocks(next, signs_.data() + round * dim_);
            std::swap(current, next);
        }
    }

private:
    size_t dim_ = 0;
    size_t block_ = 0;
    vecsim_stl::vector<float> signs_;           // kRounds x dim, each +-1/sqrt(block)
    vecsim_stl::vector<uint32_t> permutations_; // (kRounds - 1) x dim, next[i] = cur[p[i]]

    // Per block: x *= scaled signs, then the unnormalized Walsh-Hadamard butterflies (the
    // 1/sqrt(block) normalization is folded into the signs). Only element-wise multiply, add and
    // subtract in a fixed order, so the result is the same on every machine. The compiler
    // vectorizes the wider strides.
    void transformBlocks(float *x, const float *signs) const {
        for (size_t i = 0; i < dim_; ++i)
            x[i] *= signs[i];
        for (size_t start = 0; start < dim_; start += block_) {
            float *b = x + start;
            for (size_t h = 1; h < block_; h *= 2)
                for (size_t j = 0; j < block_; j += 2 * h)
                    for (size_t k = j; k < j + h; ++k) {
                        const float lo = b[k], hi = b[k + h];
                        b[k] = lo + hi;
                        b[k + h] = lo - hi;
                    }
        }
    }
    // Uniform in [0, bound) by Lemire's multiply-shift with rejection, on the top 32 bits of
    // each draw so only 64-bit arithmetic is needed. Dimensions are below 2^32.
    static size_t boundedDraw(std::mt19937_64 &rng, uint64_t bound64) { // NOSONAR(S2245)
        assert(bound64 > 0 && bound64 <= std::numeric_limits<uint32_t>::max());
        const auto bound = static_cast<uint32_t>(bound64);
        uint64_t product = (rng() >> 32) * bound;
        auto low = static_cast<uint32_t>(product);
        if (low < bound) {
            const uint32_t threshold = (0u - bound) % bound;
            while (low < threshold) {
                product = (rng() >> 32) * bound;
                low = static_cast<uint32_t>(product);
            }
        }
        return static_cast<size_t>(product >> 32);
    }
};

} // namespace vecsim::quantization

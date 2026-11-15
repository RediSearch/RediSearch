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
#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <limits>
#include <random>
#include <span>
#include <stdexcept>
#include <utility>
#ifdef BUILD_TESTS
#include <istream>
#include <ostream>
#endif

namespace vecsim::quantization {

// The value is also the TurboQuant fitted-state schema number that persists this kind.
enum class TurboQuantRotationKind : uint32_t { Unset = 0, Dense = 1, BlockHadamard = 2 };

// The orthogonal transform Pi applied to stored residuals, queries and the fitted mean.
//
// BlockHadamard (the default whenever the dimension allows it): three rounds of
//   [round > 0: permute coordinates] -> multiply by random signs -> normalized FWHT on each
//   power-of-two block,
// with block = the largest power of two dividing dim, capped at kMaxBlock. There is no
// permutation after the last round: it would only relabel output coordinates, and the scalar
// codebook is shared by all coordinates. State is 3 sign vectors and 2 permutations (about
// 15 KB at dim 768) instead of a dim x dim matrix (2.36 MB at dim 768).
//
// Dense: a Haar random orthogonal matrix, kept for dimensions whose block would be below
// kMinBlock (for example dim 100 has block 4) and for fitted state written before
// BlockHadamard existed. Its arithmetic stays in TurboQuant so those indexes score exactly as
// before.
class TurboQuantRotation {
public:
    static constexpr unsigned kRounds = 3;
    // Below this block size the per-block mixing is too local; use the dense rotation instead.
    // Recall was validated only for dim 768 (three 256 blocks), see the TQ rotation study.
    static constexpr size_t kMinBlock = 64;
    static constexpr size_t kMaxBlock = 1024;

    // Block size BlockHadamard uses for dim, or 0 when dim needs the dense fallback.
    static size_t hadamardBlock(size_t dim) {
        if (dim == 0) return 0;
        size_t block = dim & (~dim + 1);
        if (block > kMaxBlock) block = kMaxBlock;
        return block >= kMinBlock ? block : 0;
    }
    static TurboQuantRotationKind kindFor(size_t dim) {
        return hadamardBlock(dim) ? TurboQuantRotationKind::BlockHadamard
                                  : TurboQuantRotationKind::Dense;
    }
    static float signMagnitude(size_t block) { return 1.0f / std::sqrt(static_cast<float>(block)); }
    // Permanent payload bytes for a freshly built rotation of this dimension.
    static size_t sharedBytesFor(size_t dim) {
        return kindFor(dim) == TurboQuantRotationKind::Dense
                   ? dim * dim * sizeof(float)
                   : kRounds * dim * sizeof(float) + (kRounds - 1) * dim * sizeof(uint32_t);
    }
    // Permanent allocations for a freshly built rotation of this dimension.
    static size_t allocationsFor(size_t dim) {
        return kindFor(dim) == TurboQuantRotationKind::Dense ? 1 : 2;
    }

    explicit TurboQuantRotation(const std::shared_ptr<VecSimAllocator> &allocator)
        : allocator_(allocator), dense_(allocator), signs_(allocator), permutations_(allocator) {}

    TurboQuantRotationKind kind() const { return kind_; }
    bool isBlockHadamard() const { return kind_ == TurboQuantRotationKind::BlockHadamard; }
    size_t dim() const { return dim_; }
    size_t block() const { return block_; }
    std::span<const float> dense() const { return {dense_.data(), dense_.size()}; }
    std::span<const float> scaledSigns() const { return {signs_.data(), signs_.size()}; }
    std::span<const uint32_t> permutations() const {
        return {permutations_.data(), permutations_.size()};
    }
    size_t sharedBytes() const {
        return dense_.size() * sizeof(float) + signs_.size() * sizeof(float) +
               permutations_.size() * sizeof(uint32_t);
    }

    // Dense storage for TurboQuant to fill (generation or load). Row-major dim x dim.
    vecsim_stl::vector<float> &resetDense(size_t dim) {
        clear();
        kind_ = TurboQuantRotationKind::Dense;
        dim_ = dim;
        dense_.assign(dim * dim, 0.0f);
        return dense_;
    }

    // Draws signs then permutations from rng. Only mt19937_64 raw output is used (its sequence
    // is fixed by the C++ standard), not std::shuffle or std distributions, whose results are
    // implementation defined. The same seed therefore gives the same rotation everywhere.
    void generateBlockHadamard(size_t dim, std::mt19937_64 &rng) {
        const size_t block = hadamardBlock(dim);
        assert(block); // Callers check kindFor(dim) first.
        clear();
        kind_ = TurboQuantRotationKind::BlockHadamard;
        dim_ = dim;
        block_ = block;
        signs_.resize(kRounds * dim);
        permutations_.resize((kRounds - 1) * dim);
        const float magnitude = signMagnitude(block);
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
            for (size_t i = 0; i < dim; ++i) permutation[i] = static_cast<uint32_t>(i);
            for (size_t i = dim - 1; i > 0; --i)
                std::swap(permutation[i], permutation[boundedDraw(rng, i + 1)]);
        }
    }

    // out = Pi * in. out may alias in; scratch (dim floats) must not alias out, and may alias
    // in, which is fully consumed before scratch is written. Requires isBlockHadamard().
    void apply(const float *in, float *out, float *scratch) const {
        assert(isBlockHadamard() && out != scratch);
        if (in != out) std::memcpy(out, in, dim_ * sizeof(float));
        transformBlocks(out, signs_.data());
        float *current = out, *next = scratch;
        for (unsigned round = 1; round < kRounds; ++round) {
            const uint32_t *permutation = permutations_.data() + (round - 1) * dim_;
            for (size_t i = 0; i < dim_; ++i) next[i] = current[permutation[i]];
            transformBlocks(next, signs_.data() + round * dim_);
            std::swap(current, next);
        }
        if (current != out) std::memcpy(out, current, dim_ * sizeof(float));
    }

#ifdef BUILD_TESTS
    // [block u64][rounds u64][sign count u64][signs f32...][permutation count u64][u32...]
    static uint64_t minimumBlockHadamardBytes(size_t dim) {
        return 4 * sizeof(uint64_t) + uint64_t(kRounds) * dim * sizeof(float) +
               uint64_t(kRounds - 1) * dim * sizeof(uint32_t);
    }
    void saveBlockHadamard(std::ostream &out) const {
        assert(isBlockHadamard());
        const uint64_t header[] = {block_, kRounds, signs_.size()};
        out.write(reinterpret_cast<const char *>(header), sizeof(header));
        out.write(reinterpret_cast<const char *>(signs_.data()), signs_.size() * sizeof(float));
        const uint64_t count = permutations_.size();
        out.write(reinterpret_cast<const char *>(&count), sizeof(count));
        out.write(reinterpret_cast<const char *>(permutations_.data()),
                  permutations_.size() * sizeof(uint32_t));
    }
    // Restores the persisted signs and permutations (never regenerated from the seed) and
    // rejects anything that is not exactly a rotation this class would build for dim.
    void loadBlockHadamard(std::istream &in, size_t dim) {
        const size_t block = hadamardBlock(dim);
        uint64_t header[3]{};
        in.read(reinterpret_cast<char *>(header), sizeof(header));
        if (!in || !block || header[0] != block || header[1] != kRounds ||
            header[2] != uint64_t(kRounds) * dim)
            throw std::runtime_error("Invalid TurboQuant block Hadamard rotation");
        // Read and validate into locals; this object changes only once everything is valid.
        vecsim_stl::vector<float> signs(kRounds * dim, allocator_);
        vecsim_stl::vector<uint32_t> permutations((kRounds - 1) * dim, allocator_);
        in.read(reinterpret_cast<char *>(signs.data()), signs.size() * sizeof(float));
        uint64_t count = 0;
        if (in) in.read(reinterpret_cast<char *>(&count), sizeof(count));
        if (!in) throw std::runtime_error("Truncated TurboQuant block Hadamard rotation");
        if (count != permutations.size())
            throw std::runtime_error("Invalid TurboQuant block Hadamard permutations");
        in.read(reinterpret_cast<char *>(permutations.data()),
                permutations.size() * sizeof(uint32_t));
        if (!in) throw std::runtime_error("Truncated TurboQuant block Hadamard rotation");
        const float magnitude = signMagnitude(block);
        for (float sign : signs)
            if (sign != magnitude && sign != -magnitude)
                throw std::runtime_error("Invalid TurboQuant block Hadamard sign");
        vecsim_stl::vector<bool> seen(dim, false, allocator_);
        for (unsigned round = 1; round < kRounds; ++round) {
            std::fill(seen.begin(), seen.end(), false);
            for (size_t i = 0; i < dim; ++i) {
                const uint32_t source = permutations[(round - 1) * dim + i];
                if (source >= dim || seen[source])
                    throw std::runtime_error("Invalid TurboQuant block Hadamard permutation");
                seen[source] = true;
            }
        }
        clear();
        kind_ = TurboQuantRotationKind::BlockHadamard;
        dim_ = dim;
        block_ = block;
        signs_.swap(signs);
        permutations_.swap(permutations);
    }
#endif

private:
    std::shared_ptr<VecSimAllocator> allocator_;
    TurboQuantRotationKind kind_ = TurboQuantRotationKind::Unset;
    size_t dim_ = 0;
    size_t block_ = 0;
    vecsim_stl::vector<float> dense_;
    vecsim_stl::vector<float> signs_;            // kRounds x dim, each +-1/sqrt(block)
    vecsim_stl::vector<uint32_t> permutations_; // (kRounds - 1) x dim, next[i] = cur[p[i]]

    void clear() {
        kind_ = TurboQuantRotationKind::Unset;
        dim_ = block_ = 0;
        dense_.clear(); dense_.shrink_to_fit();
        signs_.clear(); signs_.shrink_to_fit();
        permutations_.clear(); permutations_.shrink_to_fit();
    }
    // Per block: x *= scaled signs, then the unnormalized Walsh-Hadamard butterflies (the
    // 1/sqrt(block) normalization is folded into the signs). Only element-wise multiply, add and
    // subtract in a fixed order, so the result is the same on every machine, which persisted
    // fitted state relies on. The compiler vectorizes the wider strides.
    void transformBlocks(float *x, const float *signs) const {
        for (size_t i = 0; i < dim_; ++i) x[i] *= signs[i];
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
    static size_t boundedDraw(std::mt19937_64 &rng, uint64_t bound64) {
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

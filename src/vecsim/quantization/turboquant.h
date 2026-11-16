/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include "vecsim/quantization/turboquant_kernels.h"
#include "vecsim/quantization/turboquant_rotation.h"
#include "vecsim/utils/vecsim_stl.h"
#include <algorithm>
#include <cassert>
#include <cmath>
#include <cstring>
#include <limits>
#include <numbers>
#include <random>
#include <span>
#include <stdexcept>
#ifdef BUILD_TESTS
#include <istream>
#include <ostream>
#endif

namespace vecsim::quantization {

// MSE TurboQuant estimator for 1 - inner product, with a projection-match scale and a
// mean-error correction. The rotation R is block Hadamard, or dense when the dimension needs it
// (see TurboQuantRotation). Stored vectors encode r = x - mean as
// [ceil(d*b/8) packed codes][scale][delta], where n = ||r||, u = R*r/n, c = the centroid decode,
// scale = n/(u.c) and delta = scale*(c.R*mean) - r.mean. Queries hold [R*q: d floats][q.mean].
// Only construction and loading validate and throw. Encoding and query preparation allocate
// scratch, so they can throw std::bad_alloc; scoring never throws.
class TurboQuant {
    std::shared_ptr<VecSimAllocator> allocator_;
    size_t dim_;
    unsigned bits_;
    uint64_t seed_;
    turboquant_detail::PackedDot packed_dot_ = nullptr;
    turboquant_detail::RowDot row_dot_ = nullptr;
    turboquant_detail::PackedPairDot stored_pair_dot_ = nullptr;
    size_t code_bytes_;
    TurboQuantRotation rotation_;
    vecsim_stl::vector<float> centroids_;
    vecsim_stl::vector<float> bias_mean_;
    vecsim_stl::vector<float> bias_rotated_mean_;
    float bias_mean_norm_squared_ = 0;

    static size_t validatedDim(const std::shared_ptr<VecSimAllocator> &allocator, size_t dim,
                               unsigned bits, std::span<const float> mean) {
        if (!allocator || !dimSupported(dim) || (bits != 2 && bits != 3) || mean.size() != dim)
            throw std::invalid_argument("TurboQuant requires dim >= 2, 2/3 bits and a dim mean");
        for (float value : mean) {
            if (!std::isfinite(value))
                throw std::invalid_argument("TurboQuant bias mean must be finite");
        }
        return dim;
    }

    static uint64_t splitSeed(uint64_t seed) {
        seed += UINT64_C(0x9e3779b97f4a7c15);
        seed = (seed ^ (seed >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
        seed = (seed ^ (seed >> 27)) * UINT64_C(0x94d049bb133111eb);
        return seed ^ (seed >> 31);
    }

    void initializeRotation(uint64_t seed) {
        std::mt19937_64 rotation_rng(splitSeed(seed));
        if (TurboQuantRotation::kindFor(dim_) == TurboQuantRotationKind::BlockHadamard) {
            rotation_.generateBlockHadamard(dim_, rotation_rng);
            return;
        }
        auto &dense = rotation_.resetDense(dim_);
        std::normal_distribution<double> gaussian;
        vecsim_stl::vector<double> basis(dim_ * dim_, allocator_);
        // Row-wise Gaussian QR with positive diagonal convention gives Haar orthogonal Pi.
        // Reorthogonalization and double workspace keep the initialization stable at large d.
        for (size_t row = 0; row < dim_; ++row) {
            double *current = basis.data() + row * dim_;
            for (size_t col = 0; col < dim_; ++col) {
                current[col] = gaussian(rotation_rng);
            }
            for (unsigned pass = 0; pass < 2; ++pass) {
                for (size_t previous = 0; previous < row; ++previous) {
                    const double *other = basis.data() + previous * dim_;
                    double dot = 0;
                    for (size_t col = 0; col < dim_; ++col) {
                        dot += current[col] * other[col];
                    }
                    for (size_t col = 0; col < dim_; ++col) {
                        current[col] -= dot * other[col];
                    }
                }
            }
            double norm = 0;
            for (size_t col = 0; col < dim_; ++col) {
                norm += current[col] * current[col];
            }
            norm = std::sqrt(norm);
            if (!(norm > 0) || !std::isfinite(norm)) {
                throw std::runtime_error("TurboQuant Gaussian QR is degenerate");
            }
            for (size_t col = 0; col < dim_; ++col) {
                current[col] /= norm;
                dense[row * dim_ + col] = static_cast<float>(current[col]);
            }
        }
    }

    void initializeCentroids() {
        // The exact finite-d sphere marginal is (1-z*z)^((d-3)/2). With z=sin(theta),
        // integration weight becomes cos(theta)^(d-2), including the singular d=2 case.
        // Midpoint quadrature approximates its Lloyd-Max solution; it is not a Gaussian codebook.
        constexpr size_t samples = 32768;
        vecsim_stl::vector<double> coordinates(samples, allocator_);
        vecsim_stl::vector<double> weights(samples, allocator_);
        const size_t levels = centroids_.size();
        vecsim_stl::vector<double> centers(levels, allocator_);
        vecsim_stl::vector<double> masses(levels, allocator_);
        vecsim_stl::vector<double> sums(levels, allocator_);
        for (size_t sample = 0; sample < samples; ++sample) {
            const double theta = std::numbers::pi * ((sample + 0.5) / samples - 0.5);
            coordinates[sample] = std::sin(theta);
            weights[sample] = std::pow(std::cos(theta), static_cast<double>(dim_ - 2));
        }
        for (size_t level = 0; level < levels; ++level) {
            centers[level] = (2.0 * level + 1.0 - levels) / levels *
                             std::min(1.0, 2.5 / std::sqrt(static_cast<double>(dim_)));
        }
        bool converged = false;
        for (unsigned iteration = 0; iteration < 1024; ++iteration) {
            std::fill(masses.begin(), masses.end(), 0);
            std::fill(sums.begin(), sums.end(), 0);
            size_t level = 0;
            for (size_t sample = 0; sample < samples; ++sample) {
                while (level + 1 < levels &&
                       coordinates[sample] > (centers[level] + centers[level + 1]) * 0.5) {
                    ++level;
                }
                masses[level] += weights[sample];
                sums[level] += weights[sample] * coordinates[sample];
            }
            double movement = 0;
            for (size_t level = 0; level < levels; ++level) {
                if (!(masses[level] > 0)) {
                    throw std::runtime_error("TurboQuant sphere codebook has an empty cell");
                }
                const double updated = sums[level] / masses[level];
                movement = std::max(movement, std::abs(updated - centers[level]));
                centers[level] = updated;
            }
            if (movement < 1e-12) {
                converged = true;
                break;
            }
        }
        if (!converged) {
            throw std::runtime_error("TurboQuant sphere codebook did not converge");
        }
        for (size_t level = 0; level < levels; ++level) {
            centroids_[level] = static_cast<float>(centers[level]);
        }
    }

    // Derived mean terms: ||mean||^2 in double, and R*mean (in double for a dense R, zero while
    // R is unset). A block Hadamard R uses `scratch`, dim floats not aliasing `rotated`.
    void computeMeanTerms(std::span<const float> mean, float *rotated, float &norm_squared,
                          float *scratch) const noexcept {
        double norm = 0;
        for (float value : mean)
            norm += double(value) * value;
        norm_squared = static_cast<float>(norm);
        if (rotation_.isBlockHadamard()) {
            rotation_.apply(mean.data(), rotated, scratch);
            return;
        }
        const auto dense = rotation_.dense();
        const size_t cols = dense.empty() ? 0 : dim_;
        for (size_t row = 0; row < dim_; ++row) {
            double dot = 0;
            for (size_t col = 0; col < cols; ++col)
                dot += double(dense[row * dim_ + col]) * mean[col];
            rotated[row] = static_cast<float>(dot);
        }
    }

    void selectKernels() {
        packed_dot_ =
            bits_ == 2 ? turboquant_detail::packedDot<2> : turboquant_detail::packedDot<3>;
        stored_pair_dot_ =
            bits_ == 2 ? turboquant_detail::packedPairDot<2> : turboquant_detail::packedPairDot<3>;
#if defined(CPU_FEATURES_ARCH_AARCH64) && defined(__ARM_NEON)
        if (cpu_features::GetAarch64Info().features.asimd) {
            if (dim_ >= 4) {
                row_dot_ = turboquant_detail::rowDotNEON;
            }
            if (dim_ >= 8) {
                packed_dot_ = bits_ == 2 ? turboquant_detail::packedDotNEON<2>
                                         : turboquant_detail::packedDotNEON<3>;
                stored_pair_dot_ = bits_ == 2 ? turboquant_detail::packedPairDotNEON<2>
                                              : turboquant_detail::packedPairDotNEON<3>;
            }
        }
#endif
    }

    static float readFloat(const unsigned char *bytes) {
        float value;
        std::memcpy(&value, bytes, sizeof(value));
        return value;
    }
    static void writeFloat(unsigned char *bytes, float value) {
        std::memcpy(bytes, &value, sizeof(value));
    }
    void writeCode(unsigned char *bytes, size_t coordinate, unsigned code) const {
        const size_t start = coordinate * bits_;
        for (unsigned bit = 0; bit < bits_; ++bit) {
            bytes[(start + bit) / 8] |= ((code >> bit) & 1U) << ((start + bit) % 8);
        }
    }
    float biasOffset(const void *stored) const {
        return readFloat(static_cast<const unsigned char *>(stored) + code_bytes_ + sizeof(float));
    }

public:
    // Bound both permanent matrices and the double-precision QR workspace before multiplying.
    static bool dimSupported(size_t dim) {
        constexpr size_t limit = std::numeric_limits<size_t>::max() / (4 * sizeof(double));
        return dim >= 2 && dim <= limit / dim;
    }

    // `initialize = false` leaves the rotation and centroids for loadFitted to fill.
    TurboQuant(std::shared_ptr<VecSimAllocator> allocator, size_t dim, unsigned bits,
               uint64_t seed, std::span<const float> mean, bool initialize = true)
        : allocator_(std::move(allocator)), dim_(validatedDim(allocator_, dim, bits, mean)),
          bits_(bits), seed_(seed), code_bytes_((dim_ * bits_ + 7) / 8),
          rotation_(allocator_), centroids_(size_t{1} << bits_, allocator_),
          bias_mean_(dim_, allocator_), bias_rotated_mean_(dim_, allocator_) {
        if (initialize) {
            initializeRotation(seed);
            initializeCentroids();
        }
        setMean(mean);
        selectKernels();
    }

    // Writer-only: install a fitted mean before any vector is stored, with readers excluded.
    // Updates the buffers in place, so the codec address and cached dispatches remain valid.
    void setMean(std::span<const float> mean) noexcept {
        assert(mean.size() == dim_ && mean.data() != bias_mean_.data());
        // bias_mean_ serves as the rotation scratch before it receives the new mean.
        computeMeanTerms(mean, bias_rotated_mean_.data(), bias_mean_norm_squared_,
                         bias_mean_.data());
        std::copy(mean.begin(), mean.end(), bias_mean_.begin());
    }

#ifdef BUILD_TESTS
    // Fitted state. The schema is the rotation kind: schema 1 stores a dense rotation as
    // [count][dim*dim floats], byte-for-byte the format written before block Hadamard existed;
    // schema 2 stores the actual block Hadamard signs and permutations in the same position.
    // Everything after the rotation is common to both. The seed is metadata only.
    void saveFitted(std::ostream &out) const {
        // schema (= rotation kind), MSE, projection-match, mean-error
        const uint32_t header[] = {static_cast<uint32_t>(rotation_.kind()), bits_, 1, 3, 1};
        out.write(reinterpret_cast<const char *>(header), sizeof(header));
        out.write(reinterpret_cast<const char *>(&seed_), sizeof(seed_));
        auto write = [&](const auto &values) {
            uint64_t count = values.size();
            out.write(reinterpret_cast<const char *>(&count), sizeof(count));
            out.write(reinterpret_cast<const char *>(values.data()), count * sizeof(float));
        };
        if (rotation_.isBlockHadamard())
            rotation_.saveBlockHadamard(out);
        else
            write(rotation_.dense());
        write(centroids_);
        write(bias_mean_);
        write(bias_rotated_mean_);
        out.write(reinterpret_cast<const char *>(&bias_mean_norm_squared_), sizeof(float));
    }
    static std::shared_ptr<TurboQuant> loadFitted(std::istream &in,
                                                  const std::shared_ptr<VecSimAllocator> &allocator,
                                                  size_t dim, unsigned bits) {
        // Check the dimension and complete payload before allocating based on file contents.
        if (!dimSupported(dim) || (bits != 2 && bits != 3))
            throw std::runtime_error("Invalid TurboQuant fitted dimension");
        uint32_t header[5]{};
        uint64_t seed = 0;
        in.read(reinterpret_cast<char *>(header), sizeof(header));
        in.read(reinterpret_cast<char *>(&seed), sizeof(seed));
        const auto kind = static_cast<TurboQuantRotationKind>(header[0]);
        // Schema 1 (dense) loads at any dimension; schema 2 only where a new index builds it.
        if (!in ||
            (kind != TurboQuantRotationKind::Dense &&
             !(kind == TurboQuantRotationKind::BlockHadamard &&
               TurboQuantRotation::kindFor(dim) == kind)) ||
            header[1] != bits || header[2] != 1 || header[3] != 3 || header[4] != 1)
            throw std::runtime_error("Unsupported TurboQuant fitted policy");
        const auto position = in.tellg();
        in.seekg(0, std::ios::end);
        const auto end = in.tellg();
        in.seekg(position);
        const uint64_t rotation_bytes = kind == TurboQuantRotationKind::Dense
                                            ? sizeof(uint64_t) + uint64_t(dim) * dim * sizeof(float)
                                            : TurboQuantRotation::minimumBlockHadamardBytes(dim);
        const uint64_t minimum = rotation_bytes + 3 * sizeof(uint64_t) + sizeof(float) +
                                 ((uint64_t(1) << bits) + 2 * dim) * sizeof(float);
        if (!in || position < 0 || end < position || uint64_t(end - position) < minimum)
            throw std::runtime_error("Truncated TurboQuant fitted state");
        vecsim_stl::vector<float> zero(dim, 0.0f, allocator);
        auto codec = std::allocate_shared<TurboQuant>(
            VecsimSTLAllocator<TurboQuant>(allocator), allocator, dim, bits, seed,
            std::span<const float>(zero.data(), zero.size()), false);
        auto read = [&](auto &values) {
            uint64_t count = 0;
            in.read(reinterpret_cast<char *>(&count), sizeof(count));
            if (!in || count != values.size())
                throw std::runtime_error("TurboQuant fitted length mismatch");
            in.read(reinterpret_cast<char *>(values.data()), count * sizeof(float));
            if (!in)
                throw std::runtime_error("Truncated TurboQuant fitted state");
            for (float value : values)
                if (!std::isfinite(value))
                    throw std::runtime_error("Nonfinite TurboQuant fitted state");
        };
        if (kind == TurboQuantRotationKind::Dense)
            read(codec->rotation_.resetDense(dim));
        else
            codec->rotation_.loadBlockHadamard(in, dim);
        read(codec->centroids_);
        read(codec->bias_mean_);
        read(codec->bias_rotated_mean_);
        in.read(reinterpret_cast<char *>(&codec->bias_mean_norm_squared_), sizeof(float));
        if (!in || !std::isfinite(codec->bias_mean_norm_squared_))
            throw std::runtime_error("Invalid TurboQuant fitted mean norm");
        for (size_t i = 0; i < codec->centroids_.size(); ++i)
            if (std::abs(codec->centroids_[i]) > 1 ||
                (i && codec->centroids_[i] <= codec->centroids_[i - 1]))
                throw std::runtime_error("Invalid TurboQuant fitted centroids");
        // Validate the relationship between persisted derived terms and the exact stored mean.
        vecsim_stl::vector<float> rotated(dim, allocator), scratch(dim, allocator);
        float norm_squared = 0;
        codec->computeMeanTerms(codec->biasMean(), rotated.data(), norm_squared, scratch.data());
        if (norm_squared != codec->bias_mean_norm_squared_ ||
            !std::equal(rotated.begin(), rotated.end(), codec->bias_rotated_mean_.begin()))
            throw std::runtime_error("Inconsistent TurboQuant fitted mean terms");
        const auto dense = codec->rotation_.dense();
        for (size_t row = 0; row < dim && !dense.empty(); ++row) {
            double squared = 0;
            for (size_t col = 0; col < dim; ++col) {
                const double value = dense[row * dim + col];
                squared += value * value;
            }
            if (std::abs(squared - 1.0) > 1e-4)
                throw std::runtime_error("Invalid TurboQuant fitted rotation");
        }
        return codec;
    }
#endif

    size_t dim() const { return dim_; }
    unsigned bits() const { return bits_; }
    uint64_t seed() const { return seed_; }
    // The dense row-major dim x dim matrix; empty for a block Hadamard rotation.
    std::span<const float> rotation() const { return rotation_.dense(); }
    const TurboQuantRotation &rotationOperator() const { return rotation_; }
    std::span<const float> centroids() const { return {centroids_.data(), centroids_.size()}; }
    std::span<const float> biasMean() const { return {bias_mean_.data(), bias_mean_.size()}; }
    std::span<const float> biasRotatedMean() const {
        return {bias_rotated_mean_.data(), bias_rotated_mean_.size()};
    }
    size_t storageBytes() const { return code_bytes_ + 2 * sizeof(float); }
    size_t queryBytes() const { return (dim_ + 1) * sizeof(float); }
    // Rotation, centroids, mean and rotated mean of a new codec, each in its own allocation(s).
    static size_t sharedDataBytes(size_t dim, unsigned bits) {
        return TurboQuantRotation::sharedBytesFor(dim) +
               ((size_t(1) << bits) + 2 * dim) * sizeof(float);
    }

    // Encodes x - mean. Reads all of `input` before writing `stored`, so they may alias.
    void encode(const float *input, void *stored) const {
        vecsim_stl::vector<float> unit(dim_, allocator_);
        float norm = 0;
        double residual_mean_dot = 0;
        for (size_t i = 0; i < dim_; ++i) {
            unit[i] = input[i] - bias_mean_[i];
            norm = std::hypot(norm, unit[i]);
            residual_mean_dot += double(unit[i]) * bias_mean_[i];
        }
        auto *bytes = static_cast<unsigned char *>(stored);
        std::memset(bytes, 0, storageBytes());
        if (norm == 0) {
            return;
        }
        for (size_t i = 0; i < dim_; ++i) {
            unit[i] /= norm;
        }
        const auto dense = rotation_.dense();
        if (rotation_.isBlockHadamard()) {
            vecsim_stl::vector<float> scratch(dim_, allocator_);
            rotation_.apply(unit.data(), unit.data(), scratch.data()); // unit now holds R*unit
        }
        double unit_code_dot = 0, code_mean_dot = 0;
        for (size_t row = 0; row < dim_; ++row) {
            float rotated = 0;
            if (dense.empty()) {
                rotated = unit[row];
            } else {
                for (size_t col = 0; col < dim_; ++col) {
                    rotated += dense[row * dim_ + col] * unit[col];
                }
            }
            unsigned code = 0;
            while (code + 1 < centroids_.size() &&
                   rotated > (centroids_[code] + centroids_[code + 1]) * 0.5f) {
                ++code;
            }
            writeCode(bytes, row, code);
            code_mean_dot += double(centroids_[code]) * bias_rotated_mean_[row];
            unit_code_dot += double(rotated) * centroids_[code];
        }
        const float scale = static_cast<float>(double(norm) / unit_code_dot);
        writeFloat(bytes + code_bytes_, scale);
        writeFloat(bytes + code_bytes_ + sizeof(float),
                   static_cast<float>(double(scale) * code_mean_dot - residual_mean_dot));
    }

    // Writes [R*q][q.mean] into an FP32-aligned buffer that does not alias `input`.
    void prepareQuery(const float *input, void *query) const {
        auto *output = static_cast<float *>(query);
        if (rotation_.isBlockHadamard()) {
            vecsim_stl::vector<float> scratch(dim_, allocator_);
            rotation_.apply(input, output, scratch.data());
        }
        const auto dense = rotation_.dense();
        for (size_t row = 0; row < dim_ && !dense.empty(); ++row) {
            float value = 0;
            if (row_dot_) {
                value = row_dot_(dense.data() + row * dim_, input, dim_);
            } else {
                for (size_t col = 0; col < dim_; ++col) {
                    value += dense[row * dim_ + col] * input[col];
                }
            }
            output[row] = value;
        }
        float mean_ip = 0;
        for (size_t i = 0; i < dim_; ++i)
            mean_ip += bias_mean_[i] * input[i];
        output[dim_] = mean_ip;
    }

    // Stored-to-query estimate of 1 - x.q. Stored buffers may be unaligned.
    float queryDistance(const void *stored, const void *query) const {
        const auto *bytes = static_cast<const unsigned char *>(stored);
        const auto *query_bytes = static_cast<const unsigned char *>(query);
        const float mean_ip = readFloat(query_bytes + dim_ * sizeof(float));
        const float norm = readFloat(bytes + code_bytes_);
        if (norm == 0) {
            return 1.0f - mean_ip;
        }
        const float score = 1.0f - norm * packed_dot_(bytes, query_bytes, centroids_.data(), dim_);
        return (score + biasOffset(stored)) - mean_ip;
    }

    // Symmetric graph estimate of 1 - x.y. Codes represent x - mean, so both cross terms and
    // the original-coordinate mean norm belong in the inner product.
    float storedDistance(const void *a, const void *b) const {
        const auto *bytes_a = static_cast<const unsigned char *>(a);
        const auto *bytes_b = static_cast<const unsigned char *>(b);
        const float norm_a = readFloat(bytes_a + code_bytes_);
        const float norm_b = readFloat(bytes_b + code_bytes_);
        const float dot = stored_pair_dot_(bytes_a, bytes_b, centroids_.data(), dim_, norm_a,
                                           norm_b, bias_rotated_mean_.data());
        const float score = 1.0f - (dot + bias_mean_norm_squared_);
        return score + (biasOffset(a) + biasOffset(b));
    }
};

} // namespace vecsim::quantization

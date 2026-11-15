/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include <cassert>
#include <memory>
#include <type_traits>

#include "vecsim/quantization/turboquant.h"
#include "vecsim/spaces/computer/calculator.h"
#include "vecsim/spaces/computer/preprocessor_container.h"
#include "vecsim/vec_sim_index.h"

namespace vecsim::quantization {

inline bool isTurboQuant(VecSimQuantType type) {
    return type == VecSimQuant_TQ2 || type == VecSimQuant_TQ3;
}
inline unsigned turboQuantBits(VecSimQuantType type) {
    assert(isTurboQuant(type));
    return type == VecSimQuant_TQ2 ? 2 : 3;
}

// FP32 input only. Storage holds the encoded x - mean; queries hold [R*q][q.mean].
class TurboQuantPreprocessor : public PreprocessorInterface {
    std::shared_ptr<TurboQuant> codec_;

public:
    TurboQuantPreprocessor(std::shared_ptr<VecSimAllocator> allocator,
                           std::shared_ptr<TurboQuant> codec)
        : PreprocessorInterface(allocator), codec_(std::move(codec)) {}

    void preprocess(const void *original_blob, void *&storage_blob, void *&query_blob,
                    size_t &storage_blob_size, size_t &query_blob_size,
                    unsigned char storage_alignment, unsigned char query_alignment) const override {
        assert(!storage_blob && !query_blob && &storage_blob_size != &query_blob_size);
        preprocessForStorage(original_blob, storage_blob, storage_blob_size, storage_alignment);
        preprocessQuery(original_blob, query_blob, query_blob_size, query_alignment);
    }
    void preprocessForStorage(const void *original_blob, void *&blob, size_t &input_blob_size,
                              unsigned char alignment) const override {
        assert(!blob);
        blob = this->allocator->allocate_aligned(codec_->storageBytes(), alignment);
        codec_->encode(static_cast<const float *>(original_blob), blob);
        input_blob_size = codec_->storageBytes();
    }
    void preprocessQuery(const void *original_blob, void *&blob, size_t &input_blob_size,
                         unsigned char alignment) const override {
        assert(!blob);
        blob = this->allocator->allocate_aligned(codec_->queryBytes(), alignment);
        codec_->prepareQuery(static_cast<const float *>(original_blob), blob);
        input_blob_size = codec_->queryBytes();
    }
    void preprocessStorageInPlace(void *original_blob, size_t input_blob_size) const override {
        assert(input_blob_size >= codec_->storageBytes());
        codec_->encode(static_cast<const float *>(original_blob), original_blob);
    }
};

// Shares the codec with the preprocessor. Dispatches capture the codec address, which stays
// stable when the tiered index installs its fitted mean.
class TurboQuantCalculator : public IndexCalculatorInterface<float> {
    std::shared_ptr<TurboQuant> codec_;

    static float stored(const void *codec, const void *a, const void *b, size_t) {
        return static_cast<const TurboQuant *>(codec)->storedDistance(a, b);
    }
    static float query(const void *codec, const void *stored, const void *query, size_t) {
        return static_cast<const TurboQuant *>(codec)->queryDistance(stored, query);
    }

public:
    TurboQuantCalculator(std::shared_ptr<VecSimAllocator> allocator,
                         std::shared_ptr<TurboQuant> codec)
        : IndexCalculatorInterface<float>(allocator), codec_(std::move(codec)) {}

    TurboQuant &codec() const { return *codec_; }

    float calcDistance(const void *v1, const void *v2, size_t dim) const override {
        return stored(codec_.get(), v1, v2, dim);
    }
    float calcDistanceForQuery(const void *candidate, const void *query_blob,
                               size_t dim) const override {
        return query(codec_.get(), candidate, query_blob, dim);
    }
    DistanceDispatch<float> getDistanceDispatch(DistanceMode mode) const override {
        return DistanceDispatch<float>::stateful(
            codec_.get(), mode == DistanceMode::StoredToStored ? stored : query);
    }
};

// Returns the codec of a TQ2/TQ3 index, or nullptr for any other calculator.
template <typename DistType>
TurboQuant *GetTurboQuantCodec(const IndexCalculatorInterface<DistType> *calculator) {
    if constexpr (std::is_same_v<DistType, float>) {
        const auto *tq = dynamic_cast<const TurboQuantCalculator *>(calculator);
        return tq ? &tq->codec() : nullptr;
    }
    return nullptr;
}

inline IndexComponents<float, float>
CreateTurboQuantIndexComponents(const std::shared_ptr<VecSimAllocator> &allocator,
                                std::shared_ptr<TurboQuant> codec) {
    auto *calculator = new (allocator) TurboQuantCalculator(allocator, codec);
    auto *container = new (allocator)
        MultiPreprocessorsContainer<float, 1>(allocator, alignof(float), /*storage*/ 0);
    [[maybe_unused]] const int ret =
        container->addPreprocessor(new (allocator) TurboQuantPreprocessor(allocator, codec));
    assert(ret != -1 && "TurboQuant preprocessor was not added correctly");
    return {calculator, container};
}

// NULL params select a zero mean and the default seed. Throws on an unsupported dim or a
// nonfinite mean; the index factory converts that into a NULL index.
inline IndexComponents<float, float>
CreateTurboQuantIndexComponents(const std::shared_ptr<VecSimAllocator> &allocator, size_t dim,
                                unsigned bits, const VecSimTurboQuantParams *params) {
    vecsim_stl::vector<float> mean(dim, 0.0f, allocator);
    if (params && params->mean) {
        std::copy(params->mean, params->mean + dim, mean.begin());
    }
    auto codec = std::allocate_shared<TurboQuant>(
        VecsimSTLAllocator<TurboQuant>(allocator), allocator, dim, bits,
        params ? params->seed : VECSIM_TURBOQUANT_DEFAULT_SEED,
        std::span<const float>(mean.data(), mean.size()));
    return CreateTurboQuantIndexComponents(allocator, std::move(codec));
}

inline size_t EstimateTurboQuantComponentsMemory(size_t dim, unsigned bits) {
    if (!TurboQuant::dimSupported(dim)) {
        throw std::invalid_argument("Unsupported TurboQuant dimension");
    }
    const size_t overhead = VecSimAllocator::getAllocationOverheadSize();
    // Calculator, container and preprocessor objects; the shared codec block (object plus
    // control block); its three float buffers; and the rotation's buffers. Sized for the
    // rotation a new codec builds at this dimension. A test-only reload of a legacy dense
    // (schema 1) state at a block Hadamard dimension allocates the d x d matrix.
    return sizeof(TurboQuantCalculator) + sizeof(MultiPreprocessorsContainer<float, 1>) +
           sizeof(TurboQuantPreprocessor) + sizeof(TurboQuant) +
           sizeof(VecsimSTLAllocator<TurboQuant>) + 4 * sizeof(void *) +
           (7 + TurboQuantRotation::allocationsFor(dim)) * overhead +
           TurboQuant::sharedDataBytes(dim, bits);
}

} // namespace vecsim::quantization

/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include <sstream>
#include <limits>
#include <cmath>
#include "gtest/gtest.h"
#include "VecSim/vec_sim.h"
#include "VecSim/quantization/sq8_training_state.h"
#include "VecSim/spaces/computer/preprocessor_container.h"
#include "VecSim/spaces/computer/preprocessors.h"
#include "VecSim/spaces/computer/calculator.h"
#include "VecSim/spaces/IP_space.h"
#include "VecSim/spaces/L2_space.h"
#include "unit_test_utils.h"
#include "tests_utils.h"

class IndexCalculatorTest : public ::testing::Test {};
namespace dummyCalcultor {

using DummyType = int;
using dummy_dist_func_t = DummyType (*)(int, int);

int dummyDistFunc(int v1, int v2) { return v1 + v2; }
int dummyQueryDistFunc(int candidate, int query) { return candidate - query; }

float commonDistFunc(const void *v1, const void *v2, size_t dim) {
    return *static_cast<const float *>(v1) + *static_cast<const float *>(v2) +
           static_cast<float>(dim);
}

float commonQueryDistFunc(const void *candidate, const void *query, size_t dim) {
    return *static_cast<const float *>(candidate) - *static_cast<const float *>(query) +
           static_cast<float>(dim);
}

struct StatefulDistanceContext {
    float offset;
};

float statefulDistFunc(const void *context, const void *v1, const void *v2, size_t dim) {
    const auto *state = static_cast<const StatefulDistanceContext *>(context);
    return commonDistFunc(v1, v2, dim) + state->offset;
}

template <typename DistType>
class DistanceCalculatorDummy : public DistanceCalculatorInterface<DistType, dummy_dist_func_t> {
public:
    DistanceCalculatorDummy(std::shared_ptr<VecSimAllocator> allocator, dummy_dist_func_t dist_func,
                            dummy_dist_func_t query_dist_func = nullptr)
        : DistanceCalculatorInterface<DistType, dummy_dist_func_t>(allocator, dist_func,
                                                                   query_dist_func) {}

    virtual DistType calcDistance(const void *v1, const void *v2, size_t dim) const {
        int v1_int = *static_cast<const int *>(v1);
        int v2_int = *static_cast<const int *>(v2);
        return this->dist_func(v1_int, v2_int);
    }

    virtual DistType calcDistanceForQuery(const void *candidate_vector, const void *query_vector,
                                          size_t dim) const {
        int candidate_int = *static_cast<const int *>(candidate_vector);
        int query_int = *static_cast<const int *>(query_vector);
        return this->query_dist_func(candidate_int, query_int);
    }

    // Dummy uses a non-standard dist func signature and is not installed in an index.
    DistanceDispatch<DistType> getDistanceDispatch(DistanceMode mode) const override { return {}; }
};

} // namespace dummyCalcultor

TEST(IndexCalculatorTest, TestIndexCalculator) {
    int v1 = 20, v2 = 10;

    std::shared_ptr<VecSimAllocator> allocator = VecSimAllocator::newVecsimAllocator();

    // Test computer with a distance function signature different from dim(v1, v2, dim()).
    using namespace dummyCalcultor;
    auto distance_calculator = DistanceCalculatorDummy<DummyType>(allocator, dummyDistFunc);
    ASSERT_EQ(distance_calculator.calcDistance(&v1, &v2, 0), 30);
    ASSERT_EQ(distance_calculator.calcDistance(&v2, &v1, 0), 30);
    ASSERT_EQ(distance_calculator.calcDistanceForQuery(&v1, &v2, 0), 30);
    ASSERT_EQ(distance_calculator.calcDistanceForQuery(&v2, &v1, 0), 30);

    auto asymmetric_distance_calculator =
        DistanceCalculatorDummy<DummyType>(allocator, dummyDistFunc, dummyQueryDistFunc);
    ASSERT_EQ(asymmetric_distance_calculator.calcDistance(&v1, &v2, 0), 30);
    ASSERT_EQ(asymmetric_distance_calculator.calcDistance(&v2, &v1, 0), 30);
    ASSERT_EQ(asymmetric_distance_calculator.calcDistanceForQuery(&v1, &v2, 0), 10);
    ASSERT_EQ(asymmetric_distance_calculator.calcDistanceForQuery(&v2, &v1, 0), -10);
}

TEST(IndexCalculatorTest, CommonCalculatorExportsStatelessDispatches) {
    auto allocator = VecSimAllocator::newVecsimAllocator();
    float candidate = 20.0f;
    float query = 10.0f;
    constexpr size_t dim = 3;

    DistanceCalculatorCommon<float> symmetric_calculator(allocator, dummyCalcultor::commonDistFunc);
    auto stored_dispatch = symmetric_calculator.getDistanceDispatch(DistanceMode::StoredToStored);
    auto query_dispatch = symmetric_calculator.getDistanceDispatch(DistanceMode::StoredToQuery);
    ASSERT_TRUE(stored_dispatch.isValid());
    ASSERT_EQ(stored_dispatch.stateless_func, dummyCalcultor::commonDistFunc);
    ASSERT_EQ(query_dispatch.stateless_func, dummyCalcultor::commonDistFunc);
    ASSERT_EQ(query_dispatch(&candidate, &query, dim), 33.0f);

    DistanceCalculatorCommon<float> asymmetric_calculator(allocator, dummyCalcultor::commonDistFunc,
                                                          dummyCalcultor::commonQueryDistFunc);
    stored_dispatch = asymmetric_calculator.getDistanceDispatch(DistanceMode::StoredToStored);
    query_dispatch = asymmetric_calculator.getDistanceDispatch(DistanceMode::StoredToQuery);
    ASSERT_EQ(stored_dispatch.stateless_func, dummyCalcultor::commonDistFunc);
    ASSERT_EQ(query_dispatch.stateless_func, dummyCalcultor::commonQueryDistFunc);
    ASSERT_EQ(query_dispatch(&candidate, &query, dim), 13.0f);
}

TEST(IndexCalculatorTest, StatefulDispatchUsesCachedContext) {
    dummyCalcultor::StatefulDistanceContext context{.offset = 7.0f};
    auto dispatch = DistanceDispatch<float>::stateful(&context, dummyCalcultor::statefulDistFunc);
    float v1 = 20.0f;
    float v2 = 10.0f;

    ASSERT_TRUE(dispatch.isValid());
    ASSERT_EQ(dispatch.stateless_func, nullptr);
    ASSERT_EQ(dispatch.context, &context);
    ASSERT_EQ(dispatch(&v1, &v2, 3), 40.0f);
}

class PreprocessorsTest : public ::testing::Test {};

namespace dummyPreprocessors {

using DummyType = int;

enum pp_mode { STORAGE_ONLY, QUERY_ONLY, BOTH, EMPTY };

// Dummy storage preprocessor
template <typename DataType>
class DummyStoragePreprocessor : public PreprocessorInterface {
public:
    DummyStoragePreprocessor(std::shared_ptr<VecSimAllocator> allocator,
                             DataType value_to_add_storage, DataType value_to_add_query = 0)
        : PreprocessorInterface(allocator), value_to_add_storage(value_to_add_storage),
          value_to_add_query(value_to_add_query) {
        if (!value_to_add_query)
            value_to_add_query = value_to_add_storage;
    }
    void preprocess(const void *original_blob, void *&storage_blob, void *&query_blob,
                    size_t &storage_blob_size, size_t &query_blob_size,
                    unsigned char storage_alignment, unsigned char query_alignment) const override {
        assert(storage_blob_size == query_blob_size);
        this->preprocessForStorage(original_blob, storage_blob, storage_blob_size,
                                   storage_alignment);
    }

    void preprocessForStorage(const void *original_blob, void *&blob, size_t &input_blob_size,
                              unsigned char storage_alignment) const override {
        // If the blob was not allocated yet, allocate it.
        if (blob == nullptr) {
            blob = this->allocator->allocate(input_blob_size);
            memcpy(blob, original_blob, input_blob_size);
        }
        static_cast<DataType *>(blob)[0] += value_to_add_storage;
    }

    void preprocessStorageInPlace(void *blob, size_t input_blob_size) const override {
        assert(blob);
        static_cast<DataType *>(blob)[0] += value_to_add_storage;
    }

    void preprocessQuery(const void *original_blob, void *&blob, size_t &input_blob_size,
                         unsigned char alignment) const override {
        /* do nothing*/
    }

private:
    DataType value_to_add_storage;
    DataType value_to_add_query;
};

// Dummy query preprocessor
template <typename DataType>
class DummyQueryPreprocessor : public PreprocessorInterface {
public:
    DummyQueryPreprocessor(std::shared_ptr<VecSimAllocator> allocator,
                           DataType value_to_add_storage, DataType _value_to_add_query = 0)
        : PreprocessorInterface(allocator), value_to_add_storage(value_to_add_storage),
          value_to_add_query(_value_to_add_query) {
        if (!_value_to_add_query)
            value_to_add_query = value_to_add_storage;
    }

    void preprocess(const void *original_blob, void *&storage_blob, void *&query_blob,
                    size_t &storage_blob_size, size_t &query_blob_size,
                    unsigned char storage_alignment, unsigned char query_alignment) const override {
        assert(storage_blob_size == query_blob_size);
        this->preprocessQuery(original_blob, query_blob, query_blob_size, query_alignment);
    }

    void preprocessForStorage(const void *original_blob, void *&blob, size_t &input_blob_size,
                              unsigned char storage_alignment) const override {
        /* do nothing*/
    }

    void preprocessStorageInPlace(void *blob, size_t input_blob_size) const override {}
    void preprocessQuery(const void *original_blob, void *&blob, size_t &input_blob_size,
                         unsigned char alignment) const override {
        // If the blob was not allocated yet, allocate it.
        if (blob == nullptr) {
            blob = this->allocator->allocate_aligned(input_blob_size, alignment);
            memcpy(blob, original_blob, input_blob_size);
        }
        static_cast<DataType *>(blob)[0] += value_to_add_query;
    }

private:
    DataType value_to_add_storage;
    DataType value_to_add_query;
};

// Dummy mixed preprocessor (precesses the blobs  differently)
template <typename DataType>
class DummyMixedPreprocessor : public PreprocessorInterface {
public:
    DummyMixedPreprocessor(std::shared_ptr<VecSimAllocator> allocator,
                           DataType value_to_add_storage, DataType value_to_add_query)
        : PreprocessorInterface(allocator), value_to_add_storage(value_to_add_storage),
          value_to_add_query(value_to_add_query) {}
    void preprocess(const void *original_blob, void *&storage_blob, void *&query_blob,
                    size_t &storage_blob_size, size_t &query_blob_size,
                    unsigned char storage_alignment, unsigned char query_alignment) const override {
        assert(storage_blob_size == query_blob_size);

        // One blob was already allocated by a previous preprocessor(s) that process both blobs the
        // same. The blobs are pointing to the same memory, we need to allocate another memory slot
        // to split them.
        if ((storage_blob == query_blob) && (query_blob != nullptr)) {
            storage_blob = this->allocator->allocate(storage_blob_size);
            memcpy(storage_blob, query_blob, storage_blob_size);
        }

        // Either both are nullptr or they are pointing to different memory slots. Both cases are
        // handled by the designated functions.
        this->preprocessForStorage(original_blob, storage_blob, storage_blob_size,
                                   storage_alignment);
        this->preprocessQuery(original_blob, query_blob, query_blob_size, query_alignment);
    }

    void preprocessForStorage(const void *original_blob, void *&blob, size_t &input_blob_size,
                              unsigned char storage_alignment) const override {
        // If the blob was not allocated yet, allocate it.
        if (blob == nullptr) {
            blob = this->allocator->allocate(input_blob_size);
            memcpy(blob, original_blob, input_blob_size);
        }
        static_cast<DataType *>(blob)[0] += value_to_add_storage;
    }

    void preprocessStorageInPlace(void *blob, size_t input_blob_size) const override {}
    void preprocessQuery(const void *original_blob, void *&blob, size_t &input_blob_size,
                         unsigned char alignment) const override {
        // If the blob was not allocated yet, allocate it.
        if (blob == nullptr) {
            blob = this->allocator->allocate_aligned(input_blob_size, alignment);
            memcpy(blob, original_blob, input_blob_size);
        }
        static_cast<DataType *>(blob)[0] += value_to_add_query;
    }

private:
    DataType value_to_add_storage;
    DataType value_to_add_query;
};

// A preprocessor that changes the allocation size of the blobs in the same manner.
// set excess bytes to (char)2
template <typename DataType>
class DummyChangeAllocSizePreprocessor : public PreprocessorInterface {
private:
    size_t processed_bytes_count;
    static constexpr unsigned char excess_value = 2;

public:
    DummyChangeAllocSizePreprocessor(std::shared_ptr<VecSimAllocator> allocator,
                                     size_t processed_bytes_count)
        : PreprocessorInterface(allocator), processed_bytes_count(processed_bytes_count) {}

    static constexpr unsigned char getExcessValue() { return excess_value; }

    void preprocess(const void *original_blob, void *&storage_blob, void *&query_blob,
                    size_t &storage_blob_size, size_t &query_blob_size,
                    unsigned char storage_alignment, unsigned char query_alignment) const override {
        // if the blobs are equal, allocate a single shared buffer aligned to satisfy both hints.
        if (storage_blob == query_blob) {
            assert(storage_blob_size == query_blob_size);
            const unsigned char shared_alignment =
                spaces::combineAlignments(storage_alignment, query_alignment);
            preprocessGeneral(original_blob, storage_blob, storage_blob_size, shared_alignment);
            query_blob = storage_blob;
            query_blob_size = storage_blob_size;
            return;
        }
        // The blobs are not equal

        auto alloc_and_process = [&](void *&blob, size_t &input_blob_size,
                                     unsigned char alignment) {
            // If the input blob size is not enough
            if (input_blob_size < processed_bytes_count) {
                auto new_blob = this->allocator->allocate_aligned(processed_bytes_count, alignment);
                if (blob == nullptr) {
                    memcpy(new_blob, original_blob, input_blob_size);
                } else {
                    // copy the blob to the new blob, and free the old one
                    memcpy(new_blob, blob, input_blob_size);
                    this->allocator->free_allocation(blob);
                }
                blob = new_blob;
                memset((char *)blob + input_blob_size, excess_value,
                       processed_bytes_count - input_blob_size);
            } else {
                if (blob == nullptr) {
                    blob = this->allocator->allocate_aligned(processed_bytes_count, alignment);
                    memcpy(blob, original_blob, processed_bytes_count);
                } else {
                    memset((char *)blob + processed_bytes_count, excess_value,
                           input_blob_size - processed_bytes_count);
                }
            }
            input_blob_size = processed_bytes_count;
        };

        alloc_and_process(storage_blob, storage_blob_size, storage_alignment);
        alloc_and_process(query_blob, query_blob_size, query_alignment);
    }

    void preprocessForStorage(const void *original_blob, void *&blob, size_t &input_blob_size,
                              unsigned char storage_alignment) const override {

        this->preprocessGeneral(original_blob, blob, input_blob_size, storage_alignment);
    }

    void preprocessStorageInPlace(void *blob, size_t input_blob_size) const override {
        // only supported if the blob in already large enough
        assert(input_blob_size >= processed_bytes_count);
        // set excess bytes to 0
        memset((char *)blob + processed_bytes_count, excess_value,
               input_blob_size - processed_bytes_count);
    }
    void preprocessQuery(const void *original_blob, void *&blob, size_t &input_blob_size,
                         unsigned char alignment) const override {
        this->preprocessGeneral(original_blob, blob, input_blob_size, alignment);
    }

private:
    void preprocessGeneral(const void *original_blob, void *&blob, size_t &input_blob_size,
                           unsigned char alignment = 0) const {
        // if the size of the input is not enough.
        if (input_blob_size < processed_bytes_count) {
            // calloc doesn't have an alignment version, so we need to allocate aligned memory and
            // cset the excess bytes to 0.
            auto new_blob = this->allocator->allocate_aligned(processed_bytes_count, alignment);
            if (blob == nullptr) {
                // copy thr original blob
                memcpy(new_blob, original_blob, input_blob_size);
            } else {
                // copy the blob to the new blob
                memcpy(new_blob, blob, input_blob_size);
                this->allocator->free_allocation(blob);
            }
            blob = new_blob;
            // set excess bytes to 0
            memset((char *)blob + input_blob_size, excess_value,
                   processed_bytes_count - input_blob_size);
        } else { // input size is larger than output
            if (blob == nullptr) {
                blob = this->allocator->allocate_aligned(processed_bytes_count, alignment);
                memcpy(blob, original_blob, processed_bytes_count);
            } else {
                // set excess bytes to 0
                memset((char *)blob + processed_bytes_count, excess_value,
                       input_blob_size - processed_bytes_count);
            }
        }
        // update the input blob size
        input_blob_size = processed_bytes_count;
    }
};
} // namespace dummyPreprocessors

TEST(PreprocessorsTest, PreprocessorsTestBasicAlignmentTest) {
    using namespace dummyPreprocessors;
    std::shared_ptr<VecSimAllocator> allocator = VecSimAllocator::newVecsimAllocator();

    unsigned char alignment = 8;
    auto preprocessor = PreprocessorsContainerAbstract(allocator, alignment);
    const int original_blob[4] = {1, 1, 1, 1};
    size_t processed_bytes_count = sizeof(original_blob);

    {
        auto aligned_query = preprocessor.preprocessQuery(original_blob, processed_bytes_count);
        unsigned char address_alignment = (uintptr_t)(aligned_query.get()) % alignment;
        ASSERT_EQ(address_alignment, 0);
    }
}

template <unsigned char alignment>
void MultiPPContainerEmpty() {
    using namespace dummyPreprocessors;
    std::shared_ptr<VecSimAllocator> allocator = VecSimAllocator::newVecsimAllocator();
    constexpr size_t dim = 4;
    const int original_blob[dim] = {1, 2, 3, 4};
    const int original_blob_cpy[dim] = {1, 2, 3, 4};

    constexpr size_t n_preprocessors = 3;

    auto multiPPContainer =
        MultiPreprocessorsContainer<DummyType, n_preprocessors>(allocator, alignment);

    {
        ProcessedBlobs processed_blobs =
            multiPPContainer.preprocess(original_blob, sizeof(original_blob));
        // Original blob should not be changed
        CompareVectors(original_blob, original_blob_cpy, dim);

        const void *storage_blob = processed_blobs.getStorageBlob();
        const void *query_blob = processed_blobs.getQueryBlob();

        // Storage blob should not be reallocated or changed
        ASSERT_EQ(storage_blob, (const int *)original_blob);
        CompareVectors(original_blob, (const int *)storage_blob, dim);

        // query blob *values* should not be changed
        CompareVectors(original_blob, (const int *)query_blob, dim);

        // If alignment is set the query blob address should be aligned to the specified alignment.
        if constexpr (alignment) {
            unsigned char address_alignment = (uintptr_t)(query_blob) % alignment;
            ASSERT_EQ(address_alignment, 0);
        }
    }
}

TEST(PreprocessorsTest, MultiPPContainerEmptyNoAlignment) {
    using namespace dummyPreprocessors;
    MultiPPContainerEmpty<0>();
}

TEST(PreprocessorsTest, MultiPPContainerEmptyAlignment) {
    using namespace dummyPreprocessors;
    MultiPPContainerEmpty<5>();
}

template <typename PreprocessorType>
void MultiPreprocessorsContainerNoAlignment(dummyPreprocessors::pp_mode MODE) {
    using namespace dummyPreprocessors;
    std::shared_ptr<VecSimAllocator> allocator = VecSimAllocator::newVecsimAllocator();

    constexpr size_t n_preprocessors = 2;
    unsigned char alignment = 0;
    int initial_value = 1;
    int value_to_add = 7;
    const int original_blob[4] = {initial_value, initial_value, initial_value, initial_value};
    size_t original_blob_size = sizeof(original_blob);

    // Test computer with multiple preprocessors of the same type.
    auto multiPPContainer =
        MultiPreprocessorsContainer<DummyType, n_preprocessors>(allocator, alignment);

    auto verify_preprocess = [&](int expected_processed_value) {
        ProcessedBlobs processed_blobs =
            multiPPContainer.preprocess(original_blob, original_blob_size);
        // Original blob should not be changed
        ASSERT_EQ(original_blob[0], initial_value);

        const void *storage_blob = processed_blobs.getStorageBlob();
        const void *query_blob = processed_blobs.getQueryBlob();
        if (MODE == STORAGE_ONLY) {
            // New storage blob should be allocated
            ASSERT_NE(storage_blob, original_blob);
            // query blob should be unprocessed
            ASSERT_EQ(query_blob, original_blob);
            ASSERT_EQ(((const int *)storage_blob)[0], expected_processed_value);
        } else if (MODE == QUERY_ONLY) {
            // New query blob should be allocated
            ASSERT_NE(query_blob, original_blob);
            // Storage blob should be unprocessed
            ASSERT_EQ(storage_blob, original_blob);
            ASSERT_EQ(((const int *)query_blob)[0], expected_processed_value);
        }
    };

    /* ==== Add the first preprocessor ==== */
    auto preprocessor0 = new (allocator) PreprocessorType(allocator, value_to_add);
    // add preprocessor returns next free spot in its preprocessors array.
    ASSERT_EQ(multiPPContainer.addPreprocessor(preprocessor0), 1);
    verify_preprocess(initial_value + value_to_add);

    /* ==== Add the second preprocessor ==== */
    auto preprocessor1 = new (allocator) PreprocessorType(allocator, value_to_add);
    // add preprocessor returns 0 when adding the last preprocessor.
    ASSERT_EQ(multiPPContainer.addPreprocessor(preprocessor1), 0);
    ASSERT_NO_FATAL_FAILURE(verify_preprocess(initial_value + 2 * value_to_add));
}

TEST(PreprocessorsTest, MultiPreprocessorsContainerStorageNoAlignment) {
    using namespace dummyPreprocessors;
    MultiPreprocessorsContainerNoAlignment<DummyStoragePreprocessor<DummyType>>(
        pp_mode::STORAGE_ONLY);
}

TEST(PreprocessorsTest, MultiPreprocessorsContainerQueryNoAlignment) {
    using namespace dummyPreprocessors;
    MultiPreprocessorsContainerNoAlignment<DummyQueryPreprocessor<DummyType>>(pp_mode::QUERY_ONLY);
}

template <typename FirstPreprocessorType, typename SecondPreprocessorType>
void multiPPContainerMixedPreprocessorNoAlignment() {
    using namespace dummyPreprocessors;
    std::shared_ptr<VecSimAllocator> allocator = VecSimAllocator::newVecsimAllocator();

    constexpr size_t n_preprocessors = 3;
    unsigned char alignment = 0;
    int initial_value = 1;
    int value_to_add_storage = 7;
    int value_to_add_query = 2;
    const int original_blob[4] = {initial_value, initial_value, initial_value, initial_value};
    size_t original_blob_size = sizeof(original_blob);

    // Test multiple preprocessors of the same type.
    auto multiPPContainer =
        MultiPreprocessorsContainer<DummyType, n_preprocessors>(allocator, alignment);

    /* ==== Add one preprocessor of each type ==== */
    auto preprocessor0 =
        new (allocator) FirstPreprocessorType(allocator, value_to_add_storage, value_to_add_query);
    ASSERT_EQ(multiPPContainer.addPreprocessor(preprocessor0), 1);
    auto preprocessor1 =
        new (allocator) SecondPreprocessorType(allocator, value_to_add_storage, value_to_add_query);
    ASSERT_EQ(multiPPContainer.addPreprocessor(preprocessor1), 2);

    // scope this section so the blobs are released before the allocator.
    {
        ProcessedBlobs processed_blobs =
            multiPPContainer.preprocess(original_blob, original_blob_size);
        // Original blob should not be changed
        ASSERT_EQ(original_blob[0], initial_value);

        // Both blobs should be allocated
        const void *storage_blob = processed_blobs.getStorageBlob();
        const void *query_blob = processed_blobs.getQueryBlob();

        // Ensure the computer process returns a new allocation of the expected processed blob with
        // the new value.
        ASSERT_NE(storage_blob, original_blob);
        ASSERT_NE(query_blob, original_blob);
        ASSERT_NE(query_blob, storage_blob);

        ASSERT_EQ(((const int *)storage_blob)[0], initial_value + value_to_add_storage);
        ASSERT_EQ(((const int *)query_blob)[0], initial_value + value_to_add_query);
    }

    /* ==== Add a preprocessor that processes both storage and query ==== */
    auto preprocessor2 = new (allocator)
        DummyMixedPreprocessor<DummyType>(allocator, value_to_add_storage, value_to_add_query);
    // add preprocessor returns 0 when adding the last preprocessor.
    ASSERT_EQ(multiPPContainer.addPreprocessor(preprocessor2), 0);
    {
        ProcessedBlobs mixed_processed_blobs =
            multiPPContainer.preprocess(original_blob, original_blob_size);

        const void *mixed_pp_storage_blob = mixed_processed_blobs.getStorageBlob();
        const void *mixed_pp_query_blob = mixed_processed_blobs.getQueryBlob();

        // Ensure the computer process both blobs.
        ASSERT_EQ(((const int *)mixed_pp_storage_blob)[0],
                  initial_value + 2 * value_to_add_storage);
        ASSERT_EQ(((const int *)mixed_pp_query_blob)[0], initial_value + 2 * value_to_add_query);
    }

    // try adding another preprocessor and fail.
    ASSERT_EQ(multiPPContainer.addPreprocessor(preprocessor2), -1);
}

TEST(PreprocessorsTest, multiPPContainerMixedPreprocessorQueryFirst) {
    using namespace dummyPreprocessors;
    multiPPContainerMixedPreprocessorNoAlignment<DummyQueryPreprocessor<DummyType>,
                                                 DummyStoragePreprocessor<DummyType>>();
}

TEST(PreprocessorsTest, multiPPContainerMixedPreprocessorStorageFirst) {
    using namespace dummyPreprocessors;
    multiPPContainerMixedPreprocessorNoAlignment<DummyStoragePreprocessor<DummyType>,
                                                 DummyQueryPreprocessor<DummyType>>();
}

template <typename PreprocessorType>
void multiPPContainerAlignment(dummyPreprocessors::pp_mode MODE) {
    using namespace dummyPreprocessors;
    std::shared_ptr<VecSimAllocator> allocator = VecSimAllocator::newVecsimAllocator();

    unsigned char alignment = 8;
    constexpr size_t n_preprocessors = 1;
    int initial_value = 1;
    int value_to_add = 7;
    const int original_blob[4] = {initial_value, initial_value, initial_value, initial_value};
    size_t original_blob_size = sizeof(original_blob);

    auto multiPPContainer =
        MultiPreprocessorsContainer<DummyType, n_preprocessors>(allocator, alignment);

    auto verify_preprocess = [&](int expected_processed_value) {
        ProcessedBlobs processed_blobs =
            multiPPContainer.preprocess(original_blob, original_blob_size);

        const void *storage_blob = processed_blobs.getStorageBlob();
        const void *query_blob = processed_blobs.getQueryBlob();
        if (MODE == STORAGE_ONLY) {
            // New storage blob should be allocated and processed
            ASSERT_NE(storage_blob, original_blob);
            ASSERT_EQ(((const int *)storage_blob)[0], expected_processed_value);
            // query blob *values* should be unprocessed, however, it might be allocated if the
            // original blob is not aligned.
            ASSERT_EQ(((const int *)query_blob)[0], original_blob[0]);
        } else if (MODE == QUERY_ONLY) {
            // New query blob should be allocated
            ASSERT_NE(query_blob, original_blob);
            // Storage blob should be unprocessed and not allocated.
            ASSERT_EQ(storage_blob, original_blob);
            ASSERT_EQ(((const int *)query_blob)[0], expected_processed_value);
        }

        // anyway the query blob should be aligned
        unsigned char address_alignment = (uintptr_t)(query_blob) % alignment;
        ASSERT_EQ(address_alignment, 0);
    };

    auto preprocessor0 = new (allocator) PreprocessorType(allocator, value_to_add);
    // add preprocessor returns next free spot in its preprocessors array.
    ASSERT_EQ(multiPPContainer.addPreprocessor(preprocessor0), 0);
    verify_preprocess(initial_value + value_to_add);
}

TEST(PreprocessorsTest, StoragePreprocessorWithAlignment) {
    using namespace dummyPreprocessors;
    multiPPContainerAlignment<DummyStoragePreprocessor<DummyType>>(pp_mode::STORAGE_ONLY);
}

TEST(PreprocessorsTest, QueryPreprocessorWithAlignment) {
    using namespace dummyPreprocessors;
    multiPPContainerAlignment<DummyQueryPreprocessor<DummyType>>(pp_mode::QUERY_ONLY);
}

TEST(PreprocessorsTest, multiPPContainerCosineThenMixedPreprocess) {
    using namespace dummyPreprocessors;
    std::shared_ptr<VecSimAllocator> allocator = VecSimAllocator::newVecsimAllocator();

    constexpr size_t n_preprocessors = 2;
    constexpr size_t dim = 4;
    unsigned char alignment = 8;

    float initial_value = 1.0f;
    float normalized_value = 0.5f;
    float value_to_add_storage = 7.0f;
    float value_to_add_query = 2.0f;
    const float original_blob[dim] = {initial_value, initial_value, initial_value, initial_value};
    constexpr size_t original_blob_size = sizeof(original_blob);

    auto multiPPContainer =
        MultiPreprocessorsContainer<float, n_preprocessors>(allocator, alignment);

    // adding cosine preprocessor
    auto cosine_preprocessor =
        new (allocator) CosinePreprocessor<float>(allocator, dim, original_blob_size);
    multiPPContainer.addPreprocessor(cosine_preprocessor);
    {
        ProcessedBlobs processed_blobs =
            multiPPContainer.preprocess(original_blob, original_blob_size);
        const void *storage_blob = processed_blobs.getStorageBlob();
        const void *query_blob = processed_blobs.getQueryBlob();
        // blobs should point to the same memory slot
        ASSERT_EQ(storage_blob, query_blob);
        // memory should be aligned
        unsigned char address_alignment = (uintptr_t)(storage_blob) % alignment;
        ASSERT_EQ(address_alignment, 0);
        // They need to be allocated and processed
        ASSERT_NE(storage_blob, nullptr);
        ASSERT_EQ(((const float *)storage_blob)[0], normalized_value);
        // the original blob should not change
        ASSERT_NE(storage_blob, original_blob);
    }
    // adding mixed preprocessor
    auto mixed_preprocessor = new (allocator)
        DummyMixedPreprocessor<float>(allocator, value_to_add_storage, value_to_add_query);
    multiPPContainer.addPreprocessor(mixed_preprocessor);
    {
        ProcessedBlobs processed_blobs =
            multiPPContainer.preprocess(original_blob, original_blob_size);
        const void *storage_blob = processed_blobs.getStorageBlob();
        const void *query_blob = processed_blobs.getQueryBlob();
        // blobs should point to a different memory slot
        ASSERT_NE(storage_blob, query_blob);
        ASSERT_NE(storage_blob, nullptr);
        ASSERT_NE(query_blob, nullptr);

        // query blob should be aligned
        unsigned char address_alignment = (uintptr_t)(query_blob) % alignment;
        ASSERT_EQ(address_alignment, 0);

        // They need to be processed by both processors.
        ASSERT_EQ(((const float *)storage_blob)[0], normalized_value + value_to_add_storage);
        ASSERT_EQ(((const float *)query_blob)[0], normalized_value + value_to_add_query);

        // the original blob should not change
        ASSERT_NE(storage_blob, original_blob);
        ASSERT_NE(query_blob, original_blob);
    }
    // The preprocessors should be released by the preprocessors container.
}

// Cosine pp receives different allocation for the storage and query blobs.
TEST(PreprocessorsTest, multiPPContainerMixedThenCosinePreprocess) {
    using namespace dummyPreprocessors;
    std::shared_ptr<VecSimAllocator> allocator = VecSimAllocator::newVecsimAllocator();

    constexpr size_t n_preprocessors = 2;
    constexpr size_t dim = 4;
    unsigned char alignment = 8;

    // In this test the first preprocessor allocates the memory for both blobs, according to the
    // size passed by the pp container. The second preprocessor expects that if the blobs are
    // already allocated, their size matches its processed_bytes_count. Hence, the blob size should
    // be sufficient for the cosine preprocessing.
    constexpr int8_t normalized_blob_bytes_count = dim * sizeof(int8_t) + sizeof(float);
    auto blob_alloc = allocator->allocate_unique(normalized_blob_bytes_count);
    int8_t *original_blob = static_cast<int8_t *>(blob_alloc.get());
    test_utils::populate_int8_vec(original_blob, dim);

    // Processing params
    int8_t value_to_add_storage = 7;
    int8_t value_to_add_query = 4;
    // Creating multi preprocessors container
    auto mixed_preprocessor = new (allocator)
        DummyMixedPreprocessor<int8_t>(allocator, value_to_add_storage, value_to_add_query);
    auto multiPPContainer =
        MultiPreprocessorsContainer<int8_t, n_preprocessors>(allocator, alignment);
    multiPPContainer.addPreprocessor(mixed_preprocessor);

    {
        ProcessedBlobs processed_blobs =
            multiPPContainer.preprocess(original_blob, normalized_blob_bytes_count);
        const void *storage_blob = processed_blobs.getStorageBlob();
        const void *query_blob = processed_blobs.getQueryBlob();
        // blobs should point to a different memory slot
        ASSERT_NE(storage_blob, query_blob);
        ASSERT_NE(storage_blob, nullptr);
        ASSERT_NE(query_blob, nullptr);

        // query blob should be aligned
        unsigned char address_alignment = (uintptr_t)(query_blob) % alignment;
        ASSERT_EQ(address_alignment, 0);

        // Both blobs were processed.
        ASSERT_EQ((static_cast<const int8_t *>(storage_blob))[0],
                  original_blob[0] + value_to_add_storage);
        ASSERT_EQ((static_cast<const int8_t *>(query_blob))[0],
                  original_blob[0] + value_to_add_query);

        // the original blob should not change
        ASSERT_NE(storage_blob, original_blob);
        ASSERT_NE(query_blob, original_blob);
    }

    // Processed blob expected output
    auto expected_processed_blob = [&](int8_t *blob, int8_t value_to_add) {
        memcpy(blob, original_blob, normalized_blob_bytes_count);
        blob[0] += value_to_add;
        VecSim_Normalize(blob, dim, VecSimType_INT8);
    };

    int8_t expected_processed_storage[normalized_blob_bytes_count] = {0};
    expected_processed_blob(expected_processed_storage, value_to_add_storage);
    int8_t expected_processed_query[normalized_blob_bytes_count] = {0};
    expected_processed_blob(expected_processed_query, value_to_add_query);

    // normalize the original blob
    auto cosine_preprocessor =
        new (allocator) CosinePreprocessor<int8_t>(allocator, dim, normalized_blob_bytes_count);
    multiPPContainer.addPreprocessor(cosine_preprocessor);
    {
        // An assertion should be raised by the cosine preprocessor for unmatching blob sizes.
        // in valgrind the test continues, but the assertion appears in its log looking like an
        // error, so to avoid confusion we skip this line in valgrind.
#if !defined(RUNNING_ON_VALGRIND) && !defined(NDEBUG)
        EXPECT_EXIT(
            {
                ProcessedBlobs processed_blobs = multiPPContainer.preprocess(
                    original_blob, normalized_blob_bytes_count - sizeof(float));
            },
            testing::KilledBySignal(SIGABRT), "blob_size == processed_bytes_count");
#endif
        // Use the correct size
        ProcessedBlobs processed_blobs =
            multiPPContainer.preprocess(original_blob, normalized_blob_bytes_count);
        const void *storage_blob = processed_blobs.getStorageBlob();
        const void *query_blob = processed_blobs.getQueryBlob();
        // blobs should point to a different memory slot
        ASSERT_NE(storage_blob, query_blob);
        // query memory should be aligned
        unsigned char address_alignment = (uintptr_t)(query_blob) % alignment;
        ASSERT_EQ(address_alignment, 0);
        // They need to be allocated and processed
        ASSERT_NE(storage_blob, nullptr);
        ASSERT_NE(query_blob, nullptr);
        // the original blob should not change
        ASSERT_NE(storage_blob, original_blob);
        ASSERT_NE(query_blob, original_blob);

        EXPECT_NO_FATAL_FAILURE(CompareVectors<int8_t>(static_cast<const int8_t *>(storage_blob),
                                                       expected_processed_storage,
                                                       normalized_blob_bytes_count));
        EXPECT_NO_FATAL_FAILURE(CompareVectors<int8_t>(static_cast<const int8_t *>(query_blob),
                                                       expected_processed_query,
                                                       normalized_blob_bytes_count));
    }
    // The preprocessors should be released by the preprocessors container.
}

template <typename PreprocessorType>
void AsymmetricPPThenCosine(dummyPreprocessors::pp_mode MODE) {
    using namespace dummyPreprocessors;

    std::shared_ptr<VecSimAllocator> allocator = VecSimAllocator::newVecsimAllocator();

    constexpr size_t n_preprocessors = 2;
    constexpr size_t dim = 4;
    unsigned char alignment = 8;

    float original_blob[dim] = {0};
    constexpr size_t original_blob_size = dim * sizeof(float);
    test_utils::populate_float_vec(original_blob, dim);

    // Processing params
    float value_to_add_storage = 0;
    float value_to_add_query = 0;
    if (MODE == STORAGE_ONLY) {
        value_to_add_storage = 7;
    } else if (MODE == QUERY_ONLY) {
        value_to_add_query = 4;
    }

    // Processed blob expected output
    auto expected_processed_blob = [&](float *blob, float value_to_add) {
        memcpy(blob, original_blob, original_blob_size);
        blob[0] += value_to_add;
        VecSim_Normalize(blob, dim, VecSimType_FLOAT32);
    };

    float expected_processed_storage[dim] = {0};
    expected_processed_blob(expected_processed_storage, value_to_add_storage);
    float expected_processed_query[dim] = {0};
    expected_processed_blob(expected_processed_query, value_to_add_query);

    auto multiPPContainer =
        MultiPreprocessorsContainer<float, n_preprocessors>(allocator, alignment);
    // will allocate either the storage or the query blob, depending on the preprocessor type.
    // TODO : can we pass just value_to_add?
    auto asymmetric_preprocessor =
        new (allocator) PreprocessorType(allocator, value_to_add_storage, value_to_add_query);
    auto cosine_preprocessor =
        new (allocator) CosinePreprocessor<float>(allocator, dim, original_blob_size);

    multiPPContainer.addPreprocessor(asymmetric_preprocessor);
    multiPPContainer.addPreprocessor(cosine_preprocessor);

    {
        ProcessedBlobs processed_blobs =
            multiPPContainer.preprocess(original_blob, original_blob_size);
        const void *storage_blob = processed_blobs.getStorageBlob();
        const void *query_blob = processed_blobs.getQueryBlob();
        // blobs should point to a different memory slot
        ASSERT_NE(storage_blob, query_blob);
        ASSERT_NE(storage_blob, nullptr);
        ASSERT_NE(query_blob, nullptr);

        // query blob should be aligned
        unsigned char address_alignment = (uintptr_t)(query_blob) % alignment;
        ASSERT_EQ(address_alignment, 0);

        EXPECT_NO_FATAL_FAILURE(CompareVectors<float>(static_cast<const float *>(storage_blob),
                                                      expected_processed_storage, dim));
        EXPECT_NO_FATAL_FAILURE(CompareVectors<float>(static_cast<const float *>(query_blob),
                                                      expected_processed_query, dim));
    }
}
// The first preprocessor in the chain allocates only the storage blob,
// and the responsibility of allocating the query blob is delegated to the next preprocessor
// in the chain (cosine preprocessor in this case).
TEST(PreprocessorsTest, STORAGE_then_cosine_no_size_change) {
    using namespace dummyPreprocessors;
    EXPECT_NO_FATAL_FAILURE(
        AsymmetricPPThenCosine<DummyStoragePreprocessor<float>>(pp_mode::STORAGE_ONLY));
}

TEST(PreprocessorsTest, QUERY_then_cosine_no_size_change) {
    using namespace dummyPreprocessors;
    EXPECT_NO_FATAL_FAILURE(
        AsymmetricPPThenCosine<DummyQueryPreprocessor<float>>(pp_mode::QUERY_ONLY));
}

// A test where the value of input_blob_size is modified by the first pp.
// The cosine pp processed_bytes_count should be set at initialization with the *modified* value,
// otherwise, an assertion will be raised by it for an allocated blob that is smaller than the
// expected size.
TEST(PreprocessorsTest, DecreaseSizeThenFloatNormalize) {
    using namespace dummyPreprocessors;
    std::shared_ptr<VecSimAllocator> allocator = VecSimAllocator::newVecsimAllocator();

    constexpr size_t n_preprocessors = 2;
    constexpr size_t alignment = 8;
    constexpr size_t elements = 8;
    constexpr size_t decrease_amount = 2;
    constexpr size_t new_elem_amount = elements - decrease_amount;

    // valgrind detects out of bound reads only if the considered memory is allocated on the heap,
    // rather than on the stack.
    constexpr size_t original_blob_size = elements * sizeof(float);
    auto original_blob_alloc = allocator->allocate_unique(original_blob_size);
    float *original_blob = static_cast<float *>(original_blob_alloc.get());
    test_utils::populate_float_vec(original_blob, elements);
    constexpr size_t new_processed_bytes_count =
        original_blob_size - decrease_amount * sizeof(float);

    // Processed blob expected output
    float expected_processed_blob[elements] = {0};
    memcpy(expected_processed_blob, original_blob, new_processed_bytes_count);
    // Only use half of the blob for normalization
    VecSim_Normalize(expected_processed_blob, new_elem_amount, VecSimType_FLOAT32);

    // A pp that decreases the original blob size
    auto decrease_size_preprocessor = new (allocator)
        DummyChangeAllocSizePreprocessor<float>(allocator, new_processed_bytes_count);
    // A normalize pp
    auto cosine_preprocessor = new (allocator)
        CosinePreprocessor<float>(allocator, new_elem_amount, new_processed_bytes_count);
    // Creating multi preprocessors container
    auto multiPPContainer =
        MultiPreprocessorsContainer<float, n_preprocessors>(allocator, alignment);
    multiPPContainer.addPreprocessor(decrease_size_preprocessor);
    multiPPContainer.addPreprocessor(cosine_preprocessor);

    {
        ProcessedBlobs processed_blobs =
            multiPPContainer.preprocess(original_blob, original_blob_size);
        const void *storage_blob = processed_blobs.getStorageBlob();
        const void *query_blob = processed_blobs.getQueryBlob();
        // blobs should point to the same memory slot
        ASSERT_EQ(storage_blob, query_blob);
        ASSERT_NE(storage_blob, nullptr);

        // memory should be aligned
        unsigned char address_alignment = (uintptr_t)(query_blob) % alignment;
        ASSERT_EQ(address_alignment, 0);

        // They need to be allocated and processed
        EXPECT_NO_FATAL_FAILURE(CompareVectors<float>(static_cast<const float *>(storage_blob),
                                                      expected_processed_blob, new_elem_amount));
    }
}

// In this test the original blob size is smaller than the processed_bytes_count of the
// cosine preprocessor. Before the bug fix, the cosine pp would try to copy processed_bytes_count
// bytes of the original blob to the allocated memory, which would cause an out of bound read,
// that should be detected by valgrind.
TEST(PreprocessorsTest, Int8NormalizeThenIncreaseSize) {
    using namespace dummyPreprocessors;
    std::shared_ptr<VecSimAllocator> allocator = VecSimAllocator::newVecsimAllocator();

    constexpr size_t n_preprocessors = 2;
    constexpr size_t alignment = 8;
    constexpr size_t elements = 7;

    // valgrind detects out of bound reads only if the considered memory is allocated on the heap,
    // rather than on the stack.
    constexpr size_t original_blob_size = elements * sizeof(int8_t);
    auto original_blob_alloc = allocator->allocate_unique(original_blob_size);
    int8_t *original_blob = static_cast<int8_t *>(original_blob_alloc.get());
    test_utils::populate_int8_vec(original_blob, elements);
    // size after normalization
    constexpr size_t normalized_blob_bytes_count = original_blob_size + sizeof(float);
    // size after increasing pp
    constexpr size_t elements_addition = 3;
    constexpr size_t final_blob_bytes_count =
        normalized_blob_bytes_count + elements_addition * sizeof(unsigned char);

    // Processed blob expected output
    int8_t expected_processed_blob[elements + sizeof(float) + elements_addition] = {0};
    memcpy(expected_processed_blob, original_blob, original_blob_size);
    // normalize the original blob
    VecSim_Normalize(expected_processed_blob, elements, VecSimType_INT8);
    // add the values of the pp that increases the blob size
    unsigned char added_value = DummyChangeAllocSizePreprocessor<int8_t>::getExcessValue();
    for (size_t i = 0; i < elements_addition; i++) {
        expected_processed_blob[elements + sizeof(float) + i] = static_cast<int8_t>(added_value);
    }

    // A normalize pp - will allocate the blob + sizeof(float)
    auto cosine_preprocessor = new (allocator)
        CosinePreprocessor<int8_t>(allocator, elements, normalized_blob_bytes_count);
    // A pp that will increase the *normalized* blob size
    auto increase_size_preprocessor =
        new (allocator) DummyChangeAllocSizePreprocessor<int8_t>(allocator, final_blob_bytes_count);
    // Creating multi preprocessors container
    auto multiPPContainer =
        MultiPreprocessorsContainer<DummyType, n_preprocessors>(allocator, alignment);
    multiPPContainer.addPreprocessor(cosine_preprocessor);
    multiPPContainer.addPreprocessor(increase_size_preprocessor);

    {
        // Note: we pass the original blob size to detect out of bound reads in the cosine pp.
        ProcessedBlobs processed_blobs =
            multiPPContainer.preprocess(original_blob, original_blob_size);
        const void *storage_blob = processed_blobs.getStorageBlob();
        const void *query_blob = processed_blobs.getQueryBlob();
        // blobs should point to the same memory slot
        ASSERT_EQ(storage_blob, query_blob);
        ASSERT_NE(storage_blob, nullptr);

        // memory should be aligned
        unsigned char address_alignment = (uintptr_t)(query_blob) % alignment;
        ASSERT_EQ(address_alignment, 0);

        // They need to be allocated and processed
        EXPECT_NO_FATAL_FAILURE(CompareVectors<int8_t>(static_cast<const int8_t *>(storage_blob),
                                                       expected_processed_blob,
                                                       final_blob_bytes_count));
    }
}

// Tests the quantization preprocessor with a single preprocessor in the chain.
// The QuantPreprocessor allocates and processes both the storage blob (quantized) and the query
// blob (original values + precomputed sum_squares for L2).
TEST(PreprocessorsTest, QuantizationTest) {
    std::shared_ptr<VecSimAllocator> allocator = VecSimAllocator::newVecsimAllocator();
    constexpr size_t n_preprocessors = 1;
    constexpr size_t alignment = 8;
    constexpr size_t dim = 6;
    constexpr size_t original_blob_size = dim * sizeof(float);
    float original_blob[dim] = {1, 2, 3, 4, 5, 6};

    // === Storage blob expected values ===
    // For L2 metric: quantized values + min + delta + sum + sum_squares = dim bytes + 4 floats
    constexpr size_t quantized_blob_bytes_count =
        dim * sizeof(uint8_t) + sq8::storage_metadata_count<VecSimMetric_L2>() * sizeof(float);
    uint8_t expected_storage_blob[quantized_blob_bytes_count] = {0};
    ComputeSQ8Quantization(original_blob, dim, expected_storage_blob);

    // === Query blob expected values ===
    // Query layout: | query_values[dim] | y_sum_squares (for L2) |
    constexpr size_t query_blob_bytes_count =
        (dim + sq8::query_metadata_count<VecSimMetric_L2>()) * sizeof(float);

    // Compute expected sum and sum of squares for L2:
    float expected_query_sum = 0;
    float expected_query_sum_squares = 0;
    for (size_t i = 0; i < dim; ++i) {
        expected_query_sum += original_blob[i];
        expected_query_sum_squares += original_blob[i] * original_blob[i];
    }

    auto quant_preprocessor =
        new (allocator) QuantPreprocessor<float, VecSimMetric_L2>(allocator, dim);
    auto multiPPContainer =
        MultiPreprocessorsContainer<float, n_preprocessors>(allocator, alignment);
    multiPPContainer.addPreprocessor(quant_preprocessor);

    // Test preprocess (both storage and query)
    {
        ProcessedBlobs processed_blobs =
            multiPPContainer.preprocess(original_blob, original_blob_size);
        const void *storage_blob = processed_blobs.getStorageBlob();
        const void *query_blob = processed_blobs.getQueryBlob();

        // Verify storage and query blobs are separate
        ASSERT_NE(storage_blob, nullptr);
        ASSERT_NE(query_blob, nullptr);
        ASSERT_NE(storage_blob, query_blob);

        // Verify storage blob content
        EXPECT_NO_FATAL_FAILURE(CompareVectors<uint8_t>(static_cast<const uint8_t *>(storage_blob),
                                                        expected_storage_blob,
                                                        quantized_blob_bytes_count));

        // Verify query blob content
        const float *query_floats = static_cast<const float *>(query_blob);
        EXPECT_NO_FATAL_FAILURE(CompareVectors<float>(query_floats, original_blob, dim));
        ASSERT_FLOAT_EQ(query_floats[dim + sq8::SUM_QUERY], expected_query_sum);
        ASSERT_FLOAT_EQ(query_floats[dim + sq8::SUM_SQUARES_QUERY], expected_query_sum_squares);
    }

    // Test preprocessForStorage
    {
        auto storage_blob =
            multiPPContainer.preprocessForStorage(original_blob, original_blob_size);

        ASSERT_NE(storage_blob.get(), nullptr);
        EXPECT_NO_FATAL_FAILURE(
            CompareVectors<uint8_t>(static_cast<const uint8_t *>(storage_blob.get()),
                                    expected_storage_blob, quantized_blob_bytes_count));
    }

    // Test preprocessQuery (query values + precomputed sum_squares for L2)
    {
        auto query_blob = multiPPContainer.preprocessQuery(original_blob, original_blob_size);
        ASSERT_NE(query_blob.get(), nullptr);

        // Verify query blob content: original floats followed by sum_squares
        const float *query_floats = static_cast<const float *>(query_blob.get());
        EXPECT_NO_FATAL_FAILURE(CompareVectors<float>(query_floats, original_blob, dim));
        ASSERT_FLOAT_EQ(query_floats[dim + sq8::SUM_QUERY], expected_query_sum);
        ASSERT_FLOAT_EQ(query_floats[dim + sq8::SUM_SQUARES_QUERY], expected_query_sum_squares);

        // Check address is aligned
        unsigned char address_alignment = (uintptr_t)(query_blob.get()) % alignment;
        ASSERT_EQ(address_alignment, 0) << "expected alignment " << alignment;
    }

    // Test preprocessStorageInPlace
    {
        // Allocate buffer large enough for in-place quantization
        auto buffer_alloc = allocator->allocate_unique(original_blob_size);
        float *buffer = static_cast<float *>(buffer_alloc.get());
        memcpy(buffer, original_blob, original_blob_size);

        multiPPContainer.preprocessStorageInPlace(buffer, original_blob_size);

        EXPECT_NO_FATAL_FAILURE(CompareVectors<uint8_t>(reinterpret_cast<const uint8_t *>(buffer),
                                                        expected_storage_blob,
                                                        quantized_blob_bytes_count));
#if !defined(NDEBUG)
        EXPECT_EXIT(
            { multiPPContainer.preprocessStorageInPlace(buffer, sizeof(uint8_t)); },
            testing::KilledBySignal(SIGABRT), "Input buffer too small for in-place quantization");
#endif
    }
}

// Verifies that the QuantPreprocessor honors distinct query_alignment and storage_alignment hints
// independently. This guards the MOD-13837 contract: storage and query buffers can have different
// SIMD alignment requirements (e.g. SQ8 storage vs FP32 query).
TEST(PreprocessorsTest, QuantizationAsymmetricAlignment) {
    std::shared_ptr<VecSimAllocator> allocator = VecSimAllocator::newVecsimAllocator();
    constexpr size_t n_preprocessors = 1;
    constexpr unsigned char query_alignment = 32;
    constexpr unsigned char storage_alignment = 16;
    constexpr size_t dim = 6;
    constexpr size_t original_blob_size = dim * sizeof(float);
    float original_blob[dim] = {1, 2, 3, 4, 5, 6};

    auto quant_preprocessor =
        new (allocator) QuantPreprocessor<float, VecSimMetric_L2>(allocator, dim);
    auto multiPPContainer = MultiPreprocessorsContainer<float, n_preprocessors>(
        allocator, query_alignment, storage_alignment);
    multiPPContainer.addPreprocessor(quant_preprocessor);

    // preprocess() exercises the joint storage+query allocation path.
    {
        ProcessedBlobs processed_blobs =
            multiPPContainer.preprocess(original_blob, original_blob_size);
        const void *storage_blob = processed_blobs.getStorageBlob();
        const void *query_blob = processed_blobs.getQueryBlob();

        ASSERT_NE(storage_blob, nullptr);
        ASSERT_NE(query_blob, nullptr);
        ASSERT_NE(storage_blob, query_blob);

        ASSERT_EQ(reinterpret_cast<uintptr_t>(storage_blob) % storage_alignment, 0u)
            << "storage blob not aligned to " << static_cast<int>(storage_alignment);
        ASSERT_EQ(reinterpret_cast<uintptr_t>(query_blob) % query_alignment, 0u)
            << "query blob not aligned to " << static_cast<int>(query_alignment);
    }

    // preprocessForStorage() is the storage-only path; must honor storage_alignment.
    {
        auto storage_blob =
            multiPPContainer.preprocessForStorage(original_blob, original_blob_size);
        ASSERT_NE(storage_blob.get(), nullptr);
        ASSERT_EQ(reinterpret_cast<uintptr_t>(storage_blob.get()) % storage_alignment, 0u)
            << "storage blob not aligned to " << static_cast<int>(storage_alignment);
    }

    // preprocessQuery() is the query-only path; must honor query_alignment.
    {
        auto query_blob = multiPPContainer.preprocessQuery(original_blob, original_blob_size);
        ASSERT_NE(query_blob.get(), nullptr);
        ASSERT_EQ(reinterpret_cast<uintptr_t>(query_blob.get()) % query_alignment, 0u)
            << "query blob not aligned to " << static_cast<int>(query_alignment);
    }
}

// Finite input whose range is not representable in FP32. max - min overflowed to inf, which made
// delta inf and inv_delta 0, and inf * 0 is NaN; converting that NaN to an integer is undefined
// behaviour, so this reproduced UB on AddVector for input the API accepts. Runs under UBSan in CI.
TEST(PreprocessorsTest, QuantizationHandlesNonRepresentableRange) {
    std::shared_ptr<VecSimAllocator> allocator = VecSimAllocator::newVecsimAllocator();
    constexpr size_t dim = 4;
    constexpr unsigned char alignment = 0;
    constexpr float huge = std::numeric_limits<float>::max();
    float original_blob[dim] = {-huge, 0.0f, huge, 1.0f};

    auto quant_preprocessor =
        new (allocator) QuantPreprocessor<float, VecSimMetric_L2>(allocator, dim);

    void *storage_blob = nullptr;
    void *query_blob = nullptr;
    size_t storage_blob_size = dim * sizeof(float);
    size_t query_blob_size = dim * sizeof(float);
    quant_preprocessor->preprocess(original_blob, storage_blob, query_blob, storage_blob_size,
                                   query_blob_size, alignment, alignment);
    ASSERT_NE(storage_blob, nullptr);

    // max - min overflows FP32 to infinity, so delta is infinite and its reciprocal is zero, and
    // every scaled value comes out zero or NaN. Both now convert to 0 instead of being undefined.
    // The result is degenerate rather than useful, and deliberately so: this PR defines the
    // conversion and leaves intermediate overflow alone. See the tech debt ticket.
    const auto *quantized = static_cast<const uint8_t *>(storage_blob);
    for (size_t i = 0; i < dim; i++) {
        EXPECT_EQ(quantized[i], 0) << "element " << i;
    }

    allocator->free_allocation(storage_blob);
    if (query_blob != storage_blob) {
        allocator->free_allocation(query_blob);
    }
    delete quant_preprocessor;
}

// Finite input whose range is not representable in FP32. max - min overflowed to inf, which made
// delta inf and inv_delta 0, and inf * 0 is NaN; converting that NaN to an integer is undefined
// behaviour, so this reproduced UB on AddVector for input the API accepts. Runs under UBSan in CI.
// delta underflows to zero while max - min is still nonzero, so the reciprocal is infinite and a
// scaled value reaches +inf. main guards only the exactly-equal case (diff == 0 ? 1 : diff / 255),
// so this input converted an infinity to uint8_t: undefined, and on x86 it produced 0, the opposite
// end of the scale from where the value belongs. This is the only case here that reaches the upper
// saturation branch, and it does so from entirely finite input.
TEST(PreprocessorsTest, QuantizationSaturatesWhenDeltaUnderflows) {
    std::shared_ptr<VecSimAllocator> allocator = VecSimAllocator::newVecsimAllocator();
    constexpr size_t dim = 2;
    // 1e-44 / 255 underflows FP32 to zero, while 1e-44 itself is a nonzero subnormal.
    float original_blob[dim] = {0.0f, 1e-44f};

    auto quant_preprocessor =
        new (allocator) QuantPreprocessor<float, VecSimMetric_L2>(allocator, dim);

    void *storage_blob = nullptr;
    size_t storage_blob_size = dim * sizeof(float);
    quant_preprocessor->preprocessForStorage(original_blob, storage_blob, storage_blob_size, 0);
    ASSERT_NE(storage_blob, nullptr);

    const auto *quantized = static_cast<const uint8_t *>(storage_blob);
    EXPECT_EQ(quantized[0], 0);   // scaled is NaN (0 * inf), so the lower branch takes it
    EXPECT_EQ(quantized[1], 255); // scaled is +inf, saturated rather than converted

    allocator->free_allocation(storage_blob);
    delete quant_preprocessor;
}

TEST(PreprocessorsTest, QuantizationHandlesNonRepresentableCenteredRange) {
    std::shared_ptr<VecSimAllocator> allocator = VecSimAllocator::newVecsimAllocator();
    constexpr size_t dim = 4;
    constexpr unsigned char alignment = 0;
    constexpr float huge = std::numeric_limits<float>::max();

    float original_blob[dim] = {huge, 0.0f, -huge, 1.0f};
    vecsim_stl::vector<float> mean_vec(allocator);
    // Centers element 0 to +inf and element 2 to -inf, from entirely finite operands.
    mean_vec.push_back(-huge);
    mean_vec.push_back(0.0f);
    mean_vec.push_back(huge);
    mean_vec.push_back(0.0f);

    auto quant_preprocessor =
        new (allocator) QuantPreprocessor<float, VecSimMetric_L2, true>(allocator, dim, mean_vec);

    void *storage_blob = nullptr;
    size_t storage_blob_size = dim * sizeof(float);
    quant_preprocessor->preprocessForStorage(original_blob, storage_blob, storage_blob_size,
                                             alignment);
    ASSERT_NE(storage_blob, nullptr);

    // Centering drives element 0 to +inf and element 2 to -inf from finite operands, so the
    // endpoints are infinite, delta is infinite and its reciprocal is zero. Every byte converts to
    // 0 rather than being undefined. Degenerate on purpose: the metadata stays infinite because
    // this PR only defines the conversion. See the tech debt ticket.
    const auto *quantized = static_cast<const uint8_t *>(storage_blob);
    for (size_t i = 0; i < dim; i++) {
        EXPECT_EQ(quantized[i], 0) << "element " << i;
    }

    allocator->free_allocation(storage_blob);
    delete quant_preprocessor;
}

// The finite-input domain matrix for quantize(). Every case is either an extreme the API accepts
// without validating, or a range too narrow for a representable delta. The contract under test: the
// byte conversion is defined, the stored min and delta are finite with delta > 0, and where the
// range survives, the extreme elements land on the ends of the byte range. Runs under UBSan
// (float-cast-overflow) in CI, which is where the conversion itself is checked.
namespace {

struct QuantizedVector {
    std::vector<uint8_t> bytes;
    float min_val;
    float delta;
};

// Runs one vector through the production preprocessor on the plain (non-centered) L2 path.
QuantizedVector quantizeThroughPreprocessor(const std::vector<float> &input) {
    std::shared_ptr<VecSimAllocator> allocator = VecSimAllocator::newVecsimAllocator();
    const size_t dim = input.size();
    auto *pp = new (allocator) QuantPreprocessor<float, VecSimMetric_L2>(allocator, dim);

    void *storage_blob = nullptr;
    size_t sz = dim * sizeof(float);
    pp->preprocessForStorage(input.data(), storage_blob, sz, 0);

    const auto *quantized = static_cast<const uint8_t *>(storage_blob);
    const auto *metadata = quantized + dim;
    QuantizedVector out;
    out.bytes.assign(quantized, quantized + dim);
    out.min_val = load_unaligned<float>(metadata + sq8::MIN_VAL * sizeof(float));
    out.delta = load_unaligned<float>(metadata + sq8::DELTA * sizeof(float));

    allocator->free_allocation(storage_blob);
    delete pp;
    return out;
}

struct QuantDomainCase {
    const char *name;
    std::vector<float> input;
    // Asserted element by element, so it must have one entry per input value. Derived by
    // simulating the pipeline rather than predicted; three of them came out different from the
    // first guess.
    std::vector<uint8_t> expected_bytes;
};

class QuantDomainTest : public testing::TestWithParam<QuantDomainCase> {};

} // namespace

TEST_P(QuantDomainTest, ScaleMetadataAndBytesAreAsExpected) {
    const auto &c = GetParam();
    const QuantizedVector q = quantizeThroughPreprocessor(c.input);

    // Every case in this table has a range FP32 can scale, so min and delta must come out finite
    // with delta positive. Ranges that overflow or underflow the FP32 metadata produce degenerate
    // values instead, and are covered by their own tests rather than folded in here, because the
    // assertions below would not hold for them. The sums are deliberately not checked: they are
    // accumulated in FP32 and are a separate problem.
    EXPECT_TRUE(std::isfinite(q.min_val)) << c.name << ": min " << q.min_val;
    EXPECT_TRUE(std::isfinite(q.delta)) << c.name << ": delta " << q.delta;
    EXPECT_GT(q.delta, 0.0f) << c.name;

    ASSERT_EQ(q.bytes.size(), c.expected_bytes.size()) << c.name;
    for (size_t i = 0; i < q.bytes.size(); ++i) {
        EXPECT_EQ(q.bytes[i], c.expected_bytes[i]) << c.name << " at index " << i;
    }
}

INSTANTIATE_TEST_SUITE_P(
    QuantDomain, QuantDomainTest,
    testing::Values(
        // Constant vectors: min == max, so delta collapses to 1 and every value maps to byte 0.
        QuantDomainCase{"all_equal_positive", {3.5f, 3.5f, 3.5f}, {0, 0, 0}},
        QuantDomainCase{"all_equal_negative", {-2.5f, -2.5f, -2.5f}, {0, 0, 0}},
        QuantDomainCase{"all_zero", {0.0f, 0.0f, 0.0f}, {0, 0, 0}},

        // Subnormal but *representable* delta: 7e-37 / 255 = 2.7e-39, which survives as an FP32
        // subnormal, and its FP64 reciprocal 3.6e38 is finite. The range is preserved, so the
        // endpoints must reach 0 and 255. This is the case an FP32 reciprocal would break.
        QuantDomainCase{"subnormal_delta_survives", {0.0f, 7e-37f}, {0, 255}},

        // Every case here is finite input with a scalable range, deliberately. The degenerate
        // cases, where the range overflows or underflows what FP32 metadata can express, live in
        // QuantizationSaturatesWhenDeltaUnderflows and the two NonRepresentable tests, which assert
        // that the conversion is defined rather than that the result is useful.
        QuantDomainCase{"single_element", {7.5f}, {0}}),
    [](const testing::TestParamInfo<QuantDomainCase> &info) { return info.param.name; });

TEST(PreprocessorsTest, QuantizationTestAllEntriesEqual) {
    std::shared_ptr<VecSimAllocator> allocator = VecSimAllocator::newVecsimAllocator();
    constexpr size_t dim = 5;
    constexpr unsigned char alignment = 0;
    float original_blob[dim] = {3.5f, 3.5f, 3.5f, 3.5f, 3.5f};

    auto quant_preprocessor =
        new (allocator) QuantPreprocessor<float, VecSimMetric_L2>(allocator, dim);

    void *storage_blob = nullptr;
    void *query_blob = nullptr;
    size_t storage_blob_size = dim * sizeof(float);
    size_t query_blob_size = dim * sizeof(float);

    quant_preprocessor->preprocess(original_blob, storage_blob, query_blob, storage_blob_size,
                                   query_blob_size, alignment, alignment);

    ASSERT_NE(storage_blob, nullptr);

    // When all values are equal: min == max, delta = 1, all quantized values should be 0
    const uint8_t *quantized = static_cast<const uint8_t *>(storage_blob);
    for (size_t i = 0; i < dim; ++i) {
        ASSERT_EQ(quantized[i], 0) << "All equal values should quantize to 0";
    }

    // Verify metadata: min_val = 3.5f, delta = 1.0f (fallback when diff == 0)
    const auto *metadata = quantized + dim;
    const float min_val = load_unaligned<float>(metadata + sq8::MIN_VAL * sizeof(float));
    const float delta = load_unaligned<float>(metadata + sq8::DELTA * sizeof(float));
    ASSERT_FLOAT_EQ(min_val, 3.5f);
    ASSERT_FLOAT_EQ(delta, 1.0f);

    // Verify sum and sum_squares for L2 metric
    float expected_sum = 3.5f * dim;
    float expected_sum_squares = 3.5f * 3.5f * dim;
    ASSERT_FLOAT_EQ(load_unaligned<float>(metadata + sq8::SUM * sizeof(float)), expected_sum);
    ASSERT_FLOAT_EQ(load_unaligned<float>(metadata + sq8::SUM_SQUARES * sizeof(float)),
                    expected_sum_squares);

    // Reconstruct and verify: min + quantized * delta = 3.5 + 0 * 1 = 3.5
    for (size_t i = 0; i < dim; ++i) {
        float reconstructed = min_val + quantized[i] * delta;
        ASSERT_FLOAT_EQ(reconstructed, original_blob[i]);
    }

    allocator->free_allocation(storage_blob);
    allocator->free_allocation(query_blob);
    delete quant_preprocessor;
}

// Parameterized test class for QuantPreprocessor with different metrics
class QuantPreprocessorMetricTest : public testing::TestWithParam<VecSimMetric> {
protected:
    static constexpr size_t dim = 5;
    static constexpr unsigned char alignment = 0;
    static constexpr size_t original_blob_size = dim * sizeof(float);

    std::shared_ptr<VecSimAllocator> allocator;
    float original_blob[dim] = {1, 2, 3, 4, 5};

    void SetUp() override { allocator = VecSimAllocator::newVecsimAllocator(); }

    // === Storage blob helpers ===

    // Storage layout: | quantized_values[dim] | min | delta | sum | (sum_squares for L2) |
    // L2: dim bytes + 4 floats (min, delta, sum, sum_squares)
    // IP/Cosine: dim bytes + 3 floats (min, delta, sum)
    template <VecSimMetric Metric>
    static size_t getExpectedStorageSize() {
        constexpr size_t extra_floats = sq8::storage_metadata_count<Metric>();
        return dim * sizeof(uint8_t) + extra_floats * sizeof(float);
    }

    // === Query blob helpers ===

    // Query layout: | query_values[dim] | y_sum (IP/Cosine) OR y_sum_squares (L2) |
    // All metrics: (dim + 1) floats
    template <VecSimMetric Metric>
    static constexpr size_t getExpectedQuerySize() {
        return (dim + sq8::query_metadata_count<Metric>()) * sizeof(float);
    }

    // Helper to run quantization test for a specific metric
    template <VecSimMetric Metric>
    void runQuantizationTest() {
        size_t expected_storage_size = getExpectedStorageSize<Metric>();
        size_t expected_query_size = getExpectedQuerySize<Metric>();

        float expected_query_sum = 0;
        float expected_query_sum_squares = 0;
        for (size_t i = 0; i < dim; ++i) {
            expected_query_sum += original_blob[i];
            expected_query_sum_squares += original_blob[i] * original_blob[i];
        }

        auto quant_preprocessor = new (allocator) QuantPreprocessor<float, Metric>(allocator, dim);

        // Test preprocess (both storage and query)
        {
            void *storage_blob = nullptr;
            void *query_blob = nullptr;
            size_t storage_blob_size = original_blob_size;
            size_t query_blob_size = original_blob_size;

            quant_preprocessor->preprocess(original_blob, storage_blob, query_blob,
                                           storage_blob_size, query_blob_size, alignment,
                                           alignment);

            // Verify storage blob
            ASSERT_NE(storage_blob, nullptr);
            ASSERT_EQ(storage_blob_size, expected_storage_size);

            // Verify query blob
            ASSERT_NE(query_blob, nullptr);
            ASSERT_EQ(query_blob_size, expected_query_size);

            // === Verify storage blob content ===
            constexpr size_t max_storage_size = dim * sizeof(uint8_t) + 4 * sizeof(float);
            uint8_t expected_storage_blob[max_storage_size];
            ComputeSQ8Quantization(original_blob, dim, expected_storage_blob);

            // For non-L2 metrics, compare only dim + 3 floats (excluding sum_squares)
            size_t compare_size = (Metric == VecSimMetric_L2)
                                      ? expected_storage_size
                                      : dim * sizeof(uint8_t) + 3 * sizeof(float);
            EXPECT_NO_FATAL_FAILURE(CompareVectors<uint8_t>(
                static_cast<const uint8_t *>(storage_blob), expected_storage_blob, compare_size));

            // === Verify query blob content ===
            const float *query_floats = static_cast<const float *>(query_blob);
            EXPECT_NO_FATAL_FAILURE(CompareVectors<float>(query_floats, original_blob, dim));

            // Verify precomputed value (sum for IP/Cosine, sum and sum_squares for L2)
            ASSERT_FLOAT_EQ(query_floats[dim + sq8::SUM_QUERY], expected_query_sum);
            if constexpr (Metric == VecSimMetric_L2) {
                ASSERT_FLOAT_EQ(query_floats[dim + sq8::SUM_SQUARES_QUERY],
                                expected_query_sum_squares);
            }

            allocator->free_allocation(storage_blob);
            allocator->free_allocation(query_blob);
        }

        // Test preprocessForStorage
        {
            void *blob = nullptr;
            size_t blob_size = original_blob_size;

            quant_preprocessor->preprocessForStorage(original_blob, blob, blob_size, alignment);

            ASSERT_EQ(blob_size, expected_storage_size);
            allocator->free_allocation(blob);
        }

        // Test preprocessQuery
        {
            void *blob = nullptr;
            size_t blob_size = original_blob_size;

            quant_preprocessor->preprocessQuery(original_blob, blob, blob_size, alignment);

            ASSERT_NE(blob, nullptr);
            ASSERT_EQ(blob_size, expected_query_size);

            // Verify query blob content: original values followed by precomputed value
            const float *query_floats = static_cast<const float *>(blob);
            EXPECT_NO_FATAL_FAILURE(CompareVectors<float>(query_floats, original_blob, dim));

            // Verify precomputed value (sum for IP/Cosine, sum and sum_squares for L2)
            ASSERT_FLOAT_EQ(query_floats[dim + sq8::SUM_QUERY], expected_query_sum);
            if constexpr (Metric == VecSimMetric_L2) {
                ASSERT_FLOAT_EQ(query_floats[dim + sq8::SUM_SQUARES_QUERY],
                                expected_query_sum_squares);
            }
            allocator->free_allocation(blob);
        }

        delete quant_preprocessor;
    }
};

TEST_P(QuantPreprocessorMetricTest, QuantizationBlobSizeAndMetadata) {
    VecSimMetric metric = GetParam();
    switch (metric) {
    case VecSimMetric_L2:
        runQuantizationTest<VecSimMetric_L2>();
        break;
    case VecSimMetric_IP:
        runQuantizationTest<VecSimMetric_IP>();
        break;
    case VecSimMetric_Cosine:
        runQuantizationTest<VecSimMetric_Cosine>();
        break;
    }
}

INSTANTIATE_TEST_SUITE_P(QuantPreprocessorTests, QuantPreprocessorMetricTest,
                         testing::Values(VecSimMetric_L2, VecSimMetric_IP, VecSimMetric_Cosine),
                         [](const testing::TestParamInfo<VecSimMetric> &info) {
                             return VecSimMetric_ToString(info.param);
                         });

// Parameterized test class for QuantPreprocessor<float16, *>. Verifies the hybrid layout:
// storage = [uint8 * dim][float * N], query = [float16 * dim][float * M], with FP32 metadata
// matching the FP32-quantized baseline of the same input widened to FP32.
class QuantPreprocessorFP16MetricTest : public testing::TestWithParam<VecSimMetric> {
protected:
    using float16 = vecsim_types::float16;
    static constexpr size_t dim = 5;
    static constexpr unsigned char alignment = 0;
    static constexpr size_t original_blob_size = dim * sizeof(float16);

    std::shared_ptr<VecSimAllocator> allocator;
    float16 original_blob[dim]; // FP16 input
    float widened_blob[dim];    // FP32 view of the same input (round-trip through FP16)

    void SetUp() override {
        allocator = VecSimAllocator::newVecsimAllocator();
        const float src[dim] = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f};
        for (size_t i = 0; i < dim; ++i) {
            original_blob[i] = vecsim_types::FP32_to_FP16(src[i]);
            widened_blob[i] = vecsim_types::FP16_to_FP32(original_blob[i]);
        }
    }

    template <VecSimMetric Metric>
    static size_t getExpectedStorageSize() {
        return dim * sizeof(uint8_t) + sq8::storage_metadata_count<Metric>() * sizeof(float);
    }

    template <VecSimMetric Metric>
    static size_t getExpectedQuerySize() {
        return dim * sizeof(float16) + sq8::query_metadata_count<Metric>() * sizeof(float);
    }

    // Reads an FP32 metadata scalar at the given byte offset from `base` via memcpy (the
    // metadata region is not guaranteed to be 4-byte aligned for FP16 query bodies).
    static float load_meta(const void *base, size_t byte_offset) {
        float v;
        std::memcpy(&v, static_cast<const uint8_t *>(base) + byte_offset, sizeof(float));
        return v;
    }

    template <VecSimMetric Metric>
    void runQuantizationTest() {
        const size_t expected_storage_size = getExpectedStorageSize<Metric>();
        const size_t expected_query_size = getExpectedQuerySize<Metric>();
        const size_t storage_meta_offset = dim * sizeof(uint8_t);
        const size_t query_meta_offset = dim * sizeof(float16);

        // FP32 baseline: quantize the widened (FP16->FP32) input through the same algorithm.
        // Read metadata via load_meta() because baseline_storage is a uint8_t buffer and the
        // metadata region (offset = dim) is not guaranteed to be 4-byte aligned.
        constexpr size_t max_storage_size = dim * sizeof(uint8_t) + 4 * sizeof(float);
        uint8_t baseline_storage[max_storage_size];
        ComputeSQ8Quantization(widened_blob, dim, baseline_storage);
        const float baseline_min = load_meta(baseline_storage, dim + sq8::MIN_VAL * sizeof(float));
        const float baseline_delta = load_meta(baseline_storage, dim + sq8::DELTA * sizeof(float));
        // The storage baseline's sums describe the reconstruction min + delta * a[i]. The query is
        // not quantized, so its sums describe the input itself. Those are different quantities and
        // must be checked separately: they were only ever equal because both used to be the input.
        float input_sum = 0.0f, input_sum_sq = 0.0f;
        for (size_t i = 0; i < dim; i++) {
            input_sum += widened_blob[i];
            input_sum_sq += widened_blob[i] * widened_blob[i];
        }
        const float baseline_sum = load_meta(baseline_storage, dim + sq8::SUM * sizeof(float));
        const float baseline_sum_sq =
            load_meta(baseline_storage, dim + sq8::SUM_SQUARES * sizeof(float));

        auto quant_preprocessor =
            new (allocator) QuantPreprocessor<float16, Metric>(allocator, dim);

        // Test preprocess (both storage and query)
        {
            void *storage_blob = nullptr;
            void *query_blob = nullptr;
            size_t storage_blob_size = original_blob_size;
            size_t query_blob_size = original_blob_size;

            quant_preprocessor->preprocess(original_blob, storage_blob, query_blob,
                                           storage_blob_size, query_blob_size, alignment,
                                           alignment);

            // Verify storage blob layout/size
            ASSERT_NE(storage_blob, nullptr);
            ASSERT_EQ(storage_blob_size, expected_storage_size);

            // Verify query blob layout/size
            ASSERT_NE(query_blob, nullptr);
            ASSERT_EQ(query_blob_size, expected_query_size);

            // Storage quantized values must match the FP32 baseline.
            EXPECT_NO_FATAL_FAILURE(CompareVectors<uint8_t>(
                static_cast<const uint8_t *>(storage_blob), baseline_storage, dim));

            // Storage FP32 metadata must match the baseline values.
            ASSERT_FLOAT_EQ(
                load_meta(storage_blob, storage_meta_offset + sq8::MIN_VAL * sizeof(float)),
                baseline_min);
            ASSERT_FLOAT_EQ(
                load_meta(storage_blob, storage_meta_offset + sq8::DELTA * sizeof(float)),
                baseline_delta);
            ASSERT_FLOAT_EQ(load_meta(storage_blob, storage_meta_offset + sq8::SUM * sizeof(float)),
                            baseline_sum);
            if constexpr (Metric == VecSimMetric_L2) {
                ASSERT_FLOAT_EQ(
                    load_meta(storage_blob, storage_meta_offset + sq8::SUM_SQUARES * sizeof(float)),
                    baseline_sum_sq);
            }

            // Query body must be a bit-equal copy of the FP16 input.
            EXPECT_NO_FATAL_FAILURE(CompareVectors<float16>(
                static_cast<const float16 *>(query_blob), original_blob, dim));

            // Query FP32 metadata: y_sum (and y_sum_squares for L2) match the FP32 baseline.
            ASSERT_FLOAT_EQ(
                load_meta(query_blob, query_meta_offset + sq8::SUM_QUERY * sizeof(float)),
                input_sum);
            if constexpr (Metric == VecSimMetric_L2) {
                ASSERT_FLOAT_EQ(load_meta(query_blob, query_meta_offset +
                                                          sq8::SUM_SQUARES_QUERY * sizeof(float)),
                                input_sum_sq);
            }

            allocator->free_allocation(storage_blob);
            allocator->free_allocation(query_blob);
        }

        // Test preprocessQuery alone.
        {
            void *blob = nullptr;
            size_t blob_size = original_blob_size;
            quant_preprocessor->preprocessQuery(original_blob, blob, blob_size, alignment);

            ASSERT_NE(blob, nullptr);
            ASSERT_EQ(blob_size, expected_query_size);
            EXPECT_NO_FATAL_FAILURE(
                CompareVectors<float16>(static_cast<const float16 *>(blob), original_blob, dim));
            ASSERT_FLOAT_EQ(load_meta(blob, query_meta_offset + sq8::SUM_QUERY * sizeof(float)),
                            input_sum);
            if constexpr (Metric == VecSimMetric_L2) {
                ASSERT_FLOAT_EQ(
                    load_meta(blob, query_meta_offset + sq8::SUM_SQUARES_QUERY * sizeof(float)),
                    input_sum_sq);
            }
            allocator->free_allocation(blob);
        }

        delete quant_preprocessor;
    }
};

TEST_P(QuantPreprocessorFP16MetricTest, QuantizationBlobSizeAndMetadata) {
    VecSimMetric metric = GetParam();
    switch (metric) {
    case VecSimMetric_L2:
        runQuantizationTest<VecSimMetric_L2>();
        break;
    case VecSimMetric_IP:
        runQuantizationTest<VecSimMetric_IP>();
        break;
    case VecSimMetric_Cosine:
        runQuantizationTest<VecSimMetric_Cosine>();
        break;
    }
}

INSTANTIATE_TEST_SUITE_P(QuantPreprocessorFP16Tests, QuantPreprocessorFP16MetricTest,
                         testing::Values(VecSimMetric_L2, VecSimMetric_IP, VecSimMetric_Cosine),
                         [](const testing::TestParamInfo<VecSimMetric> &info) {
                             return VecSimMetric_ToString(info.param);
                         });

// Quantize -> reconstruct round-trip for FP16 input. Verifies that for each quantized value
// q_i, reconstructed = min + delta * q_i is within one quantization step of the original
// FP16 value (widened to FP32). Also covers the in-place quantization path.
TEST(QuantPreprocessorFP16Test, QuantizeReconstructRoundTripL2) {
    using float16 = vecsim_types::float16;
    auto allocator = VecSimAllocator::newVecsimAllocator();
    constexpr size_t dim = 17; // odd, exercises the tail loop and unaligned metadata writes
    const float src[dim] = {-3.5f, -2.0f, -1.25f, -0.5f, -0.125f, 0.0f, 0.125f, 0.5f, 1.0f,
                            1.5f,  2.0f,  2.5f,   3.0f,  3.25f,   3.4f, 3.45f,  3.5f};
    float16 input[dim];
    float widened[dim];
    for (size_t i = 0; i < dim; ++i) {
        input[i] = vecsim_types::FP32_to_FP16(src[i]);
        widened[i] = vecsim_types::FP16_to_FP32(input[i]);
    }

    auto preprocessor = new (allocator) QuantPreprocessor<float16, VecSimMetric_L2>(allocator, dim);

    void *storage_blob = nullptr;
    size_t storage_blob_size = 0;
    preprocessor->preprocessForStorage(input, storage_blob, storage_blob_size, 0);
    ASSERT_NE(storage_blob, nullptr);
    ASSERT_EQ(storage_blob_size, dim * sizeof(uint8_t) + 4 * sizeof(float));

    const uint8_t *quantized = static_cast<const uint8_t *>(storage_blob);
    float min_val, delta;
    std::memcpy(&min_val, quantized + dim + sq8::MIN_VAL * sizeof(float), sizeof(float));
    std::memcpy(&delta, quantized + dim + sq8::DELTA * sizeof(float), sizeof(float));

    // Reconstruction error should be bounded by the quantization step (delta).
    for (size_t i = 0; i < dim; ++i) {
        const float reconstructed = min_val + delta * static_cast<float>(quantized[i]);
        EXPECT_NEAR(reconstructed, widened[i], delta);
    }

    // In-place path: seed a buffer large enough to hold both the FP16 input and the SQ8
    // storage layout, copy the FP16 input in, and quantize in place. The resulting SQ8 blob
    // must match the one produced by preprocessForStorage.
    constexpr size_t input_size = dim * sizeof(float16);
    constexpr size_t storage_size = dim * sizeof(uint8_t) + 4 * sizeof(float);
    constexpr size_t buf_size = (input_size > storage_size) ? input_size : storage_size;
    alignas(float) uint8_t in_place_buf[buf_size]{};
    std::memcpy(in_place_buf, input, input_size);
    preprocessor->preprocessStorageInPlace(in_place_buf, buf_size);
    EXPECT_NO_FATAL_FAILURE(CompareVectors<uint8_t>(
        in_place_buf, static_cast<const uint8_t *>(storage_blob), storage_size));

    allocator->free_allocation(storage_blob);
    delete preprocessor;
}

// Shared parameterized fixture for QuantPreprocessor<DataType, *, WithNorm=true>.
template <typename DataType>
class QuantPreprocessorWithNormMetricTestBase : public testing::TestWithParam<VecSimMetric> {
protected:
    static constexpr size_t dim = 5;
    static constexpr unsigned char alignment = 0;
    static constexpr size_t original_blob_size = dim * sizeof(DataType);

    std::shared_ptr<VecSimAllocator> allocator;
    DataType original_blob[dim];
    float widened_blob[dim];
    float mean_vec[dim] = {0.1f, 0.2f, 0.3f, 0.4f, 0.5f};

    void SetUp() override {
        allocator = VecSimAllocator::newVecsimAllocator();
        const float source[dim] = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f};
        for (size_t i = 0; i < dim; ++i) {
            if constexpr (std::is_same_v<DataType, vecsim_types::float16>) {
                original_blob[i] = vecsim_types::FP32_to_FP16(source[i]);
                widened_blob[i] = vecsim_types::FP16_to_FP32(original_blob[i]);
            } else {
                original_blob[i] = source[i];
                widened_blob[i] = source[i];
            }
        }
    }

    static float load_meta(const void *base, size_t byte_offset) {
        float value;
        std::memcpy(&value, static_cast<const uint8_t *>(base) + byte_offset, sizeof(value));
        return value;
    }

    template <VecSimMetric Metric>
    size_t getExpectedStorageSize() const {
        return dim * sizeof(uint8_t) + sq8::storage_metadata_count<Metric, true>() * sizeof(float);
    }

    template <VecSimMetric Metric>
    size_t getExpectedQuerySize() const {
        using QueryType = std::conditional_t<Metric == VecSimMetric_L2, float, DataType>;
        return dim * sizeof(QueryType) + sq8::query_metadata_count<Metric, true>() * sizeof(float);
    }

    template <VecSimMetric Metric>
    void runQuantizationTest() {
        using QueryType = std::conditional_t<Metric == VecSimMetric_L2, float, DataType>;
        const size_t expected_storage_size = getExpectedStorageSize<Metric>();
        const size_t expected_query_size = getExpectedQuerySize<Metric>();
        const size_t storage_meta_offset = dim * sizeof(uint8_t);
        const size_t query_meta_offset = dim * sizeof(QueryType);
        float centered[dim];
        QueryType expected_query_body[dim];
        float expected_x_mean_ip = 0.0f;
        float expected_y_sum = 0.0f;
        float expected_y_sum_squares = 0.0f;
        float expected_y_mean_ip = 0.0f;
        for (size_t i = 0; i < dim; ++i) {
            centered[i] = widened_blob[i] - mean_vec[i];
            if constexpr (Metric == VecSimMetric_L2) {
                expected_query_body[i] = centered[i];
            } else {
                expected_query_body[i] = original_blob[i];
            }
            const float query_value = to_fp32<QueryType>(expected_query_body[i]);
            expected_x_mean_ip += widened_blob[i] * mean_vec[i];
            expected_y_sum += query_value;
            expected_y_sum_squares += query_value * query_value;
            expected_y_mean_ip += widened_blob[i] * mean_vec[i];
        }

        constexpr size_t max_storage_baseline = dim * sizeof(uint8_t) + 4 * sizeof(float);
        uint8_t baseline_storage[max_storage_baseline];
        ComputeSQ8Quantization(centered, dim, baseline_storage);

        vecsim_stl::vector<float> mean_vector(allocator);
        for (size_t i = 0; i < dim; ++i) {
            mean_vector.push_back(mean_vec[i]);
        }
        auto quant_preprocessor =
            new (allocator) QuantPreprocessor<DataType, Metric, true>(allocator, dim, mean_vector);

        {
            void *storage_blob = nullptr;
            void *query_blob = nullptr;
            size_t storage_blob_size = original_blob_size;
            size_t query_blob_size = original_blob_size;
            quant_preprocessor->preprocess(original_blob, storage_blob, query_blob,
                                           storage_blob_size, query_blob_size, alignment,
                                           alignment);

            ASSERT_NE(storage_blob, nullptr);
            ASSERT_EQ(storage_blob_size, expected_storage_size);
            ASSERT_NE(query_blob, nullptr);
            ASSERT_EQ(query_blob_size, expected_query_size);

            const size_t compare_size = (Metric == VecSimMetric_L2)
                                            ? dim * sizeof(uint8_t) + 4 * sizeof(float)
                                            : dim * sizeof(uint8_t) + 3 * sizeof(float);
            EXPECT_NO_FATAL_FAILURE(CompareVectors<uint8_t>(
                static_cast<const uint8_t *>(storage_blob), baseline_storage, compare_size));
            if constexpr (Metric == VecSimMetric_IP) {
                ASSERT_FLOAT_EQ(
                    load_meta(storage_blob,
                              storage_meta_offset + sq8::mean_ip_index<Metric>() * sizeof(float)),
                    expected_x_mean_ip);
            }
            EXPECT_NO_FATAL_FAILURE(CompareVectors<QueryType>(
                static_cast<const QueryType *>(query_blob), expected_query_body, dim));
            ASSERT_FLOAT_EQ(
                load_meta(query_blob, query_meta_offset + sq8::SUM_QUERY * sizeof(float)),
                expected_y_sum);
            if constexpr (Metric == VecSimMetric_L2) {
                ASSERT_FLOAT_EQ(load_meta(query_blob, query_meta_offset +
                                                          sq8::SUM_SQUARES_QUERY * sizeof(float)),
                                expected_y_sum_squares);
            }
            if constexpr (Metric == VecSimMetric_IP) {
                ASSERT_FLOAT_EQ(
                    load_meta(query_blob, query_meta_offset +
                                              sq8::query_mean_ip_index<Metric>() * sizeof(float)),
                    expected_y_mean_ip);
            }
            allocator->free_allocation(storage_blob);
            allocator->free_allocation(query_blob);
        }

        {
            void *blob = nullptr;
            size_t blob_size = original_blob_size;
            quant_preprocessor->preprocessForStorage(original_blob, blob, blob_size, alignment);
            ASSERT_NE(blob, nullptr);
            ASSERT_EQ(blob_size, expected_storage_size);
            allocator->free_allocation(blob);
        }

        {
            void *blob = nullptr;
            size_t blob_size = original_blob_size;
            quant_preprocessor->preprocessQuery(original_blob, blob, blob_size, alignment);
            ASSERT_NE(blob, nullptr);
            ASSERT_EQ(blob_size, expected_query_size);
            EXPECT_NO_FATAL_FAILURE(CompareVectors<QueryType>(static_cast<const QueryType *>(blob),
                                                              expected_query_body, dim));
            ASSERT_FLOAT_EQ(load_meta(blob, query_meta_offset + sq8::SUM_QUERY * sizeof(float)),
                            expected_y_sum);
            if constexpr (Metric == VecSimMetric_L2) {
                ASSERT_FLOAT_EQ(
                    load_meta(blob, query_meta_offset + sq8::SUM_SQUARES_QUERY * sizeof(float)),
                    expected_y_sum_squares);
            }
            if constexpr (Metric == VecSimMetric_IP) {
                ASSERT_FLOAT_EQ(
                    load_meta(blob, query_meta_offset +
                                        sq8::query_mean_ip_index<Metric>() * sizeof(float)),
                    expected_y_mean_ip);
            }
            allocator->free_allocation(blob);
        }

        delete quant_preprocessor;
    }
};

TEST(QuantPreprocessorFP16WithNormTest, L2CenteringPreservesFP32RangeAndPrecision) {
    using DataType = vecsim_types::float16;
    auto allocator = VecSimAllocator::newVecsimAllocator();
    for (size_t dim : {1, 4, 65}) {
        for (const auto [input_value, mean_value] :
             {std::pair{1.0f, 10000.0f}, std::pair{-40000.0f, 40000.0f},
              std::pair{0.5f, 0.10001f}}) {
            SCOPED_TRACE(::testing::Message()
                         << "dim=" << dim << " input=" << input_value << " mean=" << mean_value);
            std::vector<DataType> input(dim, vecsim_types::FP32_to_FP16(input_value));
            vecsim_stl::vector<float> mean(dim, mean_value, allocator);
            QuantPreprocessor<DataType, VecSimMetric_L2, true> preprocessor(allocator, dim, mean);
            void *query_blob = nullptr;
            size_t query_size = input.size() * sizeof(DataType);
            preprocessor.preprocessQuery(input.data(), query_blob, query_size, 0);
            const auto free_blob = [&](void *blob) { allocator->free_allocation(blob); };
            std::unique_ptr<void, decltype(free_blob)> query(query_blob, free_blob);

            ASSERT_EQ(query_size, (dim + 2) * sizeof(float));
            const auto *values = static_cast<const float *>(query.get());
            const float expected = input_value - mean_value;
            for (size_t i = 0; i < dim; ++i) {
                EXPECT_FLOAT_EQ(values[i], expected);
            }
            EXPECT_FLOAT_EQ(values[dim + sq8::SUM_QUERY], expected * dim);
            EXPECT_NEAR(values[dim + sq8::SUM_SQUARES_QUERY], double(expected) * expected * dim,
                        double(expected) * expected * dim * 1e-6);
        }
    }
}

using QuantPreprocessorWithNormMetricTest = QuantPreprocessorWithNormMetricTestBase<float>;

TEST_P(QuantPreprocessorWithNormMetricTest, QuantizationBlobSizeAndMetadata) {
    VecSimMetric metric = GetParam();
    switch (metric) {
    case VecSimMetric_L2:
        runQuantizationTest<VecSimMetric_L2>();
        break;
    case VecSimMetric_IP:
        runQuantizationTest<VecSimMetric_IP>();
        break;
    }
}

INSTANTIATE_TEST_SUITE_P(QuantPreprocessorWithNormTests, QuantPreprocessorWithNormMetricTest,
                         testing::Values(VecSimMetric_L2, VecSimMetric_IP),
                         [](const testing::TestParamInfo<VecSimMetric> &info) {
                             return VecSimMetric_ToString(info.param);
                         });

using QuantPreprocessorFP16WithNormMetricTest =
    QuantPreprocessorWithNormMetricTestBase<vecsim_types::float16>;

TEST_P(QuantPreprocessorFP16WithNormMetricTest, QuantizationBlobSizeAndMetadata) {
    VecSimMetric metric = GetParam();
    switch (metric) {
    case VecSimMetric_L2:
        runQuantizationTest<VecSimMetric_L2>();
        break;
    case VecSimMetric_IP:
        runQuantizationTest<VecSimMetric_IP>();
        break;
    }
}

INSTANTIATE_TEST_SUITE_P(QuantPreprocessorFP16WithNormTests,
                         QuantPreprocessorFP16WithNormMetricTest,
                         testing::Values(VecSimMetric_L2, VecSimMetric_IP),
                         [](const testing::TestParamInfo<VecSimMetric> &info) {
                             return VecSimMetric_ToString(info.param);
                         });

// Helper: build storage blob from original vector x and mean.
static void buildStorageBlob(const std::shared_ptr<VecSimAllocator> &allocator, const float *x,
                             const float *mean, size_t dim, VecSimMetric metric,
                             void *&storage_blob) {
    vecsim_stl::vector<float> mean_vec(allocator);
    for (size_t i = 0; i < dim; ++i)
        mean_vec.push_back(mean[i]);

    storage_blob = nullptr;
    size_t sz = dim * sizeof(float);
    if (metric == VecSimMetric_IP) {
        auto *pp = new (allocator)
            QuantPreprocessor<float, VecSimMetric_IP, true>(allocator, dim, mean_vec);
        pp->preprocessForStorage(x, storage_blob, sz, 0);
        delete pp;
    } else {
        auto *pp = new (allocator)
            QuantPreprocessor<float, VecSimMetric_L2, true>(allocator, dim, mean_vec);
        pp->preprocessForStorage(x, storage_blob, sz, 0);
        delete pp;
    }
}

// Helper: build query blob from original vector y and mean.
static void buildQueryBlob(const std::shared_ptr<VecSimAllocator> &allocator, const float *y,
                           const float *mean, size_t dim, VecSimMetric metric, void *&query_blob) {
    vecsim_stl::vector<float> mean_vec(allocator);
    for (size_t i = 0; i < dim; ++i)
        mean_vec.push_back(mean[i]);

    query_blob = nullptr;
    size_t sz = dim * sizeof(float);
    if (metric == VecSimMetric_IP) {
        auto *pp = new (allocator)
            QuantPreprocessor<float, VecSimMetric_IP, true>(allocator, dim, mean_vec);
        pp->preprocessQuery(y, query_blob, sz, 0);
        delete pp;
    } else {
        auto *pp = new (allocator)
            QuantPreprocessor<float, VecSimMetric_L2, true>(allocator, dim, mean_vec);
        pp->preprocessQuery(y, query_blob, sz, 0);
        delete pp;
    }
}

// Brute-force IP distance on original (unshifted) float vectors: 1 - dot(x, y).
static float bruteForceIPDist(const float *x, const float *y, size_t dim) {
    float dot = 0.0f;
    for (size_t i = 0; i < dim; ++i)
        dot += x[i] * y[i];
    return 1.0f - dot;
}

// Brute-force L2 squared distance on original float vectors: sum((x_i - y_i)^2).
static float bruteForceL2Dist(const float *x, const float *y, size_t dim) {
    float sum = 0.0f;
    for (size_t i = 0; i < dim; ++i) {
        float d = x[i] - y[i];
        sum += d * d;
    }
    return sum;
}

// Compute mean_sum_squares = sum(mean_i^2).
static float computeMeanSumSquares(const float *mean, size_t dim) {
    float s = 0.0f;
    for (size_t i = 0; i < dim; ++i)
        s += mean[i] * mean[i];
    return s;
}

TEST(DistanceCalculatorWithNormTest, CalcDistanceForQuery_IP_FP32) {
    std::shared_ptr<VecSimAllocator> allocator = VecSimAllocator::newVecsimAllocator();
    constexpr size_t dim = 8;
    float x[dim] = {1.0f, 2.0f, 3.0f, 4.0f, 1.0f, 2.0f, 3.0f, 4.0f};
    float y[dim] = {0.5f, 1.5f, 2.5f, 3.5f, 0.5f, 1.5f, 2.5f, 3.5f};
    float mean[dim] = {0.5f, 1.0f, 1.5f, 2.0f, 0.5f, 1.0f, 1.5f, 2.0f};

    float mean_sum_sq = computeMeanSumSquares(mean, dim);

    void *storage_blob = nullptr;
    void *query_blob = nullptr;
    buildStorageBlob(allocator, x, mean, dim, VecSimMetric_IP, storage_blob);
    buildQueryBlob(allocator, y, mean, dim, VecSimMetric_IP, query_blob);

    auto asym_func = spaces::IP_SQ8_FP32_GetDistFunc(dim);
    auto sym_func = spaces::IP_SQ8_SQ8_GetDistFunc(dim);
    auto *calc = new (allocator) DistanceCalculatorWithNorm<float, float, VecSimMetric_IP>(
        allocator, asym_func, sym_func, mean_sum_sq);

    float got = calc->calcDistanceForQuery(storage_blob, query_blob, dim);
    float expected = bruteForceIPDist(x, y, dim);
    auto dispatch = calc->getDistanceDispatch(DistanceMode::StoredToQuery);

    // Allow quantization error
    EXPECT_NEAR(got, expected, 0.05f) << "Asymmetric IP distance mismatch";
    ASSERT_TRUE(dispatch.isValid());
    EXPECT_EQ(dispatch.stateless_func, nullptr);
    EXPECT_NE(dispatch.stateful_func, nullptr);
    EXPECT_NEAR(dispatch(storage_blob, query_blob, dim), expected, 0.05f);

    allocator->free_allocation(storage_blob);
    allocator->free_allocation(query_blob);
    delete calc;
}

TEST(DistanceCalculatorWithNormTest, CalcDistanceForQuery_L2_FP32) {
    std::shared_ptr<VecSimAllocator> allocator = VecSimAllocator::newVecsimAllocator();
    constexpr size_t dim = 8;
    float x[dim] = {1.0f, 2.0f, 3.0f, 4.0f, 1.0f, 2.0f, 3.0f, 4.0f};
    float y[dim] = {0.5f, 1.5f, 2.5f, 3.5f, 0.5f, 1.5f, 2.5f, 3.5f};
    float mean[dim] = {0.5f, 1.0f, 1.5f, 2.0f, 0.5f, 1.0f, 1.5f, 2.0f};

    float mean_sum_sq = computeMeanSumSquares(mean, dim);

    void *storage_blob = nullptr;
    void *query_blob = nullptr;
    buildStorageBlob(allocator, x, mean, dim, VecSimMetric_L2, storage_blob);
    buildQueryBlob(allocator, y, mean, dim, VecSimMetric_L2, query_blob);

    auto asym_func = spaces::L2_SQ8_FP32_GetDistFunc(dim);
    auto sym_func = spaces::L2_SQ8_SQ8_GetDistFunc(dim);
    auto *calc = new (allocator) DistanceCalculatorWithNorm<float, float, VecSimMetric_L2>(
        allocator, asym_func, sym_func, mean_sum_sq);

    float got = calc->calcDistanceForQuery(storage_blob, query_blob, dim);
    float expected = bruteForceL2Dist(x, y, dim);
    auto dispatch = calc->getDistanceDispatch(DistanceMode::StoredToQuery);

    EXPECT_NEAR(got, expected, 0.05f) << "Asymmetric L2 distance mismatch";
    ASSERT_TRUE(dispatch.isValid());
    EXPECT_EQ(dispatch.stateless_func, asym_func);
    EXPECT_EQ(dispatch.stateful_func, nullptr);
    EXPECT_NEAR(dispatch(storage_blob, query_blob, dim), expected, 0.05f);

    allocator->free_allocation(storage_blob);
    allocator->free_allocation(query_blob);
    delete calc;
}

TEST(DistanceCalculatorWithNormTest, L2LargeOffsetSmallDistance) {
    std::shared_ptr<VecSimAllocator> allocator = VecSimAllocator::newVecsimAllocator();
    constexpr size_t dim = 128;
    float x[dim];
    float y[dim];
    float mean[dim];
    for (size_t i = 0; i < dim; ++i) {
        mean[i] = 1000.0f;
        x[i] = 1001.0f;
        y[i] = 1001.1f;
    }

    void *storage_blob = nullptr;
    void *query_blob = nullptr;
    buildStorageBlob(allocator, x, mean, dim, VecSimMetric_L2, storage_blob);
    buildQueryBlob(allocator, y, mean, dim, VecSimMetric_L2, query_blob);

    auto asym_func = spaces::L2_SQ8_FP32_GetDistFunc(dim);
    auto sym_func = spaces::L2_SQ8_SQ8_GetDistFunc(dim);
    auto *calc = new (allocator) DistanceCalculatorWithNorm<float, float, VecSimMetric_L2>(
        allocator, asym_func, sym_func, computeMeanSumSquares(mean, dim));

    const float expected = bruteForceL2Dist(x, y, dim);
    const float direct = calc->calcDistanceForQuery(storage_blob, query_blob, dim);
    const auto dispatch = calc->getDistanceDispatch(DistanceMode::StoredToQuery);

    ASSERT_TRUE(dispatch.isValid());
    EXPECT_EQ(dispatch.stateless_func, asym_func);
    EXPECT_EQ(dispatch.stateful_func, nullptr);
    EXPECT_NEAR(direct, expected, 0.001f);
    EXPECT_NEAR(dispatch(storage_blob, query_blob, dim), expected, 0.001f);
    EXPECT_GE(direct, 0.0f);

    allocator->free_allocation(storage_blob);
    allocator->free_allocation(query_blob);
    delete calc;
}

TEST(DistanceCalculatorWithNormTest, CalcDistance_IP_Symmetric) {
    std::shared_ptr<VecSimAllocator> allocator = VecSimAllocator::newVecsimAllocator();
    constexpr size_t dim = 8;
    float x[dim] = {1.0f, 2.0f, 3.0f, 4.0f, 1.0f, 2.0f, 3.0f, 4.0f};
    float y[dim] = {0.5f, 1.5f, 2.5f, 3.5f, 0.5f, 1.5f, 2.5f, 3.5f};
    float mean[dim] = {0.5f, 1.0f, 1.5f, 2.0f, 0.5f, 1.0f, 1.5f, 2.0f};

    float mean_sum_sq = computeMeanSumSquares(mean, dim);

    void *x_blob = nullptr;
    void *y_blob = nullptr;
    buildStorageBlob(allocator, x, mean, dim, VecSimMetric_IP, x_blob);
    buildStorageBlob(allocator, y, mean, dim, VecSimMetric_IP, y_blob);

    auto asym_func = spaces::IP_SQ8_FP32_GetDistFunc(dim);
    auto sym_func = spaces::IP_SQ8_SQ8_GetDistFunc(dim);
    auto *calc = new (allocator) DistanceCalculatorWithNorm<float, float, VecSimMetric_IP>(
        allocator, asym_func, sym_func, mean_sum_sq);

    float got = calc->calcDistance(x_blob, y_blob, dim);
    float expected = bruteForceIPDist(x, y, dim);
    auto dispatch = calc->getDistanceDispatch(DistanceMode::StoredToStored);

    EXPECT_NEAR(got, expected, 0.05f) << "Symmetric IP distance mismatch";
    ASSERT_TRUE(dispatch.isValid());
    EXPECT_EQ(dispatch.stateless_func, nullptr);
    EXPECT_NE(dispatch.stateful_func, nullptr);
    EXPECT_NEAR(dispatch(x_blob, y_blob, dim), expected, 0.05f);

    allocator->free_allocation(x_blob);
    allocator->free_allocation(y_blob);
    delete calc;
}

TEST(DistanceCalculatorWithNormTest, CalcDistance_L2_Symmetric) {
    std::shared_ptr<VecSimAllocator> allocator = VecSimAllocator::newVecsimAllocator();
    constexpr size_t dim = 8;
    float x[dim] = {1.0f, 2.0f, 3.0f, 4.0f, 1.0f, 2.0f, 3.0f, 4.0f};
    float y[dim] = {0.5f, 1.5f, 2.5f, 3.5f, 0.5f, 1.5f, 2.5f, 3.5f};
    float mean[dim] = {0.5f, 1.0f, 1.5f, 2.0f, 0.5f, 1.0f, 1.5f, 2.0f};

    float mean_sum_sq = computeMeanSumSquares(mean, dim);

    void *x_blob = nullptr;
    void *y_blob = nullptr;
    buildStorageBlob(allocator, x, mean, dim, VecSimMetric_L2, x_blob);
    buildStorageBlob(allocator, y, mean, dim, VecSimMetric_L2, y_blob);

    auto asym_func = spaces::L2_SQ8_FP32_GetDistFunc(dim);
    auto sym_func = spaces::L2_SQ8_SQ8_GetDistFunc(dim);
    auto *calc = new (allocator) DistanceCalculatorWithNorm<float, float, VecSimMetric_L2>(
        allocator, asym_func, sym_func, mean_sum_sq);

    float got = calc->calcDistance(x_blob, y_blob, dim);
    float expected = bruteForceL2Dist(x, y, dim);
    auto dispatch = calc->getDistanceDispatch(DistanceMode::StoredToStored);

    EXPECT_NEAR(got, expected, 0.05f) << "Symmetric L2 distance mismatch";
    ASSERT_TRUE(dispatch.isValid());
    EXPECT_EQ(dispatch.stateless_func, sym_func);
    EXPECT_EQ(dispatch.stateful_func, nullptr);
    EXPECT_NEAR(dispatch(x_blob, y_blob, dim), expected, 0.05f);

    allocator->free_allocation(x_blob);
    allocator->free_allocation(y_blob);
    delete calc;
}

TEST(DistanceCalculatorWithNormTest, CalcDistanceForQuery_FP16) {
    std::shared_ptr<VecSimAllocator> allocator = VecSimAllocator::newVecsimAllocator();
    constexpr size_t dim = 8;
    float x_fp32[dim] = {1.0f, 2.0f, 3.0f, 4.0f, 1.0f, 2.0f, 3.0f, 4.0f};
    float y_fp32[dim] = {0.5f, 1.5f, 2.5f, 3.5f, 0.5f, 1.5f, 2.5f, 3.5f};
    float mean[dim] = {0.5f, 1.0f, 1.5f, 2.0f, 0.5f, 1.0f, 1.5f, 2.0f};

    using DataType = vecsim_types::float16;

    DataType x[dim], y[dim];
    for (size_t i = 0; i < dim; ++i) {
        x[i] = vecsim_types::FP32_to_FP16(x_fp32[i]);
        y[i] = vecsim_types::FP32_to_FP16(y_fp32[i]);
    }

    vecsim_stl::vector<float> mean_vec(allocator);
    for (size_t i = 0; i < dim; ++i)
        mean_vec.push_back(mean[i]);
    float mean_sum_sq = computeMeanSumSquares(mean, dim);

    // Build storage blob from FP16 x
    void *storage_blob = nullptr;
    size_t sz = dim * sizeof(DataType);
    auto *pp_ip = new (allocator)
        QuantPreprocessor<DataType, VecSimMetric_IP, true>(allocator, dim, mean_vec);
    pp_ip->preprocessForStorage(x, storage_blob, sz, 0);
    delete pp_ip;

    // Build query blob from FP16 y
    void *query_blob = nullptr;
    sz = dim * sizeof(DataType);
    auto *pp_qr = new (allocator)
        QuantPreprocessor<DataType, VecSimMetric_IP, true>(allocator, dim, mean_vec);
    pp_qr->preprocessQuery(y, query_blob, sz, 0);
    delete pp_qr;

    auto asym_func = spaces::IP_SQ8_FP16_GetDistFunc(dim);
    auto sym_func = spaces::IP_SQ8_SQ8_GetDistFunc(dim);
    auto *calc = new (allocator) DistanceCalculatorWithNorm<DataType, float, VecSimMetric_IP>(
        allocator, asym_func, sym_func, mean_sum_sq);

    float got = calc->calcDistanceForQuery(storage_blob, query_blob, dim);
    float expected = bruteForceIPDist(x_fp32, y_fp32, dim);

    EXPECT_NEAR(got, expected, 0.05f) << "Asymmetric IP FP16 distance mismatch";

    allocator->free_allocation(storage_blob);
    allocator->free_allocation(query_blob);
    delete calc;
}

TEST(DistanceCalculatorWithNormTest, CalcDistanceForQuery_L2_FP16) {
    std::shared_ptr<VecSimAllocator> allocator = VecSimAllocator::newVecsimAllocator();
    constexpr size_t dim = 8;
    const float x_fp32[dim] = {1.0f, 2.0f, 3.0f, 4.0f, 1.0f, 2.0f, 3.0f, 4.0f};
    const float y_fp32[dim] = {0.5f, 1.5f, 2.5f, 3.5f, 0.5f, 1.5f, 2.5f, 3.5f};
    const float mean[dim] = {0.5f, 1.0f, 1.5f, 2.0f, 0.5f, 1.0f, 1.5f, 2.0f};
    using DataType = vecsim_types::float16;

    DataType x[dim], y[dim];
    vecsim_stl::vector<float> mean_vec(allocator);
    for (size_t i = 0; i < dim; ++i) {
        x[i] = vecsim_types::FP32_to_FP16(x_fp32[i]);
        y[i] = vecsim_types::FP32_to_FP16(y_fp32[i]);
        mean_vec.push_back(mean[i]);
    }

    void *storage_blob = nullptr;
    size_t storage_size = dim * sizeof(DataType);
    auto *storage_preprocessor = new (allocator)
        QuantPreprocessor<DataType, VecSimMetric_L2, true>(allocator, dim, mean_vec);
    storage_preprocessor->preprocessForStorage(x, storage_blob, storage_size, 0);
    delete storage_preprocessor;

    void *query_blob = nullptr;
    size_t query_size = dim * sizeof(DataType);
    auto *query_preprocessor = new (allocator)
        QuantPreprocessor<DataType, VecSimMetric_L2, true>(allocator, dim, mean_vec);
    query_preprocessor->preprocessQuery(y, query_blob, query_size, 0);
    delete query_preprocessor;

    auto asym_func = spaces::L2_SQ8_FP32_GetDistFunc(dim);
    auto sym_func = spaces::L2_SQ8_SQ8_GetDistFunc(dim);
    auto *calc = new (allocator) DistanceCalculatorWithNorm<DataType, float, VecSimMetric_L2>(
        allocator, asym_func, sym_func, computeMeanSumSquares(mean, dim));
    const float expected = bruteForceL2Dist(x_fp32, y_fp32, dim);
    const auto dispatch = calc->getDistanceDispatch(DistanceMode::StoredToQuery);

    EXPECT_NEAR(calc->calcDistanceForQuery(storage_blob, query_blob, dim), expected, 0.05f);
    ASSERT_TRUE(dispatch.isValid());
    EXPECT_EQ(dispatch.stateless_func, asym_func);
    EXPECT_EQ(dispatch.stateful_func, nullptr);
    EXPECT_NEAR(dispatch(storage_blob, query_blob, dim), expected, 0.05f);

    allocator->free_allocation(storage_blob);
    allocator->free_allocation(query_blob);
    delete calc;
}

TEST(DistanceCalculatorWithNormTest, ZeroMean_MatchesBaseSQ8) {
    std::shared_ptr<VecSimAllocator> allocator = VecSimAllocator::newVecsimAllocator();
    constexpr size_t dim = 8;
    float x[dim] = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f, 7.0f, 8.0f};
    float y[dim] = {0.5f, 1.5f, 2.5f, 3.5f, 4.5f, 5.5f, 6.5f, 7.5f};
    vecsim_stl::vector<float> zero_mean(dim, 0.0f, allocator);

    // Build WithNorm storage and query blobs using zero mean
    auto *pp_ip =
        new (allocator) QuantPreprocessor<float, VecSimMetric_IP, true>(allocator, dim, zero_mean);
    void *x_norm_blob = nullptr, *y_norm_query = nullptr;
    size_t sx = dim * sizeof(float), sq = dim * sizeof(float);
    pp_ip->preprocessForStorage(x, x_norm_blob, sx, 0);
    pp_ip->preprocessQuery(y, y_norm_query, sq, 0);
    delete pp_ip;

    auto asym_func = spaces::IP_SQ8_FP32_GetDistFunc(dim);
    auto sym_func = spaces::IP_SQ8_SQ8_GetDistFunc(dim);

    auto *norm_calc = new (allocator) DistanceCalculatorWithNorm<float, float, VecSimMetric_IP>(
        allocator, asym_func, sym_func, 0.0f);

    // With zero mean, correction is 0: result equals raw base function call.
    // asym_func expects (storage, query) order.
    float norm_asym = norm_calc->calcDistanceForQuery(x_norm_blob, y_norm_query, dim);
    float base_asym = asym_func(x_norm_blob, y_norm_query, dim);
    EXPECT_FLOAT_EQ(norm_asym, base_asym)
        << "WithNorm(zero mean) asymmetric IP should match raw base dist function";

    // Build a second storage blob for y to test symmetric distance
    void *y_norm_blob = nullptr;
    sx = dim * sizeof(float);
    auto *pp_ip2 =
        new (allocator) QuantPreprocessor<float, VecSimMetric_IP, true>(allocator, dim, zero_mean);
    pp_ip2->preprocessForStorage(y, y_norm_blob, sx, 0);
    delete pp_ip2;

    float norm_sym = norm_calc->calcDistance(x_norm_blob, y_norm_blob, dim);
    float base_sym = sym_func(x_norm_blob, y_norm_blob, dim);
    EXPECT_FLOAT_EQ(norm_sym, base_sym)
        << "WithNorm(zero mean) symmetric IP should match raw base dist function";

    allocator->free_allocation(x_norm_blob);
    allocator->free_allocation(y_norm_query);
    allocator->free_allocation(y_norm_blob);
    delete norm_calc;
}

TEST(DistanceCalculatorWithNormTest, SymmetricVsAsymmetric_Sanity) {
    // For the same two stored vectors, calcDistance (symmetric) and calcDistanceForQuery
    // (asymmetric) must agree to within quantization error.
    std::shared_ptr<VecSimAllocator> allocator = VecSimAllocator::newVecsimAllocator();
    constexpr size_t dim = 8;
    float x[dim] = {3.0f, 1.0f, 4.0f, 1.0f, 5.0f, 3.0f, 2.0f, 6.0f};
    float y[dim] = {2.0f, 7.0f, 1.0f, 4.0f, 2.0f, 5.0f, 1.0f, 2.0f};
    float mean[dim] = {1.0f, 2.0f, 1.0f, 2.0f, 1.0f, 2.0f, 1.0f, 2.0f};
    float mean_sum_sq = computeMeanSumSquares(mean, dim);

    // IP
    {
        void *x_blob = nullptr, *y_blob = nullptr, *y_query = nullptr;
        buildStorageBlob(allocator, x, mean, dim, VecSimMetric_IP, x_blob);
        buildStorageBlob(allocator, y, mean, dim, VecSimMetric_IP, y_blob);
        buildQueryBlob(allocator, y, mean, dim, VecSimMetric_IP, y_query);

        auto asym_func = spaces::IP_SQ8_FP32_GetDistFunc(dim);
        auto sym_func = spaces::IP_SQ8_SQ8_GetDistFunc(dim);
        auto *calc = new (allocator) DistanceCalculatorWithNorm<float, float, VecSimMetric_IP>(
            allocator, asym_func, sym_func, mean_sum_sq);

        float sym_dist = calc->calcDistance(x_blob, y_blob, dim);
        float asym_dist = calc->calcDistanceForQuery(x_blob, y_query, dim);
        // Both should be close to the brute-force answer; use relative tolerance
        // since the IP magnitude (~157) amplifies absolute quantization error.
        float bf = bruteForceIPDist(x, y, dim);
        EXPECT_NEAR(sym_dist, bf, 0.05f) << "Symmetric IP vs brute-force";
        EXPECT_NEAR(asym_dist, bf, 0.05f) << "Asymmetric IP vs brute-force";

        allocator->free_allocation(x_blob);
        allocator->free_allocation(y_blob);
        allocator->free_allocation(y_query);
        delete calc;
    }

    // L2
    {
        void *x_blob = nullptr, *y_blob = nullptr, *y_query = nullptr;
        buildStorageBlob(allocator, x, mean, dim, VecSimMetric_L2, x_blob);
        buildStorageBlob(allocator, y, mean, dim, VecSimMetric_L2, y_blob);
        buildQueryBlob(allocator, y, mean, dim, VecSimMetric_L2, y_query);

        auto asym_func = spaces::L2_SQ8_FP32_GetDistFunc(dim);
        auto sym_func = spaces::L2_SQ8_SQ8_GetDistFunc(dim);
        auto *calc = new (allocator) DistanceCalculatorWithNorm<float, float, VecSimMetric_L2>(
            allocator, asym_func, sym_func, mean_sum_sq);

        float sym_dist = calc->calcDistance(x_blob, y_blob, dim);
        float asym_dist = calc->calcDistanceForQuery(x_blob, y_query, dim);
        float bf = bruteForceL2Dist(x, y, dim);
        EXPECT_NEAR(sym_dist, bf, 0.05f) << "Symmetric L2 vs brute-force";
        EXPECT_NEAR(asym_dist, bf, 0.05f) << "Asymmetric L2 vs brute-force";

        allocator->free_allocation(x_blob);
        allocator->free_allocation(y_blob);
        allocator->free_allocation(y_query);
        delete calc;
    }
}

TEST(DistanceCalculatorWithNormTest, RandomVectors) {
    // Generate random vector pairs, compute WithNorm distances and verify against brute-force.
    std::shared_ptr<VecSimAllocator> allocator = VecSimAllocator::newVecsimAllocator();
    constexpr size_t dim = 16;
    std::mt19937 rng(12345);
    std::uniform_real_distribution<float> dist(-1.0f, 1.0f);

    float mean[dim];
    for (size_t i = 0; i < dim; ++i)
        mean[i] = dist(rng) * 0.5f;
    float mean_sum_sq = computeMeanSumSquares(mean, dim);

    auto asym_ip = spaces::IP_SQ8_FP32_GetDistFunc(dim);
    auto sym_ip = spaces::IP_SQ8_SQ8_GetDistFunc(dim);
    auto asym_l2 = spaces::L2_SQ8_FP32_GetDistFunc(dim);
    auto sym_l2 = spaces::L2_SQ8_SQ8_GetDistFunc(dim);

    auto *calc_ip = new (allocator) DistanceCalculatorWithNorm<float, float, VecSimMetric_IP>(
        allocator, asym_ip, sym_ip, mean_sum_sq);
    auto *calc_l2 = new (allocator) DistanceCalculatorWithNorm<float, float, VecSimMetric_L2>(
        allocator, asym_l2, sym_l2, mean_sum_sq);

    int failures = 0;
    for (int trial = 0; trial < 100; ++trial) {
        float x[dim], y[dim];
        for (size_t i = 0; i < dim; ++i) {
            x[i] = dist(rng);
            y[i] = dist(rng);
        }

        void *x_blob = nullptr, *y_blob = nullptr;
        void *y_query_ip = nullptr, *y_query_l2 = nullptr;
        buildStorageBlob(allocator, x, mean, dim, VecSimMetric_IP, x_blob);
        buildStorageBlob(allocator, y, mean, dim, VecSimMetric_IP, y_blob);
        buildQueryBlob(allocator, y, mean, dim, VecSimMetric_IP, y_query_ip);

        void *x_blob_l2 = nullptr, *y_blob_l2 = nullptr;
        buildStorageBlob(allocator, x, mean, dim, VecSimMetric_L2, x_blob_l2);
        buildStorageBlob(allocator, y, mean, dim, VecSimMetric_L2, y_blob_l2);
        buildQueryBlob(allocator, y, mean, dim, VecSimMetric_L2, y_query_l2);

        float bf_ip = bruteForceIPDist(x, y, dim);
        float bf_l2 = bruteForceL2Dist(x, y, dim);

        float got_ip_asym = calc_ip->calcDistanceForQuery(x_blob, y_query_ip, dim);
        float got_ip_sym = calc_ip->calcDistance(x_blob, y_blob, dim);
        float got_l2_asym = calc_l2->calcDistanceForQuery(x_blob_l2, y_query_l2, dim);
        float got_l2_sym = calc_l2->calcDistance(x_blob_l2, y_blob_l2, dim);

        if (std::abs(got_ip_asym - bf_ip) > 0.1f)
            ++failures;
        if (std::abs(got_ip_sym - bf_ip) > 0.1f)
            ++failures;
        if (std::abs(got_l2_asym - bf_l2) > 0.1f)
            ++failures;
        if (std::abs(got_l2_sym - bf_l2) > 0.1f)
            ++failures;

        allocator->free_allocation(x_blob);
        allocator->free_allocation(y_blob);
        allocator->free_allocation(y_query_ip);
        allocator->free_allocation(x_blob_l2);
        allocator->free_allocation(y_blob_l2);
        allocator->free_allocation(y_query_l2);
    }

    EXPECT_EQ(failures, 0) << failures << " distance computations exceeded tolerance";

    delete calc_ip;
    delete calc_l2;
}

TEST(DistanceCalculatorWithNormTest, OddDimension_MatchesBruteForce) {
    // dim=13 is not a multiple of 4, so the storage blob's metadata (min, delta, sum,
    // [sum_squares], [mean_ip]) starts at an unaligned offset (dim * sizeof(uint8_t)). This
    // exercises the full normalized metadata layout (including mean_ip for IP) through an
    // unaligned load, unlike the other calculator tests which all use dim=8 or 16.
    std::shared_ptr<VecSimAllocator> allocator = VecSimAllocator::newVecsimAllocator();
    constexpr size_t dim = 13;
    float x[dim] = {3.0f, 1.0f, 4.0f, 1.0f, 5.0f, 3.0f, 2.0f, 6.0f, 2.0f, 7.0f, 1.0f, 4.0f, 2.0f};
    float y[dim] = {2.0f, 7.0f, 1.0f, 4.0f, 2.0f, 5.0f, 1.0f, 2.0f, 3.0f, 1.0f, 4.0f, 1.0f, 5.0f};
    float mean[dim] = {1.0f, 2.0f, 1.0f, 2.0f, 1.0f, 2.0f, 1.0f,
                       2.0f, 1.0f, 2.0f, 1.0f, 2.0f, 1.0f};
    float mean_sum_sq = computeMeanSumSquares(mean, dim);

    // IP
    {
        void *x_blob = nullptr, *y_blob = nullptr, *y_query = nullptr;
        buildStorageBlob(allocator, x, mean, dim, VecSimMetric_IP, x_blob);
        buildStorageBlob(allocator, y, mean, dim, VecSimMetric_IP, y_blob);
        buildQueryBlob(allocator, y, mean, dim, VecSimMetric_IP, y_query);

        auto asym_func = spaces::IP_SQ8_FP32_GetDistFunc(dim);
        auto sym_func = spaces::IP_SQ8_SQ8_GetDistFunc(dim);
        auto *calc = new (allocator) DistanceCalculatorWithNorm<float, float, VecSimMetric_IP>(
            allocator, asym_func, sym_func, mean_sum_sq);

        // Tolerance is loose: this checks the unaligned metadata layout/indexing is correct,
        // not SQ8 quantization precision. A misaligned/misindexed read produces gross garbage
        // (NaN or order-of-magnitude-off values), which this would still catch.
        float bf = bruteForceIPDist(x, y, dim);
        EXPECT_NEAR(calc->calcDistance(x_blob, y_blob, dim), bf, 1.0f)
            << "Symmetric IP vs brute-force (odd dim)";
        EXPECT_NEAR(calc->calcDistanceForQuery(x_blob, y_query, dim), bf, 1.0f)
            << "Asymmetric IP vs brute-force (odd dim)";

        allocator->free_allocation(x_blob);
        allocator->free_allocation(y_blob);
        allocator->free_allocation(y_query);
        delete calc;
    }

    // L2
    {
        void *x_blob = nullptr, *y_blob = nullptr, *y_query = nullptr;
        buildStorageBlob(allocator, x, mean, dim, VecSimMetric_L2, x_blob);
        buildStorageBlob(allocator, y, mean, dim, VecSimMetric_L2, y_blob);
        buildQueryBlob(allocator, y, mean, dim, VecSimMetric_L2, y_query);

        auto asym_func = spaces::L2_SQ8_FP32_GetDistFunc(dim);
        auto sym_func = spaces::L2_SQ8_SQ8_GetDistFunc(dim);
        auto *calc = new (allocator) DistanceCalculatorWithNorm<float, float, VecSimMetric_L2>(
            allocator, asym_func, sym_func, mean_sum_sq);

        float bf = bruteForceL2Dist(x, y, dim);
        EXPECT_NEAR(calc->calcDistance(x_blob, y_blob, dim), bf, 1.0f)
            << "Symmetric L2 vs brute-force (odd dim)";
        EXPECT_NEAR(calc->calcDistanceForQuery(x_blob, y_query, dim), bf, 1.0f)
            << "Asymmetric L2 vs brute-force (odd dim)";

        allocator->free_allocation(x_blob);
        allocator->free_allocation(y_blob);
        allocator->free_allocation(y_query);
        delete calc;
    }
}

template <typename T>
class SQ8TrainingStateTest : public ::testing::Test {};

using SQ8TrainingInputTypes = ::testing::Types<float, vecsim_types::float16>;
TYPED_TEST_SUITE(SQ8TrainingStateTest, SQ8TrainingInputTypes);

TYPED_TEST(SQ8TrainingStateTest, SumMeanAndRemoval) {
    auto allocator = VecSimAllocator::newVecsimAllocator();
    SQ8TrainingState state(allocator, 2, 2);
    const TypeParam first[] = {from_fp32<TypeParam>(1.5f), from_fp32<TypeParam>(-2.0f)};
    const TypeParam second[] = {from_fp32<TypeParam>(-0.5f), from_fp32<TypeParam>(4.0f)};
    EXPECT_EQ(state.getCount(), 0);
    EXPECT_FALSE(state.shouldFinalize());
    state.add(std::span<const TypeParam>(first));
    EXPECT_EQ(state.getCount(), 1);
    EXPECT_FALSE(state.shouldFinalize());
    state.add(std::span<const TypeParam>(second));
    EXPECT_EQ(state.getCount(), 2);
    EXPECT_TRUE(state.shouldFinalize());
    EXPECT_DOUBLE_EQ(state.getRunningSum()[0], 1.0);
    EXPECT_DOUBLE_EQ(state.getRunningSum()[1], 2.0);
    const auto mean = state.calculateMean();
    EXPECT_EQ(mean.getAllocator(), allocator);
    EXPECT_FLOAT_EQ(mean[0], 0.5f);
    EXPECT_FLOAT_EQ(mean[1], 1.0f);

    state.subtract(std::span<const TypeParam>(first));
    EXPECT_EQ(state.getCount(), 1);
    EXPECT_FALSE(state.shouldFinalize());
    const auto remaining_mean = state.calculateMean();
    EXPECT_FLOAT_EQ(remaining_mean[0], -0.5f);
    EXPECT_FLOAT_EQ(remaining_mean[1], 4.0f);
    state.subtract(std::span<const TypeParam>(second));
    EXPECT_EQ(state.getCount(), 0);
    EXPECT_FALSE(state.shouldFinalize());
    EXPECT_DOUBLE_EQ(state.getRunningSum()[0], 0.0);
    EXPECT_DOUBLE_EQ(state.getRunningSum()[1], 0.0);
}

TYPED_TEST(SQ8TrainingStateTest, ReplacementKeepsLiveCount) {
    auto allocator = VecSimAllocator::newVecsimAllocator();
    SQ8TrainingState state(allocator, 1, 2);
    const TypeParam original[] = {from_fp32<TypeParam>(2.0f)};
    const TypeParam replacement[] = {from_fp32<TypeParam>(8.0f)};
    state.add(std::span<const TypeParam>(original));
    state.subtract(std::span<const TypeParam>(original));
    state.add(std::span<const TypeParam>(replacement));
    EXPECT_EQ(state.getCount(), 1);
    EXPECT_FALSE(state.shouldFinalize());
    EXPECT_DOUBLE_EQ(state.getRunningSum()[0], 8.0);
    EXPECT_FLOAT_EQ(state.calculateMean()[0], 8.0f);
}

TEST(SQ8TrainingStateTest, ThresholdIsCapped) {
    auto allocator = VecSimAllocator::newVecsimAllocator();
    SQ8TrainingState state(allocator, 1, std::numeric_limits<size_t>::max());
    EXPECT_EQ(state.getNormalizationSetSize(), MAX_QUANT_NORMALIZATION_SET_SIZE);
    const float vector[] = {1.0f};
    for (size_t i = 0; i < MAX_QUANT_NORMALIZATION_SET_SIZE - 1; ++i) {
        state.add(std::span<const float>(vector));
    }
    EXPECT_FALSE(state.shouldFinalize());
    state.add(std::span<const float>(vector));
    EXPECT_TRUE(state.shouldFinalize());
    EXPECT_EQ(state.getCount(), MAX_QUANT_NORMALIZATION_SET_SIZE);
    EXPECT_FLOAT_EQ(state.calculateMean()[0], 1.0f);
}

TEST(SQ8TrainingStateTest, AccumulatesInFP64) {
    auto allocator = VecSimAllocator::newVecsimAllocator();
    SQ8TrainingState state(allocator, 1, 3);
    const float large[] = {16777216.0f};
    const float small[] = {1.0f};
    state.add(std::span<const float>(large));
    state.add(std::span<const float>(small));
    EXPECT_DOUBLE_EQ(state.getRunningSum()[0], 16777217.0);
    state.subtract(std::span<const float>(large));
    EXPECT_EQ(state.getCount(), 1);
    EXPECT_DOUBLE_EQ(state.getRunningSum()[0], 1.0);
    EXPECT_FLOAT_EQ(state.calculateMean()[0], 1.0f);
}

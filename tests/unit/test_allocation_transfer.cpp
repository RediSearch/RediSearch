/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#include "gtest/gtest.h"
#include "VecSim/memory/vecsim_malloc.h"
#include <cstdint>
#include <cstdlib>

TEST(AllocationTransferTest, PreservesAllocationAndBalancesAccounting) {
    for (unsigned char alignment : {0, 16, 64}) {
        for (size_t size : {0, 1, 127}) {
            SCOPED_TRACE(::testing::Message()
                         << "alignment=" << int(alignment) << " size=" << size);
            auto source = VecSimAllocator::newVecsimAllocator();
            auto destination = VecSimAllocator::newVecsimAllocator();
            const auto source_before = source->getAllocationSize();
            const auto destination_before = destination->getAllocationSize();
            auto *ptr = static_cast<unsigned char *>(source->allocate_aligned(size, alignment));
            ASSERT_NE(ptr, nullptr);
            for (size_t i = 0; i < size; ++i) {
                ptr[i] = static_cast<unsigned char>(i);
            }
            const auto charged = source->getAllocationSize() - source_before;
            source->transferAllocationTo(ptr, *destination);
            EXPECT_EQ(source->getAllocationSize(), source_before);
            EXPECT_EQ(destination->getAllocationSize(), destination_before + charged);
            if (alignment) {
                EXPECT_EQ(reinterpret_cast<uintptr_t>(ptr) % alignment, 0u);
            }
            for (size_t i = 0; i < size; ++i) {
                EXPECT_EQ(ptr[i], static_cast<unsigned char>(i));
            }
            source.reset();
            destination->free_allocation(ptr);
            EXPECT_EQ(destination->getAllocationSize(), destination_before);
        }
    }
}

TEST(AllocationTransferTest, NullAndSelfTransfersPreserveAccounting) {
    auto source = VecSimAllocator::newVecsimAllocator();
    auto destination = VecSimAllocator::newVecsimAllocator();
    const auto baseline = source->getAllocationSize();
    source->transferAllocationTo(nullptr, *destination);
    EXPECT_EQ(source->getAllocationSize(), baseline);
    EXPECT_EQ(destination->getAllocationSize(), baseline);
    void *ptr = source->callocate(37);
    ASSERT_NE(ptr, nullptr);
    const auto allocated = source->getAllocationSize();
    source->transferAllocationTo(ptr, *source);
    EXPECT_EQ(source->getAllocationSize(), allocated);
    source->transferAllocationTo(ptr, *destination);
    destination->transferAllocationTo(ptr, *source);
    EXPECT_EQ(source->getAllocationSize(), allocated);
    EXPECT_EQ(destination->getAllocationSize(), baseline);
    source->free_allocation(ptr);
    EXPECT_EQ(source->getAllocationSize(), baseline);
}

TEST(AllocationTransferTest, DoesNotAllocateOrFree) {
    static size_t allocations = 0;
    static size_t frees = 0;
    struct RestoreMemoryFunctions {
        ~RestoreMemoryFunctions() {
            VecSimAllocator::setMemoryFunctions({malloc, calloc, realloc, free});
        }
    } restore;
    VecSimAllocator::setMemoryFunctions({[](size_t n) -> void * {
                                             ++allocations;
                                             return malloc(n);
                                         },
                                         [](size_t n, size_t size) -> void * {
                                             ++allocations;
                                             return calloc(n, size);
                                         },
                                         [](void *p, size_t n) -> void * {
                                             ++allocations;
                                             return realloc(p, n);
                                         },
                                         [](void *p) {
                                             ++frees;
                                             free(p);
                                         }});
    auto source = VecSimAllocator::newVecsimAllocator();
    auto destination = VecSimAllocator::newVecsimAllocator();
    void *ptr = source->allocate_aligned(127, 64);
    ASSERT_NE(ptr, nullptr);
    const auto allocations_before = allocations;
    const auto frees_before = frees;
    source->transferAllocationTo(ptr, *destination);
    EXPECT_EQ(allocations, allocations_before);
    EXPECT_EQ(frees, frees_before);
    destination->free_allocation(ptr);
    EXPECT_EQ(frees, frees_before + 1);
}

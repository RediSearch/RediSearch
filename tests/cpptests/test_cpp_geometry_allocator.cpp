/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#include "gtest/gtest.h"
#include "redismock/redismock.h"

#include "geometry/allocator/allocator.hpp"
#include "geometry/allocator/stateful_allocator.hpp"
#include "geometry/allocator/tracking_allocator.hpp"

#include <cstdint>
#include <limits>
#include <new>

using namespace RediSearch::Allocator;

namespace {
using Elem = std::uint64_t;
// Smallest element count for which n * sizeof(Elem) overflows size_t.
constexpr std::size_t kOverflowCount = std::numeric_limits<std::size_t>::max() / sizeof(Elem) + 1;
}  // namespace

TEST(GeometryAllocatorTest, OverflowThrows) {
  EXPECT_THROW((void)Allocator<Elem>::allocate(kOverflowCount), std::bad_alloc);
}

TEST(GeometryAllocatorTest, StatefulOverflowThrowsAndKeepsCount) {
  StatefulAllocator<Elem> alloc;
  EXPECT_THROW((void)alloc.allocate(kOverflowCount), std::bad_alloc);
  EXPECT_EQ(alloc.report(), 0u);
}

TEST(GeometryAllocatorTest, TrackingOverflowThrowsAndKeepsCount) {
  std::size_t tracked = 0;
  TrackingAllocator<Elem> alloc{tracked};
  EXPECT_THROW((void)alloc.allocate(kOverflowCount), std::bad_alloc);
  EXPECT_EQ(tracked, 0u);
}

TEST(GeometryAllocatorTest, NormalAllocationIsTracked) {
  std::size_t tracked = 0;
  TrackingAllocator<Elem> alloc{tracked};
  Elem* p = alloc.allocate(4);
  ASSERT_NE(p, nullptr);
  EXPECT_EQ(tracked, 4 * sizeof(Elem));
  alloc.deallocate(p, 4);
  EXPECT_EQ(tracked, 0u);
}

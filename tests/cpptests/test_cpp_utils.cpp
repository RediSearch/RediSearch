/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"
#include <stdlib.h>
#include <cmath>

#include "util/heap_doubles.h"


class UtilsTest : public ::testing::Test {};

TEST_F(UtilsTest, testDoublesHeap) {
  size_t n = 100;
  size_t prime = 31; // GCD(100, 31) = 1
  double_heap_t *heap = double_heap_new(n);
  ASSERT_TRUE(heap != NULL);

  // Test building a heap
  for (size_t ii = 0; ii < n; ++ii) {
    double_heap_add_raw(heap, (ii * prime) % n);
  }
  double_heap_heapify(heap);
  for (size_t ii = 0; ii < n; ++ii) {
    ASSERT_DOUBLE_EQ(double_heap_peek(heap), n - ii - 1);
    double_heap_pop(heap);
  }

  // Test adding elements
  prime = 17; // GCD(100, 17) = 1
  for (size_t ii = 0; ii < n; ++ii) {
    double_heap_push(heap, (ii * prime) % n);
  }
  for (size_t ii = 0; ii < n; ++ii) {
    ASSERT_DOUBLE_EQ(double_heap_peek(heap), n - ii - 1);
    double_heap_pop(heap);
  }

  // Test finding top k elements
  prime = 3; // GCD(10, 3) = 1
  for (size_t ii = 0; ii < n / 10; ++ii) {
    double_heap_push(heap, (ii * prime) % n);
  }
  ASSERT_EQ(heap->size, n / 10);
  for (size_t ii = n / 10; ii < n; ++ii) {
    double cur = (ii * prime) % n;
    if (cur < double_heap_peek(heap)) {
      double_heap_replace(heap, cur);
    }
    ASSERT_EQ(heap->size, n / 10) << "Expected constant size. size is " << heap->size;
  }
  for (size_t ii = heap->size; ii > 0; --ii) {
    // Expect the bottom 10 elements in reverse order [9, 8, ..., 0]
    ASSERT_DOUBLE_EQ(double_heap_peek(heap), ii - 1);
    double_heap_pop(heap);
  }

  double_heap_free(heap);
}

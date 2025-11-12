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

#include "src/util/heap_doubles.h"
#include "src/hll/hll.h"


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

TEST_F(UtilsTest, testHLL) {
  struct HLL hll, hll1, hll2;

  // Test Bad init
  ASSERT_EQ(hll_init(&hll, 3), -1) << "Expected error for bits < 4";
  ASSERT_EQ(hll_init(&hll, 21), -1) << "Expected error for bits > 20";
  ASSERT_EQ(hll_load(&hll, NULL, 1), -1) << "Expected error for bits < 4";
  ASSERT_EQ(hll_load(&hll, NULL, 42), -1) << "Expected error for registers length not a power of 2";
  ASSERT_EQ(hll_set_registers(&hll, NULL, 42), -1) << "Expected error for registers length not a power of 2";

  // Test init
  ASSERT_EQ(hll_init(&hll1, 4), 0);
  ASSERT_EQ(hll1.bits, 4)         << "Expected bits to be 4";
  ASSERT_EQ(hll1.size, 16)        << "Expected size to be 2^4";
  ASSERT_EQ(hll1.rank_bits, 28)   << "Expected rank_bits to be 32 - 4";
  ASSERT_EQ(hll1.cachedCard, 0)   << "Expected cachedCard to be 0";
  ASSERT_EQ(hll_count(&hll1), 0)  << "Expected count to be 0";

  ASSERT_EQ(hll_init(&hll2, 5), 0);
  ASSERT_EQ(hll2.bits, 5)         << "Expected bits to be 5";
  ASSERT_EQ(hll2.size, 32)        << "Expected size to be 2^5";
  ASSERT_EQ(hll2.rank_bits, 27)   << "Expected rank_bits to be 32 - 5";
  ASSERT_EQ(hll2.cachedCard, 0)   << "Expected cachedCard to be 0";
  ASSERT_EQ(hll_count(&hll2), 0)  << "Expected count to be 0";

  hll_add(&hll1, "foo", 3);
  hll_add(&hll2, "bar", 3);
  ASSERT_EQ(hll_count(&hll1), 1);
  ASSERT_EQ(hll_count(&hll2), 1);
  hll_clear(&hll2);

  // Test 2 HLLs intersection
  ASSERT_EQ(hll_merge(&hll1, &hll2), -1) << "Expected error for different sizes";

  ASSERT_EQ(hll_set_registers(&hll1, hll2.registers, hll2.size), 0) << "Expected success for different sizes";
  ASSERT_EQ(hll1.bits, 5)         << "Expected bits to be 5 as of hll2";
  ASSERT_EQ(hll1.size, 32)        << "Expected size to be 2^5 as of hll2";
  ASSERT_EQ(hll1.rank_bits, 27)   << "Expected rank_bits to be 32 - 5 as of hll2";
  ASSERT_EQ(hll_count(&hll1), 0)  << "Expected count to be 0";

  // Test add
  for (size_t ii = 0; ii < 100; ++ii) {
    double d = ii * 1.1;
    hll_add(&hll1, &d, sizeof(d));
  }
  // Test count estimation
  ASSERT_GE(hll_count(&hll1), 100 * (1 - 1.04 / std::sqrt(hll1.size)));
  ASSERT_LE(hll_count(&hll1), 100 * (1 + 1.04 / std::sqrt(hll1.size)));

  hll_clear(&hll1);
  ASSERT_EQ(hll_count(&hll1), 0) << "Expected count to be 0 after clear";

  hll_destroy(&hll1);
  hll_destroy(&hll2);
}

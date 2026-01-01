/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/


#include "gtest/gtest.h"
#include "src/util/circular_buffer.h"

#include <thread>

class CircularBufferTest : public ::testing::Test {};

TEST_F(CircularBufferTest, testEmpty) {
  CircularBuffer buff = CircularBuffer_New(sizeof(int), 16);

  // a new circular buffer should be empty
  ASSERT_EQ(CircularBuffer_Empty(buff), true);

  // item count of an empty circular buffer should be 0
  ASSERT_EQ(CircularBuffer_ItemCount(buff), 0);

  // item size should be 4
  ASSERT_EQ(CircularBuffer_ItemSize(buff), 4);

  // buffer should have available slots in it i.e. not full
  ASSERT_EQ(CircularBuffer_Full(buff), false);

  // clean up
  CircularBuffer_Free(buff);
}

TEST_F(CircularBufferTest, test_CircularBufferPopulation) {
  int n;
  int cap = 16;
  CircularBuffer buff = CircularBuffer_New(sizeof(int), cap);

  // remove item from an empty buffer should report failure
  ASSERT_TRUE(CircularBuffer_Read(buff, &n) == NULL);

  //--------------------------------------------------------------------------
  // fill buffer
  //--------------------------------------------------------------------------
  for (int i = 0; i < cap; i++) {
    // make sure item was added
    ASSERT_EQ(CircularBuffer_Add(buff, &i), 1);
    // validate buffer's item count
    ASSERT_EQ(CircularBuffer_ItemCount(buff), i+1);
  }
  ASSERT_EQ(CircularBuffer_Full(buff), true);

  // forcefully try to overflow buffer
  for (int i = 0; i < 10; i++) {
    ASSERT_EQ(CircularBuffer_Add(buff, &n), 0);
  }

  //--------------------------------------------------------------------------
  // empty buffer
  //--------------------------------------------------------------------------
  for (int i = 0; i < cap; i++) {
    // get item from buffer
    ASSERT_TRUE(CircularBuffer_Read(buff, &n) != NULL);

    // validate item's value
    ASSERT_EQ(n, i);
  }
  ASSERT_EQ(CircularBuffer_Empty(buff), true);

  // forcefully try to read an item from an empty buffer
  for (int i = 0; i < 10; i++) {
    ASSERT_TRUE(CircularBuffer_Read(buff, &n) == NULL);
  }

  // clean up
  CircularBuffer_Free(buff);
}

TEST_F(CircularBufferTest, test_CircularBuffer_Circularity) {
  int n;
  int cap = 16;
  CircularBuffer buff = CircularBuffer_New(sizeof(int), cap);

  //--------------------------------------------------------------------------
  // fill buffer
  //--------------------------------------------------------------------------
  for (int i = 0; i < cap; i++) {
    // make sure item was added
    ASSERT_EQ(CircularBuffer_Add(buff, &i), 1);
  }
  ASSERT_EQ(CircularBuffer_Full(buff), true);

  // try to overflow buffer
  ASSERT_EQ(CircularBuffer_Add(buff, &n), 0);

  // removing an item should make space in the buffer
  ASSERT_TRUE(CircularBuffer_Read(buff, &n) != NULL);
  ASSERT_EQ(CircularBuffer_Add(buff, &n), 1);

  //--------------------------------------------------------------------------
  // clear buffer
  //--------------------------------------------------------------------------

  while (CircularBuffer_Empty(buff) == false) {
    CircularBuffer_Read(buff, &n);
  }

  // add/remove elements cycling through the buffer multiple times
  for (int i = 0; i < cap * 4; i++) {
    ASSERT_EQ(CircularBuffer_Add(buff, &i), 1);
    ASSERT_TRUE(CircularBuffer_Read(buff, &n) != NULL);
    ASSERT_EQ(n, i);
  }
  ASSERT_EQ(CircularBuffer_Empty(buff), true);

  // clean up
  CircularBuffer_Free(buff);
}

TEST_F(CircularBufferTest, test_CircularBuffer_free) {
  //--------------------------------------------------------------------------
  // fill a buffer of size 16 with int *
  //--------------------------------------------------------------------------

  uint cap = 16;
  CircularBuffer buff = CircularBuffer_New(sizeof(int64_t *), cap);
  for (int i = 0; i < cap; i++) {
    int64_t *j = (int64_t *)malloc(sizeof(int64_t));
    CircularBuffer_Add(buff, (void*)&j);
  }

  //--------------------------------------------------------------------------
  // free the buffer
  //--------------------------------------------------------------------------

  for (int i = 0; i < cap; i++) {
    int64_t *item;
    CircularBuffer_Read(buff, &item);
    free(item);
  }

  CircularBuffer_Free(buff);
}

TEST_F(CircularBufferTest, test_CircularBuffer_Reserve) {

  // -------------------------------------------------------------------------
  // fill a buffer of size 16 with 32 integers
  // -------------------------------------------------------------------------

  uint cap = 16;
  CircularBuffer buff = CircularBuffer_New(sizeof(int), cap);
  bool wasFull;
  for (int i = 0; i < 2 * cap; i++) {
    int *item = (int *)CircularBuffer_Reserve(buff, &wasFull);
    ASSERT_EQ(wasFull, i < cap ? false : true);
    *item = i;
  }

  // make sure item count did not exceeded buffer cap
  ASSERT_EQ(CircularBuffer_ItemCount(buff), CircularBuffer_Cap(buff));

  // -------------------------------------------------------------------------
  // assert override correctness
  // -------------------------------------------------------------------------

  for (uint i = 0; i < 16; i++) {
    int item;
    void *res = CircularBuffer_Read(buff, &item);
    ASSERT_TRUE(res != NULL);
    ASSERT_EQ(item, (i + 16));
    ASSERT_EQ(CircularBuffer_ItemCount(buff), 16-i-1);
  }

  // -------------------------------------------------------------------------
  // free the buffer
  // -------------------------------------------------------------------------

  CircularBuffer_Free(buff);
}

TEST_F(CircularBufferTest, test_CircularBuffer_ResetReader) {

  // -------------------------------------------------------------------------
  // fill a buffer of size 16 with 18 integers
  // -------------------------------------------------------------------------

  uint cap = 16;
  CircularBuffer buff = CircularBuffer_New(sizeof(int), cap);
  for (int i = 0; i < cap + 2; i++) {
    int *item = (int *)CircularBuffer_Reserve(buff, NULL);
    *item = i;
  }

  // -------------------------------------------------------------------------
  // reset reader
  // -------------------------------------------------------------------------
  CircularBuffer_ResetReader(buff);

  // -------------------------------------------------------------------------
  // assert pointer correctness (should start from 2)
  // -------------------------------------------------------------------------
  for (uint i = 0; i < 16; i++) {
    int item;
    void *res = CircularBuffer_Read(buff, &item);
    ASSERT_TRUE(res != NULL);
    ASSERT_EQ(item, i + 2);
    ASSERT_EQ(CircularBuffer_ItemCount(buff), 16-i-1);
  }

  // -------------------------------------------------------------------------
  // free the buffer
  // -------------------------------------------------------------------------

  CircularBuffer_Free(buff);
}

#define NUM_THREADS 10
#define NUM_ITEMS_PER_THREAD 100
#define NUM_ITEMS (NUM_THREADS * NUM_ITEMS_PER_THREAD)
#define SUM_ITEMS (NUM_ITEMS * (NUM_ITEMS - 1) / 2)

void thread_AddFunc(CircularBuffer cb, int thread_id) {
  for (int i = 0; i < NUM_ITEMS_PER_THREAD; i++) {
    int item = thread_id * NUM_ITEMS_PER_THREAD + i;
    CircularBuffer_Add(cb, &item);
  }
}

TEST_F(CircularBufferTest, test_CircularBuffer_multiAdd) {
  CircularBuffer cb = CircularBuffer_New(sizeof(int), NUM_THREADS * NUM_ITEMS_PER_THREAD);
  std::thread threads[NUM_THREADS];

  for (int i = 0; i < NUM_THREADS; i++) {
    threads[i] = std::thread(thread_AddFunc, cb, i);
  }

  for (int i = 0; i < NUM_THREADS; i++) {
    threads[i].join();
  }

  // Verify the buffer contents
  int item = -1;
  uint16_t n_items = CircularBuffer_ItemCount(cb);
  ASSERT_EQ(n_items, NUM_THREADS * NUM_ITEMS_PER_THREAD);

  uint16_t old_item;
  size_t sum = 0;
  for (size_t i = 0; i < n_items; i++) {
    CircularBuffer_Read(cb, &item);
    sum += item;
  }

  // Verify that all items have been read
  ASSERT_EQ(sum, SUM_ITEMS);
  CircularBuffer_Free(cb);
}

void thread_ReserveFunc(CircularBuffer cb, int thread_id) {
  for (int i = 0; i < NUM_ITEMS_PER_THREAD; i++) {
    int item = thread_id * NUM_ITEMS_PER_THREAD + i;
    void *slot = CircularBuffer_Reserve(cb, NULL);
    *(int *)slot = item;
  }
}

TEST_F(CircularBufferTest, test_CircularBuffer_multiReserve) {
  CircularBuffer cb = CircularBuffer_New(sizeof(int), NUM_THREADS * NUM_ITEMS_PER_THREAD);
  std::thread threads[NUM_THREADS];

  for (int i = 0; i < NUM_THREADS; i++) {
    threads[i] = std::thread(thread_ReserveFunc, cb, i);
  }

  for (int i = 0; i < NUM_THREADS; i++) {
    threads[i].join();
  }

  // Verify the buffer contents (this is a simple example, you may need more complex verification)
  int item = -1;
  uint16_t n_items = CircularBuffer_ItemCount(cb);
  ASSERT_EQ(n_items, NUM_THREADS * NUM_ITEMS_PER_THREAD);

  uint16_t old_item;
  size_t sum = 0;
  for (size_t i = 0; i < n_items; i++) {
    CircularBuffer_Read(cb, &item);
    sum += item;
  }

  // Verify that all items have been read
  ASSERT_EQ(sum, SUM_ITEMS);
  CircularBuffer_Free(cb);
}

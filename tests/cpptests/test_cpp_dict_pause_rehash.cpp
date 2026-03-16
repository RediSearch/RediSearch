/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"

extern "C" {
#include "util/dict.h"
#include "rmalloc.h"
}

#include <thread>
#include <atomic>
#include <vector>

class DictPauseRehashTest : public ::testing::Test {
protected:
  static uint64_t hashFunc(const void *key) {
    return (uint64_t)(uintptr_t)key;
  }

  static dictType testDictType;

  void SetUp() override {
    d = RS_dictCreate(&testDictType, nullptr);
  }

  void TearDown() override {
    if (d) {
      RS_dictRelease(d);
      d = nullptr;
    }
  }

  dict *d = nullptr;
};

dictType DictPauseRehashTest::testDictType = {
  .hashFunction = hashFunc,
  .keyDup = nullptr,
  .valDup = nullptr,
  .keyCompare = nullptr,
  .keyDestructor = nullptr,
  .valDestructor = nullptr,
};

// Test basic pause/resume functionality
TEST_F(DictPauseRehashTest, BasicPauseResume) {
  // Initial state: pauserehash should be 0
  ASSERT_EQ(d->pauserehash, 0);

  // Pause should succeed and increment counter
  ASSERT_TRUE(dictPauseRehashing(d));
  ASSERT_EQ(d->pauserehash, 1);

  // Resume should succeed and decrement counter
  ASSERT_TRUE(dictResumeRehashing(d));
  ASSERT_EQ(d->pauserehash, 0);
}

// Test that multiple pauses stack correctly
TEST_F(DictPauseRehashTest, MultiplePausesStack) {
  ASSERT_EQ(d->pauserehash, 0);

  // Multiple pauses should stack
  ASSERT_TRUE(dictPauseRehashing(d));
  ASSERT_EQ(d->pauserehash, 1);

  ASSERT_TRUE(dictPauseRehashing(d));
  ASSERT_EQ(d->pauserehash, 2);

  ASSERT_TRUE(dictPauseRehashing(d));
  ASSERT_EQ(d->pauserehash, 3);

  // Resumes should decrement one at a time
  ASSERT_TRUE(dictResumeRehashing(d));
  ASSERT_EQ(d->pauserehash, 2);

  ASSERT_TRUE(dictResumeRehashing(d));
  ASSERT_EQ(d->pauserehash, 1);

  ASSERT_TRUE(dictResumeRehashing(d));
  ASSERT_EQ(d->pauserehash, 0);
}

// Test that rehashing is blocked when paused
TEST_F(DictPauseRehashTest, RehashBlockedWhenPaused) {
  // Add enough elements to trigger expansion
  const int numElements = 1000;
  for (int i = 0; i < numElements; i++) {
    RS_dictAdd(d, (void *)(uintptr_t)(i + 1), (void *)(uintptr_t)(i + 1));
  }

  // Force rehashing to start by expanding
  RS_dictExpand(d, numElements * 2);
  ASSERT_TRUE(dictIsRehashing(d));

  // Get current rehashidx
  long initialRehashIdx = d->rehashidx;

  // Pause rehashing
  dictPauseRehashing(d);

  // Do many finds - normally these would trigger _dictRehashStep
  for (int i = 0; i < 100; i++) {
    RS_dictFind(d, (void *)(uintptr_t)(i + 1));
  }

  // rehashidx should not have changed because rehashing is paused
  ASSERT_EQ(d->rehashidx, initialRehashIdx);

  // Resume and do more finds
  dictResumeRehashing(d);

  for (int i = 0; i < 100; i++) {
    RS_dictFind(d, (void *)(uintptr_t)(i + 1));
  }

  // Now rehashidx should have advanced (or completed)
  ASSERT_TRUE(d->rehashidx > initialRehashIdx || d->rehashidx == -1);
}

// Test concurrent pause/resume from multiple threads
TEST_F(DictPauseRehashTest, ConcurrentPauseResume) {
  const int numThreads = 10;
  const int iterationsPerThread = 1000;
  std::atomic<int> readyCount{0};
  std::atomic<bool> start{false};

  std::vector<std::thread> threads;
  threads.reserve(numThreads);

  for (int t = 0; t < numThreads; t++) {
    threads.emplace_back([this, &readyCount, &start, iterationsPerThread]() {
      readyCount++;
      while (!start) {
        std::this_thread::yield();
      }

      for (int i = 0; i < iterationsPerThread; i++) {
        dictPauseRehashing(d);
        // Small delay to increase contention
        std::this_thread::yield();
        dictResumeRehashing(d);
      }
    });
  }

  // Wait for all threads to be ready
  while (readyCount < numThreads) {
    std::this_thread::yield();
  }

  // Start all threads at once
  start = true;

  // Wait for all threads to complete
  for (auto &t : threads) {
    t.join();
  }

  // After all threads complete, pauserehash should be back to 0
  ASSERT_EQ(d->pauserehash, 0);
}

// This test attempts to expose the RELAXED memory ordering issue.
// NOTE: This race is very hard to reproduce on x86 due to strong memory ordering.
// On ARM or with specific timing, this might fail more often.
// The test runs many iterations trying to catch the window where:
// - Thread A pauses rehashing (RELAXED write)
// - Thread B reads pauserehash and still sees 0 (stale cache)
// - Thread B proceeds to rehash while Thread A expects it paused
TEST_F(DictPauseRehashTest, DISABLED_RelaxedOrderingRace) {
  const int iterations = 10000;
  std::atomic<int> raceDetected{0};
  std::atomic<bool> stop{false};

  for (int iter = 0; iter < iterations && !raceDetected; iter++) {
    // Reset dict for each iteration
    if (d) RS_dictRelease(d);
    d = RS_dictCreate(&testDictType, nullptr);

    // Add elements to make rehashing possible
    for (int i = 0; i < 100; i++) {
      RS_dictAdd(d, (void *)(uintptr_t)(i + 1), (void *)(uintptr_t)(i + 1));
    }

    // Force rehashing
    RS_dictExpand(d, 200);
    if (!dictIsRehashing(d)) continue;

    long initialRehashIdx = d->rehashidx;

    std::atomic<int> readyCount{0};
    std::atomic<bool> start{false};

    // Thread A: pauses rehashing
    std::thread pauser([this, &readyCount, &start]() {
      readyCount++;
      while (!start) std::this_thread::yield();
      dictPauseRehashing(d);
    });

    // Thread B: tries to do a find (which triggers _dictRehashStep)
    std::thread finder([this, &readyCount, &start]() {
      readyCount++;
      while (!start) std::this_thread::yield();
      // Small delay to try to hit the race window
      for (volatile int i = 0; i < 10; i++);
      RS_dictFind(d, (void *)(uintptr_t)1);
    });

    while (readyCount < 2) std::this_thread::yield();
    start = true;

    pauser.join();
    finder.join();

    // If rehashidx advanced while pauserehash was being set, we caught a race
    // This is a heuristic - it might have false negatives
    if (d->pauserehash > 0 && d->rehashidx > initialRehashIdx && d->rehashidx != -1) {
      raceDetected++;
    }

    dictResumeRehashing(d);
  }

  // We don't ASSERT here because this race is very hard to trigger
  // Just report if we saw it
  if (raceDetected > 0) {
    std::cout << "Detected potential RELAXED ordering race " << raceDetected
              << " times out of " << iterations << " iterations" << std::endl;
  } else {
    std::cout << "No RELAXED ordering race detected in " << iterations
              << " iterations (expected on x86)" << std::endl;
  }
}

// Test that dictFind doesn't rehash when paused even under concurrent access
TEST_F(DictPauseRehashTest, ConcurrentFindWithPause) {
  // Add elements
  const int numElements = 500;
  for (int i = 0; i < numElements; i++) {
    RS_dictAdd(d, (void *)(uintptr_t)(i + 1), (void *)(uintptr_t)(i + 1));
  }

  // Force rehashing
  RS_dictExpand(d, numElements * 2);
  ASSERT_TRUE(dictIsRehashing(d));

  // Pause rehashing - simulating what query threads should do
  dictPauseRehashing(d);
  long initialRehashIdx = d->rehashidx;

  const int numThreads = 4;
  const int findsPerThread = 200;
  std::atomic<int> readyCount{0};
  std::atomic<bool> start{false};

  std::vector<std::thread> threads;
  threads.reserve(numThreads);

  for (int t = 0; t < numThreads; t++) {
    threads.emplace_back([this, &readyCount, &start, findsPerThread, numElements]() {
      readyCount++;
      while (!start) {
        std::this_thread::yield();
      }

      for (int i = 0; i < findsPerThread; i++) {
        int key = (i % numElements) + 1;
        dictEntry *entry = RS_dictFind(d, (void *)(uintptr_t)key);
        // Entry should be found
        ASSERT_NE(entry, nullptr);
      }
    });
  }

  while (readyCount < numThreads) {
    std::this_thread::yield();
  }

  start = true;

  for (auto &t : threads) {
    t.join();
  }

  // Rehash index should NOT have advanced because we paused
  ASSERT_EQ(d->rehashidx, initialRehashIdx);

  // Resume and verify rehashing can proceed
  dictResumeRehashing(d);

  // Do some finds to trigger rehashing
  for (int i = 0; i < 100; i++) {
    RS_dictFind(d, (void *)(uintptr_t)(i + 1));
  }

  // Now rehashidx should have advanced
  ASSERT_TRUE(d->rehashidx > initialRehashIdx || d->rehashidx == -1);
}


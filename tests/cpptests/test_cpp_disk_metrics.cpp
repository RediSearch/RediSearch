/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "gtest/gtest.h"
#include "common.h"
#include "module.h"
#include "concurrent_ctx.h"
extern "C" {
#include "util/disk_metrics.h"
#include "search_disk.h"
extern RedisSearchDiskAPI* disk;
extern RedisSearchDisk* disk_db;
}
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>
#ifndef _WIN32
#include <sys/wait.h>
#include <unistd.h>
#endif

class DiskMetricsTest : public ::testing::Test {
 protected:
  decltype(RedisModule_CreateTimer) savedCreate;
  decltype(RedisModule_StopTimer) savedStop;
  std::mutex mutex;
  std::condition_variable changed;
  unsigned calls = 0;
  bool release = false;
  static inline RedisModuleTimerProc timerProc;
  static inline void* timerData;
  static inline unsigned timersCreated;
  static inline unsigned timersStopped;

  void SetUp() override {
    savedCreate = RedisModule_CreateTimer;
    savedStop = RedisModule_StopTimer;
    timersCreated = 0;
    timersStopped = 0;
    timerProc = nullptr;
    timerData = nullptr;
    RedisModule_CreateTimer = [](RedisModuleCtx*, mstime_t, RedisModuleTimerProc proc, void* data) {
      timerProc = proc;
      timerData = data;
      return RedisModuleTimerID{++timersCreated};
    };
    RedisModule_StopTimer = [](RedisModuleCtx*, RedisModuleTimerID, void**) {
      ++timersStopped;
      return REDISMODULE_OK;
    };
  }
  void TearDown() override {
    {
      std::lock_guard<std::mutex> lock(mutex);
      release = true;
      changed.notify_all();
    }
    DiskMetrics_Stop(nullptr);
    RedisModule_CreateTimer = savedCreate;
    RedisModule_StopTimer = savedStop;
  }
  static bool blockedBatch(void* context) {
    auto& self = *static_cast<DiskMetricsTest*>(context);
    std::unique_lock<std::mutex> lock(self.mutex);
    ++self.calls;
    self.changed.notify_all();
    self.changed.wait(lock, [&] { return self.release; });
    return false;
  }
  bool await(unsigned count) {
    std::unique_lock<std::mutex> lock(mutex);
    return changed.wait_for(lock, std::chrono::seconds(5), [&] { return calls >= count; });
  }
};

TEST_F(DiskMetricsTest, StopCancelsTimerAndRejectsFurtherWork) {
  release = true;
  ASSERT_TRUE(DiskMetrics_Start(nullptr, blockedBatch, this));
  ASSERT_TRUE(await(1));
  DiskMetrics_Stop(nullptr);
  EXPECT_EQ(timersStopped, 1u);
  EXPECT_FALSE(DiskMetrics_Wake());
  EXPECT_FALSE(DiskMetrics_BeginWait());
  DiskMetrics_Stop(nullptr);
  EXPECT_EQ(timersStopped, 1u);
}

TEST_F(DiskMetricsTest, TimerCollectsAndRearmsAfterInitialCollection) {
  release = true;
  ASSERT_TRUE(DiskMetrics_Start(nullptr, blockedBatch, this));
  ASSERT_TRUE(await(1));
  EXPECT_EQ(timersCreated, 1u);
  ASSERT_NE(timerProc, nullptr);
  const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
  while (std::chrono::steady_clock::now() < deadline) {
    const auto previous = timersCreated;
    timerProc(nullptr, timerData);
    EXPECT_EQ(timersCreated, previous + 1);
    std::unique_lock<std::mutex> lock(mutex);
    if (changed.wait_for(lock, std::chrono::milliseconds(10), [this] { return calls >= 3; })) {
      return;
    }
  }
  FAIL() << "Timer did not collect again after the initial job";
}

TEST_F(DiskMetricsTest, WakeSubmitsWithoutAnEventLoopAndDoesNotWaitForNativeWork) {
  ASSERT_TRUE(DiskMetrics_Start(nullptr, blockedBatch, this));
  ASSERT_TRUE(await(1));
  const auto started = std::chrono::steady_clock::now();
  ASSERT_TRUE(DiskMetrics_Wake());
  EXPECT_LT(std::chrono::steady_clock::now() - started, std::chrono::seconds(1));
  {
    std::lock_guard<std::mutex> lock(mutex);
    release = true;
    changed.notify_all();
  }
  ASSERT_TRUE(await(2));
}

TEST_F(DiskMetricsTest, PauseDrainsNativeWorkAndNestedResumeRemainsPaused) {
  ASSERT_TRUE(DiskMetrics_Start(nullptr, blockedBatch, this));
  ASSERT_TRUE(await(1));
  std::atomic<bool> paused{false};
  std::thread pause([&] {
    DiskMetrics_Pause();
    paused.store(true);
  });
  {
    std::lock_guard<std::mutex> lock(mutex);
    EXPECT_FALSE(paused.load());
    release = true;
    changed.notify_all();
  }
  pause.join();
  EXPECT_TRUE(paused.load());
  EXPECT_FALSE(DiskMetrics_Wake());
  EXPECT_FALSE(DiskMetrics_BeginWait());
  DiskMetrics_Pause();
  EXPECT_FALSE(DiskMetrics_Resume());
  EXPECT_FALSE(DiskMetrics_Wake());
  EXPECT_TRUE(DiskMetrics_Resume());
  EXPECT_TRUE(DiskMetrics_BeginWait());
  DiskMetrics_EndWait();
  ASSERT_TRUE(await(2));
}

#ifndef _WIN32
TEST_F(DiskMetricsTest, GlobalCleanupDrainsCollectionBeforeDestroyingIndexesAndDiskContext) {
  GTEST_FLAG_SET(death_test_style, "threadsafe");
  ASSERT_EXIT(
      {
        struct State {
          std::mutex mutex;
          std::condition_variable changed;
          bool collecting = false;
          bool release = false;
          bool retainedIndexes = false;
          bool collectionFinished = false;
          std::atomic<bool> otherWorkerFinished{false};
          bool closed = false;
        } state;
        static State* active;
        active = &state;
        RedisSearchDiskAPI api{};
        api.metrics.getCollector = [](RedisSearchDisk*) {
          return reinterpret_cast<RedisSearchDiskMetricsCollector*>(active);
        };
        api.metrics.setAvailable = [](RedisSearchDiskMetricsCollector*, bool) {};
        api.basic.close = [](RedisModuleCtx*, RedisSearchDisk*) {
          active->closed = active->collectionFinished && active->otherWorkerFinished.load() &&
                           specDict_g == nullptr && !DiskMetrics_BeginWait();
        };
        disk = &api;
        disk_db = reinterpret_cast<RedisSearchDisk*>(&state);
        RedisModule_BigModuleRegister = [](RedisModuleCtx*, RedisModuleBigCallbacks*) {
          return REDISMODULE_OK;
        };
        if (!SearchDisk_RegisterBigModuleCallbacks(nullptr)) _exit(1);
        ConcurrentSearch_CreatePool(1);
        ConcurrentSearch_ThreadPoolRun([](void*) { active->otherWorkerFinished.store(true); },
                                       nullptr);
        RMCK::ArgvList args(RSDummyContext, "FT.CREATE", "cleanup_metrics", "SCHEMA", "t", "TEXT");
        QueryError error = QueryError_Default();
        if (!Indexes_CreateNewSpec(RSDummyContext, args, args.size(), &error)) _exit(1);
        if (!DiskMetrics_Start(
                nullptr,
                [](void*) {
                  std::unique_lock<std::mutex> lock(active->mutex);
                  active->collecting = true;
                  active->changed.notify_all();
                  active->changed.wait(lock, [] { return active->release; });
                  active->retainedIndexes = specDict_g && dictSize(specDict_g) > 0;
                  active->collectionFinished = true;
                  return false;
                },
                nullptr))
          _exit(1);
        {
          std::unique_lock<std::mutex> lock(state.mutex);
          if (!state.changed.wait_for(lock, std::chrono::seconds(5),
                                      [&state] { return state.collecting; }))
            _exit(1);
        }
        std::thread releaseCollection([&state] {
          std::this_thread::sleep_for(std::chrono::milliseconds(100));
          std::lock_guard<std::mutex> lock(state.mutex);
          state.release = true;
          state.changed.notify_all();
        });
        RedisModule_ThreadSafeContextLock(nullptr);
        RediSearch_CleanupModule(nullptr);
        RedisModule_ThreadSafeContextUnlock(nullptr);
        releaseCollection.join();
        _exit(state.retainedIndexes && state.closed && timersStopped == 1 ? 0 : 1);
      },
      ::testing::ExitedWithCode(0), "");
}

TEST_F(DiskMetricsTest, ForkChildReadsCacheWithoutSubmittingOrJoiningAWorker) {
  release = true;
  ASSERT_TRUE(DiskMetrics_Start(nullptr, blockedBatch, this));
  ASSERT_TRUE(await(1));
  pid_t child = fork();
  ASSERT_NE(child, -1);
  if (child == 0) {
    bool safe = DiskMetrics_InForkChild() && !DiskMetrics_Wake() && !DiskMetrics_BeginWait();
    DiskMetrics_Stop(nullptr);
    _exit(safe ? 0 : 1);
  }
  int status;
  ASSERT_EQ(waitpid(child, &status, 0), child);
  ASSERT_TRUE(WIFEXITED(status));
  EXPECT_EQ(WEXITSTATUS(status), 0);
  EXPECT_FALSE(DiskMetrics_InForkChild());
  EXPECT_TRUE(DiskMetrics_Wake());
  ASSERT_TRUE(await(2));
}
#endif

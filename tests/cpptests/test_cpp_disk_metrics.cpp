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
extern "C" {
#include "util/disk_metrics.h"
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

  void SetUp() override {
    savedCreate = RedisModule_CreateTimer;
    savedStop = RedisModule_StopTimer;
    RedisModule_CreateTimer = [](RedisModuleCtx*, mstime_t, RedisModuleTimerProc, void*) {
      return RedisModuleTimerID{1};
    };
    RedisModule_StopTimer = [](RedisModuleCtx*, RedisModuleTimerID, void**) {
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

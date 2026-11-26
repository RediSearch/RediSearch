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

#include <atomic>
#include <chrono>
#include <future>
#include <thread>

extern "C" {
#include "search_disk.h"
extern RedisSearchDiskAPI *disk;
extern RedisSearchDisk *disk_db;
extern bool isFlex;
}

namespace {

// Closing an index must be ordered against closing the shared disk context, because the last
// reference of a dropped index can be released by a worker while shutdown closes the context.
class DiskCloseLifetimeTest : public ::testing::Test {
 protected:
  RedisSearchDiskAPI savedApi{};
  RedisSearchDiskAPI *savedDisk = nullptr;
  RedisSearchDisk *savedDiskDb = nullptr;
  bool savedIsFlex = false;
  RedisSearchDiskAPI api{};
  // Stands in for an index handle; the mock never dereferences it.
  int index = 0;

  static inline std::atomic<int> closeIndexCalls{0};
  static inline std::atomic<int> closeContextCalls{0};
  static inline std::atomic<bool> indexClosedBeforeContext{false};
  static inline std::atomic<int> asyncIOQueries{0};
  static inline std::promise<void> insideClose;
  static inline std::shared_future<void> releaseClose;

  void SetUp() override {
    savedDisk = disk;
    savedDiskDb = disk_db;
    savedIsFlex = isFlex;
    closeIndexCalls = 0;
    closeContextCalls = 0;
    indexClosedBeforeContext = false;
    asyncIOQueries = 0;

    api.basic.isAsyncIOSupported = [](RedisSearchDisk *) {
      ++asyncIOQueries;
      return true;
    };
    api.basic.closeIndexSpec = [](RedisSearchDisk *, RedisSearchDiskIndexSpec *) {
      ++closeIndexCalls;
    };
    api.basic.close = [](RedisModuleCtx *, RedisSearchDisk *) {
      indexClosedBeforeContext = closeIndexCalls > 0;
      ++closeContextCalls;
    };
    api.metrics.stopMetrics = [](RedisSearchDisk *) {};
    disk = &api;
    disk_db = reinterpret_cast<RedisSearchDisk *>(&api);
    // Keeps SearchDisk_Close away from the slot-cleanup jobs, which are not started here.
    isFlex = false;
  }

  void TearDown() override {
    disk = savedDisk;
    disk_db = savedDiskDb;
    isFlex = savedIsFlex;
    SearchDisk_CacheAsyncIOSupport();
  }

  RedisSearchDiskIndexSpec *handle() {
    return reinterpret_cast<RedisSearchDiskIndexSpec *>(&index);
  }
};

TEST_F(DiskCloseLifetimeTest, ClosesTheIndexWhileTheContextIsOpen) {
  EXPECT_TRUE(SearchDisk_CloseIndex(handle()));
  EXPECT_EQ(closeIndexCalls, 1);
}

TEST_F(DiskCloseLifetimeTest, SkipsTheIndexCloseOnceTheContextIsClosed) {
  SearchDisk_Close(nullptr);
  EXPECT_EQ(closeContextCalls, 1);
  EXPECT_EQ(disk_db, nullptr);

  EXPECT_FALSE(SearchDisk_CloseIndex(handle()));
  EXPECT_EQ(closeIndexCalls, 0);
}

// Workers building a query pipeline ask this while shutdown may be freeing the context, so the
// backend is asked once, when the context opens, and never through disk_db after that.
TEST_F(DiskCloseLifetimeTest, AsyncIOSupportIsReadFromTheBackendOnlyWhenCached) {
  EXPECT_FALSE(SearchDisk_IsAsyncIOSupported());
  EXPECT_EQ(asyncIOQueries, 0);

  SearchDisk_CacheAsyncIOSupport();
  EXPECT_EQ(asyncIOQueries, 1);
  EXPECT_TRUE(SearchDisk_IsAsyncIOSupported());
  EXPECT_TRUE(SearchDisk_IsAsyncIOSupported());
  EXPECT_EQ(asyncIOQueries, 1);
}

TEST_F(DiskCloseLifetimeTest, AsyncIOSupportIsClearedWhenTheContextIsClosed) {
  SearchDisk_CacheAsyncIOSupport();
  ASSERT_TRUE(SearchDisk_IsAsyncIOSupported());

  SearchDisk_Close(nullptr);
  EXPECT_FALSE(SearchDisk_IsAsyncIOSupported());
  EXPECT_EQ(asyncIOQueries, 1);
}

TEST_F(DiskCloseLifetimeTest, ClosingTheContextWaitsForAnIndexThatIsMidClose) {
  insideClose = std::promise<void>();
  std::promise<void> release;
  releaseClose = release.get_future().share();
  api.basic.closeIndexSpec = [](RedisSearchDisk *, RedisSearchDiskIndexSpec *) {
    ++closeIndexCalls;
    insideClose.set_value();
    releaseClose.wait();
  };

  auto closeIndex = std::async(std::launch::async, [&] { return SearchDisk_CloseIndex(handle()); });
  insideClose.get_future().wait();

  std::atomic<bool> contextClosed{false};
  std::thread closeContext([&] {
    SearchDisk_Close(nullptr);
    contextClosed = true;
  });

  // The context must stay open for as long as the index close is running.
  std::this_thread::sleep_for(std::chrono::milliseconds(100));
  EXPECT_FALSE(contextClosed);
  EXPECT_EQ(closeContextCalls, 0);

  release.set_value();
  EXPECT_TRUE(closeIndex.get());
  closeContext.join();
  EXPECT_TRUE(contextClosed);
  EXPECT_EQ(closeContextCalls, 1);
  EXPECT_TRUE(indexClosedBeforeContext);
}

}  // namespace

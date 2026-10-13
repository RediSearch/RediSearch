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
#include <thread>
#include <vector>
extern "C" {
#include "hybrid/hybrid_request.h"
#include "obfuscation/hidden.h"
#include "search_ctx.h"
#include "search_disk.h"
#include "spec.h"
extern RedisSearchDiskAPI* disk;
extern RedisSearchDisk* disk_db;
}

class DiskSnapshotCounterTest : public ::testing::Test {
 protected:
  static bool failNextCreate;
  static int sentinel;

  RedisSearchDiskAPI* savedDisk = nullptr;
  RedisSearchDiskAPI api{};
  RedisSearchDiskIndexSpec* index = reinterpret_cast<RedisSearchDiskIndexSpec*>(&sentinel);

  void SetUp() override {
    failNextCreate = false;
    api.index.createSnapshot = [](RedisSearchDiskIndexSpec*) -> RedisSearchDiskSnapshot* {
      return failNextCreate ? nullptr : reinterpret_cast<RedisSearchDiskSnapshot*>(&sentinel);
    };
    api.index.freeSnapshot = [](RedisSearchDiskSnapshot*) {};
    savedDisk = disk;
    disk = &api;
    ASSERT_EQ(SearchDisk_GetLiveSnapshotCount(), 0u);
  }

  void TearDown() override {
    disk = savedDisk;
  }
};

bool DiskSnapshotCounterTest::failNextCreate = false;
int DiskSnapshotCounterTest::sentinel = 0;

TEST_F(DiskSnapshotCounterTest, CountsCreatedAndFreedSnapshots) {
  RedisSearchDiskSnapshot* a = SearchDisk_CreateSnapshot(index);
  RedisSearchDiskSnapshot* b = SearchDisk_CreateSnapshot(index);
  ASSERT_NE(a, nullptr);
  ASSERT_NE(b, nullptr);
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 2u);

  SearchDisk_FreeSnapshot(a);
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 1u);
  SearchDisk_FreeSnapshot(b);
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 0u);
}

TEST_F(DiskSnapshotCounterTest, FailedCreateIsNotCounted) {
  failNextCreate = true;
  EXPECT_EQ(SearchDisk_CreateSnapshot(index), nullptr);
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 0u);
}

TEST_F(DiskSnapshotCounterTest, FreeingNullIsNotCounted) {
  RedisSearchDiskSnapshot* snapshot = SearchDisk_CreateSnapshot(index);
  SearchDisk_FreeSnapshot(nullptr);
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 1u);
  SearchDisk_FreeSnapshot(snapshot);
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 0u);
}

// A StrongRef over a snapshot must free it through SearchDisk_FreeSnapshot on the last release.
static void freeSnapshot(void* snapshot) {
  SearchDisk_FreeSnapshot(reinterpret_cast<RedisSearchDiskSnapshot*>(snapshot));
}

TEST_F(DiskSnapshotCounterTest, SnapshotRefFreesOnLastRelease) {
  StrongRef ref = StrongRef_New(SearchDisk_CreateSnapshot(index), freeSnapshot);
  StrongRef second = StrongRef_Clone(ref);
  StrongRef third = StrongRef_Clone(ref);
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 1u);

  StrongRef_Release(ref);
  StrongRef_Release(second);
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 1u);
  StrongRef_Release(third);
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 0u);
}

TEST_F(DiskSnapshotCounterTest, ContextsShareOneSnapshotAndReleaseIndependently) {
  RedisSearchCtx first = SEARCH_CTX_STATIC(nullptr, nullptr);
  RedisSearchCtx second = SEARCH_CTX_STATIC(nullptr, nullptr);
  first.diskSnapshot = SearchDisk_CreateSnapshot(index);
  first.diskSnapshotRef = StrongRef_New(first.diskSnapshot, freeSnapshot);

  SearchCtx_ShareDiskSnapshot(&second, &first);
  EXPECT_EQ(second.diskSnapshot, first.diskSnapshot);
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 1u);

  // The first context going away must not pull the snapshot out from under the second.
  SearchCtx_CleanUp(&first);
  EXPECT_EQ(first.diskSnapshot, nullptr);
  EXPECT_NE(second.diskSnapshot, nullptr);
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 1u);

  SearchCtx_CleanUp(&second);
  EXPECT_EQ(second.diskSnapshot, nullptr);
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 0u);
}

TEST_F(DiskSnapshotCounterTest, SharingWithoutASnapshotLeavesTheContextWithout) {
  RedisSearchCtx first = SEARCH_CTX_STATIC(nullptr, nullptr);
  RedisSearchCtx second = SEARCH_CTX_STATIC(nullptr, nullptr);

  SearchCtx_ShareDiskSnapshot(&second, &first);
  EXPECT_EQ(second.diskSnapshot, nullptr);
  EXPECT_EQ(second.diskSnapshotRef.rm, nullptr);
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 0u);
}

TEST_F(DiskSnapshotCounterTest, ConcurrentReleasesFreeTheSnapshotExactlyOnce) {
  constexpr int kOwners = 8;
  StrongRef ref = StrongRef_New(SearchDisk_CreateSnapshot(index), freeSnapshot);
  std::vector<StrongRef> refs{ref};
  for (int i = 1; i < kOwners; i++) {
    refs.push_back(StrongRef_Clone(ref));
  }

  std::vector<std::thread> owners;
  for (StrongRef owned : refs) {
    owners.emplace_back([owned] { StrongRef_Release(owned); });
  }
  for (auto& t : owners) {
    t.join();
  }
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 0u);
}

// A two-subquery hybrid request on a disk-backed index, with just enough state for
// HybridRequest_TakeSharedDiskSnapshot: the subqueries' search contexts and their errors.
class HybridSharedSnapshotTest : public DiskSnapshotCounterTest {
 protected:
  bool savedFlex = false;
  RedisSearchDisk* savedDiskDb = nullptr;
  IndexSpec spec{};
  AREQ first{};
  AREQ second{};
  AREQ* subqueries[2] = {&first, &second};
  RedisSearchCtx firstCtx = SEARCH_CTX_STATIC(nullptr, &spec);
  RedisSearchCtx secondCtx = SEARCH_CTX_STATIC(nullptr, &spec);
  HybridRequest hybrid{};

  void SetUp() override {
    DiskSnapshotCounterTest::SetUp();
    savedFlex = isFlex;
    savedDiskDb = disk_db;
    isFlex = true;
    disk_db = reinterpret_cast<RedisSearchDisk*>(&sentinel);
    spec.diskSpec = index;
    spec.specName = NewHiddenString("idx", 3, true);
    first.sctx = &firstCtx;
    second.sctx = &secondCtx;
    first.base.reply.err = QueryError_Default();
    second.base.reply.err = QueryError_Default();
    hybrid.requests = subqueries;
    hybrid.nrequests = 2;
  }

  void TearDown() override {
    QueryError_ClearError(&first.base.reply.err);
    QueryError_ClearError(&second.base.reply.err);
    HiddenString_Free(spec.specName, true);
    isFlex = savedFlex;
    disk_db = savedDiskDb;
    DiskSnapshotCounterTest::TearDown();
  }
};

TEST_F(HybridSharedSnapshotTest, SubqueriesShareOneLiveSnapshotUntilTheLastIsDone) {
  ASSERT_EQ(HybridRequest_TakeSharedDiskSnapshot(&hybrid), REDISMODULE_OK);
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 1u);
  ASSERT_NE(firstCtx.diskSnapshot, nullptr);
  EXPECT_EQ(secondCtx.diskSnapshot, firstCtx.diskSnapshot);

  // A subquery cursor can outlive the request, so each context must keep the snapshot alive.
  SearchCtx_CleanUp(&firstCtx);
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 1u);
  EXPECT_NE(secondCtx.diskSnapshot, nullptr);
  SearchCtx_CleanUp(&secondCtx);
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 0u);
}

TEST_F(HybridSharedSnapshotTest, FailureLeavesNoSnapshotAndIsReportedOnTheFirstSubquery) {
  failNextCreate = true;
  EXPECT_EQ(HybridRequest_TakeSharedDiskSnapshot(&hybrid), REDISMODULE_ERR);
  EXPECT_TRUE(QueryError_HasError(&first.base.reply.err));
  EXPECT_FALSE(QueryError_HasError(&second.base.reply.err));
  EXPECT_EQ(firstCtx.diskSnapshot, nullptr);
  EXPECT_EQ(secondCtx.diskSnapshot, nullptr);
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 0u);
}

TEST_F(HybridSharedSnapshotTest, IndexWithoutDiskBackingTakesNoSnapshot) {
  spec.diskSpec = nullptr;
  ASSERT_EQ(HybridRequest_TakeSharedDiskSnapshot(&hybrid), REDISMODULE_OK);
  EXPECT_EQ(firstCtx.diskSnapshot, nullptr);
  EXPECT_EQ(secondCtx.diskSnapshot, nullptr);
  EXPECT_EQ(SearchDisk_GetLiveSnapshotCount(), 0u);
}

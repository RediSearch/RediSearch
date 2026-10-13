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
#include "search_disk.h"
extern RedisSearchDiskAPI* disk;
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

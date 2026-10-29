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
#include "disk_gc.h"
#include "geometry/geometry_api.h"
#include "slot_cleanup.h"
#include "slot_ranges.h"
#include <functional>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <thread>
#include <vector>
extern "C" {
#include "indexes.h"
#include "disk_indexer.h"
#include "doc_id_meta.h"
#include "forward_index.h"
#include "search_disk.h"
#include "vector_index.h"
#include "geometry_index.h"
#include "info/global_stats.h"
#include "notifications.h"
extern RedisSearchDiskAPI *disk;
extern RedisSearchDisk *disk_db;
extern bool isFlex;
int RegisterDebugCommands(RedisModuleCommand *debugCommand);
}

namespace {
class DiskSlotTrimTest : public ::testing::Test {
 protected:
  RedisSearchDiskAPI api{};
  RedisSearchDiskAPI *savedDisk = nullptr;
  RedisSearchDisk *savedDb = nullptr;
  bool savedFlex = false, savedEnableGC = false;
  StrongRef ref{};
  IndexSpec *spec = nullptr;
  GCContext gc{};
  void SetUp() override {
    savedDisk = disk;
    savedDb = disk_db;
    savedFlex = isFlex;
    savedEnableGC = RSGlobalConfig.gcConfigParams.enableGC;
    RSGlobalConfig.gcConfigParams.enableGC = false;
    const char *args[] = {"SCHEMA", "text", "TEXT"};
    QueryError error = QueryError_Default();
    ref = IndexSpec_ParseC(nullptr, "disk_slot_trim", args, 3, &error);
    spec = static_cast<IndexSpec *>(StrongRef_Get(ref));
    ASSERT_NE(spec, nullptr);
    Spec_AddToDict(ref.rm);
    spec->diskSpec = reinterpret_cast<RedisSearchDiskIndexSpec *>(spec);
    disk = &api;
    disk_db = reinterpret_cast<RedisSearchDisk *>(&api);
    isFlex = true;
    api.docTable.getDeletedIdsCount = [](RedisSearchDiskIndexSpec *) { return uint64_t{0}; };
    api.index.isBackgroundWorkPaused = [](RedisSearchDiskIndexSpec *) { return false; };
    api.slotMigration.hasPendingCleanup = [](RedisSearchDiskIndexSpec *) { return false; };
    gc.gcCtx = DiskGC_Create(ref, &gc.callbacks);
    spec->gc = &gc;
  }
  void TearDown() override {
    if (spec) dropIndexBeforeCollectorTermination();
    disk = savedDisk;
    disk_db = savedDb;
    isFlex = savedFlex;
    RSGlobalConfig.gcConfigParams.enableGC = savedEnableGC;
  }
  DiskGC *collector() {
    return static_cast<DiskGC *>(gc.gcCtx);
  }
  void dropIndexBeforeCollectorTermination() {
    spec->gc = nullptr;
    spec->diskSpec = nullptr;
    spec->diskRegistered = false;
    Indexes_RemoveSpecFromGlobals(ref, false);
    ref = {};
    spec = nullptr;
    gc.callbacks.onTerm(gc.gcCtx);
    gc.gcCtx = nullptr;
  }
};

TEST_F(DiskSlotTrimTest, IndexDropRetiresOutstandingDeletesAfterPartialCollection) {
  const auto logicallyDeleted = IndexesGlobalStats_GetLogicallyDeletedDocs();
  for (size_t i = 0; i < 3; ++i) GCContext_OnDelete(&gc);
  api.index.runGC = [](RedisSearchDiskIndexSpec *, DiskGCRunStats *stats) {
    stats->num_cleaned_docs = 1;
  };
  GCForcedRun forced = GCForcedRun_New(1000, 10);
  ASSERT_TRUE(gc.callbacks.periodicCallback(gc.gcCtx, &forced));
  EXPECT_EQ(collector()->deletesFromLastRun.load(), 0u);
  EXPECT_EQ(collector()->logicallyDeleted.load(), 2u);
  EXPECT_EQ(IndexesGlobalStats_GetLogicallyDeletedDocs(), logicallyDeleted + 2);
  dropIndexBeforeCollectorTermination();
  EXPECT_EQ(IndexesGlobalStats_GetLogicallyDeletedDocs(), logicallyDeleted);
}

TEST_F(DiskSlotTrimTest, RestoredDeletionsAreChargedBeforeGcAndRemovedOnIndexDrop) {
  const auto logicallyDeleted = IndexesGlobalStats_GetLogicallyDeletedDocs();
  gc.callbacks.onTerm(gc.gcCtx);
  api.docTable.getDeletedIdsCount = [](RedisSearchDiskIndexSpec *) { return uint64_t{4}; };
  gc.gcCtx = DiskGC_Create(ref, &gc.callbacks);
  EXPECT_EQ(collector()->deletesFromLastRun.load(), 4u);
  EXPECT_EQ(collector()->logicallyDeleted.load(), 4u);
  EXPECT_EQ(IndexesGlobalStats_GetLogicallyDeletedDocs(), logicallyDeleted + 4);
  dropIndexBeforeCollectorTermination();
  EXPECT_EQ(IndexesGlobalStats_GetLogicallyDeletedDocs(), logicallyDeleted);
}

TEST_F(DiskSlotTrimTest, UnchargedStorageIdsDoNotConsumeOtherIndexesGlobalContribution) {
  const auto logicallyDeleted = IndexesGlobalStats_GetLogicallyDeletedDocs();
  IndexsGlobalStats_IncreaseLogicallyDeleted(4);
  api.index.runGC = [](RedisSearchDiskIndexSpec *, DiskGCRunStats *stats) {
    stats->num_cleaned_docs = 3;
  };
  GCForcedRun forced = GCForcedRun_New(1000, 10);
  EXPECT_TRUE(gc.callbacks.periodicCallback(gc.gcCtx, &forced));
  EXPECT_EQ(IndexesGlobalStats_GetLogicallyDeletedDocs(), logicallyDeleted + 4);
  dropIndexBeforeCollectorTermination();
  EXPECT_EQ(IndexesGlobalStats_GetLogicallyDeletedDocs(), logicallyDeleted + 4);
  IndexsGlobalStats_DecreaseLogicallyDeleted(4);
}

TEST_F(DiskSlotTrimTest, ReaderReadyRetirementRunsWithoutNewWrites) {
  const auto threshold = RSGlobalConfig.gcConfigParams.gcSettings.forkGcCleanThreshold;
  RSGlobalConfig.gcConfigParams.gcSettings.forkGcCleanThreshold = 1000;
  GCContext_OnDelete(&gc);
  static bool ready;
  static size_t runs;
  ready = false;
  runs = 0;
  api.slotMigration.hasPendingCleanup = [](RedisSearchDiskIndexSpec *) { return ready; };
  api.index.runGC = [](RedisSearchDiskIndexSpec *, DiskGCRunStats *stats) {
    ++runs;
    ready = false;
    stats->num_cleaned_docs = 1;
  };
  EXPECT_TRUE(gc.callbacks.periodicCallback(gc.gcCtx, nullptr));
  EXPECT_EQ(runs, 0u);
  ready = true;
  EXPECT_TRUE(gc.callbacks.periodicCallback(gc.gcCtx, nullptr));
  EXPECT_EQ(runs, 1u);
  EXPECT_EQ(collector()->logicallyDeleted.load(), 0u);
  EXPECT_TRUE(gc.callbacks.periodicCallback(gc.gcCtx, nullptr));
  EXPECT_EQ(runs, 1u);
  RSGlobalConfig.gcConfigParams.gcSettings.forkGcCleanThreshold = threshold;
}

}  // namespace

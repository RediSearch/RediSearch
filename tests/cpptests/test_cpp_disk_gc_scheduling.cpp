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
#include "gc/disk_gc.h"

extern "C" {
#include "indexes.h"
#include "search_disk.h"
extern RedisSearchDiskAPI* disk;
extern RedisSearchDisk* disk_db;
extern bool isFlex;
}

namespace {
class DiskGCSchedulingTest : public ::testing::Test {
 protected:
  static inline DiskGCSchedulingTest* active;
  RedisSearchDiskAPI api{};
  RedisSearchDiskAPI* savedDisk = disk;
  RedisSearchDisk* savedDb = disk_db;
  bool savedFlex = isFlex;
  GCConfig savedConfig = RSGlobalConfig.gcConfigParams;
  StrongRef ref{};
  IndexSpec* spec = nullptr;
  GCCallbacks callbacks{};
  DiskGC* gc = nullptr;
  size_t runs = 0, probes = 0;
  bool pressure = false, paused = false, writeDuringRun = false;
  DiskGCRunResult outcome = DISK_GC_RUN_OK;

  GCSettings& settings() { return RSGlobalConfig.gcConfigParams.gcSettings; }

  void SetUp() override {
    active = this;
    isFlex = false;
    RSGlobalConfig.gcConfigParams.enableGC = false;
    const char* args[] = {"SCHEMA", "t", "TEXT"};
    QueryError error = QueryError_Default();
    ref = IndexSpec_ParseC(nullptr, "disk_gc_scheduling", args, 3, &error);
    spec = static_cast<IndexSpec*>(StrongRef_Get(ref));
    ASSERT_NE(spec, nullptr) << QueryError_GetUserError(&error);
    Spec_AddToDict(ref.rm);
    spec->diskSpec = reinterpret_cast<RedisSearchDiskIndexSpec*>(spec);
    disk = &api;
    disk_db = reinterpret_cast<RedisSearchDisk*>(&api);
    isFlex = true;
    api.docTable.getDeletedIdsCount = [](RedisSearchDiskIndexSpec*) { return uint64_t{0}; };
    api.index.isBackgroundWorkPaused = [](RedisSearchDiskIndexSpec*) { return active->paused; };
    api.index.gcL0Pressure = [](RedisSearchDiskIndexSpec*, uint64_t threshold) {
      EXPECT_EQ(threshold, 4u);
      ++active->probes;
      return active->pressure;
    };
    api.index.runGC = [](RedisSearchDiskIndexSpec*, DiskGCRunStats*) {
      ++active->runs;
      if (active->writeDuringRun) active->callbacks.onWrite(active->gc);
      return active->outcome;
    };
    settings().diskGcCheckIntervalSec = 1;
    settings().diskGcMinIntervalSec = 0;
    settings().diskGcL0FileThreshold = 4;
    settings().forkGcRunIntervalSec = 300;
    settings().forkGcCleanThreshold = 3;
    gc = DiskGC_Create(ref, &callbacks);
  }

  void TearDown() override {
    if (gc) callbacks.onTerm(gc);
    if (spec) {
      spec->diskSpec = nullptr;
      spec->diskRegistered = false;
    }
    disk = savedDisk;
    disk_db = savedDb;
    isFlex = savedFlex;
    if (ref.rm) Indexes_RemoveSpecFromGlobals(ref, false);
    RSGlobalConfig.gcConfigParams = savedConfig;
  }

  void check(GCForcedRun* forced = nullptr) {
    ASSERT_TRUE(callbacks.periodicCallback(gc, forced));
  }
};

TEST_F(DiskGCSchedulingTest, L0AloneTriggersAndCancellationStaysPendingWithoutL0) {
  pressure = true;
  outcome = DISK_GC_RUN_RETRY_REQUIRED;
  check();
  EXPECT_EQ(runs, 1u);
  EXPECT_EQ(gc->writesFromLastRun.load(), 0u);
  pressure = false;
  outcome = DISK_GC_RUN_OK;
  check();
  EXPECT_EQ(runs, 2u);
  check();
  EXPECT_EQ(runs, 2u);
}

TEST_F(DiskGCSchedulingTest, CooldownAndPauseSuppressChecksButForcedRunBypassesCooldown) {
  settings().diskGcMinIntervalSec = 3600;
  pressure = true;
  check();
  EXPECT_EQ(runs, 1u);
  const size_t initialProbes = probes;
  check();
  EXPECT_EQ(runs, 1u);
  EXPECT_EQ(probes, initialProbes);
  GCForcedRun forced{};
  paused = true;
  check(&forced);
  EXPECT_EQ(runs, 1u);
  paused = false;
  check(&forced);
  EXPECT_EQ(runs, 2u);
}

TEST_F(DiskGCSchedulingTest, LegacyModeUsesNotificationThresholdAndNeverProbesL0) {
  settings().diskGcCheckIntervalSec = 0;
  pressure = true;
  callbacks.onWrite(gc);
  callbacks.onWrite(gc);
  check();
  EXPECT_EQ(runs, 0u);
  callbacks.onWrite(gc);
  check();
  EXPECT_EQ(runs, 1u);
  EXPECT_EQ(probes, 0u);
  EXPECT_EQ(callbacks.getIntervalMs(gc), 300000);
}

TEST_F(DiskGCSchedulingTest, DisablingPressureChecksDoesNotForgetAnIncompleteCycle) {
  pressure = true;
  outcome = DISK_GC_RUN_RETRY_REQUIRED;
  check();
  settings().diskGcCheckIntervalSec = 0;
  pressure = false;
  outcome = DISK_GC_RUN_OK;
  check();
  EXPECT_EQ(runs, 2u);
  EXPECT_EQ(probes, 1u);
  check();
  EXPECT_EQ(runs, 2u);
}

TEST_F(DiskGCSchedulingTest, DeletedIdsTriggerBeforeAgeDeadlineAndRetryPreservesNewWrites) {
  callbacks.onDelete(gc);
  callbacks.onUpdate(gc);
  check();
  EXPECT_EQ(runs, 0u);
  callbacks.onDelete(gc);
  writeDuringRun = true;
  outcome = DISK_GC_RUN_RETRY_REQUIRED;
  check();
  EXPECT_EQ(runs, 1u);
  EXPECT_EQ(gc->deletesFromLastRun.load(), 2u);
  EXPECT_EQ(gc->updatesFromLastRun.load(), 1u);
  EXPECT_EQ(gc->writesFromLastRun.load(), 1u);
  writeDuringRun = false;
  outcome = DISK_GC_RUN_OK;
  check();
  EXPECT_EQ(runs, 2u);
  EXPECT_EQ(gc->writesFromLastRun.load(), 0u);
  EXPECT_EQ(callbacks.getIntervalMs(gc), 1000);
}
}  // namespace

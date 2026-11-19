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
#include <functional>
extern "C" {
#include "indexes.h"
#include "spec.h"
#include "search_disk.h"
extern RedisSearchDiskAPI *disk;
extern RedisSearchDisk *disk_db;
extern bool isFlex;
}

namespace {

struct CompactState {
  // Each vector field's fake VecSim handle points at its idle flag.
  bool fieldIdle[2] = {true, true};
  bool paused = false;
  DiskVectorCompactResult result = DISK_VECTOR_COMPACT_DONE;
  size_t compactCalls = 0, idleChecks = 0, gcRuns = 0;
  DiskGCRunResult gcResult = DISK_GC_RUN_OK;
  // When set, a document write lands while the compaction runs.
  GCContext *writeDuringCompaction = nullptr;
} state;

class DiskGCVectorCompactTest : public ::testing::Test {
 protected:
  RedisSearchDiskAPI api{};
  RedisSearchDiskAPI *savedDisk = nullptr;
  RedisSearchDisk *savedDb = nullptr;
  bool savedFlex = false;
  size_t savedCleanThreshold = 0;
  StrongRef ref{};
  IndexSpec *spec = nullptr;
  GCContext gc{};

  void SetUp() override {
    state = {};
    savedDisk = disk;
    savedDb = disk_db;
    savedFlex = isFlex;
    savedCleanThreshold = RSGlobalConfig.gcConfigParams.gcSettings.forkGcCleanThreshold;
    const char *args[] = {"SCHEMA",  "v1",
                          "VECTOR",  "HNSW",
                          "6",       "TYPE",
                          "FLOAT32", "DIM",
                          "2",       "DISTANCE_METRIC",
                          "L2",      "t",
                          "TAG",     "v2",
                          "VECTOR",  "HNSW",
                          "6",       "TYPE",
                          "FLOAT32", "DIM",
                          "2",       "DISTANCE_METRIC",
                          "L2"};
    QueryError err = QueryError_Default();
    ref =
        IndexSpec_ParseC(nullptr, "disk_vector_compact", args, sizeof(args) / sizeof(*args), &err);
    spec = static_cast<IndexSpec *>(StrongRef_Get(ref));
    ASSERT_NE(spec, nullptr) << QueryError_GetUserError(&err);
    Spec_AddToDict(ref.rm);
    spec->diskSpec = reinterpret_cast<RedisSearchDiskIndexSpec *>(spec);
    disk = &api;
    disk_db = reinterpret_cast<RedisSearchDisk *>(&api);
    isFlex = true;

    api.docTable.getDeletedIdsCount = [](RedisSearchDiskIndexSpec *) { return uint64_t{0}; };
    api.index.isBackgroundWorkPaused = [](RedisSearchDiskIndexSpec *) { return state.paused; };
    api.index.runGC = [](RedisSearchDiskIndexSpec *, DiskGCRunStats *) {
      ++state.gcRuns;
      return state.gcResult;
    };
    api.vector.vectorIndexIsBackgroundIdle = [](void *vecIndex) {
      ++state.idleChecks;
      return *static_cast<bool *>(vecIndex);
    };
    api.index.compactVectorStorage = [](RedisSearchDiskIndexSpec *) {
      ++state.compactCalls;
      if (state.writeDuringCompaction) GCContext_OnWrite(state.writeDuringCompaction);
      return state.result;
    };
    setVectorIndexes(true);

    gc.gcCtx = DiskGC_Create(ref, &gc.callbacks);
    spec->gc = &gc;
  }

  void TearDown() override {
    if (gc.gcCtx) gc.callbacks.onTerm(gc.gcCtx);
    if (spec) {
      setVectorIndexes(false);
      spec->gc = nullptr;
      spec->diskSpec = nullptr;
    }
    disk = savedDisk;
    disk_db = savedDb;
    isFlex = savedFlex;
    RSGlobalConfig.gcConfigParams.gcSettings.forkGcCleanThreshold = savedCleanThreshold;
    if (ref.rm) Indexes_RemoveSpecFromGlobals(ref, false);
  }

  // Fake handles: the field cleanup that frees VecSim indexes must never see them.
  void setVectorIndexes(bool present) {
    size_t next = 0;
    for (int i = 0; i < spec->numFields; i++) {
      FieldSpec *f = &spec->fields[i];
      if (!FIELD_IS(f, INDEXFLD_T_VECTOR)) continue;
      f->vectorOpts.vecSimIndex =
          present ? reinterpret_cast<VecSimIndex *>(&state.fieldIdle[next++]) : nullptr;
    }
  }

  void run() {
    ASSERT_TRUE(gc.callbacks.periodicCallback(gc.gcCtx, nullptr));
  }

  size_t vectorCompactions() {
    return static_cast<DiskGC *>(gc.gcCtx)->vectorCompactions.load();
  }
};

TEST_F(DiskGCVectorCompactTest, CompactsOnceAfterTwoIdleRuns) {
  run();
  EXPECT_EQ(state.compactCalls, 0u) << "one idle run is not a full quiet interval";
  run();
  EXPECT_EQ(state.compactCalls, 1u);
  EXPECT_EQ(vectorCompactions(), 1u);

  for (int i = 0; i < 3; ++i) run();
  EXPECT_EQ(state.compactCalls, 1u) << "nothing changed since the compaction";
}

TEST_F(DiskGCVectorCompactTest, EveryKindOfChangeRearmsIt) {
  run();
  run();
  ASSERT_EQ(state.compactCalls, 1u);

  const std::function<void()> changes[] = {
      [&] { GCContext_OnWrite(&gc); },
      [&] { GCContext_OnUpdate(&gc); },
      [&] { GCContext_OnDelete(&gc); },
      [&] { IndexSpec_DeferGCDeletes(spec, 1); },
  };
  size_t expected = 1;
  for (const auto &change : changes) {
    change();
    run();
    EXPECT_EQ(state.compactCalls, expected) << "the change restarts the quiet interval";
    run();
    EXPECT_EQ(state.compactCalls, ++expected);
  }
}

TEST_F(DiskGCVectorCompactTest, WriteBetweenIdleRunsRestartsTheInterval) {
  run();
  GCContext_OnWrite(&gc);
  run();
  EXPECT_EQ(state.compactCalls, 0u);
  run();
  EXPECT_EQ(state.compactCalls, 1u);
}

TEST_F(DiskGCVectorCompactTest, BusyFieldBlocksAndRestartsTheInterval) {
  state.fieldIdle[1] = false;
  run();
  run();
  EXPECT_EQ(state.compactCalls, 0u) << "every vector field must be idle";

  state.fieldIdle[1] = true;
  run();
  state.fieldIdle[1] = false;
  run();
  state.fieldIdle[1] = true;
  run();
  EXPECT_EQ(state.compactCalls, 0u) << "the idle runs must be consecutive";
  run();
  EXPECT_EQ(state.compactCalls, 1u);
}

TEST_F(DiskGCVectorCompactTest, NotNeededIsNotCountedOrRetried) {
  state.result = DISK_VECTOR_COMPACT_NOT_NEEDED;
  run();
  run();
  EXPECT_EQ(state.compactCalls, 1u);
  EXPECT_EQ(vectorCompactions(), 0u);
  run();
  EXPECT_EQ(state.compactCalls, 1u);
}

TEST_F(DiskGCVectorCompactTest, CancelledRetriesOnTheNextRun) {
  state.result = DISK_VECTOR_COMPACT_CANCELLED;
  run();
  run();
  EXPECT_EQ(state.compactCalls, 1u);
  run();
  EXPECT_EQ(state.compactCalls, 2u) << "a cancelled compaction waits no extra interval";
  EXPECT_EQ(vectorCompactions(), 0u);

  state.result = DISK_VECTOR_COMPACT_DONE;
  run();
  EXPECT_EQ(state.compactCalls, 3u);
  EXPECT_EQ(vectorCompactions(), 1u);
  run();
  EXPECT_EQ(state.compactCalls, 3u);
}

TEST_F(DiskGCVectorCompactTest, WriteDuringCompactionRearmsIt) {
  state.writeDuringCompaction = &gc;
  run();
  run();
  ASSERT_EQ(state.compactCalls, 1u);
  state.writeDuringCompaction = nullptr;

  run();
  EXPECT_EQ(state.compactCalls, 1u);
  run();
  EXPECT_EQ(state.compactCalls, 2u) << "the write is compacted on its own epoch";
}

TEST_F(DiskGCVectorCompactTest, IndexWithoutVectorDataSettlesUntilTheNextWrite) {
  setVectorIndexes(false);
  for (int i = 0; i < 3; ++i) run();
  EXPECT_EQ(state.compactCalls, 0u);
  auto *collector = static_cast<DiskGC *>(gc.gcCtx);
  EXPECT_EQ(collector->compactedVectorEpoch, collector->vectorEpoch.load())
      << "an index with nothing to compact must not be re-examined on every run";

  // Vector indexes are created by writes, and a write starts a new epoch.
  setVectorIndexes(true);
  run();
  EXPECT_EQ(state.idleChecks, 0u);
  GCContext_OnWrite(&gc);
  run();
  run();
  EXPECT_EQ(state.compactCalls, 1u);
}

// A retried collection re-counts its changes without starting a new epoch, so it runs in the
// same callback as the compaction it does not delay.
TEST_F(DiskGCVectorCompactTest, CompactsInTheSameRunAsARetriedCollection) {
  RSGlobalConfig.gcConfigParams.gcSettings.forkGcCleanThreshold = 1;
  run();
  GCContext_OnWrite(&gc);
  state.gcResult = DISK_GC_RUN_RETRY_REQUIRED;
  run();
  EXPECT_EQ(state.gcRuns, 1u);
  EXPECT_EQ(state.compactCalls, 0u) << "the write started a new epoch";

  state.gcResult = DISK_GC_RUN_OK;
  run();
  EXPECT_EQ(state.gcRuns, 2u) << "the retried changes still met the threshold";
  EXPECT_EQ(state.compactCalls, 1u);
}

TEST_F(DiskGCVectorCompactTest, SkipsIndexWithoutDiskHandle) {
  spec->diskSpec = nullptr;
  run();
  run();
  EXPECT_EQ(state.idleChecks, 0u);
  EXPECT_EQ(state.compactCalls, 0u);
}

TEST_F(DiskGCVectorCompactTest, SkipsPausedAndForcedRuns) {
  state.paused = true;
  run();
  run();
  EXPECT_EQ(state.idleChecks, 0u) << "a paused index is not even checked";
  state.paused = false;

  run();
  GCForcedRun forced = GCForcedRun_New(0, 0);
  ASSERT_TRUE(gc.callbacks.periodicCallback(gc.gcCtx, &forced));
  EXPECT_EQ(state.compactCalls, 0u) << "forced runs never compact the vector storage";
  run();
  EXPECT_EQ(state.compactCalls, 1u) << "nor do they break the run of idle checks";
}

}  // namespace

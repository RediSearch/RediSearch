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
#include <functional>
#include <thread>
extern "C" {
#include "indexes.h"
#include "disk_indexer.h"
#include "doc_id_meta.h"
#include "forward_index.h"
#include "search_disk.h"
#include "vector_index.h"
#include "geometry_index.h"
#include "info/global_stats.h"
extern RedisSearchDiskAPI *disk;
extern RedisSearchDisk *disk_db;
extern bool isFlex;
int RegisterDebugCommands(RedisModuleCommand *debugCommand);
}

namespace {
struct TrimState {
  bool prepare = true, apply = true, finish = true;
  bool applied = false;
  bool pendingCleanup = false;
  size_t applyCalls = 0, finishCalls = 0, frees = 0, gcRuns = 0, migrationFrees = 0;
  size_t batchCount = 1, batchesApplied = 0, batchApplyCalls = 0, failBatch = SIZE_MAX;
  std::function<void(size_t)> beforeBatch;
  RedisModuleCmdFunc forceGC = nullptr;
} state;

class DiskSlotTrimTest : public ::testing::Test {
 protected:
  RedisSearchDiskAPI api{};
  RedisSearchDiskAPI *savedDisk = nullptr;
  RedisSearchDisk *savedDb = nullptr;
  bool savedFlex = false;
  StrongRef ref{};
  IndexSpec *spec = nullptr;
  GCContext gc{};
  bool savedEnableGC = false;

  void SetUp() override {
    state = {};
    savedDisk = disk;
    savedDb = disk_db;
    savedFlex = isFlex;
    savedEnableGC = RSGlobalConfig.gcConfigParams.enableGC;
    RSGlobalConfig.gcConfigParams.enableGC = false;
    const char *args[] = {"SCHEMA", "v", "VECTOR",          "FLAT", "6", "TYPE",     "FLOAT32",
                          "DIM",    "2", "DISTANCE_METRIC", "L2",   "g", "GEOSHAPE", "FLAT"};
    QueryError err = QueryError_Default();
    ref = IndexSpec_ParseC(nullptr, "disk_slot_trim", args, sizeof(args) / sizeof(*args), &err);
    spec = static_cast<IndexSpec *>(StrongRef_Get(ref));
    ASSERT_NE(spec, nullptr) << QueryError_GetUserError(&err);
    Spec_AddToDict(ref.rm);
    spec->diskSpec = reinterpret_cast<RedisSearchDiskIndexSpec *>(spec);
    spec->stats.scoring.numDocuments = 5;
    spec->stats.scoring.totalDocsLen = 40;
    disk = &api;
    disk_db = reinterpret_cast<RedisSearchDisk *>(&api);
    isFlex = true;
    gc.gcCtx = DiskGC_Create(ref, &gc.callbacks);
    spec->gc = &gc;
    api.slotMigration.prepareTrim = [](RedisSearchDiskIndexSpec *, const SearchDiskSlotRange *,
                                       size_t, uint64_t *docs,
                                       uint64_t *len) -> SearchDiskSlotTrim * {
      if (!state.prepare) return nullptr;
      if (docs) *docs = state.applied ? 0 : 3;
      if (len) *len = state.applied ? 0 : 21;
      return reinterpret_cast<SearchDiskSlotTrim *>(&state);
    };
    api.slotMigration.applyTrim = [](RedisSearchDiskIndexSpec *, SearchDiskSlotTrim *,
                                     size_t maxBatchDocs, SearchDiskSlotTrimCallback callback,
                                     void *ctx) {
      EXPECT_GE(maxBatchDocs, 3u);
      ++state.applyCalls;
      if (!state.apply) return false;
      for (size_t batch = state.batchesApplied; batch < state.batchCount; ++batch) {
        if (state.beforeBatch) state.beforeBatch(batch);
        const t_docId ids[] = {batch * 3 + 1, batch * 3 + 2, batch * 3 + 3};
        if (!callback(ctx, ids, 3, 21, reinterpret_cast<SearchDiskSlotTrimBatch *>(&state)))
          return false;
      }
      state.applied = true;
      return true;
    };
    api.slotMigration.applyTrimBatch = [](RedisSearchDiskIndexSpec *index,
                                          const SearchDiskSlotTrimBatch *) {
      auto *spec = reinterpret_cast<IndexSpec *>(index);
      int lockResult = -1;
      std::thread probe([&] {
        lockResult = pthread_rwlock_trywrlock(&spec->rwlock);
        if (lockResult == 0) pthread_rwlock_unlock(&spec->rwlock);
      });
      probe.join();
      EXPECT_NE(lockResult, 0) << "batch apply requires the spec write lock";
      ++state.batchApplyCalls;
      if (state.batchesApplied == state.failBatch) return false;
      ++state.batchesApplied;
      return true;
    };
    api.slotMigration.finishTrim = [](RedisSearchDiskIndexSpec *, SearchDiskSlotTrim *) {
      ++state.finishCalls;
      return state.finish;
    };
    api.slotMigration.freeTrim = [](SearchDiskSlotTrim *) { ++state.frees; };
    api.index.isBackgroundWorkPaused = [](RedisSearchDiskIndexSpec *) { return false; };
    api.index.runGC = [](RedisSearchDiskIndexSpec *index, DiskGCRunStats *stats) {
      ++state.gcRuns;
      stats->retryRequired = state.gcRuns == 1;
      if (stats->retryRequired) {
        auto *sp = reinterpret_cast<IndexSpec *>(index);
        if (sp->gc) GCContext_OnWrite(sp->gc);
      }
      if (!stats->retryRequired) stats->num_cleaned_docs = 4;
    };
    disk = &api;
  }

  void TearDown() override {
    if (gc.gcCtx) gc.callbacks.onTerm(gc.gcCtx);
    spec->gc = nullptr;
    spec->diskSpec = nullptr;
    spec->diskRegistered = false;
    disk = savedDisk;
    disk_db = savedDb;
    isFlex = savedFlex;
    Indexes_RemoveSpecFromGlobals(ref, false);
    RSGlobalConfig.gcConfigParams.enableGC = savedEnableGC;
  }

  void reindexSurvivingKey(t_docId oldId, bool live, uint32_t newLength) {
    auto *key = RedisModule_CreateString(RSDummyContext, "trim_survivor", 13);
    RMCK::hset(RSDummyContext, "trim_survivor", "field", "value");
    ASSERT_EQ(DocIdMeta_Set(RSDummyContext, key, spec->specId, oldId), REDISMODULE_OK);
    api.index.createWriteBatch = [](RedisSearchDiskIndexSpec *) {
      return reinterpret_cast<SearchDiskWriteBatchHandle *>(&state);
    };
    api.index.commitWriteBatch = [](SearchDiskWriteBatchHandle *) { return true; };
    state.apply = live;
    api.docTable.putDocument = [](RedisSearchDiskIndexSpec *, SearchDiskWriteBatchHandle *,
                                  const char *, size_t, float, uint32_t, uint32_t, uint32_t,
                                  uint32_t *oldLength, t_expirationTimePoint, t_docId *oldId) {
      EXPECT_NE(*oldId, 0u);
      if (!state.apply) *oldId = 0;
      *oldLength = 0;
      return t_docId{6};
    };
    Document document{};
    Document_Init(&document, key, 1.0, DEFAULT_LANGUAGE, DocumentType_Hash);
    RSAddDocumentCtx context{};
    context.doc = &document;
    context.spec = spec;
    context.fwIdx = NewForwardIndex(&document, spec->flags);
    context.fwIdx->totalFreq = newLength;
    RedisSearchCtx search = SEARCH_CTX_STATIC(RSDummyContext, spec);
    DiskIndexer_StageDocument(&context, &search);
    EXPECT_EQ(context.oldDocId, live ? oldId : 0u);
    DiskIndexer_IndexDocument(&context, &search);
    uint64_t stored = 0;
    EXPECT_EQ(DocIdMeta_Get(RSDummyContext, key, spec->specId, &stored), REDISMODULE_OK);
    EXPECT_EQ(stored, 6u);
    ForwardIndexFree(context.fwIdx);
    Document_Free(&document);
    auto *storedKey = RedisModule_OpenKey(RSDummyContext, key, REDISMODULE_WRITE);
    RedisModule_DeleteKey(storedKey);
    RedisModule_CloseKey(storedKey);
    RedisModule_FreeString(RSDummyContext, key);
  }

  IndexSpecSlotTrimResult trim() {
    SearchDiskSlotRange range{0, 100};
    return IndexSpec_TrimDiskSlots(spec, &range, 1);
  }
  DiskGC *collector() {
    return static_cast<DiskGC *>(gc.gcCtx);
  }
  void unchanged() {
    EXPECT_EQ(spec->stats.scoring.numDocuments, 5u);
    EXPECT_EQ(spec->stats.scoring.totalDocsLen, 40u);
    EXPECT_EQ(collector()->deletesFromLastRun.load(), 0u);
    EXPECT_EQ(state.finishCalls, 0u);
  }
};

TEST_F(DiskSlotTrimTest, AdmissionProbeForwardsSlotsAndFailsClosedWithoutBackend) {
  SearchDiskSlotRange range{10, 20};
  api.slotMigration.canStartMigration = [](RedisSearchDiskIndexSpec *index,
                                           const SearchDiskSlotRange *ranges, size_t count) {
    EXPECT_NE(index, nullptr);
    EXPECT_EQ(count, 1u);
    EXPECT_EQ(ranges[0].start, 10u);
    EXPECT_EQ(ranges[0].end, 20u);
    return state.prepare;
  };
  EXPECT_TRUE(SearchDisk_CanStartSlotMigration(spec->diskSpec, &range, 1));
  state.prepare = false;
  EXPECT_FALSE(SearchDisk_CanStartSlotMigration(spec->diskSpec, &range, 1));
  api.slotMigration.canStartMigration = nullptr;
  EXPECT_FALSE(SearchDisk_CanStartSlotMigration(spec->diskSpec, &range, 1));
}

TEST_F(DiskSlotTrimTest, SharedOperationUsesTrimAccountingAndReleasesCallerHandle) {
  const SearchDiskSlotRange range{7, 9};
  EXPECT_EQ(SearchDisk_BeginSlotMigration(&range, 1), nullptr);
  api.slotMigration.beginMigration = [](RedisSearchDisk *context, const SearchDiskSlotRange *ranges,
                                        size_t count) -> SearchDiskSlotMigration * {
    EXPECT_EQ(context, disk_db);
    EXPECT_EQ(count, 1u);
    EXPECT_EQ(ranges[0].start, 7u);
    EXPECT_EQ(ranges[0].end, 9u);
    return reinterpret_cast<SearchDiskSlotMigration *>(&state);
  };
  auto *migration = SearchDisk_BeginSlotMigration(&range, 1);
  ASSERT_NE(migration, nullptr);
  EXPECT_EQ(IndexSpec_TrimDiskSlotsForMigration(spec, migration), INDEX_TRIM_NOT_APPLIED);
  api.slotMigration.prepareTrimForMigration = [](RedisSearchDiskIndexSpec *index,
                                                 const SearchDiskSlotMigration *operation,
                                                 uint64_t *, uint64_t *) -> SearchDiskSlotTrim * {
    EXPECT_NE(index, nullptr);
    EXPECT_EQ(operation, reinterpret_cast<SearchDiskSlotMigration *>(&state));
    return reinterpret_cast<SearchDiskSlotTrim *>(&state);
  };
  api.slotMigration.prepareTrim = nullptr;
  EXPECT_EQ(IndexSpec_TrimDiskSlotsForMigration(spec, migration), INDEX_TRIM_FINISHED);
  EXPECT_EQ(spec->stats.scoring.numDocuments, 2u);
  EXPECT_EQ(spec->stats.scoring.totalDocsLen, 19u);
  EXPECT_EQ(state.frees, 1u);
  api.slotMigration.freeMigration = [](SearchDiskSlotMigration *operation) {
    EXPECT_EQ(operation, reinterpret_cast<SearchDiskSlotMigration *>(&state));
    ++state.migrationFrees;
  };
  SearchDisk_FreeSlotMigration(migration);
  EXPECT_EQ(state.migrationFrees, 1u);
}

TEST_F(DiskSlotTrimTest, PrepareFailureDoesNotMutateOrFree) {
  state.prepare = false;
  EXPECT_EQ(trim(), INDEX_TRIM_NOT_APPLIED);
  unchanged();
  EXPECT_EQ(state.applyCalls, 0u);
  EXPECT_EQ(state.frees, 0u);
}

TEST_F(DiskSlotTrimTest, BoundsRejectBeforeApplyAndFreePlan) {
  spec->stats.scoring.numDocuments = 2;
  EXPECT_EQ(trim(), INDEX_TRIM_NOT_APPLIED);
  EXPECT_EQ(state.batchApplyCalls, 0u);
  EXPECT_EQ(state.frees, 1u);
  EXPECT_EQ(spec->stats.scoring.numDocuments, 2u);
  spec->stats.scoring.numDocuments = 5;
  spec->stats.scoring.totalDocsLen = 20;
  EXPECT_EQ(trim(), INDEX_TRIM_NOT_APPLIED);
  EXPECT_EQ(state.batchApplyCalls, 0u);
  EXPECT_EQ(state.frees, 2u);
  EXPECT_EQ(spec->stats.scoring.totalDocsLen, 20u);
}

TEST_F(DiskSlotTrimTest, ApplyFailureDoesNotAdjustScoringOrRunFinish) {
  state.apply = false;
  EXPECT_EQ(trim(), INDEX_TRIM_NOT_APPLIED);
  unchanged();
  EXPECT_EQ(state.applyCalls, 1u);
  EXPECT_EQ(state.frees, 1u);
}

TEST_F(DiskSlotTrimTest, FailedFinishThenRetryCountsBulkDeletionOnce) {
  state.finish = false;
  EXPECT_EQ(trim(), INDEX_TRIM_APPLIED);
  EXPECT_EQ(spec->stats.scoring.numDocuments, 2u);
  EXPECT_EQ(spec->stats.scoring.totalDocsLen, 19u);
  EXPECT_EQ(collector()->deletesFromLastRun.load(), 3u);
  EXPECT_EQ(state.frees, 1u);
  state.finish = true;
  EXPECT_EQ(trim(), INDEX_TRIM_FINISHED);
  EXPECT_EQ(spec->stats.scoring.numDocuments, 2u);
  EXPECT_EQ(spec->stats.scoring.totalDocsLen, 19u);
  EXPECT_EQ(collector()->deletesFromLastRun.load(), 3u);
  EXPECT_EQ(state.frees, 2u);
  GCContext_OnDelete(&gc);
  EXPECT_EQ(collector()->deletesFromLastRun.load(), 4u);
}

TEST_F(DiskSlotTrimTest, SuccessfulTrimRemovesOnlySelectedVectorAndGeometryIds) {
  // Build memory indexes before enabling the disk backend for this spec.
  spec->diskSpec = nullptr;
  auto *vectors = openVectorIndex(RSDummyContext, &spec->fields[0], true);
  auto *geometry = OpenGeometryIndex(&spec->fields[1], true);
  ASSERT_NE(vectors, nullptr);
  ASSERT_NE(geometry, nullptr);
  const float vector[] = {1, 2};
  const char *wkt = "POLYGON((0 0,0 1,1 1,1 0,0 0))";
  const auto *geometryApi = GeometryApi_Get(geometry);
  for (t_docId id = 1; id <= 5; ++id) {
    VecSimIndex_AddVector(vectors, vector, id);
    ASSERT_TRUE(
        geometryApi->addGeomStr(geometry, GEOMETRY_FORMAT_WKT, wkt, strlen(wkt), id, nullptr));
  }
  spec->diskSpec = reinterpret_cast<RedisSearchDiskIndexSpec *>(spec);
  ASSERT_EQ(trim(), INDEX_TRIM_FINISHED);
  EXPECT_EQ(spec->stats.scoring.numDocuments, 2u);
  EXPECT_EQ(spec->stats.scoring.totalDocsLen, 19u);
  EXPECT_EQ(VecSimIndex_IndexSize(vectors), 2u);
  for (t_docId id = 1; id <= 5; ++id) {
    EXPECT_EQ(GeometryIndex_HoldsGeom(geometry, id, GEOMETRY_FORMAT_WKT, wkt, strlen(wkt)), id > 3);
  }
  EXPECT_EQ(collector()->deletesFromLastRun.load(), 3u);
  EXPECT_EQ(state.finishCalls, 1u);
  EXPECT_EQ(state.frees, 1u);
}

TEST_F(DiskSlotTrimTest, WriterCanUpdateStatisticsBetweenBatches) {
  state.batchCount = 3;
  spec->stats.scoring.numDocuments = 11;
  spec->stats.scoring.totalDocsLen = 82;
  state.beforeBatch = [&](size_t batch) {
    int lockResult = -1;
    std::thread writer([&] {
      lockResult = pthread_rwlock_trywrlock(&spec->rwlock);
      if (lockResult == 0) {
        if (batch == 1) {
          ++spec->stats.scoring.numDocuments;
          spec->stats.scoring.totalDocsLen += 5;
        }
        pthread_rwlock_unlock(&spec->rwlock);
      }
    });
    writer.join();
    EXPECT_EQ(lockResult, 0) << "spec lock held between batches";
    EXPECT_EQ(spec->stats.scoring.numDocuments, 11u - batch * 3 + (batch > 0));
  };
  EXPECT_EQ(trim(), INDEX_TRIM_FINISHED);
  EXPECT_EQ(state.batchApplyCalls, 3u);
  EXPECT_EQ(spec->stats.scoring.numDocuments, 3u);
  EXPECT_EQ(spec->stats.scoring.totalDocsLen, 24u);
  EXPECT_EQ(collector()->deletesFromLastRun.load(), 9u);
}

TEST_F(DiskSlotTrimTest, PartialApplyIsReportedAndRetryDoesNotRecountCompletedBatches) {
  state.batchCount = 3;
  state.failBatch = 1;
  spec->stats.scoring.numDocuments = 11;
  spec->stats.scoring.totalDocsLen = 82;
  EXPECT_EQ(trim(), INDEX_TRIM_APPLIED);
  EXPECT_EQ(state.batchesApplied, 1u);
  EXPECT_EQ(state.finishCalls, 0u);
  EXPECT_EQ(spec->stats.scoring.numDocuments, 8u);
  EXPECT_EQ(spec->stats.scoring.totalDocsLen, 61u);
  EXPECT_EQ(collector()->deletesFromLastRun.load(), 3u);
  state.failBatch = SIZE_MAX;
  EXPECT_EQ(trim(), INDEX_TRIM_FINISHED);
  EXPECT_EQ(state.batchesApplied, 3u);
  EXPECT_EQ(spec->stats.scoring.numDocuments, 2u);
  EXPECT_EQ(spec->stats.scoring.totalDocsLen, 19u);
  EXPECT_EQ(collector()->deletesFromLastRun.load(), 9u);
}

TEST_F(DiskSlotTrimTest, PendingSlotCleanupRunsBelowThresholdAndRetries) {
  auto &threshold = RSGlobalConfig.gcConfigParams.gcSettings.forkGcCleanThreshold;
  const auto savedThreshold = threshold;
  threshold = 1000;
  state.pendingCleanup = true;
  api.slotMigration.hasPendingCleanup = [](RedisSearchDiskIndexSpec *) {
    return state.pendingCleanup;
  };
  api.index.runGC = [](RedisSearchDiskIndexSpec *, DiskGCRunStats *stats) {
    ++state.gcRuns;
    stats->retryRequired = state.gcRuns == 1;
    if (!stats->retryRequired) {
      stats->num_cleaned_docs = 1;
      state.pendingCleanup = false;
    }
  };
  GCContext_OnDeleteCount(&gc, 1);
  EXPECT_TRUE(gc.callbacks.periodicCallback(gc.gcCtx, nullptr));
  EXPECT_EQ(state.gcRuns, 1u);
  EXPECT_TRUE(state.pendingCleanup);
  EXPECT_EQ(collector()->deletesFromLastRun.load(), 1u);
  EXPECT_TRUE(gc.callbacks.periodicCallback(gc.gcCtx, nullptr));
  EXPECT_EQ(state.gcRuns, 2u);
  EXPECT_FALSE(state.pendingCleanup);
  EXPECT_EQ(collector()->deletesFromLastRun.load(), 0u);
  EXPECT_TRUE(gc.callbacks.periodicCallback(gc.gcCtx, nullptr));
  EXPECT_EQ(state.gcRuns, 2u);
  threshold = savedThreshold;
}

TEST_F(DiskSlotTrimTest, RecoverableGcFailureRetainsPendingChangesUntilRetry) {
  const auto logicallyDeleted = IndexesGlobalStats_GetLogicallyDeletedDocs();
  auto &threshold = RSGlobalConfig.gcConfigParams.gcSettings.forkGcCleanThreshold;
  const auto savedThreshold = threshold;
  threshold = 3;
  GCContext_OnDeleteCount(&gc, 3);
  GCContext_OnWrite(&gc);
  GCContext_OnUpdate(&gc);
  EXPECT_TRUE(gc.callbacks.periodicCallback(gc.gcCtx, nullptr));
  EXPECT_EQ(collector()->deletesFromLastRun.load(), 3u);
  EXPECT_EQ(collector()->writesFromLastRun.load(), 2u);
  EXPECT_EQ(collector()->updatesFromLastRun.load(), 1u);
  EXPECT_EQ(collector()->totalCycles.load(), 0u);
  EXPECT_TRUE(gc.callbacks.periodicCallback(gc.gcCtx, nullptr));
  EXPECT_EQ(state.gcRuns, 2u);
  EXPECT_EQ(collector()->deletesFromLastRun.load(), 0u);
  EXPECT_EQ(collector()->writesFromLastRun.load(), 0u);
  EXPECT_EQ(collector()->updatesFromLastRun.load(), 0u);
  EXPECT_EQ(collector()->totalCycles.load(), 1u);
  EXPECT_EQ(IndexesGlobalStats_GetLogicallyDeletedDocs(), logicallyDeleted);
  threshold = savedThreshold;
}

TEST_F(DiskSlotTrimTest, ForcedGcFailureReportsRetryInsteadOfSuccess) {
  GCContext_OnDeleteCount(&gc, 3);
  GCForcedRun forced = GCForcedRun_New(1000, 10);
  EXPECT_TRUE(gc.callbacks.periodicCallback(gc.gcCtx, &forced));
  EXPECT_EQ(forced.outcome, GC_FORCED_RUN_RETRY_REQUIRED);
  EXPECT_EQ(collector()->deletesFromLastRun.load(), 3u);
  EXPECT_EQ(collector()->totalCycles.load(), 0u);
}

TEST_F(DiskSlotTrimTest, InlineForcedGcReportsFailureAndThenDone) {
  spec->gc = nullptr;
  struct RestoreHooks {
    decltype(RedisModule_CreateSubcommand) create = RedisModule_CreateSubcommand;
    decltype(RedisModule_GetContextFlagsAll) flags = RedisModule_GetContextFlagsAll;
    decltype(RedisModule_ReplyWithSimpleString) reply = RedisModule_ReplyWithSimpleString;
    ~RestoreHooks() {
      RedisModule_CreateSubcommand = create;
      RedisModule_GetContextFlagsAll = flags;
      RedisModule_ReplyWithSimpleString = reply;
    }
  } restore;
  RedisModule_GetContextFlagsAll = [] { return 0; };
  RedisModule_CreateSubcommand = [](RedisModuleCommand *, const char *name,
                                    RedisModuleCmdFunc callback, const char *, int, int, int) {
    if (strcmp(name, "GC_FORCEINVOKE") == 0) state.forceGC = callback;
    return REDISMODULE_OK;
  };
  ASSERT_EQ(RegisterDebugCommands(nullptr), REDISMODULE_OK);
  RedisModule_CreateSubcommand = restore.create;
  ASSERT_NE(state.forceGC, nullptr);
  RMCK::ArgvList args(RSDummyContext, "FT.DEBUG", "GC_FORCEINVOKE", "disk_slot_trim");
  RMCK_GetLastError(RSDummyContext).clear();
  EXPECT_EQ(state.forceGC(RSDummyContext, args.data(), args.size()), REDISMODULE_OK);
  EXPECT_EQ(RMCK_GetLastError(RSDummyContext), "GC FAILED: disk cleanup requires retry");
  RMCK_GetLastError(RSDummyContext).clear();
  RedisModule_ReplyWithSimpleString = [](RedisModuleCtx *ctx, const char *reply) {
    RMCK_GetLastError(ctx) = reply;
    return REDISMODULE_OK;
  };
  EXPECT_EQ(state.forceGC(RSDummyContext, args.data(), args.size()), REDISMODULE_OK);
  EXPECT_EQ(RMCK_GetLastError(RSDummyContext), "DONE");
  EXPECT_EQ(state.gcRuns, 2u);
  RMCK_GetLastError(RSDummyContext).clear();
}

TEST_F(DiskSlotTrimTest, SurvivingKeyAfterFailedFinishIsCountedAsANewInsert) {
  spec->stats.scoring.numDocuments = 3;
  spec->stats.scoring.totalDocsLen = 21;
  state.finish = false;
  EXPECT_EQ(trim(), INDEX_TRIM_APPLIED);
  reindexSurvivingKey(1, false, 11);
  EXPECT_EQ(spec->stats.scoring.numDocuments, 1u);
  EXPECT_EQ(spec->stats.scoring.totalDocsLen, 11u);
  EXPECT_EQ(collector()->updatesFromLastRun.load(), 0u);
  EXPECT_EQ(collector()->writesFromLastRun.load(), 1u);
}

TEST_F(DiskSlotTrimTest, LiveZeroLengthReplacementKeepsTheDocumentCount) {
  spec->stats.scoring.numDocuments = 1;
  spec->stats.scoring.totalDocsLen = 0;
  reindexSurvivingKey(1, true, 0);
  EXPECT_EQ(spec->stats.scoring.numDocuments, 1u);
  EXPECT_EQ(spec->stats.scoring.totalDocsLen, 0u);
  EXPECT_EQ(collector()->updatesFromLastRun.load(), 1u);
}

}  // namespace

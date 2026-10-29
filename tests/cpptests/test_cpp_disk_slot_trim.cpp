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
struct TrimState {
  bool registerAccepted = true, readAccepted = true, liveReplacement = true;
  bool logicalPending = false, pendingCleanup = false, emptyBatch = false;
  uint64_t trimId = 42;
  t_docId maxDocId = 5;
  size_t registrations = 0, applyCalls = 0, gcRuns = 0;
  size_t batchCount = 1, batchesApplied = 0, batchApplyCalls = 0, failBatch = SIZE_MAX;
  std::vector<SearchDiskSlotRange> ranges;
  std::function<void(size_t)> beforeBatch;
  RedisModuleCmdFunc forceGC = nullptr;
  RedisModuleEventCallback persistenceEvent = nullptr, sstEvent = nullptr;
  size_t windowsOpened = 0, windowsClosed = 0, flushes = 0;
  bool lastReopenGate = false;
} state;

class PersistenceEventHooks {
  decltype(RedisModule_SubscribeToServerEvent) subscribe = RedisModule_SubscribeToServerEvent;
  decltype(RedisModule_GetContextFlags) flags = RedisModule_GetContextFlags;

 public:
  PersistenceEventHooks() {
    RedisModule_GetContextFlags = [](RedisModuleCtx *) { return REDISMODULE_CTX_FLAGS_SST_RDB; };
    RedisModule_SubscribeToServerEvent = [](RedisModuleCtx *, RedisModuleEvent event,
                                            RedisModuleEventCallback callback) {
      if (event.id == RedisModuleEvent_Persistence.id) state.persistenceEvent = callback;
      if (event.id == RedisModuleEvent_SSTReplication.id) state.sstEvent = callback;
      return REDISMODULE_OK;
    };
    Initialize_ServerEventNotifications(RSDummyContext);
    RedisModule_SubscribeToServerEvent = subscribe;
  }

  ~PersistenceEventHooks() {
    if (state.persistenceEvent) {
      state.persistenceEvent(RSDummyContext, RedisModuleEvent_Persistence,
                             REDISMODULE_SUBEVENT_PERSISTENCE_FAILED, nullptr);
    }
    RedisModule_SubscribeToServerEvent = subscribe;
    RedisModule_GetContextFlags = flags;
  }
};

IndexSpecDiskSlotTrimResult runSingleTrim(IndexSpec *spec) {
  const SearchDiskSlotRange range{0, 100};
  auto *job = IndexSpec_RegisterDiskSlotTrim(spec->own_ref, &range, 1);
  if (!job) return INDEX_DISK_TRIM_RETRY;
  const auto result = IndexSpec_RunDiskSlotTrim(job);
  IndexSpec_FreeDiskSlotTrim(job);
  return result;
}

class DiskSlotTrimTest : public ::testing::Test {
 protected:
  RedisSearchDiskAPI api{};
  RedisSearchDiskAPI *savedDisk = nullptr;
  RedisSearchDisk *savedDb = nullptr;
  bool savedFlex = false;
  StrongRef ref{};
  IndexSpec *spec = nullptr;
  IndexSpecSlotTrimJob *job = nullptr;
  GCContext gc{};
  bool savedEnableGC = false, cleanPoolStarted = false;

  void SetUp() override {
    state = {};
#ifdef ENABLE_ASSERT
#endif
    SlotCleanup_StartDiskJobs();
    savedDisk = disk;
    savedDb = disk_db;
    savedFlex = isFlex;
    savedEnableGC = RSGlobalConfig.gcConfigParams.enableGC;
    RSGlobalConfig.gcConfigParams.enableGC = false;
    const char *args[] = {"SCHEMA",
                          "v",
                          "VECTOR",
                          "FLAT",
                          "6",
                          "TYPE",
                          "FLOAT32",
                          "DIM",
                          "2",
                          "DISTANCE_METRIC",
                          "L2",
                          "g",
                          "GEOSHAPE",
                          "FLAT",
                          "v2",
                          "VECTOR",
                          "FLAT",
                          "6",
                          "TYPE",
                          "FLOAT32",
                          "DIM",
                          "2",
                          "DISTANCE_METRIC",
                          "L2"};
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
    api.docTable.getDeletedIdsCount = [](RedisSearchDiskIndexSpec *) { return uint64_t{0}; };
    gc.gcCtx = DiskGC_Create(ref, &gc.callbacks);
    spec->gc = &gc;
    api.slotMigration.hasPendingCleanup = [](RedisSearchDiskIndexSpec *) {
      return state.pendingCleanup;
    };
    api.slotMigration.registerTrim = [](RedisSearchDiskIndexSpec *,
                                        const SearchDiskSlotRange *ranges, size_t count,
                                        uint64_t *trimId, t_docId *maxDocId) {
      ++state.registrations;
      if (!state.registerAccepted) return false;
      state.logicalPending = true;
      state.ranges.assign(ranges, ranges + count);
      *trimId = state.trimId;
      *maxDocId = state.maxDocId;
      return true;
    };
    api.slotMigration.forEachPendingTrim =
        [](RedisSearchDiskIndexSpec *, SearchDiskPendingSlotTrimCallback callback, void *ctx) {
          if (state.logicalPending) {
            callback(ctx, state.trimId, state.ranges.data(), state.ranges.size(), state.maxDocId);
          }
        };
    api.slotMigration.runTrimBatch = [](RedisSearchDiskIndexSpec *index, uint64_t trimId,
                                        size_t maxScanned, SearchDiskSlotTrimCallback callback,
                                        void *ctx) {
      EXPECT_EQ(trimId, state.trimId);
      EXPECT_GE(maxScanned, 3u);
      ++state.applyCalls;
      if (!state.readAccepted) return SEARCH_DISK_TRIM_RETRY;
      const size_t batch = state.batchesApplied;
      if (state.beforeBatch) state.beforeBatch(batch);
      const t_docId ids[] = {batch * 3 + 1, batch * 3 + 2, batch * 3 + 3};
      const bool exhausted = batch + 1 == state.batchCount;
      const auto count = state.emptyBatch ? 0u : 3u;
      if (!callback(ctx, ids, count, state.emptyBatch ? 0 : 21,
                    reinterpret_cast<SearchDiskSlotTrimBatch *>(&state), exhausted)) {
        return SEARCH_DISK_TRIM_RETRY;
      }
      EXPECT_EQ(state.batchesApplied, batch + 1);
      return exhausted ? SEARCH_DISK_TRIM_FINISHED : SEARCH_DISK_TRIM_MORE;
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
      if (state.batchesApplied == state.batchCount) state.logicalPending = false;
      return true;
    };
    api.index.isBackgroundWorkPaused = [](RedisSearchDiskIndexSpec *) { return false; };
    api.index.runGC = [](RedisSearchDiskIndexSpec *index, DiskGCRunStats *stats) {
      ++state.gcRuns;
      if (state.gcRuns == 1) {
        auto *sp = reinterpret_cast<IndexSpec *>(index);
        if (sp->gc) GCContext_OnWrite(sp->gc);
        return DISK_GC_RUN_RETRY_REQUIRED;
      }
      stats->num_cleaned_docs = 4;
      return DISK_GC_RUN_OK;
    };
  }

  void TearDown() override {
    if (cleanPoolStarted) CleanPool_Wait();
    SlotCleanup_StopDiskJobs();
#ifdef ENABLE_ASSERT
#endif
    if (job) IndexSpec_FreeDiskSlotTrim(job);
    SlotCleanup_StartDiskJobs();
    if (gc.gcCtx) gc.callbacks.onTerm(gc.gcCtx);
    if (spec) {
      spec->gc = nullptr;
      spec->diskSpec = nullptr;
      spec->diskRegistered = false;
    }
    disk = savedDisk;
    disk_db = savedDb;
    isFlex = savedFlex;
    if (ref.rm) Indexes_RemoveSpecFromGlobals(ref, false);
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
    state.liveReplacement = live;
    api.docTable.putDocument = [](RedisSearchDiskIndexSpec *, SearchDiskWriteBatchHandle *,
                                  const char *, size_t, float, uint32_t, uint32_t, uint32_t,
                                  uint32_t *oldLength, t_expirationTimePoint, t_docId *oldId) {
      EXPECT_NE(*oldId, 0u);
      if (!state.liveReplacement) *oldId = 0;
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

  IndexSpecDiskSlotTrimResult trim() {
    if (!job) {
      const SearchDiskSlotRange range{0, 100};
      job = IndexSpec_RegisterDiskSlotTrim(ref, &range, 1);
    }
    return job ? IndexSpec_RunDiskSlotTrim(job) : INDEX_DISK_TRIM_RETRY;
  }
  void mockConsistencyWindows() {
    spec->gc = nullptr;
    api.index.openConsistencyWindow = [](RedisSearchDiskIndexSpec *) { ++state.windowsOpened; };
    api.index.closeConsistencyWindow = [](RedisSearchDiskIndexSpec *, bool reopenGate) {
      ++state.windowsClosed;
      state.lastReopenGate = reopenGate;
    };
    api.index.flush = [](RedisSearchDiskIndexSpec *) { ++state.flushes; };
  }
  void startCleanPool() {
    CleanPool_ThreadPoolStart();
    cleanPoolStarted = true;
  }
  void scheduleAutomaticTrim() {
    auto *slots = static_cast<RedisModuleSlotRangeArray *>(rm_malloc(SlotRangeArray_SizeOf(1)));
    slots->num_ranges = 1;
    slots->ranges[0] = {0, 100};
    Indexes_DeleteSlots(slots);
    rm_free(slots);
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
  size_t pendingDeletes() {
    return collector()->deletesFromLastRun.load() +
           __atomic_load_n(&spec->stats.gcDeferredDeletes, __ATOMIC_RELAXED);
  }
  bool pendingFilter(t_docId id = 1, uint16_t slot = 7) {
    IndexSpec_LockRead(spec);
    const bool hidden = DocTable_IsDocPendingDeletionForSlot(&spec->docs, id, slot);
    IndexSpec_Unlock(spec);
    return hidden;
  }
};

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
    return DISK_GC_RUN_OK;
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

TEST_F(DiskSlotTrimTest, RegistrationInstallsCutoffFilterWithoutScanning) {
  const SearchDiskSlotRange range{7, 9};
  state.maxDocId = 17;
  job = IndexSpec_RegisterDiskSlotTrim(ref, &range, 1);
  ASSERT_NE(job, nullptr);
  EXPECT_EQ(state.registrations, 1u);
  EXPECT_EQ(state.applyCalls, 0u);
  EXPECT_EQ(state.ranges[0].start, 7u);
  EXPECT_EQ(state.ranges[0].end, 9u);
  EXPECT_TRUE(pendingFilter(17, 7));
  EXPECT_FALSE(pendingFilter(18, 7));
  EXPECT_FALSE(pendingFilter(1, 10));
  EXPECT_EQ(spec->stats.scoring.numDocuments, 5u);
  EXPECT_EQ(pendingDeletes(), 0u);
}

TEST_F(DiskSlotTrimTest, RegistrationFailureDoesNotHideOrMutateDocuments) {
  state.registerAccepted = false;
  EXPECT_EQ(trim(), INDEX_DISK_TRIM_RETRY);
  EXPECT_EQ(job, nullptr);
  EXPECT_FALSE(pendingFilter());
  EXPECT_EQ(state.applyCalls, 0u);
  EXPECT_EQ(spec->stats.scoring.numDocuments, 5u);
  EXPECT_EQ(spec->stats.scoring.totalDocsLen, 40u);
}

TEST_F(DiskSlotTrimTest, BoundsRejectBeforeApplyingTheFinalBatch) {
  spec->stats.scoring.numDocuments = 2;
  EXPECT_EQ(trim(), INDEX_DISK_TRIM_RETRY);
  EXPECT_EQ(state.batchApplyCalls, 0u);
  EXPECT_EQ(spec->stats.scoring.numDocuments, 2u);
  EXPECT_TRUE(pendingFilter());
  spec->stats.scoring.numDocuments = 5;
  spec->stats.scoring.totalDocsLen = 20;
  EXPECT_EQ(trim(), INDEX_DISK_TRIM_RETRY);
  EXPECT_EQ(state.batchApplyCalls, 0u);
  EXPECT_EQ(spec->stats.scoring.totalDocsLen, 20u);
  EXPECT_TRUE(state.logicalPending);
}

TEST_F(DiskSlotTrimTest, ReadFailureRetainsTheFilterAndCursorForRetry) {
  state.readAccepted = false;
  EXPECT_EQ(trim(), INDEX_DISK_TRIM_RETRY);
  EXPECT_EQ(spec->stats.scoring.numDocuments, 5u);
  EXPECT_EQ(spec->stats.scoring.totalDocsLen, 40u);
  EXPECT_EQ(state.batchesApplied, 0u);
  EXPECT_EQ(state.batchApplyCalls, 0u);
  EXPECT_TRUE(pendingFilter());
  state.readAccepted = true;
  EXPECT_EQ(trim(), INDEX_DISK_TRIM_FINISHED);
  EXPECT_FALSE(pendingFilter());
}

TEST_F(DiskSlotTrimTest, FinalApplyFailureDoesNotCompleteCleanupOrDoubleAccountOnRetry) {
  state.failBatch = 0;
  EXPECT_EQ(trim(), INDEX_DISK_TRIM_RETRY);
  EXPECT_EQ(trim(), INDEX_DISK_TRIM_RETRY);
  EXPECT_TRUE(state.logicalPending);
  EXPECT_TRUE(pendingFilter());
  EXPECT_EQ(spec->stats.scoring.numDocuments, 5u);
  EXPECT_EQ(pendingDeletes(), 0u);
  state.failBatch = SIZE_MAX;
  EXPECT_EQ(trim(), INDEX_DISK_TRIM_FINISHED);
  EXPECT_FALSE(state.logicalPending);
  EXPECT_FALSE(pendingFilter());
  EXPECT_EQ(spec->stats.scoring.numDocuments, 2u);
  EXPECT_EQ(spec->stats.scoring.totalDocsLen, 19u);
  EXPECT_EQ(pendingDeletes(), 3u);
  EXPECT_EQ(trim(), INDEX_DISK_TRIM_FINISHED);
  EXPECT_EQ(state.applyCalls, 3u);
  EXPECT_EQ(state.batchesApplied, 1u);
  EXPECT_EQ(pendingDeletes(), 3u);
}

TEST_F(DiskSlotTrimTest, EmptyFinalBatchStillCompletesTheLogicalJob) {
  state.emptyBatch = true;
  EXPECT_EQ(trim(), INDEX_DISK_TRIM_FINISHED);
  EXPECT_EQ(state.batchApplyCalls, 1u);
  EXPECT_EQ(state.batchesApplied, 1u);
  EXPECT_FALSE(state.logicalPending);
  EXPECT_FALSE(pendingFilter());
  EXPECT_EQ(spec->stats.scoring.numDocuments, 5u);
  EXPECT_EQ(spec->stats.scoring.totalDocsLen, 40u);
  EXPECT_EQ(pendingDeletes(), 0u);
}

TEST_F(DiskSlotTrimTest, OneWorkerTurnAppliesOnlyOneBatch) {
  state.batchCount = 3;
  state.maxDocId = 11;
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
    EXPECT_EQ(lockResult, 0) << "spec lock held during storage scan";
  };
  EXPECT_EQ(trim(), INDEX_DISK_TRIM_MORE);
  EXPECT_EQ(state.batchesApplied, 1u);
  EXPECT_EQ(spec->stats.scoring.numDocuments, 8u);
  EXPECT_TRUE(pendingFilter());
  EXPECT_EQ(trim(), INDEX_DISK_TRIM_MORE);
  EXPECT_EQ(state.batchesApplied, 2u);
  EXPECT_EQ(spec->stats.scoring.numDocuments, 6u);
  EXPECT_EQ(trim(), INDEX_DISK_TRIM_FINISHED);
  EXPECT_EQ(state.batchesApplied, 3u);
  EXPECT_EQ(spec->stats.scoring.numDocuments, 3u);
  EXPECT_EQ(spec->stats.scoring.totalDocsLen, 24u);
  EXPECT_EQ(pendingDeletes(), 9u);
}

TEST_F(DiskSlotTrimTest, RetryDoesNotReapplyACompletedEarlierBatch) {
  state.batchCount = 2;
  state.maxDocId = 8;
  spec->stats.scoring.numDocuments = 8;
  spec->stats.scoring.totalDocsLen = 70;
  EXPECT_EQ(trim(), INDEX_DISK_TRIM_MORE);
  state.failBatch = 1;
  EXPECT_EQ(trim(), INDEX_DISK_TRIM_RETRY);
  EXPECT_EQ(state.batchesApplied, 1u);
  EXPECT_EQ(spec->stats.scoring.numDocuments, 5u);
  EXPECT_EQ(spec->stats.scoring.totalDocsLen, 49u);
  EXPECT_EQ(pendingDeletes(), 3u);
  EXPECT_TRUE(pendingFilter());
  state.failBatch = SIZE_MAX;
  EXPECT_EQ(trim(), INDEX_DISK_TRIM_FINISHED);
  EXPECT_EQ(state.batchesApplied, 2u);
  EXPECT_EQ(spec->stats.scoring.numDocuments, 2u);
  EXPECT_EQ(spec->stats.scoring.totalDocsLen, 28u);
  EXPECT_EQ(pendingDeletes(), 6u);
}

TEST_F(DiskSlotTrimTest, WorkerDeletesSelectedIdsFromEveryNativeIndex) {
  spec->diskSpec = nullptr;
  auto *vectors = openVectorIndex(RSDummyContext, &spec->fields[0], true);
  auto *otherVectors = openVectorIndex(RSDummyContext, &spec->fields[2], true);
  auto *geometry = OpenGeometryIndex(&spec->fields[1], true);
  ASSERT_NE(vectors, nullptr);
  ASSERT_NE(otherVectors, nullptr);
  ASSERT_NE(geometry, nullptr);
  const float vector[] = {1, 2};
  const char *wkt = "POLYGON((0 0,0 1,1 1,1 0,0 0))";
  const auto *geometryApi = GeometryApi_Get(geometry);
  for (t_docId id = 1; id <= 8; ++id) {
    VecSimIndex_AddVector(vectors, vector, id);
    VecSimIndex_AddVector(otherVectors, vector, id);
    ASSERT_TRUE(
        geometryApi->addGeomStr(geometry, GEOMETRY_FORMAT_WKT, wkt, strlen(wkt), id, nullptr));
  }
  spec->diskSpec = reinterpret_cast<RedisSearchDiskIndexSpec *>(spec);
  spec->stats.scoring.numDocuments = 8;
  spec->stats.scoring.totalDocsLen = 70;
  state.batchCount = 2;
  state.maxDocId = 8;
  const auto submittingThread = std::this_thread::get_id();
  state.beforeBatch = [&](size_t batch) {
    EXPECT_NE(std::this_thread::get_id(), submittingThread);
    EXPECT_EQ(VecSimIndex_IndexSize(vectors), 8u - batch * 3);
    EXPECT_EQ(VecSimIndex_IndexSize(otherVectors), 8u - batch * 3);
  };
  const SearchDiskSlotRange range{0, 100};
  job = IndexSpec_RegisterDiskSlotTrim(ref, &range, 1);
  ASSERT_NE(job, nullptr);
  IndexSpecDiskSlotTrimResult result;
  std::thread worker([&] { result = IndexSpec_RunDiskSlotTrim(job); });
  worker.join();
  EXPECT_EQ(result, INDEX_DISK_TRIM_MORE);
  std::thread nextWorker([&] { result = IndexSpec_RunDiskSlotTrim(job); });
  nextWorker.join();
  EXPECT_EQ(result, INDEX_DISK_TRIM_FINISHED);
  EXPECT_EQ(VecSimIndex_IndexSize(vectors), 2u);
  EXPECT_EQ(VecSimIndex_IndexSize(otherVectors), 2u);
  for (t_docId id = 1; id <= 8; ++id) {
    EXPECT_EQ(GeometryIndex_HoldsGeom(geometry, id, GEOMETRY_FORMAT_WKT, wkt, strlen(wkt)), id > 6);
  }
  EXPECT_EQ(spec->stats.scoring.numDocuments, 2u);
}

TEST_F(DiskSlotTrimTest, LogicalCompletionDoesNotRequireAGcContextOrPhysicalCollection) {
  spec->gc = nullptr;
  state.pendingCleanup = true;
  EXPECT_EQ(trim(), INDEX_DISK_TRIM_FINISHED);
  EXPECT_FALSE(state.logicalPending);
  EXPECT_FALSE(pendingFilter());
  EXPECT_TRUE(state.pendingCleanup);
  EXPECT_EQ(state.gcRuns, 0u);
  EXPECT_EQ(__atomic_load_n(&spec->stats.gcDeferredDeletes, __ATOMIC_RELAXED), 3u);
}

TEST_F(DiskSlotTrimTest, DeferredTrimDeletesSurviveGcRetry) {
  const auto logicallyDeleted = IndexesGlobalStats_GetLogicallyDeletedDocs();
  ASSERT_EQ(trim(), INDEX_DISK_TRIM_FINISHED);
  EXPECT_EQ(collector()->deletesFromLastRun.load(), 0u);
  EXPECT_EQ(__atomic_load_n(&spec->stats.gcDeferredDeletes, __ATOMIC_RELAXED), 3u);
  GCForcedRun forced = GCForcedRun_New(1000, 10);
  EXPECT_TRUE(gc.callbacks.periodicCallback(gc.gcCtx, &forced));
  EXPECT_EQ(forced.outcome, GC_DISK_FORCED_RUN_RETRY_REQUIRED);
  EXPECT_EQ(collector()->deletesFromLastRun.load(), 3u);
  EXPECT_EQ(__atomic_load_n(&spec->stats.gcDeferredDeletes, __ATOMIC_RELAXED), 0u);
  IndexSpec_DeferGCDeletes(spec, 1);
  EXPECT_TRUE(gc.callbacks.periodicCallback(gc.gcCtx, &forced));
  EXPECT_EQ(pendingDeletes(), 0u);
  EXPECT_EQ(state.gcRuns, 2u);
  EXPECT_EQ(IndexesGlobalStats_GetLogicallyDeletedDocs(), logicallyDeleted);
}

TEST_F(DiskSlotTrimTest, ConcurrentTrimCollectedByGcDoesNotUnderflowAfterIndexDrop) {
  const auto logicallyDeleted = IndexesGlobalStats_GetLogicallyDeletedDocs();
  api.index.runGC = [](RedisSearchDiskIndexSpec *index, DiskGCRunStats *stats) {
    auto *sp = reinterpret_cast<IndexSpec *>(index);
    IndexSpecDiskSlotTrimResult result = INDEX_DISK_TRIM_RETRY;
    std::thread writer([&] { result = runSingleTrim(sp); });
    writer.join();
    EXPECT_EQ(result, INDEX_DISK_TRIM_FINISHED);
    stats->num_cleaned_docs = 3;
    return DISK_GC_RUN_OK;
  };
  GCForcedRun forced = GCForcedRun_New(1000, 10);
  ASSERT_TRUE(gc.callbacks.periodicCallback(gc.gcCtx, &forced));
  EXPECT_EQ(IndexesGlobalStats_GetLogicallyDeletedDocs(), logicallyDeleted);
  EXPECT_EQ(collector()->logicallyDeleted.load(), 0u);
  dropIndexBeforeCollectorTermination();
  EXPECT_EQ(IndexesGlobalStats_GetLogicallyDeletedDocs(), logicallyDeleted);
}

TEST_F(DiskSlotTrimTest, LateTrimAndUpdateRemainAccountedAcrossCyclesAndIndexDrop) {
  const auto logicallyDeleted = IndexesGlobalStats_GetLogicallyDeletedDocs();
  GCContext_OnDelete(&gc);
  GCContext_OnDelete(&gc);
  api.index.runGC = [](RedisSearchDiskIndexSpec *index, DiskGCRunStats *stats) {
    auto *sp = reinterpret_cast<IndexSpec *>(index);
    if (++state.gcRuns == 1) {
      stats->num_cleaned_docs = 2;
      EXPECT_EQ(runSingleTrim(sp), INDEX_DISK_TRIM_FINISHED);
    } else {
      stats->num_cleaned_docs = 3;
      GCContext_OnUpdate(sp->gc);
    }
    return DISK_GC_RUN_OK;
  };
  GCForcedRun forced = GCForcedRun_New(1000, 10);
  ASSERT_TRUE(gc.callbacks.periodicCallback(gc.gcCtx, &forced));
  EXPECT_EQ(IndexesGlobalStats_GetLogicallyDeletedDocs(), logicallyDeleted + 3);
  EXPECT_EQ(collector()->logicallyDeleted.load(), 3u);
  ASSERT_TRUE(gc.callbacks.periodicCallback(gc.gcCtx, &forced));
  EXPECT_EQ(IndexesGlobalStats_GetLogicallyDeletedDocs(), logicallyDeleted + 1);
  EXPECT_EQ(collector()->logicallyDeleted.load(), 1u);
  dropIndexBeforeCollectorTermination();
  EXPECT_EQ(IndexesGlobalStats_GetLogicallyDeletedDocs(), logicallyDeleted);
}

TEST_F(DiskSlotTrimTest, DeferredTrimDuringFailedGcSurvivesRetryAndIndexDrop) {
  const auto logicallyDeleted = IndexesGlobalStats_GetLogicallyDeletedDocs();
  api.index.runGC = [](RedisSearchDiskIndexSpec *index, DiskGCRunStats *stats) {
    auto *sp = reinterpret_cast<IndexSpec *>(index);
    if (++state.gcRuns == 1) {
      EXPECT_EQ(runSingleTrim(sp), INDEX_DISK_TRIM_FINISHED);
      return DISK_GC_RUN_RETRY_REQUIRED;
    } else {
      stats->num_cleaned_docs = 3;
    }
    return DISK_GC_RUN_OK;
  };
  GCForcedRun forced = GCForcedRun_New(1000, 10);
  ASSERT_TRUE(gc.callbacks.periodicCallback(gc.gcCtx, &forced));
  EXPECT_EQ(forced.outcome, GC_DISK_FORCED_RUN_RETRY_REQUIRED);
  EXPECT_EQ(IndexesGlobalStats_GetLogicallyDeletedDocs(), logicallyDeleted + 3);
  EXPECT_EQ(pendingDeletes(), 3u);
  ASSERT_TRUE(gc.callbacks.periodicCallback(gc.gcCtx, &forced));
  EXPECT_EQ(IndexesGlobalStats_GetLogicallyDeletedDocs(), logicallyDeleted);
  dropIndexBeforeCollectorTermination();
  EXPECT_EQ(IndexesGlobalStats_GetLogicallyDeletedDocs(), logicallyDeleted);
}

TEST_F(DiskSlotTrimTest, IndexDropRetiresOutstandingDeletesAfterPartialCollection) {
  const auto logicallyDeleted = IndexesGlobalStats_GetLogicallyDeletedDocs();
  for (size_t i = 0; i < 3; ++i) GCContext_OnDelete(&gc);
  api.index.runGC = [](RedisSearchDiskIndexSpec *, DiskGCRunStats *stats) {
    stats->num_cleaned_docs = 1;
    return DISK_GC_RUN_OK;
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

TEST_F(DiskSlotTrimTest, RestoredIdsAndLateTrimKeepExactGlobalAccounting) {
  const auto logicallyDeleted = IndexesGlobalStats_GetLogicallyDeletedDocs();
  gc.callbacks.onTerm(gc.gcCtx);
  api.docTable.getDeletedIdsCount = [](RedisSearchDiskIndexSpec *) { return uint64_t{4}; };
  gc.gcCtx = DiskGC_Create(ref, &gc.callbacks);
  ASSERT_EQ(IndexesGlobalStats_GetLogicallyDeletedDocs(), logicallyDeleted + 4);
  api.index.runGC = [](RedisSearchDiskIndexSpec *index, DiskGCRunStats *stats) {
    if (++state.gcRuns == 1) {
      stats->num_cleaned_docs = 4;
      EXPECT_EQ(runSingleTrim(reinterpret_cast<IndexSpec *>(index)), INDEX_DISK_TRIM_FINISHED);
    } else {
      stats->num_cleaned_docs = 3;
    }
    return DISK_GC_RUN_OK;
  };
  GCForcedRun forced = GCForcedRun_New(1000, 10);
  ASSERT_TRUE(gc.callbacks.periodicCallback(gc.gcCtx, &forced));
  EXPECT_EQ(collector()->logicallyDeleted.load(), 3u);
  EXPECT_EQ(IndexesGlobalStats_GetLogicallyDeletedDocs(), logicallyDeleted + 3);
  ASSERT_TRUE(gc.callbacks.periodicCallback(gc.gcCtx, &forced));
  EXPECT_EQ(collector()->logicallyDeleted.load(), 0u);
  EXPECT_EQ(IndexesGlobalStats_GetLogicallyDeletedDocs(), logicallyDeleted);
  dropIndexBeforeCollectorTermination();
  EXPECT_EQ(IndexesGlobalStats_GetLogicallyDeletedDocs(), logicallyDeleted);
}

TEST_F(DiskSlotTrimTest, UnchargedStorageIdsDoNotConsumeOtherIndexesGlobalContribution) {
  const auto logicallyDeleted = IndexesGlobalStats_GetLogicallyDeletedDocs();
  IndexsGlobalStats_IncreaseLogicallyDeleted(4);
  api.index.runGC = [](RedisSearchDiskIndexSpec *, DiskGCRunStats *stats) {
    stats->num_cleaned_docs = 3;
    return DISK_GC_RUN_OK;
  };
  GCForcedRun forced = GCForcedRun_New(1000, 10);
  EXPECT_TRUE(gc.callbacks.periodicCallback(gc.gcCtx, &forced));
  EXPECT_EQ(IndexesGlobalStats_GetLogicallyDeletedDocs(), logicallyDeleted + 4);
  dropIndexBeforeCollectorTermination();
  EXPECT_EQ(IndexesGlobalStats_GetLogicallyDeletedDocs(), logicallyDeleted + 4);
  IndexsGlobalStats_DecreaseLogicallyDeleted(4);
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
    if (state.gcRuns == 1) return DISK_GC_RUN_RETRY_REQUIRED;
    stats->num_cleaned_docs = 1;
    state.pendingCleanup = false;
    return DISK_GC_RUN_OK;
  };
  GCContext_OnDelete(&gc);
  EXPECT_TRUE(gc.callbacks.periodicCallback(gc.gcCtx, nullptr));
  EXPECT_EQ(state.gcRuns, 1u);
  EXPECT_TRUE(state.pendingCleanup);
  EXPECT_EQ(pendingDeletes(), 1u);
  EXPECT_TRUE(gc.callbacks.periodicCallback(gc.gcCtx, nullptr));
  EXPECT_EQ(state.gcRuns, 2u);
  EXPECT_FALSE(state.pendingCleanup);
  EXPECT_EQ(pendingDeletes(), 0u);
  EXPECT_TRUE(gc.callbacks.periodicCallback(gc.gcCtx, nullptr));
  EXPECT_EQ(state.gcRuns, 2u);
  threshold = savedThreshold;
}

TEST_F(DiskSlotTrimTest, RecoverableGcFailureRetainsPendingChangesUntilRetry) {
  const auto logicallyDeleted = IndexesGlobalStats_GetLogicallyDeletedDocs();
  auto &threshold = RSGlobalConfig.gcConfigParams.gcSettings.forkGcCleanThreshold;
  const auto savedThreshold = threshold;
  threshold = 3;
  for (size_t i = 0; i < 3; ++i) GCContext_OnDelete(&gc);
  GCContext_OnWrite(&gc);
  GCContext_OnUpdate(&gc);
  EXPECT_TRUE(gc.callbacks.periodicCallback(gc.gcCtx, nullptr));
  EXPECT_EQ(pendingDeletes(), 3u);
  EXPECT_EQ(collector()->writesFromLastRun.load(), 2u);
  EXPECT_EQ(collector()->updatesFromLastRun.load(), 1u);
  EXPECT_EQ(collector()->totalCycles.load(), 0u);
  EXPECT_TRUE(gc.callbacks.periodicCallback(gc.gcCtx, nullptr));
  EXPECT_EQ(state.gcRuns, 2u);
  EXPECT_EQ(pendingDeletes(), 0u);
  EXPECT_EQ(collector()->writesFromLastRun.load(), 0u);
  EXPECT_EQ(collector()->updatesFromLastRun.load(), 0u);
  EXPECT_EQ(collector()->totalCycles.load(), 1u);
  EXPECT_EQ(IndexesGlobalStats_GetLogicallyDeletedDocs(), logicallyDeleted);
  threshold = savedThreshold;
}

TEST_F(DiskSlotTrimTest, ForcedGcFailureReportsRetryInsteadOfSuccess) {
  for (size_t i = 0; i < 3; ++i) GCContext_OnDelete(&gc);
  GCForcedRun forced = GCForcedRun_New(1000, 10);
  EXPECT_TRUE(gc.callbacks.periodicCallback(gc.gcCtx, &forced));
  EXPECT_EQ(forced.outcome, GC_DISK_FORCED_RUN_RETRY_REQUIRED);
  EXPECT_EQ(pendingDeletes(), 3u);
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

TEST_F(DiskSlotTrimTest, SurvivingDeletedIdIsCountedAsANewInsert) {
  spec->stats.scoring.numDocuments = 3;
  spec->stats.scoring.totalDocsLen = 21;
  EXPECT_EQ(trim(), INDEX_DISK_TRIM_FINISHED);
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

TEST_F(DiskSlotTrimTest, BlockedForcedGcRepliesWithTheActualRunOutcome) {
  static std::mutex mutex;
  static std::condition_variable completed;
  static void *replyData;
  static RedisModuleCmdFunc replyCallback;
  static void (*freeData)(RedisModuleCtx *, void *);
  struct RestoreHooks {
    decltype(RedisModule_CreateSubcommand) create = RedisModule_CreateSubcommand;
    decltype(RedisModule_GetContextFlagsAll) flags = RedisModule_GetContextFlagsAll;
    decltype(RedisModule_BlockClient) block = RedisModule_BlockClient;
    decltype(RedisModule_UnblockClient) unblock = RedisModule_UnblockClient;
    decltype(RedisModule_GetBlockedClientPrivateData) data =
        RedisModule_GetBlockedClientPrivateData;
    decltype(RedisModule_ReplyWithSimpleString) reply = RedisModule_ReplyWithSimpleString;
    ~RestoreHooks() {
      RedisModule_CreateSubcommand = create;
      RedisModule_GetContextFlagsAll = flags;
      RedisModule_BlockClient = block;
      RedisModule_UnblockClient = unblock;
      RedisModule_GetBlockedClientPrivateData = data;
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
  RedisModule_BlockClient = [](RedisModuleCtx *, RedisModuleCmdFunc reply, RedisModuleCmdFunc,
                               void (*release)(RedisModuleCtx *, void *), long long) {
    replyCallback = reply;
    freeData = release;
    return reinterpret_cast<RedisModuleBlockedClient *>(&state);
  };
  RedisModule_UnblockClient = [](RedisModuleBlockedClient *, void *data) {
    std::lock_guard<std::mutex> guard(mutex);
    replyData = data;
    completed.notify_one();
    return REDISMODULE_OK;
  };
  RedisModule_GetBlockedClientPrivateData = [](RedisModuleCtx *) { return replyData; };
  RedisModule_ReplyWithSimpleString = [](RedisModuleCtx *ctx, const char *reply) {
    RMCK_GetLastError(ctx) = reply;
    return REDISMODULE_OK;
  };
  GC_ThreadPoolStart();
  struct DrainPool {
    ~DrainPool() {
      GC_ThreadPoolPause();
      GC_ThreadPoolWaitForPause(10000);
      GC_ThreadPoolResume();
    }
  } drain;
  RMCK::ArgvList args(RSDummyContext, "FT.DEBUG", "GC_FORCEINVOKE", "disk_slot_trim");
  for (const char *expected : {"GC FAILED: disk cleanup requires retry", "DONE"}) {
    replyData = nullptr;
    ASSERT_EQ(state.forceGC(RSDummyContext, args.data(), args.size()), REDISMODULE_OK);
    {
      std::unique_lock<std::mutex> guard(mutex);
      ASSERT_TRUE(completed.wait_for(guard, std::chrono::seconds(10), [] { return replyData; }));
    }
    RMCK_GetLastError(RSDummyContext).clear();
    ASSERT_NE(replyCallback, nullptr);
    EXPECT_EQ(replyCallback(RSDummyContext, args.data(), args.size()), REDISMODULE_OK);
    EXPECT_EQ(RMCK_GetLastError(RSDummyContext), expected);
    freeData(RSDummyContext, replyData);
  }
  EXPECT_EQ(state.gcRuns, 2u);
  RMCK_GetLastError(RSDummyContext).clear();
}

TEST_F(DiskSlotTrimTest, FreeingAJobRetainsItsPendingIntentAndFilter) {
  const SearchDiskSlotRange range{0, 100};
  job = IndexSpec_RegisterDiskSlotTrim(ref, &range, 1);
  ASSERT_NE(job, nullptr);
  IndexSpec_FreeDiskSlotTrim(job);
  job = nullptr;
  EXPECT_TRUE(state.logicalPending);
  EXPECT_TRUE(pendingFilter());
  EXPECT_EQ(state.applyCalls, 0u);
  EXPECT_EQ(spec->stats.scoring.numDocuments, 5u);
}

TEST_F(DiskSlotTrimTest, InvalidatedOwnerCancelsBeforeCallingTheBackend) {
  static size_t released;
  released = 0;
  StrongRef owner = StrongRef_New(spec, [](void *) { ++released; });
  const SearchDiskSlotRange range{0, 100};
  job = IndexSpec_RegisterDiskSlotTrim(owner, &range, 1);
  ASSERT_NE(job, nullptr);
  StrongRef_Invalidate(owner);
  StrongRef_Release(owner);
  EXPECT_EQ(released, 1u);
  EXPECT_EQ(IndexSpec_RunDiskSlotTrim(job), INDEX_DISK_TRIM_CANCELLED);
  EXPECT_EQ(state.applyCalls, 0u);
}

TEST_F(DiskSlotTrimTest, RegistrationRejectsMissingOwnersAndNonDiskIndexes) {
  const SearchDiskSlotRange range{0, 100};
  EXPECT_EQ(IndexSpec_RegisterDiskSlotTrim(INVALID_STRONG_REF, &range, 1), nullptr);
  spec->diskSpec = nullptr;
  EXPECT_EQ(IndexSpec_RegisterDiskSlotTrim(ref, &range, 1), nullptr);
  spec->diskSpec = reinterpret_cast<RedisSearchDiskIndexSpec *>(spec);
  EXPECT_EQ(state.registrations, 0u);
}

TEST_F(DiskSlotTrimTest, StoppedWorkerDoesNotTouchTheBackend) {
  const SearchDiskSlotRange range{0, 100};
  job = IndexSpec_RegisterDiskSlotTrim(ref, &range, 1);
  ASSERT_NE(job, nullptr);
  SlotCleanup_StopDiskJobs();
  EXPECT_EQ(IndexSpec_RunDiskSlotTrim(job), INDEX_DISK_TRIM_CANCELLED);
  EXPECT_EQ(state.applyCalls, 0u);
  EXPECT_TRUE(state.logicalPending);
}

TEST_F(DiskSlotTrimTest, ShutdownWaitsForTheCurrentBatchAndPreventsAnother) {
  state.batchCount = 2;
  state.maxDocId = 8;
  std::mutex mutex;
  std::condition_variable progress;
  bool entered = false, release = false, stopping = false, stopped = false;
  state.beforeBatch = [&](size_t) {
    std::unique_lock<std::mutex> guard(mutex);
    entered = true;
    progress.notify_all();
    progress.wait(guard, [&] { return release; });
  };
  const SearchDiskSlotRange range{0, 100};
  job = IndexSpec_RegisterDiskSlotTrim(ref, &range, 1);
  ASSERT_NE(job, nullptr);
  IndexSpecDiskSlotTrimResult result;
  std::thread worker([&] { result = IndexSpec_RunDiskSlotTrim(job); });
  {
    std::unique_lock<std::mutex> guard(mutex);
    EXPECT_TRUE(progress.wait_for(guard, std::chrono::seconds(10), [&] { return entered; }));
  }
  std::thread stopper([&] {
    {
      std::lock_guard<std::mutex> guard(mutex);
      stopping = true;
      progress.notify_all();
    }
    SlotCleanup_StopDiskJobs();
    std::lock_guard<std::mutex> guard(mutex);
    stopped = true;
    progress.notify_all();
  });
  {
    std::unique_lock<std::mutex> guard(mutex);
    EXPECT_TRUE(progress.wait_for(guard, std::chrono::seconds(10), [&] { return stopping; }));
    EXPECT_FALSE(progress.wait_for(guard, std::chrono::milliseconds(100), [&] { return stopped; }));
    release = true;
    progress.notify_all();
  }
  worker.join();
  stopper.join();
  EXPECT_EQ(result, INDEX_DISK_TRIM_MORE);
  EXPECT_TRUE(stopped);
  EXPECT_EQ(IndexSpec_RunDiskSlotTrim(job), INDEX_DISK_TRIM_CANCELLED);
  EXPECT_EQ(state.applyCalls, 1u);
}

TEST_F(DiskSlotTrimTest, PausedWorkerDoesNotBeginAStorageScan) {
  SlotCleanup_PauseDiskJobs();
  EXPECT_EQ(trim(), INDEX_DISK_TRIM_RETRY);
  EXPECT_EQ(state.applyCalls, 0u);
  EXPECT_TRUE(pendingFilter());
  SlotCleanup_ResumeDiskJobs();
  EXPECT_EQ(trim(), INDEX_DISK_TRIM_FINISHED);
}

}  // namespace

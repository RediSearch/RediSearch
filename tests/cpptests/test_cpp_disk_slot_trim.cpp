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
    api.index.isBackgroundWorkPaused = [](RedisSearchDiskIndexSpec *) { return false; };
    api.index.runGC = [](RedisSearchDiskIndexSpec *index, DiskGCRunStats *stats) {
      ++state.gcRuns;
      if (state.gcRuns == 1) {
        auto *sp = reinterpret_cast<IndexSpec *>(index);
        if (sp->gc) GCContext_OnWrite(sp->gc);
        return;
      }
      stats->num_cleaned_docs = 4;

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

TEST_F(DiskSlotTrimTest, RegistrationRejectsMissingOwnersAndNonDiskIndexes) {
  const SearchDiskSlotRange range{0, 100};
  EXPECT_EQ(IndexSpec_RegisterDiskSlotTrim(INVALID_STRONG_REF, &range, 1), nullptr);
  spec->diskSpec = nullptr;
  EXPECT_EQ(IndexSpec_RegisterDiskSlotTrim(ref, &range, 1), nullptr);
  spec->diskSpec = reinterpret_cast<RedisSearchDiskIndexSpec *>(spec);
  EXPECT_EQ(state.registrations, 0u);
}

TEST_F(DiskSlotTrimTest, SurvivingDeletedIdIsCountedAsANewInsert) {
  spec->stats.scoring.numDocuments = 0;
  spec->stats.scoring.totalDocsLen = 0;
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

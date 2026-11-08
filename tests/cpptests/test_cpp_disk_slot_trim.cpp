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
#include "geometry/geometry_api.h"
#include "slot_cleanup.h"
#include "slot_ranges.h"
#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <exception>
#include <functional>
#include <future>
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
#include "indexer.h"
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
std::mutex completionMutex;
std::vector<std::pair<RedisModuleEventLoopOneShotFunc, void *>> completions;

void drainCleanup() {
  for (;;) {
    CleanPool_Wait();
    std::vector<std::pair<RedisModuleEventLoopOneShotFunc, void *>> ready;
    {
      std::lock_guard<std::mutex> lock(completionMutex);
      ready.swap(completions);
    }
    if (ready.empty()) return;
    for (auto [callback, arg] : ready) callback(arg);
  }
}

struct TrimState {
  bool registerAccepted = true, readAccepted = true, createAccepted = true;
  size_t batchesCreated = 0, batchesFreed = 0, batchesAborted = 0;
  bool logicalPending = false, emptyBatch = false;
  t_docId maxDocId = 5;
  size_t registrations = 0, applyCalls = 0, gcRuns = 0, nonGcCompactions = 0;
  size_t batchCount = 1, batchesApplied = 0, batchApplyCalls = 0, failBatch = SIZE_MAX;
  std::vector<SearchDiskSlotRange> ranges;
  std::function<void(size_t)> beforeBatch;
  std::function<void()> beforeBackendClose;
  RedisModuleCmdFunc forceGC = nullptr;
  RedisModuleEventCallback persistenceEvent = nullptr, sstEvent = nullptr, cronEvent = nullptr;
  size_t windowsOpened = 0, windowsClosed = 0, flushes = 0;
  bool lastReopenGate = false;
} state;

struct WriteBatchState {
  IndexSpec *spec;
  bool open = true, staged = false;
};

class ServerEventHooks {
  decltype(RedisModule_SubscribeToServerEvent) subscribe = RedisModule_SubscribeToServerEvent;
  decltype(RedisModule_GetContextFlags) flags = RedisModule_GetContextFlags;

 public:
  ServerEventHooks() {
    RedisModule_GetContextFlags = [](RedisModuleCtx *) { return REDISMODULE_CTX_FLAGS_SST_RDB; };
    RedisModule_SubscribeToServerEvent = [](RedisModuleCtx *, RedisModuleEvent event,
                                            RedisModuleEventCallback callback) {
      if (event.id == RedisModuleEvent_CronLoop.id) state.cronEvent = callback;
      if (event.id == RedisModuleEvent_Persistence.id) state.persistenceEvent = callback;
      if (event.id == RedisModuleEvent_SSTReplication.id) state.sstEvent = callback;
      return REDISMODULE_OK;
    };
    Initialize_ServerEventNotifications(RSDummyContext);
    RedisModule_SubscribeToServerEvent = subscribe;
  }

  ~ServerEventHooks() {
    if (state.persistenceEvent) {
      state.persistenceEvent(RSDummyContext, RedisModuleEvent_Persistence,
                             REDISMODULE_SUBEVENT_PERSISTENCE_FAILED, nullptr);
    }
    RedisModule_SubscribeToServerEvent = subscribe;
    RedisModule_GetContextFlags = flags;
  }
};

IndexSpecDiskSlotTrimResult runSingleTrim(IndexSpec *spec) {
  if (CleanPool_IsPaused()) return INDEX_DISK_TRIM_RETRY;
  const SearchDiskSlotRange range{0, 100};
  auto *job = IndexSpec_RegisterDiskSlotTrim(spec->own_ref, &range, 1);
  if (!job) return INDEX_DISK_TRIM_RETRY;
  const auto result = IndexSpec_RunDiskSlotTrim(job);
  IndexSpec_FreeDiskSlotTrim(job);
  return result;
}

class DiskSlotTrimTest : public ::testing::Test {
 protected:
  decltype(RedisModule_EventLoopAddOneShot) savedOneShot = nullptr;
  RedisSearchDiskAPI api{};
  RedisSearchDiskAPI *savedDisk = nullptr;
  RedisSearchDisk *savedDb = nullptr;
  bool savedFlex = false, savedLoading = false;
  StrongRef ref{};
  IndexSpec *spec = nullptr;
  IndexSpecSlotTrimJob *job = nullptr;
  GCContext gc{};
  bool savedEnableGC = false, cleanPoolStarted = false;

  void SetUp() override {
    savedOneShot = RedisModule_EventLoopAddOneShot;
    RedisModule_EventLoopAddOneShot = [](RedisModuleEventLoopOneShotFunc callback, void *arg) {
      std::lock_guard<std::mutex> lock(completionMutex);
      completions.emplace_back(callback, arg);
      return REDISMODULE_OK;
    };
    CleanPool_ThreadPoolStart();
    state = {};
#ifdef ENABLE_ASSERT
    SlotCleanup_FailNextDiskEnqueues(0);
#endif
    SlotCleanup_StartDiskJobs();
    savedDisk = disk;
    savedDb = disk_db;
    savedFlex = isFlex;
    savedLoading = g_isLoading;
    g_isLoading = false;
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
    api.slotMigration.registerTrim = [](RedisSearchDiskIndexSpec *,
                                        const SearchDiskSlotRange *ranges, size_t count,
                                        t_docId *maxDocId) {
      ++state.registrations;
      if (!state.registerAccepted) return false;
      state.logicalPending = true;
      state.ranges.assign(ranges, ranges + count);
      *maxDocId = state.maxDocId;
      return true;
    };
    api.slotMigration.forEachPendingTrim =
        [](RedisSearchDiskIndexSpec *, SearchDiskPendingSlotTrimCallback callback, void *ctx) {
          if (state.logicalPending) {
            callback(ctx, state.ranges.data(), state.ranges.size(), state.maxDocId);
          }
        };
    api.slotMigration.runTrimBatch = [](RedisSearchDiskIndexSpec *index,
                                        SearchDiskWriteBatchHandle *writeBatch,
                                        const SearchDiskSlotRange *ranges, size_t numRanges,
                                        t_docId maxDocId, size_t maxScanned,
                                        SearchDiskSlotTrimCallback callback, void *ctx) {
      EXPECT_EQ(maxDocId, state.maxDocId);
      EXPECT_EQ(numRanges, state.ranges.size());
      for (size_t i = 0; i < numRanges; ++i) {
        EXPECT_EQ(ranges[i].start, state.ranges[i].start);
        EXPECT_EQ(ranges[i].end, state.ranges[i].end);
      }
      EXPECT_GE(maxScanned, 3u);
      ++state.applyCalls;
      if (!state.readAccepted) return SEARCH_DISK_TRIM_RETRY;
      auto *staged = reinterpret_cast<WriteBatchState *>(writeBatch);
      EXPECT_EQ(staged->spec, reinterpret_cast<IndexSpec *>(index));
      EXPECT_TRUE(staged->open);
      staged->staged = true;
      const size_t batch = state.batchesApplied;
      if (state.beforeBatch) state.beforeBatch(batch);
      const t_docId ids[] = {batch * 3 + 1, batch * 3 + 2, batch * 3 + 3};
      const bool exhausted = batch + 1 == state.batchCount;
      const auto count = state.emptyBatch ? 0u : 3u;
      if (!callback(ctx, ids, count, state.emptyBatch ? 0 : 21,
                    writeBatch, exhausted)) {
        return SEARCH_DISK_TRIM_RETRY;
      }
      EXPECT_EQ(state.batchesApplied, batch + 1);
      return exhausted ? SEARCH_DISK_TRIM_FINISHED : SEARCH_DISK_TRIM_MORE;
    };
    api.index.createWriteBatch = [](RedisSearchDiskIndexSpec *index) -> SearchDiskWriteBatchHandle * {
      if (!state.createAccepted) return nullptr;
      ++state.batchesCreated;
      return reinterpret_cast<SearchDiskWriteBatchHandle *>(
          new WriteBatchState{reinterpret_cast<IndexSpec *>(index)});
    };
    api.index.commitWriteBatch = [](SearchDiskWriteBatchHandle *handle) {
      auto *batch = reinterpret_cast<WriteBatchState *>(handle);
      auto *spec = batch->spec;
      int lockResult = -1;
      std::thread probe([&] {
        lockResult = pthread_rwlock_trywrlock(&spec->rwlock);
        if (lockResult == 0) pthread_rwlock_unlock(&spec->rwlock);
      });
      probe.join();
      EXPECT_NE(lockResult, 0) << "batch apply requires the spec write lock";
      if (!batch->open) return false;
      batch->open = false;
      EXPECT_TRUE(batch->staged);
      ++state.batchApplyCalls;
      if (state.batchesApplied == state.failBatch) return false;
      ++state.batchesApplied;
      if (state.batchesApplied == state.batchCount) state.logicalPending = false;
      return true;
    };
    api.index.abortWriteBatch = [](SearchDiskWriteBatchHandle *handle) {
      auto *batch = reinterpret_cast<WriteBatchState *>(handle);
      EXPECT_TRUE(batch->open);
      batch->open = false;
      ++state.batchesAborted;
    };
    api.index.freeWriteBatch = [](SearchDiskWriteBatchHandle *handle) {
      ++state.batchesFreed;
      delete reinterpret_cast<WriteBatchState *>(handle);
    };
    api.index.isBackgroundWorkPaused = [](RedisSearchDiskIndexSpec *) { return false; };
    api.index.runGC = [](RedisSearchDiskIndexSpec *index, DiskGCRunStats *stats) {
      ++state.gcRuns;
      stats->bytes_collected = state.gcRuns == 1 ? 100 : 200;
      stats->cycle_time_ms = state.gcRuns == 1 ? 7 : 11;
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
    SlotCleanup_ResumeDiskJobs();
    drainCleanup();
    EXPECT_EQ(state.batchesCreated, state.batchesFreed);
    SlotCleanup_StopDiskJobs();
    drainCleanup();
    RedisModule_EventLoopAddOneShot = savedOneShot;
#ifdef ENABLE_ASSERT
    SlotCleanup_FailNextDiskEnqueues(0);
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
    g_isLoading = savedLoading;
    if (ref.rm) Indexes_RemoveSpecFromGlobals(ref, false);
    RSGlobalConfig.gcConfigParams.enableGC = savedEnableGC;
  }

  IndexSpecDiskSlotTrimResult trim() {
    if (!job) {
      const SearchDiskSlotRange range{0, 100};
      job = IndexSpec_RegisterDiskSlotTrim(ref, &range, 1);
    }
    return job && !CleanPool_IsPaused() ? IndexSpec_RunDiskSlotTrim(job) : INDEX_DISK_TRIM_RETRY;
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
  bool pendingTrim(t_docId id = 1, uint16_t slot = 7) {
    struct Lookup {
      t_docId id;
      uint16_t slot;
      bool covered = false;
    } lookup{id, slot};
    IndexSpec_LockWrite(spec);
    EXPECT_EQ(spec->docs.pendingSlotDeletions, nullptr);
    SearchDisk_ForEachPendingSlotTrim(
        spec->diskSpec,
        [](void *ctx, const SearchDiskSlotRange *ranges, size_t count, t_docId cutoff) {
          auto &lookup = *static_cast<Lookup *>(ctx);
          for (size_t i = 0; i < count; ++i) {
            lookup.covered |= lookup.id <= cutoff && ranges[i].start <= lookup.slot &&
                              lookup.slot <= ranges[i].end;
          }
        },
        &lookup);
    IndexSpec_Unlock(spec);
    return lookup.covered;
  }
};

TEST_F(DiskSlotTrimTest, AccountingWaitsForWriteLockedDeletionNotification) {
  static std::function<void()> afterStorageDelete;
  static std::promise<void> *backendReached;
  std::promise<void> backend, finished;
  auto backendFuture = backend.get_future();
  auto finishedFuture = finished.get_future();
  std::thread gcWorker;
  backendReached = &backend;
  const auto before = IndexesGlobalStats_GetLogicallyDeletedDocs();
  spec->stats.scoring.numDocuments = 1;
  spec->stats.scoring.totalDocsLen = 1;
  api.index.runGC = [](RedisSearchDiskIndexSpec *, DiskGCRunStats *stats) {
    stats->num_cleaned_docs = 1;
    backendReached->set_value();
    return DISK_GC_RUN_OK;
  };
  afterStorageDelete = [&] {
    EXPECT_TRUE(IndexSpec_IsLocked(spec));
    gcWorker = std::thread([&] {
      GCForcedRun forced = GCForcedRun_New(1000, 10);
      EXPECT_TRUE(gc.callbacks.periodicCallback(gc.gcCtx, &forced));
      finished.set_value();
    });
    EXPECT_EQ(backendFuture.wait_for(std::chrono::seconds(3)), std::future_status::ready);
    EXPECT_EQ(finishedFuture.wait_for(std::chrono::milliseconds(50)), std::future_status::timeout);
  };
  api.index.deleteDocumentById = [](RedisSearchDiskIndexSpec *, t_docId, uint32_t *length) {
    afterStorageDelete();
    *length = 1;
    return true;
  };
  IndexSpec_DeleteDocById(spec, 1);
  gcWorker.join();
  afterStorageDelete = {};
  backendReached = nullptr;
  EXPECT_EQ(spec->stats.scoring.numDocuments, 0u);
  EXPECT_EQ(collector()->logicallyDeleted.load(), 0u);
  EXPECT_EQ(IndexesGlobalStats_GetLogicallyDeletedDocs(), before);
}

TEST_F(DiskSlotTrimTest, IndexDropRetiresOutstandingDeletesAfterPartialCollection) {
  const auto logicallyDeleted = IndexesGlobalStats_GetLogicallyDeletedDocs();
  GCContext_OnDelete(&gc);
  GCContext_OnUpdate(&gc);
  GCContext_OnDelete(&gc);
  EXPECT_EQ(collector()->deletesFromLastRun.load(), 2u);
  EXPECT_EQ(collector()->updatesFromLastRun.load(), 1u);
  api.index.runGC = [](RedisSearchDiskIndexSpec *, DiskGCRunStats *stats) {
    stats->num_cleaned_docs = 1;
    return DISK_GC_RUN_OK;
  };
  GCForcedRun forced = GCForcedRun_New(1000, 10);
  ASSERT_TRUE(gc.callbacks.periodicCallback(gc.gcCtx, &forced));
  EXPECT_EQ(collector()->deletesFromLastRun.load(), 0u);
  EXPECT_EQ(collector()->updatesFromLastRun.load(), 0u);
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
    return DISK_GC_RUN_OK;
  };
  GCForcedRun forced = GCForcedRun_New(1000, 10);
  EXPECT_TRUE(gc.callbacks.periodicCallback(gc.gcCtx, &forced));
  EXPECT_EQ(IndexesGlobalStats_GetLogicallyDeletedDocs(), logicallyDeleted + 4);
  dropIndexBeforeCollectorTermination();
  EXPECT_EQ(IndexesGlobalStats_GetLogicallyDeletedDocs(), logicallyDeleted + 4);
  IndexsGlobalStats_DecreaseLogicallyDeleted(4);
}

TEST_F(DiskSlotTrimTest, RegistrationLeavesVisibilityAndCutoffInTheBackend) {
  const SearchDiskSlotRange range{7, 9};
  state.maxDocId = 17;
  job = IndexSpec_RegisterDiskSlotTrim(ref, &range, 1);
  ASSERT_NE(job, nullptr);
  EXPECT_EQ(state.registrations, 1u);
  EXPECT_EQ(state.applyCalls, 0u);
  EXPECT_EQ(state.ranges[0].start, 7u);
  EXPECT_EQ(state.ranges[0].end, 9u);
  EXPECT_TRUE(pendingTrim(17, 7));
  EXPECT_FALSE(pendingTrim(18, 7));
  EXPECT_FALSE(pendingTrim(1, 10));
  EXPECT_EQ(spec->stats.scoring.numDocuments, 5u);
  EXPECT_EQ(pendingDeletes(), 0u);
}

TEST_F(DiskSlotTrimTest, RegistrationFailureIsAContractViolation) {
  state.registerAccepted = false;
  // The mock throws on assertions; isolate it because the fatal path retains locks.
  EXPECT_EXIT(
      {
        try {
          trim();
        } catch (const std::exception &) {
          std::_Exit(1);
        }
      },
      ::testing::ExitedWithCode(1), "valid slot cleanup registration must succeed");
}

TEST_F(DiskSlotTrimTest, AppliedBatchMustFitScoringStatistics) {
  spec->stats.scoring.numDocuments = 2;
  EXPECT_EXIT(
      {
        try {
          trim();
        } catch (const std::exception &) {
          std::_Exit(1);
        }
      },
      ::testing::ExitedWithCode(1), "slot cleanup exceeds the document count");
  spec->stats.scoring.numDocuments = 5;
  spec->stats.scoring.totalDocsLen = 20;
  EXPECT_EXIT(
      {
        try {
          trim();
        } catch (const std::exception &) {
          std::_Exit(1);
        }
      },
      ::testing::ExitedWithCode(1), "slot cleanup exceeds the total document length");
}

TEST_F(DiskSlotTrimTest, StaleBatchRetriesBeforeCheckingScoringBounds) {
  state.failBatch = 0;
  spec->stats.scoring.numDocuments = 0;
  spec->stats.scoring.totalDocsLen = 0;
  EXPECT_EQ(trim(), INDEX_DISK_TRIM_RETRY);
  EXPECT_EQ(state.batchApplyCalls, 1u);
  EXPECT_EQ(spec->stats.scoring.numDocuments, 0u);
  EXPECT_EQ(spec->stats.scoring.totalDocsLen, 0u);
  EXPECT_TRUE(pendingTrim());
}

TEST_F(DiskSlotTrimTest, BatchAllocationFailureLeavesTheJobRetryable) {
  state.createAccepted = false;
  EXPECT_EQ(trim(), INDEX_DISK_TRIM_RETRY);
  EXPECT_EQ(state.applyCalls, 0u);
  EXPECT_EQ(state.batchesCreated, 0u);
  EXPECT_TRUE(state.logicalPending);
  EXPECT_EQ(spec->stats.scoring.numDocuments, 5u);
  state.createAccepted = true;
  EXPECT_EQ(trim(), INDEX_DISK_TRIM_FINISHED);
  EXPECT_EQ(state.batchesApplied, 1u);
}

TEST_F(DiskSlotTrimTest, ReadFailureRetainsTheFilterAndCursorForRetry) {
  state.readAccepted = false;
  EXPECT_EQ(trim(), INDEX_DISK_TRIM_RETRY);
  EXPECT_EQ(spec->stats.scoring.numDocuments, 5u);
  EXPECT_EQ(spec->stats.scoring.totalDocsLen, 40u);
  EXPECT_EQ(state.batchesApplied, 0u);
  EXPECT_EQ(state.batchApplyCalls, 0u);
  EXPECT_EQ(state.batchesCreated, 1u);
  EXPECT_EQ(state.batchesFreed, 1u);
  EXPECT_TRUE(pendingTrim());
  state.readAccepted = true;
  EXPECT_EQ(trim(), INDEX_DISK_TRIM_FINISHED);
  EXPECT_FALSE(pendingTrim());
}

TEST_F(DiskSlotTrimTest, FinalApplyFailureDoesNotCompleteCleanupOrDoubleAccountOnRetry) {
  state.failBatch = 0;
  EXPECT_EQ(trim(), INDEX_DISK_TRIM_RETRY);
  EXPECT_EQ(trim(), INDEX_DISK_TRIM_RETRY);
  EXPECT_TRUE(state.logicalPending);
  EXPECT_TRUE(pendingTrim());
  EXPECT_EQ(spec->stats.scoring.numDocuments, 5u);
  EXPECT_EQ(pendingDeletes(), 0u);
  state.failBatch = SIZE_MAX;
  EXPECT_EQ(trim(), INDEX_DISK_TRIM_FINISHED);
  EXPECT_FALSE(state.logicalPending);
  EXPECT_FALSE(pendingTrim());
  EXPECT_EQ(spec->stats.scoring.numDocuments, 2u);
  EXPECT_EQ(spec->stats.scoring.totalDocsLen, 19u);
  EXPECT_EQ(pendingDeletes(), 3u);
  EXPECT_EQ(state.applyCalls, 3u);
  EXPECT_EQ(state.batchesApplied, 1u);
  EXPECT_EQ(state.batchesCreated, 3u);
  EXPECT_EQ(state.batchesFreed, 3u);
}

TEST_F(DiskSlotTrimTest, EmptyFinalBatchStillCompletesTheLogicalJob) {
  state.emptyBatch = true;
  EXPECT_EQ(trim(), INDEX_DISK_TRIM_FINISHED);
  EXPECT_EQ(state.batchApplyCalls, 1u);
  EXPECT_EQ(state.batchesApplied, 1u);
  EXPECT_EQ(state.batchesCreated, 1u);
  EXPECT_EQ(state.batchesFreed, 1u);
  EXPECT_FALSE(state.logicalPending);
  EXPECT_FALSE(pendingTrim());
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
  EXPECT_TRUE(pendingTrim());
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
  EXPECT_TRUE(pendingTrim());
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
  EXPECT_EQ(trim(), INDEX_DISK_TRIM_FINISHED);
  EXPECT_FALSE(state.logicalPending);
  EXPECT_FALSE(pendingTrim());
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

TEST_F(DiskSlotTrimTest, RetryPreservesPartialStatisticsAndPendingChanges) {
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
  EXPECT_EQ(collector()->totalCycles.load(), 1u);
  EXPECT_EQ(collector()->totalCollectedBytes.load(), 100);
  EXPECT_EQ(collector()->totalTimeMs.load(), 7u);
  EXPECT_EQ(collector()->lastRunTimeMs.load(), 7u);
  EXPECT_EQ(IndexesGlobalStats_GetLogicallyDeletedDocs(), logicallyDeleted + 4);
  EXPECT_TRUE(gc.callbacks.periodicCallback(gc.gcCtx, nullptr));
  EXPECT_EQ(state.gcRuns, 2u);
  EXPECT_EQ(pendingDeletes(), 0u);
  EXPECT_EQ(collector()->writesFromLastRun.load(), 0u);
  EXPECT_EQ(collector()->updatesFromLastRun.load(), 0u);
  EXPECT_EQ(collector()->totalCycles.load(), 2u);
  EXPECT_EQ(collector()->totalCollectedBytes.load(), 300);
  EXPECT_EQ(collector()->totalTimeMs.load(), 18u);
  EXPECT_EQ(collector()->lastRunTimeMs.load(), 11u);
  EXPECT_EQ(IndexesGlobalStats_GetLogicallyDeletedDocs(), logicallyDeleted);
  threshold = savedThreshold;
}

TEST_F(DiskSlotTrimTest, ForcedGcFailureReportsRetryInsteadOfSuccess) {
  for (size_t i = 0; i < 3; ++i) GCContext_OnDelete(&gc);
  GCForcedRun forced = GCForcedRun_New(1000, 10);
  EXPECT_TRUE(gc.callbacks.periodicCallback(gc.gcCtx, &forced));
  EXPECT_EQ(forced.outcome, GC_DISK_FORCED_RUN_RETRY_REQUIRED);
  EXPECT_EQ(pendingDeletes(), 3u);
  EXPECT_EQ(collector()->totalCycles.load(), 1u);
}

TEST_F(DiskSlotTrimTest, CompactAllRequiresSuccessfulGcAndReportsIncompleteCompaction) {
  api.index.compactNonGcStorage = [](RedisSearchDiskIndexSpec *) {
    return ++state.nonGcCompactions == 2;
  };
  auto invoke = [&] {
    GCForcedRun forced = GCForcedRun_New(1000, 10);
    forced.compactAll = true;
    EXPECT_TRUE(gc.callbacks.periodicCallback(gc.gcCtx, &forced));
    return forced.outcome;
  };
  EXPECT_EQ(invoke(), GC_DISK_FORCED_RUN_RETRY_REQUIRED);
  EXPECT_EQ(state.nonGcCompactions, 0u);
  EXPECT_EQ(invoke(), GC_FORCED_RUN_COMPACT_ALL_INCOMPLETE);
  EXPECT_EQ(state.nonGcCompactions, 1u);
  EXPECT_EQ(invoke(), GC_FORCED_RUN_OK);
  EXPECT_EQ(state.nonGcCompactions, 2u);
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
  RMCK::ArgvList compactArgs(RSDummyContext, "FT.DEBUG", "GC_FORCEINVOKE", "disk_slot_trim",
                             "COMPACT_ALL");
  RMCK_GetLastError(RSDummyContext).clear();
  EXPECT_EQ(state.forceGC(RSDummyContext, compactArgs.data(), compactArgs.size()), REDISMODULE_OK);
  EXPECT_EQ(RMCK_GetLastError(RSDummyContext),
            "COMPACT_ALL requires the index's GC, which is disabled");
  EXPECT_EQ(state.gcRuns, 0u);
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
  EXPECT_TRUE(pendingTrim());
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

TEST_F(DiskSlotTrimTest, RegistrationReturnsIndependentHandlesForBackendIntent) {
  const SearchDiskSlotRange range{7, 9};
  state.maxDocId = 17;
  job = IndexSpec_RegisterDiskSlotTrim(ref, &range, 1);
  ASSERT_NE(job, nullptr);
  auto *other = IndexSpec_RegisterDiskSlotTrim(ref, &range, 1);
  ASSERT_NE(other, nullptr);
  EXPECT_NE(job, other);
  IndexSpec_FreeDiskSlotTrim(other);
  EXPECT_TRUE(pendingTrim(17, 7));
  EXPECT_FALSE(pendingTrim(18, 7));
  EXPECT_EQ(state.registrations, 2u);
  EXPECT_EQ(state.applyCalls, 0u);
}

TEST_F(DiskSlotTrimTest, BackendCloseStopsRegistrationBeforeDestroyingContext) {
  const SearchDiskSlotRange range{7, 9};
  bool closed = false;
  state.beforeBackendClose = [&] {
    closed = true;
    EXPECT_NE(spec->diskSpec, nullptr);
    EXPECT_EQ(IndexSpec_RegisterDiskSlotTrim(ref, &range, 1), nullptr);
  };
  api.metrics.stopMetrics = [](RedisSearchDisk *) {};
  api.basic.close = [](RedisModuleCtx *, RedisSearchDisk *) { state.beforeBackendClose(); };
  SearchDisk_Close(RSDummyContext);
  state.beforeBackendClose = {};
  EXPECT_TRUE(closed);
  EXPECT_EQ(disk_db, nullptr);
  EXPECT_NE(spec->diskSpec, nullptr);
  EXPECT_EQ(IndexSpec_RegisterDiskSlotTrim(ref, &range, 1), nullptr);
  EXPECT_EQ(state.registrations, 0u);
  EXPECT_EQ(spec->docs.pendingSlotDeletions, nullptr);
}

TEST_F(DiskSlotTrimTest, RestoredTrimEnumerationPreservesRangesAndOriginalCutoff) {
  state.logicalPending = true;
  state.ranges = {{7, 9}, {12, 14}};
  state.maxDocId = 17;
  spec->docs.maxDocId = 99;
  struct RestoreContext {
    IndexSpec *spec;
    size_t calls = 0;
  } restore{spec};

  IndexSpec_LockWrite(spec);
  SearchDisk_ForEachPendingSlotTrim(
      spec->diskSpec,
      [](void *ctx, const SearchDiskSlotRange *ranges, size_t count, t_docId cutoff) {
        auto &restore = *static_cast<RestoreContext *>(ctx);
        ++restore.calls;
        ASSERT_EQ(count, 2u);
        EXPECT_EQ(ranges[0].start, 7);
        EXPECT_EQ(ranges[0].end, 9);
        EXPECT_EQ(ranges[1].start, 12);
        EXPECT_EQ(ranges[1].end, 14);
        EXPECT_EQ(cutoff, 17u);
        EXPECT_EQ(restore.spec->docs.pendingSlotDeletions, nullptr);
      },
      &restore);
  IndexSpec_Unlock(spec);

  EXPECT_EQ(restore.calls, 1u);
  for (uint16_t slot : {7, 9, 12, 14}) {
    EXPECT_TRUE(pendingTrim(17, slot));
    EXPECT_FALSE(pendingTrim(18, slot));
  }
  for (uint16_t slot : {6, 10, 11, 15}) EXPECT_FALSE(pendingTrim(1, slot));
  EXPECT_EQ(state.registrations, 0u);
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
  startCleanPool();
  struct Work {
    IndexSpecSlotTrimJob *job;
    IndexSpecDiskSlotTrimResult *result;
  } work{job, &result};
  ASSERT_EQ(CleanPool_AddWork(
                [](void *arg) {
                  auto *work = static_cast<Work *>(arg);
                  *work->result = IndexSpec_RunDiskSlotTrim(work->job);
                },
                &work),
            0);
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
  CleanPool_Wait();
  stopper.join();
  EXPECT_EQ(result, INDEX_DISK_TRIM_MORE);
  EXPECT_TRUE(stopped);
  EXPECT_EQ(IndexSpec_RunDiskSlotTrim(job), INDEX_DISK_TRIM_CANCELLED);
  EXPECT_EQ(state.applyCalls, 1u);
}

TEST_F(DiskSlotTrimTest, AutomaticCleanupRequeuesBehindOtherCleanPoolWork) {
  startCleanPool();
  state.batchCount = 2;
  state.maxDocId = 8;
  spec->stats.scoring.numDocuments = 8;
  spec->stats.scoring.totalDocsLen = 70;
  bool observed = false;
  struct Observer {
    IndexSpec *spec;
    bool *observed;
  } observer{spec, &observed};
  state.beforeBatch = [&](size_t batch) {
    if (batch != 0) return;
    EXPECT_EQ(CleanPool_AddWork(
                  [](void *arg) {
                    auto *observer = static_cast<Observer *>(arg);
                    EXPECT_EQ(state.batchesApplied, 1u);
                    EXPECT_EQ(observer->spec->stats.scoring.numDocuments, 5u);
                    *observer->observed = true;
                  },
                  &observer),
              0);
  };
  auto *slots = static_cast<RedisModuleSlotRangeArray *>(rm_malloc(SlotRangeArray_SizeOf(1)));
  slots->num_ranges = 1;
  slots->ranges[0] = {0, 100};
  Indexes_DeleteSlots(slots);
  rm_free(slots);
  drainCleanup();
  EXPECT_TRUE(observed);
  EXPECT_EQ(state.registrations, 1u);
  EXPECT_EQ(state.applyCalls, 2u);
  EXPECT_EQ(spec->stats.scoring.numDocuments, 2u);
  EXPECT_FALSE(pendingTrim());
}

#ifdef ENABLE_ASSERT
TEST_F(DiskSlotTrimTest, RegisteredCronWaitsForInitializationAndLoadingBeforeRetrying) {
  ServerEventHooks hooks;
  ASSERT_NE(state.cronEvent, nullptr);
  startCleanPool();
  SlotCleanup_FailNextDiskEnqueues(1);
  scheduleAutomaticTrim();
  drainCleanup();
  ASSERT_TRUE(state.logicalPending);
  ASSERT_EQ(state.applyCalls, 0u);

  auto *initializedDb = disk_db;
  disk_db = nullptr;
  state.cronEvent(RSDummyContext, RedisModuleEvent_CronLoop, 0, nullptr);
  drainCleanup();
  EXPECT_EQ(state.applyCalls, 0u);
  EXPECT_TRUE(pendingTrim());

  disk_db = initializedDb;
  g_isLoading = true;
  state.cronEvent(RSDummyContext, RedisModuleEvent_CronLoop, 0, nullptr);
  drainCleanup();
  EXPECT_EQ(state.applyCalls, 0u);
  EXPECT_TRUE(state.logicalPending);

  g_isLoading = false;
  state.cronEvent(RSDummyContext, RedisModuleEvent_CronLoop, 0, nullptr);
  drainCleanup();
  EXPECT_EQ(state.registrations, 1u);
  EXPECT_EQ(state.applyCalls, 1u);
  EXPECT_EQ(state.batchesApplied, 1u);
  EXPECT_FALSE(state.logicalPending);
  EXPECT_FALSE(pendingTrim());
}

TEST_F(DiskSlotTrimTest, RejectedInitialEnqueueRetainsIntentUntilCronRetry) {
  startCleanPool();
  const auto caller = std::this_thread::get_id();
  state.beforeBatch = [caller](size_t batch) {
    EXPECT_NE(std::this_thread::get_id(), caller);
    EXPECT_EQ(batch, 0u);
  };
  SlotCleanup_FailNextDiskEnqueues(1);
  scheduleAutomaticTrim();
  drainCleanup();

  EXPECT_EQ(state.registrations, 1u);
  EXPECT_EQ(state.applyCalls, 0u);
  EXPECT_EQ(state.batchApplyCalls, 0u);
  EXPECT_EQ(state.batchesApplied, 0u);
  EXPECT_TRUE(state.logicalPending);
  EXPECT_TRUE(pendingTrim());
  EXPECT_EQ(spec->stats.scoring.numDocuments, 5u);
  EXPECT_EQ(spec->stats.scoring.totalDocsLen, 40u);
  EXPECT_EQ(pendingDeletes(), 0u);

  SlotCleanup_RetryDiskJobs();
  drainCleanup();
  EXPECT_EQ(state.registrations, 1u);
  EXPECT_EQ(state.applyCalls, 1u);
  EXPECT_EQ(state.batchApplyCalls, 1u);
  EXPECT_EQ(state.batchesApplied, 1u);
  EXPECT_FALSE(state.logicalPending);
  EXPECT_FALSE(pendingTrim());
  EXPECT_EQ(spec->stats.scoring.numDocuments, 2u);
  EXPECT_EQ(spec->stats.scoring.totalDocsLen, 19u);
  EXPECT_EQ(pendingDeletes(), 3u);

  SlotCleanup_RetryDiskJobs();
  drainCleanup();
  EXPECT_EQ(state.applyCalls, 1u);
  EXPECT_EQ(pendingDeletes(), 3u);
}

TEST_F(DiskSlotTrimTest, RejectedRequeueRetainsAppliedProgressUntilCronRetry) {
  startCleanPool();
  state.batchCount = 2;
  state.maxDocId = 8;
  spec->stats.scoring.numDocuments = 8;
  spec->stats.scoring.totalDocsLen = 70;
  const auto caller = std::this_thread::get_id();
  state.beforeBatch = [caller](size_t batch) {
    EXPECT_NE(std::this_thread::get_id(), caller);
    if (batch == 0) SlotCleanup_FailNextDiskEnqueues(1);
  };
  scheduleAutomaticTrim();
  drainCleanup();

  EXPECT_EQ(state.registrations, 1u);
  EXPECT_EQ(state.applyCalls, 1u);
  EXPECT_EQ(state.batchApplyCalls, 1u);
  EXPECT_EQ(state.batchesApplied, 1u);
  EXPECT_TRUE(state.logicalPending);
  EXPECT_TRUE(pendingTrim());
  EXPECT_EQ(spec->stats.scoring.numDocuments, 5u);
  EXPECT_EQ(spec->stats.scoring.totalDocsLen, 49u);
  EXPECT_EQ(pendingDeletes(), 3u);

  SlotCleanup_RetryDiskJobs();
  drainCleanup();
  EXPECT_EQ(state.registrations, 1u);
  EXPECT_EQ(state.applyCalls, 2u);
  EXPECT_EQ(state.batchApplyCalls, 2u);
  EXPECT_EQ(state.batchesApplied, 2u);
  EXPECT_FALSE(state.logicalPending);
  EXPECT_FALSE(pendingTrim());
  EXPECT_EQ(spec->stats.scoring.numDocuments, 2u);
  EXPECT_EQ(spec->stats.scoring.totalDocsLen, 28u);
  EXPECT_EQ(pendingDeletes(), 6u);

  SlotCleanup_RetryDiskJobs();
  drainCleanup();
  EXPECT_EQ(state.applyCalls, 2u);
  EXPECT_EQ(pendingDeletes(), 6u);
}
#endif

TEST_F(DiskSlotTrimTest, StorageFailureWaitsForCronBeforeRetrying) {
  startCleanPool();
  state.readAccepted = false;
  auto *slots = static_cast<RedisModuleSlotRangeArray *>(rm_malloc(SlotRangeArray_SizeOf(1)));
  slots->num_ranges = 1;
  slots->ranges[0] = {0, 100};
  Indexes_DeleteSlots(slots);
  rm_free(slots);
  drainCleanup();
  EXPECT_EQ(state.applyCalls, 1u);
  EXPECT_TRUE(pendingTrim());
  EXPECT_TRUE(state.logicalPending);
  state.readAccepted = true;
  SlotCleanup_RetryDiskJobs();
  drainCleanup();
  EXPECT_EQ(state.applyCalls, 2u);
  EXPECT_FALSE(pendingTrim());
  EXPECT_FALSE(state.logicalPending);
}

TEST_F(DiskSlotTrimTest, PausedPoolDoesNotBeginAStorageScan) {
  startCleanPool();
  SlotCleanup_PauseDiskJobs();
  scheduleAutomaticTrim();
  SlotCleanup_RetryDiskJobs();
  EXPECT_EQ(state.applyCalls, 0u);
  EXPECT_TRUE(pendingTrim());
  SlotCleanup_ResumeDiskJobs();
  SlotCleanup_RetryDiskJobs();
  drainCleanup();
  EXPECT_EQ(state.batchesApplied, 1u);
}

TEST_F(DiskSlotTrimTest, CheckpointPauseWaitsForCurrentBatchAndLeavesQueuedWorkPaused) {
  startCleanPool();
  std::promise<void> entered, release;
  auto enteredFuture = entered.get_future();
  auto released = release.get_future();
  state.beforeBatch = [&](size_t) {
    entered.set_value();
    released.wait();
  };
  scheduleAutomaticTrim();
  ASSERT_EQ(enteredFuture.wait_for(std::chrono::seconds(10)), std::future_status::ready);
  std::atomic_bool queuedRan{false};
  ASSERT_EQ(CleanPool_AddWork([](void *arg) { static_cast<std::atomic_bool *>(arg)->store(true); },
                              &queuedRan),
            0);
  auto pause = std::async(std::launch::async, [] { SlotCleanup_PauseDiskJobs(); });
  EXPECT_EQ(pause.wait_for(std::chrono::milliseconds(100)), std::future_status::timeout);
  release.set_value();
  ASSERT_EQ(pause.wait_for(std::chrono::seconds(10)), std::future_status::ready);
  pause.get();
  EXPECT_EQ(state.batchesApplied, 1u);
  EXPECT_FALSE(state.logicalPending);
  EXPECT_FALSE(queuedRan.load());
  EXPECT_EQ(spec->stats.scoring.numDocuments, 2u);
  SlotCleanup_ResumeDiskJobs();
  drainCleanup();
  EXPECT_TRUE(queuedRan.load());
}

TEST_F(DiskSlotTrimTest, ResumeUsesBackendIntentAfterAWorkerHandleIsReleased) {
  startCleanPool();
  const SearchDiskSlotRange range{0, 100};
  job = IndexSpec_RegisterDiskSlotTrim(ref, &range, 1);
  ASSERT_NE(job, nullptr);
  IndexSpec_FreeDiskSlotTrim(job);
  job = nullptr;
  IndexSpec_ResumeDiskSlotTrims(spec);
  drainCleanup();
  EXPECT_EQ(state.registrations, 1u);
  EXPECT_EQ(state.applyCalls, 1u);
  EXPECT_FALSE(pendingTrim());
}

TEST_F(DiskSlotTrimTest, RestoreReinstallsTheSavedCutoffAndResumesWithoutRegistration) {
  startCleanPool();
  state.logicalPending = true;
  state.maxDocId = 17;
  state.ranges = {{7, 9}};
  state.beforeBatch = [&](size_t) {
    EXPECT_TRUE(pendingTrim(17, 7));
    EXPECT_FALSE(pendingTrim(18, 7));
    EXPECT_FALSE(pendingTrim(1, 10));
  };
  IndexSpec_ResumeDiskSlotTrims(spec);
  drainCleanup();
  EXPECT_EQ(state.registrations, 0u);
  EXPECT_EQ(state.applyCalls, 1u);
  EXPECT_FALSE(state.logicalPending);
  EXPECT_FALSE(pendingTrim(17, 7));
}

TEST_F(DiskSlotTrimTest, SuccessfulHotRestartKeepsCleanupPausedAfterSaveEnds) {
  mockConsistencyWindows();
  ServerEventHooks hooks;
  ASSERT_NE(state.persistenceEvent, nullptr);
  state.persistenceEvent(RSDummyContext, RedisModuleEvent_Persistence,
                         REDISMODULE_SUBEVENT_PERSISTENCE_SYNC_RDB_START, nullptr);
  EXPECT_EQ(state.windowsOpened, 1u);
  EXPECT_EQ(state.flushes, 1u);
  EXPECT_EQ(trim(), INDEX_DISK_TRIM_RETRY);
  EXPECT_EQ(state.applyCalls, 0u);

  state.persistenceEvent(RSDummyContext, RedisModuleEvent_Persistence,
                         REDISMODULE_SUBEVENT_PERSISTENCE_ENDED, nullptr);
  EXPECT_EQ(state.windowsClosed, 1u);
  EXPECT_FALSE(state.lastReopenGate);
  EXPECT_EQ(trim(), INDEX_DISK_TRIM_RETRY);
  EXPECT_EQ(state.applyCalls, 0u);
  EXPECT_TRUE(state.logicalPending);
  EXPECT_TRUE(pendingTrim());
}

TEST_F(DiskSlotTrimTest, FailedHotRestartSaveResumesCleanup) {
  mockConsistencyWindows();
  ServerEventHooks hooks;
  ASSERT_NE(state.persistenceEvent, nullptr);
  state.persistenceEvent(RSDummyContext, RedisModuleEvent_Persistence,
                         REDISMODULE_SUBEVENT_PERSISTENCE_SYNC_RDB_START, nullptr);
  EXPECT_EQ(trim(), INDEX_DISK_TRIM_RETRY);
  EXPECT_EQ(state.applyCalls, 0u);
  state.persistenceEvent(RSDummyContext, RedisModuleEvent_Persistence,
                         REDISMODULE_SUBEVENT_PERSISTENCE_FAILED, nullptr);
  EXPECT_EQ(state.windowsClosed, 1u);
  EXPECT_TRUE(state.lastReopenGate);
  EXPECT_EQ(trim(), INDEX_DISK_TRIM_FINISHED);
  EXPECT_EQ(state.applyCalls, 1u);
  EXPECT_FALSE(pendingTrim());
}

TEST_F(DiskSlotTrimTest, SuccessfulSstResumesAutomaticCleanupThroughCron) {
  mockConsistencyWindows();
  ServerEventHooks hooks;
  ASSERT_NE(state.sstEvent, nullptr);
  ASSERT_NE(state.cronEvent, nullptr);
  startCleanPool();

  state.sstEvent(RSDummyContext, RedisModuleEvent_SSTReplication,
                 REDISMODULE_SUBEVENT_SST_REPL_PRE_CHECKPOINT, nullptr);
  scheduleAutomaticTrim();
  drainCleanup();
  EXPECT_EQ(state.applyCalls, 0u);
  EXPECT_TRUE(state.logicalPending);
  EXPECT_TRUE(pendingTrim());

  for (const auto event :
       {REDISMODULE_SUBEVENT_SST_REPL_POST_CHECKPOINT, REDISMODULE_SUBEVENT_SST_REPL_PRE_FORK}) {
    state.sstEvent(RSDummyContext, RedisModuleEvent_SSTReplication, event, nullptr);
    state.cronEvent(RSDummyContext, RedisModuleEvent_CronLoop, 0, nullptr);
    drainCleanup();
    EXPECT_EQ(state.applyCalls, 0u);
    EXPECT_EQ(state.batchApplyCalls, 0u);
    EXPECT_TRUE(state.logicalPending);
    EXPECT_TRUE(pendingTrim());
  }
  EXPECT_EQ(state.windowsOpened, 1u);
  EXPECT_EQ(state.windowsClosed, 0u);

  state.sstEvent(RSDummyContext, RedisModuleEvent_SSTReplication,
                 REDISMODULE_SUBEVENT_SST_REPL_POST_FORK, nullptr);
  EXPECT_EQ(state.windowsClosed, 1u);
  EXPECT_TRUE(state.lastReopenGate);
  state.cronEvent(RSDummyContext, RedisModuleEvent_CronLoop, 0, nullptr);
  drainCleanup();
  EXPECT_EQ(state.registrations, 1u);
  EXPECT_EQ(state.applyCalls, 1u);
  EXPECT_EQ(state.batchesApplied, 1u);
  EXPECT_FALSE(state.logicalPending);
  EXPECT_FALSE(pendingTrim());
  EXPECT_EQ(spec->stats.scoring.numDocuments, 2u);
  EXPECT_EQ(spec->stats.scoring.totalDocsLen, 19u);
}

TEST_F(DiskSlotTrimTest, SstCheckpointKeepsCleanupPausedUntilAbort) {
  mockConsistencyWindows();
  ServerEventHooks hooks;
  ASSERT_NE(state.sstEvent, nullptr);
  state.sstEvent(RSDummyContext, RedisModuleEvent_SSTReplication,
                 REDISMODULE_SUBEVENT_SST_REPL_PRE_CHECKPOINT, nullptr);
  EXPECT_EQ(state.windowsOpened, 1u);
  EXPECT_EQ(trim(), INDEX_DISK_TRIM_RETRY);
  state.sstEvent(RSDummyContext, RedisModuleEvent_SSTReplication,
                 REDISMODULE_SUBEVENT_SST_REPL_POST_CHECKPOINT, nullptr);
  EXPECT_EQ(state.windowsClosed, 0u);
  EXPECT_EQ(trim(), INDEX_DISK_TRIM_RETRY);
  EXPECT_EQ(state.applyCalls, 0u);
  state.sstEvent(RSDummyContext, RedisModuleEvent_SSTReplication,
                 REDISMODULE_SUBEVENT_SST_REPL_ABORT, nullptr);
  EXPECT_EQ(state.windowsClosed, 1u);
  EXPECT_TRUE(state.lastReopenGate);
  EXPECT_EQ(trim(), INDEX_DISK_TRIM_FINISHED);
  EXPECT_EQ(state.applyCalls, 1u);
}

TEST_F(DiskSlotTrimTest, CompactAllRepliesForSuccessCancellationPauseAndIndexDrop) {
  static std::promise<void> unblocked;
  static RedisModuleCmdFunc reply;
  static void *replyData;
  static std::string replyText;
  static bool compactionAccepted, backendPaused;
  static size_t gcCalls, compactions;
  struct RestoreHooks {
    decltype(RedisModule_CreateSubcommand) create = RedisModule_CreateSubcommand;
    decltype(RedisModule_GetContextFlagsAll) flags = RedisModule_GetContextFlagsAll;
    decltype(RedisModule_BlockClient) block = RedisModule_BlockClient;
    decltype(RedisModule_UnblockClient) unblock = RedisModule_UnblockClient;
    decltype(RedisModule_GetBlockedClientPrivateData) data =
        RedisModule_GetBlockedClientPrivateData;
    decltype(RedisModule_ReplyWithSimpleString) simple = RedisModule_ReplyWithSimpleString;
    decltype(RedisModule_ReplyWithError) error = RedisModule_ReplyWithError;
    RestoreHooks() {
      RedisModule_ThreadSafeContextLock(RSDummyContext);
    }
    ~RestoreHooks() {
      GC_ThreadPoolDestroy();
      RedisModule_ThreadSafeContextUnlock(RSDummyContext);
      if (replyData) GCForcedRunOutcomeFree(RSDummyContext, replyData);
      RedisModule_CreateSubcommand = create;
      RedisModule_GetContextFlagsAll = flags;
      RedisModule_BlockClient = block;
      RedisModule_UnblockClient = unblock;
      RedisModule_GetBlockedClientPrivateData = data;
      RedisModule_ReplyWithSimpleString = simple;
      RedisModule_ReplyWithError = error;
    }
  } restore;
  gcCalls = compactions = 0;
  replyData = nullptr;
  RedisModule_GetContextFlagsAll = [] { return 0; };
  RedisModule_CreateSubcommand = [](RedisModuleCommand *, const char *name,
                                    RedisModuleCmdFunc callback, const char *, int, int, int) {
    if (strcmp(name, "GC_FORCEINVOKE") == 0) state.forceGC = callback;
    return REDISMODULE_OK;
  };
  ASSERT_EQ(RegisterDebugCommands(nullptr), REDISMODULE_OK);
  RedisModule_CreateSubcommand = restore.create;
  ASSERT_NE(state.forceGC, nullptr);
  RedisModule_BlockClient = [](RedisModuleCtx *, RedisModuleCmdFunc callback, RedisModuleCmdFunc,
                               void (*)(RedisModuleCtx *, void *), long long) {
    reply = callback;
    return reinterpret_cast<RedisModuleBlockedClient *>(&replyData);
  };
  RedisModule_UnblockClient = [](RedisModuleBlockedClient *, void *data) {
    replyData = data;
    unblocked.set_value();
    return REDISMODULE_OK;
  };
  RedisModule_GetBlockedClientPrivateData = [](RedisModuleCtx *) { return replyData; };
  RedisModule_ReplyWithSimpleString = [](RedisModuleCtx *, const char *text) {
    replyText = text;
    return REDISMODULE_OK;
  };
  RedisModule_ReplyWithError = RedisModule_ReplyWithSimpleString;
  api.index.runGC = [](RedisSearchDiskIndexSpec *, DiskGCRunStats *) {
    ++gcCalls;
    return DISK_GC_RUN_OK;
  };
  api.index.compactNonGcStorage = [](RedisSearchDiskIndexSpec *) {
    ++compactions;
    return compactionAccepted;
  };
  api.index.isBackgroundWorkPaused = [](RedisSearchDiskIndexSpec *) { return backendPaused; };
  GC_ThreadPoolStart();
  auto invoke = [&](bool dropIndex = false, bool pauseBackend = false) {
    backendPaused = false;
    GC_ThreadPoolPause();
    EXPECT_TRUE(GC_ThreadPoolWaitForPause(1000));
    unblocked = std::promise<void>{};
    auto future = unblocked.get_future();
    replyText.clear();
    RMCK::ArgvList args(RSDummyContext, "FT.DEBUG", "GC_FORCEINVOKE", "disk_slot_trim",
                        "COMPACT_ALL");
    EXPECT_EQ(state.forceGC(RSDummyContext, args.data(), args.size()), REDISMODULE_OK);
    if (dropIndex) {
      spec->gc = nullptr;
      spec->diskSpec = nullptr;
      spec->diskRegistered = false;
      Indexes_RemoveSpecFromGlobals(ref, false);
      ref = {};
      spec = nullptr;
    }
    backendPaused = pauseBackend;
    GC_ThreadPoolResume();
    const auto ready = future.wait_for(std::chrono::seconds(3));
    EXPECT_EQ(ready, std::future_status::ready);
    if (ready != std::future_status::ready) return std::string{};
    EXPECT_EQ(reply(RSDummyContext, nullptr, 0), REDISMODULE_OK);
    GCForcedRunOutcomeFree(RSDummyContext, replyData);
    replyData = nullptr;
    return replyText;
  };
  compactionAccepted = true;
  EXPECT_EQ(invoke(), "DONE");
  EXPECT_EQ(gcCalls, 1u);
  EXPECT_EQ(compactions, 1u);
  compactionAccepted = false;
  EXPECT_NE(invoke().find("COMPACT_ALL did not complete"), std::string::npos);
  EXPECT_EQ(compactions, 2u);
  EXPECT_NE(invoke(false, true).find("COMPACT_ALL did not complete"), std::string::npos);
  EXPECT_EQ(gcCalls, 2u);
  EXPECT_EQ(compactions, 2u);
  EXPECT_NE(invoke(true).find("COMPACT_ALL did not complete"), std::string::npos);
  EXPECT_EQ(gcCalls, 2u);
  EXPECT_EQ(compactions, 2u);
}

}  // namespace

TEST_F(DiskSlotTrimTest, ReleasingAWorkerHandlePreservesBackendProgress) {
  state.batchCount = 2;
  spec->stats.scoring.numDocuments = 6;
  spec->stats.scoring.totalDocsLen = 42;
  const SearchDiskSlotRange range{0, 100};
  job = IndexSpec_RegisterDiskSlotTrim(ref, &range, 1);
  ASSERT_NE(job, nullptr);
  EXPECT_EQ(IndexSpec_RunDiskSlotTrim(job), INDEX_DISK_TRIM_MORE);
  IndexSpec_FreeDiskSlotTrim(job);
  job = IndexSpec_RegisterDiskSlotTrim(ref, &range, 1);
  ASSERT_NE(job, nullptr);
  EXPECT_EQ(state.batchesApplied, 1u);
  EXPECT_EQ(IndexSpec_RunDiskSlotTrim(job), INDEX_DISK_TRIM_FINISHED);
  EXPECT_FALSE(state.logicalPending);
  EXPECT_EQ(spec->stats.scoring.numDocuments, 0u);
}

TEST_F(DiskSlotTrimTest, DuplicateEventsAndResumeScheduleOnlyOneWorker) {
  startCleanPool();
  SlotCleanup_PauseDiskJobs();
  scheduleAutomaticTrim();
  scheduleAutomaticTrim();
  IndexSpec_ResumeDiskSlotTrims(spec);
  drainCleanup();
  EXPECT_EQ(state.registrations, 2u);
  EXPECT_EQ(state.applyCalls, 0u);
  EXPECT_TRUE(pendingTrim());
  SlotCleanup_ResumeDiskJobs();
  SlotCleanup_RetryDiskJobs();
  drainCleanup();
  EXPECT_EQ(state.applyCalls, 1u);
  EXPECT_EQ(state.batchesApplied, 1u);
  EXPECT_EQ(spec->stats.scoring.numDocuments, 2u);
  EXPECT_FALSE(pendingTrim());
  SlotCleanup_RetryDiskJobs();
  drainCleanup();
  EXPECT_EQ(state.applyCalls, 1u);
}

TEST_F(DiskSlotTrimTest, RegistrationAfterFinalApplySurvivesThePreviousWorkersReturn) {
  startCleanPool();
  static decltype(api.slotMigration.runTrimBatch) run;
  static std::function<void()> afterRun;
  run = api.slotMigration.runTrimBatch;
  std::promise<void> applied, release;
  auto ready = applied.get_future();
  auto released = release.get_future();
  std::atomic_bool first{true};
  afterRun = [&] {
    if (!first.exchange(false)) return;
    applied.set_value();
    released.wait();
  };
  api.slotMigration.runTrimBatch =
      [](RedisSearchDiskIndexSpec *index, SearchDiskWriteBatchHandle *batch,
         const SearchDiskSlotRange *ranges, size_t count, t_docId cutoff, size_t budget,
         SearchDiskSlotTrimCallback callback, void *ctx) {
        auto result = run(index, batch, ranges, count, cutoff, budget, callback, ctx);
        afterRun();
        return result;
      };
  scheduleAutomaticTrim();
  EXPECT_EQ(ready.wait_for(std::chrono::seconds(10)), std::future_status::ready);

  state.batchesApplied = 0;
  state.emptyBatch = true;
  scheduleAutomaticTrim();
  release.set_value();
  drainCleanup();
  afterRun = {};
  EXPECT_EQ(state.registrations, 2u);
  EXPECT_EQ(state.applyCalls, 2u);
  EXPECT_FALSE(state.logicalPending);
  EXPECT_EQ(spec->stats.scoring.numDocuments, 2u);
}

TEST_F(DiskSlotTrimTest, SchedulerKeepsDifferentRangesAndCutoffsIndependent) {
  startCleanPool();
  SlotCleanup_PauseDiskJobs();
  static std::mutex mutex;
  static std::vector<std::vector<t_docId>> seen;
  seen.clear();
  api.slotMigration.runTrimBatch =
      [](RedisSearchDiskIndexSpec *, SearchDiskWriteBatchHandle *,
         const SearchDiskSlotRange *ranges, size_t count, t_docId cutoff, size_t,
         SearchDiskSlotTrimCallback, void *) {
        std::vector<t_docId> identity{cutoff};
        for (size_t i = 0; i < count; ++i) {
          identity.push_back(ranges[i].start);
          identity.push_back(ranges[i].end);
        }
        std::lock_guard guard(mutex);
        seen.push_back(std::move(identity));
        return SEARCH_DISK_TRIM_RETRY;
      };
  scheduleAutomaticTrim();
  auto *slots = static_cast<RedisModuleSlotRangeArray *>(rm_malloc(SlotRangeArray_SizeOf(1)));
  slots->num_ranges = 1;
  slots->ranges[0] = {7, 9};
  Indexes_DeleteSlots(slots);
  rm_free(slots);
  state.maxDocId = 6;
  scheduleAutomaticTrim();
  drainCleanup();
  SlotCleanup_ResumeDiskJobs();
  SlotCleanup_RetryDiskJobs();
  drainCleanup();
  std::sort(seen.begin(), seen.end());
  const std::vector<std::vector<t_docId>> expected{{5, 0, 100}, {5, 7, 9}, {6, 0, 100}};
  EXPECT_EQ(seen, expected);
}

TEST_F(DiskSlotTrimTest, PendingCompletionAfterShutdownDoesNotAccessStorage) {
  startCleanPool();
  scheduleAutomaticTrim();
  CleanPool_Wait();
  // Leave the completion on the event loop while storage is closed.
  SlotCleanup_StopDiskJobs();
  const auto calls = state.applyCalls;
  spec->diskSpec = nullptr;
  drainCleanup();
  SlotCleanup_RetryDiskJobs();
  EXPECT_EQ(state.applyCalls, calls);
}

TEST_F(DiskSlotTrimTest, DiskShutdownDoesNotWaitOnRamCleanupJobs) {
  startCleanPool();
  std::promise<void> entered, release;
  auto started = entered.get_future();
  auto released = release.get_future();
  struct Work {
    std::promise<void> *entered;
    std::future<void> *release;
  } work{&entered, &released};
  ASSERT_EQ(CleanPool_AddWork(
                [](void *arg) {
                  auto *work = static_cast<Work *>(arg);
                  work->entered->set_value();
                  work->release->wait();
                },
                &work),
            0);
  ASSERT_EQ(started.wait_for(std::chrono::seconds(10)), std::future_status::ready);
  isFlex = false;
  auto stop = std::async(std::launch::async, [] { SlotCleanup_StopDiskJobs(); });
  EXPECT_EQ(stop.wait_for(std::chrono::seconds(1)), std::future_status::ready);
  release.set_value();
  stop.get();
  isFlex = true;
  drainCleanup();
}

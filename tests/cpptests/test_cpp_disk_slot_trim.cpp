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
  bool registerAccepted = true, readAccepted = true;
  bool logicalPending = false, emptyBatch = false;
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
  };
  GCForcedRun forced = GCForcedRun_New(1000, 10);
  EXPECT_TRUE(gc.callbacks.periodicCallback(gc.gcCtx, &forced));
  EXPECT_EQ(IndexesGlobalStats_GetLogicallyDeletedDocs(), logicallyDeleted + 4);
  dropIndexBeforeCollectorTermination();
  EXPECT_EQ(IndexesGlobalStats_GetLogicallyDeletedDocs(), logicallyDeleted + 4);
  IndexsGlobalStats_DecreaseLogicallyDeleted(4);
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

TEST_F(DiskSlotTrimTest, RegistrationDeduplicatesOnlyMatchingRangesAndCutoff) {
  const SearchDiskSlotRange range{7, 9};
  state.maxDocId = 17;
  job = IndexSpec_RegisterDiskSlotTrim(ref, &range, 1);
  ASSERT_NE(job, nullptr);
  PendingSlotDeletion *first = spec->docs.pendingSlotDeletions;
  EXPECT_EQ(IndexSpec_RegisterDiskSlotTrim(ref, &range, 1), nullptr);
  EXPECT_EQ(spec->docs.pendingSlotDeletions, first);

  const SearchDiskSlotRange shifted{8, 10};
  IndexSpecSlotTrimJob *other = IndexSpec_RegisterDiskSlotTrim(ref, &shifted, 1);
  ASSERT_NE(other, nullptr);
  EXPECT_NE(spec->docs.pendingSlotDeletions, first);
  IndexSpec_FreeDiskSlotTrim(other);

  const SearchDiskSlotRange split[]{{7, 7}, {9, 9}};
  other = IndexSpec_RegisterDiskSlotTrim(ref, split, 2);
  ASSERT_NE(other, nullptr);
  IndexSpec_FreeDiskSlotTrim(other);

  state.maxDocId = 23;
  IndexSpecSlotTrimJob *newer = IndexSpec_RegisterDiskSlotTrim(ref, &range, 1);
  ASSERT_NE(newer, nullptr);
  EXPECT_TRUE(pendingFilter(23, 7));
  EXPECT_FALSE(pendingFilter(24, 7));
  IndexSpec_FreeDiskSlotTrim(newer);
}

TEST_F(DiskSlotTrimTest, StoppedRegistrationDoesNotPublishWork) {
  const SearchDiskSlotRange range{7, 9};
  SlotCleanup_StopDiskJobs();
  EXPECT_EQ(IndexSpec_RegisterDiskSlotTrim(ref, &range, 1), nullptr);
  EXPECT_EQ(state.registrations, 0u);
  EXPECT_EQ(spec->docs.pendingSlotDeletions, nullptr);
}

TEST_F(DiskSlotTrimTest, RestoredTrimEnumerationReinstallsRangesAndOriginalCutoff) {
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
        auto *slots = static_cast<RedisModuleSlotRangeArray *>(rm_malloc(SlotRangeArray_SizeOf(count)));
        slots->num_ranges = count;
        for (size_t i = 0; i < count; ++i) slots->ranges[i] = {ranges[i].start, ranges[i].end};
        DocTable_AddPendingDeletionWithCutoff(&restore.spec->docs, slots, cutoff);
        rm_free(slots);
      },
      &restore);
  IndexSpec_Unlock(spec);

  EXPECT_EQ(restore.calls, 1u);
  for (uint16_t slot : {7, 9, 12, 14}) {
    EXPECT_TRUE(pendingFilter(17, slot));
    EXPECT_FALSE(pendingFilter(18, slot));
  }
  for (uint16_t slot : {6, 10, 11, 15}) EXPECT_FALSE(pendingFilter(1, slot));
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

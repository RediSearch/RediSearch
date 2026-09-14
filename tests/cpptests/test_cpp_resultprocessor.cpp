/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

extern "C" {
#include "byte_offsets.h"
}

#include "result_processor.h"
#include "query_request.h"
#include "common.h"
#include "query.h"
#include "value_ffi.h"
#include "gtest/gtest.h"
#include "search_result_ffi.h"
#include "search_result.h"
#include "spec.h"
#include "redismock/util.h"
#include "query_flags.h"
#include "metrics_ffi.h"
#include "debug_commands.h"
#include "search_options.h"
#include "query_term_ffi.h"

#include <atomic>
#include <thread>
#include <vector>

// Upstream owns a separate drain cursor; Next must never consume it.
struct LoaderDrainSource : ResultProcessor {
  std::vector<RSDocumentMetadata *> documents;
  size_t position = 0;
  size_t nextCalls = 0;
  RPDrainStatus terminal = RP_DRAIN_EOF;

  LoaderDrainSource() {
    *static_cast<ResultProcessor *>(this) = {};
    Drain = [](ResultProcessor *base, SearchResult *result) {
      auto *self = static_cast<LoaderDrainSource *>(base);
      if (self->position == self->documents.size()) {
        RPDrainStatus status = self->terminal;
        self->terminal = RP_DRAIN_EOF;
        return status;
      }
      auto *dmd = self->documents[self->position++];
      DMD_Incref(dmd);
      SearchResult_SetDocumentMetadata(result, dmd);
      SearchResult_SetDocId(result, dmd->id);
      return RP_DRAIN_OK;
    };
    Next = [](ResultProcessor *base, SearchResult *) -> int {
      static_cast<LoaderDrainSource *>(base)->nextCalls++;
      return RS_RESULT_TIMEDOUT;
    };
  }
};

class LoaderDrainTest : public ::testing::Test {
 protected:
  RMCK::Context ctx;
  IndexSpec spec = {};
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(ctx, &spec);
  QueryProcessingCtx qctx = {};
  LoaderDrainSource source;
  RLookup lookup = RLookup_New();
  ResultProcessor *loader = nullptr;
  SearchResult result = SearchResult_New();

  void SetUp() override {
    spec.docs = DocTable_New(1);
    qctx.totalResults = 100;
    qctx.skippedResults = 7;
  }

  void TearDown() override {
    SearchResult_Destroy(&result);
    if (loader) loader->Free(loader);
    RLookup_Cleanup(&lookup);
    DocTable_Free(&spec.docs);
    RMCK::flushdb(ctx);
  }

  RSDocumentMetadata *document(const char *name, const char *value) {
    auto *dmd = DocTable_Put(&spec.docs, name, strlen(name), 1, Document_DefaultFlags, nullptr, 0,
                             DocumentType_Hash);
    DMD_Return(dmd);
    if (value) EXPECT_TRUE(RMCK::hset(ctx, name, "field", value));
    return dmd;
  }

  const RLookupKey *create(bool all = false, uint32_t flags = 0, bool force = false,
                           bool cached = false) {
    RLookupKey *mutableKey = all ? nullptr : RLookup_GetKey_Load(&lookup, "alias", "field", 0);
    if (cached) mutableKey->flags |= RLOOKUP_F_VALAVAILABLE;
    const RLookupKey *key = mutableKey;
    uint32_t state = 0;
    loader = RPLoader_New(&sctx, flags, &lookup, all ? nullptr : &key, all ? 0 : 1, force, &state);
    loader->parent = &qctx;
    loader->upstream = &source;
    qctx.endProc = loader;
    RLookup_Seal(&lookup);
    return key;
  }

  void expectValue(const RLookupKey *key, const char *expected) {
    const RSValue *value = RLookupRow_Get(key, SearchResult_GetRowData(&result));
    ASSERT_NE(nullptr, value);
    size_t length = 0;
    const char *data = RSValue_StringPtrLen(value, &length);
    ASSERT_NE(nullptr, data);
    EXPECT_EQ(std::string(expected), std::string(data, length));
  }
};

class CrashDrainTest : public LoaderDrainTest, public ::testing::WithParamInterface<CrashLocation> {
 protected:
  void SetUp() override {
    LoaderDrainTest::SetUp();
    loader = RPCrash_New(GetParam());
    loader->upstream = &source;
  }
};

INSTANTIATE_TEST_SUITE_P(CrashLocations, CrashDrainTest,
                         ::testing::Values(CRASH_IN_C, CRASH_IN_RUST));

TEST_P(CrashDrainTest, forwardsOwnedResultsWithoutInjectingCrash) {
  auto *dmd = document("crash:drain", nullptr);
  source.documents = {dmd};
  ASSERT_EQ(RP_DRAIN_OK, loader->Drain(loader, &result));
  EXPECT_EQ(dmd, SearchResult_GetDocumentMetadata(&result));
  EXPECT_EQ(dmd->id, SearchResult_GetDocId(&result));
  EXPECT_EQ(2, dmd->ref_count);
  EXPECT_EQ(0, source.nextCalls);
  loader->Free(loader);
  loader = nullptr;
  EXPECT_EQ(dmd, SearchResult_GetDocumentMetadata(&result));
  SearchResult_Clear(&result);
  EXPECT_EQ(1, dmd->ref_count);
}

TEST_P(CrashDrainTest, forwardsErrorAndEofWithoutTouchingOutput) {
  SearchResult_SetScore(&result, 17);
  source.terminal = RP_DRAIN_ERROR;
  EXPECT_EQ(RP_DRAIN_ERROR, loader->Drain(loader, &result));
  EXPECT_EQ(RP_DRAIN_EOF, loader->Drain(loader, &result));
  EXPECT_EQ(RP_DRAIN_EOF, loader->Drain(loader, &result));
  EXPECT_EQ(17, SearchResult_GetScore(&result));
  EXPECT_EQ(0, source.nextCalls);
}

TEST_F(LoaderDrainTest, returnDrainsExplicitFieldsAfterNextUnwinds) {
  auto *first = document("drain:1", "one");
  source.documents = {first, document("drain:2", "two")};
  const auto *key = create(false, QEXEC_F_PROFILE);
  ASSERT_EQ(RS_RESULT_TIMEDOUT, loader->Next(loader, &result));
  ASSERT_EQ(RP_DRAIN_OK, loader->Drain(loader, &result));
  expectValue(key, "one");
  EXPECT_EQ(2, first->ref_count);
  SearchResult_Clear(&result);
  EXPECT_EQ(1, first->ref_count);
  ASSERT_EQ(RP_DRAIN_OK, loader->Drain(loader, &result));
  expectValue(key, "two");
  EXPECT_EQ(1, source.nextCalls);
  EXPECT_EQ(100, qctx.totalResults);
  EXPECT_EQ(7, qctx.skippedResults);
  SearchResult_Clear(&result);
  EXPECT_EQ(RP_DRAIN_EOF, loader->Drain(loader, &result));
  EXPECT_EQ(RP_DRAIN_EOF, loader->Drain(loader, &result));
}

TEST_F(LoaderDrainTest, loadAllAppendsToSealedLookup) {
  source.documents = {document("drain:all", "all")};
  create(true);
  // Supply one scan batch to exercise runtime key creation independently of the mock cursor.
  auto scanKey = RedisModule_ScanKey;
  RedisModule_ScanKey = [](RedisModuleKey *key, RedisModuleScanCursor *,
                           RedisModuleScanKeyCB callback, void *data) {
    RMCK::RString field("field"), value("all");
    callback(key, field, value, data);
    return 0;
  };
  const auto status = loader->Drain(loader, &result);
  RedisModule_ScanKey = scanKey;
  ASSERT_EQ(RP_DRAIN_OK, status);
  const auto *key = RLookup_GetKey_Read(&lookup, "field", 0);
  ASSERT_NE(nullptr, key);
  expectValue(key, "all");
}

TEST_F(LoaderDrainTest, expiredRowsUseNormalSkippedResultAccounting) {
  auto *missing = document("drain:missing", nullptr);
  auto *deleted = document("drain:deleted", "deleted");
  deleted->flags = static_cast<RSDocumentFlags>(deleted->flags | Document_Deleted);
  source.documents = {missing, deleted, document("drain:live", "live")};
  const auto *key = create();
  ASSERT_EQ(RP_DRAIN_OK, loader->Drain(loader, &result));
  expectValue(key, "live");
  EXPECT_EQ(9, qctx.skippedResults);
  EXPECT_EQ(100, qctx.totalResults);
  EXPECT_TRUE(missing->flags & Document_FailedToOpen);
  EXPECT_EQ(1, missing->ref_count);
  EXPECT_EQ(1, deleted->ref_count);
}

TEST_F(LoaderDrainTest, forwardsErrorAndEofWithoutCallingNext) {
  create();
  source.terminal = RP_DRAIN_ERROR;
  EXPECT_EQ(RP_DRAIN_ERROR, loader->Drain(loader, &result));
  EXPECT_EQ(RP_DRAIN_EOF, loader->Drain(loader, &result));
  EXPECT_EQ(0, source.nextCalls);
  EXPECT_EQ(nullptr, SearchResult_GetDocumentMetadata(&result));
}

TEST_F(LoaderDrainTest, preservesPreviouslyLoadedValues) {
  source.documents = {document("drain:cached", "document")};
  const auto *key = create(false, 0, false, true);
  RLookup_WriteOwnKey(key, SearchResult_GetRowDataMut(&result),
                      RSValue_NewCopiedString("cached", 6));
  ASSERT_EQ(RP_DRAIN_OK, loader->Drain(loader, &result));
  expectValue(key, "cached");
}

TEST_F(LoaderDrainTest, forceLoadReplacesPreviouslyLoadedValues) {
  source.documents = {document("drain:forced", "document")};
  const auto *key = create(false, 0, true, true);
  RLookup_WriteOwnKey(key, SearchResult_GetRowDataMut(&result),
                      RSValue_NewCopiedString("cached", 6));
  ASSERT_EQ(RP_DRAIN_OK, loader->Drain(loader, &result));
  expectValue(key, "document");
}

TEST_F(LoaderDrainTest, promotionToSafeLoaderUsesSafeDrain) {
  source.documents = {document("drain:safe", "safe")};
  create();
  SetLoadersForBG(&qctx);
  loader = qctx.endProc;
  EXPECT_EQ(RP_SAFE_LOADER, loader->type);
  RedisModule_ThreadSafeContextLock(ctx);
  const auto status = loader->Drain(loader, &result);
  RedisModule_ThreadSafeContextUnlock(ctx);
  ASSERT_EQ(RP_DRAIN_EOF, status);
  EXPECT_EQ(0, source.position);
}

TEST_F(LoaderDrainTest, constructedSafeLoaderUsesSafeDrain) {
  source.documents = {document("drain:safe", "safe")};
  create(false, QEXEC_F_RUN_IN_BACKGROUND);
  EXPECT_EQ(RP_SAFE_LOADER, loader->type);
  RedisModule_ThreadSafeContextLock(ctx);
  const auto status = loader->Drain(loader, &result);
  RedisModule_ThreadSafeContextUnlock(ctx);
  ASSERT_EQ(RP_DRAIN_EOF, status);
  EXPECT_EQ(0, source.position);
}

// Next and Drain deliberately have independent inputs; a parked Next owns its late row.
struct SafeLoaderSource : LoaderDrainSource {
  std::vector<RSDocumentMetadata *> nextDocuments;
  size_t nextPosition = 0;
  size_t pauseAt = SIZE_MAX;
  std::atomic<bool> entered{false}, release{false};
  int nextTerminal = RS_RESULT_EOF;
  RSDocumentMetadata *terminalDocument = nullptr;
  const RLookupKey *terminalKey = nullptr;
  RSValue *terminalValue = nullptr;

  SafeLoaderSource() {
    Next = [](ResultProcessor *base, SearchResult *result) -> int {
      auto *self = static_cast<SafeLoaderSource *>(base);
      if (self->nextPosition == self->pauseAt) {
        self->entered.store(true, std::memory_order_release);
        while (!self->release.load(std::memory_order_acquire)) {
          std::this_thread::yield();
        }
      }
      if (self->nextPosition == self->nextDocuments.size()) {
        if (self->terminalDocument) {
          DMD_Incref(self->terminalDocument);
          SearchResult_SetDocumentMetadata(result, self->terminalDocument);
          RSValue_IncrRef(self->terminalValue);
          RLookup_WriteOwnKey(self->terminalKey, SearchResult_GetRowDataMut(result),
                              self->terminalValue);
          SearchResult_SetOwnedIndexResult(result, NewVirtualResult(1, RS_FIELDMASK_ALL));
        }
        return self->nextTerminal;
      }
      auto *dmd = self->nextDocuments[self->nextPosition++];
      DMD_Incref(dmd);
      SearchResult_SetDocumentMetadata(result, dmd);
      SearchResult_SetDocId(result, dmd->id);
      return RS_RESULT_OK;
    };
  }
};

class SafeLoaderDrainTest : public LoaderDrainTest {
 protected:
  SafeLoaderSource safeSource;
  QueryRequestTimeout timeout = {};

  void SetUp() override {
    LoaderDrainTest::SetUp();
    QueryRequestTimeout_Init(&timeout, TimeoutPolicy_ReturnStrict, 1000);
    QueryRequestTimeout_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
    sctx.timeout = &timeout;
    qctx.timeoutPolicy = TimeoutPolicy_ReturnStrict;
    qctx.resultLimit = 4096;
  }

  const RLookupKey *createSafe(bool all = false, bool profile = false) {
    const auto *key = create(all, QEXEC_F_RUN_IN_BACKGROUND | (profile ? QEXEC_F_PROFILE : 0));
    loader->upstream = &safeSource;
    return key;
  }
};

TEST_F(SafeLoaderDrainTest, releasesTerminalScratchAfterDrainClosesPublication) {
  auto *buffered = document("safe:buffered", "buffered");
  auto *scratch = document("safe:scratch", "scratch");
  safeSource.nextDocuments = {buffered};
  safeSource.pauseAt = 1;
  safeSource.nextTerminal = RS_RESULT_ERROR;
  safeSource.terminalDocument = scratch;
  safeSource.terminalKey = createSafe();
  auto *value = RSValue_NewCopiedString("scratch", 7);
  safeSource.terminalValue = value;
  SearchResult next = SearchResult_New();
  int status = RS_RESULT_MAX;
  RedisModule_ThreadSafeContextLock(ctx);
  std::thread worker([&] { status = loader->Next(loader, &next); });
  bool entered = RS::WaitForCondition([&] { return safeSource.entered.load(); }, 5);
  QueryRequestTimeout_MarkTimedOut(&timeout);
  if (entered) {
    EXPECT_EQ(RP_DRAIN_EOF, loader->Drain(loader, &result));
    EXPECT_EQ(nullptr, SearchResult_GetDocumentMetadata(&result));
    EXPECT_EQ(RP_DRAIN_EOF, loader->Drain(loader, &result));
  }
  safeSource.release.store(true, std::memory_order_release);
  RedisModule_ThreadSafeContextUnlock(ctx);
  worker.join();
  EXPECT_TRUE(entered);
  EXPECT_EQ(RS_RESULT_TIMEDOUT, status);
  EXPECT_EQ(1, scratch->ref_count);
  EXPECT_EQ(1, RSValue_Refcount(value));
  EXPECT_EQ(nullptr, SearchResult_GetDocumentMetadata(&next));
  loader->Free(loader);
  loader = nullptr;
  EXPECT_EQ(1, scratch->ref_count);
  EXPECT_EQ(1, buffered->ref_count);
  EXPECT_EQ(1, RSValue_Refcount(value));
  RSValue_DecrRef(value);
  SearchResult_Destroy(&next);
}

TEST_F(SafeLoaderDrainTest, leavesUnloadedAndLateRowsWorkerOwned) {
  auto *first = document("safe:1", "one");
  auto *late = document("safe:late", "late");
  safeSource.nextDocuments = {first, late};
  safeSource.pauseAt = 1;
  safeSource.documents = {document("safe:drain", "drained")};
  createSafe(false, true);
  SearchResult next = SearchResult_New();
  int nextStatus = RS_RESULT_MAX;
  RedisModule_ThreadSafeContextLock(ctx);
  std::thread worker([&] { nextStatus = loader->Next(loader, &next); });
  bool entered = RS::WaitForCondition([&] { return safeSource.entered.load(); }, 5);
  QueryRequestTimeout_MarkTimedOut(&timeout);
  if (entered) {
    EXPECT_EQ(RP_DRAIN_EOF, loader->Drain(loader, &result));
    EXPECT_EQ(nullptr, SearchResult_GetDocumentMetadata(&result));
    EXPECT_EQ(RP_DRAIN_EOF, loader->Drain(loader, &result));
    EXPECT_EQ(RP_DRAIN_EOF, loader->Drain(loader, &result));
    EXPECT_FALSE(safeSource.release.load());
  }
  safeSource.release.store(true, std::memory_order_release);
  RedisModule_ThreadSafeContextUnlock(ctx);
  worker.join();
  ASSERT_TRUE(entered);
  EXPECT_EQ(RS_RESULT_TIMEDOUT, nextStatus);
  EXPECT_EQ(nullptr, SearchResult_GetDocumentMetadata(&next));
  EXPECT_EQ(0, safeSource.position);
  // Free, not Drain, owns cleanup of the unpublished batch after the worker finishes.
  loader->Free(loader);
  loader = nullptr;
  EXPECT_EQ(1, first->ref_count);
  EXPECT_EQ(1, late->ref_count);
  EXPECT_EQ(4096, qctx.resultLimit);
  EXPECT_EQ(7, qctx.skippedResults);
  SearchResult_Destroy(&next);
}

TEST_F(SafeLoaderDrainTest, drainsBeforeWorkerAcquiresGil) {
  safeSource.nextDocuments = {document("safe:gil", "buffered")};
  createSafe();
  // Interpose only to observe entry. The real mock mutex keeps BG parked until Drain completes.
  static std::atomic<bool> waiting;
  static decltype(RedisModule_ThreadSafeContextLock) originalLock;
  waiting.store(false);
  originalLock = RedisModule_ThreadSafeContextLock;
  originalLock(ctx);
  RedisModule_ThreadSafeContextLock = [](RedisModuleCtx *ctx) {
    waiting.store(true, std::memory_order_release);
    originalLock(ctx);
  };
  SearchResult next = SearchResult_New();
  int status = RS_RESULT_MAX;
  std::thread worker([&] { status = loader->Next(loader, &next); });
  bool entered = RS::WaitForCondition([&] { return waiting.load(std::memory_order_acquire); }, 5);
  QueryRequestTimeout_MarkTimedOut(&timeout);
  if (entered) {
    EXPECT_EQ(RP_DRAIN_EOF, loader->Drain(loader, &result));
    EXPECT_EQ(nullptr, SearchResult_GetDocumentMetadata(&result));
    EXPECT_EQ(RP_DRAIN_EOF, loader->Drain(loader, &result));
  }
  RedisModule_ThreadSafeContextUnlock(ctx);
  worker.join();
  RedisModule_ThreadSafeContextLock = originalLock;
  EXPECT_TRUE(entered);
  EXPECT_EQ(RS_RESULT_TIMEDOUT, status);
  SearchResult_Destroy(&next);
}

TEST_F(SafeLoaderDrainTest, yieldsLoadedRowsWithoutReloadingAndPreservesNextOwnedRow) {
  auto *first = document("safe:loaded:1", "one");
  safeSource.nextDocuments = {document("safe:loaded:missing", nullptr), first,
                              document("safe:loaded:2", "two")};
  const auto *key = createSafe(false, true);
  SearchResult next = SearchResult_New();
  ASSERT_EQ(RS_RESULT_OK, loader->Next(loader, &next));
  EXPECT_TRUE(RMCK::hset(ctx, "safe:loaded:2", "field", "changed"));
  RedisModule_ThreadSafeContextLock(ctx);
  QueryRequestTimeout_MarkTimedOut(&timeout);
  EXPECT_EQ(RP_DRAIN_OK, loader->Drain(loader, &result));
  expectValue(key, "two");
  EXPECT_EQ(first, SearchResult_GetDocumentMetadata(&next));
  SearchResult_Clear(&result);
  EXPECT_EQ(RP_DRAIN_EOF, loader->Drain(loader, &result));
  EXPECT_EQ(8, qctx.skippedResults);
  RedisModule_ThreadSafeContextUnlock(ctx);
  SearchResult_Destroy(&next);
}

TEST_F(SafeLoaderDrainTest, takesPublishedBatchBeforeFirstNextYield) {
  safeSource.nextDocuments = {document("safe:published", "ready")};
  safeSource.documents = {document("safe:upstream", "must not drain")};
  const auto *key = createSafe();
  // Park after the actual GIL release, before Next can claim any loaded row.
  static std::atomic<bool> published, resume;
  static decltype(RedisModule_ThreadSafeContextUnlock) originalUnlock;
  published.store(false);
  resume.store(false);
  originalUnlock = RedisModule_ThreadSafeContextUnlock;
  RedisModule_ThreadSafeContextUnlock = [](RedisModuleCtx *ctx) {
    originalUnlock(ctx);
    published.store(true, std::memory_order_release);
    while (!resume.load(std::memory_order_acquire)) std::this_thread::yield();
  };
  SearchResult next = SearchResult_New();
  int status = RS_RESULT_MAX;
  std::thread worker([&] { status = loader->Next(loader, &next); });
  bool entered = RS::WaitForCondition([&] { return published.load(); }, 5);
  RedisModule_ThreadSafeContextLock(ctx);
  QueryRequestTimeout_MarkTimedOut(&timeout);
  if (entered) {
    EXPECT_EQ(RP_DRAIN_OK, loader->Drain(loader, &result));
    expectValue(key, "ready");
    SearchResult_Clear(&result);
    EXPECT_EQ(RP_DRAIN_EOF, loader->Drain(loader, &result));
    EXPECT_EQ(0, safeSource.position);
  }
  originalUnlock(ctx);
  resume.store(true, std::memory_order_release);
  worker.join();
  RedisModule_ThreadSafeContextUnlock = originalUnlock;
  EXPECT_TRUE(entered);
  EXPECT_EQ(RS_RESULT_TIMEDOUT, status);
  EXPECT_EQ(nullptr, SearchResult_GetDocumentMetadata(&next));
  SearchResult_Destroy(&next);
}

TEST_F(SafeLoaderDrainTest, resetMakesNextBatchPrivateBeforeUpstreamCall) {
  safeSource.nextDocuments = {document("safe:old", "old"), document("safe:new", "new")};
  createSafe();
  qctx.resultLimit = 1;
  ASSERT_EQ(RS_RESULT_OK, loader->Next(loader, &result));
  SearchResult_Clear(&result);
  safeSource.pauseAt = 1;
  SearchResult next = SearchResult_New();
  int status = RS_RESULT_MAX;
  RedisModule_ThreadSafeContextLock(ctx);
  std::thread worker([&] { status = loader->Next(loader, &next); });
  bool entered = RS::WaitForCondition([&] { return safeSource.entered.load(); }, 5);
  QueryRequestTimeout_MarkTimedOut(&timeout);
  if (entered) EXPECT_EQ(RP_DRAIN_EOF, loader->Drain(loader, &result));
  safeSource.release.store(true, std::memory_order_release);
  RedisModule_ThreadSafeContextUnlock(ctx);
  worker.join();
  EXPECT_TRUE(entered);
  EXPECT_EQ(RS_RESULT_TIMEDOUT, status);
  EXPECT_EQ(nullptr, SearchResult_GetDocumentMetadata(&next));
  SearchResult_Destroy(&next);
}

TEST_F(SafeLoaderDrainTest, unstartedDrainDoesNotLoadOrReadUpstreamErrors) {
  auto *missing = document("safe:missing", nullptr);
  safeSource.documents = {missing, document("safe:live", "live")};
  safeSource.terminal = RP_DRAIN_ERROR;
  createSafe();
  RedisModule_ThreadSafeContextLock(ctx);
  QueryRequestTimeout_MarkTimedOut(&timeout);
  EXPECT_EQ(RP_DRAIN_EOF, loader->Drain(loader, &result));
  EXPECT_EQ(RP_DRAIN_EOF, loader->Drain(loader, &result));
  RedisModule_ThreadSafeContextUnlock(ctx);
  EXPECT_FALSE(missing->flags & Document_FailedToOpen);
  EXPECT_EQ(7, qctx.skippedResults);
  EXPECT_EQ(100, qctx.totalResults);
  EXPECT_EQ(0, safeSource.position);
  EXPECT_EQ(nullptr, SearchResult_GetDocumentMetadata(&result));
}

TEST_F(SafeLoaderDrainTest, reusesBlocksAcrossChunksThenFreesUndrainedRows) {
  auto *dmd = document("safe:blocks", "value");
  safeSource.nextDocuments.assign(2200, dmd);
  createSafe();
  qctx.resultLimit = 1100;
  for (size_t i = 0; i < 1101; ++i) {
    ASSERT_EQ(RS_RESULT_OK, loader->Next(loader, &result));
    SearchResult_Clear(&result);
  }
  RedisModule_ThreadSafeContextLock(ctx);
  QueryRequestTimeout_MarkTimedOut(&timeout);
  for (size_t i = 0; i < 1024; ++i) {
    EXPECT_EQ(RP_DRAIN_OK, loader->Drain(loader, &result));
    SearchResult_Clear(&result);
  }
  RedisModule_ThreadSafeContextUnlock(ctx);
  loader->Free(loader);
  loader = nullptr;
  EXPECT_EQ(1, dmd->ref_count);
}

TEST_F(SafeLoaderDrainTest, loadAllDoesNotPublishKeysFromUnloadedBuffer) {
  safeSource.nextDocuments = {document("safe:all", "all")};
  safeSource.pauseAt = 1;
  createSafe(true);
  auto scanKey = RedisModule_ScanKey;
  RedisModule_ScanKey = [](RedisModuleKey *key, RedisModuleScanCursor *,
                           RedisModuleScanKeyCB callback, void *data) {
    RMCK::RString field("field"), value("all");
    callback(key, field, value, data);
    return 0;
  };
  SearchResult next = SearchResult_New();
  int status = RS_RESULT_MAX;
  RedisModule_ThreadSafeContextLock(ctx);
  std::thread worker([&] { status = loader->Next(loader, &next); });
  bool entered = RS::WaitForCondition([&] { return safeSource.entered.load(); }, 5);
  QueryRequestTimeout_MarkTimedOut(&timeout);
  if (entered) {
    EXPECT_EQ(RP_DRAIN_EOF, loader->Drain(loader, &result));
    auto iterator = RLookup_Iter(&lookup);
    const RLookupKey *key = nullptr;
    EXPECT_FALSE(RLookupIterator_Next(&iterator, &key));
    EXPECT_EQ(RP_DRAIN_EOF, loader->Drain(loader, &result));
  }
  safeSource.release.store(true, std::memory_order_release);
  RedisModule_ThreadSafeContextUnlock(ctx);
  worker.join();
  RedisModule_ScanKey = scanKey;
  EXPECT_TRUE(entered);
  EXPECT_EQ(RS_RESULT_TIMEDOUT, status);
  SearchResult_Destroy(&next);
}

// A worker-only source with a reusable borrowed index result and an observable Drain barrier.
static thread_local bool pauseSorterAllocation = false;
struct SorterDrainSource : ResultProcessor {
  bool explanations = false;
  std::vector<double> scores;
  std::vector<RSValue *> values;
  const RLookupKey *key = nullptr;
  size_t position = 0, pauseAt = SIZE_MAX, allocationPauseAt = SIZE_MAX;
  size_t drainCalls = 0;
  std::atomic<bool> entered{false}, release{false};
  int terminal = RS_RESULT_EOF;
  RSIndexResult *borrowed = NewVirtualResult(1, RS_FIELDMASK_ALL);

  SorterDrainSource() {
    *static_cast<ResultProcessor *>(this) = {};
    Drain = [](ResultProcessor *base, SearchResult *) {
      ++static_cast<SorterDrainSource *>(base)->drainCalls;
      return RP_DRAIN_ERROR;
    };
    Next = [](ResultProcessor *base, SearchResult *result) -> int {
      auto *self = static_cast<SorterDrainSource *>(base);
      if (self->position == self->pauseAt) {
        self->entered.store(true, std::memory_order_release);
        while (!self->release.load(std::memory_order_acquire)) std::this_thread::yield();
      }
      if (self->position == self->scores.size()) return self->terminal;
      pauseSorterAllocation = self->position == self->allocationPauseAt;
      SearchResult_SetScore(result, self->scores[self->position++]);
      SearchResult_SetDocId(result, self->position);
      if (self->explanations) {
        SearchResult_SetScoreExplain(
            result, static_cast<RSScoreExplain *>(rm_calloc(1, sizeof(RSScoreExplain))));
      }
      if (self->key && self->values[self->position - 1]) {
        auto *value = self->values[self->position - 1];
        RSValue_IncrRef(value);
        RLookup_WriteOwnKey(self->key, SearchResult_GetRowDataMut(result), value);
      }
      self->borrowed->docId = self->position;
      SearchResult_SetBorrowedIndexResult(result, self->borrowed);
      return RS_RESULT_OK;
    };
  }
  ~SorterDrainSource() {
    IndexResult_Free(borrowed);
    for (auto *value : values)
      if (value) RSValue_DecrRef(value);
  }
};

class MaxScoreDrainTest : public ::testing::Test {
 protected:
  QueryProcessingCtx qctx = {};
  RLookup lookup = RLookup_New();
  const RLookupKey *key = RLookup_GetKey_Write(&lookup, "score", RLOOKUP_F_NOFLAGS);
  SorterDrainSource source;
  ResultProcessor *normalizer = nullptr;
  SearchResult result = SearchResult_New();

  void SetUp() override {
    RLookup_Seal(&lookup);
    qctx.timeoutPolicy = TimeoutPolicy_ReturnStrict;
    qctx.resultLimit = 37;
    normalizer = RPMaxScoreNormalizer_New(key);
    normalizer->parent = &qctx;
    normalizer->upstream = &source;
  }
  void TearDown() override {
    SearchResult_Destroy(&result);
    if (normalizer) normalizer->Free(normalizer);
    RLookup_Cleanup(&lookup);
  }
  std::vector<double> drain() {
    std::vector<double> scores;
    RPDrainStatus status;
    while ((status = normalizer->Drain(normalizer, &result)) == RP_DRAIN_OK) {
      double score = SearchResult_GetScore(&result);
      scores.push_back(score);
      EXPECT_DOUBLE_EQ(score,
                       RSValue_Number_Get(RLookupRow_Get(key, SearchResult_GetRowData(&result))));
      EXPECT_TRUE(SearchResult_GetFlags(&result) & Result_OwnsIndexResult);
      EXPECT_EQ(SearchResult_GetDocId(&result), SearchResult_GetIndexResult(&result)->docId);
      SearchResult_Clear(&result);
    }
    EXPECT_EQ(RP_DRAIN_EOF, status);
    EXPECT_EQ(0, source.drainCalls);
    return scores;
  }
};

TEST_F(MaxScoreDrainTest, unstartedDrainDoesNotPullUpstream) {
  source.scores = {10};
  EXPECT_TRUE(drain().empty());
  EXPECT_EQ(RS_RESULT_TIMEDOUT, normalizer->Next(normalizer, &result));
  EXPECT_EQ(0, source.position);
}

TEST_F(MaxScoreDrainTest, partialPoolUsesCommittedMaximumAndRejectsLateHigherScore) {
  source.scores = {2, 8, 4, 100};
  source.pauseAt = 3;
  SearchResult next = SearchResult_New();
  int status = RS_RESULT_MAX;
  std::thread worker([&] { status = normalizer->Next(normalizer, &next); });
  bool entered = RS::WaitForCondition([&] { return source.entered.load(); }, 5);
  if (entered) EXPECT_EQ((std::vector<double>{0.5, 1, 0.25}), drain());
  source.release.store(true, std::memory_order_release);
  worker.join();
  EXPECT_TRUE(entered);
  EXPECT_EQ(RS_RESULT_TIMEDOUT, status);
  EXPECT_EQ(37, qctx.resultLimit);
  EXPECT_FALSE(SearchResult_HasIndexResult(&next));
  EXPECT_TRUE(drain().empty());
  SearchResult_Destroy(&next);
}

TEST_F(MaxScoreDrainTest, nextClaimAndDrainKeepTheSameMaximum) {
  source.scores = {2, 4, 8};
  ASSERT_EQ(RS_RESULT_OK, normalizer->Next(normalizer, &result));
  EXPECT_DOUBLE_EQ(1, SearchResult_GetScore(&result));
  SearchResult_Clear(&result);
  EXPECT_EQ((std::vector<double>{0.5, 0.25}), drain());
  EXPECT_EQ(RS_RESULT_TIMEDOUT, normalizer->Next(normalizer, &result));
}

TEST_F(MaxScoreDrainTest, drainDoesNotWaitForPoolGrowthAndLatePreparationIsDiscarded) {
  source.scores = {2, 8, 100};
  source.allocationPauseAt = 2;
  static std::atomic<bool> prepared{false}, resume{false};
  static void *(*originalAlloc)(size_t);
  prepared.store(false);
  resume.store(false);
  originalAlloc = RedisModule_Alloc;
  RedisModule_Alloc = [](size_t size) -> void * {
    if (pauseSorterAllocation && size == sizeof(array_hdr_t) + 4 * sizeof(SearchResult *)) {
      pauseSorterAllocation = false;
      prepared.store(true, std::memory_order_release);
      while (!resume.load(std::memory_order_acquire)) std::this_thread::yield();
    }
    return originalAlloc(size);
  };
  SearchResult next = SearchResult_New();
  int status = RS_RESULT_MAX;
  std::thread worker([&] { status = normalizer->Next(normalizer, &next); });
  bool entered = RS::WaitForCondition([&] { return prepared.load(); }, 5);
  if (entered) EXPECT_EQ((std::vector<double>{1, 0.25}), drain());
  resume.store(true, std::memory_order_release);
  worker.join();
  RedisModule_Alloc = originalAlloc;
  EXPECT_TRUE(entered);
  EXPECT_EQ(RS_RESULT_TIMEDOUT, status);
  EXPECT_TRUE(drain().empty());
  SearchResult_Destroy(&next);
}

TEST_F(MaxScoreDrainTest, zeroMaximumAndErrorKeepCommittedRows) {
  source.scores = {0, 0};
  source.terminal = RS_RESULT_ERROR;
  EXPECT_EQ(RS_RESULT_ERROR, normalizer->Next(normalizer, &result));
  EXPECT_EQ((std::vector<double>{0, 0}), drain());
}

TEST_F(MaxScoreDrainTest, drainedExplanationAndRowOutliveRemainingPoolCleanup) {
  source.scores = {2, 8, 4};
  source.explanations = true;
  source.terminal = RS_RESULT_TIMEDOUT;
  EXPECT_EQ(RS_RESULT_TIMEDOUT, normalizer->Next(normalizer, &result));
  ASSERT_EQ(RP_DRAIN_OK, normalizer->Drain(normalizer, &result));
  normalizer->Free(normalizer);
  normalizer = nullptr;
  EXPECT_DOUBLE_EQ(0.5, SearchResult_GetScore(&result));
  ASSERT_NE(nullptr, SearchResult_GetScoreExplain(&result));
  EXPECT_STREQ("Final BM25STD.NORM: 0.50 = Original Score: 4.00 / Max Score: 8.00",
               SearchResult_GetScoreExplain(&result)->str);
  EXPECT_EQ(3, SearchResult_GetIndexResult(&result)->docId);
}

TEST_F(MaxScoreDrainTest, returnTimeoutYieldsPrefixThenDrainYieldsOnlyRemainingRows) {
  qctx.timeoutPolicy = TimeoutPolicy_Return;
  source.scores = {2, 8, 4};
  source.terminal = RS_RESULT_TIMEDOUT;
  ASSERT_EQ(RS_RESULT_OK, normalizer->Next(normalizer, &result));
  EXPECT_DOUBLE_EQ(0.5, SearchResult_GetScore(&result));
  SearchResult_Clear(&result);
  EXPECT_EQ((std::vector<double>{1, 0.25}), drain());
}

TEST_F(MaxScoreDrainTest, failTimeoutDoesNotYieldAndNormalEofStillNormalizes) {
  qctx.timeoutPolicy = TimeoutPolicy_Fail;
  source.scores = {2, 8, 4};
  source.terminal = RS_RESULT_TIMEDOUT;
  EXPECT_EQ(RS_RESULT_TIMEDOUT, normalizer->Next(normalizer, &result));
  EXPECT_FALSE(SearchResult_HasIndexResult(&result));
  source.terminal = RS_RESULT_EOF;
  for (double score : {0.5, 1.0, 0.25}) {
    ASSERT_EQ(RS_RESULT_OK, normalizer->Next(normalizer, &result));
    EXPECT_DOUBLE_EQ(score, SearchResult_GetScore(&result));
    SearchResult_Clear(&result);
  }
  EXPECT_EQ(RS_RESULT_EOF, normalizer->Next(normalizer, &result));
  EXPECT_EQ(0, source.drainCalls);
}

class SorterDrainTest : public ::testing::Test {
 protected:
  QueryProcessingCtx qctx = {};
  QueryError error = QueryError_Default();
  RLookup lookup = RLookup_New();
  SorterDrainSource source;
  ResultProcessor *sorter = nullptr;
  SearchResult result = SearchResult_New();

  void create(size_t capacity) {
    qctx.timeoutPolicy = TimeoutPolicy_ReturnStrict;
    qctx.resultLimit = 37;
    qctx.err = &error;
    sorter = RPSorter_NewByScore(capacity, nullptr);
    sorter->parent = &qctx;
    sorter->upstream = &source;
  }
  void TearDown() override {
    SearchResult_Destroy(&result);
    if (sorter) sorter->Free(sorter);
    RLookup_Cleanup(&lookup);
    QueryError_ClearError(&error);
  }
  void createMixedFieldSorter() {
    source.scores = {1, 1, 1, 1};
    source.values = {RSValue_NewNumber(1), RSValue_NewCopiedString("z", 1), RSValue_NewNumber(2),
                     RSValue_NewCopiedString("y", 1)};
    source.key = RLookup_GetKey_Write(&lookup, "sort", RLOOKUP_F_NOFLAGS);
    RLookup_Seal(&lookup);
    create(4);
    sorter->Free(sorter);
    sorter = RPSorter_NewByFields(4, &source.key, 1, 1);
    sorter->parent = &qctx;
    sorter->upstream = &source;
    source.terminal = RS_RESULT_TIMEDOUT;
  }
  std::vector<double> drainScores() {
    std::vector<double> output;
    RPDrainStatus status;
    while ((status = sorter->Drain(sorter, &result)) == RP_DRAIN_OK) {
      output.push_back(SearchResult_GetScore(&result));
      const auto *index = SearchResult_GetIndexResult(&result);
      EXPECT_NE(nullptr, index);
      if (index) EXPECT_EQ(SearchResult_GetDocId(&result), index->docId);
      EXPECT_TRUE(SearchResult_GetFlags(&result) & Result_OwnsIndexResult);
      SearchResult_Clear(&result);
    }
    EXPECT_EQ(RP_DRAIN_EOF, status);
    EXPECT_EQ(RP_DRAIN_EOF, sorter->Drain(sorter, &result));
    EXPECT_EQ(0, source.drainCalls);
    return output;
  }
};

TEST_F(SorterDrainTest, unstartedHeapIsTerminalWithoutSourceWork) {
  source.scores = {9};
  create(3);
  EXPECT_TRUE(drainScores().empty());
  EXPECT_EQ(RS_RESULT_TIMEDOUT, sorter->Next(sorter, &result));
  EXPECT_EQ(0, source.position);
}

TEST_F(SorterDrainTest, takesPartialTopNWhileUpstreamRemainsParked) {
  source.scores = {2, 8, 4, 10, 1};
  source.pauseAt = 3;
  create(2);
  SearchResult next = SearchResult_New();
  int status = RS_RESULT_MAX;
  std::thread worker([&] { status = sorter->Next(sorter, &next); });
  bool entered = RS::WaitForCondition([&] { return source.entered.load(); }, 5);
  if (entered) EXPECT_EQ((std::vector<double>{8, 4}), drainScores());
  source.release.store(true, std::memory_order_release);
  worker.join();
  EXPECT_TRUE(entered);
  EXPECT_EQ(RS_RESULT_TIMEDOUT, status);
  EXPECT_EQ(37, qctx.resultLimit);
  EXPECT_EQ(nullptr, SearchResult_GetIndexResult(&next));
  SearchResult_Destroy(&next);
}

TEST_F(SorterDrainTest, preservesAlreadyClaimedNextOutputAndNormalEofOrder) {
  source.scores = {2, 8, 4, 1};
  create(3);
  SearchResult next = SearchResult_New();
  ASSERT_EQ(RS_RESULT_OK, sorter->Next(sorter, &next));
  EXPECT_EQ(8, SearchResult_GetScore(&next));
  EXPECT_EQ((std::vector<double>{4, 2}), drainScores());
  EXPECT_EQ(8, SearchResult_GetScore(&next));
  EXPECT_EQ(2, SearchResult_GetIndexResult(&next)->docId);
  SearchResult_Destroy(&next);
}

TEST_F(SorterDrainTest, preparationLosesAdmissionWithoutWaitingForWorker) {
  source.scores = {2, 8};
  source.allocationPauseAt = 1;
  create(3);
  static std::atomic<bool> prepared, resume;
  static decltype(RedisModule_Alloc) originalAlloc;
  prepared.store(false);
  resume.store(false);
  originalAlloc = RedisModule_Alloc;
  RedisModule_Alloc = [](size_t size) -> void * {
    if (pauseSorterAllocation && size == sizeof(SearchResult)) {
      pauseSorterAllocation = false;
      prepared.store(true, std::memory_order_release);
      while (!resume.load(std::memory_order_acquire)) std::this_thread::yield();
    }
    return originalAlloc(size);
  };
  SearchResult next = SearchResult_New();
  int status = RS_RESULT_MAX;
  std::thread worker([&] { status = sorter->Next(sorter, &next); });
  bool entered = RS::WaitForCondition([&] { return prepared.load(); }, 5);
  if (entered) EXPECT_EQ((std::vector<double>{2}), drainScores());
  resume.store(true, std::memory_order_release);
  worker.join();
  RedisModule_Alloc = originalAlloc;
  EXPECT_TRUE(entered);
  EXPECT_EQ(RS_RESULT_TIMEDOUT, status);
  SearchResult_Destroy(&next);
}

TEST_F(SorterDrainTest, upstreamErrorDoesNotEraseCommittedHeap) {
  source.scores = {3, 1, 2};
  source.terminal = RS_RESULT_ERROR;
  create(2);
  ASSERT_EQ(RS_RESULT_ERROR, sorter->Next(sorter, &result));
  EXPECT_EQ((std::vector<double>{3, 2}), drainScores());
}

TEST_F(SorterDrainTest, limitStopLeavesRemainingHeapForFree) {
  source.scores = {3, 1, 2};
  source.terminal = RS_RESULT_TIMEDOUT;
  create(3);
  ASSERT_EQ(RS_RESULT_TIMEDOUT, sorter->Next(sorter, &result));
  ASSERT_EQ(RP_DRAIN_OK, sorter->Drain(sorter, &result));
  EXPECT_EQ(3, SearchResult_GetScore(&result));
  EXPECT_FALSE(RPSorter_TakeDrainError(sorter, &error));
  sorter->Free(sorter);
  sorter = nullptr;
  EXPECT_EQ(1, SearchResult_GetIndexResult(&result)->docId);
}

TEST_F(SorterDrainTest, sequentialPoliciesKeepNormalNextOrdering) {
  for (auto policy : {TimeoutPolicy_Return, TimeoutPolicy_Fail}) {
    source.scores = {3, 1, 2, 4};
    source.position = 0;
    create(3);
    qctx.timeoutPolicy = policy;
    for (double expected : {4, 3, 2}) {
      ASSERT_EQ(RS_RESULT_OK, sorter->Next(sorter, &result));
      EXPECT_EQ(expected, SearchResult_GetScore(&result));
      SearchResult_Clear(&result);
    }
    EXPECT_EQ(RS_RESULT_EOF, sorter->Next(sorter, &result));
    sorter->Free(sorter);
    sorter = nullptr;
  }
}

TEST_F(SorterDrainTest, sequentialDrainKeepsRemainingHeapAfterNextUnwinds) {
  source.scores = {3, 1, 2, 4};
  create(3);
  qctx.timeoutPolicy = TimeoutPolicy_Return;
  ASSERT_EQ(RS_RESULT_OK, sorter->Next(sorter, &result));
  EXPECT_EQ(4, SearchResult_GetScore(&result));
  SearchResult_Clear(&result);
  EXPECT_EQ((std::vector<double>{3, 2}), drainScores());
}

TEST_F(SorterDrainTest, preservesLegacyTimeoutDispatchUntilPipelineIntegration) {
  for (auto policy : {TimeoutPolicy_Return, TimeoutPolicy_Fail}) {
    source.scores = {3, 1, 2};
    source.position = 0;
    source.terminal = RS_RESULT_TIMEDOUT;
    create(3);
    qctx.timeoutPolicy = policy;
    if (policy == TimeoutPolicy_Return) {
      for (double expected : {3, 2, 1}) {
        ASSERT_EQ(RS_RESULT_OK, sorter->Next(sorter, &result));
        EXPECT_EQ(expected, SearchResult_GetScore(&result));
        SearchResult_Clear(&result);
      }
    }
    EXPECT_EQ(RS_RESULT_TIMEDOUT, sorter->Next(sorter, &result));
    sorter->Free(sorter);
    sorter = nullptr;
  }
}

TEST_F(SorterDrainTest, fieldOrderingAndScoreTiesMatchNext) {
  source.scores = {1, 1, 1};
  source.values = {RSValue_NewNumber(5), RSValue_NewNumber(1), RSValue_NewNumber(3)};
  source.key = RLookup_GetKey_Write(&lookup, "sort", RLOOKUP_F_NOFLAGS);
  RLookup_Seal(&lookup);
  for (int mode = 0; mode < 3; ++mode) {
    create(3);
    sorter->Free(sorter);
    sorter = mode == 2 ? RPSorter_NewByScore(3, source.key)
                       : RPSorter_NewByFields(3, &source.key, 1, mode);
    sorter->parent = &qctx;
    sorter->upstream = &source;
    source.position = 0;
    source.terminal = RS_RESULT_TIMEDOUT;
    ASSERT_EQ(RS_RESULT_TIMEDOUT, sorter->Next(sorter, &result));
    std::vector<t_docId> ids;
    while (sorter->Drain(sorter, &result) == RP_DRAIN_OK) {
      ids.push_back(SearchResult_GetDocId(&result));
      SearchResult_Clear(&result);
    }
    EXPECT_EQ(mode == 0 ? (std::vector<t_docId>{1, 3, 2}) : (std::vector<t_docId>{2, 3, 1}), ids);
    sorter->Free(sorter);
    sorter = nullptr;
  }
}

TEST_F(SorterDrainTest, comparisonDiagnosticsNeverWriteNextErrorDuringDrain) {
  createMixedFieldSorter();
  ASSERT_EQ(RS_RESULT_TIMEDOUT, sorter->Next(sorter, &result));
  EXPECT_STREQ("Error converting string", QueryError_GetUserError(&error));
  EXPECT_STREQ("Error converting string", QueryError_GetDisplayableError(&error, true));
  QueryError_ClearError(&error);
  QueryError drainError = QueryError_Default();
  size_t count = 0;
  while (sorter->Drain(sorter, &result) == RP_DRAIN_OK) {
    ++count;
    SearchResult_Clear(&result);
  }
  EXPECT_EQ(4, count);
  EXPECT_FALSE(QueryError_HasError(&error));
  EXPECT_TRUE(RPSorter_TakeDrainError(sorter, &drainError));
  EXPECT_TRUE(QueryError_HasError(&drainError));
  EXPECT_STREQ("Error converting string", QueryError_GetUserError(&drainError));
  EXPECT_STREQ("Error converting string", QueryError_GetDisplayableError(&drainError, true));
  EXPECT_FALSE(RPSorter_TakeDrainError(sorter, &drainError));
  QueryError_ClearError(&drainError);
}

TEST_F(SorterDrainTest, comparisonDiagnosticsPreserveExistingErrors) {
  createMixedFieldSorter();
  QueryError_SetCode(&error, QUERY_ERROR_CODE_GENERIC);
  QueryError_SetDetail(&error, "earlier Next error");
  ASSERT_EQ(RS_RESULT_TIMEDOUT, sorter->Next(sorter, &result));
  EXPECT_STREQ("earlier Next error", QueryError_GetUserError(&error));
  while (sorter->Drain(sorter, &result) == RP_DRAIN_OK) SearchResult_Clear(&result);
  QueryError drainError = QueryError_Default();
  QueryError_SetCode(&drainError, QUERY_ERROR_CODE_GENERIC);
  QueryError_SetDetail(&drainError, "earlier Drain error");
  EXPECT_TRUE(RPSorter_TakeDrainError(sorter, &drainError));
  EXPECT_STREQ("earlier Drain error", QueryError_GetUserError(&drainError));
  QueryError_ClearError(&drainError);
}

TEST(SearchResultComparisonTest, legacyFieldComparatorPreservesFallbackAndErrors) {
  RLookup lookup = RLookup_New();
  const RLookupKey *key = RLookup_GetKey_Write(&lookup, "sort", RLOOKUP_F_NOFLAGS);
  RLookup_Seal(&lookup);
  SearchResult number = SearchResult_New();
  SearchResult text = SearchResult_New();
  SearchResult_SetDocId(&number, 2);
  SearchResult_SetDocId(&text, 1);
  RLookup_WriteOwnKey(key, SearchResult_GetRowDataMut(&number), RSValue_NewNumber(1));
  RLookup_WriteOwnKey(key, SearchResult_GetRowDataMut(&text), RSValue_NewCopiedString("z", 1));

  // Without an error sink the legacy entrypoint falls back to string ordering.
  EXPECT_GT(SearchResult_CmpByFields(&key, 1, &number, &text, 1, nullptr), 0);
  QueryError error = QueryError_Default();
  EXPECT_LT(SearchResult_CmpByFields(&key, 1, &number, &text, 1, &error), 0);
  EXPECT_EQ(QUERY_ERROR_CODE_NUMERIC_VALUE_INVALID, QueryError_GetCode(&error));
  EXPECT_STREQ("Error converting string", QueryError_GetUserError(&error));
  EXPECT_STREQ("Error converting string", QueryError_GetDisplayableError(&error, true));
  QueryError_ClearError(&error);

  QueryError_SetCode(&error, QUERY_ERROR_CODE_GENERIC);
  QueryError_SetDetail(&error, "earlier error");
  EXPECT_LT(SearchResult_CmpByFields(&key, 1, &number, &text, 1, &error), 0);
  EXPECT_EQ(QUERY_ERROR_CODE_GENERIC, QueryError_GetCode(&error));
  EXPECT_STREQ("earlier error", QueryError_GetUserError(&error));
  QueryError_ClearError(&error);
  SearchResult_Destroy(&number);
  SearchResult_Destroy(&text);
  RLookup_Cleanup(&lookup);
}

// A shared row cursor models an upstream that can transfer different rows to either path.
struct PagerDrainSource : ResultProcessor {
  std::atomic<size_t> cursor{0};
  std::atomic<bool> entered{false}, release{true};
  bool pauseBeforeClaim = true;
  size_t nextCalls = 0, drainCalls = 0;
  int nextStatus = RS_RESULT_OK;
  RPDrainStatus drainTerminal = RP_DRAIN_EOF;
  size_t rows = 8;

  void pause() {
    entered.store(true, std::memory_order_release);
    while (!release.load(std::memory_order_acquire)) std::this_thread::yield();
  }

  bool claim(SearchResult *result) {
    size_t row = cursor.fetch_add(1);
    if (row >= rows) return false;
    SearchResult_SetDocId(result, row + 1);
    SearchResult_SetScore(result, row + 1);
    return true;
  }

  PagerDrainSource() {
    *static_cast<ResultProcessor *>(this) = {};
    Next = [](ResultProcessor *base, SearchResult *result) -> int {
      auto *self = static_cast<PagerDrainSource *>(base);
      ++self->nextCalls;
      if (self->pauseBeforeClaim) self->pause();
      if (self->nextStatus != RS_RESULT_OK) return self->nextStatus;
      bool row = self->claim(result);
      if (!self->pauseBeforeClaim) self->pause();
      return row ? RS_RESULT_OK : RS_RESULT_EOF;
    };
    Drain = [](ResultProcessor *base, SearchResult *result) {
      auto *self = static_cast<PagerDrainSource *>(base);
      ++self->drainCalls;
      return self->claim(result) ? RP_DRAIN_OK : self->drainTerminal;
    };
  }
};

class PagerDrainTest : public ::testing::Test {
 protected:
  QueryProcessingCtx qctx = {};
  PagerDrainSource source;
  ResultProcessor *pager = nullptr;
  SearchResult result = SearchResult_New();

  void create(size_t offset, size_t limit, RSTimeoutPolicy policy = TimeoutPolicy_ReturnStrict) {
    qctx.timeoutPolicy = policy;
    qctx.resultLimit = 10;
    pager = RPPager_New(offset, limit);
    pager->parent = &qctx;
    pager->upstream = &source;
  }

  std::vector<double> drain() {
    std::vector<double> rows;
    RPDrainStatus status;
    while ((status = pager->Drain(pager, &result)) == RP_DRAIN_OK) {
      rows.push_back(SearchResult_GetScore(&result));
      SearchResult_Clear(&result);
    }
    EXPECT_EQ(RP_DRAIN_EOF, status);
    return rows;
  }

  void TearDown() override {
    SearchResult_Destroy(&result);
    if (pager) pager->Free(pager);
  }
};

TEST_F(PagerDrainTest, unstartedDrainAppliesOffsetAndLimitWithoutQueryScratch) {
  create(2, 2);
  EXPECT_EQ((std::vector<double>{3, 4}), drain());
  EXPECT_EQ(0, source.nextCalls);
  EXPECT_EQ(4, source.drainCalls);
  EXPECT_EQ(10, qctx.resultLimit);
  EXPECT_EQ(RS_RESULT_TIMEDOUT, pager->Next(pager, &result));
}

TEST_F(PagerDrainTest, zeroLimitDoesNotPullUpstream) {
  create(3, 0);
  EXPECT_TRUE(drain().empty());
  EXPECT_EQ(0, source.drainCalls);
}

TEST_F(PagerDrainTest, returnDrainsRemainingBudgetAfterNextUnwinds) {
  create(2, 2, TimeoutPolicy_Return);
  ASSERT_EQ(RS_RESULT_OK, pager->Next(pager, &result));
  EXPECT_EQ(3, SearchResult_GetScore(&result));
  SearchResult_Clear(&result);
  EXPECT_EQ((std::vector<double>{4}), drain());
  EXPECT_EQ(10, qctx.resultLimit);
}

TEST_F(PagerDrainTest, failKeepsSequentialPaging) {
  create(2, 2, TimeoutPolicy_Fail);
  for (double score : {3, 4}) {
    ASSERT_EQ(RS_RESULT_OK, pager->Next(pager, &result));
    EXPECT_EQ(score, SearchResult_GetScore(&result));
    SearchResult_Clear(&result);
  }
  EXPECT_EQ(RS_RESULT_EOF, pager->Next(pager, &result));
  EXPECT_EQ(0, source.drainCalls);
  EXPECT_EQ(10, qctx.resultLimit);
}

TEST_F(PagerDrainTest, strictDrainPreservesCommittedSkipsAndOutputPrefix) {
  create(2, 3);
  ASSERT_EQ(RS_RESULT_OK, pager->Next(pager, &result));
  EXPECT_EQ(3, SearchResult_GetScore(&result));
  SearchResult_Clear(&result);
  EXPECT_EQ((std::vector<double>{4, 5}), drain());
  EXPECT_EQ(3, source.nextCalls);
  EXPECT_EQ(2, source.drainCalls);
  EXPECT_EQ(10, qctx.resultLimit);
}

TEST_F(PagerDrainTest, uncommittedSkipNeverExposesPreOffsetRow) {
  for (bool beforeClaim : {true, false}) {
    create(1, 2);
    source.cursor.store(0);
    source.entered.store(false);
    source.release.store(false);
    source.pauseBeforeClaim = beforeClaim;
    SearchResult next = SearchResult_New();
    int status = RS_RESULT_MAX;
    std::thread worker([&] { status = pager->Next(pager, &next); });
    bool entered = RS::WaitForCondition([&] { return source.entered.load(); }, 5);
    if (entered) {
      EXPECT_EQ(beforeClaim ? (std::vector<double>{2, 3}) : (std::vector<double>{3, 4}), drain());
    }
    source.release.store(true, std::memory_order_release);
    worker.join();
    EXPECT_TRUE(entered);
    EXPECT_EQ(RS_RESULT_TIMEDOUT, status);
    EXPECT_TRUE(drain().empty());
    SearchResult_Destroy(&next);
    pager->Free(pager);
    pager = nullptr;
  }
}

TEST_F(PagerDrainTest, outstandingOutputReservesCapacityAndLateFailureCannotRefundIt) {
  for (int terminal : {RS_RESULT_OK, RS_RESULT_TIMEDOUT}) {
    create(0, 2);
    source.cursor.store(0);
    source.entered.store(false);
    source.release.store(false);
    source.nextStatus = terminal;
    SearchResult next = SearchResult_New();
    int status = RS_RESULT_MAX;
    std::thread worker([&] { status = pager->Next(pager, &next); });
    bool entered = RS::WaitForCondition([&] { return source.entered.load(); }, 5);
    if (entered) EXPECT_EQ((std::vector<double>{1}), drain());
    source.release.store(true, std::memory_order_release);
    worker.join();
    EXPECT_TRUE(entered);
    EXPECT_EQ(terminal, status);
    if (terminal == RS_RESULT_OK) EXPECT_EQ(2, SearchResult_GetScore(&next));
    EXPECT_TRUE(drain().empty());
    SearchResult_Destroy(&next);
    pager->Free(pager);
    pager = nullptr;
  }
}

TEST_F(PagerDrainTest, failedReservationBeforeTakeoverIsRefunded) {
  create(0, 2);
  source.nextStatus = RS_RESULT_TIMEDOUT;
  EXPECT_EQ(RS_RESULT_TIMEDOUT, pager->Next(pager, &result));
  EXPECT_EQ((std::vector<double>{1, 2}), drain());
}

TEST_F(PagerDrainTest, cursorPolicyRestoreUsesStrictReservationAfterInlineReturn) {
  create(0, 3, TimeoutPolicy_Return);
  ASSERT_EQ(RS_RESULT_OK, pager->Next(pager, &result));
  EXPECT_EQ(1, SearchResult_GetScore(&result));
  SearchResult_Clear(&result);
  qctx.timeoutPolicy = TimeoutPolicy_ReturnStrict;
  source.entered.store(false);
  source.release.store(false);
  SearchResult next = SearchResult_New();
  int status = RS_RESULT_MAX;
  std::thread worker([&] { status = pager->Next(pager, &next); });
  bool entered = RS::WaitForCondition([&] { return source.entered.load(); }, 5);
  if (entered) EXPECT_EQ((std::vector<double>{2}), drain());
  source.release.store(true, std::memory_order_release);
  worker.join();
  EXPECT_TRUE(entered);
  EXPECT_EQ(RS_RESULT_OK, status);
  EXPECT_EQ(3, SearchResult_GetScore(&next));
  EXPECT_TRUE(drain().empty());
  SearchResult_Destroy(&next);
}

TEST_F(PagerDrainTest, drainPropagatesUpstreamErrorAndEof) {
  create(1, 2);
  source.rows = 0;
  for (auto status : {RP_DRAIN_ERROR, RP_DRAIN_EOF}) {
    source.drainTerminal = status;
    EXPECT_EQ(status, pager->Drain(pager, &result));
  }
  EXPECT_EQ(0, source.nextCalls);
}

// Next retains a different DMD from the rows owned by the inherited Drain cursor.
struct KeyNameDrainSource : LoaderDrainSource {
  std::atomic<bool> entered{false}, release{true};
  KeyNameDrainSource() {
    Next = [](ResultProcessor *base, SearchResult *result) -> int {
      auto *self = static_cast<KeyNameDrainSource *>(base);
      ++self->nextCalls;
      self->entered.store(true, std::memory_order_release);
      while (!self->release.load(std::memory_order_acquire)) std::this_thread::yield();
      auto *dmd = self->documents.back();
      DMD_Incref(dmd);
      SearchResult_SetDocumentMetadata(result, dmd);
      return RS_RESULT_OK;
    };
  }
};

class TimeoutDrainTest : public LoaderDrainTest, public ::testing::WithParamInterface<bool> {
 protected:
  KeyNameDrainSource timeoutSource;
  QueryRequestTimeout timeout = {};

  void SetUp() override {
    LoaderDrainTest::SetUp();
    QueryRequestTimeout_Init(&timeout,
                             GetParam() ? TimeoutPolicy_Return : TimeoutPolicy_ReturnStrict, 60000);
    QueryRequestTimeout_BeginCycle(&timeout, GetParam() ? QUERY_REQUEST_TIMEOUT_CLOCK_DEADLINE
                                                        : QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
    sctx.timeout = &timeout;
    qctx.endProc = &timeoutSource;
    if (GetParam())
      PipelineAddTimeoutAfterCountClock(&qctx, &sctx, 1);
    else
      PipelineAddTimeoutAfterCount(&qctx, &sctx, 1);
    loader = qctx.endProc;
    timeoutSource.documents = {document("timeout:drain", nullptr),
                               document("timeout:next", nullptr)};
  }
};

INSTANTIATE_TEST_SUITE_P(TimeoutSources, TimeoutDrainTest, ::testing::Bool());

TEST_P(TimeoutDrainTest, drainPreservesTimeoutAndNextCounter) {
  const auto kind = timeout.kind;
  const auto deadline = GetParam() ? *QueryRequestTimeout_GetClockDeadline(&timeout) : timespec{};
  ASSERT_EQ(RP_DRAIN_OK, loader->Drain(loader, &result));
  EXPECT_EQ(timeoutSource.documents.front(), SearchResult_GetDocumentMetadata(&result));
  EXPECT_EQ(0, timeoutSource.nextCalls);
  SearchResult_Clear(&result);
  // The first Next must not inject a timeout after Drain has passed a row.
  struct TimeoutProbe : ResultProcessor {
    QueryRequestTimeout *timeout;
  } probe = {};
  probe.timeout = &timeout;
  probe.Next = [](ResultProcessor *base, SearchResult *) -> int {
    return QueryRequestTimeout_IsTimedOutExact(static_cast<TimeoutProbe *>(base)->timeout)
               ? RS_RESULT_TIMEDOUT
               : RS_RESULT_OK;
  };
  loader->upstream = &probe;
  EXPECT_EQ(RS_RESULT_OK, loader->Next(loader, &result));
  EXPECT_EQ(RS_RESULT_TIMEDOUT, loader->Next(loader, &result));
  EXPECT_EQ(kind, timeout.kind);
  EXPECT_FALSE(QueryRequestTimeout_IsTimedOutExact(&timeout));
  if (GetParam()) {
    EXPECT_EQ(deadline.tv_sec, QueryRequestTimeout_GetClockDeadline(&timeout)->tv_sec);
    EXPECT_EQ(deadline.tv_nsec, QueryRequestTimeout_GetClockDeadline(&timeout)->tv_nsec);
  }
}

TEST_P(TimeoutDrainTest, forwardsTerminalStatusesWithoutAccessingTimeoutContext) {
  sctx.timeout = nullptr;
  timeoutSource.documents.clear();
  timeoutSource.terminal = RP_DRAIN_ERROR;
  EXPECT_EQ(RP_DRAIN_ERROR, loader->Drain(loader, &result));
  EXPECT_EQ(RP_DRAIN_EOF, loader->Drain(loader, &result));
  EXPECT_EQ(0, timeoutSource.nextCalls);
}

TEST_P(TimeoutDrainTest, drainsWhileNextIsParkedUpstream) {
  timeoutSource.release.store(false);
  SearchResult next = SearchResult_New();
  int status = RS_RESULT_MAX;
  std::thread worker([&] { status = loader->Next(loader, &next); });
  bool entered = RS::WaitForCondition([&] { return timeoutSource.entered.load(); }, 5);
  if (entered) {
    EXPECT_EQ(RP_DRAIN_OK, loader->Drain(loader, &result));
    EXPECT_EQ(timeoutSource.documents.front(), SearchResult_GetDocumentMetadata(&result));
  }
  timeoutSource.release.store(true, std::memory_order_release);
  worker.join();
  EXPECT_TRUE(entered);
  EXPECT_EQ(RS_RESULT_OK, status);
  EXPECT_EQ(timeoutSource.documents.back(), SearchResult_GetDocumentMetadata(&next));
  EXPECT_FALSE(QueryRequestTimeout_IsTimedOutExact(&timeout));
  SearchResult_Destroy(&next);
}

class PauseDrainTest : public LoaderDrainTest {
 protected:
  KeyNameDrainSource pauseSource;

  void SetUp() override {
    LoaderDrainTest::SetUp();
    ASSERT_FALSE(QueryDebugCtx_HasDebugRP());
    loader = RPPauseAfterCount_New(1);
    ASSERT_NE(nullptr, loader);
    loader->upstream = &pauseSource;
    pauseSource.documents = {document("pause:drain", nullptr), document("pause:next", nullptr)};
  }

  void TearDown() override {
    QueryDebugCtx_SetPause(false);
    LoaderDrainTest::TearDown();
  }
};

TEST_F(PauseDrainTest, drainPreservesPauseStateAndNextCounter) {
  QueryDebugCtx_SetPause(true);
  ASSERT_EQ(RP_DRAIN_OK, loader->Drain(loader, &result));
  EXPECT_EQ(pauseSource.documents.front(), SearchResult_GetDocumentMetadata(&result));
  EXPECT_TRUE(QueryDebugCtx_IsPaused());
  EXPECT_EQ(loader, QueryDebugCtx_GetDebugRP());
  EXPECT_EQ(0, pauseSource.nextCalls);
  SearchResult_Clear(&result);
  // A consumed debug counter would make this Next wait instead of reaching upstream.
  ASSERT_EQ(RS_RESULT_OK, loader->Next(loader, &result));
  EXPECT_EQ(pauseSource.documents.back(), SearchResult_GetDocumentMetadata(&result));
  EXPECT_TRUE(QueryDebugCtx_IsPaused());
}

TEST_F(PauseDrainTest, forwardsErrorAndEofWithoutChangingDebugState) {
  pauseSource.documents.clear();
  pauseSource.terminal = RP_DRAIN_ERROR;
  QueryDebugCtx_SetPause(true);
  EXPECT_EQ(RP_DRAIN_ERROR, loader->Drain(loader, &result));
  EXPECT_EQ(RP_DRAIN_EOF, loader->Drain(loader, &result));
  EXPECT_TRUE(QueryDebugCtx_IsPaused());
  EXPECT_EQ(0, pauseSource.nextCalls);
}

TEST_F(PauseDrainTest, drainsWhileNextIsParkedUpstream) {
  pauseSource.release.store(false);
  SearchResult next = SearchResult_New();
  int status = RS_RESULT_MAX;
  std::thread worker([&] { status = loader->Next(loader, &next); });
  bool entered = RS::WaitForCondition([&] { return pauseSource.entered.load(); }, 5);
  if (entered) {
    EXPECT_EQ(RP_DRAIN_OK, loader->Drain(loader, &result));
    EXPECT_EQ(pauseSource.documents.front(), SearchResult_GetDocumentMetadata(&result));
  }
  pauseSource.release.store(true, std::memory_order_release);
  worker.join();
  EXPECT_TRUE(entered);
  EXPECT_EQ(RS_RESULT_OK, status);
  EXPECT_EQ(pauseSource.documents.back(), SearchResult_GetDocumentMetadata(&next));
  EXPECT_FALSE(QueryDebugCtx_IsPaused());
  SearchResult_Destroy(&next);
}

// Each path creates its own row and index result; only immutable configuration is shared.
struct HighlighterDrainSource : KeyNameDrainSource {
  const RLookupKey *key = nullptr;
  bool retainIndex = true;
  bool termOffsets = false;

  void populate(SearchResult *result) const {
    static constexpr char text[] = "one  two three four five six seven eight";
    RLookup_WriteOwnKey(key, SearchResult_GetRowDataMut(result),
                        RSValue_NewCopiedString(text, sizeof(text) - 1));
    if (retainIndex) {
      RSIndexResult *index;
      if (termOffsets) {
        RSToken token = {.str = const_cast<char *>("three"), .len = 5, .flags = 0};
        index = NewTokenRecord(NewQueryTerm(&token, 1), 1);
        // The record owns its term; these encoded offsets have static lifetime.
        static const char offsets[] = {3};
        RSOffsetVector_SetData(&index->data.term.borrowed.offsets, offsets, sizeof(offsets));
      } else {
        index = NewVirtualResult(1, RS_FIELDMASK_ALL);
      }
      SearchResult_SetOwnedIndexResult(result, index);
    }
  }

  HighlighterDrainSource() {
    Next = [](ResultProcessor *base, SearchResult *result) -> int {
      auto *self = static_cast<HighlighterDrainSource *>(base);
      ++self->nextCalls;
      self->entered.store(true, std::memory_order_release);
      while (!self->release.load(std::memory_order_acquire)) std::this_thread::yield();
      auto *dmd = self->documents.back();
      DMD_Incref(dmd);
      SearchResult_SetDocumentMetadata(result, dmd);
      self->populate(result);
      return RS_RESULT_OK;
    };
    Drain = [](ResultProcessor *base, SearchResult *result) {
      auto *self = static_cast<HighlighterDrainSource *>(base);
      if (self->position == self->documents.size()) return self->terminal;
      auto *dmd = self->documents[self->position++];
      DMD_Incref(dmd);
      SearchResult_SetDocumentMetadata(result, dmd);
      self->populate(result);
      return RP_DRAIN_OK;
    };
  }
};

class HighlighterDrainTest : public LoaderDrainTest {
 protected:
  HighlighterDrainSource hlpSource;
  ReturnedField field = {};
  FieldList fields = {};

  void createHighlighter(bool allFields = false, bool termOffsets = false) {
    hlpSource.termOffsets = termOffsets;
    if (termOffsets) {
      auto *cache = static_cast<IndexSpecCache *>(rm_calloc(1, sizeof(IndexSpecCache)));
      cache->refcount = 1;
      cache->nfields = 1;
      cache->fields = static_cast<FieldSpec *>(rm_calloc(1, sizeof(FieldSpec)));
      cache->fields[0].fieldName = NewHiddenString("field", 5, true);
      cache->fields[0].fieldPath = cache->fields[0].fieldName;
      cache->fields[0].types = INDEXFLD_T_FULLTEXT;
      RLookup_SetCache(&lookup, cache);
    }
    hlpSource.key = RLookup_GetKey_Write(&lookup, "field", RLOOKUP_F_NOFLAGS);
    RLookup_Seal(&lookup);
    field.name = "field";
    field.lookupKey = hlpSource.key;
    field.mode = SummarizeMode_Synopsis;
    field.summarizeSettings.contextLen = 1;
    field.summarizeSettings.numFrags = 1;
    field.summarizeSettings.separator = const_cast<char *>("...");
    field.highlightSettings.openTag = const_cast<char *>("<b>");
    field.highlightSettings.closeTag = const_cast<char *>("</b>");
    if (allFields) {
      fields.defaultField = field;
    } else {
      fields.fields = &field;
      fields.numFields = 1;
    }
    loader = RPHighlighter_New(RS_LANG_ENGLISH, &fields, &lookup, false);
    loader->upstream = &hlpSource;
    // No query context: Drain must not consult the root iterator, even without retained data.
    hlpSource.documents = {document("highlight:drain", nullptr),
                           document("highlight:next", nullptr)};
    if (termOffsets) {
      for (auto *dmd : hlpSource.documents) {
        auto *offsets = NewByteOffsets();
        RSByteOffsets_ReserveFields(offsets, 1);
        RSByteOffsets_AddField(offsets, 0, 1)->lastTokPos = 8;
        ByteOffsetWriter writer;
        ByteOffsetWriter_Init(&writer);
        for (uint32_t offset : {0, 5, 9, 15, 20, 25, 29, 35}) {
          ByteOffsetWriter_Write(&writer, offset);
        }
        ByteOffsetWriter_Move(&writer, offsets);
        ByteOffsetWriter_Cleanup(&writer);
        DocTable_SetByteOffsets(dmd, offsets);
      }
    }
  }
};

TEST_F(HighlighterDrainTest, termOffsetsHighlightWholeFieldAndFragmentsLikeNext) {
  createHighlighter(false, true);
  for (bool fragments : {false, true}) {
    field.mode = fragments
                     ? static_cast<SummarizeMode>(SummarizeMode_Highlight | SummarizeMode_Synopsis)
                     : SummarizeMode_Highlight;
    if (fragments) {
      fields.defaultField = field;
      fields.numFields = 0;
    }
    ASSERT_EQ(RS_RESULT_OK, loader->Next(loader, &result));
    size_t len = 0;
    const char *text =
        RSValue_StringPtrLen(RLookupRow_Get(hlpSource.key, SearchResult_GetRowData(&result)), &len);
    std::string expected(text, len);
    EXPECT_NE(std::string::npos, expected.find("<b>three</b>"));
    if (!fragments)
      EXPECT_EQ("one  two <b>three</b> four five six seven eight", expected);
    else
      EXPECT_NE(std::string::npos, expected.find("..."));
    SearchResult_Clear(&result);
    ASSERT_EQ(RP_DRAIN_OK, loader->Drain(loader, &result));
    expectValue(hlpSource.key, expected.c_str());
    SearchResult_Clear(&result);
  }
}

TEST_F(HighlighterDrainTest, retainedDataMatchesNextForExplicitAndAllFields) {
  createHighlighter();
  for (bool allFields : {false, true}) {
    if (allFields) {
      fields.defaultField = field;
      fields.numFields = 0;
    }
    ASSERT_EQ(RS_RESULT_OK, loader->Next(loader, &result));
    expectValue(hlpSource.key, "one two");
    SearchResult_Clear(&result);
    ASSERT_EQ(RP_DRAIN_OK, loader->Drain(loader, &result));
    expectValue(hlpSource.key, "one two");
    SearchResult_Clear(&result);
  }
}

TEST_F(HighlighterDrainTest, missingRetainedIndexLeavesRowUnchangedWithoutIteratorAccess) {
  createHighlighter();
  hlpSource.retainIndex = false;
  ASSERT_EQ(RP_DRAIN_OK, loader->Drain(loader, &result));
  expectValue(hlpSource.key, "one  two three four five six seven eight");
  EXPECT_EQ(0, hlpSource.nextCalls);
}

TEST_F(HighlighterDrainTest, propagatesTerminalStatusesWithoutTouchingOutput) {
  createHighlighter();
  hlpSource.documents.clear();
  for (auto status : {RP_DRAIN_EOF, RP_DRAIN_ERROR}) {
    hlpSource.terminal = status;
    EXPECT_EQ(status, loader->Drain(loader, &result));
    EXPECT_FALSE(SearchResult_HasIndexResult(&result));
  }
  EXPECT_EQ(0, hlpSource.nextCalls);
}

TEST_F(HighlighterDrainTest, drainsWhileNextIsParkedUpstream) {
  createHighlighter(false, true);
  field.mode = SummarizeMode_Highlight;
  hlpSource.release.store(false);
  SearchResult next = SearchResult_New();
  int nextStatus = RS_RESULT_MAX;
  std::thread worker([&] { nextStatus = loader->Next(loader, &next); });
  bool entered = RS::WaitForCondition([&] { return hlpSource.entered.load(); }, 5);
  if (entered) {
    EXPECT_EQ(RP_DRAIN_OK, loader->Drain(loader, &result));
    expectValue(hlpSource.key, "one  two <b>three</b> four five six seven eight");
  }
  hlpSource.release.store(true, std::memory_order_release);
  worker.join();
  EXPECT_TRUE(entered);
  EXPECT_EQ(RS_RESULT_OK, nextStatus);
  EXPECT_NE(SearchResult_GetDocumentMetadata(&result), SearchResult_GetDocumentMetadata(&next));
  size_t length = 0;
  const char *text =
      RSValue_StringPtrLen(RLookupRow_Get(hlpSource.key, SearchResult_GetRowData(&next)), &length);
  EXPECT_EQ("one  two <b>three</b> four five six seven eight", std::string(text, length));
  SearchResult_Destroy(&next);
}

class KeyNameDrainTest : public LoaderDrainTest {
 protected:
  KeyNameDrainSource keySource;
  bool previousUnstable = false;
  const RLookupKey *key = nullptr;

  void SetUp() override {
    LoaderDrainTest::SetUp();
    previousUnstable = RSGlobalConfig.enableUnstableFeatures;
    RSGlobalConfig.enableUnstableFeatures = true;
    key = RLookup_GetKey_Load(&lookup, "key_alias", "__key", 0);
    uint32_t state = 0;
    loader = RPLoader_New(&sctx, QEXEC_F_RUN_IN_BACKGROUND, &lookup, &key, 1, false, &state);
    ASSERT_EQ(RP_KEY_NAME_LOADER, loader->type);
    EXPECT_EQ(0, state);
    loader->upstream = &keySource;
    RLookup_Seal(&lookup);
  }
  void TearDown() override {
    RSGlobalConfig.enableUnstableFeatures = previousUnstable;
    LoaderDrainTest::TearDown();
  }
};

TEST_F(KeyNameDrainTest, copiesBinaryKeyAndPreservesUnrelatedResultData) {
  const char name[] = "doc\0key";
  auto *dmd = DocTable_Put(&spec.docs, name, sizeof(name) - 1, 1, Document_DefaultFlags, nullptr, 0,
                           DocumentType_Hash);
  DMD_Return(dmd);
  keySource.documents = {dmd};
  SearchResult_SetScore(&result, 7);
  RLookup_WriteOwnKey(key, SearchResult_GetRowDataMut(&result), RSValue_NewNumber(-1));
  ASSERT_EQ(RP_DRAIN_OK, loader->Drain(loader, &result));
  const RSValue *value = RLookupRow_Get(key, SearchResult_GetRowData(&result));
  ASSERT_NE(nullptr, value);
  size_t len = 0;
  const char *copied = RSValue_StringPtrLen(value, &len);
  EXPECT_EQ(std::string(name, sizeof(name) - 1), std::string(copied, len));
  EXPECT_NE(dmd->keyPtr, copied);
  EXPECT_EQ(7, SearchResult_GetScore(&result));
  EXPECT_EQ(dmd->id, SearchResult_GetDocId(&result));
  SearchResult_Clear(&result);
  EXPECT_EQ(RP_DRAIN_EOF, loader->Drain(loader, &result));
  EXPECT_EQ(0, keySource.nextCalls);
}

TEST_F(KeyNameDrainTest, propagatesTerminalStatusesWithoutReadingDmd) {
  EXPECT_EQ(RP_DRAIN_EOF, loader->Drain(loader, &result));
  keySource.terminal = RP_DRAIN_ERROR;
  EXPECT_EQ(RP_DRAIN_ERROR, loader->Drain(loader, &result));
  EXPECT_EQ(RP_DRAIN_EOF, loader->Drain(loader, &result));
  EXPECT_EQ(nullptr, SearchResult_GetDocumentMetadata(&result));
  EXPECT_EQ(0, keySource.nextCalls);
}

TEST_F(KeyNameDrainTest, drainsWhileNextIsParkedAndKeepsItsOwnKey) {
  keySource.documents = {document("drained", nullptr), document("next", nullptr)};
  keySource.release.store(false);
  SearchResult next = SearchResult_New();
  int status = RS_RESULT_MAX;
  std::thread worker([&] { status = loader->Next(loader, &next); });
  bool entered = RS::WaitForCondition([&] { return keySource.entered.load(); }, 5);
  if (entered) {
    EXPECT_EQ(RP_DRAIN_OK, loader->Drain(loader, &result));
    expectValue(key, "drained");
  }
  keySource.release.store(true, std::memory_order_release);
  worker.join();
  EXPECT_TRUE(entered);
  EXPECT_EQ(RS_RESULT_OK, status);
  const RSValue *nextValue = RLookupRow_Get(key, SearchResult_GetRowData(&next));
  ASSERT_NE(nullptr, nextValue);
  size_t len = 0;
  const char *data = RSValue_StringPtrLen(nextValue, &len);
  EXPECT_EQ(std::string("next"), std::string(data, len));
  expectValue(key, "drained");
  SearchResult_Destroy(&next);
}

// Independent source payloads let a parked Next coexist with upstream Drain.
struct MetricsDrainSource : ResultProcessor {
  RSIndexResult *nextIndex = NewVirtualResult(1, RS_FIELDMASK_ALL);
  RSIndexResult *drainIndex = NewVirtualResult(1, RS_FIELDMASK_ALL);
  int nextStatus = RS_RESULT_OK;
  RPDrainStatus drainStatus = RP_DRAIN_OK;
  size_t nextCalls = 0, drainCalls = 0;
  std::atomic<bool> entered{false}, release{true};

  MetricsDrainSource() {
    *static_cast<ResultProcessor *>(this) = {};
    Next = [](ResultProcessor *base, SearchResult *res) -> int {
      auto *self = static_cast<MetricsDrainSource *>(base);
      ++self->nextCalls;
      self->entered.store(true, std::memory_order_release);
      while (!self->release.load(std::memory_order_acquire)) std::this_thread::yield();
      if (self->nextStatus == RS_RESULT_OK) {
        SearchResult_SetBorrowedIndexResult(res, self->nextIndex);
      }
      return self->nextStatus;
    };
    Drain = [](ResultProcessor *base, SearchResult *res) {
      auto *self = static_cast<MetricsDrainSource *>(base);
      ++self->drainCalls;
      if (self->drainStatus != RP_DRAIN_OK) return self->drainStatus;
      SearchResult_SetOwnedIndexResult(res, self->drainIndex);
      self->drainIndex = nullptr;
      self->drainStatus = RP_DRAIN_EOF;
      return RP_DRAIN_OK;
    };
  }
  ~MetricsDrainSource() {
    if (nextIndex) IndexResult_Free(nextIndex);
    if (drainIndex) IndexResult_Free(drainIndex);
  }
};

class MetricsDrainTest : public ::testing::Test {
 protected:
  RLookup lookup = RLookup_New();
  const RLookupKey *key = RLookup_GetKey_Write(&lookup, "metric", RLOOKUP_F_NOFLAGS);
  const RLookupKey *other = RLookup_GetKey_Write(&lookup, "other", RLOOKUP_F_NOFLAGS);
  MetricsDrainSource source;
  ResultProcessor *metrics = RPMetricsLoader_New();
  SearchResult result = SearchResult_New();

  void SetUp() override {
    RLookup_Seal(&lookup);
    // No parent: transforming a row must not access live query bookkeeping.
    metrics->upstream = &source;
  }
  void TearDown() override {
    SearchResult_Destroy(&result);
    metrics->Free(metrics);
    RLookup_Cleanup(&lookup);
  }
  void expectMetric(SearchResult *res, const RLookupKey *metricKey, double expected) {
    const RSValue *value = RLookupRow_Get(metricKey, SearchResult_GetRowData(res));
    ASSERT_NE(nullptr, value);
    EXPECT_DOUBLE_EQ(expected, RSValue_Number_Get(value));
  }
};

TEST_F(MetricsDrainTest, drainMatchesNextAndOwnsOutputValues) {
  for (auto *index : {source.nextIndex, source.drainIndex}) {
    ResultMetrics_Add(index, key, 1.25);
    ResultMetrics_Add(index, other, -2.5);
    ResultMetrics_Add(index, key, 3.75);
  }
  SearchResult next = SearchResult_New();
  ASSERT_EQ(RS_RESULT_OK, metrics->Next(metrics, &next));
  ASSERT_EQ(RP_DRAIN_OK, metrics->Drain(metrics, &result));
  for (auto *res : {&next, &result}) {
    expectMetric(res, key, 3.75);
    expectMetric(res, other, -2.5);
  }
  EXPECT_NE(RLookupRow_Get(key, SearchResult_GetRowData(&next)),
            RLookupRow_Get(key, SearchResult_GetRowData(&result)));
  ResultMetrics_Reset(source.nextIndex);
  SearchResult_Destroy(&next);
  expectMetric(&result, key, 3.75);
  EXPECT_TRUE(SearchResult_GetFlags(&result) & Result_OwnsIndexResult);
  SearchResult_Clear(&result);
  EXPECT_EQ(RP_DRAIN_EOF, metrics->Drain(metrics, &result));
}

TEST_F(MetricsDrainTest, emptyAndMissingPayloadPreserveTheRow) {
  for (bool missing : {false, true}) {
    SearchResult_Clear(&result);
    if (missing) {
      // The previous Drain transferred the empty payload to result, now cleared.
      EXPECT_EQ(nullptr, source.drainIndex);
      source.drainStatus = RP_DRAIN_OK;
    }
    SearchResult_SetDocId(&result, 42);
    SearchResult_SetScore(&result, 7);
    RLookup_WriteOwnKey(other, SearchResult_GetRowDataMut(&result), RSValue_NewNumber(9));
    ASSERT_EQ(RP_DRAIN_OK, metrics->Drain(metrics, &result));
    EXPECT_EQ(42, SearchResult_GetDocId(&result));
    EXPECT_EQ(7, SearchResult_GetScore(&result));
    expectMetric(&result, other, 9);
    EXPECT_EQ(nullptr, RLookupRow_Get(key, SearchResult_GetRowData(&result)));
  }
}

TEST_F(MetricsDrainTest, terminalStatusesDoNotTransformOrCallNext) {
  for (auto status : {RP_DRAIN_EOF, RP_DRAIN_ERROR}) {
    source.drainStatus = status;
    EXPECT_EQ(status, metrics->Drain(metrics, &result));
    EXPECT_EQ(nullptr, SearchResult_GetIndexResult(&result));
  }
  EXPECT_EQ(0, source.nextCalls);
  EXPECT_EQ(2, source.drainCalls);
  for (int status : {RS_RESULT_EOF, RS_RESULT_ERROR, RS_RESULT_TIMEDOUT, RS_RESULT_PAUSED}) {
    source.nextStatus = status;
    EXPECT_EQ(status, metrics->Next(metrics, &result));
    EXPECT_EQ(nullptr, SearchResult_GetIndexResult(&result));
  }
}

TEST_F(MetricsDrainTest, drainsWhileNextIsParkedWithoutSharingOutput) {
  ResultMetrics_Add(source.nextIndex, key, 11);
  ResultMetrics_Add(source.drainIndex, key, 22);
  source.release.store(false);
  SearchResult next = SearchResult_New();
  int status = RS_RESULT_MAX;
  std::thread worker([&] { status = metrics->Next(metrics, &next); });
  bool entered = RS::WaitForCondition([&] { return source.entered.load(); }, 5);
  if (entered) {
    EXPECT_EQ(RP_DRAIN_OK, metrics->Drain(metrics, &result));
    expectMetric(&result, key, 22);
  }
  source.release.store(true, std::memory_order_release);
  worker.join();
  EXPECT_TRUE(entered);
  EXPECT_EQ(RS_RESULT_OK, status);
  expectMetric(&next, key, 11);
  expectMetric(&result, key, 22);
  SearchResult_Destroy(&next);
}

// Each path creates its own values from immutable configuration.
struct VectorDrainSource : ResultProcessor {
  const RLookupKey *key = nullptr;
  double nextDistance = 0.5, drainDistance = 0.5;
  const char *text = nullptr;
  bool missing = false;
  int nextStatus = RS_RESULT_OK;
  RPDrainStatus drainStatus = RP_DRAIN_OK;
  size_t nextCalls = 0, drainCalls = 0;
  std::atomic<bool> entered{false}, release{true};

  void fill(SearchResult *res, double distance) const {
    SearchResult_SetDocId(res, 42);
    SearchResult_SetScore(res, -1);
    if (!missing) {
      RSValue *value =
          text ? RSValue_NewCopiedString(text, strlen(text)) : RSValue_NewNumber(distance);
      RLookup_WriteOwnKey(key, SearchResult_GetRowDataMut(res), value);
    }
  }
  VectorDrainSource() {
    *static_cast<ResultProcessor *>(this) = {};
    Next = [](ResultProcessor *base, SearchResult *res) -> int {
      auto *self = static_cast<VectorDrainSource *>(base);
      ++self->nextCalls;
      self->entered.store(true, std::memory_order_release);
      while (!self->release.load(std::memory_order_acquire)) std::this_thread::yield();
      if (self->nextStatus == RS_RESULT_OK) self->fill(res, self->nextDistance);
      return self->nextStatus;
    };
    Drain = [](ResultProcessor *base, SearchResult *res) {
      auto *self = static_cast<VectorDrainSource *>(base);
      ++self->drainCalls;
      if (self->drainStatus == RP_DRAIN_OK) self->fill(res, self->drainDistance);
      return self->drainStatus;
    };
  }
};

class VectorNormalizerDrainTest : public ::testing::Test {
 protected:
  RLookup lookup = RLookup_New();
  const RLookupKey *key = RLookup_GetKey_Write(&lookup, "distance", 0);
  VectorDrainSource source;
  ResultProcessor *normalizer = nullptr;
  SearchResult result = SearchResult_New();

  void SetUp() override {
    RLookup_Seal(&lookup);
    source.key = key;
    create(VectorNorm_L2);
  }
  void create(VectorNormFunction function) {
    if (normalizer) normalizer->Free(normalizer);
    normalizer = RPVectorNormalizer_New(function, key);
    normalizer->upstream = &source;
  }
  void TearDown() override {
    SearchResult_Destroy(&result);
    normalizer->Free(normalizer);
    RLookup_Cleanup(&lookup);
  }
  void expectScore(const SearchResult *res, double expected) {
    EXPECT_DOUBLE_EQ(expected, SearchResult_GetScore(res));
    const RSValue *value = RLookupRow_Get(key, SearchResult_GetRowData(res));
    ASSERT_NE(nullptr, value);
    EXPECT_DOUBLE_EQ(expected, RSValue_Number_Get(value));
    EXPECT_EQ(42, SearchResult_GetDocId(res));
  }
};

TEST_F(VectorNormalizerDrainTest, nextAndDrainUseTheSameFormulas) {
  const VectorNormFunction functions[] = {VectorNorm_L2, VectorNorm_IP, VectorNorm_Cosine};
  const double expected[] = {2.0 / 3.0, 0.75, 0.75};
  for (size_t i = 0; i < 3; ++i) {
    create(functions[i]);
    SearchResult next = SearchResult_New();
    ASSERT_EQ(RS_RESULT_OK, normalizer->Next(normalizer, &next));
    ASSERT_EQ(RP_DRAIN_OK, normalizer->Drain(normalizer, &result));
    expectScore(&next, expected[i]);
    expectScore(&result, expected[i]);
    SearchResult_Destroy(&next);
    SearchResult_Clear(&result);
  }
}

TEST_F(VectorNormalizerDrainTest, convertsStringsAndUsesZeroForInvalidOrMissingDistance) {
  for (const char *input : {"3", "not-a-number", ""}) {
    source.text = input;
    source.missing = *input == '\0';
    ASSERT_EQ(RP_DRAIN_OK, normalizer->Drain(normalizer, &result));
    expectScore(&result, *input == '3' ? 0.25 : 0);
    SearchResult_Clear(&result);
  }
}

TEST_F(VectorNormalizerDrainTest, forwardsTerminalStatusesWithoutTransforming) {
  for (auto status : {RP_DRAIN_EOF, RP_DRAIN_ERROR}) {
    source.drainStatus = status;
    SearchResult_SetScore(&result, -5);
    EXPECT_EQ(status, normalizer->Drain(normalizer, &result));
    EXPECT_EQ(-5, SearchResult_GetScore(&result));
    EXPECT_EQ(nullptr, RLookupRow_Get(key, SearchResult_GetRowData(&result)));
  }
  EXPECT_EQ(0, source.nextCalls);
  for (int status : {RS_RESULT_EOF, RS_RESULT_ERROR, RS_RESULT_TIMEDOUT, RS_RESULT_PAUSED}) {
    source.nextStatus = status;
    EXPECT_EQ(status, normalizer->Next(normalizer, &result));
    EXPECT_EQ(-5, SearchResult_GetScore(&result));
  }
}

TEST_F(VectorNormalizerDrainTest, parkedNextDoesNotBlockOrChangeDrainedScore) {
  source.nextDistance = 3;
  source.drainDistance = 7;
  source.release.store(false);
  SearchResult next = SearchResult_New();
  int status = RS_RESULT_MAX;
  std::thread worker([&] { status = normalizer->Next(normalizer, &next); });
  bool entered = RS::WaitForCondition([&] { return source.entered.load(); }, 5);
  if (entered) {
    EXPECT_EQ(RP_DRAIN_OK, normalizer->Drain(normalizer, &result));
    expectScore(&result, 0.125);
  }
  source.release.store(true, std::memory_order_release);
  worker.join();
  EXPECT_TRUE(entered);
  EXPECT_EQ(RS_RESULT_OK, status);
  expectScore(&next, 0.25);
  expectScore(&result, 0.125);
  SearchResult_Destroy(&next);
}

struct processor1Ctx : public ResultProcessor {
  processor1Ctx() {
    memset(static_cast<ResultProcessor *>(this), 0, sizeof(ResultProcessor));
    counter = 0;
  }
  int counter;
  RLookupKey *kout = NULL;
};

#define NUM_RESULTS 5

static int p1_Next(ResultProcessor *rp, SearchResult *res) {
  processor1Ctx *p = static_cast<processor1Ctx *>(rp);
  if (p->counter >= NUM_RESULTS) return RS_RESULT_EOF;

  SearchResult_SetDocId(res, ++p->counter);
  SearchResult_SetScore(res, (double)SearchResult_GetDocId(res));
  RLookup_WriteOwnKey(p->kout, SearchResult_GetRowDataMut(res),
                      RSValue_NewNumber(SearchResult_GetDocId(res)));
  return RS_RESULT_OK;
}

static RPDrainStatus p1_Drain(ResultProcessor *rp, SearchResult *res) {
  processor1Ctx *p = static_cast<processor1Ctx *>(rp);
  if (p->counter >= NUM_RESULTS) return RP_DRAIN_EOF;

  SearchResult_SetDocId(res, ++p->counter);
  return RP_DRAIN_OK;
}

static RPDrainStatus drainError(ResultProcessor *, SearchResult *) {
  return RP_DRAIN_ERROR;
}

static int p2_Next(ResultProcessor *rp, SearchResult *res) {
  int rc = rp->upstream->Next(rp->upstream, res);
  processor1Ctx *p = static_cast<processor1Ctx *>(rp);
  if (rc == RS_RESULT_EOF) return rc;
  rp->parent->totalResults++;
  return RS_RESULT_OK;
}

static int numFreed = 0;

static void resultProcessor_GenericFree(ResultProcessor *rp) {
  numFreed++;
  delete static_cast<processor1Ctx *>(rp);
}

class ResultProcessorTest : public ::testing::Test {};

struct BlockingQueryIterator {
  QueryIterator base = {};
  std::atomic_bool entered = false;
  std::atomic_bool release = false;
  bool yieldResult = false;

  explicit BlockingQueryIterator(bool yieldResult = false) : yieldResult(yieldResult) {
    if (yieldResult) {
      base.current = NewVirtualResult(1, RS_FIELDMASK_ALL);
      base.current->docId = base.lastDocId = 1;
    }
    base.Read = [](QueryIterator *base) {
      auto *self = reinterpret_cast<BlockingQueryIterator *>(base);
      self->entered.store(true, std::memory_order_release);
      while (!self->release.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      if (self->yieldResult) return ITERATOR_OK;
      base->atEOF = true;
      return ITERATOR_EOF;
    };
    base.Free = [](QueryIterator *base) {
      if (base->current) IndexResult_Free(base->current);
      delete reinterpret_cast<BlockingQueryIterator *>(base);
    };
  }
};

TEST_F(ResultProcessorTest, testProcessorChain) {
  QueryProcessingCtx qitr = {0};
  RLookup lk = RLookup_New();
  processor1Ctx *p = new processor1Ctx();
  p->counter = 0;
  p->Next = p1_Next;
  p->Free = resultProcessor_GenericFree;
  p->kout = RLookup_GetKey_Write(&lk, "foo", RLOOKUP_F_NOFLAGS);
  QITR_PushRP(&qitr, p);

  processor1Ctx *p2 = new processor1Ctx();
  p2->Next = p2_Next;
  p2->Free = resultProcessor_GenericFree;
  QITR_PushRP(&qitr, p2);

  size_t count = 0;
  SearchResult r = SearchResult_New();
  ResultProcessor *rpTail = qitr.endProc;
  while (rpTail->Next(rpTail, &r) == RS_RESULT_OK) {
    count++;
    ASSERT_EQ(count, SearchResult_GetDocId(&r));
    ASSERT_EQ(count, SearchResult_GetScore(&r));
    RSValue *v = RLookupRow_Get(p->kout, SearchResult_GetRowData(&r));
    ASSERT_TRUE(v != NULL);
    ASSERT_EQ(RSValueType_Number, RSValue_Type(v));
    ASSERT_EQ(count, RSValue_Number_Get(v));
    SearchResult_Clear(&r);
  }

  ASSERT_EQ(NUM_RESULTS, count);
  ASSERT_EQ(NUM_RESULTS, qitr.totalResults);
  SearchResult_Destroy(&r);

  numFreed = 0;
  QITR_FreeChain(&qitr);
  ASSERT_EQ(2, numFreed);
  RLookup_Cleanup(&lk);
}

TEST_F(ResultProcessorTest, drainCallsProcessorImplementationDirectly) {
  processor1Ctx processor;
  processor.Drain = p1_Drain;
  SearchResult result = SearchResult_New();
  for (t_docId expected = 1; expected <= NUM_RESULTS; ++expected) {
    ASSERT_EQ(RP_DRAIN_OK, processor.Drain(&processor, &result));
    ASSERT_EQ(expected, SearchResult_GetDocId(&result));
    SearchResult_Clear(&result);
  }
  ASSERT_EQ(RP_DRAIN_EOF, processor.Drain(&processor, &result));
  SearchResult_Destroy(&result);
}

TEST_F(ResultProcessorTest, pushInstallsEofDrainWhileProcessorIsNotMigrated) {
  QueryProcessingCtx qitr = {0};
  processor1Ctx processor;
  QITR_PushRP(&qitr, &processor);
  SearchResult result = SearchResult_New();
  ASSERT_NE(nullptr, processor.Drain);
  ASSERT_EQ(RP_DRAIN_EOF, processor.Drain(&processor, &result));
  SearchResult_Destroy(&result);
}

TEST_F(ResultProcessorTest, profileConstructorProvidesDrainWithoutChainInsertion) {
  QueryProcessingCtx qitr = {0};
  processor1Ctx source;
  source.Drain = p1_Drain;
  ResultProcessor *profile = RPProfile_New(&source, &qitr);
  SearchResult result = SearchResult_New();
  ASSERT_NE(nullptr, profile->Drain);
  EXPECT_EQ(RP_DRAIN_EOF, profile->Drain(profile, &result));
  EXPECT_EQ(0, RPProfile_GetCount(profile));
  profile->Free(profile);
  SearchResult_Destroy(&result);
}

TEST_F(ResultProcessorTest, drainPropagatesErrors) {
  processor1Ctx processor;
  processor.Drain = drainError;

  SearchResult result = SearchResult_New();
  ASSERT_EQ(RP_DRAIN_ERROR, processor.Drain(&processor, &result));
  SearchResult_Destroy(&result);
}

TEST_F(ResultProcessorTest, indexDrainDoesNotWaitForOrAdvanceNext) {
  IndexSpec spec = {0};
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(nullptr, &spec);
  QueryRequestTimeout timeout = {};
  QueryRequestTimeout_Init(&timeout, TimeoutPolicy_Return, 0);
  sctx.timeout = &timeout;
  sctx.lock_state = SPEC_LOCK_READ_BORROWED;

  auto *iterator = new BlockingQueryIterator();
  ResultProcessor *rp = RPQueryIterator_New(&iterator->base, nullptr, 0, &sctx);

  int nextStatus = RS_RESULT_MAX;
  std::thread nextThread([&]() {
    SearchResult nextResult = SearchResult_New();
    nextStatus = rp->Next(rp, &nextResult);
    SearchResult_Destroy(&nextResult);
  });

  const bool nextEntered =
      RS::WaitForCondition([&]() { return iterator->entered.load(std::memory_order_acquire); }, 5);

  SearchResult drainResult = SearchResult_New();
  RPDrainStatus firstDrain = RP_DRAIN_ERROR;
  RPDrainStatus secondDrain = RP_DRAIN_ERROR;
  if (nextEntered) {
    firstDrain = rp->Drain(rp, &drainResult);
    secondDrain = rp->Drain(rp, &drainResult);
    EXPECT_FALSE(iterator->release.load(std::memory_order_relaxed));
  }
  SearchResult_Destroy(&drainResult);

  iterator->release.store(true, std::memory_order_release);
  nextThread.join();
  ASSERT_TRUE(nextEntered);
  ASSERT_EQ(RP_DRAIN_EOF, firstDrain);
  ASSERT_EQ(RP_DRAIN_EOF, secondDrain);
  ASSERT_EQ(RS_RESULT_EOF, nextStatus);
  rp->Free(rp);
}

TEST_F(ResultProcessorTest, indexDrainLeavesSuccessfulInFlightResultOwnedByNext) {
  IndexSpec spec = {};
  spec.docs = DocTable_New(1);
  auto *dmd =
      DocTable_Put(&spec.docs, "late", 4, 1, Document_DefaultFlags, nullptr, 0, DocumentType_Hash);
  DMD_Return(dmd);  // Keep only the table's reference before Next borrows the document.
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(nullptr, &spec);
  QueryRequestTimeout timeout = {};
  QueryRequestTimeout_Init(&timeout, TimeoutPolicy_ReturnStrict, 1000);
  QueryRequestTimeout_BeginCycle(&timeout, QUERY_REQUEST_TIMEOUT_BLOCKED_CLIENT);
  sctx.timeout = &timeout;
  sctx.lock_state = SPEC_LOCK_READ_BORROWED;
  QueryProcessingCtx qctx = {};
  auto *iterator = new BlockingQueryIterator(true);
  ResultProcessor *rp = RPQueryIterator_New(&iterator->base, nullptr, 0, &sctx);
  rp->parent = &qctx;
  SearchResult next = SearchResult_New(), drained = SearchResult_New();
  int status = RS_RESULT_MAX;
  std::thread worker([&] { status = rp->Next(rp, &next); });
  const bool entered =
      RS::WaitForCondition([&] { return iterator->entered.load(std::memory_order_acquire); }, 5);
  QueryRequestTimeout_MarkTimedOut(&timeout);
  EXPECT_EQ(RP_DRAIN_EOF, rp->Drain(rp, &drained));
  EXPECT_EQ(RP_DRAIN_EOF, rp->Drain(rp, &drained));
  iterator->release.store(true, std::memory_order_release);
  worker.join();
  ASSERT_TRUE(entered);
  EXPECT_EQ(RS_RESULT_OK, status);
  EXPECT_EQ(1, SearchResult_GetDocId(&next));
  EXPECT_EQ(dmd, SearchResult_GetDocumentMetadata(&next));
  EXPECT_EQ(iterator->base.current, SearchResult_GetIndexResult(&next));
  EXPECT_EQ(2, dmd->ref_count);
  EXPECT_EQ(RP_DRAIN_EOF, rp->Drain(rp, &drained));
  SearchResult_Destroy(&next);
  EXPECT_EQ(1, dmd->ref_count);
  SearchResult_Destroy(&drained);
  rp->Free(rp);
  DocTable_Free(&spec.docs);
}

/*
 * Test SearchResult_mergeFlags function with no flags set
 */
TEST_F(ResultProcessorTest, testmergeFlags_NoFlags) {
  SearchResult a = SearchResult_New();
  SearchResult b = SearchResult_New();

  // Test merging no flags
  SearchResult_MergeFlags(&a, &b);
  EXPECT_EQ(SearchResult_GetFlags(&a), 0);
}

/*
 * Test SearchResult_mergeFlags function with Result_ExpiredDoc flag
 */
TEST_F(ResultProcessorTest, testmergeFlags_ExpiredDoc) {
  SearchResult a = SearchResult_New();
  SearchResult b = SearchResult_New();
  SearchResult_SetFlags(&b, Result_ExpiredDoc);  // Source has expired flag

  // Test merging expired flag
  SearchResult_MergeFlags(&a, &b);
  EXPECT_TRUE(SearchResult_GetFlags(&a) & Result_ExpiredDoc);
}

/*
 * Test that SearchResult_MergeFlags does NOT propagate the ownership flag
 * `Result_OwnsIndexResult` from `other` into `res`. This flag is a per-result
 * memory-management property (it tracks whether *this* result's `_index_result`
 * was deep-copied and therefore must be freed by `SearchResult_Clear`).
 * Inheriting it from a sibling would cause `clear()` to free a borrowed
 * (or NULL) pointer.
 */
TEST_F(ResultProcessorTest, testmergeFlags_OwnsIndexResultNotPropagated) {
  SearchResult a = SearchResult_New();
  SearchResult b = SearchResult_New();
  // `b` "owns" its index result; `a` does not.
  SearchResult_SetFlags(&b, Result_OwnsIndexResult | Result_ExpiredDoc);

  SearchResult_MergeFlags(&a, &b);

  // Document-semantic flag should propagate.
  EXPECT_TRUE(SearchResult_GetFlags(&a) & Result_ExpiredDoc);
  // Ownership flag must NOT propagate — `a` did not perform a deep copy.
  EXPECT_FALSE(SearchResult_GetFlags(&a) & Result_OwnsIndexResult);
  // `b`'s flags must be left untouched by the merge.
  EXPECT_TRUE(SearchResult_GetFlags(&b) & Result_OwnsIndexResult);
}

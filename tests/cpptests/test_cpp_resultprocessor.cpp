/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

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

TEST_F(LoaderDrainTest, promotionToSafeLoaderDoesNotEnableItsDrain) {
  source.documents = {document("drain:safe", "safe")};
  create();
  SetLoadersForBG(&qctx);
  loader = qctx.endProc;
  EXPECT_EQ(RP_SAFE_LOADER, loader->type);
  EXPECT_EQ(RP_DRAIN_EOF, loader->Drain(loader, &result));
  EXPECT_EQ(0, source.position);
}

TEST_F(LoaderDrainTest, constructedSafeLoaderKeepsDefaultDrain) {
  source.documents = {document("drain:safe", "safe")};
  create(false, QEXEC_F_RUN_IN_BACKGROUND);
  EXPECT_EQ(RP_SAFE_LOADER, loader->type);
  EXPECT_EQ(RP_DRAIN_EOF, loader->Drain(loader, &result));
  EXPECT_EQ(0, source.position);
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

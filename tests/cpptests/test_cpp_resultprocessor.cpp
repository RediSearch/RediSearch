/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/


#include "result_processor.h"
#include "common.h"
#include "query.h"
#include "value_ffi.h"
#include "gtest/gtest.h"
#include "search_result_ffi.h"
#include "search_result.h"
#include "spec.h"

#include <atomic>
#include <thread>

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
  RLookup_WriteOwnKey(p->kout, SearchResult_GetRowDataMut(res), RSValue_NewNumber(SearchResult_GetDocId(res)));
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

  BlockingQueryIterator() {
    base.Read = [](QueryIterator *base) {
      auto *self = reinterpret_cast<BlockingQueryIterator *>(base);
      self->entered.store(true, std::memory_order_release);
      while (!self->release.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      base->atEOF = true;
      return ITERATOR_EOF;
    };
    base.Free = [](QueryIterator *base) { delete reinterpret_cast<BlockingQueryIterator *>(base); };
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

TEST_F(ResultProcessorTest, drainSkipsProcessorsWithoutAnImplementation) {
  processor1Ctx source;
  source.Drain = p1_Drain;

  processor1Ctx transparent;
  transparent.upstream = &source;

  SearchResult result = SearchResult_New();
  for (t_docId expected = 1; expected <= NUM_RESULTS; ++expected) {
    ASSERT_EQ(RP_DRAIN_OK, ResultProcessor_Drain(&transparent, &result));
    ASSERT_EQ(expected, SearchResult_GetDocId(&result));
    SearchResult_Clear(&result);
  }
  ASSERT_EQ(RP_DRAIN_EOF, ResultProcessor_Drain(&transparent, &result));
  SearchResult_Destroy(&result);
}

TEST_F(ResultProcessorTest, drainReturnsEofWhenNoProcessorImplementsIt) {
  processor1Ctx source;
  processor1Ctx transparent;
  transparent.upstream = &source;

  SearchResult result = SearchResult_New();
  ASSERT_EQ(RP_DRAIN_EOF, ResultProcessor_Drain(&transparent, &result));
  SearchResult_Destroy(&result);
}

TEST_F(ResultProcessorTest, drainPropagatesErrors) {
  processor1Ctx processor;
  processor.Drain = drainError;

  SearchResult result = SearchResult_New();
  ASSERT_EQ(RP_DRAIN_ERROR, ResultProcessor_Drain(&processor, &result));
  SearchResult_Destroy(&result);
}

TEST_F(ResultProcessorTest, indexDrainDoesNotWaitForOrAdvanceNext) {
  IndexSpec spec = {0};
  RedisSearchCtx sctx = SEARCH_CTX_STATIC(nullptr, &spec);
  sctx.time.skipTimeoutChecks = true;
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
    firstDrain = ResultProcessor_Drain(rp, &drainResult);
    secondDrain = ResultProcessor_Drain(rp, &drainResult);
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
  SearchResult_SetFlags(&b, Result_ExpiredDoc); // Source has expired flag

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

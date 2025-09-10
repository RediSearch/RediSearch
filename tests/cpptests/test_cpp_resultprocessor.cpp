/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/


#include "result_processor.h"
#include "query.h"
#include "value.h"
#include "gtest/gtest.h"
#include "search_result_rs.h"

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

TEST_F(ResultProcessorTest, testProcessorChain) {
  QueryProcessingCtx qitr = {0};
  RLookup lk = {0};
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
    RSValue *v = RLookup_GetItem(p->kout, SearchResult_GetRowData(&r));
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

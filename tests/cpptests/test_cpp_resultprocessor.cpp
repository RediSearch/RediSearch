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
#include "gtest/gtest.h"
#include <thread>
#include <chrono>

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

  res->docId = ++p->counter;
  res->score = (double)res->docId;
  RLookup_WriteOwnKey(p->kout, &res->rowdata, RS_NumVal(res->docId));
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
  QueryIterator qitr = {0};
  RLookup lk = {0};
  processor1Ctx *p = new processor1Ctx();
  p->counter = 0;
  p->Next = p1_Next;
  p->Free = resultProcessor_GenericFree;
  p->kout = RLookup_GetKey(&lk, "foo", RLOOKUP_M_WRITE, RLOOKUP_F_NOFLAGS);
  QITR_PushRP(&qitr, p);

  processor1Ctx *p2 = new processor1Ctx();
  p2->Next = p2_Next;
  p2->Free = resultProcessor_GenericFree;
  QITR_PushRP(&qitr, p2);

  size_t count = 0;
  SearchResult r = {0};
  ResultProcessor *rpTail = qitr.endProc;
  while (rpTail->Next(rpTail, &r) == RS_RESULT_OK) {
    count++;
    ASSERT_EQ(count, r.docId);
    ASSERT_EQ(count, r.score);
    RSValue *v = RLookup_GetItem(p->kout, &r.rowdata);
    ASSERT_TRUE(v != NULL);
    ASSERT_EQ(RSValue_Number, v->t);
    ASSERT_EQ(count, v->numval);
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

// Old depleter tests removed - new depleter works differently

// Old depleter timeout test removed

// Old depleter long run test removed

// Old depleter error test removed

TEST_F(ResultProcessorTest, RPDepleter_RegisterChild) {
  // Test the new depleter functionality with child registration
  const size_t n_docs1 = 3;
  const size_t n_docs2 = 2;

  // Mock upstream processors with some delay to simulate real work
  struct MockUpstream1 : public ResultProcessor {
    int count = 0;
    static int NextFn(ResultProcessor *rp, SearchResult *res) {
      MockUpstream1 *self = (MockUpstream1 *)rp;
      if (self->count >= n_docs1) return RS_RESULT_EOF;
      res->docId = 100 + (++self->count); // Use 100+ range for child1
      return RS_RESULT_OK;
    }
    MockUpstream1() {
      memset(this, 0, sizeof(*this));
      this->Next = NextFn;
    }
  } mockUpstream1;

  struct MockUpstream2 : public ResultProcessor {
    int count = 0;
    static int NextFn(ResultProcessor *rp, SearchResult *res) {
      MockUpstream2 *self = (MockUpstream2 *)rp;
      if (self->count >= n_docs2) return RS_RESULT_EOF;
      res->docId = 200 + (++self->count); // Use 200+ range for child2
      return RS_RESULT_OK;
    }
    MockUpstream2() {
      memset(this, 0, sizeof(*this));
      this->Next = NextFn;
    }
  } mockUpstream2;

  // Create the depleter
  StrongRef depleter_ref = Depleter_New();
  ASSERT_NE(StrongRef_Get(depleter_ref), nullptr);

  // Register children and get RPDepleterFutures
  ResultProcessor *future1 = Depleter_RegisterChild(depleter_ref, &mockUpstream1);
  ResultProcessor *future2 = Depleter_RegisterChild(depleter_ref, &mockUpstream2);

  ASSERT_NE(future1, nullptr);
  ASSERT_NE(future2, nullptr);

  SearchResult res = {0};
  int rc;

  // Test future1
  int depletingCount1 = 0;
  int maxDepletingChecks = 100; // Limit to avoid infinite loop
  while ((rc = future1->Next(future1, &res)) == RS_RESULT_DEPLETING && depletingCount1 < maxDepletingChecks) {
    depletingCount1++;
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }



  ASSERT_GT(depletingCount1, 0);

  int resultCount1 = 0;
  do {
    if (rc == RS_RESULT_OK) {
      ASSERT_GE(res.docId, 101u);
      ASSERT_LE(res.docId, 103u);
      resultCount1++;
      SearchResult_Clear(&res);
    }
  } while ((rc = future1->Next(future1, &res)) == RS_RESULT_OK);

  ASSERT_EQ(resultCount1, n_docs1);
  ASSERT_EQ(rc, RS_RESULT_EOF);

  // Test future2
  int depletingCount2 = 0;
  while ((rc = future2->Next(future2, &res)) == RS_RESULT_DEPLETING) {
    depletingCount2++;
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  int resultCount2 = 0;
  do {
    if (rc == RS_RESULT_OK) {
      ASSERT_GE(res.docId, 201u);
      ASSERT_LE(res.docId, 202u);
      resultCount2++;
      SearchResult_Clear(&res);
    }
  } while ((rc = future2->Next(future2, &res)) == RS_RESULT_OK);

  ASSERT_EQ(resultCount2, n_docs2);
  ASSERT_EQ(rc, RS_RESULT_EOF);

  // Cleanup
  SearchResult_Destroy(&res);
  future1->Free(future1);
  future2->Free(future2);
  StrongRef_Release(depleter_ref);
}

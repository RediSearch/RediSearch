#include <result_processor.h>
#include <query.h>
#include <gtest/gtest.h>

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
  p->kout = RLookup_GetKey(&lk, "foo", RLOOKUP_F_OCREAT);
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
#include "test_util.h"
#include <result_processor.h>
#include <query.h>

struct processor1Ctx {
  int counter;
};

#define NUM_RESULTS 5

int p1_Next(ResultProcessorCtx *ctx, SearchResult *res) {

  struct processor1Ctx *p = ctx->privdata;
  if (p->counter >= NUM_RESULTS) return RS_RESULT_EOF;

  res->docId = ++p->counter;
  res->score = (double)res->docId;
  RSFieldMap_Set(&res->fields, "foo", RS_NumVal(res->docId));

  return RS_RESULT_OK;
}

int p2_Next(ResultProcessorCtx *ctx, SearchResult *res) {

  int rc = ResultProcessor_Next(ctx->upstream, res, 0);
  if (rc == RS_RESULT_EOF) return rc;

  RSFieldMap_Set(&res->fields, "bar", RS_NumVal(1337));
  ctx->qxc->errorString = "Foo";
  ctx->qxc->totalResults++;

  return RS_RESULT_OK;
}

static int numFreed = 0;

static void resultProcessor_GenericFree(ResultProcessor *rp) {
  free(rp->ctx.privdata);
  free(rp);
  numFreed++;
}

int testProcessorChain() {

  QueryProcessingCtx pc = {};

  struct processor1Ctx *p = malloc(sizeof(*p));
  p->counter = 0;
  ResultProcessor *p1 = NewResultProcessor(NULL, p);
  p1->ctx.qxc = &pc;
  ASSERT(p1->ctx.privdata == p);
  ASSERT(p1->ctx.qxc == &pc);
  ASSERT(p1->ctx.upstream == NULL);

  p1->Next = p1_Next;
  p1->Free = resultProcessor_GenericFree;

  ResultProcessor *p2 = NewResultProcessor(p1, NULL);
  ASSERT(p2->ctx.privdata == NULL);
  ASSERT(p2->ctx.qxc == &pc);
  ASSERT(p2->ctx.upstream == p1);
  p2->Next = p2_Next;
  p2->Free = resultProcessor_GenericFree;

  int count = 0;

  SearchResult *r = NewSearchResult();
  ASSERT(r != NULL);
  while (RS_RESULT_EOF != ResultProcessor_Next(p2, r, 0)) {
    count++;
    ASSERT_EQUAL(count, r->docId);
    ASSERT_EQUAL(count, r->score);
    RSValue *v = RSFieldMap_Get(r->fields, "foo");
    ASSERT(v != NULL);
    ASSERT_EQUAL(RSValue_Number, v->t);
    ASSERT_EQUAL(count, v->numval);
  }
  ASSERT_EQUAL(NUM_RESULTS, count);
  ASSERT_EQUAL(NUM_RESULTS, pc.totalResults);
  ASSERT_STRING_EQ("Foo", pc.errorString);
  SearchResult_Free(r);

  numFreed = 0;
  ResultProcessor_Free(p2);
  ASSERT_EQUAL(2, numFreed);

  RETURN_TEST_SUCCESS;
}

TEST_MAIN({ TESTFUNC(testProcessorChain); })
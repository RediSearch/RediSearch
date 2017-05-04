#include "test_util.h"
#include "../extension.h"
#include "../rmutil/alloc.h"
#include "../redisearch.h"
#include "../query.h"
#include "../stopwords.h"
#include "../ext/default.h"

struct privdata {
  int freed;
};

/* Calculate sum(TF-IDF)*document score for each result */
double myScorer(RSScoringFunctionCtx *ctx, RSIndexResult *h, RSDocumentMetadata *dmd,
                double minScore) {
  return 3.141;
}

void myExpander(RSQueryExpanderCtx *ctx, RSToken *token) {
  ctx->ExpandToken(ctx, strdup("foo"), 3, 0x00ff);
}

int numFreed = 0;
void myFreeFunc(void *p) {
  // printf("Freeing %p\n", p);
  numFreed++;
  free(p);
}

/* Register the default extension */
int myRegisterFunc(RSExtensionCtx *ctx) {

  struct privdata *spd = malloc(sizeof(struct privdata));
  spd->freed = 0;
  if (ctx->RegisterScoringFunction("myScorer", myScorer, myFreeFunc, spd) == REDISEARCH_ERR) {
    return REDISEARCH_ERR;
  }

  spd = malloc(sizeof(struct privdata));
  spd->freed = 0;
  /* Snowball Stemmer is the default expander */
  if (ctx->RegisterQueryExpander("myExpander", myExpander, myFreeFunc, spd) == REDISEARCH_ERR) {
    return REDISEARCH_ERR;
  }

  return REDISEARCH_OK;
}

int testExtenionRegistration() {

  Extensions_Init();
  numFreed = 0;
  ASSERT(REDISEARCH_OK == Extension_Load("testung", myRegisterFunc));

  RSQueryExpanderCtx qexp;
  ExtQueryExpanderCtx *qx = Extensions_GetQueryExpander(&qexp, "myExpander");
  ASSERT(qx != NULL);
  ASSERT(qx->exp == myExpander);
  ASSERT(qx->ff == myFreeFunc);
  ASSERT(qx->privdata != NULL);
  ASSERT(qexp.privdata == qx->privdata);
  qx->ff(qx->privdata);
  ASSERT_EQUAL(1, numFreed);
  // verify case sensitivity and null on not-found
  ASSERT(NULL == Extensions_GetQueryExpander(&qexp, "MYEXPANDER"));

  RSScoringFunctionCtx scxp;
  ExtScoringFunctionCtx *sx = Extensions_GetScoringFunction(&scxp, "myScorer");
  ASSERT(sx != NULL);
  ASSERT(sx->privdata = scxp.privdata);
  ASSERT(sx->ff = myFreeFunc);
  ASSERT(sx->sf = myScorer);
  sx->ff(sx->privdata);
  ASSERT_EQUAL(2, numFreed);
  ASSERT(NULL == Extensions_GetScoringFunction(&scxp, "MYScorer"));
  return 0;
}

int testDynamicLoading() {
  Extensions_Init();

  char *errMsg = NULL;
  int rc = Extension_LoadDynamic("./ext-example/example.so", &errMsg);
  ASSERT_EQUAL(rc, REDISMODULE_OK);
  if (errMsg != NULL) {
    FAIL("Error loading extension: %s", errMsg);
  }

  RSScoringFunctionCtx scxp;
  ExtScoringFunctionCtx *sx = Extensions_GetScoringFunction(&scxp, "example_scorer");
  ASSERT(sx != NULL);

  RSQueryExpanderCtx qxcp;
  ExtQueryExpanderCtx *qx = Extensions_GetQueryExpander(&qxcp, "example_expander");
  ASSERT(qx != NULL)
  return 0;
}

int testQueryExpander() {
  Extensions_Init();
  numFreed = 0;
  ASSERT(REDISEARCH_OK == Extension_Load("testung", myRegisterFunc));

  const char *qt = "hello world";
  char *err = NULL;

  Query *q = NewQuery(NULL, qt, strlen(qt), 0, 1, 0xff, 0, "en", DEFAULT_STOPWORDS, "myExpander",
                      -1, 0, "myScorer", (RSPayload){}, NULL);

  ASSERT(q->expander = myExpander);
  ASSERT(q->expanderFree = myFreeFunc);
  ASSERT(q->expCtx.privdata != NULL);
  QueryNode *n = Query_Parse(q, &err);

  if (err) FAIL("Error parsing query: %s", err);

  ASSERT_EQUAL(q->numTokens, 2)
  Query_Expand(q);
  //__queryNode_Print(n, 0);
  ASSERT_EQUAL(q->numTokens, 4)

  ASSERT(n->pn.children[0]->type == QN_UNION);
  ASSERT_STRING_EQ("hello", n->pn.children[0]->un.children[0]->tn.str);
  ASSERT(n->pn.children[0]->un.children[0]->tn.expanded == 0)
  ASSERT_STRING_EQ("foo", n->pn.children[0]->un.children[1]->tn.str);
  ASSERT_EQUAL(0x00FF, n->pn.children[0]->un.children[1]->tn.flags);

  ASSERT(n->pn.children[0]->un.children[1]->tn.expanded != 0);

  ASSERT(n->pn.children[1]->type == QN_UNION);
  ASSERT_STRING_EQ("world", n->pn.children[1]->un.children[0]->tn.str);
  ASSERT_STRING_EQ("foo", n->pn.children[1]->un.children[1]->tn.str);

  RSQueryTerm *qtr = NewTerm(&n->pn.children[1]->un.children[1]->tn);
  ASSERT_STRING_EQ(qtr->str, n->pn.children[1]->un.children[1]->tn.str);
  ASSERT_EQUAL(0x00FF, qtr->flags);

  Term_Free(qtr);

  Query_Free(q);
  ASSERT_EQUAL(2, numFreed);
  RETURN_TEST_SUCCESS;
}

TEST_MAIN({
  RMUTil_InitAlloc();
  TESTFUNC(testExtenionRegistration);
  TESTFUNC(testQueryExpander);
  TESTFUNC(testDynamicLoading);
});
#include "../extension.h"
#include "../redisearch.h"
#include "../query.h"
#include "../stopwords.h"
#include "../ext/default.h"
#include <gtest/gtest.h>

int myRegisterFunc(RSExtensionCtx *ctx);

class ExtTest : public ::testing::Test {
 protected:
  virtual void SetUp(void) {
    Extensions_Init();
    Extension_Load("testung", myRegisterFunc);
  }

  virtual void TearDown(void) {
  }
};

static const char *getExtensionPath(void) {
  const char *extPath = getenv("EXT_TEST_PATH");
  if (extPath == NULL || *extPath == 0) {
#ifdef EXT_TEST_PATH
    extPath = EXT_TEST_PATH;
#else
    extPath = "./src/ext-example/example.so";
#endif
  }
  return extPath;
}

/* Calculate sum(TF-IDF)*document score for each result */
static double myScorer(const ScoringFunctionArgs *ctx, const RSIndexResult *h,
                       const RSDocumentMetadata *dmd, double minScore) {
  return 3.141;
}

static int myExpander(RSQueryExpanderCtx *ctx, RSToken *token) {
  ctx->ExpandToken(ctx, strdup("foo"), 3, 0x00ff);
  return REDISMODULE_OK;
}

static int numFreed = 0;
void myFreeFunc(void *p) {
  numFreed++;
  // printf("Freeing %p %d\n", p, numFreed);
  free(p);
}

#define SCORER_NAME "myScorer_" __FILE__
#define EXPANDER_NAME "myExpander_" __FILE__
#define EXTENSION_NAME "testung_" __FILE__

/* Register the default extension */
int myRegisterFunc(RSExtensionCtx *ctx) {
  if (ctx->RegisterScoringFunction(SCORER_NAME, myScorer, myFreeFunc, NULL) == REDISEARCH_ERR) {
    return REDISEARCH_ERR;
  }

  /* Snowball Stemmer is the default expander */
  if (ctx->RegisterQueryExpander(EXPANDER_NAME, myExpander, myFreeFunc, NULL) == REDISEARCH_ERR) {
    return REDISEARCH_ERR;
  }

  return REDISEARCH_OK;
}

TEST_F(ExtTest, testRegistration) {
  numFreed = 0;

  RSQueryExpanderCtx qexp;
  ExtQueryExpanderCtx *qx = Extensions_GetQueryExpander(&qexp, EXPANDER_NAME);
  ASSERT_TRUE(qx != NULL);
  ASSERT_TRUE(qx->exp == myExpander);
  ASSERT_TRUE(qx->ff == myFreeFunc);
  ASSERT_TRUE(qexp.privdata == qx->privdata);
  qx->ff(qx->privdata);
  ASSERT_EQ(1, numFreed);
  // verify case sensitivity and null on not-found

  std::string ucExpander(EXPANDER_NAME);
  std::transform(ucExpander.begin(), ucExpander.end(), ucExpander.begin(), toupper);
  ASSERT_TRUE(NULL == Extensions_GetQueryExpander(&qexp, ucExpander.c_str()));

  ScoringFunctionArgs scxp;
  ExtScoringFunctionCtx *sx = Extensions_GetScoringFunction(&scxp, SCORER_NAME);
  ASSERT_TRUE(sx != NULL);
  ASSERT_EQ(sx->privdata, scxp.extdata);
  ASSERT_TRUE(sx->ff == myFreeFunc);
  ASSERT_TRUE(sx->sf == myScorer);
  sx->ff(sx->privdata);
  ASSERT_EQ(2, numFreed);
  std::string ucScorer(SCORER_NAME);
  std::transform(ucScorer.begin(), ucScorer.end(), ucScorer.begin(), toupper);
  ASSERT_TRUE(NULL == Extensions_GetScoringFunction(&scxp, ucScorer.c_str()));
}

TEST_F(ExtTest, testDynamicLoading) {
  char *errMsg = NULL;
  int rc = Extension_LoadDynamic(getExtensionPath(), &errMsg);
  ASSERT_EQ(rc, REDISMODULE_OK);
  if (errMsg != NULL) {
    FAIL() << "Error loading extension: " << errMsg;
  }

  ScoringFunctionArgs scxp;
  ExtScoringFunctionCtx *sx = Extensions_GetScoringFunction(&scxp, "example_scorer");
  ASSERT_TRUE(sx != NULL);

  RSQueryExpanderCtx qxcp;
  ExtQueryExpanderCtx *qx = Extensions_GetQueryExpander(&qxcp, "example_expander");
  ASSERT_TRUE(qx != NULL);
}

TEST_F(ExtTest, testQueryExpander) {
  numFreed = 0;

  const char *qt = "hello world";
  RSSearchOptions opts = {0};
  opts.fieldmask = RS_FIELDMASK_ALL;
  opts.flags = RS_DEFAULT_QUERY_FLAGS;
  opts.language = "en";
  opts.expanderName = EXPANDER_NAME;
  opts.scorerName = SCORER_NAME;
  QueryAST qast = {0};

  QueryError err = {QUERY_OK};
  int rc = QAST_Parse(&qast, NULL, &opts, qt, strlen(qt), &err);
  ASSERT_EQ(REDISMODULE_OK, rc) << QueryError_GetError(&err);

  ASSERT_EQ(qast.numTokens, 2);
  ASSERT_EQ(REDISMODULE_OK, QAST_Expand(&qast, opts.expanderName, &opts, NULL, &err));
  ASSERT_EQ(qast.numTokens, 4);

  QueryNode *n = qast.root;
  ASSERT_EQ(QN_UNION, n->children[0]->type);
  ASSERT_STREQ("hello", n->children[0]->children[0]->tn.str);
  ASSERT_EQ(0, n->children[0]->children[0]->tn.expanded);
  ASSERT_STREQ("foo", n->children[0]->children[1]->tn.str);
  ASSERT_EQ(0x00FF, n->children[0]->children[1]->tn.flags);

  ASSERT_NE(0, n->children[0]->children[1]->tn.expanded);

  ASSERT_EQ(QN_UNION, n->children[1]->type);
  ASSERT_STREQ("world", n->children[1]->children[0]->tn.str);
  ASSERT_STREQ("foo", n->children[1]->children[1]->tn.str);

  RSQueryTerm *qtr = NewQueryTerm(&n->children[1]->children[1]->tn, 1);
  ASSERT_STREQ(qtr->str, n->children[1]->children[1]->tn.str);
  ASSERT_EQ(0x00FF, qtr->flags);

  Term_Free(qtr);
  QAST_Destroy(&qast);
  ASSERT_EQ(1, numFreed);
}

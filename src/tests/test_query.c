#include "../expander.h"
#include "../query.h"
#include "../query_parser/tokenizer.h"
#include "../stopwords.h"
#include "test_util.h"
#include "time_sample.h"
#include <stdio.h>
#include "../rmutil/alloc.h"

void __queryNode_Print(QueryNode *qs, int depth);

int isValidQuery(char *qt) {
  char *err = NULL;
  RedisSearchCtx ctx;
  Query *q = NewQuery(NULL, qt, strlen(qt), 0, 1, 0xff, 0, "en", DEFAULT_STOPWORDS, NULL);

  QueryNode *n = Query_Parse(q, &err);

  if (err) {
    FAIL("Error parsing query '%s': %s", qt, err);
  }
  ASSERT(n != NULL);
  __queryNode_Print(n, 0);
  Query_Free(q);
  return 0;
}

#define assertValidQuery(qt)              \
  {                                       \
    if (0 != isValidQuery(qt)) return -1; \
  }

#define assertInvalidQuery(qt)            \
  {                                       \
    if (0 == isValidQuery(qt)) return -1; \
  }

int testQueryParser() {
  // test some valid queries
  assertValidQuery("hello");
  assertValidQuery("hello world");
  assertValidQuery("hello (world)");
  assertValidQuery("\"hello world\"");
  assertValidQuery("\"hello world\" \"foo bar\"");
  assertValidQuery("hello \"foo bar\" world");
  assertValidQuery("hello|hallo|yellow world");
  assertValidQuery("(hello|world|foo) bar baz");
  assertValidQuery("(hello|world|foo) (bar baz)");
  assertValidQuery("(hello world|foo \"bar baz\") \"bar baz\" bbbb");
  assertValidQuery("(barack|barrack) obama");
  // assertValidQuery("(hello world)|(goodbye moon)");

  assertInvalidQuery("(foo");
  assertInvalidQuery("\"foo");
  assertInvalidQuery("");
  assertInvalidQuery("()");

  char *err = NULL;
  char *qt = "(hello|world) and \"another world\" (foo is bar) baz ";
  RedisSearchCtx ctx;
  Query *q = NewQuery(NULL, qt, strlen(qt), 0, 1, 0xff, 0, "zz", DEFAULT_STOPWORDS, NULL);

  QueryNode *n = Query_Parse(q, &err);

  if (err) FAIL("Error parsing query: %s", err);
  __queryNode_Print(n, 0);
  ASSERT(err == NULL);
  ASSERT(n != NULL);
  ASSERT(n->type == QN_PHRASE);
  ASSERT(n->pn.exact == 0);
  ASSERT(n->pn.numChildren == 4);

  ASSERT(n->pn.children[0]->type == QN_UNION);
  ASSERT_STRING_EQ("hello", n->pn.children[0]->un.children[0]->tn.str);
  ASSERT_STRING_EQ("world", n->pn.children[0]->un.children[1]->tn.str);

  ASSERT(n->pn.children[1]->type == QN_PHRASE);
  ASSERT(n->pn.children[1]->pn.exact == 1);
  ASSERT_STRING_EQ("another", n->pn.children[1]->pn.children[0]->tn.str);
  ASSERT_STRING_EQ("world", n->pn.children[1]->pn.children[1]->tn.str);

  ASSERT(n->pn.children[2]->type == QN_PHRASE);
  ASSERT_STRING_EQ("foo", n->pn.children[2]->pn.children[0]->tn.str);
  ASSERT_STRING_EQ("bar", n->pn.children[2]->pn.children[1]->tn.str);

  ASSERT(n->pn.children[3]->type == QN_TOKEN);
  ASSERT_STRING_EQ("baz", n->pn.children[3]->tn.str);
  Query_Free(q);
  return 0;
}

// a dummy expander that expands all tokens to TOKEN or "foo"
QueryNode *dummyExpander(void *ctx, Query *q, QueryNode *n) {
  QueryNode *ret = NULL;
  if (n->type == QN_TOKEN) {
    ret = NewUnionNode();
    // Add the token and the ste as the union's children

    int *md = malloc(sizeof(int));
    *md = 1337;
    QueryUnionNode_AddChild(&ret->un, n);
    QueryUnionNode_AddChild(&ret->un, NewTokenNodeMetadata(q, strdup("foo"), 3, md));
  }

  return ret;
}

int testQueryExpander() {

  RegisterQueryExpander("dummy",
                        (QueryExpander){.Expand = dummyExpander, .Free = NULL, .ctx = NULL});

  QueryExpander *e = GetQueryExpander("dummy");
  ASSERT(e != NULL);
  ASSERT(NULL == GetQueryExpander("return null"));

  // now create some query
  char *err = NULL;
  char *qt = "hello world";

  Query *q = NewQuery(NULL, qt, strlen(qt), 0, 1, 0xff, 0, "zz", DEFAULT_STOPWORDS, "dummy");
  QueryNode *n = Query_Parse(q, &err);

  if (err) FAIL("Error parsing query: %s", err);

  ASSERT_EQUAL(q->numTokens, 2)
  Query_Expand(q);
  __queryNode_Print(n, 0);
  ASSERT_EQUAL(q->numTokens, 4)

  ASSERT(n->pn.children[0]->type == QN_UNION);
  ASSERT_STRING_EQ("hello", n->pn.children[0]->un.children[0]->tn.str);
  ASSERT_STRING_EQ("foo", n->pn.children[0]->un.children[1]->tn.str);
  ASSERT(n->pn.children[0]->un.children[1]->tn.metadata != NULL);
  ASSERT((*(int *)(n->pn.children[0]->un.children[1]->tn.metadata)) == 1337);

  ASSERT(n->pn.children[1]->type == QN_UNION);
  ASSERT_STRING_EQ("world", n->pn.children[1]->un.children[0]->tn.str);
  ASSERT_STRING_EQ("foo", n->pn.children[1]->un.children[1]->tn.str);
  ASSERT(n->pn.children[1]->un.children[1]->tn.metadata != NULL);
  ASSERT((*(int *)(n->pn.children[1]->un.children[1]->tn.metadata)) == 1337);
  Query_Free(q);
  RETURN_TEST_SUCCESS;
}

void benchmarkQueryParser() {
  char *qt = "(hello|world) \"another world\"";
  RedisSearchCtx ctx;
  char *err = NULL;

  Query *q = NewQuery(NULL, qt, strlen(qt), 0, 1, 0xff, 0, "en", DEFAULT_STOPWORDS, NULL);
  TIME_SAMPLE_RUN_LOOP(50000, { Query_Parse(q, &err); });
}

int main(int argc, char **argv) {

  // LOGGING_INIT(L_INFO);
  RMUTil_InitAlloc();

  TESTFUNC(testQueryParser);
  TESTFUNC(testQueryExpander);

  benchmarkQueryParser();
}
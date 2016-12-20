#include <stdio.h>
#include "test_util.h"
#include "../query.h"
#include "../query_parser/tokenizer.h"
#include "time_sample.h"

void __queryStage_Print(QueryStage *qs, int depth);

int isValidQuery(char *qt) {
  char *err = NULL;
  RedisSearchCtx ctx;
  Query *q =
      NewQuery(NULL, qt, strlen(qt), 0, 1, 0xff, 0, "en", DEFAULT_STOPWORDS);

  QueryStage *n = Query_Parse(q, &err);

  if (err) {
    FAIL("Error parsing query '%s': %s", qt, err);
  }
  ASSERT(n != NULL);
  __queryStage_Print(n, 0);
  Query_Free(q);
  return 0;
}

#define assertValidQuery(qt)                                                   \
  {                                                                            \
    if (0 != isValidQuery(qt))                                                 \
      return -1;                                                               \
  }

#define assertInvalidQuery(qt)                                                 \
  {                                                                            \
    if (0 == isValidQuery(qt))                                                 \
      return -1;                                                               \
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
  // assertValidQuery("(hello world)|(goodbye moon)");

  assertInvalidQuery("(foo");
  assertInvalidQuery("\"foo");
  assertInvalidQuery("");
  assertInvalidQuery("()");

  char *err = NULL;
  char *qt = "(hello|world) and \"another world\" (foo is bar) baz ";
  RedisSearchCtx ctx;
  Query *q =
      NewQuery(NULL, qt, strlen(qt), 0, 1, 0xff, 0, "zz", DEFAULT_STOPWORDS);

  QueryStage *n = Query_Parse(q, &err);

  if (err)
    FAIL("Error parsing query: %s", err);
  __queryStage_Print(n, 0);
  ASSERT(err == NULL);
  ASSERT(n != NULL);
  ASSERT(n->op == Q_INTERSECT);
  ASSERT(n->nchildren == 4);

  ASSERT(n->children[0]->op == Q_UNION);
  ASSERT_STRING_EQ("hello", n->children[0]->children[0]->value);
  ASSERT_STRING_EQ("world", n->children[0]->children[1]->value);

  ASSERT(n->children[1]->op == Q_EXACT);
  ASSERT_STRING_EQ("another", n->children[1]->children[0]->value);
  ASSERT_STRING_EQ("world", n->children[1]->children[1]->value);

  ASSERT(n->children[2]->op == Q_INTERSECT);
  ASSERT_STRING_EQ("foo", n->children[2]->children[0]->value);
  ASSERT_STRING_EQ("bar", n->children[2]->children[1]->value);

  ASSERT(n->children[3]->op == Q_LOAD);
  ASSERT_STRING_EQ("baz", n->children[3]->value);
  Query_Free(q);
  return 0;
}

void benchmarkQueryParser() {
  char *qt = "(hello|world) \"another world\"";
  RedisSearchCtx ctx;
  char *err = NULL;

  Query *q =
      NewQuery(NULL, qt, strlen(qt), 0, 1, 0xff, 0, "en", DEFAULT_STOPWORDS);
  TIME_SAMPLE_RUN_LOOP(50000, { Query_Parse(q, &err); });
}

int main(int argc, char **argv) {

  // LOGGING_INIT(L_INFO);
  TESTFUNC(testQueryParser);
  benchmarkQueryParser();
}
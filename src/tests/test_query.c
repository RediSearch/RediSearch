#include "../query.h"
#include "../query_parser/tokenizer.h"
#include "../stopwords.h"
#include "test_util.h"
#include "time_sample.h"
#include "../extension.h"
#include "../ext/default.h"
#include <stdio.h>

void __queryNode_Print(Query *q, QueryNode *qs, int depth);

int isValidQuery(char *qt) {
  char *err = NULL;
  RedisSearchCtx ctx;
  static const char *args[] = {"SCHEMA", "title",  "text", "weight", "0.1",    "body",
                               "text",   "weight", "2.0",  "bar",    "numeric"};

  ctx.spec = IndexSpec_Parse("idx", args, sizeof(args) / sizeof(const char *), &err);
  Query *q = NewQuery(&ctx, qt, strlen(qt), 0, 1, 0xff, 0, "en", DEFAULT_STOPWORDS, NULL, -1, 0,
                      NULL, (RSPayload){}, NULL);

  QueryNode *n = Query_Parse(q, &err);

  if (err) {
    Query_Free(q);
    IndexSpec_Free(ctx.spec);
    fprintf(stderr, "Error parsing query '%s': %s", qt, err);
    free(err);
    return 1;
  }
  if (!n) {
    Query_Free(q);
    IndexSpec_Free(ctx.spec);
  }
  ASSERT(n != NULL);
  __queryNode_Print(q, n, 0);
  Query_Free(q);
  IndexSpec_Free(ctx.spec);
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

  assertValidQuery("hello wor*");
  assertValidQuery("hello world");
  assertValidQuery("hello (world)");
  assertValidQuery("\"hello world\"");
  assertValidQuery("\"hello world\" \"foo bar\"");
  assertValidQuery("hello \"foo bar\" world");
  assertValidQuery("hello|hallo|yellow world");
  assertValidQuery("(hello|world|foo) bar baz 123");
  assertValidQuery("(hello|world|foo) (bar baz)");
  assertValidQuery("(hello world|foo \"bar baz\") \"bar baz\" bbbb");
  assertValidQuery("@title:(barack obama)  @body:us|president");
  assertValidQuery("@title:barack obama  @body:us");
  assertValidQuery("@title:barack @body:obama");
  assertValidQuery("@title|body:barack @body|title|url|something|else:obama");
  assertValidQuery("foo -bar -(bar baz)");
  // assertValidQuery("(hello world)|(goodbye moon)");
  assertInvalidQuery("@title:");
  assertInvalidQuery("@body:@title:");
  assertInvalidQuery("@body|title:@title:");
  assertInvalidQuery("@body|title");

  assertValidQuery("@number:[100 200]");
  assertValidQuery("@number:[100 -200]");
  assertValidQuery("@number:[(100 (200]");
  assertValidQuery("@number:[100 inf]");
  assertValidQuery("@number:[100 -inf]");
  assertValidQuery("@number:[-inf +inf]");

  assertInvalidQuery("@number:[100 foo]");

  assertInvalidQuery("(foo");
  assertInvalidQuery("\"foo");
  assertInvalidQuery("");
  assertInvalidQuery("()");

  char *err = NULL;
  char *qt = "(hello|world) and \"another world\" (foo is bar) -baz boo*";
  RedisSearchCtx ctx;
  Query *q = NewQuery(NULL, qt, strlen(qt), 0, 1, 0xff, 0, "zz", DEFAULT_STOPWORDS, NULL, -1, 0,
                      NULL, (RSPayload){}, NULL);

  QueryNode *n = Query_Parse(q, &err);

  if (err) FAIL("Error parsing query: %s", err);
  __queryNode_Print(q, n, 0);
  ASSERT(err == NULL);
  ASSERT(n != NULL);
  ASSERT_EQUAL(n->type, QN_PHRASE);
  ASSERT_EQUAL(n->pn.exact, 0);
  ASSERT_EQUAL(n->pn.numChildren, 5);
  ASSERT_EQUAL(n->fieldMask, RS_FIELDMASK_ALL);

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

  ASSERT(n->pn.children[3]->type == QN_NOT);
  ASSERT_EQUAL(QN_TOKEN, n->pn.children[3]->not.child->type);
  ASSERT_STRING_EQ("baz", n->pn.children[3]->not.child->tn.str);

  ASSERT_EQUAL(QN_PREFX, n->pn.children[4]->type);
  ASSERT_STRING_EQ("boo", n->pn.children[4]->pfx.str);
  Query_Free(q);

  return 0;
}

int testFieldSpec() {
  char *err = NULL;

  static const char *args[] = {"SCHEMA", "title",  "text", "weight", "0.1",    "body",
                               "text",   "weight", "2.0",  "bar",    "numeric"};
  RedisSearchCtx ctx = {
      .spec = IndexSpec_Parse("idx", args, sizeof(args) / sizeof(const char *), &err)};
  char *qt = "@title:hello world";
  Query *q = NewQuery(&ctx, qt, strlen(qt), 0, 1, 0xff, 0, "en", DEFAULT_STOPWORDS, NULL, -1, 0,
                      NULL, (RSPayload){}, NULL);

  QueryNode *n = Query_Parse(q, &err);

  if (err) FAIL("Error parsing query: %s", err);
  __queryNode_Print(q, n, 0);
  ASSERT(err == NULL);
  ASSERT(n != NULL);
  ASSERT_EQUAL(n->type, QN_PHRASE);
  ASSERT_EQUAL(n->fieldMask, 0x01)
  Query_Free(q);

  qt = "@title:hello @body:world";
  q = NewQuery(&ctx, qt, strlen(qt), 0, 1, 0xff, 0, "en", DEFAULT_STOPWORDS, NULL, -1, 0, NULL,
               (RSPayload){}, NULL);
  n = Query_Parse(q, &err);
  if (err) {
    Query_Free(q);
    IndexSpec_Free(ctx.spec);
    FAIL("Error parsing query: %s", err);
  }

  ASSERT(n != NULL);
  printf("%s ====> ", qt);
  __queryNode_Print(q, n, 0);
  ASSERT_EQUAL(n->type, QN_PHRASE);
  ASSERT_EQUAL(n->fieldMask, 0x03)
  ASSERT_EQUAL(n->pn.children[0]->fieldMask, 0x01)
  ASSERT_EQUAL(n->pn.children[1]->fieldMask, 0x02)
  Query_Free(q);

  // test field modifiers
  qt = "@title:(hello world) @body|title:(world apart) @adasdfsd:fofofof";
  q = NewQuery(&ctx, qt, strlen(qt), 0, 1, 0xff, 0, "en", DEFAULT_STOPWORDS, NULL, -1, 0, NULL,
               (RSPayload){}, NULL);
  n = Query_Parse(q, &err);
  if (err) FAIL("Error parsing query: %s", err);
  ASSERT(n != NULL);
  ASSERT_EQUAL(n->type, QN_PHRASE);
  ASSERT_EQUAL(n->fieldMask, 0x03)
  ASSERT_EQUAL(n->pn.children[0]->fieldMask, 0x01)
  ASSERT_EQUAL(n->pn.children[1]->fieldMask, 0x03)
  ASSERT_EQUAL(n->pn.children[2]->fieldMask, 0x00)
  Query_Free(q);

  // test numeric ranges
  qt = "@num:[0.4 (500]";
  q = NewQuery(&ctx, qt, strlen(qt), 0, 1, 0xff, 0, "en", DEFAULT_STOPWORDS, NULL, -1, 0, NULL,
               (RSPayload){}, NULL);
  n = Query_Parse(q, &err);
  if (err) FAIL("Error parsing query: %s", err);
  ASSERT(n != NULL);
  ASSERT_EQUAL(n->type, QN_NUMERIC);
  ASSERT_EQUAL(n->nn.nf->min, 0.4);
  ASSERT_EQUAL(n->nn.nf->max, 500.0);
  ASSERT_EQUAL(n->nn.nf->inclusiveMin, 1);
  ASSERT_EQUAL(n->nn.nf->inclusiveMax, 0);
  Query_Free(q);
  IndexSpec_Free(ctx.spec);

  return 0;
}
void benchmarkQueryParser() {
  char *qt = "(hello|world) \"another world\"";
  RedisSearchCtx ctx;
  char *err = NULL;

  Query *q = NewQuery(NULL, qt, strlen(qt), 0, 1, 0xff, 0, "en", DEFAULT_STOPWORDS, NULL, -1, 0,
                      NULL, (RSPayload){}, NULL);
  TIME_SAMPLE_RUN_LOOP(50000, { Query_Parse(q, &err); });
}

TEST_MAIN({
  RMUTil_InitAlloc();
  // LOGGING_INIT(L_INFO);
  TESTFUNC(testQueryParser);
  TESTFUNC(testFieldSpec);
  benchmarkQueryParser();

});
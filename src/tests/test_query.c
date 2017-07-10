#include "../query.h"
#include "../query_parser/tokenizer.h"
#include "../stopwords.h"
#include "test_util.h"
#include "time_sample.h"
#include "../extension.h"
#include "../ext/default.h"
#include "../rmutil/alloc.h"
#include <stdio.h>

void QueryNode_Print(Query *q, QueryNode *qs, int depth);

int isValidQuery(char *qt) {
  char *err = NULL;
  RedisSearchCtx ctx;
  static const char *args[] = {"SCHEMA", "title",  "text", "weight", "0.1",    "body",
                               "text",   "weight", "2.0",  "bar",    "numeric"};

  ctx.spec = IndexSpec_Parse("idx", args, sizeof(args) / sizeof(const char *), &err);
  Query *q = NewQuery(&ctx, qt, strlen(qt), 0, 1, 0xff, 0, "en", DefaultStopWordList(), NULL, -1, 0,
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
  QueryNode_Print(q, n, 0);
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
  assertValidQuery("\"hello\"");

  assertValidQuery("\"hello world\" \"foo bar\"");
  assertValidQuery("\"hello world\"|\"foo bar\"");
  assertValidQuery("\"hello world\" (\"foo bar\")");
  assertValidQuery("hello \"foo bar\" world");
  assertValidQuery("hello|hallo|yellow world");
  assertValidQuery("(hello|world|foo) bar baz 123");
  assertValidQuery("(hello|world|foo) (bar baz)");
  // assertValidQuery("(hello world|foo \"bar baz\") \"bar baz\" bbbb");
  assertValidQuery("@title:(barack obama)  @body:us|president");
  assertValidQuery("@ti_tle:barack obama  @body:us");
  assertValidQuery("@title:barack @body:obama");
  assertValidQuery("@tit_le|bo_dy:barack @body|title|url|something_else:obama");
  assertValidQuery("hello,world;good+bye foo.bar");
  assertValidQuery("@BusinessName:\"Wells Fargo Bank, National Association\"");
  assertValidQuery("foo -bar -(bar baz)");
  assertValidQuery("(hello world)|(goodbye moon)");
  assertInvalidQuery("@title:");
  assertInvalidQuery("@body:@title:");
  assertInvalidQuery("@body|title:@title:");
  assertInvalidQuery("@body|title");
  assertValidQuery("hello ~world ~war");
  assertValidQuery("hello ~(world war)");

  assertValidQuery("@number:[100 200]");
  assertValidQuery("@number:[100 -200]");
  assertValidQuery("@number:[(100 (200]");
  assertValidQuery("@number:[100 inf]");
  assertValidQuery("@number:[100 -inf]");
  assertValidQuery("@number:[-inf +inf]");
  assertValidQuery("@number:[-inf +inf]|@number:[100 200]");

  assertInvalidQuery("@number:[100 foo]");

  assertInvalidQuery("(foo");
  assertInvalidQuery("\"foo");
  assertInvalidQuery("");
  assertInvalidQuery("()");

  // test utf-8 query
  assertValidQuery("שלום עולם");

  char *err = NULL;
  char *qt = "(hello|world) and \"another world\" (foo is bar) -(baz boo*)";
  Query *q = NewQuery(NULL, qt, strlen(qt), 0, 1, 0xff, 0, "zz", DefaultStopWordList(), NULL, -1, 0,
                      NULL, (RSPayload){}, NULL);

  QueryNode *n = Query_Parse(q, &err);

  if (err) FAIL("Error parsing query: %s", err);
  QueryNode_Print(q, n, 0);
  ASSERT(err == NULL);
  ASSERT(n != NULL);
  ASSERT_EQUAL(n->type, QN_PHRASE);
  ASSERT_EQUAL(n->pn.exact, 0);
  ASSERT_EQUAL(n->pn.numChildren, 4);
  ASSERT_EQUAL(n->fieldMask, RS_FIELDMASK_ALL);

  ASSERT(n->pn.children[0]->type == QN_UNION);
  ASSERT_STRING_EQ("hello", n->pn.children[0]->un.children[0]->tn.str);
  ASSERT_STRING_EQ("world", n->pn.children[0]->un.children[1]->tn.str);

  QueryNode *_n = n->pn.children[1];

  ASSERT(_n->type == QN_PHRASE);
  ASSERT(_n->pn.exact == 1);
  ASSERT_EQUAL(_n->pn.numChildren, 2);
  ASSERT_STRING_EQ("another", _n->pn.children[0]->tn.str);
  ASSERT_STRING_EQ("world", _n->pn.children[1]->tn.str);

  _n = n->pn.children[2];
  ASSERT(_n->type == QN_PHRASE);

  ASSERT(_n->pn.exact == 0);
  ASSERT_EQUAL(_n->pn.numChildren, 2);
  ASSERT_STRING_EQ("foo", _n->pn.children[0]->tn.str);
  ASSERT_STRING_EQ("bar", _n->pn.children[1]->tn.str);

  _n = n->pn.children[3];
  ASSERT(_n->type == QN_NOT);
  _n = _n->not.child;
  ASSERT(_n->pn.exact == 0);
  ASSERT_EQUAL(_n->pn.numChildren, 2);
  ASSERT_STRING_EQ("baz", _n->pn.children[0]->tn.str);

  ASSERT_EQUAL(_n->pn.children[1]->type, QN_PREFX);
  ASSERT_STRING_EQ("boo", _n->pn.children[1]->pfx.str);

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
  Query *q = NewQuery(&ctx, qt, strlen(qt), 0, 1, 0xff, 0, "en", DefaultStopWordList(), NULL, -1, 0,
                      NULL, (RSPayload){}, NULL);

  QueryNode *n = Query_Parse(q, &err);

  if (err) FAIL("Error parsing query: %s", err);
  QueryNode_Print(q, n, 0);
  ASSERT(err == NULL);
  ASSERT(n != NULL);
  ASSERT_EQUAL(n->type, QN_PHRASE);
  ASSERT_EQUAL(n->fieldMask, 0x01)
  Query_Free(q);

  qt = "(@title:hello) (@body:world)";
  q = NewQuery(&ctx, qt, strlen(qt), 0, 1, 0xff, 0, "en", DefaultStopWordList(), NULL, -1, 0, NULL,
               (RSPayload){}, NULL);
  n = Query_Parse(q, &err);
  if (err) {
    Query_Free(q);
    IndexSpec_Free(ctx.spec);
    FAIL("Error parsing query: %s", err);
  }

  ASSERT(n != NULL);
  printf("%s ====> ", qt);
  QueryNode_Print(q, n, 0);
  ASSERT_EQUAL(n->type, QN_PHRASE);
  ASSERT_EQUAL(n->fieldMask, 0x03)
  ASSERT_EQUAL(n->pn.children[0]->fieldMask, 0x01)
  ASSERT_EQUAL(n->pn.children[1]->fieldMask, 0x02)
  Query_Free(q);

  // test field modifiers
  qt = "@title:(hello world) @body:(world apart) @adas_dfsd:fofofof";
  q = NewQuery(&ctx, qt, strlen(qt), 0, 1, 0xff, 0, "en", DefaultStopWordList(), NULL, -1, 0, NULL,
               (RSPayload){}, NULL);
  n = Query_Parse(q, &err);
  if (err) FAIL("Error parsing query: %s", err);
  ASSERT(n != NULL);
  printf("%s ====> ", qt);
  QueryNode_Print(q, n, 0);
  ASSERT_EQUAL(n->type, QN_PHRASE);
  ASSERT_EQUAL(n->fieldMask, 0x03)
  ASSERT_EQUAL(n->pn.numChildren, 2)
  ASSERT_EQUAL(n->pn.children[0]->fieldMask, 0x03)
  ASSERT_EQUAL(n->pn.children[1]->fieldMask, 0x00)

  n = n->pn.children[0];
  ASSERT_EQUAL(n->type, QN_PHRASE);
  ASSERT_EQUAL(n->fieldMask, 0x03)
  ASSERT_EQUAL(n->pn.numChildren, 2)
  ASSERT_EQUAL(n->pn.children[0]->fieldMask, 0x01)
  ASSERT_EQUAL(n->pn.children[1]->fieldMask, 0x02)
  // ASSERT_EQUAL(n->pn.children[2]->fieldMask, 0x00)
  Query_Free(q);
  // test numeric ranges
  qt = "@num:[0.4 (500]";
  q = NewQuery(&ctx, qt, strlen(qt), 0, 1, 0xff, 0, "en", DefaultStopWordList(), NULL, -1, 0, NULL,
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
  char *err = NULL;

  Query *q = NewQuery(NULL, qt, strlen(qt), 0, 1, 0xff, 0, "en", DefaultStopWordList(), NULL, -1, 0,
                      NULL, (RSPayload){}, NULL);
  TIME_SAMPLE_RUN_LOOP(50000, { Query_Parse(q, &err); });
}

void RMUTil_InitAlloc();
TEST_MAIN({
  RMUTil_InitAlloc();
  // LOGGING_INIT(L_INFO);
  TESTFUNC(testQueryParser);
  TESTFUNC(testFieldSpec);
  benchmarkQueryParser();

});

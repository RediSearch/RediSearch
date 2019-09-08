#include "../query.h"
#include "../query_parser/tokenizer.h"
#include "../stopwords.h"
#include "test_util.h"
#include "time_sample.h"
#include "../extension.h"
#include "../search_request.h"
#include "../ext/default.h"
#include "../rmutil/alloc.h"
#include <stdio.h>

void QueryNode_Print(QueryParseCtx *q, QueryNode *qs, int depth);

#define SEARCH_OPTS(ctx)                                                                \
  (RSSearchOptions) {                                                                   \
    .flags = RS_DEFAULT_QUERY_FLAGS, .fieldMask = RS_FIELDMASK_ALL, .indexName = "idx", \
    .language = "en", .stopwords = DefaultStopWordList()                                \
  }

#define QUERY_PARSE_CTX(ctx, qt, opts) NewQueryParseCtx(&ctx, qt, strlen(qt), &opts);

int isValidQuery(char *qt, RedisSearchCtx ctx) {
  char *err = NULL;

  RSSearchOptions opts = SEARCH_OPTS(ctx);
  QueryParseCtx *q = QUERY_PARSE_CTX(ctx, qt, opts);

  QueryNode *n = Query_Parse(q, &err);

  if (err) {
    Query_Free(q);
    fprintf(stderr, "Error parsing query '%s': %s\n", qt, err);
    rm_free(err);
    return 1;
  }
  if (n) {
    QueryNode_Print(q, n, 0);
  }
  Query_Free(q);

  return 0;
}

#define assertValidQuery(qt, ctx)              \
  {                                            \
    if (0 != isValidQuery(qt, ctx)) return -1; \
  }

#define assertInvalidQuery(qt, ctx)            \
  {                                            \
    if (0 == isValidQuery(qt, ctx)) return -1; \
  }

int testQueryParser() {

  RedisSearchCtx ctx;
  static const char *args[] = {"SCHEMA",  "title", "text",   "weight", "0.1",
                               "body",    "text",  "weight", "2.0",    "bar",
                               "numeric", "loc",   "geo",    "tags",   "tag"};
  char *err = NULL;
  ctx.spec = IndexSpec_Parse("idx", args, sizeof(args) / sizeof(const char *), &err);
  ASSERT(err == NULL);

  // test some valid queries
  assertValidQuery("hello", ctx);

  assertValidQuery("hello wor*", ctx);
  assertValidQuery("hello world", ctx);
  assertValidQuery("hello (world)", ctx);

  assertValidQuery("\"hello world\"", ctx);
  assertValidQuery("\"hello\"", ctx);

  assertValidQuery("\"hello world\" \"foo bar\"", ctx);
  assertValidQuery("\"hello world\"|\"foo bar\"", ctx);
  assertValidQuery("\"hello world\" (\"foo bar\")", ctx);
  assertValidQuery("hello \"foo bar\" world", ctx);
  assertValidQuery("hello|hallo|yellow world", ctx);
  assertValidQuery("(hello|world|foo) bar baz 123", ctx);
  assertValidQuery("(hello|world|foo) (bar baz)", ctx);
  // assertValidQuery("(hello world|foo \"bar baz\") \"bar baz\" bbbb");
  assertValidQuery("@title:(barack obama)  @body:us|president", ctx);
  assertValidQuery("@ti_tle:barack obama  @body:us", ctx);
  assertValidQuery("@title:barack @body:obama", ctx);
  assertValidQuery("@tit_le|bo_dy:barack @body|title|url|something_else:obama", ctx);
  assertValidQuery("hello world&good+bye foo.bar", ctx);
  assertValidQuery("@BusinessName:\"Wells Fargo Bank, National Association\"", ctx);
  // escaping and unicode in field names
  assertValidQuery("@Business\\:\\-\\ Name:Wells Fargo", ctx);
  assertValidQuery("@שלום:Wells Fargo", ctx);

  assertValidQuery("foo -bar -(bar baz)", ctx);
  assertValidQuery("(hello world)|(goodbye moon)", ctx);
  assertInvalidQuery("@title:", ctx);
  assertInvalidQuery("@body:@title:", ctx);
  assertInvalidQuery("@body|title:@title:", ctx);
  assertInvalidQuery("@body|title", ctx);
  assertValidQuery("hello ~world ~war", ctx);
  assertValidQuery("hello ~(world war)", ctx);
  assertValidQuery("-foo", ctx);
  assertValidQuery("@title:-foo", ctx);
  assertValidQuery("-@title:foo", ctx);

  // some geo queries
  assertValidQuery("@loc:[15.1 -15 30 km]", ctx);
  assertValidQuery("@loc:[15 -15.1 30 m]", ctx);
  assertValidQuery("@loc:[15.03 -15.45 30 mi]", ctx);
  assertValidQuery("@loc:[15.65 -15.65 30 ft]", ctx);
  assertValidQuery("hello world @loc:[15.65 -15.65 30 ft]", ctx);
  assertValidQuery("hello world -@loc:[15.65 -15.65 30 ft]", ctx);
  assertValidQuery("hello world ~@loc:[15.65 -15.65 30 ft]", ctx);
  assertValidQuery("@title:hello world ~@loc:[15.65 -15.65 30 ft]", ctx);
  assertValidQuery("@loc:[15.65 -15.65 30 ft] @loc:[15.65 -15.65 30 ft]", ctx);
  assertValidQuery("@loc:[15.65 -15.65 30 ft]|@loc:[15.65 -15.65 30 ft]", ctx);
  assertValidQuery("hello (world @loc:[15.65 -15.65 30 ft])", ctx);

  assertInvalidQuery("@loc:[190.65 -100.65 30 ft])", ctx);
  assertInvalidQuery("@loc:[50 50 -1 ft])", ctx);
  assertInvalidQuery("@loc:[50 50 1 quoops])", ctx);
  assertInvalidQuery("@loc:[50 50 1 ftps])", ctx);
  assertInvalidQuery("@loc:[50 50 1 1])", ctx);
  assertInvalidQuery("@loc:[50 50 1])", ctx);
  // numeric
  assertValidQuery("@number:[100 200]", ctx);
  assertValidQuery("@number:[100 -200]", ctx);
  assertValidQuery("@number:[(100 (200]", ctx);
  assertValidQuery("@number:[100 inf]", ctx);
  assertValidQuery("@number:[100 -inf]", ctx);
  assertValidQuery("@number:[-inf +inf]", ctx);
  assertValidQuery("@number:[-inf +inf]|@number:[100 200]", ctx);

  assertInvalidQuery("@number:[100 foo]", ctx);

  // Tag queries
  assertValidQuery("@tags:{foo}", ctx);
  assertValidQuery("@tags:{foo|bar baz|boo}", ctx);
  assertValidQuery("@tags:{foo|bar\\ baz|boo}", ctx);
  assertValidQuery("@tags:{foo*}", ctx);
  assertValidQuery("@tags:{foo\\-*}", ctx);
  assertValidQuery("@tags:{bar | foo*}", ctx);
  assertValidQuery("@tags:{bar* | foo}", ctx);
  assertValidQuery("@tags:{bar* | foo*}", ctx);

  assertInvalidQuery("@tags:{foo|bar\\ baz|}", ctx);
  assertInvalidQuery("@tags:{foo|bar\\ baz|", ctx);
  assertInvalidQuery("{foo|bar\\ baz}", ctx);

  assertInvalidQuery("(foo", ctx);
  assertInvalidQuery("\"foo", ctx);
  assertValidQuery("", ctx);
  assertInvalidQuery("()", ctx);

  // test stopwords
  assertValidQuery("a for is", ctx);
  assertValidQuery("a|for|is", ctx);
  assertValidQuery("a little bit of party", ctx);
  assertValidQuery("no-as", ctx);
  assertValidQuery("~no~as", ctx);
  assertValidQuery("(no -as) =>{$weight: 0.5}", ctx);
  assertValidQuery("@foo:-as", ctx);

  // test utf-8 query
  assertValidQuery("שלום עולם", ctx);

  // Test attribute
  assertValidQuery("(foo bar) => {$weight: 0.5; $slop: 2}", ctx);
  assertValidQuery("foo => {$weight: 0.5} bar => {$weight: 0.1}", ctx);

  assertValidQuery("@title:(foo bar) => {$weight: 0.5; $slop: 2}", ctx);
  assertValidQuery(
      "@title:(foo bar) => {$weight: 0.5; $slop: 2} @body:(foo bar) => {$weight: 0.5; $slop: 2}",
      ctx);
  assertValidQuery("(foo => {$weight: 0.5;}) | ((bar) => {$weight: 0.5})", ctx);
  assertValidQuery("(foo => {$weight: 0.5;})  ((bar) => {}) => {}", ctx);
  assertValidQuery("@tag:{foo | bar} => {$weight: 0.5;} ", ctx);
  assertValidQuery("@num:[0 100] => {$weight: 0.5;} ", ctx);
  assertInvalidQuery("@tag:{foo | bar} => {$weight: -0.5;} ", ctx);
  assertInvalidQuery("@tag:{foo | bar} => {$great: 0.5;} ", ctx);
  assertInvalidQuery("@tag:{foo | bar} => {$great:;} ", ctx);
  assertInvalidQuery("@tag:{foo | bar} => {$:1;} ", ctx);

  assertInvalidQuery(" => {$weight: 0.5;} ", ctx);

  char *qt = "(hello|world) and \"another world\" (foo is bar) -(baz boo*)";
  RSSearchOptions opts = SEARCH_OPTS(ctx);
  QueryParseCtx *q = QUERY_PARSE_CTX(ctx, qt, opts);

  QueryNode *n = Query_Parse(q, &err);

  if (err) FAIL("Error parsing query: %s", err);
  QueryNode_Print(q, n, 0);
  ASSERT(err == NULL);
  ASSERT(n != NULL);
  ASSERT_EQUAL(n->type, QN_PHRASE);
  ASSERT_EQUAL(n->pn.exact, 0);
  ASSERT_EQUAL(n->pn.numChildren, 4);
  ASSERT_EQUAL(n->opts.fieldMask, RS_FIELDMASK_ALL);

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
  IndexSpec_Free(ctx.spec);

  return 0;
}

int testPureNegative() {
  char *err = NULL;
  const char *qs[] = {"-@title:hello", "-hello", "@title:-hello", "-(foo)", "-foo", "(-foo)", NULL};

  static const char *args[] = {"SCHEMA", "title",  "text", "weight", "0.1",    "body",
                               "text",   "weight", "2.0",  "bar",    "numeric"};
  RedisSearchCtx ctx = {
      .spec = IndexSpec_Parse("idx", args, sizeof(args) / sizeof(const char *), &err)};
  RSSearchOptions opts = SEARCH_OPTS(ctx);

  for (int i = 0; qs[i] != NULL; i++) {
    QueryParseCtx *q = QUERY_PARSE_CTX(ctx, qs[i], opts);

    QueryNode *n = Query_Parse(q, &err);

    if (err) FAIL("Error parsing query: %s", err);
    // QueryNode_Print(q, n, 0);
    ASSERT(err == NULL);
    ASSERT(n != NULL);
    ASSERT_EQUAL(n->type, QN_NOT);
    ASSERT(n->not.child != NULL);

    Query_Free(q);
  }
  return 0;
}

int testGeoQuery() {
  char *err;
  static const char *args[] = {"SCHEMA", "title", "text", "loc", "geo"};
  RedisSearchCtx ctx = {
      .spec = IndexSpec_Parse("idx", args, sizeof(args) / sizeof(const char *), &err)};
  char *qt = "@title:hello world @loc:[31.52 32.1342 10.01 km]";
  RSSearchOptions opts = SEARCH_OPTS(ctx);

  QueryParseCtx *q = QUERY_PARSE_CTX(ctx, qt, opts);

  QueryNode *n = Query_Parse(q, &err);

  if (err) FAIL("Error parsing query: %s", err);
  QueryNode_Print(q, n, 0);
  ASSERT(err == NULL);
  ASSERT(n != NULL);
  ASSERT_EQUAL(n->type, QN_PHRASE);
  ASSERT((n->opts.fieldMask == RS_FIELDMASK_ALL));
  ASSERT_EQUAL(n->pn.numChildren, 2);

  QueryNode *gn = n->pn.children[1];
  ASSERT_EQUAL(gn->type, QN_GEO);
  ASSERT_STRING_EQ(gn->gn.gf->property, "loc");
  ASSERT_STRING_EQ(gn->gn.gf->unit, "km");
  ASSERT_EQUAL(gn->gn.gf->lon, 31.52);
  ASSERT_EQUAL(gn->gn.gf->lat, 32.1342);
  ASSERT_EQUAL(gn->gn.gf->radius, 10.01);
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
  RSSearchOptions opts = SEARCH_OPTS(ctx);
  QueryParseCtx *q = QUERY_PARSE_CTX(ctx, qt, opts);

  QueryNode *n = Query_Parse(q, &err);

  if (err) FAIL("Error parsing query: %s", err);
  QueryNode_Print(q, n, 0);
  ASSERT(err == NULL);
  ASSERT(n != NULL);
  ASSERT_EQUAL(n->type, QN_PHRASE);
  ASSERT_EQUAL(n->opts.fieldMask, 0x01)
  Query_Free(q);

  qt = "(@title:hello) (@body:world)";
  q = QUERY_PARSE_CTX(ctx, qt, opts);

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
  ASSERT_EQUAL(n->opts.fieldMask, RS_FIELDMASK_ALL)
  ASSERT_EQUAL(n->pn.children[0]->opts.fieldMask, 0x01)
  ASSERT_EQUAL(n->pn.children[1]->opts.fieldMask, 0x02)
  Query_Free(q);

  // test field modifiers
  qt = "@title:(hello world) @body:(world apart) @adas_dfsd:fofofof";
  q = QUERY_PARSE_CTX(ctx, qt, opts);

  n = Query_Parse(q, &err);
  if (err) FAIL("Error parsing query: %s", err);
  ASSERT(n != NULL);
  printf("%s ====> ", qt);
  QueryNode_Print(q, n, 0);
  ASSERT_EQUAL(n->type, QN_PHRASE);
  ASSERT_EQUAL(n->opts.fieldMask, RS_FIELDMASK_ALL)
  ASSERT_EQUAL(n->pn.numChildren, 3)
  ASSERT_EQUAL(n->pn.children[0]->opts.fieldMask, 0x01)
  ASSERT_EQUAL(n->pn.children[1]->opts.fieldMask, 0x02)
  ASSERT_EQUAL(n->pn.children[2]->opts.fieldMask, 0x00)
  // ASSERT_EQUAL(n->pn.children[2]->fieldMask, 0x00)
  Query_Free(q);

  // test numeric ranges
  qt = "@num:[0.4 (500]";
  q = QUERY_PARSE_CTX(ctx, qt, opts);
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

int testAttributes() {
  char *err = NULL;

  static const char *args[] = {"SCHEMA", "title", "text", "body", "text"};
  RedisSearchCtx ctx = {
      .spec = IndexSpec_Parse("idx", args, sizeof(args) / sizeof(const char *), &err)};

  char *qt =
      "(@title:(foo bar) => {$weight: 0.5} @body:lol => {$weight: 0.2}) => "
      "{$weight:0.3; $slop:2; $inorder:true}";

  RSSearchOptions opts = SEARCH_OPTS(ctx);
  QueryParseCtx *q = QUERY_PARSE_CTX(ctx, qt, opts);
  QueryNode *n = Query_Parse(q, &err);

  if (err) FAIL("Error parsing query: %s", err);
  QueryNode_Print(q, n, 0);
  ASSERT(err == NULL);
  ASSERT(n != NULL);

  ASSERT_EQUAL(0.3, n->opts.weight);
  ASSERT_EQUAL(2, n->opts.maxSlop);
  ASSERT_EQUAL(1, n->opts.inOrder);

  ASSERT_EQUAL(n->type, QN_PHRASE);
  ASSERT_EQUAL(n->pn.numChildren, 2)
  ASSERT_EQUAL(0.5, n->pn.children[0]->opts.weight)
  ASSERT_EQUAL(0.2, n->pn.children[1]->opts.weight)

  RETURN_TEST_SUCCESS;
}
int testTags() {

  char *err = NULL;

  static const char *args[] = {"SCHEMA", "title", "text", "tags", "tag", "separator", ";"};
  RedisSearchCtx ctx = {
      .spec = IndexSpec_Parse("idx", args, sizeof(args) / sizeof(const char *), &err)};
  char *qt = "@tags:{hello world  |foo| שלום|  lorem\\ ipsum    }";

  RSSearchOptions opts = SEARCH_OPTS(ctx);
  QueryParseCtx *q = QUERY_PARSE_CTX(ctx, qt, opts);
  QueryNode *n = Query_Parse(q, &err);

  if (err) FAIL("Error parsing query: %s", err);
  QueryNode_Print(q, n, 0);
  ASSERT(err == NULL);
  ASSERT(n != NULL);

  ASSERT_EQUAL(n->type, QN_TAG);
  ASSERT_EQUAL(4, n->tag.numChildren);
  ASSERT_EQUAL(QN_PHRASE, n->tag.children[0]->type)
  ASSERT_STRING_EQ("hello", n->tag.children[0]->pn.children[0]->tn.str)
  ASSERT_STRING_EQ("world", n->tag.children[0]->pn.children[1]->tn.str)

  ASSERT_EQUAL(QN_TOKEN, n->tag.children[1]->type)
  ASSERT_STRING_EQ("foo", n->tag.children[1]->tn.str)

  ASSERT_EQUAL(QN_TOKEN, n->tag.children[2]->type)
  ASSERT_STRING_EQ("שלום", n->tag.children[2]->tn.str)

  ASSERT_EQUAL(QN_TOKEN, n->tag.children[3]->type)
  ASSERT_STRING_EQ("lorem ipsum", n->tag.children[3]->tn.str)

  Query_Free(q);
  IndexSpec_Free(ctx.spec);

  RETURN_TEST_SUCCESS;
}
// void benchmarkQueryParser() {
//   char *qt = "(hello|world) \"another world\"";
//   char *err = NULL;

//   RSSearcreq = SEARCH_REQUEST(qt, NULL);

//   q = NewQueryParseCtx(&req);

//   QueryParseCtx *q = NewQuery(NULL, qt, strlen(qt), 0, 1, 0xff, 0, "en", DefaultStopWordList(),
//                               NULL, -1, 0, NULL, (RSPayload){}, NULL);
//   TIME_SAMPLE_RUN_LOOP(50000, { Query_Parse(q, &err); });
// }

void RMUTil_InitAlloc();
TEST_MAIN({
  RMUTil_InitAlloc();
  LOGGING_INIT(L_INFO);
  TESTFUNC(testTags)
  TESTFUNC(testGeoQuery);
  TESTFUNC(testQueryParser);
  TESTFUNC(testPureNegative);
  TESTFUNC(testFieldSpec);
  TESTFUNC(testAttributes);
  // benchmarkQueryParser();
});

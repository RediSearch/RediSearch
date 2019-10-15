#include "../query.h"
#include "../query_parser/tokenizer.h"
#include "../stopwords.h"
#include "../extension.h"
#include "../ext/default.h"
#include <stdio.h>
#include <gtest/gtest.h>

#define QUERY_PARSE_CTX(ctx, qt, opts) NewQueryParseCtx(&ctx, qt, strlen(qt), &opts);

struct SearchOptionsCXX : RSSearchOptions {
  SearchOptionsCXX() {
    memset(this, 0, sizeof(*this));
    flags = RS_DEFAULT_QUERY_FLAGS;
    fieldmask = RS_FIELDMASK_ALL;
    language = "en";
    stopwords = DefaultStopWordList();
  }
};

class QASTCXX : public QueryAST {
  SearchOptionsCXX m_opts;
  QueryError m_status = {QueryErrorCode(0)};
  RedisSearchCtx *sctx = NULL;

 public:
  QASTCXX() {
    memset(static_cast<QueryAST *>(this), 0, sizeof(QueryAST));
  }
  QASTCXX(RedisSearchCtx &sctx) : QASTCXX() {
    setContext(&sctx);
  }
  void setContext(RedisSearchCtx *sctx) {
    this->sctx = sctx;
  }

  bool parse(const char *s) {
    QueryError_ClearError(&m_status);
    QAST_Destroy(this);

    int rc = QAST_Parse(this, sctx, &m_opts, s, strlen(s), &m_status);
    return rc == REDISMODULE_OK && !QueryError_HasError(&m_status) && root != NULL;
  }

  void print() const {
    QAST_Print(this, sctx->spec);
  }

  const char *getError() const {
    return QueryError_GetError(&m_status);
  }

  ~QASTCXX() {
    QueryError_ClearError(&m_status);
    QAST_Destroy(this);
  }
};

bool isValidQuery(const char *qt, RedisSearchCtx &ctx) {
  QASTCXX ast;
  ast.setContext(&ctx);
  return ast.parse(qt);

  // if (err) {
  //   Query_Free(q);
  //   fprintf(stderr, "Error parsing query '%s': %s\n", qt, err);
  //   free(err);
  //   return 1;
  // }
  // Query_Free(q);

  // return 0;
}

#define assertValidQuery(qt, ctx) ASSERT_TRUE(isValidQuery(qt, ctx))
#define assertInvalidQuery(qt, ctx) ASSERT_FALSE(isValidQuery(qt, ctx))

class QueryTest : public ::testing::Test {};

TEST_F(QueryTest, testParser) {
  RedisSearchCtx ctx;
  static const char *args[] = {"SCHEMA",  "title", "text",   "weight", "0.1",
                               "body",    "text",  "weight", "2.0",    "bar",
                               "numeric", "loc",   "geo",    "tags",   "tag"};
  QueryError err = {QueryErrorCode(0)};
  ctx.spec = IndexSpec_Parse("idx", args, sizeof(args) / sizeof(const char *), &err);
  ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetError(&err);

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

  const char *qt = "(hello|world) and \"another world\" (foo is bar) -(baz boo*)";
  QASTCXX ast;
  ast.setContext(&ctx);
  ASSERT_TRUE(ast.parse(qt));
  QueryNode *n = ast.root;
  QAST_Print(&ast, ctx.spec);
  ASSERT_TRUE(n != NULL);
  ASSERT_EQ(n->type, QN_PHRASE);
  ASSERT_EQ(n->pn.exact, 0);
  ASSERT_EQ(QueryNode_NumChildren(n), 4);
  ASSERT_EQ(n->opts.fieldMask, RS_FIELDMASK_ALL);

  ASSERT_TRUE(n->children[0]->type == QN_UNION);
  ASSERT_STREQ("hello", n->children[0]->children[0]->tn.str);
  ASSERT_STREQ("world", n->children[0]->children[1]->tn.str);

  QueryNode *_n = n->children[1];

  ASSERT_TRUE(_n->type == QN_PHRASE);
  ASSERT_TRUE(_n->pn.exact == 1);
  ASSERT_EQ(QueryNode_NumChildren(_n), 2);
  ASSERT_STREQ("another", _n->children[0]->tn.str);
  ASSERT_STREQ("world", _n->children[1]->tn.str);

  _n = n->children[2];
  ASSERT_TRUE(_n->type == QN_PHRASE);

  ASSERT_TRUE(_n->pn.exact == 0);
  ASSERT_EQ(QueryNode_NumChildren(_n), 2);
  ASSERT_STREQ("foo", _n->children[0]->tn.str);
  ASSERT_STREQ("bar", _n->children[1]->tn.str);

  _n = n->children[3];
  ASSERT_TRUE(_n->type == QN_NOT);
  _n = QueryNode_GetChild(_n, 0);
  ASSERT_TRUE(_n->pn.exact == 0);
  ASSERT_EQ(2, QueryNode_NumChildren(_n));
  ASSERT_STREQ("baz", _n->children[0]->tn.str);

  ASSERT_EQ(_n->children[1]->type, QN_PREFX);
  ASSERT_STREQ("boo", _n->children[1]->pfx.str);
  QAST_Destroy(&ast);
  IndexSpec_Free(ctx.spec);
}

TEST_F(QueryTest, testPureNegative) {
  const char *qs[] = {"-@title:hello", "-hello", "@title:-hello", "-(foo)", "-foo", "(-foo)", NULL};
  static const char *args[] = {"SCHEMA", "title",  "text", "weight", "0.1",    "body",
                               "text",   "weight", "2.0",  "bar",    "numeric"};
  QueryError err = {QueryErrorCode(0)};
  IndexSpec *spec = IndexSpec_Parse("idx", args, sizeof(args) / sizeof(const char *), &err);
  RedisSearchCtx ctx = SEARCH_CTX_STATIC(NULL, spec);
  for (size_t i = 0; qs[i] != NULL; i++) {
    QASTCXX ast;
    ast.setContext(&ctx);
    ASSERT_TRUE(ast.parse(qs[i])) << ast.getError();
    QueryNode *n = ast.root;
    ASSERT_TRUE(n != NULL);
    ASSERT_EQ(n->type, QN_NOT);
    ASSERT_TRUE(QueryNode_GetChild(n, 0) != NULL);
  }
  IndexSpec_Free(ctx.spec);
}

TEST_F(QueryTest, testGeoQuery) {
  static const char *args[] = {"SCHEMA", "title", "text", "loc", "geo"};
  QueryError err = {QueryErrorCode(0)};
  IndexSpec *spec = IndexSpec_Parse("idx", args, sizeof(args) / sizeof(const char *), &err);
  RedisSearchCtx ctx = SEARCH_CTX_STATIC(NULL, spec);
  const char *qt = "@title:hello world @loc:[31.52 32.1342 10.01 km]";
  QASTCXX ast;
  ast.setContext(&ctx);
  ASSERT_TRUE(ast.parse(qt)) << ast.getError();
  QueryNode *n = ast.root;
  ASSERT_EQ(n->type, QN_PHRASE);
  ASSERT_TRUE((n->opts.fieldMask == RS_FIELDMASK_ALL));
  ASSERT_EQ(QueryNode_NumChildren(n), 2);

  QueryNode *gn = n->children[1];
  ASSERT_EQ(gn->type, QN_GEO);
  ASSERT_STREQ(gn->gn.gf->property, "loc");
  ASSERT_EQ(gn->gn.gf->unitType, GEO_DISTANCE_KM);
  ASSERT_EQ(gn->gn.gf->lon, 31.52);
  ASSERT_EQ(gn->gn.gf->lat, 32.1342);
  ASSERT_EQ(gn->gn.gf->radius, 10.01);
  IndexSpec_Free(ctx.spec);
}

TEST_F(QueryTest, testFieldSpec) {
  static const char *args[] = {"SCHEMA", "title",  "text", "weight", "0.1",    "body",
                               "text",   "weight", "2.0",  "bar",    "numeric"};
  QueryError err = {QUERY_OK};
  IndexSpec *spec = IndexSpec_Parse("idx", args, sizeof(args) / sizeof(const char *), &err);
  RedisSearchCtx ctx = SEARCH_CTX_STATIC(NULL, spec);
  const char *qt = "@title:hello world";
  QASTCXX ast(ctx);
  ASSERT_TRUE(ast.parse(qt)) << ast.getError();
  ast.print();
  QueryNode *n = ast.root;
  ASSERT_EQ(n->type, QN_PHRASE);
  ASSERT_EQ(n->opts.fieldMask, 0x01);

  qt = "(@title:hello) (@body:world)";
  ASSERT_TRUE(ast.parse(qt)) << ast.getError();
  n = ast.root;

  ASSERT_TRUE(n != NULL);
  printf("%s ====> ", qt);
  ast.print();
  ASSERT_EQ(n->type, QN_PHRASE);
  ASSERT_EQ(n->opts.fieldMask, RS_FIELDMASK_ALL);
  ASSERT_EQ(n->children[0]->opts.fieldMask, 0x01);
  ASSERT_EQ(n->children[1]->opts.fieldMask, 0x02);

  // test field modifiers
  qt = "@title:(hello world) @body:(world apart) @adas_dfsd:fofofof";
  ASSERT_TRUE(ast.parse(qt)) << ast.getError();
  n = ast.root;
  printf("%s ====> ", qt);
  ast.print();
  ASSERT_EQ(n->type, QN_PHRASE);
  ASSERT_EQ(n->opts.fieldMask, RS_FIELDMASK_ALL);
  ASSERT_EQ(QueryNode_NumChildren(n), 3);
  ASSERT_EQ(n->children[0]->opts.fieldMask, 0x01);
  ASSERT_EQ(n->children[1]->opts.fieldMask, 0x02);
  ASSERT_EQ(n->children[2]->opts.fieldMask, 0x00);
  // ASSERT_EQ(n->children[2]->fieldMask, 0x00)

  // test numeric ranges
  qt = "@num:[0.4 (500]";
  ASSERT_TRUE(ast.parse(qt)) << ast.getError();
  n = ast.root;
  ASSERT_EQ(n->type, QN_NUMERIC);
  ASSERT_EQ(n->nn.nf->min, 0.4);
  ASSERT_EQ(n->nn.nf->max, 500.0);
  ASSERT_EQ(n->nn.nf->inclusiveMin, 1);
  ASSERT_EQ(n->nn.nf->inclusiveMax, 0);
  IndexSpec_Free(ctx.spec);
}

TEST_F(QueryTest, testAttributes) {
  static const char *args[] = {"SCHEMA", "title", "text", "body", "text"};
  QueryError err = {QUERY_OK};
  IndexSpec *spec = IndexSpec_Parse("idx", args, sizeof(args) / sizeof(const char *), &err);
  RedisSearchCtx ctx = SEARCH_CTX_STATIC(NULL, spec);

  const char *qt =
      "(@title:(foo bar) => {$weight: 0.5} @body:lol => {$weight: 0.2}) => "
      "{$weight:0.3; $slop:2; $inorder:true}";
  QASTCXX ast(ctx);
  ASSERT_TRUE(ast.parse(qt)) << ast.getError();
  QueryNode *n = ast.root;
  ASSERT_EQ(0.3, n->opts.weight);
  ASSERT_EQ(2, n->opts.maxSlop);
  ASSERT_EQ(1, n->opts.inOrder);

  ASSERT_EQ(n->type, QN_PHRASE);
  ASSERT_EQ(QueryNode_NumChildren(n), 2);
  ASSERT_EQ(0.5, n->children[0]->opts.weight);
  ASSERT_EQ(0.2, n->children[1]->opts.weight);
  IndexSpec_Free(ctx.spec);
}

TEST_F(QueryTest, testTags) {
  static const char *args[] = {"SCHEMA", "title", "text", "tags", "tag", "separator", ";"};
  QueryError err = {QUERY_OK};
  IndexSpec *spec = IndexSpec_Parse("idx", args, sizeof(args) / sizeof(const char *), &err);
  RedisSearchCtx ctx = SEARCH_CTX_STATIC(NULL, spec);

  const char *qt = "@tags:{hello world  |foo| שלום|  lorem\\ ipsum    }";
  QASTCXX ast(ctx);
  ASSERT_TRUE(ast.parse(qt)) << ast.getError();
  ast.print();
  QueryNode *n = ast.root;
  ASSERT_EQ(n->type, QN_TAG);
  ASSERT_EQ(4, QueryNode_NumChildren(n));
  ASSERT_EQ(QN_PHRASE, n->children[0]->type);
  ASSERT_STREQ("hello", n->children[0]->children[0]->tn.str);
  ASSERT_STREQ("world", n->children[0]->children[1]->tn.str);

  ASSERT_EQ(QN_TOKEN, n->children[1]->type);
  ASSERT_STREQ("foo", n->children[1]->tn.str);

  ASSERT_EQ(QN_TOKEN, n->children[2]->type);
  ASSERT_STREQ("שלום", n->children[2]->tn.str);

  ASSERT_EQ(QN_TOKEN, n->children[3]->type);
  ASSERT_STREQ("lorem ipsum", n->children[3]->tn.str);
  IndexSpec_Free(ctx.spec);
}

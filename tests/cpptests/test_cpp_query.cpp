
#include "src/query.h"
#include "src/query_parser/tokenizer.h"
#include "src/stopwords.h"
#include "src/extension.h"
#include "src/ext/default.h"
#include "src/util/references.h"

#include "gtest/gtest.h"

#include <stdio.h>

#define QUERY_PARSE_CTX(ctx, qt, opts) NewQueryParseCtx(&ctx, qt, strlen(qt), &opts);

struct SearchOptionsCXX : RSSearchOptions {
  SearchOptionsCXX() {
    memset(this, 0, sizeof(*this));
    flags = RS_DEFAULT_QUERY_FLAGS;
    fieldmask = RS_FIELDMASK_ALL;
    language = DEFAULT_LANGUAGE;
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
    return parse(s, 1);
  }
  bool parse(const char *s, int ver) {
    QueryError_ClearError(&m_status);
    QAST_Destroy(this);

    int rc = QAST_Parse(this, sctx, &m_opts, s, strlen(s), ver, &m_status);
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

bool isValidQuery(const char *qt, int ver, RedisSearchCtx &ctx) {
  QASTCXX ast;
  ast.setContext(&ctx);
  return ast.parse(qt, ver);

  // if (err) {
  //   Query_Free(q);
  //   fprintf(stderr, "Error parsing query '%s': %s\n", qt, err);
  //   free(err);
  //   return 1;
  // }
  // Query_Free(q);

  // return 0;
}

#define assertValidQuery(qt, ctx) ASSERT_TRUE(isValidQuery(qt, version, ctx))
#define assertInvalidQuery(qt, ctx) ASSERT_FALSE(isValidQuery(qt, version, ctx))

#define assertValidQuery_v(v, qt) ASSERT_TRUE(isValidQuery(qt, v, ctx))
#define assertInvalidQuery_v(v, qt) ASSERT_FALSE(isValidQuery(qt, v, ctx))

class QueryTest : public ::testing::Test {};

TEST_F(QueryTest, testParser_delta) {
  RedisSearchCtx ctx;
  static const char *args[] = {"SCHEMA",  "title", "text",   "weight", "0.1",
                               "body",    "text",  "weight", "2.0",    "bar",
                               "numeric", "loc",   "geo",    "tags",   "tag"};
  QueryError err = {QueryErrorCode(0)};
  StrongRef ref = IndexSpec_Parse("idx", args, sizeof(args) / sizeof(const char *), &err);
  ctx.spec = (IndexSpec *)StrongRef_Get(ref);
  ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetError(&err);

  // wildcard with parentheses are avalible from version 2
  assertInvalidQuery_v(1, "(*)");
  assertValidQuery_v(2, "(*)");

  // params are avalible from version 2.
  assertInvalidQuery_v(1, "$hello");
  assertValidQuery_v(2, "$hello");
  assertInvalidQuery_v(1, "\"$hello\"");
  assertValidQuery_v(2, "\"$hello\"");

  // difference between `expr` and `text_expr` were introduced in version 2
  assertValidQuery_v(1, "@title:@num:[0 10]");
  assertValidQuery_v(1, "@title:(@num:[0 10])");
  assertValidQuery_v(1, "@t1:@t2:@t3:hello");
  assertInvalidQuery_v(2, "@title:@num:[0 10]");
  assertInvalidQuery_v(2, "@title:(@num:[0 10])");
  assertInvalidQuery_v(2, "@t1:@t2:@t3:hello");

  // minor bug in v1
  assertValidQuery_v(1, "@title:{foo}}}}}");
  assertInvalidQuery_v(2, "@title:{foo}}}}}");

  // Test basic vector similarity query - invalid in version 1
  assertInvalidQuery_v(1, "*=>[KNN 10 @vec_field $BLOB]");
  assertInvalidQuery_v(1, "*=>[knn $K @vec_field $BLOB as as]");
  assertInvalidQuery_v(1, "*=>[KNN $KNN @KNN $KNN KNN $KNN AS $AS]");
  assertInvalidQuery_v(1, "*=>[KNN $K @vec_field $BLOB]");
  assertInvalidQuery_v(1, "*=>[KNN $K @vec_field $BLOB AS score]");
  assertInvalidQuery_v(1, "*=>[KNN $K @vec_field $BLOB EF $ef foo bar x 5 AS score]");
  assertInvalidQuery_v(1, "*=>[KNN $K @vec_field $BLOB foo bar x 5]");

  StrongRef_Release(ref);
}

TEST_F(QueryTest, testParser_v1) {
  RedisSearchCtx ctx;
  static const char *args[] = {"SCHEMA",  "title", "text",   "weight", "0.1",
                               "body",    "text",  "weight", "2.0",    "bar",
                               "numeric", "loc",   "geo",    "tags",   "tag"};
  QueryError err = {QueryErrorCode(0)};
  StrongRef ref = IndexSpec_Parse("idx", args, sizeof(args) / sizeof(const char *), &err);
  ctx.spec = (IndexSpec *)StrongRef_Get(ref);
  ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetError(&err);
  int version = 1;

  // test some valid queries
  assertValidQuery("hello", ctx);

  assertValidQuery("*", ctx);

  assertValidQuery("hello wor*", ctx);
  assertValidQuery("hello world", ctx);
  assertValidQuery("hello (world)", ctx);

  assertValidQuery("\"hello world\"", ctx);
  assertValidQuery("\"hello\"", ctx);
  assertInvalidQuery("\"$hello\"", ctx);
  assertValidQuery("\"\\$hello\"", ctx);
  assertValidQuery("\"\\@hello\"", ctx);

  assertValidQuery("\"hello world\" \"foo bar\"", ctx);
  assertValidQuery("\"hello world\"|\"foo bar\"", ctx);
  assertValidQuery("\"hello world\" (\"foo bar\")", ctx);
  assertValidQuery("hello \"foo bar\" world", ctx);
  assertValidQuery("hello|hallo|yellow world", ctx);
  assertValidQuery("(hello|world|foo) bar baz 123", ctx);
  assertValidQuery("(hello|world|foo) (bar baz)", ctx);
  assertValidQuery("@a:foo (@b:bar (@c:baz @d:gaz))", ctx);
  assertValidQuery("(hello world|foo \"bar baz\") \"bar baz\" bbbb", ctx);
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
  assertValidQuery("@title:@num:[0 10]", ctx);
  assertValidQuery("@title:(@num:[0 10])", ctx);
  assertValidQuery("@t1:@t2:@t3:hello", ctx);
  assertValidQuery("@t1|t2|t3:hello", ctx);
  assertValidQuery("@title:(hello=>{$phonetic: true} world)", ctx);
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

  assertInvalidQuery("@title:{{{{{foo}", ctx);
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
  assertValidQuery("@title:(conversation) (@title:(conversation the conversation))=>{$inorder: true;$slop: 0}", ctx);
  assertValidQuery("(foo => {$weight: 0.5;}) | ((bar) => {$weight: 0.5})", ctx);
  assertValidQuery("(foo => {$weight: 0.5;})  ((bar) => {}) => {}", ctx);
  assertValidQuery("@tag:{foo | bar} => {$weight: 0.5;} ", ctx);
  assertValidQuery("@num:[0 100] => {$weight: 0.5;} ", ctx);
  assertInvalidQuery("@tag:{foo | bar} => {$weight: -0.5;} ", ctx);
  assertInvalidQuery("@tag:{foo | bar} => {$great: 0.5;} ", ctx);
  assertInvalidQuery("@tag:{foo | bar} => {$great:;} ", ctx);
  assertInvalidQuery("@tag:{foo | bar} => {$:1;} ", ctx);
  assertInvalidQuery(" => {$weight: 0.5;} ", ctx);

  assertValidQuery("@title:((hello world)|((hello world)|(hallo world|werld) | hello world werld))", ctx);
  assertValidQuery("(hello world)|((hello world)|(hallo world|werld) | hello world werld)", ctx);

  const char *qt = "(hello|world) and \"another world\" (foo is bar) -(baz boo*)";
  QASTCXX ast;
  ast.setContext(&ctx);
  ASSERT_TRUE(ast.parse(qt));
  QueryNode *n = ast.root;
  //QAST_Print(&ast, ctx.spec);
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

  ASSERT_EQ(_n->children[1]->type, QN_PREFIX);
  ASSERT_STREQ("boo", _n->children[1]->pfx.tok.str);
  QAST_Destroy(&ast);
  StrongRef_Release(ref);
}

TEST_F(QueryTest, testParser_v2) {
  RedisSearchCtx ctx;
  static const char *args[] = {"SCHEMA",  "title", "text",   "weight", "0.1",
                               "body",    "text",  "weight", "2.0",    "bar",
                               "numeric", "loc",   "geo",    "tags",   "tag"};
  QueryError err = {QueryErrorCode(0)};
  StrongRef ref = IndexSpec_Parse("idx", args, sizeof(args) / sizeof(const char *), &err);
  ctx.spec = (IndexSpec *)StrongRef_Get(ref);
  ASSERT_FALSE(QueryError_HasError(&err)) << QueryError_GetError(&err);
  int version = 2;

  // test some valid queries
  assertValidQuery("hello", ctx);

  assertValidQuery("*", ctx);
  assertValidQuery("(*)", ctx);
  assertValidQuery("((((((*))))))", ctx);
  assertInvalidQuery("((((*))))))", ctx);

  assertValidQuery("hello wor*", ctx);
  assertValidQuery("hello world", ctx);
  assertValidQuery("hello (world)", ctx);

  assertValidQuery("\"hello world\"", ctx);
  assertValidQuery("\"hello\"", ctx);
  assertValidQuery("\"$hello\"", ctx);
  assertValidQuery("\"\\$hello\"", ctx);
  assertValidQuery("\"\\@hello\"", ctx);

  assertValidQuery("\"hello world\" \"foo bar\"", ctx);
  assertValidQuery("\"hello world\"|\"foo bar\"", ctx);
  assertValidQuery("\"hello world\" (\"foo bar\")", ctx);
  assertValidQuery("hello \"foo bar\" world", ctx);
  assertValidQuery("hello|hallo|yellow world", ctx);
  assertValidQuery("(hello|world|foo) bar baz 123", ctx);
  assertValidQuery("(hello|world|foo) (bar baz)", ctx);
  assertValidQuery("@a:foo (@b:bar (@c:baz @d:gaz))", ctx);
  assertValidQuery("(hello world|foo \"bar baz\") \"bar baz\" bbbb", ctx);
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
  assertInvalidQuery("@title:@num:[0 10]", ctx);
  assertInvalidQuery("@title:(@num:[0 10])", ctx);
  assertInvalidQuery("@t1:@t2:@t3:hello", ctx);
  assertValidQuery("@t1|t2|t3:hello", ctx);
  assertValidQuery("@title:(hello=>{$phonetic: true} world)", ctx);
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

  assertInvalidQuery("@title:{foo}}}}}", ctx);
  assertInvalidQuery("@title:{{{{{foo}", ctx);
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
  assertValidQuery("@title:(conversation) (@title:(conversation the conversation))=>{$inorder: true;$slop: 0}", ctx);
  assertValidQuery("(foo => {$weight: 0.5;}) | ((bar) => {$weight: 0.5})", ctx);
  assertValidQuery("(foo => {$weight: 0.5;})  ((bar) => {}) => {}", ctx);
  assertValidQuery("@tag:{foo | bar} => {$weight: 0.5;} ", ctx);
  assertValidQuery("@num:[0 100] => {$weight: 0.5;} ", ctx);
  assertInvalidQuery("@tag:{foo | bar} => {$weight: -0.5;} ", ctx);
  assertInvalidQuery("@tag:{foo | bar} => {$great: 0.5;} ", ctx);
  assertInvalidQuery("@tag:{foo | bar} => {$great:;} ", ctx);
  assertInvalidQuery("@tag:{foo | bar} => {$:1;} ", ctx);
  assertInvalidQuery(" => {$weight: 0.5;} ", ctx);
  // Vector attributes are invalid for non-vector queries.
  assertInvalidQuery("@title:(foo bar) => {$ef_runtime: 100;}", ctx);
  assertInvalidQuery("@title:(foo bar) => {$yield_distance_as:my_dist;}", ctx);
  assertInvalidQuery("@title:(foo bar) => {$weight: 2.0; $ef_runtime: 100;}", ctx);

  // Test basic vector similarity query
  assertValidQuery("*=>[KNN 10 @vec_field $BLOB]", ctx);
  assertValidQuery("*=>[knn $K @vec_field $BLOB as as]", ctx); // using command name lowercase
  assertValidQuery("*=>[KNN $KNN @KNN $KNN KNN $KNN AS $AS]", ctx); // using reserved word as an attribute or field
  assertValidQuery("*=>[KNN $K @vec_field $BLOB]", ctx);
  assertValidQuery("*=>[KNN $K @vec_field $BLOB AS score]", ctx);
  assertValidQuery("*=>[KNN $K @vec_field $BLOB EF $ef foo bar x 5 AS score]", ctx);
  assertValidQuery("*=>[KNN $K @vec_field $BLOB foo bar x 5]", ctx);
  // Using query attributes syntax is also allowed.
  assertValidQuery("*=>[knn $K @vec_field $BLOB]=>{$yield_distance_as: vec_dist;}", ctx);
  assertValidQuery("*=>[knn $K @vec_field $BLOB]=>{$yield_distance_as: as;}", ctx); // using stop-word as the attribute value
  assertValidQuery("*=>[KNN $KNN @KNN $KNN KNN $KNN]=>{$yield_distance_as: VECTOR_RANGE;}", ctx); // using reserved word as an attribute or field
  assertValidQuery("*=>[KNN $K @vec_field $BLOB] =>{$yield_distance_as: vec_dist; $ef_runtime: 100;}", ctx);
  assertValidQuery("*=>[KNN $K @vec_field $BLOB] =>{$weight: 2.0; $ef_runtime: 100;}", ctx); // weight is valid, but ignored

  // Test basic vector similarity query combined with other expressions
  // This should fail for now because right now we only allow KNN query to be the root node.
  assertInvalidQuery("*=>[KNN $K @vec_field $BLOB] title=>{$weight: 0.5; $slop: 2}", ctx);
  assertInvalidQuery("*=>[KNN $K1 @vec_field $BLOB1] OR *=>[KNN $K2 @vec_field $BLOB2]", ctx);

  // Test basic vector similarity query errors
  assertInvalidQuery("*=>[ANN $K @vec_field $BLOB]", ctx); // wrong command name
  assertInvalidQuery("*=>[KNN $K @vec_field BLOB]", ctx); // pass vector as value (must be an attribute)
  assertInvalidQuery("*=>[KNN $K vec_field $BLOB]", ctx); // wrong field value (must be @field)
  assertInvalidQuery("*=>[KNN K @vec_field $BLOB]", ctx); // wrong k value (can be an attribute or integer)
  assertInvalidQuery("*=>[KNN 3.14 @vec_field $BLOB]", ctx); // wrong k value (can be an attribute or integer)
  assertInvalidQuery("*=>[KNN -42 @vec_field $BLOB]", ctx); // wrong k value (can be an attribute or integer)
  assertInvalidQuery("*=>[KNN $K @vec_field $BLOB $EF ef foo bar x 5 AS score]", ctx); // parameter as attribute
  assertInvalidQuery("*=>[KNN $K @vec_field $BLOB EF ef foo bar x 5 AS ]", ctx); // not specifying score field name
  assertInvalidQuery("*=>[KNN $K @vec_field $BLOB EF ef foo bar x]", ctx); // missing parameter value (passing only key)
  assertInvalidQuery("*=>[KNN $K @vec_field $BLOB => {$yield:dist}]", ctx); // invalid attributes syntax
  assertInvalidQuery("*=>[KNN $K @vec_field $BLOB EF_RUNTIME 100 => {$yield_distance_as:dist;}]", ctx); // invalid combined syntax
  assertInvalidQuery("*=>[KNN $K @vec_field $BLOB EF_RUNTIME 100] => {$bad_attr:dist;}", ctx); // invalid vector attribute

  // Test simple hybrid vector query
  assertValidQuery("KNN=>[KNN 10 @vec_field $BLOB]", ctx); // using KNN command in other context
  assertValidQuery("(hello world)=>[KNN 10 @vec_field $BLOB]", ctx);
  assertValidQuery("(@title:hello)=>[KNN 10 @vec_field $BLOB]", ctx);
  assertValidQuery("@title:hello=>[KNN 10 @vec_field $BLOB]", ctx);
  assertValidQuery("@title:hello=>[KNN 10 @vec_field $BLOB EF_RUNTIME 100 HYBRID_POLICY BATCHES]", ctx);
  assertValidQuery("@title:hello=>[KNN 10 @vec_field $BLOB AS score]", ctx);
  assertValidQuery("@title:hello=>[KNN 10 @vec_field $BLOB] => {$yield_distance_as:score;}", ctx);
  assertValidQuery("hello=>[KNN 10 @vec_field $BLOB] => {$yield_distance_as:score; $hybrid_policy:batches; $BATCH_SIZE:100}", ctx);

  assertValidQuery("hello=>[KNN 10 @vec_field $BLOB]", ctx);
  assertValidQuery("(hello|world)=>[KNN 10 @vec_field $BLOB]", ctx);
  assertValidQuery("@hello:[0 10]=>[KNN 10 @vec_field $BLOB]", ctx);
  assertValidQuery("(@tit_le|bo_dy:barack @body|title|url|something_else:obama)=>[KNN 10 @vec_field $BLOB]", ctx);
  assertValidQuery("(-hello ~world ~war)=>[KNN 10 @vec_field $BLOB]", ctx);
  assertValidQuery("@tags:{bar* | foo}=>[KNN 10 @vec_field $BLOB]", ctx);
  assertValidQuery("(no -as) => {$weight: 0.5} => [KNN 10 @vec_field $BLOB]", ctx);

  // Invalid complex queries with hybrid vector
  assertInvalidQuery("hello world=>[KNN 10 @vec_field $BLOB]", ctx);
  assertInvalidQuery("@title:hello world=>[KNN 10 @vec_field $BLOB]", ctx);
  assertInvalidQuery("(hello world => [KNN 10 @vec_field $BLOB]) other phrase", ctx);
  assertInvalidQuery("(hello world => [KNN 10 @vec_field $BLOB]) @title:other", ctx);
  assertInvalidQuery("hello world => [KNN 10 @vec_field $BLOB] OR other => [KNN 10 @vec_field $BLOB]", ctx);

  // Test range queries
  assertValidQuery("@v:[VECTOR_RANGE 0.01 $BLOB]", ctx);
  assertValidQuery("@v:[vector_range 0.01 $BLOB]", ctx);
  assertValidQuery("@v:[vEcToR_RaNgE 0.01 $BLOB]", ctx);
  assertValidQuery("@v:[VECTOR_RANGE 2 $BLOB]", ctx);
  assertValidQuery("@v:[VECTOR_RANGE $radius $BLOB]", ctx);
  assertValidQuery("@v:[VECTOR_RANGE 2e-2 $BLOB]", ctx);
  assertValidQuery("@v:[VECTOR_RANGE 2E-2 $BLOB]", ctx);
  assertValidQuery("@v:[VECTOR_RANGE 0.01 $BLOB]=>{$yield_distance_as: V_SCORE;}", ctx);
  assertValidQuery("@v:[VECTOR_RANGE 0.01 $BLOB]=>{$yield_distance_as: as;}", ctx);
  assertValidQuery("@v:[VECTOR_RANGE 0.01 $BLOB]=>{$epsilon: 0.01;}", ctx);
  assertValidQuery("@v:[VECTOR_RANGE 0.01 $BLOB]=>{$epsilon: 0.01; $yield_distance_as: V_SCORE;}", ctx);
  assertValidQuery("@v:[VECTOR_RANGE $r $BLOB]=>{$epsilon: 0.01; $yield_distance_as: V_SCORE;}", ctx);

  // Complex queries with range
  assertValidQuery("@v:[VECTOR_RANGE 0.01 $BLOB] @text:foo OR bar", ctx);
  assertValidQuery("(@v:[VECTOR_RANGE 0.01 $BLOB] @text:foo) => { $weight: 2.0 }", ctx);
  assertValidQuery("@v:[VECTOR_RANGE 0.01 $BLOB] @text:foo OR bar @v:[VECTOR_RANGE 0.04 $BLOB2]", ctx);
  assertValidQuery("(@v:[VECTOR_RANGE 0.01 $BLOB] @text:foo) => [KNN 5 @v $BLOB2]", ctx);
  assertValidQuery("@v:[VECTOR_RANGE 0.01 $BLOB] => [KNN 5 @v2 $BLOB2 AS second_score]", ctx);
  assertValidQuery("@v:[VECTOR_RANGE 0.01 $BLOB]=>{$yield_distance_as: score1;} => [KNN 5 @v2 $BLOB2 AS second_score]", ctx);
  assertValidQuery("@v:[VECTOR_RANGE 0.01 $BLOB]=>{$yield_distance_as: score1;} => [KNN 5 @v2 $BLOB2] => {$yield_distance_as:second_score;}", ctx);
  assertValidQuery("@v:[VECTOR_RANGE 0.01 $BLOB] VECTOR_RANGE", ctx); // Fallback VECTOR_RANGE into a term.

  // Invalid queries
  assertInvalidQuery("@v:[vector-range 0.01 $BLOB]", ctx);
  assertInvalidQuery("@v:[BAD 0.01 $BLOB]", ctx);
  assertInvalidQuery("@v:[VECTOR_RANGE 0.01]", ctx);
  assertInvalidQuery("@v:[VECTOR_RANGE $BLOB]", ctx);
  assertInvalidQuery("@v:[VECTOR_RANGE bad $BLOB]", ctx);
  assertInvalidQuery("@v:[VECTOR_RANGE 0.01 param]", ctx);
  assertInvalidQuery("@v:[VECTOR_RANGE 0.01 param val $BLOB]", ctx);

  assertValidQuery("@title:((hello world)|((hello world)|(hallo world|werld) | hello world werld))", ctx);
  assertValidQuery("(hello world)|((hello world)|(hallo world|werld) | hello world werld)", ctx);

  assertValidQuery("hello 13 again", ctx);

  assertValidQuery("w'hello'", ctx);
  assertValidQuery("w'\\hello'", ctx);
  assertValidQuery("w'\\\\hello'", ctx);
  assertValidQuery("w'he\\\\llo'", ctx);
  assertValidQuery("w'he\\\\llo'", ctx);

  const char *qt = "(hello|world) and \"another world\" (foo is bar) -(baz boo*)";
  QASTCXX ast;
  ast.setContext(&ctx);
  ASSERT_TRUE(ast.parse(qt, version));
  QueryNode *n = ast.root;
  //QAST_Print(&ast, ctx.spec);
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

  ASSERT_EQ(_n->children[1]->type, QN_PREFIX);
  ASSERT_STREQ("boo", _n->children[1]->pfx.tok.str);
  QAST_Destroy(&ast);
  StrongRef_Release(ref);
}

TEST_F(QueryTest, testVectorHybridQuery) {
  static const char *args[] = {"SCHEMA", "title", "text", "vec", "vector", "HNSW", "6",
                               "TYPE", "FLOAT32", "DIM", "5", "DISTANCE_METRIC", "L2"};
  QueryError err = {QueryErrorCode(0)};
  StrongRef ref = IndexSpec_Parse("idx", args, sizeof(args) / sizeof(const char *), &err);
  RedisSearchCtx ctx = SEARCH_CTX_STATIC(NULL, (IndexSpec *)StrongRef_Get(ref));
  QASTCXX ast;
  ast.setContext(&ctx);
  int ver = 2;

  const char *vqt[] = {
    "(hello world)=>[KNN 10 @vec $BLOB]",
    "@title:(hello|world)=>[KNN 10 @vec $BLOB]",
    "@title:hello=>[KNN 10 @vec $BLOB]",
    NULL};

  for (size_t i = 0; vqt[i] != NULL; i++) {
    ASSERT_TRUE(ast.parse(vqt[i], ver));
    QueryNode *vn = ast.root;
    // ast.print();
    ASSERT_TRUE(vn != NULL);
    ASSERT_EQ(vn->type, QN_VECTOR);
    ASSERT_EQ(QueryNode_NumChildren(vn), 1);
  }

  ast.parse(vqt[0], ver);
  ASSERT_EQ(ast.root->children[0]->type, QN_PHRASE);
  ASSERT_EQ(ast.root->children[0]->opts.fieldMask, -1);
  ast.parse(vqt[1], ver);
  ASSERT_EQ(ast.root->children[0]->type, QN_UNION);
  ASSERT_EQ(ast.root->children[0]->opts.fieldMask, 0x01);
  ast.parse(vqt[2], ver);
  ASSERT_EQ(ast.root->children[0]->type, QN_TOKEN);
  ASSERT_EQ(ast.root->children[0]->opts.fieldMask, 0x01);

  StrongRef_Release(ref);
}

TEST_F(QueryTest, testPureNegative) {
  const char *qs[] = {"-@title:hello", "-hello", "@title:-hello", "-(foo)", "-foo", "(-foo)", NULL};
  static const char *args[] = {"SCHEMA", "title",  "text", "weight", "0.1",    "body",
                               "text",   "weight", "2.0",  "bar",    "numeric"};
  QueryError err = {QueryErrorCode(0)};
  StrongRef ref = IndexSpec_Parse("idx", args, sizeof(args) / sizeof(const char *), &err);
  RedisSearchCtx ctx = SEARCH_CTX_STATIC(NULL, (IndexSpec *)StrongRef_Get(ref));
  for (size_t i = 0; qs[i] != NULL; i++) {
    QASTCXX ast;
    ast.setContext(&ctx);
    ASSERT_TRUE(ast.parse(qs[i])) << ast.getError();
    QueryNode *n = ast.root;
    ASSERT_TRUE(n != NULL);
    ASSERT_EQ(n->type, QN_NOT);
    ASSERT_TRUE(QueryNode_GetChild(n, 0) != NULL);
  }
  StrongRef_Release(ref);
}

TEST_F(QueryTest, testGeoQuery_v1) {
  static const char *args[] = {"SCHEMA", "title", "text", "loc", "geo"};
  QueryError err = {QueryErrorCode(0)};
  StrongRef ref = IndexSpec_Parse("idx", args, sizeof(args) / sizeof(const char *), &err);
  RedisSearchCtx ctx = SEARCH_CTX_STATIC(NULL, (IndexSpec *)StrongRef_Get(ref));
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
  StrongRef_Release(ref);
}

TEST_F(QueryTest, testGeoQuery_v2) {
  static const char *args[] = {"SCHEMA", "title", "text", "loc", "geo"};
  QueryError err = {QueryErrorCode(0)};
  StrongRef ref = IndexSpec_Parse("idx", args, sizeof(args) / sizeof(const char *), &err);
  RedisSearchCtx ctx = SEARCH_CTX_STATIC(NULL, (IndexSpec *)StrongRef_Get(ref));
  const char *qt = "@title:hello world @loc:[31.52 32.1342 10.01 km]";
  QASTCXX ast;
  ast.setContext(&ctx);
  int ver = 2;

  ASSERT_TRUE(ast.parse(qt, ver)) << ast.getError();
  QueryNode *n = ast.root;
  ASSERT_EQ(n->type, QN_PHRASE);
  ASSERT_TRUE((n->opts.fieldMask == RS_FIELDMASK_ALL));
  ASSERT_EQ(QueryNode_NumChildren(n), 3);

  QueryNode *gn = n->children[2];
  ASSERT_EQ(gn->type, QN_GEO);
  ASSERT_STREQ(gn->gn.gf->property, "loc");
  ASSERT_EQ(gn->gn.gf->unitType, GEO_DISTANCE_KM);
  ASSERT_EQ(gn->gn.gf->lon, 31.52);
  ASSERT_EQ(gn->gn.gf->lat, 32.1342);
  ASSERT_EQ(gn->gn.gf->radius, 10.01);
  StrongRef_Release(ref);
}

TEST_F(QueryTest, testFieldSpec_v1) {
  static const char *args[] = {"SCHEMA", "title",  "text", "weight", "0.1",    "body",
                               "text",   "weight", "2.0",  "bar",    "numeric"};
  QueryError err = {QUERY_OK};
  StrongRef ref = IndexSpec_Parse("idx", args, sizeof(args) / sizeof(const char *), &err);
  RedisSearchCtx ctx = SEARCH_CTX_STATIC(NULL, (IndexSpec *)StrongRef_Get(ref));
  const char *qt = "@title:hello world";
  QASTCXX ast(ctx);
  ASSERT_TRUE(ast.parse(qt)) << ast.getError();
  //ast.print();
  QueryNode *n = ast.root;
  ASSERT_EQ(n->type, QN_PHRASE);
  ASSERT_EQ(QueryNode_NumChildren(n), 2);
  ASSERT_EQ(n->opts.fieldMask, 0x01);
  ASSERT_EQ(n->children[0]->opts.fieldMask, 0x01);
  ASSERT_EQ(n->children[1]->opts.fieldMask, 0x01);

  qt = "(@title:hello) (@body:world)";
  ASSERT_TRUE(ast.parse(qt)) << ast.getError();
  n = ast.root;

  ASSERT_TRUE(n != NULL);
  //printf("%s ====> ", qt);
  //ast.print();
  ASSERT_EQ(n->type, QN_PHRASE);
  ASSERT_EQ(n->opts.fieldMask, RS_FIELDMASK_ALL);
  ASSERT_EQ(n->children[0]->opts.fieldMask, 0x01);
  ASSERT_EQ(n->children[1]->opts.fieldMask, 0x02);

  // test field modifiers
  qt = "@title:(hello world) @body:(world apart) @adas_dfsd:fofofof";
  ASSERT_TRUE(ast.parse(qt)) << ast.getError();
  n = ast.root;
  //printf("%s ====> ", qt);
  //ast.print();
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
  StrongRef_Release(ref);
}

TEST_F(QueryTest, testFieldSpec_v2) {
  static const char *args[] = {"SCHEMA", "title",  "text", "weight", "0.1",    "body",
                               "text",   "weight", "2.0",  "bar",    "numeric"};
  QueryError err = {QUERY_OK};
  StrongRef ref = IndexSpec_Parse("idx", args, sizeof(args) / sizeof(const char *), &err);
  RedisSearchCtx ctx = SEARCH_CTX_STATIC(NULL, (IndexSpec *)StrongRef_Get(ref));
  const char *qt = "@title:hello world";
  QASTCXX ast(ctx);
  int ver = 2;

  ASSERT_TRUE(ast.parse(qt, ver)) << ast.getError();
  //ast.print();
  QueryNode *n = ast.root;
  ASSERT_EQ(n->type, QN_PHRASE);
  ASSERT_EQ(QueryNode_NumChildren(n), 2);
  ASSERT_EQ(n->opts.fieldMask, RS_FIELDMASK_ALL);
  ASSERT_EQ(n->children[0]->opts.fieldMask, 0x01);
  ASSERT_EQ(n->children[1]->opts.fieldMask, RS_FIELDMASK_ALL);

  qt = "(@title:hello) (@body:world)";
  ASSERT_TRUE(ast.parse(qt, ver)) << ast.getError();
  n = ast.root;

  ASSERT_TRUE(n != NULL);
  //printf("%s ====> ", qt);
  //ast.print();
  ASSERT_EQ(n->type, QN_PHRASE);
  ASSERT_EQ(n->opts.fieldMask, RS_FIELDMASK_ALL);
  ASSERT_EQ(n->children[0]->opts.fieldMask, 0x01);
  ASSERT_EQ(n->children[1]->opts.fieldMask, 0x02);

  // test field modifiers
  qt = "@title:(hello world) @body:(world apart) @adas_dfsd:fofofof";
  ASSERT_TRUE(ast.parse(qt, ver)) << ast.getError();
  n = ast.root;
  //printf("%s ====> ", qt);
  //ast.print();
  ASSERT_EQ(n->type, QN_PHRASE);
  ASSERT_EQ(n->opts.fieldMask, RS_FIELDMASK_ALL);
  ASSERT_EQ(QueryNode_NumChildren(n), 3);
  ASSERT_EQ(n->children[0]->opts.fieldMask, 0x01);
  ASSERT_EQ(n->children[1]->opts.fieldMask, 0x02);
  ASSERT_EQ(n->children[2]->opts.fieldMask, 0x00);
  // ASSERT_EQ(n->children[2]->fieldMask, 0x00)

  // test numeric ranges
  qt = "@num:[0.4 (500]";
  ASSERT_TRUE(ast.parse(qt, ver)) << ast.getError();
  n = ast.root;
  ASSERT_EQ(n->type, QN_NUMERIC);
  ASSERT_EQ(n->nn.nf->min, 0.4);
  ASSERT_EQ(n->nn.nf->max, 500.0);
  ASSERT_EQ(n->nn.nf->inclusiveMin, 1);
  ASSERT_EQ(n->nn.nf->inclusiveMax, 0);
  StrongRef_Release(ref);
}

TEST_F(QueryTest, testAttributes) {
  static const char *args[] = {"SCHEMA", "title", "text", "body", "text"};
  QueryError err = {QUERY_OK};
  StrongRef ref = IndexSpec_Parse("idx", args, sizeof(args) / sizeof(const char *), &err);
  RedisSearchCtx ctx = SEARCH_CTX_STATIC(NULL, (IndexSpec *)StrongRef_Get(ref));

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
  StrongRef_Release(ref);
}

TEST_F(QueryTest, testTags) {
  static const char *args[] = {"SCHEMA", "title", "text", "tags", "tag", "separator", ";"};
  QueryError err = {QUERY_OK};
  StrongRef ref = IndexSpec_Parse("idx", args, sizeof(args) / sizeof(const char *), &err);
  RedisSearchCtx ctx = SEARCH_CTX_STATIC(NULL, (IndexSpec *)StrongRef_Get(ref));

  const char *qt = "@tags:{hello world  |foo| שלום|  lorem\\ ipsum    }";
  QASTCXX ast(ctx);
  ASSERT_TRUE(ast.parse(qt)) << ast.getError();
  //ast.print();
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
  ASSERT_STREQ("lorem\\ ipsum", n->children[3]->tn.str);
  StrongRef_Release(ref);
}

TEST_F(QueryTest, testWildcard) {
  static const char *args[] = {"SCHEMA", "title", "text"};
  QueryError err = {QUERY_OK};
  StrongRef ref = IndexSpec_Parse("idx", args, sizeof(args) / sizeof(const char *), &err);
  RedisSearchCtx ctx = SEARCH_CTX_STATIC(NULL, (IndexSpec *)StrongRef_Get(ref));

  const char *qt = "w'hello world'";
  QASTCXX ast(ctx);
  ASSERT_TRUE(ast.parse(qt, 2)) << ast.getError();
  QueryNode *n = ast.root;
  ASSERT_EQ(n->type, QN_WILDCARD_QUERY);
  ASSERT_EQ(11, n->verb.tok.len);
  ASSERT_STREQ("hello world", n->verb.tok.str);

  qt = "w'?*?*?'";
  ASSERT_TRUE(ast.parse(qt, 2)) << ast.getError();
  n = ast.root;
  ASSERT_EQ(n->type, QN_WILDCARD_QUERY);
  ASSERT_EQ(5, n->verb.tok.len);
  ASSERT_STREQ("?*?*?", n->verb.tok.str);

  StrongRef_Release(ref);
}

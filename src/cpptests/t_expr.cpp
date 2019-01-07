#include <gtest/gtest.h>
#include <aggregate/expr/expression.h>
#include <aggregate/expr/exprast.h>
#include <aggregate/functions/function.h>
#include <util/arr.h>

class ExprTest : public ::testing::Test {
 public:
  static void SetUpTestCase() {
    RegisterAllFunctions();
  }
};

struct EvalCtx : ExprEval {
  QueryError status_s = {QueryErrorCode(0)};
  RSValue res_s = {RSValue_Null};

  EvalCtx(const char *s) {
    lookup = NULL;
    root = NULL;
    assign(s);
  }

  EvalCtx(RSExpr *root_) {
    err = &status_s;
    lookup = NULL;
    root = root_;
  }

  void assign(const char *s) {
    clear();

    memset(static_cast<ExprEval *>(this), 0, sizeof(ExprEval));

    root = ExprAST_Parse(s, strlen(s), &status_s);
    if (!root) {
      assert(QueryError_HasError(&status_s));
    }
    lookup = NULL;
  }

  int bindLookupKeys() {
    assert(lookup);
    return ExprAST_GetLookupKeys((RSExpr *)root, (RLookup *)lookup, &status_s);
  }

  int eval() {
    return ExprEval_Eval(this, &res_s);
  }

  EvalCtx operator=(EvalCtx &) = delete;
  EvalCtx(const EvalCtx &) = delete;

  RSValue &result() {
    return res_s;
  }

  const char *error() const {
    return QueryError_GetError(&status_s);
  }

  operator bool() const {
    return root && !QueryError_HasError(&status_s);
  }

  void clear() {
    QueryError_ClearError(&status_s);

    RSValue_Clear(&res_s);
    memset(&res_s, 0, sizeof(res_s));

    if (root) {
      ExprAST_Free(const_cast<RSExpr *>(root));
      root = NULL;
    }
  }

  ~EvalCtx() {
    clear();
  }
};

TEST_F(ExprTest, testExpr) {
  RSExpr *l = RS_NewNumberLiteral(2);
  RSExpr *r = RS_NewNumberLiteral(4);
  RSExpr *op = RS_NewOp('+', l, r);
  QueryError status = {QueryErrorCode(0)};
  EvalCtx eval(op);

  int rc = eval.eval();
  ASSERT_EQ(EXPR_EVAL_OK, rc);
  ASSERT_EQ(RSValue_Number, eval.result().t);
  ASSERT_EQ(6, eval.result().numval);
}

TEST_F(ExprTest, testParser) {
  const char *e = "(((2 + 2) * (3 / 4) + 2 % 3 - 0.43) ^ -3)";
  QueryError status = {QueryErrorCode(0)};
  RSExpr *root = ExprAST_Parse(e, strlen(e), &status);
  if (!root) {
    FAIL() << "Could not parse expression";
  }
  ASSERT_TRUE(root != NULL);
  // ExprAST_Print(root);
  // printf("\n");

  EvalCtx eval(root);
  int rc = eval.eval();
  ASSERT_EQ(EXPR_EVAL_OK, rc);
  ASSERT_EQ(RSValue_Number, eval.result().t);
  // RSValue_Print(&eval.result());
}

TEST_F(ExprTest, testGetFields) {
  const char *e = "@foo + sqrt(@bar) / @baz + ' '";
  QueryError status = {QueryErrorCode(0)};
  RSExpr *root = ExprAST_Parse(e, strlen(e), &status);
  ASSERT_TRUE(root) << "Failed to parse query " << e << " " << QueryError_GetError(&status);
  RLookup lk;

  RLookup_Init(&lk, NULL);
  auto *kfoo = RLookup_GetKey(&lk, "foo", RLOOKUP_F_OCREAT);
  auto *kbar = RLookup_GetKey(&lk, "bar", RLOOKUP_F_OCREAT);
  auto *kbaz = RLookup_GetKey(&lk, "baz", RLOOKUP_F_OCREAT);
  int rc = ExprAST_GetLookupKeys(root, &lk, &status);
  ASSERT_EQ(EXPR_EVAL_OK, rc);
  RLookup_Cleanup(&lk);
  ExprAST_Free(root);
}

TEST_F(ExprTest, testFunction) {
  const char *e = "floor(log2(35) + sqrt(4) % 10) - abs(-5/20)";
  EvalCtx ctx(e);
  // ExprAST_Print(ctx.root);
  int rc = ctx.eval();
  if (rc != EXPR_EVAL_OK) {
    FAIL() << "Could not parse " << e << " " << ctx.error();
  }
  ASSERT_EQ(RSValue_Number, ctx.result().t);
  // RSValue_Print(&ctx.result());
}

struct EvalResult {
  double rv;
  bool success;
  std::string errmsg;

  static EvalResult failure(const QueryError *status = NULL) {
    return EvalResult{0, false, status ? QueryError_GetError(status) : ""};
  }

  static EvalResult ok(double rv) {
    return {rv, true};
  }
};

static EvalResult testEval(const char *e, RLookup *lk, RLookupRow *rr, QueryError *status) {
  RSExpr *root = ExprAST_Parse(e, strlen(e), status);
  if (root == NULL) {
    assert(QueryError_HasError(status));
    return EvalResult::failure(status);
  }

  EvalCtx ctx(root);
  ctx.lookup = lk;
  ctx.bindLookupKeys();
  ctx.srcrow = rr;
  int rc = ctx.eval();
  if (rc != EXPR_EVAL_OK) {
    return EvalResult::failure(&ctx.status_s);
  }

  return EvalResult::ok(ctx.result().numval);
}

TEST_F(ExprTest, testPredicate) {
  RLookup lk = {0};
  RLookup_Init(&lk, NULL);
  auto *kfoo = RLookup_GetKey(&lk, "foo", RLOOKUP_F_OCREAT);
  auto *kbar = RLookup_GetKey(&lk, "bar", RLOOKUP_F_OCREAT);
  RLookupRow rr = {0};
  RLookup_WriteOwnKey(kfoo, &rr, RS_NumVal(1));
  RLookup_WriteOwnKey(kbar, &rr, RS_NumVal(2));
  // RLookupRow_Dump(&rr);
  QueryError status = {QueryErrorCode(0)};
#define TEST_EVAL(e, expected)                          \
  {                                                     \
    EvalResult restmp = testEval(e, &lk, &rr, &status); \
    ASSERT_TRUE(restmp.success) << restmp.errmsg;       \
    ASSERT_EQ(expected, restmp.rv);                     \
  }

  TEST_EVAL("1 == 1", 1);
  TEST_EVAL("1 < 2", 1);
  TEST_EVAL("1 <= 1", 1);
  TEST_EVAL("-1 == -1", 1);
  TEST_EVAL("-1 == 1", 0);
  TEST_EVAL("1 < 1", 0);
  TEST_EVAL("1 != 1", 0);
  TEST_EVAL("1 != 'foo'", 1);
  TEST_EVAL("1 == NULL", 0);
  TEST_EVAL("1 != NULL", 1);
  TEST_EVAL("'foo' == 'foo'", 1);
  TEST_EVAL("'foo' != 'bar'", 1);
  TEST_EVAL("'foo' != 'foo'", 0);
  TEST_EVAL("'foo' < 'goo'", 1);
  TEST_EVAL("@foo == @bar", 0);
  TEST_EVAL("@foo != @bar", 1);
  TEST_EVAL("@foo != NULL", 1);
  TEST_EVAL("@foo < @bar", 1);
  TEST_EVAL("@foo <= @bar", 1);
  TEST_EVAL("@foo >= @bar", 0);
  TEST_EVAL("@foo > @bar", 0);

  TEST_EVAL("NULL == NULL", 1);
  TEST_EVAL("0 == NULL", 0);
  TEST_EVAL("1 == 1 && 2 ==2 ", 1);
  TEST_EVAL("1 == 1 && 1 ==2 ", 0);
  TEST_EVAL("1 == 1 || 1 ==2 ", 1);
  TEST_EVAL("1 == 3 || 1 ==2 ", 0);
  TEST_EVAL("!(1 == 3)", 1);
  TEST_EVAL("!(1 == 3) || 2", 1);
  TEST_EVAL("!0", 1);
  TEST_EVAL("!1", 0);
  TEST_EVAL("!('foo' == 'bar')", 1);
  TEST_EVAL("!NULL", 1);

  RLookupRow_Cleanup(&rr);
  RLookup_Cleanup(&lk);
}

TEST_F(ExprTest, testNull) {
  EvalCtx ctx("NULL");
  ASSERT_TRUE(ctx) << ctx.error();
  int rc = ctx.eval();
  ASSERT_EQ(EXPR_EVAL_OK, rc) << ctx.error();
  ASSERT_TRUE(RSValue_IsNull(&ctx.result()));

  ctx.assign("null");
  ASSERT_FALSE(ctx);
}

TEST_F(ExprTest, testPropertyFetch) {
  EvalCtx ctx("log(@foo) + 2*sqrt(@bar)");
  RLookup lk;
  RLookup_Init(&lk, NULL);
  RLookupRow rr = {0};
  RLookupKey *kfoo = RLookup_GetKey(&lk, "foo", RLOOKUP_F_OCREAT);
  RLookupKey *kbar = RLookup_GetKey(&lk, "bar", RLOOKUP_F_OCREAT);
  RLookup_WriteOwnKey(kfoo, &rr, RS_NumVal(10));
  RLookup_WriteOwnKey(kbar, &rr, RS_NumVal(10));

  ctx.lookup = &lk;
  ctx.srcrow = &rr;

  int rc = ctx.bindLookupKeys();
  ASSERT_EQ(EXPR_EVAL_OK, rc);
  rc = ctx.eval();
  ASSERT_EQ(EXPR_EVAL_OK, rc);
  ASSERT_EQ(RSValue_Number, ctx.result().t);
  // RSValue_Print(&ctx.result());
  RLookupRow_Cleanup(&rr);
  RLookup_Cleanup(&lk);
}
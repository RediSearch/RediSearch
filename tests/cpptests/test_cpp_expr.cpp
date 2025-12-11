#include "gtest/gtest.h"
#include "aggregate/expr/expression.h"
#include "aggregate/expr/exprast.h"
#include "aggregate/functions/function.h"
#include "util/arr.h"
#include "config.h"

class ExprTest : public ::testing::Test {
 protected:
  bool originalEnableUnstableFeatures;

 public:
  static void SetUpTestCase() {
    RegisterAllFunctions();
  }

  virtual void SetUp() {
    // Save original value and enable unstable features for case() function tests
    originalEnableUnstableFeatures = RSGlobalConfig.enableUnstableFeatures;
    RSGlobalConfig.enableUnstableFeatures = true;
  }

  virtual void TearDown() {
    // Restore original value
    RSGlobalConfig.enableUnstableFeatures = originalEnableUnstableFeatures;
  }
};

struct TEvalCtx : ExprEval {
  QueryError status_s = {QueryErrorCode(0)};
  RSValue res_s = {RSValue_Null};

  TEvalCtx() {
    root = NULL;
    lookup = NULL;
    memset(static_cast<ExprEval *>(this), 0, sizeof(ExprEval));
  }

  TEvalCtx(const char *s) {
    lookup = NULL;
    root = NULL;
    assign(s);
  }

  TEvalCtx(RSExpr *root_) {
    err = &status_s;
    lookup = NULL;
    root = root_;
  }

  void assign(const char *s) {
    clear();

    memset(static_cast<ExprEval *>(this), 0, sizeof(ExprEval));
    err = &status_s;  // Set the error context

    HiddenString* hidden = NewHiddenString(s, strlen(s), false);
    root = ExprAST_Parse(hidden, &status_s);
    HiddenString_Free(hidden, false);
    if (!root) {
      assert(QueryError_HasError(&status_s));
    }
    lookup = NULL;
  }

  std::string dump(bool obfuscate) {
    char *s = ExprAST_Dump((RSExpr *)root, obfuscate);
    std::string ret(s);
    rm_free(s);
    return ret;
  }

  int bindLookupKeys() {
    assert(lookup);
    return ExprAST_GetLookupKeys((RSExpr *)root, (RLookup *)lookup, &status_s);
  }

  int eval() {
    return ExprEval_Eval(this, &res_s);
  }

  TEvalCtx operator=(TEvalCtx &) = delete;
  TEvalCtx(const TEvalCtx &) = delete;

  RSValue &result() {
    return res_s;
  }

  const char *error() const {
    return QueryError_GetUserError(&status_s);
  }

  operator bool() const {
    return root && !QueryError_HasError(&status_s);
  }

  void clear() {
    QueryError_ClearError(&status_s);

    RSValue_Clear(&res_s);
    memset((void *)&res_s, 0, sizeof(res_s));

    if (root) {
      ExprAST_Free(const_cast<RSExpr *>(root));
      root = NULL;
    }
  }

  ~TEvalCtx() {
    clear();
  }
};

TEST_F(ExprTest, testExpr) {
  RSExpr *l = RS_NewNumberLiteral(2);
  RSExpr *r = RS_NewNumberLiteral(4);
  RSExpr *op = RS_NewOp('+', l, r);
  QueryError status = {QueryErrorCode(0)};
  TEvalCtx eval(op);

  int rc = eval.eval();
  ASSERT_EQ(EXPR_EVAL_OK, rc);
  ASSERT_EQ(RSValue_Number, eval.result().t);
  ASSERT_EQ(6, eval.result().numval);
}

TEST_F(ExprTest, testDump) {
  using String = const char *;
  std::map<String, std::pair<String, String>> exprToDump = {
    {"NULL", {"NULL", "NULL"}},
    {"4 + 2", {"(4 + 2)", "(Number + Number)"}},
    {"!9", {"!9", "!Number"}},
    {"((@foo + (sqrt(@bar) / @baz)) + ' ')", {"((@foo + (sqrt(@bar) / @baz)) + \" \")", "((@Text + (sqrt(@Text) / @Text)) + \"Text\")"}},
  };
  for (auto& [expression, pair] : exprToDump) {
    QueryError status = {QueryErrorCode(0)};
    HiddenString *expr = NewHiddenString(expression, strlen(expression), false);
    RSExpr *root = ExprAST_Parse(expr, &status);
    HiddenString_Free(expr, false);
    if (!root) {
      FAIL() << "Could not parse expression " << expression;
    }
    char *value = ExprAST_Dump(root, false);
    ASSERT_STREQ(value, pair.first);
    rm_free(value);
    char *obfuscated = ExprAST_Dump(root, true);
    ASSERT_STREQ(obfuscated, pair.second);
    rm_free(obfuscated);
    ExprAST_Free(root);
  }
}


TEST_F(ExprTest, testParser) {
  const char *e = "(((2 + 2) * (3 / 4) + 2 % 3 - 0.43) ^ -3)";
  QueryError status = {QueryErrorCode(0)};
  HiddenString *hidden = NewHiddenString(e, strlen(e), false);
  RSExpr *root = ExprAST_Parse(hidden, &status);
  HiddenString_Free(hidden, false);
  if (!root) {
    FAIL() << "Could not parse expression";
  }
  ASSERT_TRUE(root != NULL);
  // ExprAST_Print(root);
  // printf("\n");

  TEvalCtx eval(root);
  int rc = eval.eval();
  ASSERT_EQ(EXPR_EVAL_OK, rc);
  ASSERT_EQ(RSValue_Number, eval.result().t);
}

TEST_F(ExprTest, testGetFields) {
  const char *e = "@foo + sqrt(@bar) / @baz + ' '";
  QueryError status = {QueryErrorCode(0)};
  HiddenString *hidden = NewHiddenString(e, strlen(e), false);
  RSExpr *root = ExprAST_Parse(hidden, &status);
  HiddenString_Free(hidden, false);
  ASSERT_TRUE(root) << "Failed to parse query " << e << " " << QueryError_GetUserError(&status);
  RLookup lk;

  RLookup_Init(&lk, NULL);
  auto *kfoo = RLookup_GetKey(&lk, "foo", RLOOKUP_M_WRITE, RLOOKUP_F_NOFLAGS);
  auto *kbar = RLookup_GetKey(&lk, "bar", RLOOKUP_M_WRITE, RLOOKUP_F_NOFLAGS);
  auto *kbaz = RLookup_GetKey(&lk, "baz", RLOOKUP_M_WRITE, RLOOKUP_F_NOFLAGS);
  int rc = ExprAST_GetLookupKeys(root, &lk, &status);
  ASSERT_EQ(EXPR_EVAL_OK, rc);
  RLookup_Cleanup(&lk);
  ExprAST_Free(root);
}

TEST_F(ExprTest, testFunction) {
  const char *e = "floor(log2(35) + sqrt(4) % 10) - abs(-5/20)";
  TEvalCtx ctx(e);
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
    return EvalResult{0, false, status ? QueryError_GetUserError(status) : ""};
  }

  static EvalResult ok(double rv) {
    return {rv, true};
  }
};

static EvalResult testEval(const char *e, RLookup *lk, RLookupRow *rr, QueryError *status) {
  HiddenString* hidden = NewHiddenString(e, strlen(e), false);
  RSExpr *root = ExprAST_Parse(hidden, status);
  HiddenString_Free(hidden, false);
  if (root == NULL) {
    assert(QueryError_HasError(status));
    return EvalResult::failure(status);
  }

  TEvalCtx ctx(root);
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
  auto *kfoo = RLookup_GetKey(&lk, "foo", RLOOKUP_M_WRITE, RLOOKUP_F_NOFLAGS);
  auto *kbar = RLookup_GetKey(&lk, "bar", RLOOKUP_M_WRITE, RLOOKUP_F_NOFLAGS);
  RLookupRow rr = {0};
  RLookup_WriteOwnKey(kfoo, &rr, RS_NumVal(1));
  RLookup_WriteOwnKey(kbar, &rr, RS_NumVal(2));
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
  TEvalCtx ctx("NULL");
  ASSERT_TRUE(ctx) << ctx.error();
  int rc = ctx.eval();
  ASSERT_EQ(EXPR_EVAL_OK, rc) << ctx.error();
  ASSERT_TRUE(RSValue_IsNull(&ctx.result()));

  ctx.assign("null");
  ASSERT_FALSE(ctx);
}

TEST_F(ExprTest, testPropertyFetch) {
  TEvalCtx ctx("log(@foo) + 2*sqrt(@bar)");
  RLookup lk;
  RLookup_Init(&lk, NULL);
  RLookupRow rr = {0};
  RLookupKey *kfoo = RLookup_GetKey(&lk, "foo", RLOOKUP_M_WRITE, RLOOKUP_F_NOFLAGS);
  RLookupKey *kbar = RLookup_GetKey(&lk, "bar", RLOOKUP_M_WRITE, RLOOKUP_F_NOFLAGS);
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

// Macro for testing expression evaluation with expected numeric result
#define ASSERT_EXPR_EVAL_NUMBER(ctx_var, expected_value)    \
  {                                                         \
    ASSERT_TRUE(ctx_var) << ctx_var.error();                \
    ASSERT_EQ(EXPR_EVAL_OK, ctx_var.eval());                \
    auto res = RSValue_Dereference(&ctx_var.result());      \
    ASSERT_EQ(RSValue_Number, res->t);                      \
    ASSERT_EQ(expected_value, res->numval);                 \
  }

TEST_F(ExprTest, testEvalFuncCase) {
  TEvalCtx ctx;

  // Basic case function tests - condition evaluates to true
  ctx.assign("case(1, 42, 99)");
  ASSERT_EXPR_EVAL_NUMBER(ctx, 42);

  ctx.assign("case(0 < 1, 42, 99)");
  ASSERT_EXPR_EVAL_NUMBER(ctx, 42);

  ctx.assign("case(!NULL, 100, 200)");
  ASSERT_EXPR_EVAL_NUMBER(ctx, 100);

  // Basic case function tests - condition evaluates to false
  ctx.assign("case(0, 42, 99)");
  ASSERT_EXPR_EVAL_NUMBER(ctx, 99);

  ctx.assign("case(1 > 2, 100, 200)");
  ASSERT_EXPR_EVAL_NUMBER(ctx, 200);

  ctx.assign("case(NULL, 100, 200)");
  ASSERT_EXPR_EVAL_NUMBER(ctx, 200);

}

TEST_F(ExprTest, testEvalFuncCaseWithComparisons) {
  RLookup lk = {0};
  RLookup_Init(&lk, NULL);
  auto *kfoo = RLookup_GetKey(&lk, "foo", RLOOKUP_M_WRITE, RLOOKUP_F_NOFLAGS);
  auto *kbar = RLookup_GetKey(&lk, "bar", RLOOKUP_M_WRITE, RLOOKUP_F_NOFLAGS);
  RLookupRow rr = {0};
  RLookup_WriteOwnKey(kfoo, &rr, RS_NumVal(5));
  RLookup_WriteOwnKey(kbar, &rr, RS_NumVal(10));

  TEvalCtx ctx("case(@foo < @bar, 1, 0)");  // 5 < 10 is true
  ASSERT_TRUE(ctx) << ctx.error();
  ctx.lookup = &lk;
  ctx.srcrow = &rr;

  ASSERT_EQ(EXPR_EVAL_OK, ctx.bindLookupKeys());
  ASSERT_EXPR_EVAL_NUMBER(ctx, 1);  // @foo < @bar is true, so should return 1

  RLookupRow_Cleanup(&rr);
  RLookup_Cleanup(&lk);
}

TEST_F(ExprTest, testEvalFuncCaseWithExists) {
  RLookup lk = {0};
  RLookup_Init(&lk, NULL);
  auto *kfoo = RLookup_GetKey(&lk, "foo", RLOOKUP_M_WRITE, RLOOKUP_F_NOFLAGS);
  RLookupRow rr = {0};
  RLookup_WriteOwnKey(kfoo, &rr, RS_NumVal(42));

  TEvalCtx ctx("case(exists(@foo), 1, 0)");  // @foo exists
  ASSERT_TRUE(ctx) << ctx.error();
  ctx.lookup = &lk;
  ctx.srcrow = &rr;

  ASSERT_EQ(EXPR_EVAL_OK, ctx.bindLookupKeys());
  ASSERT_EXPR_EVAL_NUMBER(ctx, 1);  // @foo exists, so should return true branch (1)

  // Test with negated exists - should return false branch
  TEvalCtx ctx1("case(!exists(@foo), 1, 0)");  // @foo exists, so !exists(@foo) is false
  ASSERT_TRUE(ctx1) << ctx1.error();
  ctx1.lookup = &lk;
  ctx1.srcrow = &rr;

  ASSERT_EQ(EXPR_EVAL_OK, ctx1.bindLookupKeys());
  ASSERT_EXPR_EVAL_NUMBER(ctx1, 0);  // !exists(@foo) is false, so should return false branch (0)

  RLookupRow_Cleanup(&rr);
  RLookup_Cleanup(&lk);
}

TEST_F(ExprTest, testEvalFuncCaseNested) {
  TEvalCtx ctx;

  // Test nested case expressions
  ctx.assign("case(1, case(1, 'inner_true', 'inner_false'), 'outer_false')");
  ASSERT_TRUE(ctx) << ctx.error();
  ASSERT_EQ(EXPR_EVAL_OK, ctx.eval());
  auto res = RSValue_Dereference(&ctx.result());
  ASSERT_EQ(RSValue_String, res->t);
  ASSERT_STREQ("inner_true", res->strval.str);

  ctx.assign("case(0, 'outer_true', case(1, 'nested_true', 'nested_false'))");
  ASSERT_TRUE(ctx) << ctx.error();
  ASSERT_EQ(EXPR_EVAL_OK, ctx.eval());
  res = RSValue_Dereference(&ctx.result());
  ASSERT_EQ(RSValue_String, res->t);
  ASSERT_STREQ("nested_true", res->strval.str);

  ctx.assign("case(0, 'outer_true', case(0, 'nested_true', 'nested_false'))");
  ASSERT_TRUE(ctx) << ctx.error();
  ASSERT_EQ(EXPR_EVAL_OK, ctx.eval());
  res = RSValue_Dereference(&ctx.result());
  ASSERT_EQ(RSValue_String, res->t);
  ASSERT_STREQ("nested_false", res->strval.str);
}

TEST_F(ExprTest, testEvalFuncCaseWithNullValues) {
  TEvalCtx ctx;

  // Test case with NULL in different positions
  ctx.assign("case(NULL, 'true_branch', 'false_branch')");
  ASSERT_TRUE(ctx) << ctx.error();
  ASSERT_EQ(EXPR_EVAL_OK, ctx.eval());
  auto res = RSValue_Dereference(&ctx.result());
  ASSERT_EQ(RSValue_String, res->t);
  ASSERT_STREQ("false_branch", res->strval.str);

  ctx.assign("case(1, NULL, 'false_branch')");
  ASSERT_TRUE(ctx) << ctx.error();
  ASSERT_EQ(EXPR_EVAL_OK, ctx.eval());
  res = RSValue_Dereference(&ctx.result());
  ASSERT_TRUE(RSValue_IsNull(res));

  ctx.assign("case(0, 'true_branch', NULL)");
  ASSERT_TRUE(ctx) << ctx.error();
  ASSERT_EQ(EXPR_EVAL_OK, ctx.eval());
  res = RSValue_Dereference(&ctx.result());
  ASSERT_TRUE(RSValue_IsNull(res));
}

TEST_F(ExprTest, testEvalFuncCaseErrorConditions) {
  TEvalCtx ctx;

  // Test case with invalid number of arguments (should fail at evaluation time)
  ctx.assign("case()");  // Missing arguments
  ASSERT_TRUE(ctx) << "Should parse case() successfully";
  ASSERT_EQ(EXPR_EVAL_ERR, ctx.eval()) << "Should fail to evaluate case with 0 arguments";
  ASSERT_STREQ("Function `case()` requires exactly 3 arguments", ctx.error());

  ctx.assign("case(1)");  // Missing second and third arguments
  ASSERT_TRUE(ctx) << "Should parse case(1) successfully";
  ASSERT_EQ(EXPR_EVAL_ERR, ctx.eval()) << "Should fail to evaluate case with 1 argument";
  ASSERT_STREQ("Function `case()` requires exactly 3 arguments", ctx.error());

  ctx.assign("case(1, 2)");  // Missing third argument
  ASSERT_TRUE(ctx) << "Should parse case(1, 2) successfully";
  ASSERT_EQ(EXPR_EVAL_ERR, ctx.eval()) << "Should fail to evaluate case with 2 arguments";
  ASSERT_STREQ("Function `case()` requires exactly 3 arguments", ctx.error());

  ctx.assign("case(1, 2, 3, 4)");  // Too many arguments
  ASSERT_TRUE(ctx) << "Should parse case(1, 2, 3, 4) successfully";
  ASSERT_EQ(EXPR_EVAL_ERR, ctx.eval()) << "Should fail to evaluate case with 4 arguments";
  ASSERT_STREQ("Function `case()` requires exactly 3 arguments", ctx.error());

  // Test case with invalid function in condition
  ctx.assign("case(invalid_func(), 'true', 'false')");
  ASSERT_FALSE(ctx) << "Should fail to parse case with invalid function";
}

TEST_F(ExprTest, testEvalFuncCaseShortCircuitEvaluation) {
  RLookup lk = {0};
  RLookup_Init(&lk, NULL);
  auto *kfoo = RLookup_GetKey(&lk, "foo", RLOOKUP_M_WRITE, RLOOKUP_F_NOFLAGS);
  RLookupRow rr = {0};
  RLookup_WriteOwnKey(kfoo, &rr, RS_NumVal(5));

  TEvalCtx ctx("case(1, @foo + 10, @foo / 0)");
  ASSERT_TRUE(ctx) << ctx.error();
  ctx.lookup = &lk;
  ctx.srcrow = &rr;

  // Test that only the selected branch is evaluated
  // When condition is true, only the true branch should be evaluated
  ASSERT_EQ(EXPR_EVAL_OK, ctx.bindLookupKeys());
  ASSERT_EXPR_EVAL_NUMBER(ctx, 15);  // @foo + 10 = 5 + 10 = 15

  RLookupRow_Cleanup(&rr);
  RLookup_Cleanup(&lk);
}

TEST_F(ExprTest, testEvalFuncCaseWithDifferentTypes) {
  TEvalCtx ctx;

  // Test case returning different types based on condition
  ctx.assign("case(1, 42, 'string_result')");
  ASSERT_TRUE(ctx) << ctx.error();
  ASSERT_EQ(EXPR_EVAL_OK, ctx.eval());
  auto res = RSValue_Dereference(&ctx.result());
  ASSERT_EQ(RSValue_Number, res->t);
  ASSERT_EQ(42, res->numval);

  ctx.assign("case(0, 42, 'string_result')");
  ASSERT_TRUE(ctx) << ctx.error();
  ASSERT_EQ(EXPR_EVAL_OK, ctx.eval());
  res = RSValue_Dereference(&ctx.result());
  ASSERT_EQ(RSValue_String, res->t);
  ASSERT_STREQ("string_result", res->strval.str);

  // Test with complex expressions returning different types
  ctx.assign("case(1, 3.14 * 2, 'pi_doubled')");
  ASSERT_EXPR_EVAL_NUMBER(ctx, 6.28);

  // Test returning boolean values
  ctx.assign("case(1, 1==1, 2!=2)");
  ASSERT_TRUE(ctx) << ctx.error();
  ASSERT_EQ(EXPR_EVAL_OK, ctx.eval());
  res = RSValue_Dereference(&ctx.result());
  ASSERT_EQ(RSValue_Number, res->t);
  ASSERT_EQ(1, res->numval);

  ctx.assign("case(0, 1==1, 2!=2)");
  ASSERT_EXPR_EVAL_NUMBER(ctx, 0);

  // Error during evaluation due to missing key
  ctx.assign("case(1, exists(@missing), 0)");
  ASSERT_TRUE(ctx) << ctx.error();
  ASSERT_EQ(EXPR_EVAL_ERR, ctx.eval());

  ctx.assign("case(0, 0, exists(@missing))");
  ASSERT_TRUE(ctx) << ctx.error();
  ASSERT_EQ(EXPR_EVAL_ERR, ctx.eval());
}

TEST_F(ExprTest, testEvalFuncCaseNullComparison) {
  TEvalCtx ctx;

  // Test case where condition uses comparison with NULL
  ctx.assign("case(NULL == NULL, 1, 0)");
  ASSERT_EXPR_EVAL_NUMBER(ctx, 1);  // NULL == NULL should be true

  ctx.assign("case(NULL != NULL, 1, 0)");
  ASSERT_EXPR_EVAL_NUMBER(ctx, 0);  // NULL != NULL should be false
}

TEST_F(ExprTest, testEvalFuncCaseWithDifferentTypeComparison) {
  TEvalCtx ctx;

  // Test case where condition uses comparison with different types
  ctx.assign("case(1 == '1', 1, 0)");
  ASSERT_EXPR_EVAL_NUMBER(ctx, 1);  // 1 == '1' should be true due to type coercion

  ctx.assign("case(1 == '0', 1, 0)");
  ASSERT_EXPR_EVAL_NUMBER(ctx, 0);  // 1 == '0' should be false

  ctx.assign("case(1 == 'hello', 1, 0)");
  ASSERT_EXPR_EVAL_NUMBER(ctx, 0);  // 1 == 'hello' should be false

  ctx.assign("case(1 == NULL, 1, 0)");
  ASSERT_EXPR_EVAL_NUMBER(ctx, 0);  // 1 == NULL should be false

  ctx.assign("case(NULL == 'hello', 1, 0)");
  ASSERT_EXPR_EVAL_NUMBER(ctx, 0);  // NULL == 'hello' should be false
}

#undef ASSERT_EXPR_EVAL_NUMBER

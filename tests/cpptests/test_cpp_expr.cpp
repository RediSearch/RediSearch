/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "gtest/gtest.h"
#include "aggregate/expr/expression.h"
#include "aggregate/expr/exprast.h"
#include "aggregate/functions/function.h"
#include "util/arr.h"
#include "value.h"

class ExprTest : public ::testing::Test {
 public:
  static void SetUpTestCase() {
    RegisterAllFunctions();
  }
};

struct TEvalCtx : ExprEval {
  QueryError status_s = QueryError_Default();
  RSValue *res_s = RSValue_NewUndefined();

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
    res_s = RSValue_NewUndefined();

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
    return ExprEval_Eval(this, res_s);
  }

  TEvalCtx operator=(TEvalCtx &) = delete;
  TEvalCtx(const TEvalCtx &) = delete;

  RSValue *result() {
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

    if (res_s) {
      RSValue_DecrRef(res_s);
      res_s = NULL;
    }

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
  QueryError status = QueryError_Default();
  TEvalCtx eval(op);

  int rc = eval.eval();
  ASSERT_EQ(EXPR_EVAL_OK, rc);
  RSValue *result = eval.result();
  ASSERT_EQ(RSValueType_Number, RSValue_Type(result));
  ASSERT_EQ(6, RSValue_Number_Get(result));
}


TEST_F(ExprTest, testDump) {
  using String = const char *;
  std::map<String, std::pair<String, String>> exprToDump = {
    {"NULL", {"NULL", "NULL"}},
    {"4 + 2", {"6", "Number"}},
    {"!9", {"!9", "!Number"}},
    {"((@foo + (sqrt(@bar) / @baz)) + ' ')", {"((@foo + (sqrt(@bar) / @baz)) + \" \")", "((@Text + (sqrt(@Text) / @Text)) + \"Text\")"}},
  };
  for (auto& [expression, pair] : exprToDump) {
    QueryError status = QueryError_Default();
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

TEST_F(ExprTest, testArithmetics) {
  TEvalCtx ctx;
#define TEST_ARITHMETIC(e, expected)                \
  {                                                 \
    ctx.assign(e);                                  \
    ASSERT_TRUE(ctx) << ctx.error();                \
    ASSERT_EQ(EXPR_EVAL_OK, ctx.eval());            \
    auto res = RSValue_Dereference(ctx.result());   \
    ASSERT_EQ(RSValueType_Number, RSValue_Type(res));   \
    auto numval = RSValue_Number_Get(res);          \
    if (std::isnan(expected)) {                     \
      EXPECT_TRUE(std::isnan(numval));              \
    } else {                                        \
      EXPECT_FLOAT_EQ(expected, numval);            \
    }                                               \
  }

  TEST_ARITHMETIC("3 + 3", 6);
  TEST_ARITHMETIC("3 - 3", 0);
  TEST_ARITHMETIC("3 * 3", 9);
  TEST_ARITHMETIC("3 / 3", 1);
  TEST_ARITHMETIC("3 % 3", 0);
  TEST_ARITHMETIC("3 ^ 3", 27);

  TEST_ARITHMETIC("3 + sqrt(9)", 6);
  TEST_ARITHMETIC("3 - sqrt(9)", 0);
  TEST_ARITHMETIC("3 * sqrt(9)", 9);
  TEST_ARITHMETIC("3 / sqrt(9)", 1);
  TEST_ARITHMETIC("3 % sqrt(9)", 0);
  TEST_ARITHMETIC("3 ^ sqrt(9)", 27);

  TEST_ARITHMETIC("sqrt(9) + 3", 6);
  TEST_ARITHMETIC("sqrt(9) - 3", 0);
  TEST_ARITHMETIC("sqrt(9) * 3", 9);
  TEST_ARITHMETIC("sqrt(9) / 3", 1);
  TEST_ARITHMETIC("sqrt(9) % 3", 0);
  TEST_ARITHMETIC("sqrt(9) ^ 3", 27);

  TEST_ARITHMETIC("sqrt(9) + sqrt(9)", 6);
  TEST_ARITHMETIC("sqrt(9) - sqrt(9)", 0);
  TEST_ARITHMETIC("sqrt(9) * sqrt(9)", 9);
  TEST_ARITHMETIC("sqrt(9) / sqrt(9)", 1);
  TEST_ARITHMETIC("sqrt(9) % sqrt(9)", 0);
  TEST_ARITHMETIC("sqrt(9) ^ sqrt(9)", 27);

  // Test 0 edge cases
  TEST_ARITHMETIC("0 / 0", NAN);
  TEST_ARITHMETIC("0 % 0", NAN);
  TEST_ARITHMETIC("0 ^ 0", 1);
  TEST_ARITHMETIC("1 / 0", INFINITY);
  TEST_ARITHMETIC("1 % 0", NAN);

  TEST_ARITHMETIC("sqrt(0) / 0", NAN);
  TEST_ARITHMETIC("sqrt(0) % 0", NAN);
  TEST_ARITHMETIC("sqrt(0) ^ 0", 1);
  TEST_ARITHMETIC("sqrt(1) / 0", INFINITY);
  TEST_ARITHMETIC("sqrt(1) % 0", NAN);

  TEST_ARITHMETIC("0 / sqrt(0)", NAN);
  TEST_ARITHMETIC("0 % sqrt(0)", NAN);
  TEST_ARITHMETIC("0 ^ sqrt(0)", 1);
  TEST_ARITHMETIC("1 / sqrt(0)", INFINITY);
  TEST_ARITHMETIC("1 % sqrt(0)", NAN);

  TEST_ARITHMETIC("sqrt(0) / sqrt(0)", NAN);
  TEST_ARITHMETIC("sqrt(0) % sqrt(0)", NAN);
  TEST_ARITHMETIC("sqrt(0) ^ sqrt(0)", 1);
  TEST_ARITHMETIC("sqrt(1) / sqrt(0)", INFINITY);
  TEST_ARITHMETIC("sqrt(1) % sqrt(0)", NAN);

}

TEST_F(ExprTest, testParser) {
  const char *e = "(((2 + 2) * (3 / 4) + 2 % 3 - 0.43) ^ -3)";
  QueryError status = QueryError_Default();
  HiddenString *hidden = NewHiddenString(e, strlen(e), false);
  RSExpr *root = ExprAST_Parse(hidden, &status);
  HiddenString_Free(hidden, false);
  ASSERT_TRUE(root) << "Could not parse expression " << e << " " << QueryError_GetUserError(&status);
  // ExprAST_Print(root);
  // printf("\n");

  TEvalCtx eval(root);
  int rc = eval.eval();
  ASSERT_EQ(EXPR_EVAL_OK, rc);
  ASSERT_EQ(RSValueType_Number, RSValue_Type(eval.result()));
}

TEST_F(ExprTest, testGetFields) {
  const char *e = "@foo + sqrt(@bar) / @baz + ' '";
  QueryError status = QueryError_Default();
  HiddenString *hidden = NewHiddenString(e, strlen(e), false);
  RSExpr *root = ExprAST_Parse(hidden, &status);
  HiddenString_Free(hidden, false);
  ASSERT_TRUE(root) << "Failed to parse query " << e << " " << QueryError_GetUserError(&status);
  RLookup lk = RLookup_New();

  auto *kfoo = RLookup_GetKey_Write(&lk, "foo", RLOOKUP_F_NOFLAGS);
  auto *kbar = RLookup_GetKey_Write(&lk, "bar", RLOOKUP_F_NOFLAGS);
  auto *kbaz = RLookup_GetKey_Write(&lk, "baz", RLOOKUP_F_NOFLAGS);
  int rc = ExprAST_GetLookupKeys(root, &lk, &status);
  ASSERT_EQ(EXPR_EVAL_OK, rc);
  RLookup_Cleanup(&lk);
  ExprAST_Free(root);
}

TEST_F(ExprTest, testFunction) {
  const char *e = "floor(log2(35) + sqrt(4) % 10) - abs(-5/20)";
  TEvalCtx ctx(e);

  EXPECT_EQ(ctx.eval(), EXPR_EVAL_OK) << "Could not parse " << e << " " << ctx.error();
  EXPECT_EQ(RSValueType_Number, RSValue_Type(ctx.result()));

  ctx.assign("banana(1, 2, 3)");
  EXPECT_TRUE(!ctx) << "Parsed invalid function";
  EXPECT_STREQ(ctx.error(), "SEARCH_EXPR Unknown function name 'banana'");

  ctx.assign("!banana(1, 2, 3)");
  EXPECT_TRUE(!ctx) << "Parsed invalid function";
  EXPECT_STREQ(ctx.error(), "SEARCH_EXPR Unknown function name 'banana'");

  ctx.assign("!!banana(1, 2, 3)");
  EXPECT_TRUE(!ctx) << "Parsed invalid function";
  EXPECT_STREQ(ctx.error(), "SEARCH_EXPR Unknown function name 'banana'");
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

  return EvalResult::ok(RSValue_Number_Get(RSValue_Dereference(ctx.result())));
}

TEST_F(ExprTest, testPredicate) {
  RLookup lk = RLookup_New();
  auto *kfoo = RLookup_GetKey_Write(&lk, "foo", RLOOKUP_F_NOFLAGS);
  auto *kbar = RLookup_GetKey_Write(&lk, "bar", RLOOKUP_F_NOFLAGS);
  RLookupRow rr = RLookupRow_New();
  RLookup_WriteOwnKey(kfoo, &rr, RSValue_NewNumber(1));
  RLookup_WriteOwnKey(kbar, &rr, RSValue_NewNumber(2));
  QueryError status = QueryError_Default();
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
  TEST_EVAL("!!1", 1);
  TEST_EVAL("!!0", 0);
  TEST_EVAL("!('foo' == 'bar')", 1);
  TEST_EVAL("!NULL", 1);

  // Test order of operations
  TEST_EVAL("1 + 2 * 3", 7);
  TEST_EVAL("1 + 2 * 3 + 4", 11);
  TEST_EVAL("1 + 2 * 3 ^ 2", 19);
  TEST_EVAL("1 + 2 * sqrt(9)", 7);
  TEST_EVAL("1 + sqrt(9) * 2", 7);
  TEST_EVAL("2 * sqrt(9) + 1", 7);
  TEST_EVAL("sqrt(9) * 2 + 1", 7);
  TEST_EVAL("1 + 3 * @bar", 7);
  TEST_EVAL("1 + @bar * 3", 7);
  TEST_EVAL("3 * @bar + 1", 7);
  TEST_EVAL("@bar * 3 + 1", 7);

  RLookupRow_Reset(&rr);
  RLookup_Cleanup(&lk);
}

TEST_F(ExprTest, testNull) {
  TEvalCtx ctx("NULL");
  ASSERT_TRUE(ctx) << ctx.error();
  int rc = ctx.eval();
  ASSERT_EQ(EXPR_EVAL_OK, rc) << ctx.error();
  ASSERT_TRUE(RSValue_IsNull(ctx.result()));

  ctx.assign("null");
  ASSERT_FALSE(ctx);
}

TEST_F(ExprTest, testPropertyFetch) {
  TEvalCtx ctx("log(@foo) + 2*sqrt(@bar)");
  RLookup lk = RLookup_New();
  RLookupRow rr = RLookupRow_New();
  RLookupKey *kfoo = RLookup_GetKey_Write(&lk, "foo", RLOOKUP_F_NOFLAGS);
  RLookupKey *kbar = RLookup_GetKey_Write(&lk, "bar", RLOOKUP_F_NOFLAGS);
  RLookup_WriteOwnKey(kfoo, &rr, RSValue_NewNumber(10));
  RLookup_WriteOwnKey(kbar, &rr, RSValue_NewNumber(10));

  ctx.lookup = &lk;
  ctx.srcrow = &rr;

  int rc = ctx.bindLookupKeys();
  ASSERT_EQ(EXPR_EVAL_OK, rc);
  rc = ctx.eval();
  ASSERT_EQ(EXPR_EVAL_OK, rc);
  ASSERT_EQ(RSValueType_Number, RSValue_Type(ctx.result()));
  ASSERT_FLOAT_EQ(log(10) + 2 * sqrt(10), RSValue_Number_Get(ctx.result()));
  RLookupRow_Reset(&rr);
  RLookup_Cleanup(&lk);
}

// Macro for testing expression evaluation with expected numeric result
#define ASSERT_EXPR_EVAL_NUMBER(ctx_var, expected_value)    \
  {                                                         \
    ASSERT_TRUE(ctx_var) << ctx_var.error();                \
    ASSERT_EQ(EXPR_EVAL_OK, ctx_var.eval());                \
    auto res = RSValue_Dereference(ctx_var.result());       \
    ASSERT_EQ(RSValueType_Number, RSValue_Type(res));           \
    ASSERT_EQ(expected_value, RSValue_Number_Get(res));     \
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
  RLookup lk = RLookup_New();
  auto *kfoo = RLookup_GetKey_Write(&lk, "foo", RLOOKUP_F_NOFLAGS);
  auto *kbar = RLookup_GetKey_Write(&lk, "bar", RLOOKUP_F_NOFLAGS);
  RLookupRow rr = RLookupRow_New();
  RLookup_WriteOwnKey(kfoo, &rr, RSValue_NewNumber(5));
  RLookup_WriteOwnKey(kbar, &rr, RSValue_NewNumber(10));

  TEvalCtx ctx("case(@foo < @bar, 1, 0)");  // 5 < 10 is true
  ASSERT_TRUE(ctx) << ctx.error();
  ctx.lookup = &lk;
  ctx.srcrow = &rr;

  ASSERT_EQ(EXPR_EVAL_OK, ctx.bindLookupKeys());
  ASSERT_EXPR_EVAL_NUMBER(ctx, 1);  // @foo < @bar is true, so should return 1

  RLookupRow_Reset(&rr);
  RLookup_Cleanup(&lk);
}

TEST_F(ExprTest, testEvalFuncCaseWithExists) {
  RLookup lk = RLookup_New();
  auto *kfoo = RLookup_GetKey_Write(&lk, "foo", RLOOKUP_F_NOFLAGS);
  RLookupRow rr = RLookupRow_New();
  RLookup_WriteOwnKey(kfoo, &rr, RSValue_NewNumber(42));

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

  RLookupRow_Reset(&rr);
  RLookup_Cleanup(&lk);
}

TEST_F(ExprTest, testEvalFuncCaseNested) {
  TEvalCtx ctx;

  // Test nested case expressions
  ctx.assign("case(1, case(1, 'inner_true', 'inner_false'), 'outer_false')");
  ASSERT_TRUE(ctx) << ctx.error();
  ASSERT_EQ(EXPR_EVAL_OK, ctx.eval());
  auto res = RSValue_Dereference(ctx.result());
  ASSERT_EQ(RSValueType_String, RSValue_Type(res));
  ASSERT_STREQ("inner_true", RSValue_String_Get(res, NULL));

  ctx.assign("case(0, 'outer_true', case(1, 'nested_true', 'nested_false'))");
  ASSERT_TRUE(ctx) << ctx.error();
  ASSERT_EQ(EXPR_EVAL_OK, ctx.eval());
  res = RSValue_Dereference(ctx.result());
  ASSERT_EQ(RSValueType_String, RSValue_Type(res));
  ASSERT_STREQ("nested_true", RSValue_String_Get(res, NULL));

  ctx.assign("case(0, 'outer_true', case(0, 'nested_true', 'nested_false'))");
  ASSERT_TRUE(ctx) << ctx.error();
  ASSERT_EQ(EXPR_EVAL_OK, ctx.eval());
  res = RSValue_Dereference(ctx.result());
  ASSERT_EQ(RSValueType_String, RSValue_Type(res));
  ASSERT_STREQ("nested_false", RSValue_String_Get(res, NULL));
}

TEST_F(ExprTest, testEvalFuncCaseWithNullValues) {
  TEvalCtx ctx;

  // Test case with NULL in different positions
  ctx.assign("case(NULL, 'true_branch', 'false_branch')");
  ASSERT_TRUE(ctx) << ctx.error();
  ASSERT_EQ(EXPR_EVAL_OK, ctx.eval());
  auto res = RSValue_Dereference(ctx.result());
  ASSERT_EQ(RSValueType_String, RSValue_Type(res));
  ASSERT_STREQ("false_branch", RSValue_String_Get(res, NULL));

  ctx.assign("case(1, NULL, 'false_branch')");
  ASSERT_TRUE(ctx) << ctx.error();
  ASSERT_EQ(EXPR_EVAL_OK, ctx.eval());
  res = RSValue_Dereference(ctx.result());
  ASSERT_TRUE(RSValue_IsNull(res));

  ctx.assign("case(0, 'true_branch', NULL)");
  ASSERT_TRUE(ctx) << ctx.error();
  ASSERT_EQ(EXPR_EVAL_OK, ctx.eval());
  res = RSValue_Dereference(ctx.result());
  ASSERT_TRUE(RSValue_IsNull(res));
}

TEST_F(ExprTest, testEvalFuncCaseErrorConditions) {
  TEvalCtx ctx;

  // Test case with invalid number of arguments (should fail at parse time)
  ctx.assign("case()");  // Missing arguments
  ASSERT_FALSE(ctx) << "Should fail to parse case with only 2 arguments";

  ctx.assign("case(1)");  // Missing second and third arguments
  ASSERT_FALSE(ctx) << "Should fail to parse case with only 2 arguments";

  ctx.assign("case(1, 2)");  // Missing third argument
  ASSERT_FALSE(ctx) << "Should fail to parse case with only 2 arguments";

  ctx.assign("case(1, 2, 3, 4)");  // Too many arguments
  ASSERT_FALSE(ctx) << "Should fail to parse case with 4 arguments";

  // Test case with invalid function in condition
  ctx.assign("case(invalid_func(), 'true', 'false')");
  ASSERT_FALSE(ctx) << "Should fail to parse case with invalid function";
}

TEST_F(ExprTest, testEvalFuncCaseShortCircuitEvaluation) {
  RLookup lk = RLookup_New();
  auto *kfoo = RLookup_GetKey_Write(&lk, "foo", RLOOKUP_F_NOFLAGS);
  RLookupRow rr = RLookupRow_New();
  RLookup_WriteOwnKey(kfoo, &rr, RSValue_NewNumber(5));

  TEvalCtx ctx("case(1, @foo + 10, @foo / 0)");
  ASSERT_TRUE(ctx) << ctx.error();
  ctx.lookup = &lk;
  ctx.srcrow = &rr;

  // Test that only the selected branch is evaluated
  // When condition is true, only the true branch should be evaluated
  ASSERT_EQ(EXPR_EVAL_OK, ctx.bindLookupKeys());
  ASSERT_EXPR_EVAL_NUMBER(ctx, 15);  // @foo + 10 = 5 + 10 = 15

  RLookupRow_Reset(&rr);
  RLookup_Cleanup(&lk);
}

TEST_F(ExprTest, testEvalFuncCaseWithDifferentTypes) {
  TEvalCtx ctx;

  // Test case returning different types based on condition
  ctx.assign("case(1, 42, 'string_result')");
  ASSERT_TRUE(ctx) << ctx.error();
  ASSERT_EQ(EXPR_EVAL_OK, ctx.eval());
  auto res = RSValue_Dereference(ctx.result());
  ASSERT_EQ(RSValueType_Number, RSValue_Type(res));
  ASSERT_EQ(42, RSValue_Number_Get(res));

  ctx.assign("case(0, 42, 'string_result')");
  ASSERT_TRUE(ctx) << ctx.error();
  ASSERT_EQ(EXPR_EVAL_OK, ctx.eval());
  res = RSValue_Dereference(ctx.result());
  ASSERT_EQ(RSValueType_String, RSValue_Type(res));
  ASSERT_STREQ("string_result", RSValue_String_Get(res, NULL));

  // Test with complex expressions returning different types
  ctx.assign("case(1, 3.14 * 2, 'pi_doubled')");
  ASSERT_EXPR_EVAL_NUMBER(ctx, 6.28);

  // Test returning boolean values
  ctx.assign("case(1, 1==1, 2!=2)");
  ASSERT_TRUE(ctx) << ctx.error();
  ASSERT_EQ(EXPR_EVAL_OK, ctx.eval());
  res = RSValue_Dereference(ctx.result());
  ASSERT_EQ(RSValueType_Number, RSValue_Type(res));
  ASSERT_EQ(1, RSValue_Number_Get(res));

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

TEST_F(ExprTest, testEvalCtxEvalExprNullExpr) {
  // Test that EvalCtx_EvalExpr returns EXPR_EVAL_ERR when called with NULL expression
  EvalCtx *ctx = EvalCtx_Create();
  ASSERT_NE(ctx, nullptr);

  // Calling EvalCtx_EvalExpr with NULL should set _expr to NULL and return error
  int rc = EvalCtx_EvalExpr(ctx, nullptr);
  ASSERT_EQ(EXPR_EVAL_ERR, rc);

  EvalCtx_Destroy(ctx);
}

TEST_F(ExprTest, testEvalCtxEvalExprUnknownProperty) {
  // Test that EvalCtx_EvalExpr returns EXPR_EVAL_ERR when the expression
  // references a property that doesn't exist in the lookup registry
  EvalCtx *ctx = EvalCtx_Create();
  ASSERT_NE(ctx, nullptr);

  // Create an expression that references a property @foo
  // The lookup is empty (no properties registered), so this should fail
  RSExpr *expr = RS_NewProp("foo", 3);

  int rc = EvalCtx_EvalExpr(ctx, expr);
  ASSERT_EQ(EXPR_EVAL_ERR, rc);

  // Verify the error message mentions the missing property
  const char *err = QueryError_GetUserError(&ctx->status);
  ASSERT_NE(err, nullptr);
  ASSERT_TRUE(strstr(err, "SEARCH_PROP_NOT_FOUND Property not loaded nor in pipeline: `foo`") != nullptr) << "Error should mention the missing property: " << err;

  // Clean up - we own the expression since EvalCtx_EvalExpr sets _own_expr = false
  ExprAST_Free(expr);
  EvalCtx_Destroy(ctx);
}

#undef ASSERT_EXPR_EVAL_NUMBER

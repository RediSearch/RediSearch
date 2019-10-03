#include "test_util.h"
#include "time_sample.h"
#include <aggregate/expr/expression.h>
#include <aggregate/functions/function.h>
#include <util/arr.h>
#include "../rmutil/alloc.h"

int testExpr() {

  RSExpr *l = RS_NewNumberLiteral(2);
  RSExpr *r = RS_NewNumberLiteral(4);

  RSExpr *op = RS_NewOp('+', l, r);
  RSValue val;
  char *err;
  int rc = RSExpr_Eval(NULL, op, &val, &err);
  ASSERT_EQUAL(EXPR_EVAL_OK, rc);
  ASSERT_EQUAL(RSValue_Number, val.t);
  ASSERT_EQUAL(6, val.numval);

  RETURN_TEST_SUCCESS;
}

int testParser() {
  char *e = "(((2 + 2) * (3 / 4) + 2 % 3 - 0.43) ^ -3)";

  char *err = NULL;
  RSExpr *root = RSExpr_Parse(e, strlen(e), &err);
  if (err != NULL) {
    FAIL("Error parsing expression: %s", err);
  }
  ASSERT(root != NULL);
  RSExpr_Print(root);
  printf("\n");

  RSValue val;
  int rc = RSExpr_Eval(NULL, root, &val, &err);
  ASSERT_EQUAL(EXPR_EVAL_OK, rc);
  ASSERT_EQUAL(RSValue_Number, val.t);
  RSValue_Print(&val);
  RETURN_TEST_SUCCESS;
}

int testGetFields() {
  char *e = "@foo + sqrt(@bar) / @baz + ' '";

  char *err = NULL;
  RSExpr *root = RSExpr_Parse(e, strlen(e), &err);
  if (err != NULL) {
    FAIL("Error parsing expression: %s", err);
  }

  const char **fields = Expr_GetRequiredFields(root);
  ASSERT_EQUAL(3, array_len(fields));
  ASSERT_STRING_EQ("foo", fields[0]);
  ASSERT_STRING_EQ("bar", fields[1]);
  ASSERT_STRING_EQ("baz", fields[2]);
  array_free(fields);
  RETURN_TEST_SUCCESS;
}

int testFunction() {
  // RSFunctionRegistry funcs = {0};
  RegisterMathFunctions();

  char *e = "floor(log2(35) + sqrt(4) % 10) - abs(-5/20)";

  char *err = NULL;
  RSExpr *root = RSExpr_Parse(e, strlen(e), &err);
  if (err != NULL) {
    FAIL("Error parsing expression: %s", err);
  }
  ASSERT(root != NULL);
  RSExpr_Print(root);

  RSExprEvalCtx ctx = {};
  RSValue val;
  int rc = RSExpr_Eval(&ctx, root, &val, &err);
  if (err != NULL) {
    FAIL("Error evaluating expression: %s", err);
  }
  ASSERT_EQUAL(EXPR_EVAL_OK, rc);
  ASSERT_EQUAL(RSValue_Number, val.t);
  RSValue_Print(&val);
  RETURN_TEST_SUCCESS;
}

int testEval(const char *e, SearchResult *r, int expected, char **err) {
  RSExpr *root = RSExpr_Parse(e, strlen(e), err);
  if (root == NULL) return 0;

  // RSExpr_Print(root);

  RSExprEvalCtx ctx = {.r = r};
  RSValue val;
  int rc = RSExpr_Eval(&ctx, root, &val, err);
  if (*err != NULL) {
    return 0;
  }
  // printf(" => ");
  // RSValue_Print(&val);
  // printf("\n-----\n");
  if ((int)val.numval != expected) return 0;
  return 1;
}

int testForErr(const char *e, SearchResult *r, char **err) {
  RSExpr *root = RSExpr_Parse(e, strlen(e), err);
  if (root == NULL) return 0;

  RSExprEvalCtx ctx = {.r = r};
  RSValue val;
  int rc = RSExpr_Eval(&ctx, root, &val, err);
  return *err != NULL;
}

int testPredicate() {
  SearchResult *rs = NewSearchResult();
  rs->docId = 1;
  RSFieldMap_Add(&rs->fields, "foo", RS_NumVal(1));
  RSFieldMap_Add(&rs->fields, "bar", RS_NumVal(2));

  char *err = NULL;

#define TEST_EVAL(e, rs, expected, err)        \
  do {                                         \
    if (!testEval(e, rs, expected, &err)) {    \
      if (err) FAIL("%s", err);                \
      FAIL("Expression eval failed: %s\n", e); \
    }                                          \
  } while(0)

#define TEST_ERR(e, rs, err)                                  \
  do {                                                        \
    if (!testForErr(e, rs, &err)) {                           \
      FAIL("Expression eval did not produce error: %s\n", e); \
    }                                                         \
    *err = 0;                                                 \
  } while(0)

  TEST_EVAL("1 == 1", rs, 1, err);
  TEST_EVAL("1 < 2", rs, 1, err);
  TEST_EVAL("1 <= 1", rs, 1, err);
  TEST_EVAL("-1 == -1", rs, 1, err);
  TEST_EVAL("-1 == 1", rs, 0, err);
  TEST_EVAL("1 < 1", rs, 0, err);
  TEST_EVAL("1 != 1", rs, 0, err);
  TEST_EVAL("1 != 'foo'", rs, 1, err);
  TEST_EVAL("1 != NULL", rs, 1, err);
  TEST_EVAL("'foo' == 'foo'", rs, 1, err);
  TEST_EVAL("'foo' != 'bar'", rs, 1, err);
  TEST_EVAL("'foo' != 'foo'", rs, 0, err);
  TEST_EVAL("'foo' < 'goo'", rs, 1, err);

  TEST_EVAL("@foo == @bar", rs, 0, err);
  TEST_EVAL("@foo != @bar", rs, 1, err);
  TEST_EVAL("@foo != NULL", rs, 1, err);

  TEST_EVAL("@foo < @bar", rs, 1, err);
  TEST_EVAL("@foo <= @bar", rs, 1, err);
  TEST_EVAL("@foo >= @bar", rs, 0, err);
  TEST_EVAL("@foo > @bar", rs, 0, err);

  TEST_EVAL("NULL == NULL", rs, 1, err);
  TEST_EVAL("0 == NULL", rs, 0, err);
  TEST_EVAL("1 == 1 && 2 ==2 ", rs, 1, err);
  TEST_EVAL("1 == 1 && 1 ==2 ", rs, 0, err);
  TEST_EVAL("1 == 1 || 1 ==2 ", rs, 1, err);
  TEST_EVAL("1 == 3 || 1 ==2 ", rs, 0, err);
  TEST_EVAL("!(1 == 3)", rs, 1, err);
  TEST_EVAL("!(1 == 3) || 2", rs, 1, err);
  TEST_EVAL("!0", rs, 1, err);
  TEST_EVAL("!1", rs, 0, err);
  TEST_EVAL("!('foo' == 'bar')", rs, 1, err);

  TEST_EVAL("!NULL", rs, 1, err);

  RETURN_TEST_SUCCESS;
}

int testNull() {
  char *e = "NULL";
  char *err = NULL;
  RSExpr *root = RSExpr_Parse(e, strlen(e), &err);
  ASSERT(root != NULL);
  ASSERT(err == NULL);

  RSExprEvalCtx ctx = {};
  RSValue val;
  int rc = RSExpr_Eval(&ctx, root, &val, &err);
  if (err != NULL) {
    FAIL("Error evaluating expression: %s", err);
  }
  ASSERT_EQUAL(EXPR_EVAL_OK, rc);
  ASSERT(RSValue_IsNull(&val));

  e = "null";
  root = RSExpr_Parse(e, strlen(e), &err);
  ASSERT(root == NULL);
  ASSERT(err != NULL);
  RETURN_TEST_SUCCESS;
}
int testPropertyFetch() {

  RSFunctionRegistry funcs = {0};
  RegisterMathFunctions(&funcs);

  char *e = "log(@foo) + 2*sqrt(@bar)";
  char *err = NULL;

  SearchResult *rs = NewSearchResult();
  rs->docId = 1;
  RSFieldMap_Add(&rs->fields, "foo", RS_NumVal(10));
  RSFieldMap_Add(&rs->fields, "bar", RS_NumVal(10));

  RSExpr *root = RSExpr_Parse(e, strlen(e), &err);
  RSExprEvalCtx ctx = {.r = rs};
  RSValue val;
  int rc = RSExpr_Eval(&ctx, root, &val, &err);
  if (err != NULL) {
    FAIL("Error evaluating expression: %s", err);
  }
  ASSERT_EQUAL(EXPR_EVAL_OK, rc);
  ASSERT_EQUAL(RSValue_Number, val.t);
  RSValue_Print(&val);
  RETURN_TEST_SUCCESS;
  RETURN_TEST_SUCCESS;
}
TEST_MAIN({
  RMUTil_InitAlloc();
  TESTFUNC(testNull);
  TESTFUNC(testPredicate);
  TESTFUNC(testExpr);
  TESTFUNC(testParser);
  TESTFUNC(testFunction);
  TESTFUNC(testPropertyFetch);
  TESTFUNC(testGetFields);
});

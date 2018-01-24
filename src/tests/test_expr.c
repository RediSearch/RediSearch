#include "test_util.h"
#include "time_sample.h"
#include <aggregate/expr/expression.h>

int testExpr() {

  RSExpr *l = RS_NewNumberLiteral(2);
  RSExpr *r = RS_NewNumberLiteral(4);

  RSExpr *op = RS_NewOp('+', l, r);

  RETURN_TEST_SUCCESS;
}
TEST_MAIN({ TESTFUNC(testExpr); });
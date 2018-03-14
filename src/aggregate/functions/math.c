#include "function.h"
#include <aggregate/expr/expression.h>
#include <math.h>

/* Template for single argument double to double math function */
#define NUMERIC_SIMPLE_FUNCTION(f)                                                          \
  static int mathfunc_##f(RSFunctionEvalCtx *ctx, RSValue *result, RSValue *argv, int argc, \
                          char **err) {                                                     \
    if (argc != 1) {                                                                        \
      *err = strdup("Invalid number of arguments for function '" #f);                       \
    }                                                                                       \
    double d;                                                                               \
    if (!RSValue_ToNumber(&argv[0], &d)) {                                                  \
      RSValue_SetNumber(result, NAN);                                                       \
      return EXPR_EVAL_OK;                                                                  \
    }                                                                                       \
    RSValue_SetNumber(result, f(d));                                                        \
    return EXPR_EVAL_OK;                                                                    \
  }

NUMERIC_SIMPLE_FUNCTION(log);
NUMERIC_SIMPLE_FUNCTION(floor);
NUMERIC_SIMPLE_FUNCTION(fabs);
NUMERIC_SIMPLE_FUNCTION(ceil);
NUMERIC_SIMPLE_FUNCTION(sqrt);
NUMERIC_SIMPLE_FUNCTION(log2);
NUMERIC_SIMPLE_FUNCTION(exp);

#define REGISTER_MATHFUNC(reg, name, f) \
  RSFunctionRegistry_RegisterFunction(reg, name, mathfunc_##f);

void RegisterMathFunctions(RSFunctionRegistry *reg) {
  REGISTER_MATHFUNC(reg, "log", log);
  REGISTER_MATHFUNC(reg, "floor", floor);
  REGISTER_MATHFUNC(reg, "abs", fabs);
  REGISTER_MATHFUNC(reg, "ceil", ceil);
  REGISTER_MATHFUNC(reg, "sqrt", sqrt);
  REGISTER_MATHFUNC(reg, "log2", log2);
  REGISTER_MATHFUNC(reg, "exp", exp);
}
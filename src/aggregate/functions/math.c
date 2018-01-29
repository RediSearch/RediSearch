#include "function.h"
#include <aggregate/expr/expression.h>
#include <math.h>

#define VALIDATE_ARGC(f, argc, len)                       \
  {                                                       \
    if (argc != len) {                                    \
      *err = strdup("Invalid arguments for "__STRING(f)); \
      return EXPR_EVAL_ERR;                               \
    }                                                     \
  }

#define VALIDATE_ARG_TYPE(f, arg, typ)                           \
  {                                                              \
    if (arg.t != typ) {                                          \
      *err = strdup("Invalid type for argument in "__STRING(f)); \
      return EXPR_EVAL_ERR;                                      \
    }                                                            \
  }

int func_log(RSValue *result, RSValue *argv, int argc, char **err) {
  VALIDATE_ARGC(log, argc, 1);
  double d;
  if (!RSValue_ToNumber(&argv[0], &d)) {
    return EXPR_EVAL_ERR;
  }

  RSValue_SetNumber(result, log(d));
  return EXPR_EVAL_OK;
}

void RegisterMathFunctions(RSFunctionRegistry *reg) {
  RSFunctionRegistry_RegisterFunction(reg, "log", func_log);
}
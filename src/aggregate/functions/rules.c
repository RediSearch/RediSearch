#include "function.h"
#include <aggregate/expr/expression.h>
#include <math.h>
#include <err.h>


static int stringfunc_prefix(ExprEval *ctx, RSValue *result,
                                RSValue **argv, size_t argc, QueryError *err) {
  // TODO: inject of language and score
  RSValue ret;                                
  ExprEval_Eval(ctx, &ret);
  return REDISMODULE_OK;
}



void RegisterRulesFunctions() {
  RSFunctionRegistry_RegisterFunction("prefix", stringfunc_prefix, RSValue_Number);
}
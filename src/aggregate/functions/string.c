#include <util/minmax.h>
#include <util/array.h>
#include <util/block_alloc.h>
#include <aggregate/expr/expression.h>
#include <ctype.h>

#include "function.h"
#define STRING_BLOCK_SIZE 512

static int stringfunc_tolower(RSValue *result, RSValue *argv, int argc, char **err) {
  if (argc != 1) {
    *err = strdup("Invalid arguments for 'tolower'");
    return EXPR_EVAL_ERR;
  }
  RSValue *v = RSValue_IsString(&argv[0]) ? &argv[0] : RSValue_ToString(&argv[0]);

  size_t sz;
  char *p = (char *)RSValue_StringPtrLen(v, &sz);
  for (size_t i = 0; i < sz; i++) {
    p[i] = tolower(p[i]);
  }
  RSValue_MakeReference(result, v);
  return EXPR_EVAL_OK;
}

static int stringfunc_toupper(RSValue *result, RSValue *argv, int argc, char **err) {
  if (argc != 1) {
    *err = strdup("Invalid arguments for 'tolower'");
    return EXPR_EVAL_ERR;
  }
  RSValue *v = RSValue_IsString(&argv[0]) ? &argv[0] : RSValue_ToString(&argv[0]);

  size_t sz;
  char *p = (char *)RSValue_StringPtrLen(v, &sz);
  for (size_t i = 0; i < sz; i++) {
    p[i] = toupper(p[i]);
  }
  RSValue_MakeReference(result, v);
  return EXPR_EVAL_OK;
}

void RegisterStringFunctions(RSFunctionRegistry *reg) {
  RSFunctionRegistry_RegisterFunction(reg, "lower", stringfunc_tolower);
  RSFunctionRegistry_RegisterFunction(reg, "upper", stringfunc_toupper);
}
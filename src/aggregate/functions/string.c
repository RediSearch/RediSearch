#include <util/minmax.h>
#include <util/array.h>
#include <util/block_alloc.h>
#include <aggregate/expr/expression.h>
#include <ctype.h>

#include "function.h"
#define STRING_BLOCK_SIZE 512

#define VALIDATE_ARGS(fname, minargs, maxargs, err)              \
  if (argc < minargs || argc > maxargs) {                        \
    *err = strdup("Invalid arguments for function '" fname "'"); \
    return EXPR_EVAL_ERR;                                        \
  }

#define VALIDATE_ARG_TYPE(fname, args, idx, type)                                             \
  {                                                                                           \
    RSValue *dref = RSValue_Dereference(&args[idx]);                                          \
    if (dref->t != type) {                                                                    \
                                                                                              \
      asprintf(err, "Invalid type %d for argument %d in function '%s'", dref->t, idx, fname); \
      printf("%s\n", *err);                                                                   \
      return EXPR_EVAL_ERR;                                                                   \
    }                                                                                         \
  }

/* lower(str) */
static int stringfunc_tolower(RSValue *result, RSValue *argv, int argc, char **err) {

  VALIDATE_ARGS("lower", 1, 1, err);
  RSValue *v = RSValue_IsString(&argv[0]) ? &argv[0] : RSValue_ToString(&argv[0]);

  size_t sz;
  char *p = (char *)RSValue_StringPtrLen(v, &sz);
  for (size_t i = 0; i < sz; i++) {
    p[i] = tolower(p[i]);
  }
  RSValue_MakeReference(result, v);
  return EXPR_EVAL_OK;
}

/* uppert(str) */
static int stringfunc_toupper(RSValue *result, RSValue *argv, int argc, char **err) {
  VALIDATE_ARGS("upper", 1, 1, err);

  RSValue *v = RSValue_IsString(&argv[0]) ? &argv[0] : RSValue_ToString(&argv[0]);

  size_t sz;
  char *p = (char *)RSValue_StringPtrLen(v, &sz);
  for (size_t i = 0; i < sz; i++) {
    p[i] = toupper(p[i]);
  }
  RSValue_MakeReference(result, v);
  return EXPR_EVAL_OK;
}

/* substr(str, offset, len) */
static int stringfunc_substr(RSValue *result, RSValue *argv, int argc, char **err) {
  VALIDATE_ARGS("substr", 3, 3, err);
  VALIDATE_ARG_TYPE("substr", argv, 1, RSValue_Number);
  VALIDATE_ARG_TYPE("substr", argv, 2, RSValue_Number);

  size_t sz;
  const char *str = RSValue_StringPtrLen(&argv[0], &sz);
  int offset = (int)RSValue_Dereference(&argv[1])->numval;
  int len = (int)RSValue_Dereference(&argv[2])->numval;

  // for negative offsets we count from the end of the string
  if (offset < 0) {
    offset = (int)sz + offset;
  }
  offset = MAX(0, MIN(offset, sz));
  // len < 0 means read until the end of the string
  if (len < 0) {
    len = MAX(0, (sz - offset) + len);
  }
  if (offset + len > sz) {
    len = sz - offset;
  }

  printf("sz %zd, offset %d, len %d\n", sz, offset, len);
  char *dup = strndup(&str[offset], len);
  RSValue_SetString(result, dup, len);
  return EXPR_EVAL_OK;
}

void RegisterStringFunctions(RSFunctionRegistry *reg) {
  RSFunctionRegistry_RegisterFunction(reg, "lower", stringfunc_tolower);
  RSFunctionRegistry_RegisterFunction(reg, "upper", stringfunc_toupper);
  RSFunctionRegistry_RegisterFunction(reg, "substr", stringfunc_substr);
}
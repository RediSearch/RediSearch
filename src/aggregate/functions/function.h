#ifndef RS_FUNCTION_H_
#define RS_FUNCTION_H_

#include <value.h>

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

typedef int (*RSFunction)(RSValue *result, RSValue *argv, int argc, char **err);

typedef struct {
  size_t len;
  size_t cap;
  struct {
    RSFunction f;
    const char *name;
  } * funcs;
} RSFunctionRegistry;

RSFunction RSFunctionRegistry_Get(RSFunctionRegistry *reg, const char *name, size_t len);
int RSFunctionRegistry_RegisterFunction(RSFunctionRegistry *reg, const char *name, RSFunction f);

void RegisterMathFunctions(RSFunctionRegistry *reg);
void RegisterStringFunctions(RSFunctionRegistry *reg);
void RegisterDateFunctions(RSFunctionRegistry *reg);

#endif
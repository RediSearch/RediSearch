#ifndef RS_FUNCTION_H_
#define RS_FUNCTION_H_

#include <value.h>
#include <util/block_alloc.h>

#define VALIDATE_ARGS(fname, minargs, maxargs, err)              \
  if (argc < minargs || argc > maxargs) {                        \
    *err = strdup("Invalid arguments for function '" fname "'"); \
    return EXPR_EVAL_ERR;                                        \
  }

#define VALIDATE_ARG__COMMON(fname, args, idx, verifier, varg)                                  \
  {                                                                                             \
    RSValue *dref = RSValue_Dereference(&args[idx]);                                            \
    if (!verifier(dref, varg)) {                                                                \
                                                                                                \
      asprintf(err, "Invalid type (%d) for argument %d in function '%s'. %s(v, %s) was false.", \
               dref->t, idx, fname, #verifier, #varg);                                          \
      printf("%s\n", *err);                                                                     \
      return EXPR_EVAL_ERR;                                                                     \
    }                                                                                           \
  }

#define VALIDATE_ARG__TYPE(arg, t_) ((arg)->t == t_)
#define VALIDATE_ARG_TYPE(fname, args, idx, t) \
  VALIDATE_ARG__COMMON(fname, args, idx, VALIDATE_ARG__TYPE, t)

#define VALIDATE_ARG__STRING(arg, unused) RSValue_IsString(arg)
#define VALIDATE_ARG_ISSTRING(fname, args, idx) \
  VALIDATE_ARG__COMMON(fname, args, idx, VALIDATE_ARG__STRING, 0)

typedef struct RSFunctionEvalCtx {
  BlkAlloc alloc;
} RSFunctionEvalCtx;

RSFunctionEvalCtx *RS_NewFunctionEvalCtx();

void RSFunctionEvalCtx_Free(RSFunctionEvalCtx *ctx);

void *RSFunction_Alloc(RSFunctionEvalCtx *ctx, size_t sz);
char *RSFunction_Strndup(RSFunctionEvalCtx *ctx, const char *str, size_t len);

typedef int (*RSFunction)(RSFunctionEvalCtx *ctx, RSValue *result, RSValue *argv, int argc,
                          char **err);

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
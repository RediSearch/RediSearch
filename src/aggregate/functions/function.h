#ifndef RS_FUNCTION_H_
#define RS_FUNCTION_H_

#include <value.h>

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
int RSFunctionRegistry_RegisterFunction(RSFunctionRegistry *ret, const char *name, RSFunction f);

void RegisterMathFunctions(RSFunctionRegistry *reg);

#endif
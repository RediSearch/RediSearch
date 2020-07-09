#define RS_FUNCTION_C_
#include "function.h"

static RSFunctionRegistry functions_g = {0};

RSFunction RSFunctionRegistry_Get(const char *name, size_t len) {

  for (size_t i = 0; i < functions_g.len; i++) {
    if (len == strlen(functions_g.funcs[i].name) &&
        !strncasecmp(functions_g.funcs[i].name, name, len)) {
      return functions_g.funcs[i].f;
    }
  }
  return NULL;
}

int RSFunctionRegistry_RegisterFunction(const char *name, RSFunction f, RSValueType retType) {
  if (functions_g.len + 1 >= functions_g.cap) {
    functions_g.cap += functions_g.cap ? functions_g.cap : 2;
    functions_g.funcs = rm_realloc(functions_g.funcs, functions_g.cap * sizeof(*functions_g.funcs));
  }
  functions_g.funcs[functions_g.len].f = f;
  functions_g.funcs[functions_g.len].name = name;
  functions_g.funcs[functions_g.len].retType = retType;
  functions_g.len++;
  return 1;
}

void RegisterAllFunctions() {
  RegisterMathFunctions();
  RegisterDateFunctions();
  RegisterStringFunctions();
}

void FunctionRegistry_Free(void) {
  rm_free(functions_g.funcs);
  memset(&functions_g, 0, sizeof(functions_g));
}

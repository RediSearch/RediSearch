/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#define RS_FUNCTION_C_
#include "function.h"

static RSFunctionRegistry functions_g = {0};

RSFunctionInfo *RSFunctionRegistry_Get(const char *name, size_t len) {

  for (size_t i = 0; i < functions_g.len; i++) {
    if (STR_EQCASE(name, len, functions_g.funcs[i].name)) {
      return &functions_g.funcs[i];
    }
  }
  return NULL;
}

int RSFunctionRegistry_RegisterFunction(const char *name, RSFunction f, RSValueType retType, uint8_t minArgs,
                                        uint16_t maxArgs) {
  if (functions_g.len + 1 >= functions_g.cap) {
    functions_g.cap += functions_g.cap ? functions_g.cap : 2;
    functions_g.funcs = rm_realloc(functions_g.funcs, functions_g.cap * sizeof(*functions_g.funcs));
  }
  functions_g.funcs[functions_g.len].f = f;
  functions_g.funcs[functions_g.len].name = name;
  functions_g.funcs[functions_g.len].retType = retType;
  functions_g.funcs[functions_g.len].minArgs = minArgs;
  functions_g.funcs[functions_g.len].maxArgs = maxArgs;
  functions_g.len++;
  return 1;
}

void RegisterAllFunctions() {
  RegisterMathFunctions();
  RegisterDateFunctions();
  RegisterStringFunctions();
  RegisterGeoFunctions();
}

void FunctionRegistry_Free(void) {
  rm_free(functions_g.funcs);
  memset(&functions_g, 0, sizeof(functions_g));
}

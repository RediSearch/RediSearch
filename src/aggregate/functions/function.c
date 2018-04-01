#define RS_FUNCTION_C_
#include "function.h"

/* Allocate some memory for a function that can be freed automatically when the execution is done */
inline void *RSFunction_Alloc(RSFunctionEvalCtx *ctx, size_t sz) {
  return BlkAlloc_Alloc(&ctx->alloc, sz, MAX(sz, 1024));
}

char *RSFunction_Strndup(RSFunctionEvalCtx *ctx, const char *str, size_t len) {
  char *ret = RSFunction_Alloc(ctx, len + 1);
  memcpy(ret, str, len);
  ret[len] = '\0';
  return ret;
}

void RSFunctionEvalCtx_Free(RSFunctionEvalCtx *ctx) {
  BlkAlloc_FreeAll(&ctx->alloc, NULL, NULL, 0);
  free(ctx);
}
RSFunctionEvalCtx *RS_NewFunctionEvalCtx() {
  RSFunctionEvalCtx *ret = malloc(sizeof(*ret));
  BlkAlloc_Init(&ret->alloc);
  return ret;
}

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

RSValueType RSFunctionRegistry_GetType(const char *name, size_t len) {
  for (size_t i = 0; i < functions_g.len; i++) {
    if (len == strlen(functions_g.funcs[i].name) &&
        !strncasecmp(functions_g.funcs[i].name, name, len)) {
      return functions_g.funcs[i].retType;
    }
  }
  return RSValue_Null;
}

int RSFunctionRegistry_RegisterFunction(const char *name, RSFunction f, RSValueType retType) {
  if (functions_g.len + 1 >= functions_g.cap) {
    functions_g.cap += functions_g.cap ? functions_g.cap : 2;
    functions_g.funcs = realloc(functions_g.funcs, functions_g.cap * sizeof(*functions_g.funcs));
  }
  functions_g.funcs[functions_g.len].f = f;
  functions_g.funcs[functions_g.len].name = name;
  functions_g.funcs[functions_g.len].retType = retType;
  functions_g.len++;
  return 1;
}

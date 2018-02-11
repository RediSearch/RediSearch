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
RSFunction RSFunctionRegistry_Get(RSFunctionRegistry *reg, const char *name, size_t len) {
  for (size_t i = 0; i < reg->len; i++) {
    if (len == strlen(reg->funcs[i].name) && !strncasecmp(reg->funcs[i].name, name, len)) {
      return reg->funcs[i].f;
    }
  }
  return NULL;
}

int RSFunctionRegistry_RegisterFunction(RSFunctionRegistry *ret, const char *name, RSFunction f) {
  if (ret->len + 1 >= ret->cap) {
    ret->cap += ret->cap ? ret->cap : 2;
    ret->funcs = realloc(ret->funcs, ret->cap * sizeof(*ret->funcs));
  }
  ret->funcs[ret->len].f = f;
  ret->funcs[ret->len].name = name;
  ret->len++;
  return 1;
}

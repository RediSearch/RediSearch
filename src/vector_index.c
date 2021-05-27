#include "vector_index.h"

#define MAX_VECTOR_ELEMENTS 100000

static RS_Vector *openVectorKeysDict(RedisSearchCtx *ctx, RedisModuleString *keyName,
                                             int write) {
  KeysDictValue *kdv = dictFetchValue(ctx->spec->keysDict, keyName);
  if (kdv) {
    return kdv->p;
  }
  if (!write) {
    return NULL;
  }
  kdv = rm_calloc(1, sizeof(*kdv));
  kdv->dtor = (void (*)(void *))RemoveHNSWIndex;
  // TODO: get good values from Dvir
  kdv->p = InitHNSWIndex(MAX_VECTOR_ELEMENTS, 2);
  dictAdd(ctx->spec->keysDict, keyName, kdv);
  return kdv->p;
}

RS_Vector *OpenVectorIndex(RedisSearchCtx *ctx,
                            RedisModuleString *keyName) {
  return openVectorKeysDict(ctx, keyName, 1);
}

/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "geometry_index.h"
#include "geometry/geometry_api.h"
#include "rmalloc.h"
#include "field_spec.h"

void GeometryQuery_Free(GeometryQuery *geomq) {
    rm_free(geomq);
}

RedisModuleType *GeometryIndexType = NULL;
#define GEOMETRYINDEX_KEY_FMT "gm:%s/%s"

RedisModuleString *fmtRedisGeometryIndexKey(RedisSearchCtx *ctx, const char *field) {
  return RedisModule_CreateStringPrintf(ctx->redisCtx, GEOMETRYINDEX_KEY_FMT, ctx->spec->name,
                                        field);
}

static GeometryIndex *openGeometryKeysDict(RedisSearchCtx *ctx, RedisModuleString *keyName, int write, const FieldSpec *fs) {
  KeysDictValue *kdv = dictFetchValue(ctx->spec->keysDict, keyName);
  if (kdv) {
    return kdv->p;
  }
  if (!write) {
    return NULL;
  }
  GeometryApi *api = GeometryApi_GetOrCreate(fs->geometryOpts.geometryLibType, NULL);
  if (!api) {
    return NULL;
  }
  kdv = rm_calloc(1, sizeof(*kdv));
  kdv->dtor = (void (*)(void *))api->freeIndex;
  kdv->p = api->createIndex();
  dictAdd(ctx->spec->keysDict, keyName, kdv);
  return kdv->p;
}

GeometryIndex OpenGeometryIndex(RedisSearchCtx *ctx, RedisModuleString *keyName,
                                 RedisModuleKey **idxKey, const FieldSpec *fs) {

  GeometryIndex ret;
  if (!ctx->spec->keysDict) {
    RedisModuleKey *key_s = NULL;

    if (!idxKey) {
      idxKey = &key_s;
    }

    *idxKey = RedisModule_OpenKey(ctx->redisCtx, keyName, REDISMODULE_READ | REDISMODULE_WRITE);

    int type = RedisModule_KeyType(*idxKey);
    if (type != REDISMODULE_KEYTYPE_EMPTY &&
        RedisModule_ModuleTypeGetType(*idxKey) != GeometryIndexType) {
      return NULL;
    }
    GeometryApi *api = GeometryApi_GetOrCreate(fs->geometryOpts.geometryLibType, NULL);
    if (!api) {
      return NULL;
    }
    /* Create an empty value object if the key is currently empty. */
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
      ret = api->createIndex();
      RedisModule_ModuleTypeSetValue((*idxKey), GeometryIndexType, ret);
    } else {
      ret = RedisModule_ModuleTypeGetValue(*idxKey);
    }
  } else {
    ret = openGeometryKeysDict(ctx, keyName, 1, fs);
  }
  return ret;
}
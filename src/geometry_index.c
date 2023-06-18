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
  if (geomq->str) {
    rm_free((void*)geomq->str);
    rm_free((void*)geomq->attr);
  }
  rm_free(geomq);
}

RedisModuleType *GeometryIndexType = NULL;
#define GEOMETRYINDEX_KEY_FMT "gm:%s/%s"

RedisModuleString *fmtRedisGeometryIndexKey(RedisSearchCtx *ctx, const char *field) {
  return RedisModule_CreateStringPrintf(ctx->redisCtx, GEOMETRYINDEX_KEY_FMT, ctx->spec->name,
                                        field);
}

static GeometryIndex *openGeometryKeysDict(const IndexSpec *spec, RedisModuleString *keyName, int write, const FieldSpec *fs) {
  KeysDictValue *kdv = dictFetchValue(spec->keysDict, keyName);
  if (kdv) {
    return kdv->p;
  }
  if (!write) {
    return NULL;
  }
  kdv = rm_calloc(1, sizeof(*kdv));
  GeometryIndex *idx = GeometryIndexFactory(fs->geometryOpts.geometryCoords);
  const GeometryApi *api = GeometryApi_Get(idx);
  kdv->p = idx;
  kdv->dtor = (void (*)(void *))api->freeIndex;
  dictAdd(spec->keysDict, keyName, kdv);
  return kdv->p;
}

GeometryIndex *OpenGeometryIndex(RedisModuleCtx *redisCtx, IndexSpec *spec,
                                 RedisModuleKey **idxKey, const FieldSpec *fs) {
  GeometryIndex *ret;
  RedisModuleString *keyName = IndexSpec_GetFormattedKey(spec, fs, INDEXFLD_T_GEOMETRY);
  if (!keyName) {
    return NULL;
  }
  if (!spec->keysDict) {
    RedisModuleKey *key_s = NULL;

    if (!idxKey) {
      idxKey = &key_s;
    }

    *idxKey = RedisModule_OpenKey(redisCtx, keyName, REDISMODULE_READ | REDISMODULE_WRITE);

    int type = RedisModule_KeyType(*idxKey);
    if (type != REDISMODULE_KEYTYPE_EMPTY &&
        RedisModule_ModuleTypeGetType(*idxKey) != GeometryIndexType) {
      return NULL;
    }
    /* Create an empty value object if the key is currently empty. */
    if (type == REDISMODULE_KEYTYPE_EMPTY) {
      ret = GeometryIndexFactory(fs->geometryOpts.geometryCoords);
      RedisModule_ModuleTypeSetValue((*idxKey), GeometryIndexType, ret);
    } else {
      ret = RedisModule_ModuleTypeGetValue(*idxKey);
    }
  } else {
    ret = openGeometryKeysDict(spec, keyName, 1, fs);
  }
  return ret;
}

void GeometryIndex_RemoveId(RedisModuleCtx *ctx, IndexSpec *spec, t_docId id) {
  for (int i = 0; i < spec->numFields; ++i) {
    if (spec->fields[i].types & INDEXFLD_T_GEOMETRY) {
      const FieldSpec *fs = spec->fields + i;
      GeometryIndex *idx = OpenGeometryIndex(ctx, spec, NULL, fs);
      if (idx) {
        const GeometryApi *api = GeometryApi_Get(idx);
        api->delGeom(idx, id);
      }
    }
  }
}
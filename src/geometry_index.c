/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "geometry_index.h"
#include "geometry/geometry_api.h"
#include "rmalloc.h"
#include "field_spec.h"
#include "redis_index.h"
#include "obfuscation/obfuscation_api.h"

void GeometryQuery_Free(GeometryQuery *geomq) {
  if (geomq->str) {
    rm_free((void *)geomq->str);
  }
  rm_free(geomq);
}

RedisModuleType *GeometryIndexType = NULL;
#define GEOMETRYINDEX_KEY_FMT "gm:%s/%s"

RedisModuleString *fmtRedisGeometryIndexKey(const RedisSearchCtx *ctx, const HiddenString *field) {
  return RedisModule_CreateStringPrintf(ctx->redisCtx, GEOMETRYINDEX_KEY_FMT, HiddenString_GetUnsafe(ctx->spec->specName, NULL), HiddenString_GetUnsafe(field, NULL));
}

static GeometryIndex *openGeometryKeysDict(const IndexSpec *spec, RedisModuleString *keyName,
                                           bool create_if_missing, const FieldSpec *fs) {
  KeysDictValue *kdv = dictFetchValue(spec->keysDict, keyName);
  if (kdv) {
    return kdv->p;
  }
  if (!create_if_missing) {
    return NULL;
  }

  GeometryIndex *idx = GeometryIndexFactory(fs->geometryOpts.geometryCoords);
  const GeometryApi *api = GeometryApi_Get(idx);

  kdv = rm_malloc(sizeof(*kdv));
  *kdv = (KeysDictValue){
    .p = idx,
    .dtor = (void (*)(void *))api->freeIndex,
  };
  dictAdd(spec->keysDict, keyName, kdv);
  return idx;
}

GeometryIndex *OpenGeometryIndex(IndexSpec *spec, const FieldSpec *fs, bool create_if_missing) {
  RedisModuleString *keyName = IndexSpec_GetFormattedKey(spec, fs, INDEXFLD_T_GEOMETRY);
  return openGeometryKeysDict(spec, keyName, create_if_missing, fs);
}

void GeometryIndex_RemoveId(IndexSpec *spec, t_docId id) {
  for (int i = 0; i < spec->numFields; ++i) {
    if (spec->fields[i].types & INDEXFLD_T_GEOMETRY) {
      const FieldSpec *fs = spec->fields + i;
      GeometryIndex *idx = OpenGeometryIndex(spec, fs, CREATE_INDEX);
      if (idx) {
        const GeometryApi *api = GeometryApi_Get(idx);
        api->delGeom(idx, id);
      }
    }
  }
}

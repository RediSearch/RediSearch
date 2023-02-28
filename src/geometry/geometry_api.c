/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "geometry_api.h"
#include "geometry.h"
#include "rmalloc.h"

GeometryApi* apis[GEOMETRY_LIB_TYPE__NUM] = {0};

GEOMETRY bg_createGeom(GEOMETRY_FORMAT format, const char *str, size_t len, RedisModuleString **err_msg) {
  switch (format) {
   case GEOMETRY_FORMAT_WKT: {
    GEOMETRY d = From_WKT(str, len, 0);
    // TODO: GEOMETRY handle error
    return d;
   }
   case GEOMETRY_FORMAT_GEOJSON:
    // TODO: GEOMETRY Support GEOJSON
    return NULL;
   case GEOMETRY_FORMAT_NONE:
    RedisModule_Assert(format != GEOMETRY_FORMAT_NONE);
   default:
    RedisModule_Log(NULL, "error", "unknown geometry format");
  }
  return NULL;
}

void bg_freeIndex(GeometryIndex index) {
  RTree_Free(index);
}

struct GeometryIndex* bg_createIndex() {
  return (struct GeometryIndex*)RTree_New();
}

IndexIterator* bg_query(struct GeometryIndex *index, enum QueryType queryType, GEOMETRY_FORMAT format, const char *str, size_t len) {
  switch (format) {
  case GEOMETRY_FORMAT_WKT:
    return RTree_Query_WKT((struct RTree*)index, str, len, queryType);
  
  case GEOMETRY_FORMAT_GEOJSON:
  default:
    return NULL;
  }
}

int bg_addGeomStr(struct GeometryIndex *index, GEOMETRY_FORMAT format, const char *str, size_t len, t_docId docId, RedisModuleString **err_msg) {
  
  switch (format) {
  case GEOMETRY_FORMAT_WKT:
    if (!RTree_Insert_WKT((struct RTree*)index, str, len, docId)) {
      if (err_msg) {
        *err_msg = RedisModule_CreateStringPrintf(NULL, "Invalid WKT %.*s", (int)len, str);
      }
    }
    break;

  default:
  case GEOMETRY_FORMAT_GEOJSON:
    return 1;

  }
  return 0;
}

int bg_addGeom(struct GeometryIndex *index, GEOMETRY geom) {
  RTree_Insert((struct RTree*)index, geom);
}

int bg_delGeom(struct GeometryIndex *index, GEOMETRY geom, void *data) {
  // TODO: GEOMETRY
}

GEOMETRY s2_createGeom(GEOMETRY_FORMAT format, const char *str, size_t len, RedisModuleString **err_msg) {
  // TODO: GEOMETRY
  return NULL;
}

void s2_freeIndex(GeometryIndex index) {
  // TODO: GEOMETRY
}

GeometryApi* GeometryApi_GetOrCreate(GEOMETRY_LIB_TYPE type, __attribute__((__unused__)) void *pdata) {
  if (type == GEOMETRY_LIB_TYPE_NONE) {
    return NULL;
  }
  if (apis[type]) {
    return apis[type];
  }
  
  GeometryApi *api = (GeometryApi*)rm_malloc(sizeof(*api));
  switch (type) {
   case GEOMETRY_LIB_TYPE_BOOST_GEOMETRY:
    api->createGeom = bg_createGeom;
    api->createIndex = bg_createIndex;
    api->freeIndex = bg_freeIndex;
    api->addGeomStr = bg_addGeomStr;
    api->addGeom = bg_addGeom;
    api->delGeom = bg_delGeom;
    api->query = bg_query;
    break;
   case GEOMETRY_LIB_TYPE_S2:
    api->freeIndex = s2_freeIndex;
    // TODO: GEOMETRY
    break;
   default:
    rm_free(api);
    return NULL;
  }

  apis[type] = api;
  return api;
}

void GeometryApi_Free() {
  for (int i = 0; i < GEOMETRY_LIB_TYPE__NUM; ++i) {
    if (apis[i]) {
      rm_free(apis[i]);
    }
  }
}
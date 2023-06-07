/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "geometry_api.h"
#include "geometry.h"
#include "rmalloc.h"

GeometryApi* apis[GEOMETRY_LIB_TYPE__NUM] = {0};

void bg_freeIndex(GeometryIndex *index) {
  RTree_Free(reinterpret_cast<RTree*>(index));
}

struct GeometryIndex* bg_createIndex() {
  return reinterpret_cast<GeometryIndex*>(RTree_New());
}

IndexIterator* bg_query(struct GeometryIndex *index, enum QueryType queryType, GEOMETRY_FORMAT format, const char *str, size_t len, RedisModuleString **err_msg) {
  switch (format) {
  case GEOMETRY_FORMAT_WKT:
    return RTree_Query_WKT((struct RTree*)index, str, len, queryType, err_msg);
  
  case GEOMETRY_FORMAT_GEOJSON:
  default:
    return NULL;
  }
}

int bg_addGeomStr(struct GeometryIndex *index, GEOMETRY_FORMAT format, const char *str, size_t len, t_docId docId, RedisModuleString **err_msg) {
  
  switch (format) {
  case GEOMETRY_FORMAT_WKT:
    return !RTree_Insert_WKT((struct RTree*)index, str, len, docId, err_msg);

  default:
  case GEOMETRY_FORMAT_GEOJSON:
    // TODO: GEOMETRY Support GeoJSON
    return 1;

  }
  return 0;
}

int bg_delGeom(struct GeometryIndex *index, t_docId docId) {
  return RTree_RemoveByDocId(reinterpret_cast<RTree*>(index), docId);
}

void bg_dumpIndex(GeometryIndex *index, RedisModuleCtx *ctx) {
  RTree_Dump(reinterpret_cast<RTree*>(index), ctx);
}

void s2_freeIndex(GeometryIndex *index) {
  // TODO: GEOMETRY
}

size_t GeometryTotalMemUsage() {
  return RTree_TotalMemUsage();
}

extern "C"
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
    api->createIndex = bg_createIndex;
    api->freeIndex = bg_freeIndex;
    api->addGeomStr = bg_addGeomStr;
    api->delGeom = bg_delGeom;
    api->query = bg_query;
    api->dump = bg_dumpIndex;
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

extern "C"
void GeometryApi_Free() {
  for (int i = 0; i < GEOMETRY_LIB_TYPE__NUM; ++i) {
    if (apis[i]) {
      rm_free(apis[i]);
    }
  }
}

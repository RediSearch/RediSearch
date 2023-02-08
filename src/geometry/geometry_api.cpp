/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "geometry_api.h"
#include "rmalloc.h"
#include "rtdoc.h"

GeometryApi* apis[GEOMETRY_LIB_TYPE__LAST + 1] = {0};

GEOMETRY bg_createGeom(GEOMETRY_FORMAT format, const char *str, size_t len, RedisModuleString **err_msg) {
  switch (format) {
    case GEOMETRY_FORMAT_WKT:      
      {
        RTDoc *d = From_WKT(str, len, 0);
        // TODO: GEOMETRY handle error
        return (GEOMETRY)d;
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

void bg_freeGeom(GEOMETRY geom) {
  RTDoc_Free((RTDoc*)geom);
}


GEOMETRY s2_createGeom(GEOMETRY_FORMAT format, const char *str, size_t len, RedisModuleString **err_msg) {
  // TODO: GEOMETRY
  return nullptr;
}

void s2_freeGeom(GEOMETRY geom) {
  // TODO: GEOMETRY
}

GeometryApi* GeometryApi_GetOrCreate(GEOMETRY_LIB_TYPE type, void *pdata) {
    
    if (type == GEOMETRY_LIB_TYPE_NONE)
      return NULL;
    if (apis[type]) {
        return apis[type];
    }
    
    GeometryApi *api = (GeometryApi*)rm_malloc(sizeof(*api));
    switch (type) {
        case GEOMETRY_LIB_TYPE_BOOST_GEOMETRY:
            api->createGeom = bg_createGeom;
            api->freeGeom = bg_freeGeom;
            break;
        case GEOMETRY_LIB_TYPE_S2:
            api->createGeom = s2_createGeom;
            api->freeGeom = s2_freeGeom;
            break;
        default:
            rm_free(api);
            return NULL;
    }

    apis[type] = api;
    return api;
}

void GeometryApi_Free() {
  for (int i = 0; i < GEOMETRY_LIB_TYPE__LAST; i++) {
    if (apis[i]) {
      rm_free(apis[i]);
    }
  }
}
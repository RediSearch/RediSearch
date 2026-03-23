/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "geometry_index.h"
#include "geometry/geometry_api.h"
#include "rmalloc.h"
#include "field_spec.h"
#include "redis_index.h"

void GeometryQuery_Free(GeometryQuery *geomq) {
  if (geomq->str) {
    rm_free((void *)geomq->str);
  }
  rm_free(geomq);
}

GeometryIndex *OpenGeometryIndex(FieldSpec *fs, bool create_if_missing) {
  RS_ASSERT(FIELD_IS(fs, INDEXFLD_T_GEOMETRY));
  if (create_if_missing && !fs->geometryOpts.geometryIndex) {
    fs->geometryOpts.geometryIndex = GeometryIndexFactory(fs->geometryOpts.geometryCoords);
  }
  return fs->geometryOpts.geometryIndex;
}

void GeometryIndex_RemoveId(IndexSpec *spec, t_docId id) {
  for (int i = 0; i < spec->numFields; ++i) {
    if (spec->fields[i].types & INDEXFLD_T_GEOMETRY) {
      FieldSpec *fs = spec->fields + i;
      GeometryIndex *idx = OpenGeometryIndex(fs, CREATE_INDEX);
      if (idx) {
        const GeometryApi *api = GeometryApi_Get(idx);
        api->delGeom(idx, id);
      }
    }
  }
}

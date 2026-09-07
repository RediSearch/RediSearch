/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

#include "geometry/geometry_types.h"
#include "search_ctx.h"

typedef struct GeometryQuery {
    GEOMETRY_FORMAT format;
    QueryType query_type;
    t_fieldIndex fieldIndex;  // stable index of the geometry field into IndexSpec.fields;
                              // re-derive the FieldSpec* from this at evaluation time - a
                              // pointer captured at parse time may already be freed by then
    const char *str;
    size_t str_len;
} GeometryQuery;

void GeometryQuery_Free(GeometryQuery *geomq);

// Sets `geomq->fieldIndex` from `fs` (or RS_INVALID_FIELD_INDEX if NULL).
void GeometryQuery_SetField(GeometryQuery *geomq, const FieldSpec *fs);

GeometryIndex *OpenGeometryIndex(FieldSpec *fs, bool create_if_missing);

// Remove indexed data for the given document ID
void GeometryIndex_RemoveId(IndexSpec *spec, t_docId id);

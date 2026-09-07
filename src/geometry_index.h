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
    const FieldSpec *fs;      // the geometry field, as it existed when the query was parsed.
                              // Only safe to dereference at parse time - see fieldIndex.
    t_fieldIndex fieldIndex;  // stable index of `fs` into IndexSpec.fields; use this for a
                              // fresh lookup at evaluation time instead of dereferencing `fs`
                              // directly (a concurrent FT.ALTER may have since reallocated the
                              // spec's field array under WORKERS>0 - see MOD-18368)
    const char *str;
    size_t str_len;
} GeometryQuery;

void GeometryQuery_Free(GeometryQuery *geomq);

// Sets `geomq->fs` and `geomq->fieldIndex` together, so a caller that resolves the field
// after construction cannot update one and forget the other.
void GeometryQuery_SetField(GeometryQuery *geomq, const FieldSpec *fs);

GeometryIndex *OpenGeometryIndex(FieldSpec *fs, bool create_if_missing);

// Remove indexed data for the given document ID
void GeometryIndex_RemoveId(IndexSpec *spec, t_docId id);

/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */

#pragma once

#include "redisearch.h"
#include "redismodule.h"
#include "index_result.h"
#include "iterators/iterator_api.h"
#include "search_ctx.h"
#include "geo_ffi.h"
#include "query_node.h"
#include "obfuscation/hidden.h"

// Keep C entrypoints live until the following swap commit.
#define X_GEO_DISTANCE(X) \
  X(KM, "km")             \
  X(M, "m")               \
  X(FT, "ft")             \
  X(MI, "mi")

GeoFilter *NewGeoFilter(double lon, double lat, double radius, const char *unit, size_t unit_len);
void GeoFilter_SetField(GeoFilter *gf, const FieldSpec *fs);
GeoDistance GeoDistance_Parse(const char *s);
const char *GeoDistance_ToString(GeoDistance dist);
GeoDistance GeoDistance_Parse_Buffer(const char *s, size_t len);
int GeoFilter_Validate(const GeoFilter *gf, QueryError *status);
int GeoFilter_LegacyParse(LegacyGeoFilter *gf, ArgsCursor *ac, bool *hasEmptyFilterValue,
                          QueryError *status);
void GeoFilter_Free(GeoFilter *gf);
void LegacyGeoFilter_Free(LegacyGeoFilter *gf);
int isWithinRadius(const GeoFilter *gf, double d, double *distance);

#define INVALID_GEOHASH -1.0

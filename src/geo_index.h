/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#pragma once

#include "redisearch.h"
#include "redismodule.h"
#include "index_result.h"
#include "index_iterator.h"
#include "search_ctx.h"
#include "query_error.h"
#include "rs_geo.h"
#include "numeric_index.h"
#include "query_node.h"

typedef struct geoIndex {
  RedisSearchCtx *ctx;
  const FieldSpec *sp;
} GeoIndex;

#define GEOINDEX_KEY_FMT "geo:%s/%s"

typedef enum {  // Placeholder for bad/invalid unit
  GEO_DISTANCE_INVALID = -1,
#define X_GEO_DISTANCE(X) \
  X(KM, "km")             \
  X(M, "m")               \
  X(FT, "ft")             \
  X(MI, "mi")

#define X(c, unused) GEO_DISTANCE_##c,
  X_GEO_DISTANCE(X)
#undef X
} GeoDistance;

typedef struct GeoFilter {
  const char *property;
  double lat;
  double lon;
  double radius;
  GeoDistance unitType;
  NumericFilter **numericFilters;
} GeoFilter;

/* Create a geo filter from parsed strings and numbers */
GeoFilter *NewGeoFilter(double lon, double lat, double radius, const char *unit, size_t unit_len);

/** @param s CString (null-terminated string) */
GeoDistance GeoDistance_Parse(const char *s);
const char *GeoDistance_ToString(GeoDistance dist);

/** @param s non null-terminated string */
GeoDistance GeoDistance_Parse_Buffer(const char *s, size_t len);

/* Make sure that the parameters of the filter make sense - i.e. coordinates are in range, radius is
 * sane, unit is valid. Return 1 if valid, 0 if not, and set the error string into err */
int GeoFilter_Validate(const GeoFilter *gf, QueryError *status);

/* Parse a geo filter from redis arguments. We assume the filter args start at argv[0] */
int GeoFilter_Parse(GeoFilter *gf, ArgsCursor *ac, QueryError *status);
void GeoFilter_Free(GeoFilter *gf);
IndexIterator *NewGeoRangeIterator(RedisSearchCtx *ctx, const GeoFilter *gf, ConcurrentSearchCtx *csx, IteratorsConfig *config, t_fieldIndex fieldIndex);

/*****************************************************************************/

#define INVALID_GEOHASH -1.0
double calcGeoHash(double lon, double lat);
int isWithinRadius(const GeoFilter *gf, double d, double *distance);

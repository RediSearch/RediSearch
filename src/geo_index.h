#ifndef __GEO_INDEX_H__
#define __GEO_INDEX_H__

#include "redisearch.h"
#include "redismodule.h"
#include "index_result.h"
#include "index_iterator.h"
#include "search_ctx.h"
#include "query_error.h"
#include "dep/geo/rs_geo.h"
#include "numeric_index.h"
#include "param.h"
#include "query_parser/tokenizer.h"

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

#define GEO_FILTER_PARAMS_COUNT 5

typedef struct GeoFilter {
  const char *property;
  double lat;
  double lon;
  double radius;
  GeoDistance unitType;
  NumericFilter **numericFilters;
  Param (*params)[GEO_FILTER_PARAMS_COUNT];
} GeoFilter;

/* Create a geo filter from parsed strings and numbers */
GeoFilter *NewGeoFilter(double lon, double lat, double radius, const char *unit);

/* Create a geo filter from actual or parameterized strings and numbers */
GeoFilter *NewGeoFilter_FromParams(QueryToken *lon_param, QueryToken *lat_param, QueryToken *radius_param, QueryToken *unit_param);

/*
 * Substitute parameters with actual values used by geo filter
 * If a parameters is missing, has wrong kind, or the resulting geo filter is invalid
 * Returns REDISMODULE_ERR
 * Otherwise, returns REDISMODULE_OK
 */
int GeoFilter_EvalParams(dict *params, GeoFilter *gf, QueryError *status);

GeoDistance GeoDistance_Parse(const char *s);
const char *GeoDistance_ToString(GeoDistance dist);

/* Make sure that the parameters of the filter make sense - i.e. coordinates are in range, radius is
 * sane, unit is valid. Return 1 if valid, 0 if not, and set the error string into err */
int GeoFilter_Validate(GeoFilter *f, QueryError *status);

/* Parse a geo filter from redis arguments. We assume the filter args start at argv[0] */
int GeoFilter_Parse(GeoFilter *gf, ArgsCursor *ac, QueryError *status);
void GeoFilter_Free(GeoFilter *gf);
IndexIterator *NewGeoRangeIterator(RedisSearchCtx *ctx, const GeoFilter *gf);

/*****************************************************************************/

#define INVALID_GEOHASH -1.0
double calcGeoHash(double lon, double lat);
int isWithinRadius(const GeoFilter *gf, double d, double *distance);

#endif

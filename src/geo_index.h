#ifndef __GEO_INDEX_H__
#define __GEO_INDEX_H__

#include "redisearch.h"
#include "redismodule.h"
#include "index_result.h"
#include "index_iterator.h"
#include "search_ctx.h"

typedef struct geoIndex {
  RedisSearchCtx *ctx;
  const FieldSpec *sp;
} GeoIndex;

int GeoIndex_AddStrings(GeoIndex *gi, t_docId docId, char *slon, char *slat);

typedef struct geoFilter {

  const char *property;
  double lat;
  double lon;
  double radius;
  const char *unit;
} GeoFilter;

/* Create a geo filter from parsed strings and numbers */
GeoFilter *NewGeoFilter(double lon, double lat, double radius, const char *unit);

/* Make sure that the parameters of the filter make sense - i.e. coordinates are in range, radius is
 * sane, unit is valid. Return 1 if valid, 0 if not, and set the error string into err */
int GeoFilter_IsValid(GeoFilter *f, char **err);

/* Parse a geo filter from redis arguments. We assume the filter args start at argv[0] */
int GeoFilter_Parse(GeoFilter *gf, RedisModuleString **argv, int argc);
void GeoFilter_Free(GeoFilter *gf);
IndexIterator *NewGeoRangeIterator(GeoIndex *gi, GeoFilter *gf);

#endif
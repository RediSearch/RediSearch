/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "geo_index.h"
#include "rmutil/util.h"
#include "rmalloc.h"
#include "rmutil/rm_assert.h"
#include "query_node.h"
#include "query_param.h"
#include "iterators/union_iterator.h"

static double extractUnitFactor(GeoDistance unit);

static void CheckAndSetEmptyFilterValue(ArgsCursor *ac, bool *hasEmptyFilterValue) {
  const char *val;

  int rv = AC_GetString(ac, &val, NULL, AC_F_NOADVANCE);
  if (rv == AC_OK && !(*val)) {
    *hasEmptyFilterValue = true;
  }
}

/* Parse a geo filter from redis arguments. We assume the filter args start at argv[0], and FILTER
 * is not passed to us.
 * The GEO filter syntax is (FILTER) <property> LONG LAT DIST m|km|ft|mi
 * Returns REDISMODUEL_OK or ERR  */
int GeoFilter_LegacyParse(LegacyGeoFilter *gf, ArgsCursor *ac, bool *hasEmptyFilterValue, QueryError *status) {
  *gf = (LegacyGeoFilter){0};

  if (AC_NumRemaining(ac) < 5) {
    QueryError_SetError(status, QUERY_ERROR_CODE_PARSE_ARGS, "GEOFILTER requires 5 arguments");
    return REDISMODULE_ERR;
  }

  int rv;
  // Store the field name at the field spec pointer, to validate later
  const char *fieldName = NULL;
  if ((rv = AC_GetString(ac, &fieldName, NULL, 0)) != AC_OK) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments", " for <geo property>: %s", AC_Strerror(rv));
    return REDISMODULE_ERR;
  }
  if ((rv = AC_GetDouble(ac, &gf->base.lon, AC_F_NOADVANCE) != AC_OK)) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments", " for <lon>: %s", AC_Strerror(rv));
    return REDISMODULE_ERR;
  }
  if (gf->base.lon == 0) {
    CheckAndSetEmptyFilterValue(ac, hasEmptyFilterValue);
  }
  AC_Advance(ac);

  if ((rv = AC_GetDouble(ac, &gf->base.lat, AC_F_NOADVANCE)) != AC_OK) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments", " for <lat>: %s", AC_Strerror(rv));
    return REDISMODULE_ERR;
  }
  if (gf->base.lat == 0) {
    CheckAndSetEmptyFilterValue(ac, hasEmptyFilterValue);
  }
  AC_Advance(ac);

  if ((rv = AC_GetDouble(ac, &gf->base.radius, AC_F_NOADVANCE)) != AC_OK) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Bad arguments", " for <radius>: %s", AC_Strerror(rv));
    return REDISMODULE_ERR;
  }
  if (gf->base.radius == 0) {
    CheckAndSetEmptyFilterValue(ac, hasEmptyFilterValue);
  }
  AC_Advance(ac);

  const char *unitstr = AC_GetStringNC(ac, NULL);
  if ((gf->base.unitType = GeoDistance_Parse(unitstr)) == GEO_DISTANCE_INVALID) {
    QueryError_SetWithUserDataFmt(status, QUERY_ERROR_CODE_PARSE_ARGS, "Unknown distance unit", " %s", unitstr);
    return REDISMODULE_ERR;
  }
  // only allocate on the success path
  gf->field = NewHiddenString(fieldName, strlen(fieldName), false);
  return REDISMODULE_OK;
}

void GeoFilter_Free(GeoFilter *gf) {
  if (gf->numericFilters) {
    for (int i = 0; i < GEO_RANGE_COUNT; ++i) {
      if (gf->numericFilters[i])
        NumericFilter_Free(gf->numericFilters[i]);
    }
    rm_free(gf->numericFilters);
  }
  rm_free(gf);
}

void LegacyGeoFilter_Free(LegacyGeoFilter *gf) {
  if (gf->field) {
    HiddenString_Free(gf->field, false);
  }
  GeoFilter_Free(&gf->base);
}

QueryIterator *NewGeoRangeIterator(const RedisSearchCtx *ctx, const GeoFilter *gf, IteratorsConfig *config) {
  // check input parameters are valid
  if (gf->radius <= 0 ||
      gf->lon > GEO_LONG_MAX || gf->lon < GEO_LONG_MIN ||
      gf->lat > GEO_LAT_MAX || gf->lat < GEO_LAT_MIN) {
    return NULL;
  }

  GeoHashRange ranges[GEO_RANGE_COUNT] = {{0}};
  double radius_meter = gf->radius * extractUnitFactor(gf->unitType);
  calcRanges(gf->lon, gf->lat, radius_meter, ranges);

  QueryIterator **iters = rm_calloc(GEO_RANGE_COUNT, sizeof(*iters));
  ((GeoFilter *)gf)->numericFilters = rm_calloc(GEO_RANGE_COUNT, sizeof(*gf->numericFilters));
  FieldFilterContext filterCtx = {.field = {.index_tag = FieldMaskOrIndex_Index, .index = gf->fieldSpec->index}, .predicate = FIELD_EXPIRATION_DEFAULT};
  for (size_t ii = 0; ii < GEO_RANGE_COUNT; ++ii) {
    if (ranges[ii].min != ranges[ii].max) {
      NumericFilter *filt = gf->numericFilters[ii] =
              NewNumericFilter(ranges[ii].min, ranges[ii].max, 1, 1, true, NULL);
      filt->fieldSpec = gf->fieldSpec;
      filt->geoFilter = gf;
      iters[ii] = NewNumericFilterIterator(ctx, filt, INDEXFLD_T_GEO, config, &filterCtx);
    }
  }

  return NewUnionIterator(iters, GEO_RANGE_COUNT, true, 1, QN_GEO, NULL, config);
}

GeoDistance GeoDistance_Parse(const char *s) {
#define X(c, val)            \
  if (!strcasecmp(val, s)) { \
    return GEO_DISTANCE_##c; \
  }
  X_GEO_DISTANCE(X)
#undef X
  return GEO_DISTANCE_INVALID;
}

GeoDistance GeoDistance_Parse_Buffer(const char *s, size_t len) {
  char buf[16] = {0};
  if (len < 16) {
    memcpy(buf, s, len);
  } else {
    strcpy(buf, "INVALID");
  }
  return GeoDistance_Parse(buf);
}

const char *GeoDistance_ToString(GeoDistance d) {
#define X(c, val)              \
  if (d == GEO_DISTANCE_##c) { \
    return val;                \
  }
  X_GEO_DISTANCE(X)
#undef X
  return "<badunit>";
}

/* Create a geo filter from parsed strings and numbers */
GeoFilter *NewGeoFilter(double lon, double lat, double radius, const char *unit, size_t unit_len) {
  GeoFilter *gf = rm_malloc(sizeof(*gf));
  *gf = (GeoFilter){
      .lon = lon,
      .lat = lat,
      .radius = radius,
  };
  if (unit) {
    gf->unitType = GeoDistance_Parse_Buffer(unit, unit_len);
  } else {
    gf->unitType = GEO_DISTANCE_KM;
  }
  return gf;
}

/* Make sure that the parameters of the filter make sense - i.e. coordinates are in range, radius is
 * sane, unit is valid. Return 1 if valid, 0 if not, and set the error string into err */
int GeoFilter_Validate(const GeoFilter *gf, QueryError *status) {
  if (gf->unitType == GEO_DISTANCE_INVALID) {
    QERR_MKSYNTAXERR(status, "Invalid GeoFilter unit");
    return 0;
  }

  // validate lat/lon
  if (gf->lat > 90 || gf->lat < -90 || gf->lon > 180 || gf->lon < -180) {
    QERR_MKSYNTAXERR(status, "Invalid GeoFilter lat/lon");
    return 0;
  }

  // validate radius
  if (gf->radius <= 0) {
    QERR_MKSYNTAXERR(status, "Invalid GeoFilter radius");
    return 0;
  }

  return 1;
}

/**
 * Generates a geo hash from a given latitude and longtitude
 */
double calcGeoHash(double lon, double lat) {
  double res;
  int rv = encodeGeo(lon, lat, &res);
  if (rv == 0) {
    return INVALID_GEOHASH;
  }
  return res;
}

/**
 * Convert different units to meters
 */
static double extractUnitFactor(GeoDistance unit) {
  double rv;
  switch (unit) {
    case GEO_DISTANCE_M:
      rv = 1;
      break;
    case GEO_DISTANCE_KM:
      rv = 1000;
      break;
    case GEO_DISTANCE_FT:
      rv = 0.3048;
      break;
    case GEO_DISTANCE_MI:
      rv = 1609.34;
      break;
    default:
      rv = -1;
      RS_ABORT("ERROR");
      break;
  }
  return rv;
}

/**
 * Checks if the given coordinate d is within the radius gf
 */
int isWithinRadius(const GeoFilter *gf, double d, double *distance) {
  double xy[2];
  decodeGeo(d, xy);
  double radius_meters = gf->radius * extractUnitFactor(gf->unitType);
  int rv = isWithinRadiusLonLat(gf->lon, gf->lat, xy[0], xy[1], radius_meters, distance);
  return rv;
}

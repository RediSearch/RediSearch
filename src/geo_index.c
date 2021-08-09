#include "index.h"
#include "geo_index.h"
#include "rmutil/util.h"
#include "rmalloc.h"
#include "rmutil/rm_assert.h"

static double extractUnitFactor(GeoDistance unit);

/* Parse a geo filter from redis arguments. We assume the filter args start at argv[0], and FILTER
 * is not passed to us.
 * The GEO filter syntax is (FILTER) <property> LONG LAT DIST m|km|ft|mi
 * Returns REDISMODUEL_OK or ERR  */
int GeoFilter_Parse(GeoFilter *gf, ArgsCursor *ac, QueryError *status) {
  gf->lat = 0;
  gf->lon = 0;
  gf->radius = 0;
  gf->unitType = GEO_DISTANCE_KM;

  if (AC_NumRemaining(ac) < 5) {
    QERR_MKBADARGS_FMT(status, "GEOFILTER requires 5 arguments");
    return REDISMODULE_ERR;
  }

  int rv;
  if ((rv = AC_GetString(ac, &gf->property, NULL, 0)) != AC_OK) {
    QERR_MKBADARGS_AC(status, "<geo property>", rv);
    return REDISMODULE_ERR;
  } else {
    gf->property = rm_strdup(gf->property);
  }
  if ((rv = AC_GetDouble(ac, &gf->lon, 0) != AC_OK)) {
    QERR_MKBADARGS_AC(status, "<lon>", rv);
    return REDISMODULE_ERR;
  }

  if ((rv = AC_GetDouble(ac, &gf->lat, 0)) != AC_OK) {
    QERR_MKBADARGS_AC(status, "<lat>", rv);
    return REDISMODULE_ERR;
  }

  if ((rv = AC_GetDouble(ac, &gf->radius, 0)) != AC_OK) {
    QERR_MKBADARGS_AC(status, "<radius>", rv);
    return REDISMODULE_ERR;
  }

  const char *unitstr = AC_GetStringNC(ac, NULL);
  if ((gf->unitType = GeoDistance_Parse(unitstr)) == GEO_DISTANCE_INVALID) {
    QERR_MKBADARGS_FMT(status, "Unknown distance unit %s", unitstr);
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

void GeoFilter_Free(GeoFilter *gf) {
  if (gf->property) rm_free((char *)gf->property);
  if (gf->numericFilters) {
    for (int i = 0; i < GEO_RANGE_COUNT; ++i) {
      if (gf->numericFilters[i])
        NumericFilter_Free(gf->numericFilters[i]);
    }
    rm_free(gf->numericFilters);
  }
  rm_free(gf);
}

IndexIterator *NewGeoRangeIterator(RedisSearchCtx *ctx, const GeoFilter *gf) {
  GeoHashRange ranges[GEO_RANGE_COUNT] = {{0}};
  double radius_meter = gf->radius * extractUnitFactor(gf->unitType);
  calcRanges(gf->lon, gf->lat, radius_meter, ranges);

  IndexIterator **iters = rm_calloc(GEO_RANGE_COUNT, sizeof(*iters));
  ((GeoFilter *)gf)->numericFilters = rm_calloc(GEO_RANGE_COUNT, sizeof(*gf->numericFilters));
  size_t itersCount = 0;
  for (size_t ii = 0; ii < GEO_RANGE_COUNT; ++ii) {
    if (ranges[ii].min != ranges[ii].max) {
      NumericFilter *filt = gf->numericFilters[ii] =
              NewNumericFilter(ranges[ii].min, ranges[ii].max, 1, 1);
      filt->fieldName = rm_strdup(gf->property);
      filt->geoFilter = gf;
      struct indexIterator *numIter = NewNumericFilterIterator(ctx, filt, NULL, INDEXFLD_T_GEO);
      if (numIter != NULL) {
        iters[itersCount++] = numIter;
      }
    }
  }

  if (itersCount == 0) {
    rm_free(iters);
    return NULL;
  } else if (itersCount == 1) {
    IndexIterator *it = iters[0];
    rm_free(iters);
    return it;
  }
  IndexIterator *it = NewUnionIterator(iters, itersCount, NULL, 1, 1, QN_GEO, NULL);
  if (!it) {
    return NULL;
  }
  return it;
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
GeoFilter *NewGeoFilter(double lon, double lat, double radius, const char *unit) {
  GeoFilter *gf = rm_malloc(sizeof(*gf));
  *gf = (GeoFilter){
      .lon = lon,
      .lat = lat,
      .radius = radius,
  };
  if (unit) {
    gf->unitType = GeoDistance_Parse(unit);
  } else {
    gf->unitType = GEO_DISTANCE_KM;
  }
  return gf;
}

/* Make sure that the parameters of the filter make sense - i.e. coordinates are in range, radius is
 * sane, unit is valid. Return 1 if valid, 0 if not, and set the error string into err */
int GeoFilter_Validate(GeoFilter *gf, QueryError *status) {
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
      assert(0);
      break;
  }
  return rv;
}

/**
 * Populates the numeric range to search for within a given square direction
 * specified by `dir`
 */
static int populateRange(const GeoFilter *gf, GeoHashRange *ranges) {
  double xy[2] = {gf->lon, gf->lat};

  double radius_meters = gf->radius * extractUnitFactor(gf->unitType);
  if (radius_meters < 0) {
    return -1;
  }
  calcRanges(gf->lon, gf->lat, radius_meters, ranges);
  return 0;
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

static int checkResult(const GeoFilter *gf, const RSIndexResult *cur) {
  double distance;
  if (cur->type == RSResultType_Numeric) {
    return isWithinRadius(gf, cur->num.value, &distance);
  }
  for (size_t ii = 0; ii < cur->agg.numChildren; ++ii) {
    if (checkResult(gf, cur->agg.children[ii])) {
      return 1;
    }
  }
  return 0;
}

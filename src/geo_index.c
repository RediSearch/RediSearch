#include "index.h"
#include "geo_index.h"
#include "rmutil/util.h"
#include "rmalloc.h"
#include "module.h"
#include "rmutil/rm_assert.h"

GeoIndex *GeoIndex_Create() {
  GeoIndex *gi = rm_calloc(1, sizeof(*gi));
  gi->rt = NewNumericRangeTree();
  return gi;
}

void GeoIndex_Free(GeoIndex *gi) {
  NumericRangeTree_Free(gi->rt);
  rm_free(gi);
}
static double calcGeoHash(double lon, double lat);

#define INVALID_GEOHASH -1.0

int GeoIndex_AddStrings(GeoIndex *gi, t_docId docId, const char *slon, const char *slat) {
  // @ariel: convert to hash
  char *end1 = NULL, *end2 = NULL;
  double lon = strtod(slon, &end1);
  double lat = strtod(slat, &end2);
  if (*end1 || *end2) {
    return REDISMODULE_ERR;
  }

  double geohash = calcGeoHash(lon, lat);
  if (geohash == INVALID_GEOHASH) {
    return REDISMODULE_ERR;
  }

  NumericRangeTree_Add(gi->rt, docId, geohash);
  return REDISMODULE_OK;
}

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
  rm_free(gf);
}

typedef struct {
  IndexIterator base;
  t_docId lastId;
  const GeoFilter *gf;
  NumericFilter **filters;
  IndexIterator *wrapped;  // Union iterator
} GeoIterator;

/**
 * Generates a geo hash from a given latitude and longtitude
 */
static double calcGeoHash(double lon, double lat) {
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
static int isWithinRadius(const GeoFilter *gf, double d, double *distance) {
  double xy[2];
  decodeGeo(d, xy);
  int rv = isWithinRadiusLonLat(gf->lon, gf->lat, xy[0], xy[1], gf->radius, distance);
  return rv;
}

static int checkResult(const GeoFilter *gf, const RSIndexResult *cur) {
  double distance;
  if (cur->type == RSResultType_Numeric) {
    return isWithinRadius(gf, cur->num.value, &distance);
  }
  for (size_t ii = 0; ii < cur->agg.numChildren; ++ii) {
    const RSIndexResult *res = cur->agg.children[ii];
    if (isWithinRadius(gf, res->num.value, &distance)) {
      // TODO: use distance to sort
      return 1;
    }
  }
  return 0;
}

static int GI_Read(void *ctx, RSIndexResult **hit) {
  GeoIterator *gi = ctx;
  IndexIterator *wrapped = gi->wrapped;
  RSIndexResult *cur = NULL;
  int found = 0;
  while (1) {
    int rc = wrapped->Read(wrapped->ctx, &cur);
    if (rc == INDEXREAD_EOF) {
      IITER_SET_EOF(&gi->base);
      return rc;
    }
    assert(RSIndexResult_IsAggregate(cur));
    if (checkResult(gi->gf, cur)) {
      found = 1;
      break;
    }
  }

  if (found) {
    *hit = gi->base.current = cur;
    gi->lastId = cur->docId;
    return INDEXREAD_OK;
  } else {
    *hit = NULL;
    return INDEXREAD_EOF;
  }
}

static int GI_SkipTo(void *ctx, t_docId id, RSIndexResult **hit) {
  GeoIterator *gi = ctx;
  IndexIterator *wrapped = gi->wrapped;
  RSIndexResult *cur = NULL;
  int rc = wrapped->SkipTo(wrapped->ctx, id, &cur);
  if (rc == INDEXREAD_EOF) {
    IITER_SET_EOF(&gi->base);
    return rc;
  }
  if (checkResult(gi->gf, cur)) {
    gi->lastId = id;
    *hit = gi->base.current = cur;
    if (cur->docId == id) {
      return INDEXREAD_OK;
    } else {
      return INDEXREAD_NOTFOUND;
    }
  } else {
    rc = GI_Read(ctx, hit);
    if (rc == INDEXREAD_OK) {
      return INDEXREAD_NOTFOUND;
    }
    return rc;
  }
}

static size_t GI_NumEstimated(void *ctx) {
  GeoIterator *gi = ctx;
  return gi->wrapped->NumEstimated(gi->wrapped->ctx);
}
static IndexCriteriaTester *GI_GetCriteriaTester(void *ctx) {
  GeoIterator *gi = ctx;
  return gi->wrapped->GetCriteriaTester(gi->wrapped->ctx);
}
static void GI_Rewind(void *ctx) {
  GeoIterator *gi = ctx;
  IITER_CLEAR_EOF(&gi->base);
  gi->wrapped->Rewind(gi->wrapped->ctx);
  gi->lastId = 0;
}

static t_docId GI_LastDocId(void *ctx) {
  GeoIterator *gi = ctx;
  return gi->lastId;
}

static void GI_Abort(void *ctx) {
  GeoIterator *gi = ctx;
  gi->wrapped->Abort(gi->wrapped->ctx);
}

static void GI_Free(IndexIterator *ctx) {
  GeoIterator *gi = (GeoIterator *)ctx;
  gi->wrapped->Free(gi->wrapped);
  rm_free(gi->filters);
  rm_free(gi);
}

IndexIterator *NewGeoRangeIterator(GeoIndex *idx, IndexSpec *sp, const GeoFilter *gf, double weight,
                                   Yielder *yld) {
  GeoHashRange ranges[GEO_RANGE_COUNT] = {0};
  populateRange(gf, ranges);

  int numericFilterCount = 0;
  GeoIterator *gi = rm_calloc(1, sizeof(*gi));
  gi->gf = gf;
  IndexIterator **subiters = rm_calloc(GEO_RANGE_COUNT, sizeof(*subiters));
  gi->filters = rm_calloc(GEO_RANGE_COUNT, sizeof(*gi->filters));
  for (size_t ii = 0; ii < GEO_RANGE_COUNT; ++ii) {
    if (ranges[ii].min != ranges[ii].max) {
      gi->filters[numericFilterCount] = 
          NewNumericFilter(ranges[ii].min, ranges[ii].max, 1, 1);
      subiters[numericFilterCount] = 
          NumericTree_GetIterator(idx->rt, sp, gi->filters[numericFilterCount], yld);
      if (!subiters[numericFilterCount]) {
        subiters[numericFilterCount] = NewEmptyIterator();
      }
      numericFilterCount++;
    }
  }

  gi->wrapped = NewUnionIterator(subiters, numericFilterCount, &sp->docs, 0, weight);
  IndexIterator *ret = &gi->base;
  ret->ctx = gi;
  ret->LastDocId = GI_LastDocId;
  ret->NumEstimated = GI_NumEstimated;
  ret->GetCriteriaTester = GI_GetCriteriaTester;
  ret->Read = GI_Read;
  ret->SkipTo = GI_SkipTo;
  ret->Free = GI_Free;
  ret->Abort = GI_Abort;
  ret->Rewind = GI_Rewind;
  ret->GetCurrent = NULL;
  ret->HasNext = NULL;
  ret->mode = MODE_SORTED;
  return ret;
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

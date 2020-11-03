#include "geo_index.h"
#include "index.h"

#include "rmutil/util.h"
#include "rmalloc.h"
#include "rmutil/rm_assert.h"

///////////////////////////////////////////////////////////////////////////////////////////////

// Add a docId to a geoindex key. Right now we just use redis' own GEOADD

int GeoIndex::AddStrings(t_docId docId, const char *slon, const char *slat) {
  RedisModuleString *ks = IndexSpec_GetFormattedKey(ctx->spec, fs, INDEXFLD_T_GEO);
  RedisModuleCtx *rctx = ctx->redisCtx;

  // GEOADD key longitude latitude member
  RedisModuleCallReply *rep = RedisModule_Call(rctx, "GEOADD", "sccl", ks, slon, slat, docId);
  if (rep == NULL) {
    return REDISMODULE_ERR;
  }

  int repType = RedisModule_CallReplyType(rep);
  RedisModule_FreeCallReply(rep);
  if (repType == REDISMODULE_REPLY_ERROR) {
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

//---------------------------------------------------------------------------------------------

void GeoIndex::RemoveEntries(t_docId docId) {
  RemoveEntries(ctx->spec, docId);
}

void GeoIndex::RemoveEntries(IndexSpec *sp, t_docId docId) {
  RedisModuleString *ks = IndexSpec_GetFormattedKey(sp, fs, INDEXFLD_T_GEO);
  RedisModuleCtx *rctx = ctx->redisCtx;
  RedisModuleCallReply *rep = RedisModule_Call(rctx, "ZREM", "sl", ks, docId);

  if (rep == NULL || RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_ERROR) {
    RedisModule_Log(rctx, "warning", "Document %s was not removed", docId);
  }
  RedisModule_FreeCallReply(rep);
}

///////////////////////////////////////////////////////////////////////////////////////////////

/* Parse a geo filter from redis arguments. We assume the filter args start at argv[0], and FILTER
 * is not passed to us.
 * The GEO filter syntax is (FILTER) <property> LONG LAT DIST m|km|ft|mi
 * Returns REDISMODUEL_OK or ERR  */
GeoFilter::GeoFilter(ArgsCursor *ac, QueryError *status) {
  lat = 0;
  lon = 0;
  radius = 0;
  unitType = GeoDistance::Unit::KM;

  if (AC_NumRemaining(ac) < 5) {
    QERR_MKBADARGS_FMT(status, "GEOFILTER requires 5 arguments");
    throw Error(status);
  }

  int rv;
  if ((rv = AC_GetString(ac, &property, NULL, 0)) != AC_OK) {
    QERR_MKBADARGS_AC(status, "<geo property>", rv);
    throw Error(status);
  }
  property = rm_strdup(property);

  if ((rv = AC_GetDouble(ac, &lon, 0) != AC_OK)) {
    QERR_MKBADARGS_AC(status, "<lon>", rv);
    throw Error(status);
  }

  if ((rv = AC_GetDouble(ac, &lat, 0)) != AC_OK) {
    QERR_MKBADARGS_AC(status, "<lat>", rv);
    throw Error(status);
  }

  if ((rv = AC_GetDouble(ac, &radius, 0)) != AC_OK) {
    QERR_MKBADARGS_AC(status, "<radius>", rv);
    throw Error(status);
  }

  const char *unitstr = AC_GetStringNC(ac, NULL);
  if ((unitType = GeoDistance(unitstr)) == GeoDistance::Unit::INVALID) {
    QERR_MKBADARGS_FMT(status, "Unknown distance unit %s", unitstr);
    throw Error(status);
  }
}

//---------------------------------------------------------------------------------------------

GeoFilter::~GeoFilter() {
  if (property) rm_free((char *)property);
}

//---------------------------------------------------------------------------------------------

t_docId *GeoIndex::RangeLoad(const GeoFilter &gf, size_t &num) const {
  num = 0;
  t_docId *docIds = NULL;
  RedisModuleString *s = IndexSpec_GetFormattedKey(ctx->spec, fs, INDEXFLD_T_GEO);
  RS_LOG_ASSERT(s, "failed to retrive key");
  // GEORADIUS key longitude latitude radius m|km|ft|mi
  RedisModuleCtx *rctx = ctx->redisCtx;
  RedisModuleString *slon = RedisModule_CreateStringPrintf(rctx, "%f", gf.lon);
  RedisModuleString *slat = RedisModule_CreateStringPrintf(rctx, "%f", gf.lat);
  RedisModuleString *srad = RedisModule_CreateStringPrintf(rctx, "%f", gf.radius);
  const char *unitstr = gf.unitType.ToString();
  RedisModuleCallReply *rep =
      RedisModule_Call(rctx, "GEORADIUS", "ssssc", s, slon, slat, srad, unitstr);
  if (rep == NULL || RedisModule_CallReplyType(rep) != REDISMODULE_REPLY_ARRAY) {
    goto done;
  }

  size_t sz = RedisModule_CallReplyLength(rep);
  docIds = rm_calloc(sz, sizeof(t_docId));
  for (size_t i = 0; i < sz; i++) {
    const char *s = RedisModule_CallReplyStringPtr(RedisModule_CallReplyArrayElement(rep, i), NULL);
    if (!s) continue;

    docIds[i] = (t_docId)atol(s);
  }

  num = sz;

done:
  RedisModule_FreeString(rctx, slon);
  RedisModule_FreeString(rctx, slat);
  RedisModule_FreeString(rctx, srad);
  if (rep) {
    RedisModule_FreeCallReply(rep);
  }

  return docIds;
}

//---------------------------------------------------------------------------------------------

IndexIterator *GeoIndex::NewGeoRangeIterator(const GeoFilter &gf, double weight) {
  size_t size;
  t_docId *docIds = GeoIndex::RangeLoad(gf, size);
  if (!docIds) {
    return NULL;
  }

  IndexIterator *ret = NewIdListIterator(docIds, (t_offset)size, weight);
  rm_free(docIds);
  return ret;
}

///////////////////////////////////////////////////////////////////////////////////////////////

GeoDistance::GeoDistance(const char *s) {
#define X(c, val)            \
  if (!strcasecmp(val, s)) { \
    dist = GeoDistance::Unit::c; \
    return; \
  }
  X_GEO_DISTANCE(X)
#undef X
  dist = GeoDistance::Unit::INVALID; //@@ maybe throw
}

//---------------------------------------------------------------------------------------------

const char *GeoDistance::ToString() const {
#define X(c, val)              \
  if (dist == GeoDistance::Unit::c) { \
    return val;                \
  }
  X_GEO_DISTANCE(X)
#undef X
  return "<badunit>"; //@@ maybe throw
}

///////////////////////////////////////////////////////////////////////////////////////////////

// Create a geo filter from parsed strings and numbers

GeoFilter::GeoFilter(double lon_, double lat_, double radius_, const char *unit) {
  lon = lon_;
  lat = lat_;
  radius = radius_;

  if (unit) {
    unitType = GeoDistance(unit);
  } else {
    unitType = GeoDistance::Unit::KM;
  }
}

//---------------------------------------------------------------------------------------------

/* Make sure that the parameters of the filter make sense - i.e. coordinates are in range, radius is
 * sane, unit is valid. Return true if valid, false if not, and set the error string into err */
bool GeoFilter::Validate(QueryError *status) {
  if (unitType == GeoDistance::Unit::INVALID) {
    QERR_MKSYNTAXERR(status, "Invalid GeoFilter unit");
    return false;
  }

  // validate lat/lon
  if (lat > 90 || lat < -90 || lon > 180 || lon < -180) {
    QERR_MKSYNTAXERR(status, "Invalid GeoFilter lat/lon");
    return false;
  }

  // validate radius
  if (radius <= 0) {
    QERR_MKSYNTAXERR(status, "Invalid GeoFilter radius");
    return false;
  }

  return true;
}

#include "index.h"
#include "geo_index.h"
#include "rmutil/util.h"
#include "rmalloc.h"
#include "id_list.h"

#define GEOINDEX_KEY_FMT "geo:%s/%s"

RedisModuleString *fmtGeoIndexKey(GeoIndex *gi) {
  return RedisModule_CreateStringPrintf(gi->ctx->redisCtx, GEOINDEX_KEY_FMT, gi->ctx->spec->name,
                                        gi->sp->name);
}

/* Add a docId to a geoindex key. Right now we just use redis' own GEOADD */
int GeoIndex_AddStrings(GeoIndex *gi, t_docId docId, char *slon, char *slat) {

  RedisModuleString *ks = fmtGeoIndexKey(gi);

  RedisModuleCtx *ctx = gi->ctx->redisCtx;
  /* GEOADD key longitude latitude member*/
  RedisModuleCallReply *rep =
      RedisModule_Call(ctx, "GEOADD", "sccs", ks, slon, slat,
                       RedisModule_CreateStringFromLongLong(ctx, (long long)docId));
  RedisModule_FreeString(gi->ctx->redisCtx, ks);
  if (rep == NULL) {
    return REDISMODULE_ERR;
  }
  int repType = RedisModule_CallReplyType(rep);
  RedisModule_FreeCallReply(rep);
  if (repType == REDISMODULE_REPLY_ERROR) {
    return REDISMODULE_ERR;
  } else {
    return REDISMODULE_OK;
  }
}

/* Parse a geo filter from redis arguments. We assume the filter args start at argv[0], and FILTER
 * is not passed to us.
 * The GEO filter syntax is (FILTER) <property> LONG LAT DIST m|km|ft|mi
 * Returns REDISMODUEL_OK or ERR  */
int GeoFilter_Parse(GeoFilter *gf, RedisModuleString **argv, int argc) {
  gf->property = NULL;
  gf->lat = 0;
  gf->lon = 0;
  gf->unit = NULL;
  gf->radius = 0;

  if (argc != 5) {
    return REDISMODULE_ERR;
  }

  if (RMUtil_ParseArgs(argv, argc, 0, "cdddc", &gf->property, &gf->lon, &gf->lat, &gf->radius,
                       &gf->unit) == REDISMODULE_ERR) {

    // don't dup the strings since we are exiting now
    if (gf->property) gf->property = NULL;
    if (gf->unit) gf->unit = NULL;

    return REDISMODULE_ERR;
  }
  gf->property = gf->property ? strdup(gf->property) : NULL;
  gf->unit = gf->unit ? strdup(gf->unit) : NULL;
  // verify unit
  if (!gf->unit || (strcasecmp(gf->unit, "m") && strcasecmp(gf->unit, "km") &&
                    strcasecmp(gf->unit, "ft") && strcasecmp(gf->unit, "mi"))) {
    // printf("wrong unit %s\n", gf->unit);
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

void GeoFilter_Free(GeoFilter *gf) {
  if (gf->property) free((char *)gf->property);
  if (gf->unit) free((char *)gf->unit);
  free(gf);
}

static int cmp_docids(const void *p1, const void *p2) {
  const t_docId *d1 = p1, *d2 = p2;

  return (int)(*d1 - *d2);
}

t_docId *__gr_load(GeoIndex *gi, GeoFilter *gf, size_t *num) {

  *num = 0;
  /*GEORADIUS key longitude latitude radius m|km|ft|mi */
  RedisModuleCtx *ctx = gi->ctx->redisCtx;
  RedisModuleString *ks = fmtGeoIndexKey(gi);
  RedisModuleCallReply *rep = RedisModule_Call(
      gi->ctx->redisCtx, "GEORADIUS", "ssssc", ks,
      RedisModule_CreateStringPrintf(ctx, "%f", gf->lon),
      RedisModule_CreateStringPrintf(ctx, "%f", gf->lat),
      RedisModule_CreateStringPrintf(ctx, "%f", gf->radius), gf->unit ? gf->unit : "km");

  if (rep == NULL || RedisModule_CallReplyType(rep) != REDISMODULE_REPLY_ARRAY) {

    return NULL;
  }

  size_t sz = RedisModule_CallReplyLength(rep);
  t_docId *docIds = rm_calloc(sz, sizeof(t_docId));
  for (size_t i = 0; i < sz; i++) {
    const char *s = RedisModule_CallReplyStringPtr(RedisModule_CallReplyArrayElement(rep, i), NULL);
    if (!s) continue;

    docIds[i] = (t_docId)atol(s);
  }

  *num = sz;
  return docIds;
}

IndexIterator *NewGeoRangeIterator(GeoIndex *gi, GeoFilter *gf) {
  size_t sz;
  t_docId *docIds = __gr_load(gi, gf, &sz);
  if (!docIds) {
    return NULL;
  }

  IndexIterator *ret = NewIdListIterator(docIds, (t_offset)sz);
  rm_free(docIds);
  return ret;
}

/* Create a geo filter from parsed strings and numbers */
GeoFilter *NewGeoFilter(double lon, double lat, double radius, const char *unit) {
  GeoFilter *gf = malloc(sizeof(*gf));
  *gf = (GeoFilter){
      .lon = lon, .lat = lat, .radius = radius, .unit = unit,
  };
  return gf;
}

/* Make sure that the parameters of the filter make sense - i.e. coordinates are in range, radius is
 * sane, unit is valid. Return 1 if valid, 0 if not, and set the error string into err */
int GeoFilter_IsValid(GeoFilter *gf, char **err) {
  if (!gf->unit || (strcasecmp(gf->unit, "m") && strcasecmp(gf->unit, "km") &&
                    strcasecmp(gf->unit, "ft") && strcasecmp(gf->unit, "mi"))) {
    if (err) *err = "Invalid GeoFilter unit";

    return 0;
  }

  // validate lat/lon
  if (gf->lat > 90 || gf->lat < -90 || gf->lon > 180 || gf->lon < -180) {
    if (err) *err = "Invalid GeoFilter lat/lon";
    return 0;
  }

  // validate radius
  if (gf->radius <= 0) {
    if (err) *err = "Invalid GeoFilter radius";
    return 0;
  }

  return 1;
}

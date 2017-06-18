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

  if (rep == NULL || RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_ERROR) {
    return REDISMODULE_ERR;
  }

  return REDISMODULE_OK;
}

/* Parse a geo filter from redis arguments. We assume the filter args start at argv[0], and FILTER
 * is not passed to us.
 * The GEO filter syntax is (FILTER) <property> LONG LAT DIST m|km|ft|mi
 * Returns REDISMODUEL_OK or ERR  */
int GeoFilter_Parse(GeoFilter *gf, RedisModuleString **argv, int argc) {
  if (argc != 5) {
    return REDISMODULE_ERR;
  }

  if (RMUtil_ParseArgs(argv, argc, 0, "cdddc", &gf->property, &gf->lon, &gf->lat, &gf->radius,
                       &gf->unit) == REDISMODULE_ERR) {
    printf("error parsing\n");
    return REDISMODULE_ERR;
  }

  // verify unit
  if (!gf->unit || (strcasecmp(gf->unit, "m") && strcasecmp(gf->unit, "km") &&
                    strcasecmp(gf->unit, "ft") && strcasecmp(gf->unit, "mi"))) {
    // printf("wrong unit %s\n", gf->unit);
    return REDISMODULE_ERR;
  }
  gf->property = strdup(gf->property);
  gf->unit = strdup(gf->unit);
  return REDISMODULE_OK;
}

void GeoFilter_Free(GeoFilter *gf) {
  free((char *)gf->property);
  free((char *)gf->unit);
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

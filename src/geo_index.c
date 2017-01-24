#include "index.h"
#include "geo_index.h"
#include "rmutil/util.h"

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
    printf("wrong argc %d\n", argc);
    return REDISMODULE_ERR;
  }

  if (RMUtil_ParseArgs(argv, argc, 0, "cdddc", &gf->property, &gf->lon, &gf->lat, &gf->radius,
                       &gf->unit) == REDISMODULE_ERR) {
    printf("could not parse args\n");
    return REDISMODULE_ERR;
  }

  // verify unit
  if (!gf->unit || (strcasecmp(gf->unit, "m") && strcasecmp(gf->unit, "km") &&
                    strcasecmp(gf->unit, "ft") && strcasecmp(gf->unit, "mi"))) {
    printf("wrong unit %s\n", gf->unit);
    return REDISMODULE_ERR;
  }
  return REDISMODULE_OK;
}

static int cmp_docids(const void *p1, const void *p2) {
  const t_docId *d1 = p1, *d2 = p2;

  return d1 - d2;
}

GeoRangeIterator *__gr_load(GeoIndex *gi, GeoFilter *gf) {

  /*GEORADIUS key longitude latitude radius m|km|ft|mi */
  RedisModuleCtx *ctx = gi->ctx->redisCtx;
  RedisModuleString *ks = fmtGeoIndexKey(gi);
  RedisModuleCallReply *rep = RedisModule_Call(
      gi->ctx->redisCtx, "GEORADIUS", "ssssc", ks,
      RedisModule_CreateStringPrintf(ctx, "%f", gf->lon),
      RedisModule_CreateStringPrintf(ctx, "%f", gf->lat),
      RedisModule_CreateStringPrintf(ctx, "%f", gf->radius), gf->unit ? gf->unit : "km");

  if (rep == NULL || RedisModule_CallReplyType(rep) == REDISMODULE_REPLY_ARRAY) {
    return NULL;
  }

  GeoRangeIterator *ret = malloc(sizeof(GeoRangeIterator));
  ret->atEOF = 0;
  ret->idx = gi;
  ret->offset = 0;
  ret->lastDocId = 0;

  size_t sz = RedisModule_CallReplyLength(rep);
  ret->size = 0;
  ret->docIds = calloc(sz, sizeof(t_docId));
  for (size_t i = 0; i < sz; i++) {
    const char *s = RedisModule_CallReplyStringPtr(RedisModule_CallReplyArrayElement(rep, i), NULL);
    if (!s) continue;

    ret->docIds[ret->size++] = (t_docId)atol(s);
  }

  if (ret->size != 0) {
    qsort(ret->docIds, ret->size, sizeof(t_docId), cmp_docids);
  } else {
    ret->atEOF = 1;
  }
  return ret;
}
/* Read the next entry from the iterator, into hit *e.
*  Returns INDEXREAD_EOF if at the end */
int GR_Read(void *ctx, IndexResult *r) {
  GeoRangeIterator *it = ctx;
  if (it->atEOF || it->size == 0) {
    it->atEOF = 1;
    return INDEXREAD_EOF;
  }

  it->lastDocId = it->docIds[it->offset];
  ++it->offset;
  if (it->offset == it->size) {
    it->atEOF = 1;
  }
  // TODO: Filter here
  IndexRecord rec = {.flags = 0xFF, .docId = it->lastDocId, .tf = 0};
  IndexResult_PutRecord(r, &rec);

  return INDEXREAD_OK;
}

/* Skip to a docid, potentially reading the entry into hit, if the docId
 * matches */
int GR_SkipTo(void *ctx, u_int32_t docId, IndexResult *r) {
  GeoRangeIterator *it = ctx;
  if (it->atEOF || it->size == 0) {
    it->atEOF = 1;
    return INDEXREAD_EOF;
  }

  if (docId > it->docIds[it->size - 1]) {
    it->atEOF = 1;
    return INDEXREAD_EOF;
  }

  t_offset top = it->size - 1, bottom = it->offset;
  t_offset i = bottom;
  t_offset newi;

  while (bottom < top) {
    t_docId did = it->docIds[i];
    if (did == docId) {
      break;
    }
    if (docId <= did) {
      top = i;
    } else {
      bottom = i;
    }
    newi = (bottom + top) / 2;
    if (newi == i) {
      break;
    }
    i = newi;
  }
  it->offset = i + 1;
  if (it->offset == it->size) {
    it->atEOF = 1;
  }

  it->lastDocId = it->docIds[i];
  IndexRecord rec = {.flags = 0xFF, .docId = it->lastDocId, .tf = 0};
  IndexResult_PutRecord(r, &rec);
  // printf("lastDocId: %d, docId%d\n", it->lastDocId, docId);
  if (it->lastDocId == docId) {
    return INDEXREAD_OK;
  }
  return INDEXREAD_NOTFOUND;
}

/* the last docId read */
t_docId GR_LastDocId(void *ctx) {
  return ((GeoRangeIterator *)ctx)->lastDocId;
}

/* can we continue iteration? */
int GR_HasNext(void *ctx) {
  return !((GeoRangeIterator *)ctx)->atEOF;
}

/* release the iterator's context and free everything needed */
void GR_Free(struct indexIterator *self) {
  GeoRangeIterator *it = self->ctx;
  free(it->docIds);
  free(it);
  free(self);
}

/* Return the number of results in this iterator. Used by the query execution
 * on the top iterator */
size_t GR_Len(void *ctx) {
  return (size_t)((GeoRangeIterator *)ctx)->size;
}

IndexIterator *NewGeoRangeIterator(GeoIndex *gi, GeoFilter *gf) {
  GeoRangeIterator *it = __gr_load(gi, gf);
  if (!it) {
    return NULL;
  }

  IndexIterator *ret = malloc(sizeof(IndexIterator));
  ret->ctx = it;
  ret->Free = GR_Free;
  ret->HasNext = GR_HasNext;
  ret->LastDocId = GR_LastDocId;
  ret->Len = GR_Len;
  ret->Read = GR_Read;
  ret->SkipTo = GR_SkipTo;
  return ret;
}
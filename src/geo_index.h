#ifndef __GEO_INDEX_H__
#define __GEO_INDEX_H__

#include "types.h"
#include "redismodule.h"
#include "index_result.h"
#include "search_ctx.h"

typedef struct {
  RedisSearchCtx *ctx;
  FieldSpec *sp;
} GeoIndex;

typedef struct {
  GeoIndex *idx;
  t_docId *docIds;
  t_docId lastDocId;
  t_offset size;
  t_offset offset;
  int atEOF;
} GeoRangeIterator;

int GeoIndex_AddStrings(GeoIndex *gi, t_docId docId, char *slon, char *slat);

typedef struct {

  const char *property;
  double lat;
  double lon;
  double radius;
  const char *unit;
} GeoFilter;

/* Parse a geo filter from redis arguments. We assume the filter args start at argv[0] */
int GeoFilter_Parse(GeoFilter *gf, RedisModuleString **argv, int argc);

/* forward declaration */
struct indexIterator;

/* Read the next entry from the iterator, into hit *e.
  *  Returns INDEXREAD_EOF if at the end */
int GR_Read(void *ctx, IndexResult *e);

/* Skip to a docid, potentially reading the entry into hit, if the docId
 * matches */
int GR_SkipTo(void *ctx, u_int32_t docId, IndexResult *hit);

/* the last docId read */
t_docId GR_LastDocId(void *ctx);

/* can we continue iteration? */
int GR_HasNext(void *ctx);

/* release the iterator's context and free everything needed */
void GR_Free(struct indexIterator *self);

/* Return the number of results in this iterator. Used by the query execution
 * on the top iterator */
size_t GR_Len(void *ctx);

struct indexIterator *NewGeoRangeIterator(GeoIndex *gi, GeoFilter *gf);

#endif
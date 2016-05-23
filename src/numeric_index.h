#ifndef __NUMERIC_INDEX_H__
#define __NUMERIC_INDEX_H__
#include "types.h"
#include "spec.h"
#include "rmutil/strings.h"
#include "redismodule.h"
#include "index.h"

typedef struct numericIndex {
    RedisModuleKey *key;
    RedisSearchCtx *ctx;
} NumericIndex;

typedef struct {
    NumericIndex *idx;
    double min;
    double max;
    int minNegInf;
    int maxInf;
    int inclusiveMin;
    int inclusiveMax;
    t_docId lastDocid;
} NumericFilter;


NumericIndex *NewNumericIndex(RedisSearchCtx *ctx, FieldSpec *sp);

void NumerIndex_Free(NumericIndex *idx);

int NumerIndex_Add(NumericIndex *idx, t_docId docId, double score);

int NumericFilter_SkipTo(void *ctx, u_int32_t docId, IndexHit *hit);
int NumericFilter_Read(void *ctx, IndexHit *e);
int NumericFilter_HasNext(void *ctx);
t_docId NumericFilter_LastDocId(void *ctx);


NumericFilter *NewNumericFilter(RedisSearchCtx *ctx, FieldSpec *fs, double min, double max, int inclusiveMin, int inclusiveMax);

IndexIterator *NewNumericFilterIterator(NumericFilter *f);

NumericFilter *ParseNumericFilter(RedisSearchCtx *ctx, FieldSpec *fs, RedisModuleString **argv, int argv);

#endif
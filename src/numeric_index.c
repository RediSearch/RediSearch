#include "numeric_index.h"
/*
A numeric index allows indexing of documents by numeric ranges, and intersection of them with
fulltext indexes.
*/

int numericFilter_Match(NumericFilter *f, double score) {
    // match min - -inf or x >/>= score
    int matchMin = f->minNegInf || (f->inclusiveMin ? score >= f->min : score > f->min);

    if (matchMin) {
        // match max - +inf or x </<= score
        return f->maxInf || (f->inclusiveMax ? score <= f->max : score < f->max);
    }

    return 0;
}

#define NUMERIC_INDEX_KEY_FMT "num:%s/%s"

RedisModuleString *fmtNumericIndexKey(RedisSearchCtx *ctx, const char *field) {
    return RMUtil_CreateFormattedString(ctx->redisCtx, NUMERIC_INDEX_KEY_FMT, ctx->spec->name,
                                        field);
}

NumericIndex *NewNumericIndex(RedisSearchCtx *ctx, FieldSpec *sp) {
    RedisModuleString *s = fmtNumericIndexKey(ctx, sp->name);
    RedisModuleKey *k = RedisModule_OpenKey(ctx->redisCtx, s, REDISMODULE_READ | REDISMODULE_WRITE);
    if (k == NULL || (RedisModule_KeyType(k) != REDISMODULE_KEYTYPE_EMPTY &&
                      RedisModule_KeyType(k) != REDISMODULE_KEYTYPE_ZSET)) {
        k == NULL;
    }

    NumericIndex *ret = malloc(sizeof(NumericIndex));
    ret->ctx = ctx;
    ret->key = k;
    return ret;
}

void NumerIndex_Free(NumericIndex *idx) {
    if (idx->key) RedisModule_CloseKey(idx->key);
    free(idx);
}

int NumerIndex_Add(NumericIndex *idx, t_docId docId, double score) {
    if (idx->key == NULL) return REDISMODULE_ERR;

    return RedisModule_ZsetAdd(idx->key, score,
                               RMUtil_CreateFormattedString(idx->ctx->redisCtx, "%u", docId), NULL);
}

int NumericFilter_Read(void *ctx, IndexHit *e) {
    NumericFilter *f = ctx;
    if (!NumericFilter_HasNext(ctx) || f->idx->key == NULL) {
        return INDEXREAD_EOF;
    }

    if (!f->isRangeLoaded) {
        return INDEXREAD_OK;
    }

    Vector_Get(f->docIds, f->docIdsOffset++, &e->docId);
    e->flags = 0xFF;
    e->numOffsetVecs = 0;
    e->totalFreq = 0;
    e->type = H_RAW;

    return INDEXREAD_EOF;
}
// Skip to a docid, potentially reading the entry into hit, if the docId matches
//
// In this case we don't actually skip to a docId, just check whether it is within our range
int NumericFilter_SkipTo(void *ctx, u_int32_t docId, IndexHit *hit) {
    NumericFilter *f = ctx;
    if (f->idx == NULL || f->idx->key == NULL || !NumericFilter_HasNext(ctx)) {
        return INDEXREAD_EOF;
    }

    // for unloaded ranges - simply check if this docId is in the numeric range
    if (!f->isRangeLoaded) {
        double score = 0;
        f->lastDocid = docId;
        // See if the filter
        RedisModuleString *s = RedisModule_CreateStringFromLongLong(f->idx->ctx->redisCtx, docId);
        if (RedisModule_ZsetScore(f->idx->key, s, &score) == REDISMODULE_OK) {
            // RedisModule_FreeString(f->idx->ctx->redisCtx, s);
            if (numericFilter_Match(f, score)) {
                hit->docId = docId;
                hit->flags = 0xFF;
                hit->numOffsetVecs = 0;
                hit->totalFreq = 0;
                hit->type = H_RAW;
                return INDEXREAD_OK;
            }
        }
        // RedisModule_FreeString(f->idx->ctx->redisCtx, s);
        return INDEXREAD_NOTFOUND;
        ;
    }

    // if the index was loaded - just advance until we hit or pass the docId
    // TODO: Can be optimized with binary search
    while (f->lastDocid < docId && NumericFilter_HasNext(f)) {
        Vector_Get(f->docIds, f->docIdsOffset++, &f->lastDocid);
    }

    if (f->lastDocid == docId) {
        hit->flags = 0xFF;
        hit->numOffsetVecs = 0;
        hit->totalFreq = 0;
        hit->docId = f->lastDocid;
        return INDEXREAD_OK;
    } else if (!NumericFilter_HasNext(f)) {
        return INDEXREAD_EOF;
    }
    return INDEXREAD_NOTFOUND;
}

// the last docId read
t_docId NumericFilter_LastDocId(void *ctx) {
    NumericFilter *f = ctx;
    return f->lastDocid;
}
// can we continue iteration?
inline int NumericFilter_HasNext(void *ctx) {
    NumericFilter *f = ctx;

    if (!f->isRangeLoaded) return 1;

    return f->docIdsOffset < Vector_Size(f->docIds);
}
// release the iterator's context and free everything needed
void NumericFilter_Free(struct indexIterator *self) {
    NumericFilter *f = self->ctx;
    free(f->idx);
    if (f->docIds) {
        Vector_Free(f->docIds);
    }
    free(f);
    free(self);
}

NumericFilter *NewNumericFilter(RedisSearchCtx *ctx, FieldSpec *fs, double min, double max,
                                int inclusiveMin, int inclusiveMax) {
    NumericFilter *f = malloc(sizeof(NumericFilter));
    f->idx = NewNumericIndex(ctx, fs);
    f->min = min;
    f->max = max;
    f->inclusiveMax = inclusiveMax;
    f->inclusiveMin = inclusiveMin;
    f->lastDocid = 0;
    return f;
}

/* qsort docId comparison function */
int cmp_docId(const void *a, const void *b) {
    return *(const int *)a - *(const int *)b;  // casting pointer types
}

int _numericFilter_LoadRange(NumericFilter *f) {
    f->docIds = NewVector(t_docId, 1000);

    RedisModuleKey *key = f->idx->key;
    RedisModuleCtx *ctx = f->idx->ctx->redisCtx;
    RedisModule_ZsetFirstInScoreRange(key, f->minNegInf ? REDISMODULE_NEGATIVE_INFINITE : f->min,
                                      f->maxInf ? REDISMODULE_POSITIVE_INFINITE : f->max,
                                      !f->inclusiveMin, !f->inclusiveMax);

    int n = 0;
    while (!RedisModule_ZsetRangeEndReached(key)) {
        // abort loading the index if it's over a certain threshold which makes it too expensive
        // to load
        if (++n > NUMERICFILTER_LOAD_THRESHOLD) {
            RedisModule_ZsetRangeStop(key);
            Vector_Free(f->docIds);
            f->docIds = NULL;
            f->isRangeLoaded = 0;
            return 0;
        }
        RedisModuleString *ele = RedisModule_ZsetRangeCurrentElement(key, NULL);
        long long ll;
        RedisModule_StringToLongLong(ele, &ll);
        RedisModule_FreeString(ctx, ele);
        Vector_Push(f->docIds, (t_docId)ll);

        RedisModule_ZsetRangeNext(key);
    }
    RedisModule_ZsetRangeStop(key);

    qsort(f->docIds->data, Vector_Size(f->docIds), sizeof(t_docId), cmp_docId);

    f->docIdsOffset = 0;
    f->isRangeLoaded = 1;
    return Vector_Size(f->docIds);
}
IndexIterator *NewNumericFilterIterator(NumericFilter *f) {
    f->docIds = 0;
    f->lastDocid = 0;
    _numericFilter_LoadRange(f);
    IndexIterator *ret = malloc(sizeof(IndexIterator));
    ret->ctx = f;
    ret->Free = NumericFilter_Free;
    ret->HasNext = NumericFilter_HasNext;
    ret->LastDocId = NumericFilter_LastDocId;
    ret->Read = NumericFilter_Read;
    ret->SkipTo = NumericFilter_SkipTo;
    return ret;
}

/*
*  Parse numeric filter arguments, in the form of:
*  <fieldname> min max
*
*  By default, the interval specified by min and max is closed (inclusive).
*  It is possible to specify an open interval (exclusive) by prefixing the score with the character
* (.
*  For example: "score (1 5"
*  Will return filter elements with 1 < score <= 5
*
*  min and max can be -inf and +inf
*
*  Returns a numeric filter on success, NULL if there was a problem with the arguments
*/
NumericFilter *ParseNumericFilter(RedisSearchCtx *ctx, RedisModuleString **argv, int argc) {
    if (argc != 3) {
        return NULL;
    }
    // make sure we have an index spec for this filter and it's indeed numeric
    size_t len;
    const char *f = RedisModule_StringPtrLen(argv[0], &len);
    FieldSpec *fs = IndexSpec_GetField(ctx->spec, f, len);
    if (fs == NULL || fs->type != F_NUMERIC) {
        return NULL;
    }

    NumericIndex *ni = NewNumericIndex(ctx, fs);

    NumericFilter *nf = malloc(sizeof(NumericFilter));
    nf->idx = ni;
    nf->inclusiveMax = 1;
    nf->inclusiveMin = 1;
    nf->min = 0;
    nf->max = 0;
    nf->minNegInf = 0;
    nf->maxInf = 0;
    nf->docIdsOffset = 0;
    nf->lastDocid = 0;
    nf->isRangeLoaded = 0;
    // Parse the min range

    // -inf means anything is acceptable as a minimum
    if (RMUtil_StringEqualsC(argv[1], "-inf")) {
        nf->minNegInf = 1;
    } else {
        // parse the min range value - if it's OK we just set the value
        if (RedisModule_StringToDouble(argv[1], &nf->min) != REDISMODULE_OK) {
            size_t len = 0;
            const char *p = RedisModule_StringPtrLen(argv[1], &len);

            // if the first character is ( we treat the minimum as exclusive
            if (*p == '(' && len > 1) {
                p++;
                nf->inclusiveMin = 0;
                // we need to create a temporary string to parse it again...
                RedisModuleString *s = RedisModule_CreateString(ctx->redisCtx, p, len - 1);
                if (RedisModule_StringToDouble(s, &nf->min) != REDISMODULE_OK) {
                    RedisModule_FreeString(ctx->redisCtx, s);
                    goto error;
                }
                // free the string now that it's parsed
                RedisModule_FreeString(ctx->redisCtx, s);

            } else
                goto error;  // not a number
        }
    }

    // check if the max range is +inf
    if (RMUtil_StringEqualsC(argv[2], "+inf")) {
        nf->maxInf = 1;
    } else {
        // parse the max range. OK means we just read it into nf->max
        if (RedisModule_StringToDouble(argv[2], &nf->max) != REDISMODULE_OK) {
            // check see if the first char is ( and this is an exclusive range
            size_t len = 0;
            const char *p = RedisModule_StringPtrLen(argv[2], &len);
            if (*p == '(' && len > 1) {
                p++;
                nf->inclusiveMax = 0;
                // now parse the number part of the
                RedisModuleString *s = RedisModule_CreateString(ctx->redisCtx, p, len - 1);
                if (RedisModule_StringToDouble(s, &nf->max) != REDISMODULE_OK) {
                    RedisModule_FreeString(ctx->redisCtx, s);
                    goto error;
                }
                RedisModule_FreeString(ctx->redisCtx, s);

            } else
                goto error;  // not a number
        }
    }

    return nf;

error:
    free(nf->idx);
    free(nf);
    return NULL;
}
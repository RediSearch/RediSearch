#include "numeric_filter.h"
#include "rmutil/strings.h"
#include "rmutil/util.h"
#include "rmutil/vector.h"
/*
*  Parse numeric filter arguments, in the form of:
*  <fieldname> min max
*
*  By default, the interval specified by min and max is closed (inclusive).
*  It is possible to specify an open interval (exclusive) by prefixing the score
* with the character
* (.
*  For example: "score (1 5"
*  Will return filter elements with 1 < score <= 5
*
*  min and max can be -inf and +inf
*
*  Returns a numeric filter on success, NULL if there was a problem with the
* arguments
*/
NumericFilter *ParseNumericFilter(RedisSearchCtx *ctx, RedisModuleString **argv, int argc) {
  if (argc < 3) {
    return NULL;
  }
  // make sure we have an index spec for this filter and it's indeed numeric
  size_t len;
  const char *f = RedisModule_StringPtrLen(argv[0], &len);
  FieldSpec *fs = IndexSpec_GetField(ctx->spec, f, len);
  if (fs == NULL || fs->type != F_NUMERIC) {
    return NULL;
  }

  NumericFilter *nf = malloc(sizeof(NumericFilter));
  nf->fieldName = strndup(f, len);
  nf->inclusiveMax = 1;
  nf->inclusiveMin = 1;
  nf->min = 0;
  nf->max = 0;

  // Parse the min range

  // -inf means anything is acceptable as a minimum
  if (RMUtil_StringEqualsC(argv[1], "-inf")) {
    nf->min = NF_NEGATIVE_INFINITY;
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
    nf->max = NF_INFINITY;
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

  free(nf);
  return NULL;
}

void NumericFilter_Free(NumericFilter *nf) {
  if (nf->fieldName) {
    free((char *)nf->fieldName);
  }
  free(nf);
}

/* Parse multiple filters from an argument list. Returns a vector of filters parse, or NULL if no
 * filter could be parsed */
Vector *ParseMultipleFilters(RedisSearchCtx *ctx, RedisModuleString **argv, int argc) {

  int offset = RMUtil_ArgIndex("FILTER", argv, argc);
  if (offset == -1) {
    return NULL;
  }

  // the base offset from the original argv
  int base = 0;
  Vector *vec = NewVector(NumericFilter *, 2);
  while (offset >= 0) {

    base++;
    NumericFilter *flt = ParseNumericFilter(ctx, &argv[base + offset], argc - (offset + base));
    if (flt) {
      Vector_Push(vec, flt);

      base += 3;
      offset = RMUtil_ArgIndex("FILTER", &argv[base + offset], argc - (offset + base));
    } else {
      // we got a FILTER keyword but invalid filter - return NULL
      Vector_Free(vec);
      return NULL;
    }
  }

  return vec;
}

NumericFilter *NewNumericFilter(double min, double max, int inclusiveMin, int inclusiveMax) {
  NumericFilter *f = malloc(sizeof(NumericFilter));

  f->min = min;
  f->max = max;
  f->fieldName = NULL;
  f->inclusiveMax = inclusiveMax;
  f->inclusiveMin = inclusiveMin;
  return f;
}

/*
A numeric index allows indexing of documents by numeric ranges, and intersection
of them with
fulltext indexes.
*/
inline int NumericFilter_Match(NumericFilter *f, double score) {

  int rc = 0;
  // match min - -inf or x >/>= score
  int matchMin = (f->inclusiveMin ? score >= f->min : score > f->min);

  if (matchMin) {
    // match max - +inf or x </<= score
    rc = (f->inclusiveMax ? score <= f->max : score < f->max);
  }

  // printf("numeric filter %s=>%f..%f. match %f?  %d\n", f->fieldName, f->min, f->max, score, rc);
  return rc;
}

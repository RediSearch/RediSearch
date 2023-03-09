/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once

#include "geo_index.h"
#include "query_parser/tokenizer.h"
#include "param.h"

struct QueryParseCtx;

typedef enum {
  QP_GEO_FILTER,
  QP_NUMERIC_FILTER,
  } QueryParamType;

typedef struct {
  union {
    GeoFilter *gf;
    NumericFilter *nf;
  };
  QueryParamType type;
  Param *params;
} QueryParam;

QueryParam *NewQueryParam(QueryParamType type);
QueryParam *NewGeoFilterQueryParam_WithParams(struct QueryParseCtx *q, QueryToken *lon, QueryToken *lat, QueryToken *radius, QueryToken *unit);

QueryParam *NewNumericFilterQueryParam_WithParams(struct QueryParseCtx *q, QueryToken *min, QueryToken *max, int inclusiveMin, int inclusiveMax);

#define QueryParam_NumParams(p) (array_len((p)->params))
#define QueryParam_GetParam(p, ix) (QueryParam_NumParams(p) > ix ? (p)->params[ix] : NULL)

void QueryParam_InitParams(QueryParam *p, size_t num);
void QueryParam_Free(QueryParam *p);

/*
 * Resolve the value of a param
 * Return 0 if not parameterized
 * Return 1 if value was resolved successfully
 * Return -1 if param is missing or its kind is wrong
 */
int QueryParam_Resolve(Param *param, dict *params, QueryError *status);

/*
 * Set the `target` Param according to `source`
 * Return true if `source` is parameterized (not a concrete value)
 * Return false otherwise
 */
bool QueryParam_SetParam(struct QueryParseCtx *q, Param *target_param, void *target_value,
                         size_t *target_len, QueryToken *source);

/*
 * Parse the parameters from ac into the dest params.
 */
int parseParams (dict **destParams, ArgsCursor *ac, QueryError *status);

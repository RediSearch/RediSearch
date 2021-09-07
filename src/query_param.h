#pragma once
#include "geo_index.h"
#include "query_parser/tokenizer.h"
#include "param.h"

typedef enum {
  QP_TOK,
  QP_GEO_FILTER,
  QP_NUMERIC_FILTER,
  QP_RANGE_NUMBER,
  } QueryParamType;

typedef struct {
  union {
    QueryToken *qt;
    GeoFilter *gf;
    NumericFilter *nf;
    RangeNumber *rn;
  };
  QueryParamType type;
  Param *params;
} QueryParam;


QueryParam *NewQueryParam(QueryParamType type);
QueryParam *NewTokenQueryParam(QueryToken *qt);
QueryParam *NewGeoFilterQueryParam(GeoFilter *gf);
QueryParam *NewGeoFilterQueryParam_WithParams(QueryToken *lon, QueryToken *lat, QueryToken *radius, QueryToken *unit);

#define QueryParam_NumParams(p) ((p)->params ? array_len((p)->params) : 0)
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
 * Set the target Param according to the source
 */
void QueryParam_SetParam(Param *target_param, void *target_value, size_t *target_len, QueryToken *source);

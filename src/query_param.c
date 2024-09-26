/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "query_param.h"
#include "query_error.h"
#include "geo_index.h"
#include "numeric_filter.h"
#include "query_internal.h"

QueryParam *NewQueryParam(QueryParamType type) {
  QueryParam *ret = rm_calloc(1, sizeof(*ret));
  ret->type = type;
  return ret;
}

QueryParam *NewGeoFilterQueryParam_WithParams(struct QueryParseCtx *q, QueryToken *lon, QueryToken *lat, QueryToken *radius, QueryToken *unit) {
  QueryParam *ret = NewQueryParam(QP_GEO_FILTER);

  GeoFilter *gf = NewGeoFilter(0, 0, 0, NULL, 0); // TODO: Just call rm_calloc ?
  ret->gf = gf;
  QueryParam_InitParams(ret, 4);
  QueryParam_SetParam(q, &ret->params[0], &gf->lon, NULL, lon);
  QueryParam_SetParam(q, &ret->params[1], &gf->lat, NULL, lat);
  QueryParam_SetParam(q, &ret->params[2], &gf->radius, NULL, radius);
  assert (unit->type != QT_TERM_CASE);
  if (unit->type == QT_TERM && unit->s) {
    gf->unitType = GeoDistance_Parse_Buffer(unit->s, unit->len);
  } else if (unit->type == QT_PARAM_GEO_UNIT) {
    QueryParam_SetParam(q, &ret->params[3], &gf->unitType, NULL, unit);
  } else {
    QERR_MKSYNTAXERR(q->status, "Invalid GeoFilter unit");
  }
  return ret;
}

QueryParam *NewNumericFilterQueryParam_WithParams(struct QueryParseCtx *q, QueryToken *min, QueryToken *max, int inclusiveMin, int inclusiveMax) {
  QueryParam *ret = NewQueryParam(QP_NUMERIC_FILTER);
  NumericFilter *nf = NewNumericFilter(0, 0, inclusiveMin, inclusiveMax, true);
  ret->nf = nf;
  QueryParam_InitParams(ret, 2);
  if(min != NULL) {
    QueryParam_SetParam(q, &ret->params[0], &nf->min, NULL, min);
  } else {
    nf->min = -INFINITY;
  }
  if(max != NULL) {
    QueryParam_SetParam(q, &ret->params[1], &nf->max, NULL, max);
  } else {
    nf->max = INFINITY;
  }
  return ret;
}

void QueryParam_Free(QueryParam *p) {
  switch (p->type) {
    case QP_GEO_FILTER:
      GeoFilter_Free(p->gf);
      break;
    case QP_NUMERIC_FILTER:
      NumericFilter_Free(p->nf);
      break;
  }
  size_t n = QueryParam_NumParams(p);
  if (n) {
    for (size_t ii = 0; ii < n; ++ii) {
      Param_FreeInternal(&p->params[ii]);
    }
    array_free(p->params);
  }
  p->params = NULL;
  rm_free(p);
}

bool QueryParam_SetParam(QueryParseCtx *q, Param *target_param, void *target_value,
                         size_t *target_len, QueryToken *source) {

  ParamType type = PARAM_NONE;
  switch (source->type) {

  case QT_TERM:
    target_param->type = PARAM_NONE;
    *(char**)target_value = rm_strdupcase(source->s, source->len);
    if (target_len) *target_len = strlen(target_value);
    return false; // done

  case QT_TERM_CASE:
    target_param->type = PARAM_NONE;
    *(char**)target_value = rm_strndup(source->s, source->len);
    if (target_len) *target_len = source->len;
    return false; // done

  case QT_NUMERIC:
    target_param->type = PARAM_NONE;
    *(double *)target_value = source->numval;
    return false; // done

  case QT_SIZE:
    target_param->type = PARAM_NONE;
    *(size_t *)target_value = (size_t)source->numval;
    return false; // done

  case QT_WILDCARD:
    target_param->type = PARAM_NONE;
    *(char**)target_value = rm_calloc(1, source->len + 1);
    memcpy(*(char**)target_value, source->s, source->len);
    if (target_len) *target_len = source->len;
    return false; // done

  case QT_PARAM_ANY:
    type = PARAM_ANY;
    break;
  case QT_PARAM_TERM:
    type = PARAM_TERM;
    break;
  case QT_PARAM_TERM_CASE:
    type = PARAM_TERM_CASE;
    break;
  case QT_PARAM_NUMERIC:
    type = PARAM_NUMERIC;
    break;
  case QT_PARAM_NUMERIC_MIN_RANGE:
    type = PARAM_NUMERIC_MIN_RANGE;
    break;
  case QT_PARAM_NUMERIC_MAX_RANGE:
    type = PARAM_NUMERIC_MAX_RANGE;
    break;
  case QT_PARAM_GEO_UNIT:
    type = PARAM_GEO_UNIT;
    break;
  case QT_PARAM_GEO_COORD:
    type = PARAM_GEO_COORD;
    break;
  case QT_PARAM_VEC:
    type = PARAM_VEC;
    break;
  case QT_PARAM_SIZE:
    type = PARAM_SIZE;
    break;
  case QT_PARAM_WILDCARD:
    type = PARAM_WILDCARD;
    break;
  }
  target_param->type = type;
  target_param->target = target_value;
  target_param->target_len = target_len;
  target_param->name = rm_strndup(source->s, source->len);
  target_param->len = source->len;
  target_param->sign = source->sign;
  q->numParams++;
  return true;
}

void QueryParam_InitParams(QueryParam *p, size_t num) {
  p->params = array_newlen(Param, num);
  memset(p->params, 0, sizeof(*p->params) * num);
}

int QueryParam_Resolve(Param *param, dict *params, QueryError *status) {
  if (param->type == PARAM_NONE)
    return 0;
  size_t val_len;
  const char *val = Param_DictGet(params, param->name, &val_len, status);
  if (!val)
    return -1;

  int val_is_numeric = 0;
  switch(param->type) {

    case PARAM_NONE:
      return 0;

    case PARAM_ANY:
    case PARAM_TERM:
      if (ParseDouble(val, (double*)param->target, param->sign)) {
        // parsed as double to check +inf, -inf
        val_is_numeric = 1;
      }
      *(char**)param->target = rm_strdupcase(val, val_len);
      if (param->target_len) *param->target_len = strlen(*(char**)param->target);
      return 1 + val_is_numeric;

    case PARAM_WILDCARD:
      *(char**)param->target = rm_calloc(1, val_len + 1);
      memcpy(*(char**)param->target, val, val_len);
      if (param->target_len) *param->target_len = val_len;
      return 1;

    case PARAM_TERM_CASE:
      *(char**)param->target = rm_strdup(val);
      if (param->target_len) *param->target_len = val_len;
      return 1;

    case PARAM_NUMERIC:
    case PARAM_GEO_COORD:
      if (!ParseDouble(val, (double*)param->target, param->sign)) {
        QueryError_SetErrorFmt(status, QUERY_ESYNTAX, "Invalid numeric value (%s) for parameter `%s`", \
        val, param->name);
        return -1;
      }
      return 1;

    case PARAM_SIZE:
      if (!ParseInteger(val, (long long *)param->target) || *(long long *)param->target < 0) {
        QueryError_SetErrorFmt(status, QUERY_ESYNTAX, "Invalid numeric value (%s) for parameter `%s`", \
        val, param->name);
        return -1;
      }
      return 1;

    case PARAM_NUMERIC_MIN_RANGE:
    case PARAM_NUMERIC_MAX_RANGE:
    {
      int inclusive = 1;
      int isMin = param->type == PARAM_NUMERIC_MIN_RANGE ? 1 : 0;
      if (parseDoubleRange(val, &inclusive, (double*)param->target, isMin, param->sign, status) == REDISMODULE_OK)
        return 1;
      else
        return -1;
    }

    case PARAM_GEO_UNIT:
      *(GeoDistance*)param->target = GeoDistance_Parse(val);
      return 1;

    case PARAM_VEC: {
      *(const char **)param->target = val;
      *param->target_len = val_len;
      return 1;
    }
  }
  return -1;
}

int parseParams (dict **destParams, ArgsCursor *ac, QueryError *status) {
  ArgsCursor paramsArgs = {0};
  int rv = AC_GetVarArgs(ac, &paramsArgs);
  if (rv != AC_OK) {
    QERR_MKBADARGS_AC(status, "PARAMS", rv);
    return REDISMODULE_ERR;
  }
  if (*destParams) {
    QueryError_SetError(status, QUERY_EADDARGS,"Multiple PARAMS are not allowed. Parameters can be defined only once");
    return REDISMODULE_ERR;
  }
  if (paramsArgs.argc == 0 || paramsArgs.argc % 2) {
    QueryError_SetError(status, QUERY_EADDARGS,"Parameters must be specified in PARAM VALUE pairs");
    return REDISMODULE_ERR;
  }

  dict *params = Param_DictCreate();
  size_t value_len;
  while (!AC_IsAtEnd(&paramsArgs)) {
    const char *param = AC_GetStringNC(&paramsArgs, NULL);
    const char *value = AC_GetStringNC(&paramsArgs, &value_len);
    // FIXME: Validate param is [a-zA-Z][a-zA-z_\-:0-9]*
    if (DICT_ERR == Param_DictAdd(params, param, value, value_len, status)) {
      Param_DictFree(params);
      return REDISMODULE_ERR;
    }
  }
  *destParams = params;

  return REDISMODULE_OK;
}

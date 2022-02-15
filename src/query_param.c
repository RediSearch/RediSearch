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
  } else {
    QueryParam_SetParam(q, &ret->params[3], &gf->unitType, NULL, unit);
  }
  return ret;
}

QueryParam *NewNumericFilterQueryParam_WithParams(struct QueryParseCtx *q, QueryToken *min, QueryToken *max, int inclusiveMin, int inclusiveMax) {
  QueryParam *ret = NewQueryParam(QP_NUMERIC_FILTER);
  NumericFilter *nf = NewNumericFilter(0, 0, inclusiveMin, inclusiveMax);
  ret->nf = nf;
  QueryParam_InitParams(ret, 2);
  QueryParam_SetParam(q, &ret->params[0], &nf->min, NULL, min);
  QueryParam_SetParam(q, &ret->params[1], &nf->max, NULL, max);
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
  }
  target_param->type = type;
  target_param->target = target_value;
  target_param->target_len = target_len;
  target_param->name = rm_strndup(source->s, source->len);
  target_param->len = source->len;
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

  switch(param->type) {

    case PARAM_NONE:
      return 0;

    case PARAM_ANY:
    case PARAM_TERM:
      *(char**)param->target = rm_strdupcase(val, val_len);
      if (param->target_len) *param->target_len = strlen(*(char**)param->target);
      return 1;

    case PARAM_TERM_CASE:
      *(char**)param->target = rm_strdup(val);
      if (param->target_len) *param->target_len = val_len;
      return 1;

    case PARAM_NUMERIC:
    case PARAM_GEO_COORD:
      if (!ParseDouble(val, (double*)param->target)) {
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
      int inclusive;
      int isMin = param->type == PARAM_NUMERIC_MIN_RANGE ? 1 : 0;
      if (parseDoubleRange(val, &inclusive, (double*)param->target, isMin , status) == REDISMODULE_OK)
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

#include "query_param.h"
#include "query_error.h"
#include "geo_index.h"

QueryParam *NewQueryParam(QueryParamType type) {
  QueryParam *ret = rm_calloc(1, sizeof(*ret));
  ret->type = type;
  return ret;
}

QueryParam *NewTokenQueryParam(QueryToken *qt) {
  QueryParam *ret = NewQueryParam(QP_TOK);
  ret->qt = qt;
  return ret;
}

QueryParam *NewGeoFilterQueryParam(GeoFilter *gf) {
  QueryParam *ret = NewQueryParam(QP_GEO_FILTER);
  ret->gf = gf;
  return ret;
}

QueryParam *NewGeoFilterQueryParam_WithParams(QueryToken *lon, QueryToken *lat, QueryToken *radius, QueryToken *unit) {
  QueryParam *ret = NewQueryParam(QP_GEO_FILTER);

  GeoFilter *gf = NewGeoFilter(0, 0, 0, NULL, 0); // TODO: Just call rm_calloc ?
  ret->gf = gf;
  QueryParam_InitParams(ret, 5);
  QueryParam_SetParam(&ret->params[1], &gf->lon, NULL, lon);
  QueryParam_SetParam(&ret->params[2], &gf->lat, NULL, lat);
  QueryParam_SetParam(&ret->params[3], &gf->radius, NULL, radius);
  if (unit->type == QT_TERM && unit->s) {
    gf->unitType = GeoDistance_Parse_Buffer(unit->s, unit->len);
  } else {
    QueryParam_SetParam(&ret->params[4], &gf->unitType, NULL, unit);
  }
  return ret;
}

void QueryParam_Free(QueryParam *p) {
  size_t n = QueryParam_NumParams(p);
  if (n) {
    for (size_t ii = 0; ii < n; ++ii) {
      Param_Free(&p->params[ii]);
    }
    array_free(p->params);
  }
  p->params = NULL;
  rm_free(p);
}

void QueryParam_SetParam(Param *target_param, void *target_value, size_t *target_len, QueryToken *source) {

  if (source->type == QT_TERM) {
    target_param->type = PARAM_NONE;
    *(char**)target_value = strndup(source->s, source->len);
  } else if (source->type == QT_NUMERIC) {
    target_param->type = PARAM_NONE;
    *(double *)target_value = source->numval;
  }else {
    ParamType type;
    if (source->type == QT_PARAM_ANY)
      type = PARAM_ANY;
    else if (source->type == QT_PARAM_TERM)
      type = PARAM_TERM;
    else if (source->type == QT_PARAM_NUMERIC)
      type = PARAM_NUMERIC;
    else if (source->type == QT_PARAM_GEO_UNIT)
      type = PARAM_GEO_UNIT;
    else if (source->type == QT_PARAM_GEO_COORD)
      type = PARAM_GEO_COORD;
    else
      type = PARAM_ANY; // avoid warning - not supposed to reach here - all source->type enum options are covered

    target_param->type = type;
    target_param->target = target_value;
    target_param->target_len = target_len;
    target_param->name = strndup(source->s, source->len);
    target_param->len = source->len;
  }
}

void QueryParam_InitParams(QueryParam *p, size_t num) {
  p->params = array_newlen(Param, num);
  memset(p->params, 0, sizeof(*p->params) * num);
}

int QueryParam_Resolve(Param *param, dict *params, QueryError *status) {
  if (param->type == PARAM_NONE)
    return 0;
  dictEntry *e = dictFind(params, param->name);
  if (!e) {
    QueryError_SetErrorFmt(status, QUERY_ENOPARAM, "No such parameter `%s`", param->name);
    return -1;
  }
  char *val = dictGetVal(e);

  switch(param->type) {

    case PARAM_NONE:
      return 0;

    case PARAM_ANY:
    case PARAM_TERM:
      *(char**)param->target = strdup(val);
      *param->target_len = strlen(val);
      return 1;

    case PARAM_NUMERIC:
    case PARAM_GEO_COORD:
      if (!ParseDouble(val, (double*)param->target)) {
        QueryError_SetErrorFmt(status, QUERY_ESYNTAX, "Invalid numeric value (%s) for parameter `%s`", \
        val, param->name);
        return -1;
      }
      return 1;

    case PARAM_GEO_UNIT:
      *(GeoDistance*)param->target = GeoDistance_Parse(val);
      return 1;

  }
  return -1;
}

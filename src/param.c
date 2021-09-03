#include "param.h"
#include "rmalloc.h"
#include "query_error.h"
#include "geo_index.h"

Param *NewParam(const char *name, size_t len, ParamKind kind) {
  Param *param = rm_calloc(1, sizeof(*param));
  *param = (Param) {
    .name = rm_strndup(name, len),
    .len = len,
    .kind = kind,
  };
  return param;
}

void Param_Free(Param *param) {
  rm_free((void*)param->name);
}

int Param_Resolve(Param *param, dict *params, QueryError *status) {
  if (param->kind == PARAM_NONE)
    return 0;
  dictEntry *e = dictFind(params, param->name);
  if (!e) {
    QueryError_SetErrorFmt(status, QUERY_ENOPARAM, "%s: no such parameter", param->name);
    return -1;
  }
  char *val = dictGetVal(e);

  switch(param->kind) {
    case PARAM_ANY:
    case PARAM_TERM:
      param->target = val;
      return 1;

    case PARAM_NUMERIC:
    case PARAM_GEO_COORD:
    {
      char *end = NULL;
      double d = strtod(val, &end);
      *(double*)param->target = d;
      return 1;
    }

    case PARAM_GEO_UNIT:
      *(GeoDistance*)param->target = GeoDistance_Parse(val);
      return 1;

    case PARAM_NONE:
      return 0;
  }
  return -1;
}
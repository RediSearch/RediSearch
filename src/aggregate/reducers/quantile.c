#include <aggregate/reducer.h>
#include "util/quantile.h"

typedef struct {
  RSKey property;
  double pct;
} quantileParams;

typedef struct {
  QuantStream *strm;
  quantileParams *params;
  RSSortingTable *sortables;
} quantileCtx;

static void *quantile_NewInstance(ReducerCtx *ctx) {
  quantileCtx *qctx = calloc(1, sizeof(*qctx));
  qctx->params = ctx->privdata;
  qctx->strm = NewQuantileStream(&qctx->params->pct, 1, 500);
  qctx->sortables = ctx->ctx->spec->sortables;
  return qctx;
}

static int quantile_Add(void *ctx, SearchResult *res) {
  quantileCtx *qctx = ctx;
  double d;
  RSValue *v = SearchResult_GetValue(res, qctx->sortables, &qctx->params->property);
  if (v == NULL || !RSValue_ToNumber(v, &d)) {
    return 1;
  }
  QS_Insert(qctx->strm, d);
  return 1;
}

static int quantile_Finalize(void *ctx, const char *key, SearchResult *res) {
  quantileCtx *qctx = ctx;
  double value = QS_Query(qctx->strm, qctx->params->pct);
  RSFieldMap_Set(&res->fields, key, RS_NumVal(value));
  return 1;
}

static void quantile_Free(Reducer *r) {
  quantileParams *params = r->ctx.privdata;
  // free(params->property);
  free(params);
  free(r);
}

static void quantile_FreeInstance(void *p) {
  quantileCtx *qctx = p;
  QS_Free(qctx->strm);
  free(qctx);
}

Reducer *NewQuantile(RedisSearchCtx *ctx, const char *property, const char *alias, double pct) {
  Reducer *r = malloc(sizeof(*r));
  r->Add = quantile_Add;
  r->Finalize = quantile_Finalize;
  r->Free = quantile_Free;
  r->FreeInstance = quantile_FreeInstance;
  r->NewInstance = quantile_NewInstance;
  r->alias = alias ? alias : "quantile";

  quantileParams *params = calloc(1, sizeof(*params));

  params->property = RS_KEY(property);
  params->pct = pct;
  r->ctx = (ReducerCtx){.ctx = ctx, .privdata = params};
  return r;
}
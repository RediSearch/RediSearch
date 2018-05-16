#include <aggregate/reducer.h>

struct sumCtx {
  size_t count;
  double total;
  RSKey property;
  RSSortingTable *sortables;
  int isAvg;  // We might use an enum if there are more modes
};

void *sum_NewInstance(ReducerCtx *rctx) {
  struct sumCtx *ctx = ReducerCtx_Alloc(rctx, sizeof(*ctx), 100*sizeof(*ctx));  // malloc(sizeof(*ctr));
  ctx->count = 0;
  ctx->total = 0;
  ctx->sortables = SEARCH_CTX_SORTABLES(rctx->ctx);
  ctx->property = RS_KEY(rctx->property);
  ctx->isAvg = rctx->privdata != NULL;
  return ctx;
}

int sum_Add(void *ctx, SearchResult *res) {
  struct sumCtx *ctr = ctx;
  ctr->count++;
  RSValue *v = SearchResult_GetValue(res, ctr->sortables, &ctr->property);

  if (v && v->t == RSValue_Number) {
    ctr->total += v->numval;
  } else {  // try to convert value to number
    double d = 0;
    if (RSValue_ToNumber(v, &d)) {
      ctr->total += d;
    }
  }

  return 1;
}

int sum_Finalize(void *ctx, const char *key, SearchResult *res) {
  struct sumCtx *ctr = ctx;
  double v = 0;
  if (ctr->isAvg) {
    if (ctr->count) {
      v = ctr->total / ctr->count;
    }
  } else {
    v = ctr->total;
  }

  RSFieldMap_SetNumber(&res->fields, key, v);
  return 1;
}

void sum_FreeInstance(void *p) {
  struct sumCtx *c = p;
  free(c);
}

static int sentinel =  0;
Reducer *newSumCommon(RedisSearchCtx *ctx, const char *property, const char *alias, int isAvg) {
  Reducer *r = malloc(sizeof(*r));
  r->Add = sum_Add;
  r->Finalize = sum_Finalize;
  r->Free = Reducer_GenericFreeWithStaticPrivdata;
  r->FreeInstance = NULL;
  r->NewInstance = sum_NewInstance;
  r->alias = FormatAggAlias(alias, isAvg ? "avg" : "sum", property);
  // Note, malloc for one byte because it's freed at the end. Simple pointer won't do
  r->ctx = (ReducerCtx){.ctx = ctx, .property = property, .privdata = isAvg ? &sentinel : NULL};

  return r;
}

Reducer *NewSum(RedisSearchCtx *ctx, const char *property, const char *alias) {
  return newSumCommon(ctx, property, alias, 0);
}

Reducer *NewAvg(RedisSearchCtx *ctx, const char *property, const char *alias) {
  return newSumCommon(ctx, property, alias, 1);
}
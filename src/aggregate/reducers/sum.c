#include <aggregate/reducer.h>

struct sumCtx {
  size_t count;
  double total;
  RSKey property;
  RSSortingTable *sortables;
};

void *sum_NewInstance(ReducerCtx *rctx) {
  struct sumCtx *ctx = malloc(sizeof(*ctx));
  ctx->count = 0;
  ctx->total = 0;
  ctx->sortables = rctx->ctx->spec->sortables;
  ctx->property = RS_KEY(rctx->property);
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
  RSFieldMap_Set(&res->fields, key, RS_NumVal(ctr->total));
  return 1;
}

void sum_FreeInstance(void *p) {
  struct sumCtx *c = p;
  free(c);
}

Reducer *NewSum(RedisSearchCtx *ctx, const char *property, const char *alias) {
  Reducer *r = malloc(sizeof(*r));
  r->Add = sum_Add;
  r->Finalize = sum_Finalize;
  r->Free = Reducer_GenericFree;
  r->FreeInstance = sum_FreeInstance;
  r->NewInstance = sum_NewInstance;
  r->alias = FormatAggAlias(alias, "sum", property);
  r->ctx = (ReducerCtx){.ctx = ctx, .property = property};

  return r;
}
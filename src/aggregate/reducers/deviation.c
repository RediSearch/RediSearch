#include <aggregate/reducer.h>
#include <math.h>

typedef struct {
  RSKey property;
  size_t n;
  double oldM, newM, oldS, newS;
  RSSortingTable *sortables;
} devCtx;

static void *stddev_NewInstance(ReducerCtx *ctx) {
  devCtx *dctx = calloc(1, sizeof(*dctx));
  dctx->property = RS_KEY(ctx->property);
  dctx->sortables = SEARCH_CTX_SORTABLES(ctx->ctx);
  return dctx;
}

void stddev_addInternal(devCtx *dctx, double d) {
  // https://www.johndcook.com/blog/standard_deviation/
  dctx->n++;
  if (dctx->n == 1) {
    dctx->oldM = dctx->newM = d;
    dctx->oldS = 0.0;
  } else {
    dctx->newM = dctx->oldM + (d - dctx->oldM) / dctx->n;
    dctx->newS = dctx->oldS + (d - dctx->oldM) * (d - dctx->newM);

    // set up for next iteration
    dctx->oldM = dctx->newM;
    dctx->oldS = dctx->newS;
  }
}

static int stddev_Add(void *ctx, SearchResult *res) {
  devCtx *dctx = ctx;
  double d;
  RSValue *v = SearchResult_GetValue(res, dctx->sortables, &dctx->property);
  if (v) {
    if (v->t != RSValue_Array) {
      if (RSValue_ToNumber(v, &d)) {
        stddev_addInternal(dctx, d);
      }
    } else {
      uint32_t sz = RSValue_ArrayLen(v);
      for (uint32_t i = 0; i < sz; i++) {
        if (RSValue_ToNumber(RSValue_ArrayItem(v, i), &d)) {
          stddev_addInternal(dctx, d);
        }
      }
    }
  }
  return 1;
}

static int stddev_Finalize(void *ctx, const char *key, SearchResult *res) {
  devCtx *dctx = ctx;
  double variance = ((dctx->n > 1) ? dctx->newS / (dctx->n - 1) : 0.0);
  double stddev = sqrt(variance);
  RSFieldMap_SetNumber(&res->fields, key, stddev);
  return 1;
}

static void stddev_FreeInstance(void *p) {
  free(p);
}

Reducer *NewStddev(RedisSearchCtx *ctx, const char *property, const char *alias) {
  Reducer *r = malloc(sizeof(*r));
  r->Add = stddev_Add;
  r->Finalize = stddev_Finalize;
  r->Free = Reducer_GenericFree;
  r->FreeInstance = stddev_FreeInstance;
  r->NewInstance = stddev_NewInstance;
  r->alias = FormatAggAlias(alias, "stddev", property);
  r->ctx = (ReducerCtx){.ctx = ctx, .property = property};
  return r;
}
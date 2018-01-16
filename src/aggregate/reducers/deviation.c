#include <aggregate/reducer.h>
#include <math.h>

typedef struct {
  char *property;
  size_t n;
  double oldM, newM, oldS, newS;
  RSSortingTable *sortables;
} devCtx;

static void *stddev_NewInstance(ReducerCtx *ctx) {
  devCtx *dctx = calloc(1, sizeof(*dctx));
  dctx->property = ctx->privdata;
  dctx->sortables = ctx->ctx->spec->sortables;
  return dctx;
}

static int stddev_Add(void *ctx, SearchResult *res) {
  devCtx *dctx = ctx;
  double d;
  RSValue *v = SearchResult_GetValue(res, dctx->sortables, dctx->property);
  if (v == NULL || !RSValue_ToNumber(v, &d)) {
    return 1;
  }

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
  return 1;
}

static int stddev_Finalize(void *ctx, const char *key, SearchResult *res) {
  devCtx *dctx = ctx;
  double variance = ((dctx->n > 1) ? dctx->newS / (dctx->n - 1) : 0.0);
  double stddev = sqrt(variance);
  RSFieldMap_Set(&res->fields, key, RS_NumVal(stddev));
  return 1;
}

static void stddev_FreeInstance(void *p) {
  free(p);
}

static void stddev_Free(Reducer *r) {
  free(r->ctx.privdata);
  free(r);
}

Reducer *NewStddev(RedisSearchCtx *ctx, const char *property, const char *alias) {
  Reducer *r = malloc(sizeof(*r));
  r->Add = stddev_Add;
  r->Finalize = stddev_Finalize;
  r->Free = stddev_Free;
  r->FreeInstance = stddev_FreeInstance;
  r->NewInstance = stddev_NewInstance;
  r->alias = alias ? alias : "stddev";
  r->ctx = (ReducerCtx){.ctx = ctx, .privdata = strdup(property)};
  return r;
}
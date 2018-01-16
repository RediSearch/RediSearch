#include <aggregate/reducer.h>
#include <float.h>

typedef enum { Minmax_Min = 1, Minmax_Max = 2, Minmax_Avg } MinmaxMode;

struct minmaxCtx {
  double val;
  const char *property;
  RSSortingTable *sortables;
  MinmaxMode mode;
  size_t numMatches;
};

static void *newInstanceCommon(ReducerCtx *ctx, MinmaxMode mode) {
  struct minmaxCtx *m = malloc(sizeof(*m));
  m->mode = mode;
  m->property = ctx->privdata;
  m->numMatches = 0;
  m->sortables = ctx->ctx->spec->sortables;
  if (mode == Minmax_Min) {
    m->val = DBL_MAX;
  } else if (mode == Minmax_Max) {
    m->val = DBL_MIN;
  } else {
    m->val = 0;
  }
  return m;
}

static void *min_NewInstance(ReducerCtx *ctx) {
  return newInstanceCommon(ctx, Minmax_Min);
}

static void *max_NewInstance(ReducerCtx *ctx) {
  return newInstanceCommon(ctx, Minmax_Max);
}

static void *avg_NewInstance(ReducerCtx *ctx) {
  return newInstanceCommon(ctx, Minmax_Avg);
}

static int minmax_Add(void *ctx, SearchResult *res) {
  struct minmaxCtx *m = ctx;
  double val;
  RSValue *v = SearchResult_GetValue(res, m->sortables, m->property);
  if (!RSValue_ToNumber(v, &val)) {
    return 1;
  }

  if (m->mode == Minmax_Max && val > m->val) {
    m->val = val;
  } else if (m->mode == Minmax_Min && val < m->val) {
    m->val = val;
  } else if (m->mode == Minmax_Avg) {
    m->val += val;
  }

  m->numMatches++;
  return 1;
}

static int minmax_Finalize(void *base, const char *key, SearchResult *res) {
  struct minmaxCtx *ctx = base;
  if (ctx->mode == Minmax_Avg && ctx->numMatches) {
    ctx->val /= (double)ctx->numMatches;
  }
  RSFieldMap_Set(&res->fields, key, RS_NumVal(ctx->numMatches ? ctx->val : 0));
  return 1;
}

static void minmax_Free(Reducer *r) {
  free(r->ctx.privdata);
  free(r);
}

static void minmax_FreeInstance(void *p) {
  free(p);
}

static Reducer *newMinMax(RedisSearchCtx *ctx, const char *property, const char *alias,
                          MinmaxMode mode) {
  Reducer *r = malloc(sizeof(*r));
  r->Add = minmax_Add;
  r->Finalize = minmax_Finalize;
  r->Free = minmax_Free;
  r->FreeInstance = minmax_FreeInstance;
  r->ctx = (ReducerCtx){.privdata = strdup(property), .ctx = ctx};

  const char *fmtstr = NULL;
  if (mode == Minmax_Max) {
    r->NewInstance = max_NewInstance;
    fmtstr = "max(%s)";
  } else if (mode == Minmax_Min) {
    r->NewInstance = min_NewInstance;
    fmtstr = "min(%s)";
  } else if (mode == Minmax_Avg) {
    r->NewInstance = avg_NewInstance;
    fmtstr = "avg(%s)";
  }
  if (!alias) {
    asprintf((char **)&r->alias, fmtstr, property);
  } else {
    r->alias = alias;
  }

  return r;
}

Reducer *NewMin(RedisSearchCtx *ctx, const char *property, const char *alias) {
  return newMinMax(ctx, property, alias, Minmax_Min);
}

Reducer *NewMax(RedisSearchCtx *ctx, const char *property, const char *alias) {
  return newMinMax(ctx, property, alias, Minmax_Max);
}

Reducer *NewAvg(RedisSearchCtx *ctx, const char *property, const char *alias) {
  return newMinMax(ctx, property, alias, Minmax_Avg);
}
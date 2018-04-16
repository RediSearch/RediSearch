#include <aggregate/reducer.h>
#include <float.h>

typedef enum { Minmax_Min = 1, Minmax_Max = 2 } MinmaxMode;

struct minmaxCtx {
  double val;
  RSKey property;
  RSSortingTable *sortables;
  MinmaxMode mode;
  size_t numMatches;
};

static void *newInstanceCommon(ReducerCtx *ctx, MinmaxMode mode) {
  struct minmaxCtx *m = ReducerCtx_Alloc(ctx, sizeof(*ctx), 1024);
  m->mode = mode;

  
  m->property = RS_KEY(ctx->property);
  m->numMatches = 0;
  m->sortables = SEARCH_CTX_SORTABLES(ctx->ctx);
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
static int minmax_Add(void *ctx, SearchResult *res) {
  struct minmaxCtx *m = ctx;
  double val;
  RSValue *v = SearchResult_GetValue(res, m->sortables, &m->property);
  if (!RSValue_ToNumber(v, &val)) {
    return 1;
  }

  if (m->mode == Minmax_Max && val > m->val) {
    m->val = val;
  } else if (m->mode == Minmax_Min && val < m->val) {
    m->val = val;
  }

  m->numMatches++;
  return 1;
}

static int minmax_Finalize(void *base, const char *key, SearchResult *res) {
  struct minmaxCtx *ctx = base;
  RSFieldMap_SetNumber(&res->fields, key, ctx->numMatches ? ctx->val : 0);
  return 1;
}


static Reducer *newMinMax(RedisSearchCtx *ctx, const char *property, const char *alias,
                          MinmaxMode mode) {
  Reducer *r = malloc(sizeof(*r));
  r->Add = minmax_Add;
  r->Finalize = minmax_Finalize;
  r->Free = Reducer_GenericFree;
  r->FreeInstance = NULL;//minmax_FreeInstance;
  r->ctx = (ReducerCtx){.ctx = ctx, .property = property};

  const char *fstr = NULL;
  if (mode == Minmax_Max) {
    r->NewInstance = max_NewInstance;
    fstr = "max";
  } else if (mode == Minmax_Min) {
    r->NewInstance = min_NewInstance;
    fstr = "min";
  }

  r->alias = FormatAggAlias(alias, fstr, property);
  return r;
}

Reducer *NewMin(RedisSearchCtx *ctx, const char *property, const char *alias) {
  return newMinMax(ctx, property, alias, Minmax_Min);
}

Reducer *NewMax(RedisSearchCtx *ctx, const char *property, const char *alias) {
  return newMinMax(ctx, property, alias, Minmax_Max);
}

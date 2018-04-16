#include <aggregate/reducer.h>

struct firstValueCtx {
  RSKey property;
  RSKey sortBy;
  RSSortingTable *sortables;
  RSValue value;
  RSValue sortValue;
  int ascending;
  int hasValue;
};

struct firstValueParams {
  const char *property;
  const char *sortProperty;
  int ascending;
};

static void *fv_NewInstance(ReducerCtx *ctx) {
  struct firstValueParams *params = ctx->privdata;
  BlkAlloc *ba = &ctx->alloc;
  struct firstValueCtx *fv =
      ReducerCtx_Alloc(ctx, sizeof(*fv), 1024 * sizeof(*fv));  // malloc(sizeof(*ctr));
  fv->property = RS_KEY(RSKEY(params->property));
  fv->sortBy = RS_KEY(RSKEY(params->sortProperty));
  fv->sortables = SEARCH_CTX_SORTABLES(ctx->ctx);
  fv->hasValue = 0;
  fv->ascending = params->ascending;
  RSValue_MakeReference(&fv->value, RS_NullVal());
  RSValue_MakeReference(&fv->sortValue, RS_NullVal());

  return fv;
}

static int fv_Add(void *ctx, SearchResult *res) {
  struct firstValueCtx *fvx = ctx;
  RSValue *sortval = SearchResult_GetValue(res, fvx->sortables, &fvx->sortBy);
  RSValue *val = SearchResult_GetValue(res, fvx->sortables, &fvx->property);
  if (RSValue_IsNull(sortval)) {
    if (!fvx->hasValue) {
      fvx->hasValue = 1;
      RSValue_MakeReference(&fvx->value, val ? RSValue_MakePersistent(val) : RS_NullVal());
    }
    return 1;
  }

  int rc = (fvx->ascending ? -1 : 1) * RSValue_Cmp(sortval, &fvx->sortValue);
  int isnull = RSValue_IsNull(&fvx->sortValue);

  if (!fvx->hasValue || (!isnull && rc > 0) || (isnull && rc < 0)) {
    RSValue_Free(&fvx->sortValue);
    RSValue_Free(&fvx->value);
    RSValue_MakeReference(&fvx->sortValue, RSValue_MakePersistent(sortval));
    RSValue_MakeReference(&fvx->value, val ? RSValue_MakePersistent(val) : RS_NullVal());
    fvx->hasValue = 1;
  }

  return 1;
}

static int fv_Finalize(void *ctx, const char *key, SearchResult *res) {
  struct firstValueCtx *fvx = ctx;
  RSFieldMap_Set(&res->fields, key, RSValue_Dereference(&fvx->value));

  return 1;
}

static void fv_FreeInstance(void *p) {
  struct firstValueCtx *fvx = p;
  RSValue_Free(&fvx->value);
  RSValue_Free(&fvx->sortValue);
}


Reducer *NewFirstValue(RedisSearchCtx *ctx, const char *key, const char *sortKey, int asc,
                       const char *alias) {

  struct firstValueParams *params = malloc(sizeof(*params));
  params->property = key;
  params->sortProperty = sortKey;
  params->ascending = asc;

  Reducer *r = NewReducer(ctx, params);

  r->Add = fv_Add;
  r->Finalize = fv_Finalize;
  r->Free = Reducer_GenericFree;
  r->FreeInstance = fv_FreeInstance;
  r->NewInstance = fv_NewInstance;
  r->alias = FormatAggAlias(alias, "first_value", key);
  return r;
}
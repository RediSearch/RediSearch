#include <aggregate/reducer.h>
#include <util/block_alloc.h>

struct counter {
  size_t count;
};

void *counter_NewInstance(ReducerCtx *ctx) {
  BlkAlloc *ba = ctx->privdata;
  struct counter *ctr =
      ReducerCtx_Alloc(ctx, sizeof(*ctr), 1024 * sizeof(*ctr));  // malloc(sizeof(*ctr));
  ctr->count = 0;
  return ctr;
}

int counter_Add(void *ctx, SearchResult *res) {
  struct counter *ctr = ctx;
  ctr->count++;
  return 1;
}

int counter_Finalize(void *ctx, const char *key, SearchResult *res) {
  struct counter *ctr = ctx;
  // printf("Counter finalize! count %zd\n", ctr->count);
  RSFieldMap_SetNumber(&res->fields, key, ctr->count);
  return 1;
}

Reducer *NewCount(RedisSearchCtx *ctx, const char *alias) {
  Reducer *r = NewReducer(ctx, NULL);

  r->Add = counter_Add;
  r->Finalize = counter_Finalize;
  r->Free = Reducer_GenericFree;
  r->FreeInstance = NULL;
  r->NewInstance = counter_NewInstance;
  r->alias = FormatAggAlias(alias, "count", "");
  return r;
}
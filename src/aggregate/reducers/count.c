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
  RSFieldMap_Set(&res->fields, key, RS_NumVal(ctr->count));
  return 1;
}

// Free just frees up the processor. If left as NULL we simply use free()
void counter_Free(Reducer *r) {
  BlkAlloc_FreeAll(&r->ctx.alloc, NULL, NULL, 0);
  free(r);
}
void counter_FreeInstance(void *p) {
}

Reducer *NewCount(RedisSearchCtx *ctx, const char *alias) {
  Reducer *r = NewReducer(ctx, alias ? alias : "count", NULL);

  r->Add = counter_Add;
  r->Finalize = counter_Finalize;
  r->Free = counter_Free;
  r->FreeInstance = counter_FreeInstance;
  r->NewInstance = counter_NewInstance;
  return r;
}
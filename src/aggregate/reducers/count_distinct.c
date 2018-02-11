#include <aggregate/reducer.h>
#include <util/block_alloc.h>
#include <util/khash.h>
#include <util/fnv.h>

static const int khid = 35;
KHASH_SET_INIT_INT64(khid);

struct distinctCounter {
  size_t count;
  RSKey key;
  RSSortingTable *sortables;
  khash_t(khid) * dedup;
};

static void *countDistinct_NewInstance(ReducerCtx *ctx) {
  BlkAlloc *ba = &ctx->alloc;
  struct distinctCounter *ctr =
      ReducerCtx_Alloc(ctx, sizeof(*ctr), 1024 * sizeof(*ctr));  // malloc(sizeof(*ctr));
  ctr->count = 0;
  ctr->dedup = kh_init(khid);
  ctr->key = RS_KEY(RSKEY((char *)ctx->privdata));
  ctr->sortables = ctx->ctx->spec->sortables;
  return ctr;
}

static int countDistinct_Add(void *ctx, SearchResult *res) {
  struct distinctCounter *ctr = ctx;
  RSValue *val = SearchResult_GetValue(res, ctr->sortables, &ctr->key);
  if (!val || val->t == RSValue_Null) {
    return 1;
  }

  uint64_t hval = RSValue_Hash(val, 0);

  khiter_t k = kh_get(khid, ctr->dedup, hval);  // first have to get ieter
  if (k == kh_end(ctr->dedup)) {
    ctr->count++;
    int ret;
    kh_put(khid, ctr->dedup, hval, &ret);
  }
  return 1;
}

static int countDistinct_Finalize(void *ctx, const char *key, SearchResult *res) {
  struct distinctCounter *ctr = ctx;
  // printf("Counter finalize! count %zd\n", ctr->count);
  RSFieldMap_SetNumber(&res->fields, key, ctr->count);
  return 1;
}

static void countDistinct_FreeInstance(void *p) {
  struct distinctCounter *ctr = p;
  // we only destroy the hash table. The object itself is allocated from a block and needs no
  // freeing
  kh_destroy(khid, ctr->dedup);
}

static inline void countDistinct_Free(Reducer *r) {
  BlkAlloc_FreeAll(&r->ctx.alloc, NULL, 0, 0);
  free(r->alias);
  free(r);
}

Reducer *NewCountDistinct(RedisSearchCtx *ctx, const char *alias, const char *key) {
  Reducer *r = NewReducer(ctx, (void *)key);

  r->Add = countDistinct_Add;
  r->Finalize = countDistinct_Finalize;
  r->Free = countDistinct_Free;
  r->FreeInstance = countDistinct_FreeInstance;
  r->NewInstance = countDistinct_NewInstance;
  r->alias = FormatAggAlias(alias, "count", "");
  return r;
}
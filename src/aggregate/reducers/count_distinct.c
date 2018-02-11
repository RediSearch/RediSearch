#include <aggregate/reducer.h>
#include <util/block_alloc.h>
#include <util/khash.h>
#include <util/fnv.h>
#include <dep/hll/hll.h>

#define HLL_PRECISION_BITS 8

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
  r->alias = FormatAggAlias(alias, "count_distinct", key);
  return r;
}

struct distinctishCounter {
  struct HLL hll;
  RSKey key;
  RSSortingTable *sortables;
};

static void *countDistinctish_NewInstance(ReducerCtx *ctx) {
  BlkAlloc *ba = &ctx->alloc;
  struct distinctishCounter *ctr =
      ReducerCtx_Alloc(ctx, sizeof(*ctr), 1024 * sizeof(*ctr));  // malloc(sizeof(*ctr));
  hll_init(&ctr->hll, HLL_PRECISION_BITS);
  ctr->key = RS_KEY(RSKEY((char *)ctx->privdata));
  ctr->sortables = ctx->ctx->spec->sortables;
  return ctr;
}

static int countDistinctish_Add(void *ctx, SearchResult *res) {
  struct distinctishCounter *ctr = ctx;
  RSValue *val = SearchResult_GetValue(res, ctr->sortables, &ctr->key);
  if (!val || val->t == RSValue_Null) {
    return 1;
  }

  uint64_t hval = RSValue_Hash(val, 0x5f61767a);
  uint32_t val32 = (uint32_t)hval ^ (uint32_t)(hval >> 32);
  hll_add_hash(&ctr->hll, val32);
  return 1;
}

static int countDistinctish_Finalize(void *ctx, const char *key, SearchResult *res) {
  struct distinctishCounter *ctr = ctx;
  // rintf("Counter finalize! count %f\n", hll_count(&ctr->hll));
  RSFieldMap_SetNumber(&res->fields, key, (uint64_t)hll_count(&ctr->hll));
  return 1;
}

static void countDistinctish_FreeInstance(void *p) {
  struct distinctishCounter *ctr = p;
  hll_destroy(&ctr->hll);
}

static inline void countDistinctish_Free(Reducer *r) {
  BlkAlloc_FreeAll(&r->ctx.alloc, NULL, 0, 0);
  free(r->alias);
  free(r);
}

Reducer *NewCountDistinctish(RedisSearchCtx *ctx, const char *alias, const char *key) {
  Reducer *r = NewReducer(ctx, (void *)key);

  r->Add = countDistinctish_Add;
  r->Finalize = countDistinctish_Finalize;
  r->Free = countDistinctish_Free;
  r->FreeInstance = countDistinctish_FreeInstance;
  r->NewInstance = countDistinctish_NewInstance;
  r->alias = FormatAggAlias(alias, "count_distinctish", key);
  return r;
}
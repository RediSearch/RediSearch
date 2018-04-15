#include <aggregate/reducer.h>
#include <util/block_alloc.h>
#include <util/khash.h>
#include <util/fnv.h>
#include <dep/hll/hll.h>
#include <rmutil/sds.h>

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
  ctr->sortables = SEARCH_CTX_SORTABLES(ctx->ctx);
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

Reducer *NewCountDistinct(RedisSearchCtx *ctx, const char *alias, const char *key) {
  Reducer *r = NewReducer(ctx, (void *)key);

  r->Add = countDistinct_Add;
  r->Finalize = countDistinct_Finalize;
  r->Free = Reducer_GenericFreeWithStaticPrivdata;
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
  ctr->sortables = SEARCH_CTX_SORTABLES(ctx->ctx);
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

/** Serialized HLL format */
typedef struct __attribute__((packed)) {
  uint32_t flags;  // Currently unused
  uint8_t bits;
  // uint32_t size -- NOTE - always 1<<bits
} HLLSerializedHeader;

static int hllFinalize(void *ctx, const char *key, SearchResult *res) {
  struct distinctishCounter *ctr = ctx;
  // Serialize field map.
  HLLSerializedHeader hdr = {.flags = 0, .bits = ctr->hll.bits};
  char *str = malloc(sizeof(hdr) + ctr->hll.size);
  size_t hdrsize = sizeof(hdr);
  memcpy(str, &hdr, hdrsize);
  memcpy(str + hdrsize, ctr->hll.registers, ctr->hll.size);
  RSFieldMap_Add(&res->fields, key, RS_StringVal(str, sizeof(hdr) + ctr->hll.size));
  return 1;
}

static Reducer *newHllCommon(RedisSearchCtx *ctx, const char *alias, const char *key, int isRaw) {
  Reducer *r = NewReducer(ctx, (void *)key);
  r->Add = countDistinctish_Add;
  r->Free = Reducer_GenericFreeWithStaticPrivdata;
  r->FreeInstance = countDistinctish_FreeInstance;
  r->NewInstance = countDistinctish_NewInstance;

  if (isRaw) {
    r->Finalize = hllFinalize;
    r->alias = FormatAggAlias(alias, "hll", key);
  } else {
    r->Finalize = countDistinctish_Finalize;
    r->alias = FormatAggAlias(alias, "count_distinctish", key);
  }
  return r;
}

Reducer *NewCountDistinctish(RedisSearchCtx *ctx, const char *alias, const char *key) {
  return newHllCommon(ctx, alias, key, 0);
}

Reducer *NewHLL(RedisSearchCtx *ctx, const char *alias, const char *key) {
  return newHllCommon(ctx, alias, key, 1);
}

typedef struct {
  RSKey key;
  RSSortingTable *sortables;
  struct HLL hll;
} hllSumCtx;

static int hllSum_Add(void *ctx, SearchResult *res) {
  hllSumCtx *ctr = ctx;
  RSValue *val = SearchResult_GetValue(res, ctr->sortables, &ctr->key);

  if (val == NULL || !RSValue_IsString(val)) {
    // Not a string!
    return 0;
  }

  size_t len;
  const char *buf = RSValue_StringPtrLen(val, &len);
  // Verify!

  const HLLSerializedHeader *hdr = (const void *)buf;
  const char *registers = buf + sizeof(*hdr);

  // Need at least the header size
  if (len < sizeof(*hdr)) {
    return 0;
  }

  // Can't be an insane bit value - we don't want to overflow either!
  size_t regsz = len - sizeof(*hdr);
  if (hdr->bits > 64) {
    return 0;
  }

  // Expected length should be determined from bits (whose value we've also
  // verified)
  if (regsz != 1 << hdr->bits) {
    return 0;
  }

  if (ctr->hll.bits) {
    if (hdr->bits != ctr->hll.bits) {
      return 0;
    }
    // Merge!
    struct HLL tmphll = {
        .bits = hdr->bits, .size = 1 << hdr->bits, .registers = (uint8_t *)registers};
    if (hll_merge(&ctr->hll, &tmphll) != 0) {
      return 0;
    }
  } else {
    // Not yet initialized - make this our first register and continue.
    hll_init(&ctr->hll, hdr->bits);
    memcpy(ctr->hll.registers, registers, regsz);
  }
  return 1;
}

static int hllSum_Finalize(void *ctx, const char *key, SearchResult *res) {
  hllSumCtx *ctr = ctx;
  RSFieldMap_SetNumber(&res->fields, key, ctr->hll.bits ? (uint64_t)hll_count(&ctr->hll) : 0);
  return 1;
}

static void *hllSum_NewInstance(ReducerCtx *ctx) {
  hllSumCtx *ctr = ReducerCtx_Alloc(ctx, sizeof(*ctr), 1024 * sizeof(*ctr));
  ctr->hll.bits = 0;
  ctr->hll.registers = NULL;
  ctr->key = RS_KEY(RSKEY((char *)ctx->privdata));
  ctr->sortables = SEARCH_CTX_SORTABLES(ctx->ctx);
  return ctr;
}

static void hllSum_FreeInstance(void *p) {
  hllSumCtx *ctr = p;
  hll_destroy(&ctr->hll);
}

Reducer *NewHLLSum(RedisSearchCtx *ctx, const char *alias, const char *key) {
  Reducer *r = NewReducer(ctx, (void *)key);
  r->Add = hllSum_Add;
  r->Finalize = hllSum_Finalize;
  r->NewInstance = hllSum_NewInstance;
  r->FreeInstance = hllSum_FreeInstance;
  r->Free = Reducer_GenericFreeWithStaticPrivdata;
  r->alias = FormatAggAlias(alias, "hll_sum", key);
  return r;
}
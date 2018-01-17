#include <aggregate/reducer.h>
#include "util/khtable.h"
#include "util/block_alloc.h"
#include "util/minmax.h"
#include "util/fnv.h"
#include "dep/bloom/sb.h"

typedef struct {
  KHTableEntry base;
  char *key;
  size_t keyLen;
  uint32_t hash;
} DistinctItem;

typedef enum { DistinctMode_Exact, DistinctMode_Ish } DistinctMode;

typedef struct {
  DistinctMode mode;
  union {
    struct {
      KHTable table;
      BlkAlloc itemsAlloc;
      BlkAlloc keysAlloc;
    } exact;
    SBChain *approx;
  } u;
  RSSortingTable *sortables;
  RSKey property;
  size_t distinctItems;
} DistinctContext;

#define ITEMS_PER_BLOCK 128  // For entry block allocator
#define KEYS_BLOCK_SIZE 4096
#define DEFAULT_HT_SIZE 16384       // Number of initial buckets in hashtable
#define DEFAULT_FILTER_SIZE 100000  // Bloom filter capacity
#define FILTER_ERROR_RATE 0.001     // Bloom filter error rate

static int ditCompare(const KHTableEntry *ent, const void *s, size_t n, uint32_t h) {
  const DistinctItem *item = (const void *)ent;
  if (item->hash != h || n != item->keyLen) {
    return 1;
  }
  return strncmp(s, item->key, n);
}

static uint32_t ditHash(const KHTableEntry *ent) {
  return ((const DistinctItem *)ent)->hash;
}

static KHTableEntry *ditAlloc(void *ctxbase) {
  BlkAlloc *alloc = ctxbase;
  DistinctItem *itm = BlkAlloc_Alloc(alloc, sizeof(DistinctItem), ITEMS_PER_BLOCK);
  memset(itm, 0, sizeof *itm);
  return &itm->base;
}

static const KHTableProcs ditProcs_g = {.Compare = ditCompare, .Hash = ditHash, .Alloc = ditAlloc};

static void *cdt_NewInstanceCommon(ReducerCtx *ctx, DistinctMode mode) {
  DistinctContext *cdt = calloc(1, sizeof(*cdt));
  cdt->mode = mode;
  if (cdt->mode == DistinctMode_Exact) {
    BlkAlloc_Init(&cdt->u.exact.itemsAlloc);
    BlkAlloc_Init(&cdt->u.exact.keysAlloc);
    KHTable_Init(&cdt->u.exact.table, &ditProcs_g, &cdt->u.exact.itemsAlloc, DEFAULT_HT_SIZE);
  } else {
    cdt->u.approx = SB_NewChain(DEFAULT_FILTER_SIZE, FILTER_ERROR_RATE, 0);
  }

  // property to search for
  cdt->property = RS_KEY(ctx->privdata);
  cdt->sortables = ctx->ctx->spec->sortables;
  return cdt;
}

static void *cdt_NewInstanceExact(ReducerCtx *ctx) {
  return cdt_NewInstanceCommon(ctx, DistinctMode_Exact);
}
static void *cdt_NewInstanceApprox(ReducerCtx *ctx) {
  return cdt_NewInstanceCommon(ctx, DistinctMode_Ish);
}

static void cdt_FreeInstance(void *p) {
  DistinctContext *ctx = p;
  if (ctx->mode == DistinctMode_Exact) {
    BlkAlloc_FreeAll(&ctx->u.exact.itemsAlloc, NULL, NULL, 0);
    BlkAlloc_FreeAll(&ctx->u.exact.keysAlloc, NULL, NULL, 0);
    KHTable_Free(&ctx->u.exact.table);
  } else {
    SBChain_Free(ctx->u.approx);
  }
}

static int cdt_Add(void *ctx, SearchResult *res) {
  DistinctContext *cdt = ctx;
  RSValue *val = SearchResult_GetValue(res, cdt->sortables, &cdt->property);
  if (!val || val->t == RSValue_Null) {
    return 1;
  }

  size_t buflen;
  const void *buf = RSValue_ToBuffer(val, &buflen);
  int isNew = 0;
  if (cdt->mode == DistinctMode_Ish) {
    isNew = SBChain_Add(cdt->u.approx, buf, buflen);
  } else {
    uint32_t hash = fnv_32a_buf((void *)buf, buflen, 0);
    DistinctItem *itm =
        (DistinctItem *)KHTable_GetEntry(&cdt->u.exact.table, buf, buflen, hash, &isNew);
    if (isNew) {
      itm->key = BlkAlloc_Alloc(&cdt->u.exact.keysAlloc, buflen, Max(KEYS_BLOCK_SIZE, buflen));
      memcpy(itm->key, buf, buflen);
      itm->keyLen = buflen;
      itm->hash = hash;
    }

    if (isNew) {
      cdt->distinctItems++;
    }
  }
  return 1;
}

static int cdt_Finalize(void *ctx, const char *key, SearchResult *res) {
  DistinctContext *cdt = ctx;
  RSFieldMap_Set(&res->fields, key, RS_NumVal(cdt->distinctItems));
  return 1;
}

static void cdt_Free(Reducer *r) {
  free(r);
}

static Reducer *newCountDistinctCommon(RedisSearchCtx *ctx, const char *property, const char *alias,
                                       DistinctMode mode) {
  Reducer *r = malloc(sizeof(*r));
  r->Add = cdt_Add;
  r->Finalize = cdt_Finalize;
  r->Free = cdt_Free;
  r->FreeInstance = cdt_FreeInstance;
  r->NewInstance = mode == DistinctMode_Exact ? cdt_NewInstanceExact : cdt_NewInstanceApprox;
  r->ctx = (ReducerCtx){ctx, NULL};
  if (!alias) {
    asprintf((char **)&r->alias,
             mode == DistinctMode_Exact ? "count_distinct(%s)" : "count_distinctish(%s)", property);
  } else {
    r->alias = alias;
  }
  r->ctx = (ReducerCtx){.privdata = (void *)property, .ctx = ctx};
  return r;
}

Reducer *NewCountDistinct(RedisSearchCtx *ctx, const char *property, const char *alias) {
  return newCountDistinctCommon(ctx, property, alias, DistinctMode_Exact);
}

Reducer *NewCountDistinctish(RedisSearchCtx *ctx, const char *property, const char *alias) {
  return newCountDistinctCommon(ctx, property, alias, DistinctMode_Ish);
}

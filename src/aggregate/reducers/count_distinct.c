/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "aggregate/reducer.h"
#include "util/block_alloc.h"
#include "util/khash.h"
#include "util/fnv.h"
#include "hll/hll.h"

#define HLL_PRECISION_BITS 8
#define INSTANCE_BLOCK_NUM 1024

static const int khid = 35;
KHASH_SET_INIT_INT64(khid);

typedef struct {
  size_t count;
  khash_t(khid) * dedup;
} distinctCounter;

static void *distinctNewInstance(Reducer *r) {
  BlkAlloc *ba = &r->alloc;
  distinctCounter *ctr =
      BlkAlloc_Alloc(ba, sizeof(*ctr), INSTANCE_BLOCK_NUM * sizeof(*ctr));  // malloc(sizeof(*ctr));
  ctr->count = 0;
  ctr->dedup = kh_init(khid);
  return ctr;
}

static int distinctAdd(Reducer *r, void *ctx, const RLookupRow *srcrow) {
  distinctCounter *ctr = ctx;
  const RSValue *val = RLookup_GetItem(r->srckey, srcrow);
  if (!val || val == RS_NullVal()) {
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

static RSValue *distinctFinalize(Reducer *parent, void *ctx) {
  distinctCounter *ctr = ctx;
  return RS_NumVal(ctr->count);
}

static void distinctFreeInstance(Reducer *r, void *p) {
  distinctCounter *ctr = p;
  // we only destroy the hash table. The object itself is allocated from a block and needs no
  // freeing
  kh_destroy(khid, ctr->dedup);
}

Reducer *RDCRCountDistinct_New(const ReducerOptions *options) {
  Reducer *r = rm_calloc(1, sizeof(*r));
  if (!ReducerOpts_GetKey(options, &r->srckey)) {
    rm_free(r);
    return NULL;
  }
  r->Add = distinctAdd;
  r->Finalize = distinctFinalize;
  r->Free = Reducer_GenericFree;
  r->FreeInstance = distinctFreeInstance;
  r->NewInstance = distinctNewInstance;
  r->reducerId = REDUCER_T_DISTINCT;
  return r;
}

typedef struct {
  struct HLL hll;
  const RLookupKey *key;
} distinctishCounter;

static void *distinctishNewInstance(Reducer *parent) {
  BlkAlloc *ba = &parent->alloc;
  distinctishCounter *ctr =
      BlkAlloc_Alloc(ba, sizeof(*ctr), 1024 * sizeof(*ctr));  // malloc(sizeof(*ctr));
  hll_init(&ctr->hll, HLL_PRECISION_BITS);
  ctr->key = parent->srckey;
  return ctr;
}

static int distinctishAdd(Reducer *parent, void *instance, const RLookupRow *srcrow) {
  distinctishCounter *ctr = instance;
  const RSValue *val = RLookup_GetItem(ctr->key, srcrow);
  if (!val || val == RS_NullVal()) {
    return 1;
  }

  uint64_t hval = RSValue_Hash(val, 0x5f61767a);
  uint32_t val32 = (uint32_t)hval ^ (uint32_t)(hval >> 32);
  hll_add_hash(&ctr->hll, val32);
  return 1;
}

static RSValue *distinctishFinalize(Reducer *parent, void *instance) {
  distinctishCounter *ctr = instance;
  return RS_NumVal((uint64_t)hll_count(&ctr->hll));
}

static void distinctishFreeInstance(Reducer *r, void *p) {
  distinctishCounter *ctr = p;
  hll_destroy(&ctr->hll);
}

/** Serialized HLL format */
typedef struct __attribute__((packed)) {
  uint32_t flags;  // Currently unused
  uint8_t bits;
  // uint32_t size -- NOTE - always 1<<bits
} HLLSerializedHeader;

static RSValue *hllFinalize(Reducer *parent, void *ctx) {
  distinctishCounter *ctr = ctx;

  // Serialize field map.
  HLLSerializedHeader hdr = {.flags = 0, .bits = ctr->hll.bits};
  char *str = rm_malloc(sizeof(hdr) + ctr->hll.size);
  size_t hdrsize = sizeof(hdr);
  memcpy(str, &hdr, hdrsize);
  memcpy(str + hdrsize, ctr->hll.registers, ctr->hll.size);
  RSValue *ret = RS_StringVal(str, sizeof(hdr) + ctr->hll.size);
  return ret;
}

static Reducer *newHllCommon(const ReducerOptions *options, int isRaw) {
  Reducer *r = rm_calloc(1, sizeof(*r));
  if (!ReducerOpts_GetKey(options, &r->srckey)) {
    rm_free(r);
    return NULL;
  }
  r->Add = distinctishAdd;
  r->Free = Reducer_GenericFree;
  r->FreeInstance = distinctishFreeInstance;
  r->NewInstance = distinctishNewInstance;

  if (isRaw) {
    r->reducerId = REDUCER_T_HLL;
    r->Finalize = hllFinalize;
  } else {
    r->reducerId = REDUCER_T_DISTINCTISH;
    r->Finalize = distinctishFinalize;
  }
  return r;
}

Reducer *RDCRCountDistinctish_New(const ReducerOptions *options) {
  return newHllCommon(options, 0);
}

Reducer *RDCRHLL_New(const ReducerOptions *options) {
  return newHllCommon(options, 1);
}

typedef struct HLL hllSumCtx;

static int hllsumAdd(Reducer *r, void *ctx, const RLookupRow *srcrow) {
  hllSumCtx *ctr = ctx;
  const RSValue *val = RLookup_GetItem(r->srckey, srcrow);

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

  if (ctr->bits) {
    if (hdr->bits != ctr->bits) {
      return 0;
    }
    // Merge!
    struct HLL tmphll = {
        .bits = hdr->bits, .size = 1 << hdr->bits, .registers = (uint8_t *)registers};
    if (hll_merge(ctr, &tmphll) != 0) {
      return 0;
    }
  } else {
    // Not yet initialized - make this our first register and continue.
    hll_load(ctr, registers, regsz);
  }
  return 1;
}

static RSValue *hllsumFinalize(Reducer *parent, void *ctx) {
  hllSumCtx *ctr = ctx;
  return RS_NumVal(ctr->bits ? (uint64_t)hll_count(ctr) : 0);
}

static void *hllsumNewInstance(Reducer *r) {
  hllSumCtx *ctr = BlkAlloc_Alloc(&r->alloc, sizeof(*ctr), 1024 * sizeof(*ctr));
  ctr->bits = 0;
  ctr->registers = NULL;
  return ctr;
}

static void hllsumFreeInstance(Reducer *r, void *p) {
  hllSumCtx *ctr = p;
  hll_destroy(ctr);
}

Reducer *RDCRHLLSum_New(const ReducerOptions *options) {
  Reducer *r = rm_calloc(1, sizeof(*r));
  if (!ReducerOpts_GetKey(options, &r->srckey)) {
    rm_free(r);
    return NULL;
  }
  r->reducerId = REDUCER_T_HLLSUM;
  r->Add = hllsumAdd;
  r->Finalize = hllsumFinalize;
  r->NewInstance = hllsumNewInstance;
  r->FreeInstance = hllsumFreeInstance;
  r->Free = Reducer_GenericFree;
  return r;
}

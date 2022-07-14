
#include "aggregate/reducer.h"
#include "util/block_alloc.h"
#include "util/map.h"
#include "util/fnv.h"
#include "hll/hll.h"
#include "rmutil/sds.h"

///////////////////////////////////////////////////////////////////////////////////////////////

#define INSTANCE_BLOCK_NUM 1024

static const int khid = 35;
KHASH_SET_INIT_INT64(khid);

//---------------------------------------------------------------------------------------------

RDCRCountDistinct::Data *RDCRCountDistinct::NewInstance(Reducer *r) {
  Data *dd = alloc.Alloc(sizeof(*ctr), INSTANCE_BLOCK_NUM * sizeof(*ctr));
  count = 0;
  // dedup = kh_init(khid);
  srckey = r->srckey;
  return dd;
}

//---------------------------------------------------------------------------------------------

int RDCRCountDistinct::Add(Data *dd, const RLookupRow *srcrow) {
  const RSValue *val = srcrow->GetItem(ctr->srckey);
  if (!val || val->t == RSValue_Null) {
    return 1;
  }

  uint64_t hval = val->Hash(0);

  khiter_t k = kh_get(khid, ctr->dedup, hval);  // first have to get ieter
  if (k == kh_end(ctr->dedup)) {
    ctr->count++;
    int ret;
    kh_put(khid, dd->dedup, hval, &ret);
  }
  return 1;
}

//---------------------------------------------------------------------------------------------

RSValue *RDCRCountDistinct::Finalize(Data *dd) {
  return RS_NumVal(dd->count);
}

//---------------------------------------------------------------------------------------------

// void RDCRCountDistinct::FreeInstance(Data *dd) {
//   // we only destroy the hash table. The object itself is allocated from a block and needs no freeing
//   // kh_destroy(khid, dd->dedup);
// }

//---------------------------------------------------------------------------------------------

RDCRCountDistinct::RDCRCountDistinct(const ReducerOptions *options) {
  if (!options->GetKey(&srckey)) {
    throw Error("RDCRCountDistinct: no key found");

  }
  reducerId = REDUCER_T_DISTINCT;
}

///////////////////////////////////////////////////////////////////////////////////////////////

#define HLL_PRECISION_BITS 8

//---------------------------------------------------------------------------------------------

static void *distinctishNewInstance(Reducer *parent) {
  BlkAlloc *ba = &parent->alloc;
  distinctishCounter *ctr = ba->Alloc(sizeof(*ctr), 1024 * sizeof(*ctr));
  hll_init(&ctr->hll, HLL_PRECISION_BITS);
  ctr->key = parent->srckey;
  return ctr;
}

//---------------------------------------------------------------------------------------------

static int distinctishAdd(Reducer *parent, void *instance, const RLookupRow *srcrow) {
  distinctishCounter *ctr = instance;
  const RSValue *val = srcrow->GetItem(ctr->key);
  if (!val || val->t == RSValue_Null) {
    return 1;
  }

  uint64_t hval = RSValue_Hash(val, 0x5f61767a);
  uint32_t val32 = (uint32_t)hval ^ (uint32_t)(hval >> 32);
  hll_add_hash(&ctr->hll, val32);
  return 1;
}

//---------------------------------------------------------------------------------------------

static RSValue *distinctishFinalize(Reducer *parent, void *instance) {
  distinctishCounter *ctr = instance;
  return RS_NumVal((uint64_t)hll_count(&ctr->hll));
}

//---------------------------------------------------------------------------------------------

static void distinctishFreeInstance(Reducer *r, void *p) {
  distinctishCounter *ctr = p;
  hll_destroy(&ctr->hll);
}

//---------------------------------------------------------------------------------------------

// Serialized HLL format
struct __attribute__((packed)) HLLSerializedHeader {
  uint32_t flags;  // Currently unused
  uint8_t bits;
  // uint32_t size -- NOTE - always 1<<bits
};

//---------------------------------------------------------------------------------------------

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

//---------------------------------------------------------------------------------------------

static Reducer *newHllCommon(const ReducerOptions *options, int isRaw) {
  Reducer *r = rm_calloc(1, sizeof(*r));
  if (!options->GetKey(&r->srckey)) {
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

//---------------------------------------------------------------------------------------------

Reducer *RDCRCountDistinctish_New(const ReducerOptions *options) {
  return newHllCommon(options, 0);
}

//---------------------------------------------------------------------------------------------

Reducer *RDCRHLL_New(const ReducerOptions *options) {
  return newHllCommon(options, 1);
}

///////////////////////////////////////////////////////////////////////////////////////////////

int RDCRHLLSum::Add(Data *dd, const RLookupRow *srcrow) {
  const RSValue *val = srcrow->GetItem(dd->srckey);
  if (val == NULL || !val->IsString()) {
    // Not a string!
    return 0;
  }

  size_t len;
  const char *buf = val->StringPtrLen(&len);
  //@@ Verify!

  return dd->Add(buf);
}

//---------------------------------------------------------------------------------------------

int RDCRHLLSum::Data::Add(const char *buf, size_t len) {
  const HLLSerializedHeader *hdr = (const void *)buf;

  // Need at least the header size
  if (len < sizeof(*hdr)) {
    return 0;
  }

  const char *registers = buf + sizeof(*hdr);

  // Can't be an insane bit value - we don't want to overflow either!
  size_t regsz = len - sizeof(*hdr);
  if (hdr->bits > 64) {
    return 0;
  }

  // Expected length should be determined from bits (whose value we've also verified)
  if (regsz != 1 << hdr->bits) {
    return 0;
  }

  if (hll.bits) {
    if (hdr->bits != hll.bits) {
      return 0;
    }
    // Merge!
    struct HLL tmphll = {
        .bits = hdr->bits, .size = 1 << hdr->bits, .registers = (uint8_t *)registers};
    if (hll_merge(&hll, &tmphll) != 0) {
      return 0;
    }
  } else {
    // Not yet initialized - make this our first register and continue.
    hll_init(&hll, hdr->bits);
    memcpy(hll.registers, registers, regsz);
  }
  return 1;
}

//---------------------------------------------------------------------------------------------

RSValue *RDCRHLLSum::Finalize(Data *dd) {
  return RS_NumVal(dd->hll.bits ? (uint64_t)hll_count(&dd->hll) : 0);
}

//---------------------------------------------------------------------------------------------

RDCRHLLSum::Data *RDCRHLLSum::NewInstance() {
  Data *dd = alloc.Alloc(sizeof(*dd), 1024 * sizeof(*ctr));
  dd->hll.bits = 0;
  dd->hll.registers = NULL;
  dd->srckey = srckey;
  return dd;
}

//---------------------------------------------------------------------------------------------

void RDCRHLLSum::FreeInstance(Data *dd) {
  hll_destroy(&dd->hll);
}

//---------------------------------------------------------------------------------------------

RDCRHLLSum::RDCRHLLSum(const ReducerOptions *options) {
  if (!options->GetKey(&srckey)) {
    throw Error("RDCRHLLSum: no key found");
  }
  reducerId = REDUCER_T_HLLSUM;
}

///////////////////////////////////////////////////////////////////////////////////////////////

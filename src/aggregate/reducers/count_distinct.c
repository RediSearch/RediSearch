
#include "aggregate/reducer.h"
#include "util/block_alloc.h"
#include "util/map.h"
#include "util/fnv.h"
#include "hll/hll.h"
#include "rmutil/sds.h"

///////////////////////////////////////////////////////////////////////////////////////////////

#ifdef 0
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
#endif //0

//---------------------------------------------------------------------------------------------

int RDCRCountDistinct::Add(const RLookupRow *srcrow) {
  const RSValue *val = srcrow->GetItem(srckey);
  if (!val || val->t == RSValue_Null) {
    return 1;
  }

  uint64_t hval = val->Hash(0);
  auto got = data.dedup.find(hval);

  if (got == data.dedup.end()) {
    data.dedup.insert(hval);//@@ What for? we use just the size of dedup
  }
  return 1;
}

//---------------------------------------------------------------------------------------------

RSValue *RDCRCountDistinct::Finalize() {
  return RS_NumVal(data.dedup.size());
}

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

int RDCRCountDistinctish::Add(const RLookupRow *srcrow) {
  const RSValue *val = srcrow->GetItem(ctr->key);
  if (!val || val->t == RSValue_Null) {
    return 1;
  }

  uint64_t hval = RSValue_Hash(val, 0x5f61767a);
  uint32_t val32 = (uint32_t)hval ^ (uint32_t)(hval >> 32);
  hll_add_hash(&data.hll, val32);
  return 1;
}

//---------------------------------------------------------------------------------------------

RSValue *RDCRCountDistinctish::Finalize() {
  return RS_NumVal((uint64_t)hll_count(&hll));
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
#ifdef 0
Reducer *newHllCommon(const ReducerOptions *options) {
  if (!options->GetKey(&srckey)) {
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
#endif

//---------------------------------------------------------------------------------------------

RDCRCountDistinctish::RDCRCountDistinctish(const ReducerOptions *options) {
    if (!options->GetKey(&srckey)) {
    throw Error("RDCRCountDistinctish: no key found");
  }
  reducerId = REDUCER_T_DISTINCTISH;
}

//---------------------------------------------------------------------------------------------

#ifdef 0
Reducer *RDCRHLL_New(const ReducerOptions *options) {
  return newHllCommon(options, 1);
}
#endif

///////////////////////////////////////////////////////////////////////////////////////////////

int RDCRHLLSum::Add(Data *dd, const RLookupRow *srcrow) {
  const RSValue *val = srcrow->GetItem(srckey);
  if (val == NULL || !val->IsString()) {
    // Not a string!
    return 0;
  }

  size_t len;
  const char *buf = val->StringPtrLen(&len);
  //@@ Verify!

  return data.Add(buf);
}

//---------------------------------------------------------------------------------------------

int RDCRHLLSum::Data::Add(const char *buf) {
  size_t len = strlen(buf);
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

RSValue *RDCRHLLSum::Finalize() {
  return RS_NumVal(data.hll.bits ? (uint64_t)hll_count(&data.hll) : 0);
}

//---------------------------------------------------------------------------------------------

#ifdef 0
RDCRHLLSum::Data *RDCRHLLSum::NewInstance() {
  Data *dd = alloc.Alloc(sizeof(*dd), 1024 * sizeof(*ctr));
  dd->hll.bits = 0;
  dd->hll.registers = NULL;
  dd->srckey = srckey;
  return dd;
}
#endif //0

//---------------------------------------------------------------------------------------------

RDCRHLLSum::~RDCRHLLSum() {
  hll_destroy(&data.hll);
}

//---------------------------------------------------------------------------------------------

RDCRHLLSum::RDCRHLLSum(const ReducerOptions *options) {
  if (!options->GetKey(&srckey)) {
    throw Error("RDCRHLLSum: no key found");
  }
  reducerId = REDUCER_T_HLLSUM;
  data.hll.bits = 0;
  data.hll.registers = NULL;
}

///////////////////////////////////////////////////////////////////////////////////////////////

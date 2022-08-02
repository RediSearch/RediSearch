
#include "aggregate/reducer.h"
#include "util/block_alloc.h"
#include "util/map.h"
#include "util/fnv.h"
#include "hll/hll.h"
#include "rmutil/sds.h"

// Serialized HLL format
struct __attribute__((packed)) HLLSerializedHeader {
  uint32_t flags;  // Currently unused
  uint8_t bits;
  // uint32_t size -- NOTE - always 1<<bits
};

///////////////////////////////////////////////////////////////////////////////////////////////

int RDCRCountDistinct::Add(const RLookupRow *srcrow) {
  const RSValue *val = srcrow->GetItem(srckey);
  if (!val || val->t == RSValue_Null) {
    return 1;
  }

  uint64_t hval = val->Hash(0);
  data.dedup.insert(hval);
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

RDCRHLLCommon::RDCRHLLCommon(const ReducerOptions *options) {
  if (!options->GetKey(&srckey)) {
    throw Error("RDCRHLLCommon: no key found");
  }

  reducerId = REDUCER_T_HLL;
}

//---------------------------------------------------------------------------------------------

int RDCRHLLCommon::Add(const RLookupRow *srcrow) {
  const RSValue *val = srcrow->GetItem(srckey);
  if (!val || val->t == RSValue_Null) {
    return 1;
  }

  uint64_t hval = val->Hash(0x5f61767a);
  uint32_t val32 = (uint32_t)hval ^ (uint32_t)(hval >> 32);
  data.hll.add_hash(val32);
  return 1;
}

//---------------------------------------------------------------------------------------------

int RDCRHLLCommon::Data::Add(const char *buf) {
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
    HLL tmphll(hdr->bits);
    tmphll.size = 1 << hdr->bits;
    tmphll.registers = (uint8_t *)registers;

    if (!hll.merge(&tmphll)) {
      return 0;
    }
  } else {
    // Not yet initialized - make this our first register and continue.
    hll = new HLL(hdr->bits);
    memcpy(hll.registers, registers, regsz);
  }
  return 1;
}

//---------------------------------------------------------------------------------------------

RSValue *RDCRHLLCommon::Finalize() {
  // Serialize field map.
  HLLSerializedHeader hdr = {.flags = 0, .bits = data.hll.bits};
  char *str = rm_malloc(sizeof(hdr) + data.hll.size);
  size_t hdrsize = sizeof(hdr);
  memcpy(str, &hdr, hdrsize);
  memcpy(str + hdrsize, data.hll.registers, data.hll.size);
  RSValue *ret = RS_StringVal(str, sizeof(hdr) + data.hll.size);
  return ret;
}

///////////////////////////////////////////////////////////////////////////////////////////////

RSValue *RDCRCountDistinctish::Finalize() {
  return RS_NumVal((uint64_t)data.hll.count());
}

//---------------------------------------------------------------------------------------------

RDCRCountDistinctish::RDCRCountDistinctish(const ReducerOptions *options) :
  RDCRHLLCommon(options) {
    if (!options->GetKey(&srckey)) {
    throw Error("RDCRCountDistinctish: no key found");
  }
  reducerId = REDUCER_T_DISTINCTISH;
}

///////////////////////////////////////////////////////////////////////////////////////////////

int RDCRHLLSum::Add(const RLookupRow *srcrow) {
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

RSValue *RDCRHLLSum::Finalize() {
  return RS_NumVal(data.hll.bits ? (uint64_t)data.hll.count() : 0);
}

//---------------------------------------------------------------------------------------------

RDCRHLLSum::RDCRHLLSum(const ReducerOptions *options) : RDCRHLLCommon(options) {
  if (!options->GetKey(&srckey)) {
    throw Error("RDCRHLLSum: no key found");
  }
  reducerId = REDUCER_T_HLLSUM;
  data.hll.bits = 0;
  data.hll.registers = NULL;
}

///////////////////////////////////////////////////////////////////////////////////////////////

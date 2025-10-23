/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "slot_ranges.h"
#include "rmalloc.h"
#include "redismodule.h"
#include "rmutil/rm_assert.h"

#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>

extern RedisModuleCtx *RSDummyContext;

struct SharedSlotRangeArray {
  atomic_uint refcount;
  RedisModuleSlotRangeArray array;
};

// Cached local slots. Initially NULL
// Set to allocated SharedSlotRangeArray when Slots_GetLocalSlots is called (if NULL)
// Dropped when Slots_DropCachedLocalSlots is called (when we know local slots have changed)
static SharedSlotRangeArray *localSlots = NULL;

const SharedSlotRangeArray *Slots_GetLocalSlots(void) {
  if (!localSlots) {
    RedisModuleSlotRangeArray *ranges = RedisModule_ClusterGetLocalSlotRanges(RSDummyContext);
    RS_LOG_ASSERT(ranges != NULL, "Expected non-NULL ranges from ClusterGetLocalSlotRanges in any mode");

    localSlots = rm_malloc(sizeof(SharedSlotRangeArray) + sizeof(RedisModuleSlotRange) * ranges->num_ranges);
    localSlots->array.num_ranges = ranges->num_ranges;
    memcpy(localSlots->array.ranges, ranges->ranges, sizeof(RedisModuleSlotRange) * ranges->num_ranges);
    atomic_init(&localSlots->refcount, 2); // One for the caller, one for the cache
    RedisModule_ClusterFreeSlotRanges(RSDummyContext, ranges);
  } else {
    atomic_fetch_add_explicit(&localSlots->refcount, 1, memory_order_acquire);
  }
  return localSlots;
}

void Slots_FreeLocalSlots(const SharedSlotRangeArray *slots) {
  SharedSlotRangeArray *slots_ = (SharedSlotRangeArray *)slots; // Cast away constness for refcount management
  if (slots_ && atomic_fetch_sub_explicit(&slots_->refcount, 1, memory_order_release) == 1) {
    rm_free(slots_);
  }
}

// Drops the cached info - used when we know local slots have changed (or might have changed)
void Slots_DropCachedLocalSlots(void) {
  Slots_FreeLocalSlots(localSlots);
  localSlots = NULL;
}

inline bool Slots_CanAccessKeysInSlot(const SharedSlotRangeArray *slotRanges, uint16_t slot) {
  const RedisModuleSlotRange *ranges = slotRanges->array.ranges;
  for (int i = 0; i < slotRanges->array.num_ranges; i++) {
    if (ranges[i].start <= slot && slot <= ranges[i].end) {
      return true;
    }
  }
  return false;
}

/* ===== Helpers ===== */
size_t RedisModuleSlotRangeArray_SerializedSize_Binary(uint32_t n) {
  return /*num_ranges*/sizeof(int32_t) + /*space for num_ranges*/(size_t)n * (sizeof(uint16_t) + sizeof(uint16_t));
}

static int write_u32_be(uint8_t *buf, size_t len, size_t *off, uint32_t v) {
    if (*off > len || len - *off < 4) return 0;
    uint32_t be = htonl(v);
    memcpy(buf + *off, &be, 4); *off += 4; return 1;
}
static int write_u16_be(uint8_t *buf, size_t len, size_t *off, uint16_t v) {
    if (*off > len || len - *off < 2) return 0;
    uint16_t be = htons(v);
    memcpy(buf + *off, &be, 2); *off += 2; return 1;
}
static int read_u32_be(const uint8_t *buf, size_t len, size_t *off, uint32_t *out) {
    if (*off > len || len - *off < 4) return 0;
    uint32_t be; memcpy(&be, buf + *off, 4);
    *out = ntohl(be); *off += 4; return 1;
}
static int read_u16_be(const uint8_t *buf, size_t len, size_t *off, uint16_t *out) {
    if (*off > len || len - *off < 2) return 0;
    uint16_t be; memcpy(&be, buf + *off, 2);
    *out = ntohs(be); *off += 2; return 1;
}

/* ===== Binary (client-managed buffers) ===== */
bool RedisModuleSlotRangeArray_SerializeBinary(
    const RedisModuleSlotRangeArray *slot_range_array,
    uint8_t *out_buf,
    size_t buf_len) {
    if (!slot_range_array || !out_buf || slot_range_array->num_ranges < 0) return false;
    uint32_t n = (uint32_t)slot_range_array->num_ranges;
    size_t need = RedisModuleSlotRangeArray_SerializedSize_Binary(n);

    if (buf_len < need) return false; // Insufficient buffer size

    size_t off = 0;
    if (!write_u32_be(out_buf, buf_len, &off, n)) return false;
    for (uint32_t i = 0; i < n; ++i) {
        if (!write_u16_be(out_buf, buf_len, &off, slot_range_array->ranges[i].start) ||
            !write_u16_be(out_buf, buf_len, &off, slot_range_array->ranges[i].end)) {
            return false;
        }
    }
    return true;
}

bool RedisModuleSlotRangeArray_DeserializeBinary(
    const uint8_t *in_buf,
    size_t in_len,
    RedisModuleSlotRangeArray *out) {
    if (!in_buf || !out) return false;

    size_t off = 0;
    uint32_t n = 0;
    if (!read_u32_be(in_buf, in_len, &off, &n)) return false;

    size_t expect = RedisModuleSlotRangeArray_SerializedSize_Binary(n);
    if (in_len != expect) return false;

    // Set the number of ranges in the output structure
    out->num_ranges = (int32_t)n;

    for (uint32_t i = 0; i < n; ++i) {
        uint16_t s, e;
        if (!read_u16_be(in_buf, in_len, &off, &s) ||
            !read_u16_be(in_buf, in_len, &off, &e)) {
            return false;
        }
        /* Optional domain checks for cluster slots:
           if (s > 16383 || e > 16383 || s > e) { return false; } */
        out->ranges[i].start = s;
        out->ranges[i].end   = e;
    }
    return true;
}

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
#include <arpa/inet.h>

// Cross-platform byte order conversion functions
#ifdef __linux__
  #include <endian.h>
#elif defined(__APPLE__)
  #include <libkern/OSByteOrder.h>
  #define htole16(x) OSSwapHostToLittleInt16(x)
  #define le16toh(x) OSSwapLittleToHostInt16(x)
#elif defined(_WIN32)
  #include <winsock2.h>
  #define htole16(x) (x)  // Windows is little-endian
  #define le16toh(x) (x)
#else
  // Generic fallback using htons/ntohs
  #include <arpa/inet.h>
  #if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
    #define htole16(x) (x)
    #define le16toh(x) (x)
  #else
    #define htole16(x) __builtin_bswap16(x)
    #define le16toh(x) __builtin_bswap16(x)
  #endif
#endif

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
size_t RedisModuleSlotRangeArray_SerializedSize_Binary(uint32_t num_ranges) {
  return /*space for num_ranges*/(size_t)num_ranges * (sizeof(uint16_t) + sizeof(uint16_t));
}

static bool write_u16_le(uint8_t *buf, size_t len, size_t *off, uint16_t v) {
    if (*off > len || len - *off < 2) return false;
    uint16_t le = htole16(v);  // No-op on little-endian
    memcpy(buf + *off, &le, 2); *off += 2; return true;
}

static bool read_u16_le(const uint8_t *buf, size_t len, size_t *off, uint16_t *out) {
    if (*off > len || len - *off < 2) return false;
    uint16_t le; memcpy(&le, buf + *off, 2);
    *out = le16toh(le); *off += 2; return true;  // No-op on little-endian
}

/* ===== Binary (client-managed buffers) ===== */
bool RedisModuleSlotRangeArray_SerializeBinary(const RedisModuleSlotRangeArray *slot_range_array,
                                                uint8_t *out_buf,
                                                size_t buf_len) {
    RS_ASSERT(out_buf);
    RS_ASSERT(slot_range_array);
    RS_ASSERT(slot_range_array->num_ranges >= 0);
    uint32_t n = (uint32_t)slot_range_array->num_ranges;
    size_t off = 0;
    for (uint32_t i = 0; i < n; ++i) {
        if (!write_u16_le(out_buf, buf_len, &off, slot_range_array->ranges[i].start) ||
            !write_u16_le(out_buf, buf_len, &off, slot_range_array->ranges[i].end)) {
            return false;
        }
    }
    return true;
}

bool RedisModuleSlotRangeArray_DeserializeBinary(const uint8_t *in_buf,
                                                size_t in_len,
                                                RedisModuleSlotRangeArray *out) {
    // Validate input
    RS_ASSERT(in_buf);
    RS_ASSERT(out);
    if (in_len % (sizeof(uint16_t) + sizeof(uint16_t)) != 0) {
        // Some invalid data - not a multiple of the expected size
        return false;
    }

    size_t off = 0;
    // Set the number of ranges in the output structure
    out->num_ranges = in_len / (sizeof(uint16_t) + sizeof(uint16_t));

    for (uint32_t i = 0; i < out->num_ranges; ++i) {
        uint16_t s, e;
        if (!read_u16_le(in_buf, in_len, &off, &s) ||
            !read_u16_le(in_buf, in_len, &off, &e)) {
            return false;
        }
        out->ranges[i].start = s;
        out->ranges[i].end   = e;
    }
    return true;
}

  // Helper function to check if a slot is covered by any range in the array
static inline bool SlotInRanges(const RedisModuleSlotRangeArray *ranges, uint16_t slot) {
  for (int i = 0; i < ranges->num_ranges; i++) {
    if (ranges->ranges[i].start <= slot && slot <= ranges->ranges[i].end) {
      return true;
    }
  }
  return false;
}

SlotRangesComparisonResult CompareSlotRanges(const RedisModuleSlotRangeArray *ranges_expected,
                                             const RedisModuleSlotRangeArray *ranges_actual) {
  RS_ASSERT(ranges_expected)
  RS_ASSERT(ranges_actual)
  RS_ASSERT(ranges_expected->num_ranges > 0)
  RS_ASSERT(ranges_actual->num_ranges > 0)

  const uint16_t num_expected = ranges_expected->num_ranges;
  const uint16_t num_actual = ranges_actual->num_ranges;
  // Fast path: identical arrays => MATCH
  if (num_expected == num_actual &&
      memcmp(ranges_expected->ranges, ranges_actual->ranges,
             (size_t)ranges_expected->num_ranges * sizeof(RedisModuleSlotRange)) == 0) {
      return SLOT_RANGES_MATCH;
  }

  // Coverage check: every ranges_expected[i] must be fully covered by union of ranges_actual
  uint16_t i = 0; // index in ranges_expected
  uint16_t j = 0; // index in ranges_actual

  while (i < num_expected) {
      const uint16_t expected_start = ranges_expected->ranges[i].start;
      const uint16_t expected_end   = ranges_expected->ranges[i].end;

      // Move ranges_actual forward until it could cover expected_start
      while (j < num_actual && ranges_actual->ranges[j].end < expected_start) j++;

      if (j == num_actual || ranges_actual->ranges[j].start > expected_start) {
          // No ranges_actual range starts at/before expected_start
          return SLOT_RANGES_DOES_NOT_INCLUDE;
      }

      // Accumulate coverage from ranges_actual until we reach expected_end
      uint16_t covered_end = ranges_actual->ranges[j].end;
      while (covered_end < expected_end) {
          j++;
          if (j == num_actual || ranges_actual->ranges[j].start > (uint16_t)(covered_end + 1)) {
              // Gap before we can extend coverage up to expected_end
              return SLOT_RANGES_DOES_NOT_INCLUDE;
          }
          if (ranges_actual->ranges[j].end > covered_end) covered_end = ranges_actual->ranges[j].end;
      }

      // ranges_expected[i] fully covered; proceed to next ranges_expected (j stays where last coverage ended)
      i++;
  }

  // All A covered; since we already ruled out exact equality, it's a proper subset
  return SLOT_RANGES_SUBSET;
}

bool RedisModuleSlotRangeArray_FromClusterShardsReply(RedisModuleCallReply *slots, RedisModuleSlotRangeArray **slotRanges) {
  if (!slots || !slotRanges) {
    return false;
  }

  if (RedisModule_CallReplyType(slots) != REDISMODULE_REPLY_ARRAY) {
    return false;
  }

  size_t numSlots = RedisModule_CallReplyLength(slots);
  if (numSlots % 2 != 0) {
    return false;
  }

  size_t numRanges = numSlots / 2;
  size_t total_size = sizeof(RedisModuleSlotRangeArray) + sizeof(RedisModuleSlotRange) * numRanges;
  RedisModuleSlotRangeArray *ranges = (RedisModuleSlotRangeArray*)rm_malloc(total_size);
  if (!ranges) {
    return false;
  }

  ranges->num_ranges = numRanges;
  for (size_t i = 0; i < numRanges; i++) {
    RedisModuleCallReply *start = RedisModule_CallReplyArrayElement(slots, i * 2);
    RedisModuleCallReply *end = RedisModule_CallReplyArrayElement(slots, i * 2 + 1);

    if (!start || !end ||
        RedisModule_CallReplyType(start) != REDISMODULE_REPLY_INTEGER ||
        RedisModule_CallReplyType(end) != REDISMODULE_REPLY_INTEGER) {
      rm_free(ranges);
      return false;
    }

    ranges->ranges[i].start = (uint16_t)RedisModule_CallReplyInteger(start);
    ranges->ranges[i].end = (uint16_t)RedisModule_CallReplyInteger(end);
  }

  *slotRanges = ranges;
  return true;
}

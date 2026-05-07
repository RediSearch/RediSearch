/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

#include "ttl_table.h"
#include "rmalloc.h"
#include "util/minmax.h"
#include "util/misc.h"
#include "rmutil/rm_assert.h"

// Direct-modulo bucket array with contiguous-vec collision chains.
//
// Rationale:
//  - docIds are allocated monotonically via ++maxDocId in DocTable, so the
//    iterator-time lookups of `VerifyDocAndField*` see them in ascending,
//    mostly-sequential order. Hashing would scatter that locality; direct
//    modulo keeps sequential docIds in sequential slots so the CPU hardware
//    prefetcher can pull upcoming bucket headers into L1 ahead of demand.
//    This primarily benefits the "miss" hot path — docs without TTL, the
//    common case at query time — where each `ttl_find_entry` probes a
//    bucket pointer (8 B), sees NULL, and returns. Eight such probes fit in
//    one 64-byte cache line, and the stride-1 access pattern matches the
//    hardware prefetcher's sequential detector so it streams the next lines
//    in without demand-miss stalls as long as the iterator walks docIds in
//    ascending order.
//  - When maxDocId exceeds `maxSize`, multiple docIds collide on the same
//    slot. A per-bucket contiguous-vec chain keeps walks cache-line
//    sequential and bounds worst-case behavior (Poisson tail at load factor
//    1 gives chain length ≤ ~5) — unlike linear probing, which degrades to
//    O(cap) once deletes create probe-gaps in a near-full table.
//  - Each non-empty bucket is an arr.h fat pointer: len/cap live in the
//    `array_hdr_t` immediately preceding the elements, so the slot itself
//    is just an 8 B pointer. Writes go through arr.h's geometric growth
//    rather than a +1 realloc per insert.
//  - Lazy growth mirrors DocTable_Set: `maxSize` (the modulus used by the
//    slot formula) is captured once at init and never changes, while `cap`
//    (the number of buckets actually allocated) starts at 0 and grows on
//    demand via rm_realloc up to `maxSize`. Because the slot formula depends
//    only on `maxSize`, growing `cap` never relocates an existing entry —
//    it only extends the tail of the bucket array. Reads for docIds whose
//    slot lies in the still-unallocated tail treat the entry as absent,
//    which is correct because Add always grows `cap` to cover the slot
//    before writing. This keeps the per-index footprint proportional to the
//    number of TTL docs ever inserted rather than to maxDocTableSize, which
//    matters in deployments with many indexes that sparsely use TTL.

typedef struct {
  t_docId docId;                              // 0 is reserved / unused
  arrayof(FieldExpiration) fieldExpirations;  // owned, sorted by field index, never empty
} TimeToLiveEntry;

// A bucket is a NULL-or-arr.h-managed chain of entries. NULL means empty.
typedef arrayof(TimeToLiveEntry) TTLBucket;

struct TimeToLiveTable {
  TTLBucket *buckets;                   // allocated for the first `cap` slots
  size_t cap;                           // number of bucket slots physically allocated
  size_t maxSize;                       // modulus for slot calculation, fixed at init
  size_t count;                         // total live entries
};

static inline size_t ttl_slot(const TimeToLiveTable *t, t_docId docId) {
  return docId < t->maxSize ? (size_t)docId : (size_t)(docId % t->maxSize);
}

// Initial bucket-array size the first time we grow from zero. Chosen so an
// index that only ever holds a handful of TTL docs pays one small allocation
// and no reallocs. Must be ≥ 1.
#define TTL_BUCKET_INITIAL_CAP ((size_t)64)

// Upper bound on the geometric +1.5x step once the bucket array is non-empty,
// mirroring DocTable_Set's cap so huge tables don't take a single multi-MB
// realloc hit and so the two allocators scale in lockstep.
#define TTL_BUCKET_MAX_GROW_STEP ((size_t)(1 << 20))  // 1 Mi buckets

// Ensure buckets[slot] is allocated. Called from Add only; reads treat
// slot >= cap as "not present" and never grow. Growth curve matches
// DocTable_Set: +1.5x geometric up to TTL_BUCKET_MAX_GROW_STEP, clamped to
// maxSize, with TTL_BUCKET_INITIAL_CAP as the first-grow seed.
static void ttl_grow(TimeToLiveTable *t, size_t slot) {
  RS_ASSERT(slot < t->maxSize);    // ttl_slot's contract; tightens the call boundary
  if (slot < t->cap) return;
  RS_ASSERT(t->cap < t->maxSize);  // slot is always < maxSize, so room must exist
  const size_t oldcap = t->cap;
  size_t newcap = t->cap == 0
      ? TTL_BUCKET_INITIAL_CAP
      : t->cap + 1 + MIN(t->cap / 2, TTL_BUCKET_MAX_GROW_STEP);
  if (newcap > t->maxSize) newcap = t->maxSize;
  if (newcap < slot + 1) newcap = slot + 1;
  t->buckets = rm_realloc(t->buckets, newcap * sizeof(*t->buckets));
  memset(t->buckets + oldcap, 0, (newcap - oldcap) * sizeof(*t->buckets));
  t->cap = newcap;
}

static inline TimeToLiveEntry *ttl_find_entry(const TimeToLiveTable *t, t_docId docId) {
  const size_t slot = ttl_slot(t, docId);
  if (slot >= t->cap) return NULL;  // bucket unallocated => entry cannot exist
  TTLBucket bucket = t->buckets[slot];
  if (!bucket) return NULL;
  const uint32_t n = array_len(bucket);
  for (uint32_t i = 0; i < n; i++) {
    if (bucket[i].docId == docId) return &bucket[i];
  }
  return NULL;
}

// Debug-only duplicate probe for the monotonic-docId invariant. Extracted to
// keep the Add call site readable (`assert(!ttl_bucket_contains(...))`); the
// invariant itself is enforced upstream by the spec write lock in
// DocTable_Put, so eliding the scan in release is intentional.
static bool ttl_bucket_contains(TTLBucket bucket, t_docId docId) {
  const uint32_t n = array_len(bucket);
  for (uint32_t i = 0; i < n; i++) {
    if (bucket[i].docId == docId) return true;
  }
  return false;
}

// Release the owned allocations of a single entry. The entry slot itself is
// owned by the bucket's `entries` block and is freed with it.
static inline void ttl_entry_release(TimeToLiveEntry *e) {
  array_free(e->fieldExpirations);
}

void TimeToLiveTable_VerifyInit(TimeToLiveTable **table, size_t maxSize) {
  if (*table) return;
  // maxSize == 0 would make ttl_slot divide by zero; the caller (DocTable)
  // floors its own maxSize to 1 on load, so treat this as a hard invariant.
  RS_LOG_ASSERT_ALWAYS(maxSize >= 1, "TTL table maxSize must be >= 1");
  TimeToLiveTable *t = rm_malloc(sizeof(*t));
  t->buckets = NULL;
  t->cap = 0;
  t->maxSize = maxSize;
  t->count = 0;
  *table = t;
}

void TimeToLiveTable_Destroy(TimeToLiveTable **table) {
  if (!*table) return;
  TimeToLiveTable *t = *table;
  for (size_t s = 0; s < t->cap; s++) {
    TTLBucket bucket = t->buckets[s];
    if (!bucket) continue;
    const uint32_t n = array_len(bucket);
    for (uint32_t i = 0; i < n; i++) {
      ttl_entry_release(&bucket[i]);
    }
    array_free(bucket);
  }
  rm_free(t->buckets);
  rm_free(t);
  *table = NULL;
}

void TimeToLiveTable_Add(TimeToLiveTable *t, t_docId docId, arrayof(FieldExpiration) sortedById) {
  RS_ASSERT(sortedById && array_len(sortedById) > 0);
  const size_t slot = ttl_slot(t, docId);
  ttl_grow(t, slot);
  // docIds are monotonically assigned in DocTable_Put under the spec write
  // lock, so duplicates should not reach here; the assert catches a broken
  // locking discipline during development before it corrupts the table.
  RS_LOG_ASSERT(!ttl_bucket_contains(t->buckets[slot], docId), "duplicate docId in TTL table");
  TimeToLiveEntry entry = { .docId = docId, .fieldExpirations = sortedById };
  t->buckets[slot] = array_ensure_append_1(t->buckets[slot], entry);
  t->count++;
}

void TimeToLiveTable_Remove(TimeToLiveTable *t, t_docId docId) {
  const size_t slot = ttl_slot(t, docId);
  if (slot >= t->cap) return;  // bucket unallocated => nothing to remove
  TTLBucket bucket = t->buckets[slot];
  if (!bucket) return;
  const uint32_t n = array_len(bucket);
  for (uint32_t i = 0; i < n; i++) {
    if (bucket[i].docId != docId) continue;
    ttl_entry_release(&bucket[i]);
    array_del_fast(bucket, i);  // swap-last + dec len; in-place, no realloc
    t->count--;
    if (array_len(bucket) == 0) {
      array_free(bucket);
      t->buckets[slot] = NULL;
    }
    // No-shrink-on-delete: arr.h keeps the allocation at its high-water
    // mark via remain_cap, avoiding realloc churn for buckets that churn.
    return;
  }
}

bool TimeToLiveTable_IsEmpty(TimeToLiveTable *t) {
  return t->count == 0;
}

size_t TimeToLiveTable_DebugAllocatedBuckets(const TimeToLiveTable *t) {
  return t ? t->cap : 0;
}

const arrayof(FieldExpiration) TimeToLiveTable_GetFieldExpirations(const TimeToLiveTable *t, t_docId docId) {
  const TimeToLiveEntry *e = ttl_find_entry(t, docId);
  return e ? e->fieldExpirations : NULL;
}

static inline bool DidExpire(const t_expirationTimePoint* field, const t_expirationTimePoint* now) {
  if (!field->tv_sec && !field->tv_nsec) {
    return false;
  }

  return !((field->tv_sec > now->tv_sec) || (field->tv_sec == now->tv_sec && field->tv_nsec > now->tv_nsec));
}

bool TimeToLiveTable_VerifyDocAndField(TimeToLiveTable *t, t_docId docId, t_fieldIndex field, enum FieldExpirationPredicate predicate, const struct timespec* expirationPoint) {
  const TimeToLiveEntry *e = ttl_find_entry(t, docId);
  if (!e) {
    // the document did not have a ttl for itself or its fields
    // if predicate is default then we know at least one field is valid
    // if predicate is missing then we know the field is indeed missing since the document has no expiration for it
    return true;
  }

  const size_t fieldWithExpirationCount = array_len(e->fieldExpirations);
  if (fieldWithExpirationCount == 0) {
    // the document has no fields with expiration times, there exists at least one valid field
    return true;
  }

  for (size_t currentFieldIndex = 0; currentFieldIndex < fieldWithExpirationCount; currentFieldIndex++) {
    FieldExpiration* fieldExpiration = &e->fieldExpirations[currentFieldIndex];
    if (field == fieldExpiration->index) {
      // the field has an expiration time
      const bool expired = DidExpire(&fieldExpiration->point, expirationPoint);
      if (expired) {
        // the document is invalid (should return `false`), unless we look for missing fields
        return (predicate == FIELD_EXPIRATION_PREDICATE_MISSING);
      } else {
        // the document is valid (should return `true`), unless we look for missing fields
        return (predicate != FIELD_EXPIRATION_PREDICATE_MISSING);
      }
    }
  }
  // the field was not found in the document's field expirations,
  // which means it is valid unless the predicate is FIELD_EXPIRATION_PREDICATE_MISSING
  return (predicate != FIELD_EXPIRATION_PREDICATE_MISSING);
}

bool TimeToLiveTable_VerifyDocAndFieldMask(TimeToLiveTable *t, t_docId docId, uint32_t fieldMask, enum FieldExpirationPredicate predicate, const struct timespec* expirationPoint, const t_fieldIndex* ftIdToFieldIndex) {
  const TimeToLiveEntry *e = ttl_find_entry(t, docId);
  if (!e) {
    // the document did not have a ttl for itself or its fields
    // if predicate is default then we know at least one field is valid
    // if predicate is missing then we know the field is indeed missing since the document has no expiration for it
    return true;
  }

  const size_t fieldWithExpirationCount = array_len(e->fieldExpirations);
  const size_t fieldCount = __builtin_popcount(fieldMask);
  if (fieldWithExpirationCount == 0) {
    // the document has no fields with expiration times, there exists at least one valid field
    return true;
  } else if (fieldWithExpirationCount < fieldCount && predicate == FIELD_EXPIRATION_PREDICATE_DEFAULT) {
    // the document has less fields with expiration times than the fields we are checking
    // at least one field is valid
    return true;
  }

  size_t predicateMisses = 0;
  size_t currentFieldIndex = 0;
  int bitIndex;
  while ((bitIndex = ffs(fieldMask))) {
    bitIndex--;  // ffs returns 1-based index, we need 0-based
    fieldMask &= ~(1U << bitIndex);  // Clear the bit we just processed
    t_fieldIndex fieldIndexToCheck = ftIdToFieldIndex[bitIndex];

    // Attempt to find the next field expiration that matches the current field index
    while (currentFieldIndex < fieldWithExpirationCount && fieldIndexToCheck > e->fieldExpirations[currentFieldIndex].index) {
      currentFieldIndex++;
    }
    if (currentFieldIndex >= fieldWithExpirationCount) {
      // No more fields with expiration times to check
      break;
    } else if (fieldIndexToCheck < e->fieldExpirations[currentFieldIndex].index) {
      // The field we are checking is not present in the current field expiration
      continue;
    }

    RS_ASSERT(fieldIndexToCheck == e->fieldExpirations[currentFieldIndex].index);
    // Match found - we need to check if it has an expiration time
    const bool expired = DidExpire(&e->fieldExpirations[currentFieldIndex].point, expirationPoint);
    if (!expired && predicate == FIELD_EXPIRATION_PREDICATE_DEFAULT) {
      return true;
    } else if (expired && predicate == FIELD_EXPIRATION_PREDICATE_MISSING) {
      return true;
    }
    predicateMisses++; // Count the predicate misses for the current match
  }
  if (predicate == FIELD_EXPIRATION_PREDICATE_DEFAULT) {
    // If we are checking for the default predicate, we need at least one valid field
    return predicateMisses < fieldCount;
  } else { // if (predicate == FIELD_EXPIRATION_PREDICATE_MISSING)
    // If we are checking for the missing predicate, we need at least one expired field
    // If we reached here, it means we did not find any expired fields
    return false;
  }
}


// TODO: Rust - unify with the implementation above using generic field mask
bool TimeToLiveTable_VerifyDocAndWideFieldMask(TimeToLiveTable *t, t_docId docId, t_fieldMask fieldMask, enum FieldExpirationPredicate predicate, const struct timespec* expirationPoint, const t_fieldIndex* ftIdToFieldIndex) {
  const TimeToLiveEntry *e = ttl_find_entry(t, docId);
  if (!e) {
    // the document did not have a ttl for itself or its fields
    // if predicate is default then we know at least one field is valid
    // if predicate is missing then we know the field is indeed missing since the document has no expiration for it
    return true;
  }

  uint64_t fieldMask64[2];
  if (sizeof(fieldMask) == sizeof(uint64_t)) {
    fieldMask64[0] = fieldMask;
    fieldMask64[1] = 0;
  } else {
    fieldMask64[0] = (uint64_t)fieldMask;
    fieldMask64[1] = fieldMask >> 64;
  }

  const size_t fieldWithExpirationCount = array_len(e->fieldExpirations);
  const size_t fieldCount = __builtin_popcountll(fieldMask64[0]) + __builtin_popcountll(fieldMask64[1]);
  if (fieldWithExpirationCount == 0) {
    // the document has no fields with expiration times, there exists at least one valid field
    return true;
  } else if (fieldWithExpirationCount < fieldCount && predicate == FIELD_EXPIRATION_PREDICATE_DEFAULT) {
    // the document has less fields with expiration times than the fields we are checking
    // at least one field is valid
    return true;
  }

  size_t predicateMisses = 0;
  size_t currentFieldIndex = 0;
  int bitIndex;
  for (int i = 0; i < 2; i++) {
    while ((bitIndex = ffsll(fieldMask64[i]))) {
      bitIndex--;  // ffsll returns 1-based index, we need 0-based
      fieldMask64[i] &= ~(1ULL << bitIndex);  // Clear the bit we just processed
      t_fieldIndex fieldIndexToCheck = ftIdToFieldIndex[bitIndex + (i * 64)];

      // Attempt to find the next field expiration that matches the current field index
      while (currentFieldIndex < fieldWithExpirationCount && fieldIndexToCheck > e->fieldExpirations[currentFieldIndex].index) {
        currentFieldIndex++;
      }
      if (currentFieldIndex >= fieldWithExpirationCount) {
        // No more fields with expiration times to check
        goto end; // Break out of all loops
      } else if (fieldIndexToCheck < e->fieldExpirations[currentFieldIndex].index) {
        // The field we are checking is not present in the current field expiration
        continue;
      }

      RS_ASSERT(fieldIndexToCheck == e->fieldExpirations[currentFieldIndex].index);
      // Match found - we need to check if it has an expiration time
      const bool expired = DidExpire(&e->fieldExpirations[currentFieldIndex].point, expirationPoint);
      if (!expired && predicate == FIELD_EXPIRATION_PREDICATE_DEFAULT) {
        return true;
      } else if (expired && predicate == FIELD_EXPIRATION_PREDICATE_MISSING) {
        return true;
      }
      predicateMisses++; // Count the predicate misses for the current match
    }
  }
end:
  if (predicate == FIELD_EXPIRATION_PREDICATE_DEFAULT) {
    // If we are checking for the default predicate, we need at least one valid field
    return predicateMisses < fieldCount;
  } else { // if (predicate == FIELD_EXPIRATION_PREDICATE_MISSING)
    // If we are checking for the missing predicate, we need at least one expired field
    // If we reached here, it means we did not find any expired fields
    return false;
  }
}

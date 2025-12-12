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

typedef struct {
  t_expirationTimePoint documentExpirationPoint;
  FieldExpiration* fieldExpirations;
} TimeToLiveEntry;

static uint64_t hashFunction_DocId(const void *key) {
  return (t_docId)key;
}
static void destructor_TimeToLiveEntry(void *privdata, void *val) {
    (void)privdata;
    TimeToLiveEntry* entry = (TimeToLiveEntry*)val;
    array_free(entry->fieldExpirations);
    rm_free(entry);
}

static dictType dictTimeToLive = {
  .hashFunction = hashFunction_DocId,
  .keyDup = NULL,
  .valDup = NULL,
  .keyCompare = NULL,
  .keyDestructor = NULL,
  .valDestructor = destructor_TimeToLiveEntry,
};

void TimeToLiveTable_VerifyInit(TimeToLiveTable **table) {
    if (!*table) {
      *table = dictCreate(&dictTimeToLive, NULL);
    }
}

void TimeToLiveTable_Destroy(TimeToLiveTable **table) {
    if (*table) {
      dictRelease(*table);
      *table = NULL;
    }
}

void TimeToLiveTable_Add(TimeToLiveTable *table, t_docId docId, t_expirationTimePoint docExpirationTime, arrayof(FieldExpiration) sortedById) {
  TimeToLiveEntry* entry = (TimeToLiveEntry*)rm_malloc(sizeof(TimeToLiveEntry));
  entry->documentExpirationPoint = docExpirationTime;
  entry->fieldExpirations = sortedById;
  // we don't want the operation to fail so we use dictReplace
  const bool added __attribute__((unused)) = dictAdd(table, (void*)docId, entry) == DICT_OK;
  RS_LOG_ASSERT(added, "Failed to add document to ttl table");
}

void TimeToLiveTable_Remove(TimeToLiveTable *table, t_docId docId) {
  dictDelete(table, (void*)docId);
}

bool TimeToLiveTable_IsEmpty(TimeToLiveTable *table) {
  return dictSize(table) == 0;
}

static inline bool DidExpire(const t_expirationTimePoint* field, const t_expirationTimePoint* now) {
  if (!field->tv_sec && !field->tv_nsec) {
    return false;
  }

  return !((field->tv_sec > now->tv_sec) || (field->tv_sec == now->tv_sec && field->tv_nsec > now->tv_nsec));
}

bool TimeToLiveTable_HasDocExpired(TimeToLiveTable *table, t_docId docId, const struct timespec* expirationPoint) {
  dictEntry *entry = dictFind(table, (void*)docId);
  if (!entry) {
    return false;
  }

  TimeToLiveEntry* ttlEntry = (TimeToLiveEntry*)dictGetVal(entry);
  return DidExpire(&ttlEntry->documentExpirationPoint, expirationPoint);
}

bool TimeToLiveTable_VerifyDocAndField(TimeToLiveTable *table, t_docId docId, t_fieldIndex field, enum FieldExpirationPredicate predicate, const struct timespec* expirationPoint) {
  dictEntry *entry = dictFind(table, (void*)docId);
  if (!entry) {
    // the document did not have a ttl for itself or its fields
    // if predicate is default then we know at least one field is valid
    // if predicate is missing then we know the field is indeed missing since the document has no expiration for it
    return true;
  }

  TimeToLiveEntry* ttlEntry = (TimeToLiveEntry*)dictGetVal(entry);
  const size_t fieldWithExpirationCount = array_len(ttlEntry->fieldExpirations);
  if (fieldWithExpirationCount == 0) {
    // the document has no fields with expiration times, there exists at least one valid field
    return true;
  }

  for (size_t currentFieldIndex = 0; currentFieldIndex < fieldWithExpirationCount; currentFieldIndex++) {
    FieldExpiration* fieldExpiration = &ttlEntry->fieldExpirations[currentFieldIndex];
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
  // which means it is valid unless the predicate is FIELD_EXPIRATION_MISSING
  return (predicate != FIELD_EXPIRATION_PREDICATE_MISSING);
}

bool TimeToLiveTable_VerifyDocAndFieldMask(TimeToLiveTable *table, t_docId docId, uint32_t fieldMask, enum FieldExpirationPredicate predicate, const struct timespec* expirationPoint, const t_fieldIndex* ftIdToFieldIndex) {
  dictEntry *entry = dictFind(table, (void*)docId);
  if (!entry) {
    // the document did not have a ttl for itself or its fields
    // if predicate is default then we know at least one field is valid
    // if predicate is missing then we know the field is indeed missing since the document has no expiration for it
    return true;
  }

  TimeToLiveEntry* ttlEntry = (TimeToLiveEntry*)dictGetVal(entry);
  const size_t fieldWithExpirationCount = array_len(ttlEntry->fieldExpirations);
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
    while (currentFieldIndex < fieldWithExpirationCount && fieldIndexToCheck > ttlEntry->fieldExpirations[currentFieldIndex].index) {
      currentFieldIndex++;
    }
    if (currentFieldIndex >= fieldWithExpirationCount) {
      // No more fields with expiration times to check
      break;
    } else if (fieldIndexToCheck < ttlEntry->fieldExpirations[currentFieldIndex].index) {
      // The field we are checking is not present in the current field expiration
      continue;
    }

    RS_ASSERT(fieldIndexToCheck == ttlEntry->fieldExpirations[currentFieldIndex].index);
    // Match found - we need to check if it has an expiration time
    const bool expired = DidExpire(&ttlEntry->fieldExpirations[currentFieldIndex].point, expirationPoint);
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
  } else { // if (predicate == FIELD_EXPIRATION_MISSING)
    // If we are checking for the missing predicate, we need at least one expired field
    // If we reached here, it means we did not find any expired fields
    return false;
  }
}


// TODO: Rust - unify with the implementation above using generic field mask
bool TimeToLiveTable_VerifyDocAndWideFieldMask(TimeToLiveTable *table, t_docId docId, t_fieldMask fieldMask, enum FieldExpirationPredicate predicate, const struct timespec* expirationPoint, const t_fieldIndex* ftIdToFieldIndex) {
  dictEntry *entry = dictFind(table, (void*)docId);
  if (!entry) {
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

  TimeToLiveEntry* ttlEntry = (TimeToLiveEntry*)dictGetVal(entry);
  const size_t fieldWithExpirationCount = array_len(ttlEntry->fieldExpirations);
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
      while (currentFieldIndex < fieldWithExpirationCount && fieldIndexToCheck > ttlEntry->fieldExpirations[currentFieldIndex].index) {
        currentFieldIndex++;
      }
      if (currentFieldIndex >= fieldWithExpirationCount) {
        // No more fields with expiration times to check
        goto end; // Break out of all loops
      } else if (fieldIndexToCheck < ttlEntry->fieldExpirations[currentFieldIndex].index) {
        // The field we are checking is not present in the current field expiration
        continue;
      }

      RS_ASSERT(fieldIndexToCheck == ttlEntry->fieldExpirations[currentFieldIndex].index);
      // Match found - we need to check if it has an expiration time
      const bool expired = DidExpire(&ttlEntry->fieldExpirations[currentFieldIndex].point, expirationPoint);
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
  } else { // if (predicate == FIELD_EXPIRATION_MISSING)
    // If we are checking for the missing predicate, we need at least one expired field
    // If we reached here, it means we did not find any expired fields
    return false;
  }
}

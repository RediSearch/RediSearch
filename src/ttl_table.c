#include "ttl_table.h"
#include "rmalloc.h"
#include "util/minmax.h"
#include "util/misc.h"

typedef struct {
  t_docId id;
  t_expirationTimePoint point;
} DocumentExpiration;

typedef struct {
  DocumentExpiration documentExpiration;
  FieldExpiration* fieldExpirations;
} TimeToLiveEntry;

static uint64_t hashFunction_DocId(const void *key) {
  return (uint64_t)key;
}
static void *dup_DocId(void *p, const void *key) {
  return (void*)key;
}
static int compare_DocId(void *privdata, const void *key1, const void *key2) {
  return (t_docId)key1 < (t_docId)key2;
}
static void destructor_DocId(void *privdata, void *key) {
}

static void destructor_TimeToLiveEntry(void *privdata, void *key) {
    TimeToLiveEntry* entry = (TimeToLiveEntry*)key;
    rm_free(entry->fieldExpirations);
    rm_free(entry);
}

static dictType dictTimeToLive = {
  .hashFunction = hashFunction_DocId,
  .keyDup = dup_DocId,
  .valDup = NULL,
  .keyCompare = compare_DocId,
  .keyDestructor = destructor_DocId,
  .valDestructor = destructor_TimeToLiveEntry,
};

void TimeToLiveTable_Init(TimeToLiveTable *table) {
    table->hashTable = dictCreate(&dictTimeToLive, NULL);
}

void TimeToLiveTable_Destroy(TimeToLiveTable *table) {
    dictRelease(table->hashTable);
}

void TimeToLiveTable_Add(TimeToLiveTable *table, t_docId docId, t_expirationTimePoint docExpirationTime, arrayof(FieldExpiration) sortedById) {
  TimeToLiveEntry* entry = (TimeToLiveEntry*)rm_malloc(sizeof(TimeToLiveEntry));
  entry->documentExpiration.id = docId;
  entry->documentExpiration.point = docExpirationTime;
  entry->fieldExpirations = sortedById;
  // we don't want the operation to fail so we use dictReplace
  dictReplace(table->hashTable, (void*)docId, entry);
}

void TimeToLiveTable_Remove(TimeToLiveTable *table, t_docId docId) {
  dictDelete(table->hashTable, (void*)docId);
}

bool TimeToLiveTable_Empty(TimeToLiveTable *table) {
  return dictSize(table->hashTable) == 0;
}

void TimeToLiveTable_UpdateDocExpirationTime(TimeToLiveTable *table, t_docId docId, t_expirationTimePoint docExpirationTime, arrayof(FieldExpiration) allFieldSorted)
{
  arrayof(FieldExpiration) validFieldSorted = NULL;
  size_t valid = 0;
  for (t_fieldIndex i = 0; i < array_len(allFieldSorted); i++) {
    FieldExpiration* fieldExpiration = &allFieldSorted[i];
    if (fieldExpiration->point.tv_sec || fieldExpiration->point.tv_nsec) {
      validFieldSorted = array_ensure_append(validFieldSorted, fieldExpiration, 1, FieldExpiration);
    }
  }
  TimeToLiveTable_Add(table, docId, docExpirationTime, validFieldSorted);
}

struct timespec* TimeToLiveTable_GetTimeForCurrentThread(const TimeToLiveTable *table) {
  static __thread struct timespec now = {0};
  return &now;
}

void TimeToLiveTable_SetTimeForCurrentThread(TimeToLiveTable *table, const struct timespec* now) {
  struct timespec* cached = TimeToLiveTable_GetTimeForCurrentThread(table);
  cached->tv_sec = now->tv_sec;
  cached->tv_nsec = now->tv_nsec;
}

static inline bool DidExpire(const t_expirationTimePoint* field, const t_expirationTimePoint* now) {
  if (!field->tv_sec && !field->tv_nsec) {
    return false;
  }

  if (field->tv_sec > now->tv_sec) {
    return false;
  } else if (field->tv_sec == now->tv_sec && field->tv_nsec > now->tv_nsec) {
    return false;
  } else {
    return true;
  }
}

bool TimeToLiveTable_HasDocExpired(const TimeToLiveTable *table, t_docId docId) {
  dictEntry *entry = dictFind(table->hashTable, (void*)docId);
  if (!entry) {
    // the document did not have a ttl for it or its children
    return false;
  }

  TimeToLiveEntry* ttlEntry = (TimeToLiveEntry*)dictGetVal(entry);
  return DidExpire(&ttlEntry->documentExpiration.point, TimeToLiveTable_GetTimeForCurrentThread(table));
}

bool TimeToLiveTable_HasDocOrFieldIndexExpired(const TimeToLiveTable *table, t_docId docId, t_fieldIndex fieldIndex) {
  dictEntry *entry = dictFind(table->hashTable, (void*)docId);
  if (!entry) {
    // the document did not have a ttl for it or its children
    return false;
  }

  TimeToLiveEntry* ttlEntry = (TimeToLiveEntry*)dictGetVal(entry);
  struct timespec* now = TimeToLiveTable_GetTimeForCurrentThread(table);
  if (DidExpire(&ttlEntry->documentExpiration.point, now)) {
    // the document itself has expired
    return true;
  }

  if (ttlEntry->fieldExpirations == NULL || array_len(ttlEntry->fieldExpirations) == 0) {
    // the document has no fields with expiration times
    return false;
  }

  for (t_fieldIndex runningIndex = 0; runningIndex < array_len(ttlEntry->fieldExpirations); ++runningIndex) {
    FieldExpiration* fieldExpiration = &ttlEntry->fieldExpirations[fieldIndex];
    if (fieldExpiration->index == fieldIndex) {
      return DidExpire(&fieldExpiration->point, now);
    } else if (fieldExpiration->index > fieldIndex) {
      break; // the array is sorted, if we passed fieldIndex then we aren't getting to it
    }
  }
  // if we reached here, then all the fields had expiration times
  // if the policy was all then implicitly all of them were expired due to not returning false
  // if the policy was any then implicitly all of them didn't yet expir due to not returning true
  return false;
}

bool TimeToLiveTable_HasDocOrFieldIndicesExpired(const TimeToLiveTable *table, t_docId docId, t_fieldIndex* sortedFieldIndices, enum FieldExpirationPolicy policy) {
  dictEntry *entry = dictFind(table->hashTable, (void*)docId);
  if (!entry) {
    // the document did not have a ttl for it or its children
    return false;
  }

  TimeToLiveEntry* ttlEntry = (TimeToLiveEntry*)dictGetVal(entry);
  struct timespec* now = TimeToLiveTable_GetTimeForCurrentThread(table);
  if (DidExpire(&ttlEntry->documentExpiration.point, now)) {
    // the document itself has expired
    return true;
  }
  if (ttlEntry->fieldExpirations == NULL || array_len(ttlEntry->fieldExpirations) == 0) {
    // the document has no fields with expiration times
    return false;
  }

  size_t currentRecord = 0;
  for (size_t runningIndex = 0; runningIndex < array_len(sortedFieldIndices); ) {
    t_fieldIndex fieldIndexToCheck = sortedFieldIndices[runningIndex];
    FieldExpiration* fieldExpiration = &ttlEntry->fieldExpirations[currentRecord];
    if (fieldIndexToCheck > fieldExpiration->index) {
      ++currentRecord;
    } else if (fieldIndexToCheck < fieldExpiration->index) {
      ++runningIndex;
    } else {
      // the field has an expiration time
      if (!DidExpire(&fieldExpiration->point, now) && policy == FIELD_EXPIRATION_POLICY_ALL) {
        return false;
      } else if (DidExpire(&fieldExpiration->point, now) && policy == FIELD_EXPIRATION_POLICY_ANY) {
        return true;
      }
      ++currentRecord;
      ++runningIndex;
    }
  }
  // if we reached here, then all the fields had expiration times
  // if the policy was all then implicitly all of them were expired due to not returning false
  // if the policy was any then implicitly all of them didn't yet expir due to not returning true
  return policy == FIELD_EXPIRATION_POLICY_ALL;
}
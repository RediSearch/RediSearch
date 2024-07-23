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
    array_free(entry->fieldExpirations);
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
    if (!table->hashTable) {
      table->hashTable = dictCreate(&dictTimeToLive, NULL);
    }
}

void TimeToLiveTable_Destroy(TimeToLiveTable *table) {
    if (table->hashTable) {
      dictRelease(table->hashTable);
      table->hashTable = NULL;
    }
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

bool TimeToLiveTable_HasDocExpired(const TimeToLiveTable *table, t_docId docId, const struct timespec* expirationPoint) {
  dictEntry *entry = dictFind(table->hashTable, (void*)docId);
  if (!entry) {
    return false;
  }

  TimeToLiveEntry* ttlEntry = (TimeToLiveEntry*)dictGetVal(entry);
  return DidExpire(&ttlEntry->documentExpiration.point, expirationPoint);
}

static inline bool verifyFieldIndices(const TimeToLiveTable *table, t_docId docId, t_fieldIndex* sortedFieldIndices, size_t fieldCount, enum FieldExpirationPredicate predicate, const struct timespec* expirationPoint) {
  dictEntry *entry = dictFind(table->hashTable, (void*)docId);
  if (!entry) {
    // the document did not have a ttl for itself or its children
    // if predicate is default then we know at least one field is valid
    // if predicate is missing then we know the field is indeed missing since the document has no expiration for it
    return true;
  }

  TimeToLiveEntry* ttlEntry = (TimeToLiveEntry*)dictGetVal(entry);
  if (ttlEntry->fieldExpirations == NULL || array_len(ttlEntry->fieldExpirations) == 0) {
    // the document has no fields with expiration times, there exists at least one valid field
    return true;
  }

  size_t currentRecord = 0;
  const size_t recordCount = array_len(ttlEntry->fieldExpirations);
  for (size_t runningIndex = 0; runningIndex < fieldCount && currentRecord < recordCount; ) {
    t_fieldIndex fieldIndexToCheck = sortedFieldIndices[runningIndex];
    FieldExpiration* fieldExpiration = &ttlEntry->fieldExpirations[currentRecord];
    if (fieldIndexToCheck > fieldExpiration->index) {
      ++currentRecord;
    } else if (fieldIndexToCheck < fieldExpiration->index) {
      ++runningIndex;
    } else {
      // the field has an expiration time
      const bool expired = DidExpire(&fieldExpiration->point, expirationPoint);
      if (!expired && predicate == FIELD_EXPIRATION_DEFAULT) {
        return true;
      } else if (expired && predicate == FIELD_EXPIRATION_MISSING) {
        return true;
      }
      ++currentRecord;
      ++runningIndex;
    }
  }
  return false;
}

bool TimeToLiveTable_VerifyDocAndFieldIndexPredicate(const TimeToLiveTable *table, t_docId docId, t_fieldIndex fieldIndex, enum FieldExpirationPredicate predicate, const struct timespec* expirationPoint) {
  return verifyFieldIndices(table, docId, &fieldIndex, 1, predicate, expirationPoint);
}

bool TimeToLiveTable_VerifyFieldIndicesPredicate(const TimeToLiveTable *table, t_docId docId, t_fieldIndex* sortedFieldIndices, enum FieldExpirationPredicate predicate, const struct timespec* expirationPoint) {
  return verifyFieldIndices(table, docId, sortedFieldIndices, array_len(sortedFieldIndices), predicate, expirationPoint);
}

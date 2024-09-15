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
  const bool added = dictAdd(table, (void*)docId, entry) == DICT_OK;
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

bool TimeToLiveTable_HasExpiration(TimeToLiveTable *table, t_docId docId) {
  return dictFind(table, (void*)docId) != NULL;
}

bool TimeToLiveTable_HasDocExpired(TimeToLiveTable *table, t_docId docId, const struct timespec* expirationPoint) {
  dictEntry *entry = dictFind(table, (void*)docId);
  if (!entry) {
    return false;
  }

  TimeToLiveEntry* ttlEntry = (TimeToLiveEntry*)dictGetVal(entry);
  return DidExpire(&ttlEntry->documentExpirationPoint, expirationPoint);
}

bool TimeToLiveTable_VerifyDocAndFields(TimeToLiveTable *table, t_docId docId, const t_fieldIndex* sortedFieldIndices, size_t fieldCount, enum FieldExpirationPredicate predicate, const struct timespec* expirationPoint) {
  dictEntry *entry = dictFind(table, (void*)docId);
  if (!entry) {
    // the document did not have a ttl for itself or its fields
    // if predicate is default then we know at least one field is valid
    // if predicate is missing then we know the field is indeed missing since the document has no expiration for it
    return true;
  }

  TimeToLiveEntry* ttlEntry = (TimeToLiveEntry*)dictGetVal(entry);
  if (ttlEntry->fieldExpirations == NULL) {
    // the document has no fields with expiration times, there exists at least one valid field
    return true;
  }

  const size_t fieldWithExpirationCount = array_len(ttlEntry->fieldExpirations);
  if (fieldWithExpirationCount < fieldCount && predicate == FIELD_EXPIRATION_DEFAULT) {
    // the document has less fields with expiration times than the fields we are checking
    // at least one field is valid
    return true;
  }

  size_t currentFieldIndex = 0;
  for (size_t runningFieldIndex = 0; runningFieldIndex < fieldCount && currentFieldIndex < fieldWithExpirationCount; ) {
    t_fieldIndex fieldIndexToCheck = sortedFieldIndices[runningFieldIndex];
    FieldExpiration* fieldExpiration = &ttlEntry->fieldExpirations[currentFieldIndex];
    if (fieldIndexToCheck > fieldExpiration->index) {
      ++currentFieldIndex;
    } else if (fieldIndexToCheck < fieldExpiration->index) {
      ++runningFieldIndex;
    } else {
      // the field has an expiration time
      const bool expired = DidExpire(&fieldExpiration->point, expirationPoint);
      if (!expired && predicate == FIELD_EXPIRATION_DEFAULT) {
        return true;
      } else if (expired && predicate == FIELD_EXPIRATION_MISSING) {
        return true;
      }
      ++currentFieldIndex;
      ++runningFieldIndex;
    }
  }
  return false;
}

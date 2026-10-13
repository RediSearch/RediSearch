/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include <inttypes.h>
#include <limits.h>
#include <string.h>

#include "spec.h"
#include "synonym_map.h"
#include "util/hash/hash.h"
#include "rmalloc.h"
#include "rmutil/rm_assert.h"
#include "rdb.h"
#include "util/likely.h"
#include "util/arr/arr.h"
#include "util/strconv.h"

#define INITIAL_CAPACITY 2
#define SYNONYM_PREFIX "~%s"

static uint64_t synonymKeyHash(const void* key) {
  const CharBuf* term = key;
  // The dict hash API accepts an int; equality still compares the entire key.
  return dictGenHashFunction(term->buf, term->len > INT_MAX ? INT_MAX : term->len);
}

static int synonymKeyCompare(void* privdata, const void* key1, const void* key2) {
  const CharBuf* term1 = key1;
  const CharBuf* term2 = key2;
  return term1->len == term2->len && memcmp(term1->buf, term2->buf, term1->len) == 0;
}

static void* synonymKeyDup(void* privdata, const void* key) {
  CharBuf* copy = rm_malloc(sizeof(*copy));
  // TermData owns the bytes. Only the descriptor belongs to the dictionary.
  *copy = *(const CharBuf*)key;
  return copy;
}

static void synonymKeyFree(void* privdata, void* key) {
  rm_free(key);
}

static dictType synonymDictType = {
    .hashFunction = synonymKeyHash,
    .keyDup = synonymKeyDup,
    .keyCompare = synonymKeyCompare,
    .keyDestructor = synonymKeyFree,
};

static void synonymMapAddTerm(SynonymMap* smap, TermData* term) {
  CharBuf key = {.buf = term->term, .len = sdslen(term->term)};
  dictAdd(smap->h_table, &key, term);
}

static TermData* TermData_New(sds term) {
  TermData* t_data = rm_malloc(sizeof(TermData));
  t_data->term = term;
  t_data->groupIds = array_new(char*, INITIAL_CAPACITY);
  return t_data;
}

static void TermData_Free(TermData* t_data) {
  sdsfree(t_data->term);
  for (size_t i = 0; i < array_len(t_data->groupIds); ++i) {
    sdsfree(t_data->groupIds[i]);
  }
  array_free(t_data->groupIds);
  rm_free(t_data);
}

static bool TermData_IdExists(TermData* t_data, const char* id) {
  for (uint32_t i = 0; i < array_len(t_data->groupIds); ++i) {
    if (strcmp(t_data->groupIds[i] + 1, id) == 0) { /* skip the `~` when comparing */
      return true;
    }
  }
  return false;
}

static SynonymMapResult TermData_AddId(TermData* t_data, const char* id) {
  if (!TermData_IdExists(t_data, id)) {
    if (unlikely(array_len(t_data->groupIds) >= MAX_SYNONYM_GROUP_IDS)) {
      return SYNONYM_MAP_ERR_MAX_GROUP_IDS;
    }
    sds newId = sdscatprintf(sdsempty(), SYNONYM_PREFIX, id);
    array_append(t_data->groupIds, newId);
  }
  return SYNONYM_MAP_OK;
}

static TermData* TermData_Copy(TermData* t_data) {
  TermData* copy = TermData_New(sdsdup(t_data->term));
  for (int i = 0; i < array_len(t_data->groupIds); ++i) {
    SynonymMapResult ret = TermData_AddId(copy, t_data->groupIds[i] + 1 /*we do not need the ~*/);
    // If source is valid, copy should never fail (same number of group IDs)
    RS_LOG_ASSERT(ret == SYNONYM_MAP_OK, "TermData_Copy: unexpected failure in TermData_AddId");
  }
  return copy;
}

// todo: fix
static void TermData_RdbSave(RedisModuleIO* rdb, TermData* t_data) {
  RedisModule_SaveStringBuffer(rdb, t_data->term, sdslen(t_data->term) + 1);
  RedisModule_SaveUnsigned(rdb, array_len(t_data->groupIds));
  for (int i = 0; i < array_len(t_data->groupIds); ++i) {
    RedisModule_SaveStringBuffer(rdb, t_data->groupIds[i] + 1 /* do not save the ~ */,
                                 sdslen(t_data->groupIds[i]));
  }
}

// todo: fix
TermData* TermData_RdbLoad(RedisModuleIO* rdb, int encver) {
  TermData *t_data = NULL;
  char* term = NULL;
  uint64_t ids_len = 0;

  term = LoadStringBuffer_IOError(rdb, NULL, goto cleanup);
  t_data = TermData_New(sdsnew(term));
  RedisModule_Free(term);
  ids_len = LoadUnsigned_IOError(rdb, goto cleanup);

  if (unlikely(ids_len > MAX_SYNONYM_GROUP_IDS)) {
    RedisModule_LogIOError(
        rdb, "warning",
        "RDB Load: Synonym group IDs (%" PRIu64 ") exceeds maximum allowed (%d)",
        ids_len, MAX_SYNONYM_GROUP_IDS);
    goto cleanup;
  }

  for (int i = 0; i < ids_len; ++i) {
    char* groupId = NULL;
    if (encver <= INDEX_MIN_WITH_SYNONYMS_INT_GROUP_ID) {
      uint64_t id = LoadUnsigned_IOError(rdb, goto cleanup);
      rm_asprintf(&groupId, "%" PRIu64, id);
    } else {
      groupId = LoadStringBuffer_IOError(rdb, NULL, goto cleanup);
    }
    SynonymMapResult ret = TermData_AddId(t_data, groupId);
    // Should not fail since we already checked ids_len <= MAX_SYNONYM_GROUP_IDS
    RS_LOG_ASSERT(ret == SYNONYM_MAP_OK, "TermData_RdbLoad: unexpected failure in TermData_AddId");
    rm_free(groupId);
  }
  return t_data;

cleanup:
  if (t_data)
    TermData_Free(t_data);
  return NULL;
}

SynonymMap* SynonymMap_New(bool is_read_only) {
  SynonymMap* smap = rm_new(SynonymMap);
  smap->h_table = dictCreate(&synonymDictType, NULL);
  smap->is_read_only = is_read_only;
  smap->read_only_copy = NULL;
  smap->ref_count = 1;
  return smap;
}

void SynonymMap_Free(SynonymMap* smap) {
  if (smap->is_read_only) {
    --smap->ref_count;
    if (smap->ref_count > 0) {
      return;
    }
  }
  TermData* t_data;
  dictIterator* iter = dictGetIterator(smap->h_table);
  dictEntry* entry = NULL;
  while ((entry = dictNext(iter))) {
    TermData* t_data = dictGetVal(entry);
    TermData_Free(t_data);
  }
  dictReleaseIterator(iter);
  dictRelease(smap->h_table);
  if (smap->read_only_copy) {
    SynonymMap_Free(smap->read_only_copy);
  }
  rm_free(smap);
}

SynonymMapResult SynonymMap_UpdateRedisStr(SynonymMap* smap, RedisModuleString** synonyms,
                                           size_t size, const char* groupId) {
  CharBuf* terms = rm_malloc(sizeof(*terms) * size);
  for (size_t i = 0; i < size; ++i) {
    terms[i].buf = (char*)RedisModule_StringPtrLen(synonyms[i], &terms[i].len);
  }
  SynonymMapResult ret = SynonymMap_Update(smap, terms, size, groupId);
  rm_free(terms);
  return ret;
}

SynonymMapResult SynonymMap_Update(SynonymMap* smap, const CharBuf* synonyms, size_t size, const char* groupId) {
  RS_LOG_ASSERT(!smap->is_read_only, "SynonymMap should not be read only");

  if (size == 0) {
    return SYNONYM_MAP_OK;
  }

  // Lowercase every input synonym up front. We will validate against the
  // limits before mutating the dictionary, so a failure leaves the map
  // unchanged.
  sds* lowered = rm_malloc(sizeof(*lowered) * size);
  for (size_t i = 0; i < size; i++) {
    size_t len = synonyms[i].len;
    // SYNUPDATE has historically treated each term as a C string.
    const char* nul = memchr(synonyms[i].buf, '\0', len);
    if (nul) {
      len = nul - synonyms[i].buf;
    }
    sds term = sdsnewlen(synonyms[i].buf, len);
    char* dst = unicode_tolower(term, &len);
    if (dst) {
      sdsfree(term);
      term = sdsnewlen(dst, len);
      rm_free(dst);
    } else {
      sdssetlen(term, len);
      term[len] = '\0';
    }
    lowered[i] = term;
  }

  SynonymMapResult ret = SYNONYM_MAP_OK;

  // Validation pass. Track distinct new terms via a temporary dict so we
  // don't over-count duplicates within the input batch.
  dict* pending_new = dictCreate(&synonymDictType, NULL);
  for (size_t i = 0; i < size; i++) {
    TermData* termData = SynonymMap_GetIdsBySynonym(smap, lowered[i], sdslen(lowered[i]));
    if (termData) {
      // Existing term gains one group id iff it doesn't already have it.
      if (!TermData_IdExists(termData, groupId) &&
          array_len(termData->groupIds) >= MAX_SYNONYM_GROUP_IDS) {
        ret = SYNONYM_MAP_ERR_MAX_GROUP_IDS;
        break;
      }
    } else {
      CharBuf key = {.buf = lowered[i], .len = sdslen(lowered[i])};
      if (dictFind(pending_new, &key) == NULL) {
        dictAdd(pending_new, &key, NULL);
      }
    }
  }
  if (ret == SYNONYM_MAP_OK &&
      unlikely(dictSize(smap->h_table) + dictSize(pending_new) > MAX_SYNONYM_TERMS)) {
    ret = SYNONYM_MAP_ERR_MAX_TERMS;
  }
  dictRelease(pending_new);

  if (ret != SYNONYM_MAP_OK) {
    for (size_t i = 0; i < size; i++) {
      sdsfree(lowered[i]);
    }
    rm_free(lowered);
    return ret;
  }

  // Apply pass. Validation guarantees TermData_AddId cannot fail here.
  for (size_t i = 0; i < size; i++) {
    TermData* termData = SynonymMap_GetIdsBySynonym(smap, lowered[i], sdslen(lowered[i]));
    if (termData) {
      sdsfree(lowered[i]);
    } else {
      termData = TermData_New(lowered[i]);
      synonymMapAddTerm(smap, termData);
    }
    SynonymMapResult r = TermData_AddId(termData, groupId);
    RS_LOG_ASSERT(r == SYNONYM_MAP_OK,
                  "SynonymMap_Update: TermData_AddId failed after validation");
  }
  rm_free(lowered);

  if (smap->read_only_copy) {
    SynonymMap_Free(smap->read_only_copy);
    smap->read_only_copy = NULL;
  }
  return SYNONYM_MAP_OK;
}

TermData* SynonymMap_GetIdsBySynonym(SynonymMap* smap, const char* synonym, size_t len) {
  const CharBuf key = {.buf = (char*)synonym, .len = len};
  return dictFetchValue(smap->h_table, &key);
}

TermData** SynonymMap_DumpAllTerms(SynonymMap* smap, size_t* size) {
  *size = dictSize(smap->h_table);
  TermData** dump = rm_malloc(sizeof(TermData*) * (*size));
  int j = 0;
  dictIterator* iter = dictGetIterator(smap->h_table);
  dictEntry* entry = NULL;
  while ((entry = dictNext(iter))) {
    TermData* val = dictGetVal(entry);
    dump[j++] = val;
  }
  dictReleaseIterator(iter);
  return dump;
}

static void SynonymMap_CopyEntry(SynonymMap* smap, TermData* t_data) {
  synonymMapAddTerm(smap, TermData_Copy(t_data));
}

static SynonymMap* SynonymMap_GenerateReadOnlyCopy(SynonymMap* smap) {
  int ret;
  SynonymMap* read_only_smap = SynonymMap_New(true);
  dictIterator* iter = dictGetIterator(smap->h_table);
  dictEntry* entry = NULL;
  while ((entry = dictNext(iter))) {
    TermData* val = dictGetVal(entry);
    SynonymMap_CopyEntry(read_only_smap, val);
  }
  dictReleaseIterator(iter);
  return read_only_smap;
}

SynonymMap* SynonymMap_GetReadOnlyCopy(SynonymMap* smap) {
  RS_LOG_ASSERT(!smap->is_read_only, "SynonymMap should not be read only");
  if (!smap->read_only_copy) {
    // create a new read only copy and return it
    smap->read_only_copy = SynonymMap_GenerateReadOnlyCopy(smap);
  }

  ++smap->read_only_copy->ref_count;
  return smap->read_only_copy;
}

void SynonymMap_RdbSave(RedisModuleIO* rdb, void* value) {
  SynonymMap* smap = value;
  uint64_t key;
  TermData* t_data;
  RedisModule_SaveUnsigned(rdb, dictSize(smap->h_table));
  dictIterator* iter = dictGetIterator(smap->h_table);
  dictEntry* entry = NULL;
  while ((entry = dictNext(iter))) {
    TermData* val = dictGetVal(entry);
    TermData_RdbSave(rdb, val);
  }
  dictReleaseIterator(iter);
}

typedef struct {
  const TermData* term;
  uint32_t group;
} SynonymFingerprintEntry;

static void fingerprintSynonymEntry(Sha1Context* hash, const void* value) {
  const SynonymFingerprintEntry* entry = value;
  Sha1_UpdateBuffer(hash, entry->term->term, sdslen(entry->term->term));
  const sds group = entry->term->groupIds[entry->group];
  Sha1_UpdateBuffer(hash, group + 1, sdslen(group) - 1);
}

uint64_t SynonymMap_Fingerprint(const SynonymMap* smap) {
  uint64_t acc = 0;
  dictIterator* iter = dictGetIterator(smap->h_table);
  const dictEntry* entry;
  while ((entry = dictNext(iter))) {
    const TermData* term = dictGetVal(entry);
    for (uint32_t i = 0; i < array_len(term->groupIds); ++i) {
      const SynonymFingerprintEntry pair = {.term = term, .group = i};
      acc ^= Sha1_ComputeValue(fingerprintSynonymEntry, &pair);
    }
  }
  dictReleaseIterator(iter);
  return acc;
}

void* SynonymMap_RdbLoad(RedisModuleIO* rdb, int encver) {
  int ret = 0;
  size_t unused = 0;
  uint64_t smap_kh_size = 0;

  SynonymMap* smap = SynonymMap_New(false);
  if (encver <= INDEX_MIN_WITH_SYNONYMS_INT_GROUP_ID) {
    unused = LoadUnsigned_IOError(rdb, goto cleanup);
  }
  smap_kh_size = LoadUnsigned_IOError(rdb, goto cleanup);

  if (unlikely(smap_kh_size > MAX_SYNONYM_TERMS)) {
    RedisModule_LogIOError(
        rdb, "warning",
        "RDB Load: Synonym map size (%llu) exceeds maximum allowed (%d)",
        (unsigned long long)smap_kh_size, MAX_SYNONYM_TERMS);
    goto cleanup;
  }

  for (int i = 0; i < smap_kh_size; ++i) {
    if (encver <= INDEX_MIN_WITH_SYNONYMS_INT_GROUP_ID) {
      uint64_t unudes = LoadUnsigned_IOError(rdb, goto cleanup);
    }
    TermData* t_data = TermData_RdbLoad(rdb, encver);
    if (t_data == NULL) goto cleanup;
    synonymMapAddTerm(smap, t_data);
  }
  return smap;

cleanup:
  SynonymMap_Free(smap);
  return NULL;
}

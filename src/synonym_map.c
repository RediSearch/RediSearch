/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include <inttypes.h>
#include <string.h>

#include "spec.h"
#include "synonym_map.h"
#include "rmalloc.h"
#include "rmutil/rm_assert.h"
#include "rdb.h"
#include "util/likely.h"
#include "util/arr/arr.h"
#include "util/strconv.h"

#define INITIAL_CAPACITY 2
#define SYNONYM_PREFIX "~%s"

static TermData* TermData_New(char* term) {
  TermData* t_data = rm_malloc(sizeof(TermData));
  t_data->term = term;
  t_data->groupIds = array_new(char*, INITIAL_CAPACITY);
  return t_data;
}

static void TermData_Free(TermData* t_data) {
  rm_free(t_data->term);
  for (size_t i = 0; i < array_len(t_data->groupIds); ++i) {
    rm_free(t_data->groupIds[i]);
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
    char* newId;
    rm_asprintf(&newId, SYNONYM_PREFIX, id);
    array_append(t_data->groupIds, newId);
  }
  return SYNONYM_MAP_OK;
}

static TermData* TermData_Copy(TermData* t_data) {
  TermData* copy = TermData_New(rm_strdup(t_data->term));
  for (int i = 0; i < array_len(t_data->groupIds); ++i) {
    SynonymMapResult ret = TermData_AddId(copy, t_data->groupIds[i] + 1 /*we do not need the ~*/);
    // If source is valid, copy should never fail (same number of group IDs)
    RS_LOG_ASSERT(ret == SYNONYM_MAP_OK, "TermData_Copy: unexpected failure in TermData_AddId");
  }
  return copy;
}

// todo: fix
static void TermData_RdbSave(RedisModuleIO* rdb, TermData* t_data) {
  RedisModule_SaveStringBuffer(rdb, t_data->term, strlen(t_data->term) + 1);
  RedisModule_SaveUnsigned(rdb, array_len(t_data->groupIds));
  for (int i = 0; i < array_len(t_data->groupIds); ++i) {
    RedisModule_SaveStringBuffer(rdb, t_data->groupIds[i] + 1 /* do not save the ~ */,
                                 strlen(t_data->groupIds[i]));
  }
}

// todo: fix
TermData* TermData_RdbLoad(RedisModuleIO* rdb, int encver) {
  TermData *t_data = NULL;
  char* term = NULL;
  uint64_t ids_len = 0;

  term = LoadStringBuffer_IOError(rdb, NULL, goto cleanup);
  t_data = TermData_New(rm_strdup(term));
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
  smap->h_table = dictCreate(&dictTypeHeapStrings, NULL);
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

static const char** SynonymMap_RedisStringArrToArr(RedisModuleString** synonyms, size_t size) {
  const char** arr = rm_malloc(sizeof(char*) * size);
  for (size_t i = 0; i < size; ++i) {
    arr[i] = RedisModule_StringPtrLen(synonyms[i], NULL);
  }
  return arr;
}

SynonymMapResult SynonymMap_UpdateRedisStr(SynonymMap* smap, RedisModuleString** synonyms, size_t size,
                               const char* groupId) {
  const char** arr = SynonymMap_RedisStringArrToArr(synonyms, size);
  SynonymMapResult ret = SynonymMap_Update(smap, arr, size, groupId);
  rm_free(arr);
  return ret;
}

SynonymMapResult SynonymMap_Add(SynonymMap* smap, const char* groupId, const char** synonyms, size_t size) {
  return SynonymMap_Update(smap, synonyms, size, groupId);
}

SynonymMapResult SynonymMap_Update(SynonymMap* smap, const char** synonyms, size_t size, const char* groupId) {
  RS_LOG_ASSERT(!smap->is_read_only, "SynonymMap should not be read only");

  if (size == 0) {
    return SYNONYM_MAP_OK;
  }

  // Lowercase every input synonym up front. We will validate against the
  // limits before mutating the dictionary, so a failure leaves the map
  // unchanged.
  char** lowered = rm_malloc(sizeof(char*) * size);
  for (size_t i = 0; i < size; i++) {
    char* s = rm_strdup(synonyms[i]);
    size_t len = strlen(s);
    char* dst = unicode_tolower(s, &len);
    if (dst) {
      rm_free(s);
      s = dst;
    } else {
      s[len] = '\0';
    }
    lowered[i] = s;
  }

  SynonymMapResult ret = SYNONYM_MAP_OK;

  // Validation pass. Track distinct new terms via a temporary dict so we
  // don't over-count duplicates within the input batch.
  dict* pending_new = dictCreate(&dictTypeHeapStrings, NULL);
  for (size_t i = 0; i < size; i++) {
    TermData* termData = dictFetchValue(smap->h_table, lowered[i]);
    if (termData) {
      // Existing term gains one group id iff it doesn't already have it.
      if (!TermData_IdExists(termData, groupId) &&
          array_len(termData->groupIds) >= MAX_SYNONYM_GROUP_IDS) {
        ret = SYNONYM_MAP_ERR_MAX_GROUP_IDS;
        break;
      }
    } else if (dictFind(pending_new, lowered[i]) == NULL) {
      dictAdd(pending_new, lowered[i], NULL);
    }
  }
  if (ret == SYNONYM_MAP_OK &&
      unlikely(dictSize(smap->h_table) + dictSize(pending_new) > MAX_SYNONYM_TERMS)) {
    ret = SYNONYM_MAP_ERR_MAX_TERMS;
  }
  dictRelease(pending_new);

  if (ret != SYNONYM_MAP_OK) {
    for (size_t i = 0; i < size; i++) {
      rm_free(lowered[i]);
    }
    rm_free(lowered);
    return ret;
  }

  // Apply pass. Validation guarantees TermData_AddId cannot fail here.
  for (size_t i = 0; i < size; i++) {
    TermData* termData = dictFetchValue(smap->h_table, lowered[i]);
    if (termData) {
      rm_free(lowered[i]);
    } else {
      termData = TermData_New(lowered[i]);
      dictAdd(smap->h_table, lowered[i], termData);
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
  char syn[len + 1];
  memcpy(syn, synonym, len);
  syn[len] = '\0';
  return dictFetchValue(smap->h_table, syn);
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

static void SynonymMap_CopyEntry(SynonymMap* smap, const char* key, TermData* t_data) {
  dictAdd(smap->h_table, (char*)key, TermData_Copy(t_data));
}

static SynonymMap* SynonymMap_GenerateReadOnlyCopy(SynonymMap* smap) {
  int ret;
  SynonymMap* read_only_smap = SynonymMap_New(true);
  dictIterator* iter = dictGetIterator(smap->h_table);
  dictEntry* entry = NULL;
  while ((entry = dictNext(iter))) {
    char* key = dictGetKey(entry);
    TermData* val = dictGetVal(entry);
    SynonymMap_CopyEntry(read_only_smap, key, val);
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
    if (t_data == NULL)
      goto cleanup;
    dictAdd(smap->h_table, t_data->term, t_data);
  }
  return smap;

cleanup:
  SynonymMap_Free(smap);
  return NULL;
}

#include "spec.h"
#include "synonym_map.h"
#include "rmalloc.h"
#include "util/fnv.h"
#include "rmutil/rm_assert.h"
#include "rdb.h"

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
    if (strcmp(t_data->groupIds[i], id) == 0) {
      return true;
    }
  }
  return false;
}

static void TermData_AddId(TermData* t_data, const char* id) {
  char* newId;
  rm_asprintf(&newId, SYNONYM_PREFIX, id);
  if (!TermData_IdExists(t_data, id)) {
    t_data->groupIds = array_append(t_data->groupIds, newId);
  }
}

static TermData* TermData_Copy(TermData* t_data) {
  TermData* copy = TermData_New(rm_strdup(t_data->term));
  for (int i = 0; i < array_len(t_data->groupIds); ++i) {
    TermData_AddId(copy, t_data->groupIds[i] + 1 /*we do not need the ~*/);
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
static TermData* TermData_RdbLoad(RedisModuleIO* rdb, int encver) {
  TermData *t_data = NULL;
  char* term = LoadStringBuffer_IOError(rdb, NULL, goto cleanup);
  t_data = TermData_New(rm_strdup(term));
  RedisModule_Free(term);
  uint64_t ids_len = LoadUnsigned_IOError(rdb, goto cleanup);
  for (int i = 0; i < ids_len; ++i) {
    char* groupId = NULL;
    if (encver <= INDEX_MIN_WITH_SYNONYMS_INT_GROUP_ID) {
      uint64_t id = LoadUnsigned_IOError(rdb, goto cleanup);
      rm_asprintf(&groupId, "%ld", id);
    } else {
      groupId = LoadStringBuffer_IOError(rdb, NULL, goto cleanup);
    }
    TermData_AddId(t_data, groupId);
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

void SynonymMap_UpdateRedisStr(SynonymMap* smap, RedisModuleString** synonyms, size_t size,
                               const char* groupId) {
  const char** arr = SynonymMap_RedisStringArrToArr(synonyms, size);
  SynonymMap_Update(smap, arr, size, groupId);
  rm_free(arr);
}

void SynonymMap_Add(SynonymMap* smap, const char* groupId, const char** synonyms, size_t size) {
  SynonymMap_Update(smap, synonyms, size, groupId);
}

void SynonymMap_Update(SynonymMap* smap, const char** synonyms, size_t size, const char* groupId) {
  RS_LOG_ASSERT(!smap->is_read_only, "SynonymMap should not be read only");
  int ret;
  for (size_t i = 0; i < size; i++) {
    char *lowerSynonym = rm_strdup(synonyms[i]);
    strtolower(lowerSynonym);
    TermData* termData = dictFetchValue(smap->h_table, lowerSynonym);
    if (termData) {
      // if term exists in dictionary, we should release the lower cased string
      rm_free(lowerSynonym);
    } else {
      termData = TermData_New(lowerSynonym); //strtolower
      dictAdd(smap->h_table, lowerSynonym, termData);
    }
    TermData_AddId(termData, groupId);
  }
  if (smap->read_only_copy) {
    SynonymMap_Free(smap->read_only_copy);
    smap->read_only_copy = NULL;
  }
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
  int ret;
  SynonymMap* smap = SynonymMap_New(false);
  if (encver <= INDEX_MIN_WITH_SYNONYMS_INT_GROUP_ID) {
    size_t unused = LoadUnsigned_IOError(rdb, goto cleanup);
  }
  uint64_t smap_kh_size = LoadUnsigned_IOError(rdb, goto cleanup);
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

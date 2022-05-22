#include "synonym_map.h"
#include "rmalloc.h"
#include "util/fnv.h"
#include "rmutil/rm_assert.h"

///////////////////////////////////////////////////////////////////////////////////////////////

#define INITIAL_CAPACITY 2
#define SYNONYM_PREFIX "~"

//---------------------------------------------------------------------------------------------

static const uint64_t calculate_hash(const char* str, size_t len) {
  return fnv_64a_buf((void*)str, len, 0);
}

//---------------------------------------------------------------------------------------------

static TermData* TermData_New(char* term) {
  TermData* t_data = rm_malloc(sizeof(TermData));
  t_data->term = term;
  t_data->ids = array_new(uint32_t, INITIAL_CAPACITY);
  return t_data;
}

//---------------------------------------------------------------------------------------------

static void TermData_Free(TermData* t_data) {
  rm_free(t_data->term);
  array_free(t_data->ids);
  rm_free(t_data);
}

//---------------------------------------------------------------------------------------------

static bool TermData_IdExists(TermData* t_data, uint32_t id) {
  for (uint32_t i = 0; i < array_len(t_data->ids); ++i) {
    if (t_data->ids[i] == id) {
      return true;
    }
  }
  return false;
}

//---------------------------------------------------------------------------------------------

static void TermData_AddId(TermData* t_data, uint32_t id) {
  if (!TermData_IdExists(t_data, id)) {
    t_data->ids = array_append(t_data->ids, id);
  }
}

//---------------------------------------------------------------------------------------------

static TermData* TermData_Copy(TermData* t_data) {
  TermData* copy = TermData_New(rm_strdup(t_data->term));
  for (int i = 0; i < array_len(t_data->ids); ++i) {
    TermData_AddId(copy, t_data->ids[i]);
  }
  return copy;
}

//---------------------------------------------------------------------------------------------

static void TermData_RdbSave(RedisModuleIO* rdb, TermData* t_data) {
  RedisModule_SaveStringBuffer(rdb, t_data->term, strlen(t_data->term) + 1);
  RedisModule_SaveUnsigned(rdb, array_len(t_data->ids));
  for (int i = 0; i < array_len(t_data->ids); ++i) {
    RedisModule_SaveUnsigned(rdb, t_data->ids[0]);
  }
}

//---------------------------------------------------------------------------------------------

static TermData* TermData_RdbLoad(RedisModuleIO* rdb) {
  char* term = RedisModule_LoadStringBuffer(rdb, NULL);
  TermData* t_data = TermData_New(rm_strdup(term));
  RedisModule_Free(term);
  uint64_t ids_len = RedisModule_LoadUnsigned(rdb);
  for (int i = 0; i < ids_len; ++i) {
    uint64_t id = RedisModule_LoadUnsigned(rdb);
    TermData_AddId(t_data, id);
  }
  return t_data;
}

///////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Creates a new synonym map data structure.
 * If is_read_only is true then it will only be possible to read from
 * this synonym map, Any attemp to write to it will result in assert failure.
 */

SynonymMap* SynonymMap_New(bool is_read_only) {
  SynonymMap* smap = rm_new(SynonymMap);
  smap->h_table = kh_init(SynMapKhid);
  smap->curr_id = 0;
  smap->is_read_only = is_read_only;
  smap->read_only_copy = NULL;
  smap->ref_count = 1;
  return smap;
}

//---------------------------------------------------------------------------------------------

/**
 * Free the given SynonymMap internal and structure
 */

void SynonymMap_Free(SynonymMap* smap) {
  if (smap->is_read_only) {
    --smap->ref_count;
    if (smap->ref_count > 0) {
      return;
    }
  }
  TermData* t_data;
  kh_foreach_value(smap->h_table, t_data, TermData_Free(t_data));
  kh_destroy(SynMapKhid, smap->h_table);
  if (smap->read_only_copy) {
    SynonymMap_Free(smap->read_only_copy);
  }
  rm_free(smap);
}

//---------------------------------------------------------------------------------------------

static const char** SynonymMap_RedisStringArrToArr(RedisModuleString** synonyms, size_t size) {
  const char** arr = rm_malloc(sizeof(char*) * size);
  for (size_t i = 0; i < size; ++i) {
    arr[i] = RedisModule_StringPtrLen(synonyms[i], NULL);
  }
  return arr;
}

//---------------------------------------------------------------------------------------------

/**
 * Synonym groups ids are increasing uint32_t.
 * Return the max id i.e the next id which will be given to the next synonym group.
 */

uint32_t SynonymMap_GetMaxId(SynonymMap* smap) {
  return smap->curr_id;
}

//---------------------------------------------------------------------------------------------

/**
 * Add new synonym group
 * smap - the synonym map
 * synonyms - RedisModuleString array contains the terms to add to the synonym map
 * size - RedisModuleString array size
 */

uint32_t SynonymMap_AddRedisStr(SynonymMap* smap, RedisModuleString** synonyms, size_t size) {

  const char** arr = SynonymMap_RedisStringArrToArr(synonyms, size);
  uint32_t ret_val = SynonymMap_Add(smap, arr, size);
  rm_free(arr);
  return ret_val;
}

//---------------------------------------------------------------------------------------------

/**
 * Updating an already existing synonym group
 * smap - the synonym map
 * synonyms - RedisModuleString array contains the terms to add to the synonym map
 * size - RedisModuleString array size
 * id - the synoym group id to update
 */

void SynonymMap_UpdateRedisStr(SynonymMap* smap, RedisModuleString** synonyms, size_t size,
                               uint32_t id) {
  const char** arr = SynonymMap_RedisStringArrToArr(synonyms, size);
  SynonymMap_Update(smap, arr, size, id);
  rm_free(arr);
}

//---------------------------------------------------------------------------------------------

/**
 * Add new synonym group
 * smap - the synonym map
 * synonyms - char* array contains the terms to add to the synonym map
 * size - char* array size
 */

uint32_t SynonymMap_Add(SynonymMap* smap, const char** synonyms, size_t size) {
  uint32_t id = smap->curr_id;
  SynonymMap_Update(smap, synonyms, size, id);
  return id;
}

//---------------------------------------------------------------------------------------------

/**
 * Updating an already existing synonym group
 * smap - the synonym map
 * synonyms - char* array contains the terms to add to the synonym map
 * size - char* array size
 * id - the synoym group id to update
 */

void SynonymMap_Update(SynonymMap* smap, const char** synonyms, size_t size, uint32_t id) {
  RS_LOG_ASSERT(!smap->is_read_only, "SynonymMap should not be read only");
  int ret;
  for (size_t i = 0; i < size; i++) {
    khiter_t k =
        kh_get(SynMapKhid, smap->h_table, calculate_hash(synonyms[i], strlen(synonyms[i])));
    if (k == kh_end(smap->h_table)) {
      k = kh_put(SynMapKhid, smap->h_table, calculate_hash(synonyms[i], strlen(synonyms[i])), &ret);
      kh_value(smap->h_table, k) = TermData_New(rm_strdup(synonyms[i]));
    }
    TermData_AddId(kh_value(smap->h_table, k), id);
  }
  if (smap->read_only_copy) {
    SynonymMap_Free(smap->read_only_copy);
    smap->read_only_copy = NULL;
  }
  if (id >= smap->curr_id) {
    smap->curr_id = id + 1;
  }
}

//---------------------------------------------------------------------------------------------

/**
 * Return all the ids of a given term
 * smap - the synonym map
 * synonym - the term to search for
 * len - term len
 */

TermData* SynonymMap_GetIdsBySynonym(SynonymMap* smap, const char* synonym, size_t len) {
  khiter_t k = kh_get(SynMapKhid, smap->h_table, calculate_hash(synonym, len));
  if (k == kh_end(smap->h_table)) {
    return NULL;
  }
  TermData* t_data = kh_value(smap->h_table, k);
  return t_data;
}

//---------------------------------------------------------------------------------------------

/**
 * Return array of all terms and the group ids they belong to
 * smap - the synonym map
 * size - a pointer to size_t to retrieve the result size
 */

TermData** SynonymMap_DumpAllTerms(SynonymMap* smap, size_t* size) {
  *size = kh_size(smap->h_table);
  TermData** dump = rm_malloc(sizeof(TermData*) * (*size));
  int j = 0;
  TermData* val;
  kh_foreach_value(smap->h_table, val, dump[j++] = val);
  return dump;
}

//---------------------------------------------------------------------------------------------

/**
 * Return an str representation of the given id
 * id - the id
 * buff - buffer to put the str representation
 * len - the buff len
 * return the size of the str writen to buff
 */

size_t SynonymMap_IdToStr(uint32_t id, char* buff, size_t len) {
  int bytes_written = snprintf(buff, len, SYNONYM_PREFIX "%d", id);
  RS_LOG_ASSERT(bytes_written >= 0 && bytes_written < len, "buffer is not big enough");
  return bytes_written;
}

//---------------------------------------------------------------------------------------------

static void SynonymMap_CopyEntry(SynonymMap* smap, uint64_t key, TermData* t_data) {
  int ret;
  khiter_t k = kh_put(SynMapKhid, smap->h_table, key, &ret);
  kh_value(smap->h_table, k) = TermData_Copy(t_data);
}

//---------------------------------------------------------------------------------------------

static SynonymMap* SynonymMap_GenerateReadOnlyCopy(SynonymMap* smap) {
  int ret;
  SynonymMap* read_only_smap = SynonymMap_New(true);
  read_only_smap->curr_id = smap->curr_id;
  uint64_t key;
  TermData* t_data;
  kh_foreach(smap->h_table, key, t_data, SynonymMap_CopyEntry(read_only_smap, key, t_data));
  return read_only_smap;
}

//---------------------------------------------------------------------------------------------

/**
 * Retun a read only copy of the given smap.
 * The read only copy is used in indexing to allow thread safe access to the synonym data structur
 * The read only copy is manage with ref count. The smap contians a reference to its read only copy
 * and will free it only when its data structure will change, then when someone will ask again for a
 * read only copy it will create a new one. The old read only copy will be freed when all the
 * indexers will finish using it.
 */

SynonymMap* SynonymMap_GetReadOnlyCopy(SynonymMap* smap) {
  RS_LOG_ASSERT(!smap->is_read_only, "SynonymMap should not be read only");
  if (!smap->read_only_copy) {
    // create a new read only copy and return it
    smap->read_only_copy = SynonymMap_GenerateReadOnlyCopy(smap);
  }

  ++smap->read_only_copy->ref_count;
  return smap->read_only_copy;
}

//---------------------------------------------------------------------------------------------

void SynonymMap_RdbSaveEntry(RedisModuleIO* rdb, uint64_t key, TermData* t_data) {
  RedisModule_SaveUnsigned(rdb, key);
  TermData_RdbSave(rdb, t_data);
}

//---------------------------------------------------------------------------------------------

/**
 * Save the given smap to an rdb
 */

void SynonymMap_RdbSave(RedisModuleIO* rdb, void* value) {
  SynonymMap* smap = value;
  uint64_t key;
  TermData* t_data;
  RedisModule_SaveUnsigned(rdb, smap->curr_id);
  RedisModule_SaveUnsigned(rdb, kh_size(smap->h_table));
  kh_foreach(smap->h_table, key, t_data, SynonymMap_RdbSaveEntry(rdb, key, t_data));
}

//---------------------------------------------------------------------------------------------

/**
 * Loadin smap from an rdb
 */

void* SynonymMap_RdbLoad(RedisModuleIO* rdb, int encver) {
  int ret;
  SynonymMap* smap = SynonymMap_New(false);
  smap->curr_id = RedisModule_LoadUnsigned(rdb);
  uint64_t smap_kh_size = RedisModule_LoadUnsigned(rdb);
  for (int i = 0; i < smap_kh_size; ++i) {
    uint64_t key = RedisModule_LoadUnsigned(rdb);
    TermData* t_data = TermData_RdbLoad(rdb);
    khiter_t k = kh_put(SynMapKhid, smap->h_table, key, &ret);
    kh_value(smap->h_table, k) = t_data;
  }
  return smap;
}

///////////////////////////////////////////////////////////////////////////////////////////////

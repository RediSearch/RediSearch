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

TermData::TermData(char* term) {
  term = term;
  ids = array_new(uint32_t, INITIAL_CAPACITY);
}

//---------------------------------------------------------------------------------------------

TermData::~TermData() {
  rm_free(term);
  array_free(ids);
}

//---------------------------------------------------------------------------------------------

inline bool TermData::IdExists(uint32_t id) {
  for (uint32_t i = 0; i < array_len(ids); ++i) {
    if (ids[i] == id) {
      return true;
    }
  }
  return false;
}

//---------------------------------------------------------------------------------------------

inline void TermData::AddId(uint32_t id) {
  if (!IdExists(id)) {
    ids = array_append(ids, id);
  }
}

//---------------------------------------------------------------------------------------------

inline TermData* TermData::Copy() {
  TermData* copy;
  for (int i = 0; i < array_len(ids); ++i) {
    copy->AddId(ids[i]);
  }
  return copy;
}

//---------------------------------------------------------------------------------------------

void TermData::RdbSave(RedisModuleIO* rdb) {
  RedisModule_SaveStringBuffer(rdb, term, strlen(term) + 1);
  RedisModule_SaveUnsigned(rdb, array_len(ids));
  for (int i = 0; i < array_len(ids); ++i) {
    RedisModule_SaveUnsigned(rdb, ids[0]);
  }
}

//---------------------------------------------------------------------------------------------

TermData::TermData(RedisModuleIO* rdb) {
  char* term = RedisModule_LoadStringBuffer(rdb, NULL);
  RedisModule_Free(term);
  uint64_t ids_len = RedisModule_LoadUnsigned(rdb);
  for (int i = 0; i < ids_len; ++i) {
    uint64_t id = RedisModule_LoadUnsigned(rdb);
    AddId(id);
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

/**
 * Creates a new synonym map data structure.
 * If is_read_only is true then it will only be possible to read from
 * this synonym map, Any attemp to write to it will result in assert failure.
 */

void SynonymMap::ctor(bool is_read_only_) {
  h_table = kh_init(SynMapKhid);
  curr_id = 0;
  is_read_only = is_read_only_;
  read_only_copy = NULL;
  ref_count = 1;
}

//---------------------------------------------------------------------------------------------

// Free the given SynonymMap internal and structure
SynonymMap::~SynonymMap() {
  if (is_read_only) {
    --ref_count;
    if (ref_count > 0) {
      return;
    }
  }
  TermData* t_data;
  kh_foreach_value(h_table, t_data, TermData_Free(t_data));
  kh_destroy(SynMapKhid, h_table);
  if (read_only_copy) {
    delete read_only_copy;
  }
}

//---------------------------------------------------------------------------------------------

static inline const char** SynonymMap::RedisStringArrToArr(RedisModuleString** synonyms, size_t size) {
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

uint32_t SynonymMap::GetMaxId() {
  return curr_id;
}

//---------------------------------------------------------------------------------------------

/**
 * Add new synonym group
 * smap - the synonym map
 * synonyms - RedisModuleString array contains the terms to add to the synonym map
 * size - RedisModuleString array size
 */

uint32_t SynonymMap::AddRedisStr(RedisModuleString** synonyms, size_t size) {
  const char** arr = RedisStringArrToArr(synonyms, size);
  uint32_t ret_val = Add(arr, size);
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

void SynonymMap::UpdateRedisStr(RedisModuleString** synonyms, size_t size, uint32_t id) {
  const char** arr = RedisStringArrToArr(synonyms, size);
  Update(arr, size, id);
  rm_free(arr);
}

//---------------------------------------------------------------------------------------------

/**
 * Add new synonym group
 * smap - the synonym map
 * synonyms - char* array contains the terms to add to the synonym map
 * size - char* array size
 */

uint32_t SynonymMap::Add(const char** synonyms, size_t size) {
  uint32_t id = curr_id;
  Update(synonyms, size, id);
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

void SynonymMap::Update(const char** synonyms, size_t size, uint32_t id) {
  RS_LOG_ASSERT(!is_read_only, "SynonymMap should not be read only");
  int ret;
  for (size_t i = 0; i < size; i++) {
    /*
    auto &syn = synonyms[i];
    auto it = h_table.get(syn);
    if (!!it) {
      it->set(new TermData(rm_strdup(syn)));
    }
    */
    khiter_t k =
        kh_get(SynMapKhid, h_table, calculate_hash(synonyms[i], strlen(synonyms[i])));
    if (k == kh_end(h_table)) {
      k = kh_put(SynMapKhid, h_table, calculate_hash(synonyms[i], strlen(synonyms[i])), &ret);
      kh_value(h_table, k) = new TermData(rm_strdup(synonyms[i]));
    }
    kh_value(h_table, k)->AddId(id);
  }
  if (read_only_copy) {
    delete read_only_copy;
  }
  if (id >= curr_id) {
    curr_id = id + 1;
  }
}

//---------------------------------------------------------------------------------------------

/**
 * Return all the ids of a given term
 * smap - the synonym map
 * synonym - the term to search for
 * len - term len
 */

TermData* SynonymMap::GetIdsBySynonym(const char* synonym, size_t len) {
  khiter_t k = kh_get(SynMapKhid, h_table, calculate_hash(synonym, len));
  if (k == kh_end(h_table)) {
    return NULL;
  }
  TermData* t_data = kh_value(h_table, k);
  return t_data;
}

//---------------------------------------------------------------------------------------------

/**
 * Return array of all terms and the group ids they belong to
 * smap - the synonym map
 * size - a pointer to size_t to retrieve the result size
 */

TermData** SynonymMap::DumpAllTerms(size_t* size) {
  *size = kh_size(h_table);
  TermData** dump;
  int j = 0;
  TermData* val;
  kh_foreach_value(h_table, val, dump[j++] = val);
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

static size_t SynonymMap::IdToStr(uint32_t id, char* buff, size_t len) {
  int bytes_written = snprintf(buff, len, SYNONYM_PREFIX "%d", id);
  RS_LOG_ASSERT(bytes_written >= 0 && bytes_written < len, "buffer is not big enough");
  return bytes_written;
}

//---------------------------------------------------------------------------------------------

inline void SynonymMap::CopyEntry(uint64_t key, TermData* t_data) {
  int ret;
  khiter_t k = kh_put(SynMapKhid, h_table, key, &ret);
  kh_value(h_table, k) = t_data->Copy();
}

//---------------------------------------------------------------------------------------------

inline SynonymMap* SynonymMap::GenerateReadOnlyCopy() {
  int ret;
  SynonymMap* read_only_smap(true);
  read_only_smap->curr_id = curr_id;
  uint64_t key;
  TermData* t_data;
  kh_foreach(h_table, key, t_data, read_only_smap->CopyEntry(key, t_data));
  return read_only_smap;
}

//---------------------------------------------------------------------------------------------

/**
 * Retun a read only copy of the smap.
 * The read only copy is used in indexing to allow thread safe access to the synonym data structur
 * The read only copy is manage with ref count. The smap contians a reference to its read only copy
 * and will free it only when its data structure will change, then when someone will ask again for a
 * read only copy it will create a new one. The old read only copy will be freed when all the
 * indexers will finish using it.
 */

SynonymMap* SynonymMap::GetReadOnlyCopy() {
  RS_LOG_ASSERT(!is_read_only, "SynonymMap should not be read only");
  if (!read_only_copy) {
    // create a new read only copy and return it
    read_only_copy = GenerateReadOnlyCopy();
  }

  ++read_only_copy->ref_count;
  return read_only_copy;
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

static void SynonymMap::RdbSave(RedisModuleIO* rdb, void* value) {
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
SynonymMap::SynonymMap(RedisModuleIO* rdb, int encver) {
  int ret;
  ctor(false);
  curr_id = RedisModule_LoadUnsigned(rdb);
  uint64_t smap_kh_size = RedisModule_LoadUnsigned(rdb);
  for (int i = 0; i < smap_kh_size; ++i) {
    uint64_t key = RedisModule_LoadUnsigned(rdb);
    TermData* t_data(rdb);
    khiter_t k = kh_put(SynMapKhid, h_table, key, &ret);
    kh_value(h_table, k) = t_data;
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

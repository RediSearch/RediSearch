#include "synonym_map.h"
#include "rmalloc.h"
#include "util/fnv.h"
#include "rmutil/rm_assert.h"

///////////////////////////////////////////////////////////////////////////////////////////////

#define INITIAL_CAPACITY 2
#define SYNONYM_PREFIX "~"

//---------------------------------------------------------------------------------------------

TermData::TermData(char* t) : term(t) {
  ids.reserve(INITIAL_CAPACITY);
}

//---------------------------------------------------------------------------------------------

TermData::TermData(const TermData &data) : term(data.term) {
  for (auto i : data.ids) {
    AddId(i);
  }
}

//---------------------------------------------------------------------------------------------

bool TermData::IdExists(uint32_t id) {
  for (auto i : ids) {
    if (i == id) {
      return true;
    }
  }
  return false;
}

//---------------------------------------------------------------------------------------------

void TermData::AddId(uint32_t id) {
  if (!IdExists(id)) {
    ids.push_back(id);
  }
}

//---------------------------------------------------------------------------------------------

void TermData::RdbSave(RedisModuleIO* rdb) {
  RedisModule_SaveStringBuffer(rdb, term.c_str(), term.length() + 1);
  RedisModule_SaveUnsigned(rdb, ids.size());
  for (auto i : ids) {
    RedisModule_SaveUnsigned(rdb, i);
  }
}

//---------------------------------------------------------------------------------------------

TermData::TermData(RedisModuleIO* rdb) {
  char *term_str = RedisModule_LoadStringBuffer(rdb, NULL);
  term = term_str;
  RedisModule_Free(term_str);
  uint64_t ids_len = RedisModule_LoadUnsigned(rdb);
  for (int i = 0; i < ids_len; ++i) {
    uint64_t id = RedisModule_LoadUnsigned(rdb);
    AddId(id);
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

SynonymMap::SynonymMap(const SynonymMap &map, bool read_only) : curr_id(map.curr_id), ref_count(1),
  is_read_only(read_only), read_only_copy(NULL) {
  for(const auto& [key, val] : map.h_table) {
    h_table.insert({key, new TermData{*val}});
  }
}

//---------------------------------------------------------------------------------------------

// Creates a new synonym map data structure.
// If is_read_only is true then it will only be possible to read from
// this synonym map, Any attemp to write to it will result in assert failure.

void SynonymMap::ctor(bool is_readonly) {
  curr_id = 0;
  is_read_only = is_readonly;
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

  if (read_only_copy) {
    delete read_only_copy;
  }

  h_table.clear();
}

//---------------------------------------------------------------------------------------------

const char** SynonymMap::RedisStringArrToArr(RedisModuleString** synonyms, size_t size) {
  const char** arr = rm_malloc(sizeof(char*) * size);
  for (size_t i = 0; i < size; ++i) {
    arr[i] = RedisModule_StringPtrLen(synonyms[i], NULL);
  }
  return arr;
}

//---------------------------------------------------------------------------------------------

// Synonym groups ids are increasing uint32_t.
// Return the max id i.e the next id which will be given to the next synonym group.

uint32_t SynonymMap::GetMaxId() {
  return curr_id;
}

//---------------------------------------------------------------------------------------------

// Add new synonym group
// synonyms - RedisModuleString array contains the terms to add to the synonym map
// size - RedisModuleString array size

uint32_t SynonymMap::AddRedisStr(RedisModuleString** synonyms, size_t size) {
  const char** arr = RedisStringArrToArr(synonyms, size);
  uint32_t ret_val = Add(arr, size);
  rm_free(arr);
  return ret_val;
}

//---------------------------------------------------------------------------------------------

// Updating an already existing synonym group
// synonyms - RedisModuleString array contains the terms to add to the synonym map
// size - RedisModuleString array size
// id - the synoym group id to update

void SynonymMap::UpdateRedisStr(RedisModuleString** synonyms, size_t size, uint32_t id) {
  const char** arr = RedisStringArrToArr(synonyms, size);
  Update(arr, size, id);
  rm_free(arr);
}

//---------------------------------------------------------------------------------------------

// Add new synonym group
// synonyms - char* array contains the terms to add to the synonym map
// size - char* array size

uint32_t SynonymMap::Add(const char** synonyms, size_t size) {
  uint32_t id = curr_id;
  Update(synonyms, size, id);
  return id;
}

//---------------------------------------------------------------------------------------------

// Updating an already existing synonym group
// synonyms - char* array contains the terms to add to the synonym map
// size - char* array size
// id - the synoym group id to update

void SynonymMap::Update(const char** synonyms, size_t size, uint32_t id) {
  RS_LOG_ASSERT(!is_read_only, "SynonymMap should not be read only");

  for (size_t i = 0; i < size; i++) {
    TermData *term = new TermData{synonyms[i]};
    term->AddId(id);
    h_table.insert({term->term, term});
  }

  if (read_only_copy) {
    delete read_only_copy;
  }

  if (id >= curr_id) {
    curr_id = id + 1;
  }
}

//---------------------------------------------------------------------------------------------

// Return all the ids of a given term
// synonym - the term to search for
// len - term len

TermData* SynonymMap::GetIdsBySynonym(std::string_view synonym) {
  auto it = h_table.find(String{synonym});
  return it == h_table.end() ? NULL : it->second;
}

//---------------------------------------------------------------------------------------------

// Return array of all terms and the group ids they belong to
// size - a pointer to size_t to retrieve the result size

Vector<TermData*> SynonymMap::DumpAllTerms() {
  Vector<TermData*> dump;
  dump.reserve(h_table.size());
  for(const auto& [_, val] : h_table) {
    dump.push_back(val);
  }
  return dump;
}

//---------------------------------------------------------------------------------------------

// Return an str representation of the given id
// id - the id
// buff - buffer to put the str representation
// len - the buff len
// return the size of the str writen to buff

String SynonymMap::IdToStr(uint32_t id) {
  return stringf(SYNONYM_PREFIX "%d", id);
}

//---------------------------------------------------------------------------------------------

// Retun a read only copy of the smap.
// The read only copy is used in indexing to allow thread safe access to the synonym data structur
// The read only copy is manage with ref count. The smap contians a reference to its read only copy
// and will free it only when its data structure will change, then when someone will ask again for a
// read only copy it will create a new one. The old read only copy will be freed when all the
// indexers will finish using it.

SynonymMap* SynonymMap::GetReadOnlyCopy() {
  RS_LOG_ASSERT(!is_read_only, "SynonymMap should not be read only");
  if (!read_only_copy) {
    read_only_copy = new SynonymMap{*this, true};
  }

  for(const auto& [key, val] : read_only_copy->h_table) {
    printf("%s: %s\n", key.c_str(), val->term.c_str());
  }

  ++read_only_copy->ref_count;
  return read_only_copy;
}

//---------------------------------------------------------------------------------------------

// Save the smap to an rdb

void SynonymMap::RdbSave(RedisModuleIO* rdb) {
  RedisModule_SaveUnsigned(rdb, curr_id);
  RedisModule_SaveUnsigned(rdb, h_table.size());

  for (const auto& [_, term] : h_table) {
    RedisModule_SaveUnsigned(rdb, 0);
    term->RdbSave(rdb);
  }
}

//---------------------------------------------------------------------------------------------

// Loadin smap from an rdb

SynonymMap::SynonymMap(RedisModuleIO* rdb, int encver) {
  ctor(false);
  curr_id = RedisModule_LoadUnsigned(rdb);
  uint64_t smap_kh_size = RedisModule_LoadUnsigned(rdb);
  for (int i = 0; i < smap_kh_size; ++i) {
    uint64_t key = RedisModule_LoadUnsigned(rdb);
    auto term = new TermData(rdb);
    h_table.insert({term->term, term});
  }
}

//---------------------------------------------------------------------------------------------

// void SynonymMap::print_h_table() const {
//   for(const auto& [key, val] : h_table) {
//     printf("%s: %s\n", key.c_str(), val->term.c_str());
//   }
// }

///////////////////////////////////////////////////////////////////////////////////////////////

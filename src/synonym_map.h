
#pragma once

#include "rmutil/vector.h"
#include "util/map.h"
#include "redismodule.h"
#include "util/arr.h"

#include <stdbool.h>

///////////////////////////////////////////////////////////////////////////////////////////////

// Holding a term data

struct TermData {
  String term;            // the term itself
  Vector<uint32_t> ids;  // array of synonyms group ids that the term is belong to

  TermData(char *t);
  TermData(const TermData &data);
  TermData(RedisModuleIO *rdb);

  void RdbSave(RedisModuleIO *rdb);
  bool IdExists(uint32_t id);
  void AddId(uint32_t id);
};

///////////////////////////////////////////////////////////////////////////////////////////////

// The synonym map data structure

struct SynonymMap : Object {
private:
  UnorderedMap<String, TermData*> h_table;
public:
  uint32_t ref_count;
  uint32_t curr_id;
  bool is_read_only;
  struct SynonymMap* read_only_copy;

  void ctor(bool is_read_only_);
  SynonymMap(bool is_read_only_) { ctor(is_read_only_); }
  SynonymMap(const SynonymMap &map, bool read_only);
  SynonymMap(RedisModuleIO *rdb, int encver);

  ~SynonymMap();

  uint32_t GetMaxId();

  uint32_t Add(const char **synonyms, size_t size);
  uint32_t AddRedisStr(RedisModuleString **synonyms, size_t size);
  void Update(const char **synonyms, size_t size, uint32_t id);
  void UpdateRedisStr(RedisModuleString **synonyms, size_t size, uint32_t id);

  TermData* GetIdsBySynonym(std::string_view synonym);

  Vector<TermData*> DumpAllTerms();

  SynonymMap* GetReadOnlyCopy();

  void RdbSave(RedisModuleIO *rdb);

  static String IdToStr(uint32_t id);
  static const char **RedisStringArrToArr(RedisModuleString **synonyms, size_t size);

  // void print_h_table() const;
};

///////////////////////////////////////////////////////////////////////////////////////////////

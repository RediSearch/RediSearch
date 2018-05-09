/*
 * synonym_map.h
 *
 *  Created on: May 2, 2018
 *      Author: meir
 */

#ifndef SRC_SYNONYM_MAP_H_
#define SRC_SYNONYM_MAP_H_

#include "util/khash.h"
#include "redismodule.h"
#include "util/arr.h"
#include <stdbool.h>

typedef struct {
  char* term;
  uint32_t* ids;
} TermData;

static const int SynMapKhid = 90;
KHASH_MAP_INIT_INT64(SynMapKhid, TermData*);

typedef struct SynonymMap_s {
  uint32_t ref_count;
  uint32_t curr_id;
  khash_t(SynMapKhid) * h_table;
  bool is_read_only;
  struct SynonymMap_s* read_only_copy;
} SynonymMap;

SynonymMap* SynonymMap_New(bool is_read_only);
void SynonymMap_Free(SynonymMap* smap);
uint32_t SynonymMap_GetMaxId(SynonymMap* smap);
uint32_t SynonymMap_AddRedisStr(SynonymMap* smap, RedisModuleString** synonyms, size_t size);
void SynonymMap_UpdateRedisStr(SynonymMap* smap, RedisModuleString** synonyms, size_t size,
                               uint32_t id);
uint32_t SynonymMap_Add(SynonymMap* smap, const char** synonyms, size_t size);
void SynonymMap_Update(SynonymMap* smap, const char** synonyms, size_t size, uint32_t id);
TermData* SynonymMap_GetIdsBySynonym(SynonymMap* smap, const char* synonym, size_t len);
TermData** SynonymMap_DumpAllTerms(SynonymMap* smap, size_t* size);
size_t SynonymMap_IdToStr(uint32_t id, char* buff, size_t len);

SynonymMap* SynonymMap_GetReadOnlyCopy(SynonymMap* smap);

#define SynonymMap_GetIdsBySynonym_cstr(smap, synonym) \
  SynonymMap_GetIdsBySynonym(smap, synonym, strlen(synonym))

void SynonymMap_RdbSave(RedisModuleIO* rdb, void* value);
void* SynonymMap_RdbLoad(RedisModuleIO* rdb, int encver);

#endif /* SRC_SYNONYM_MAP_H_ */

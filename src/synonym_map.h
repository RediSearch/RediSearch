/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */


#ifndef SRC_SYNONYM_MAP_H_
#define SRC_SYNONYM_MAP_H_

#include "util/dict.h"
#include "redismodule.h"
#include "util/arr.h"
#include "util/strconv.h"
#include <stdbool.h>

#define SYNONYM_PREFIX_CHAR '~'

/**
 * Holding a term data
 *  term - the term itself
 *  ids - array of synonyms group ids that the term is belong to
 */
typedef struct {
  char* term;
  char** groupIds;
} TermData;

/**
 * The synonym map data structure
 */
typedef struct SynonymMap_s {
  uint32_t ref_count;
  dict* h_table;
  bool is_read_only;
  struct SynonymMap_s* read_only_copy;
} SynonymMap;

/**
 * Creates a new synonym map data structure.
 * If is_read_only is true then it will only be possible to read from
 * this synonym map, Any attemp to write to it will result in assert failure.
 */
SynonymMap* SynonymMap_New(bool is_read_only);

/**
 * Free the given SynonymMap internal and structure
 */
void SynonymMap_Free(SynonymMap* smap);

/**
 * Updating an already existing synonym group
 * smap - the synonym map
 * synonyms - RedisModuleString array contains the terms to add to the synonym map
 * size - RedisModuleString array size
 * id - the synoym group id to update
 */
void SynonymMap_UpdateRedisStr(SynonymMap* smap, RedisModuleString** synonyms, size_t size, const char* groupId);

/**
 * Add new synonym group
 * smap - the synonym map
 * synonyms - char* array contains the terms to add to the synonym map
 * size - char* array size
 */
void SynonymMap_Add(SynonymMap* smap, const char* groupId, const char** synonyms, size_t size);

/**
 * Updating an already existing synonym group
 * smap - the synonym map
 * synonyms - char* array contains the terms to add to the synonym map
 * size - char* array size
 * id - the synoym group id to update
 */
void SynonymMap_Update(SynonymMap* smap, const char** synonyms, size_t size, const char* groupId);

/**
 * Return all the ids of a given term
 * smap - the synonym map
 * synonym - the term to search for
 * len - term len
 */
TermData* SynonymMap_GetIdsBySynonym(SynonymMap* smap, const char* synonym, size_t len);

/**
 * Return array of all terms and the group ids they belong to
 * smap - the synonym map
 * size - a pointer to size_t to retrieve the result size
 */
TermData** SynonymMap_DumpAllTerms(SynonymMap* smap, size_t* size);

/**
 * Return an str representation of the given id
 * id - the id
 * buff - buffer to put the str representation
 * len - the buff len
 * return the size of the str writen to buff
 */
size_t SynonymMap_IdToStr(uint32_t id, char* buff, size_t len);

/**
 * Retun a read only copy of the given smap.
 * The read only copy is used in indexing to allow thread safe access to the synonym data structur
 * The read only copy is manage with ref count. The smap contians a reference to its read only copy
 * and will free it only when its data structure will change, then when someone will ask again for a
 * read only copy it will create a new one. The old read only copy will be freed when all the
 * indexers will finish using it.
 */
SynonymMap* SynonymMap_GetReadOnlyCopy(SynonymMap* smap);

/**
 * Macro for using SynonymMap_GetIdsBySynonym with NULL terminated string
 */
#define SynonymMap_GetIdsBySynonym_cstr(smap, synonym) \
  SynonymMap_GetIdsBySynonym(smap, synonym, strlen(synonym))

/**
 * Save the given smap to an rdb
 */
void SynonymMap_RdbSave(RedisModuleIO* rdb, void* value);

/**
 * Loadin smap from an rdb
 */
void* SynonymMap_RdbLoad(RedisModuleIO* rdb, int encver);

#endif /* SRC_SYNONYM_MAP_H_ */

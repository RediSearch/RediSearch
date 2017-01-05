#ifndef __SPEC_H__
#define __SPEC_H__
#include "redismodule.h"
#include <stdlib.h>
#include <string.h>

typedef enum fieldType { F_FULLTEXT, F_NUMERIC, F_GEO } FieldType;

#define NUMERIC_STR "NUMERIC"

#define INDEX_SPEC_KEY_FMT "idx:%s"

#define SPEC_MAX_FIELDS 32

/* The fieldSpec represents a single field in the document's field spec.
Each field has a unique id that's a power of two, so we can filter fields
by a bit mask.
Each field has a type, allowing us to add non text fields in the future */
typedef struct fieldSpec {
  char *name;
  FieldType type;
  double weight;
  int id;
  // TODO: More options here..
} FieldSpec;

typedef struct {
  size_t numDocuments;
  size_t numTerms;
  size_t numRecords;
  size_t invertedSize;
  size_t invertedCap;
  size_t skipIndexesSize;
  size_t scoreIndexesSize;
  size_t offsetVecsSize;
  size_t offsetVecRecords;
  size_t termsSize;
} IndexStats;

typedef enum {
  Index_DefaultFlags = 0x00,
  Index_StoreTermOffsets = 0x01,
  Index_StoreFieldFlags = 0x02,
  Index_StoreScores = 0x04,
} IndexFlags;

#define INDEX_CURRENT_VERSION 1

typedef struct {
  char *name;
  FieldSpec *fields;
  int numFields;

  IndexStats stats;
  IndexFlags flags;
} IndexSpec;

extern RedisModuleType *IndexSpecType;

/*
* Get a field spec by field name. Case insensitive!
* Return the field spec if found, NULL if not
*/
FieldSpec *IndexSpec_GetField(IndexSpec *spec, const char *name, size_t len);

/*
* Parse an index spec from redis command arguments.
* Returns REDISMODULE_ERR if there's a parsing error.
* The command only receives the relvant part of argv.
*
* The format currently is <field> <weight>, <field> <weight> ...
*/
IndexSpec *IndexSpec_ParseRedisArgs(RedisModuleCtx *ctx, RedisModuleString *name,
                                    RedisModuleString **argv, int argc);

/* Same as above but with ordinary strings, to allow unit testing */
IndexSpec *IndexSpec_Parse(const char *name, const char **argv, int argc);

IndexSpec *IndexSpec_Load(RedisModuleCtx *ctx, const char *name, int openWrite);

/*
* Free an indexSpec. This doesn't free the spec itself as it's not allocated by the parser
* and should be on the request's stack
*/
void IndexSpec_Free(void *spec);

IndexSpec *NewIndexSpec(const char *name, size_t numFields);
void *IndexSpec_RdbLoad(RedisModuleIO *rdb, int encver);
void IndexSpec_RdbSave(RedisModuleIO *rdb, void *value);
void IndexSpec_Digest(RedisModuleDigest *digest, void *value);
void IndexSpec_AofRewrite(RedisModuleIO *aof, RedisModuleString *key, void *value);
int IndexSpec_RegisterType(RedisModuleCtx *ctx);
// void IndexSpec_Free(void *value);

/*
* Parse the field mask passed to a query, map field names to a bit mask passed down to the
* execution engine, detailing which fields the query works on. See FT.SEARCH for API details
*/
u_char IndexSpec_ParseFieldMask(IndexSpec *sp, RedisModuleString **argv, int argc);

#endif
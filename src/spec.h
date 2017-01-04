#ifndef __SPEC_H__
#define __SPEC_H__
#include "redismodule.h"
#include <stdlib.h>
#include <string.h>

typedef enum fieldType { F_FULLTEXT, F_NUMERIC, F_GEO } FieldType;

#define NUMERIC_STR "NUMERIC"

/* The fieldSpec represents a single field in the document's field spec.
Each field has a unique id that's a power of two, so we can filter fields
by a bit mask.
Each field has a type, allowing us to add non text fields in the future */
typedef struct fieldSpec {
  const char *name;
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

typedef struct {
  FieldSpec *fields;
  int numFields;
  const char *name;
  IndexStats *stats;
} IndexSpec;

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
int IndexSpec_ParseRedisArgs(IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString **argv,
                             int argc);

/* Same as above but with ordinary strings, to allow unit testing */
int IndexSpec_Parse(IndexSpec *s, const char **argv, int argc);

/*
* Free an indexSpec. This doesn't free the spec itself as it's not allocated by the parser
* and should be on the request's stack
*/
void IndexSpec_Free(IndexSpec *spec);

int IndexSpec_Load(RedisModuleCtx *ctx, IndexSpec *sp, const char *name);
int IndexSpec_Save(RedisModuleCtx *ctx, IndexSpec *sp);

/*
* Parse the field mask passed to a query, map field names to a bit mask passed down to the
* execution engine, detailing which fields the query works on. See FT.SEARCH for API details
*/
u_char IndexSpec_ParseFieldMask(IndexSpec *sp, RedisModuleString **argv, int argc);

#endif
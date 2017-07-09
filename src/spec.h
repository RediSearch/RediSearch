#ifndef __SPEC_H__
#define __SPEC_H__
#include <stdlib.h>
#include <string.h>


#include "redismodule.h"
#include "doc_table.h"
#include "trie/trie_type.h"
#include "sortable.h"
#include "stopwords.h"

typedef enum fieldType { F_FULLTEXT, F_NUMERIC, F_GEO, F_TAG } FieldType;

#define NUMERIC_STR "NUMERIC"
#define GEO_STR "GEO"

#define SPEC_NOOFFSETS_STR "NOOFFSETS"
#define SPEC_NOFIELDS_STR "NOFIELDS"
#define SPEC_NOSCOREIDX_STR "NOSCOREIDX"
#define SPEC_SCHEMA_STR "SCHEMA"
#define SPEC_TEXT_STR "TEXT"
#define SPEC_WEIGHT_STR "WEIGHT"
#define SPEC_TAG_STR "TAG"
#define SPEC_SORTABLE_STR "SORTABLE"
#define SPEC_STOPWORDS_STR "STOPWORDS"

static const char *SpecTypeNames[] = {[F_FULLTEXT] = SPEC_TEXT_STR, [F_NUMERIC] = NUMERIC_STR,
                                      [F_GEO] = GEO_STR, [F_TAG] = SPEC_TAG_STR};
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
  t_fieldMask id;

  int sortable;
  int sortIdx;

  // TODO: const char **separators;
  // size_t numSeparators;
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
  Index_StoreTermOffsets = 0x01,
  Index_StoreFieldFlags = 0x02,
  Index_StoreScoreIndexes = 0x04,
  Index_HasCustomStopwords = 0x08,
} IndexFlags;

#define INDEX_DEFAULT_FLAGS Index_StoreTermOffsets | Index_StoreFieldFlags | Index_StoreScoreIndexes
#define INDEX_CURRENT_VERSION 5
#define INDEX_MIN_COMPAT_VERSION 2

typedef struct {
  char *name;
  FieldSpec *fields;
  int numFields;

  IndexStats stats;
  IndexFlags flags;

  Trie *terms;

  RSSortingTable *sortables;

  DocTable docs;

  StopWordList *stopwords;
} IndexSpec;

extern RedisModuleType *IndexSpecType;

/*
* Get a field spec by field name. Case insensitive!
* Return the field spec if found, NULL if not
*/
FieldSpec *IndexSpec_GetField(IndexSpec *spec, const char *name, size_t len);

char *GetFieldNameByBit(IndexSpec *sp, uint32_t id);
/* Get the field bitmask id of a text field by name. Return 0 if the field is not found or is not a
 * text field */
uint32_t IndexSpec_GetFieldBit(IndexSpec *spec, const char *name, size_t len);

/* Get a sortable field's sort table index by its name. return -1 if the field was not found or is
 * not sortable */
int IndexSpec_GetFieldSortingIndex(IndexSpec *sp, const char *name, size_t len);
/*
* Parse an index spec from redis command arguments.
* Returns REDISMODULE_ERR if there's a parsing error.
* The command only receives the relvant part of argv.
*
* The format currently is <field> <weight>, <field> <weight> ...
*/
IndexSpec *IndexSpec_ParseRedisArgs(RedisModuleCtx *ctx, RedisModuleString *name,
                                    RedisModuleString **argv, int argc, char **err);

/* Same as above but with ordinary strings, to allow unit testing */
IndexSpec *IndexSpec_Parse(const char *name, const char **argv, int argc, char **err);

IndexSpec *IndexSpec_Load(RedisModuleCtx *ctx, const char *name, int openWrite);

int IndexSpec_AddTerm(IndexSpec *sp, const char *term, size_t len);
/*
* Free an indexSpec. This doesn't free the spec itself as it's not allocated by the parser
* and should be on the request's stack
*/
void IndexSpec_Free(void *spec);

/* Parse a new stopword list and set it. If the parsing fails we revert to the default stopword
 * list, and return 0 */
int IndexSpec_ParseStopWords(IndexSpec *sp, RedisModuleString **strs, size_t len);

/* Return 1 if a term is a stopword for the specific index */
int IndexSpec_IsStopWord(IndexSpec *sp, const char *term, size_t len);

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
t_fieldMask IndexSpec_ParseFieldMask(IndexSpec *sp, RedisModuleString **argv, int argc);

#endif
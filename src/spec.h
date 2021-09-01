#ifndef __SPEC_H__
#define __SPEC_H__

#include <stdlib.h>
#include <string.h>

#include "default_gc.h"
#include "redismodule.h"
#include "doc_table.h"
#include "trie/trie_type.h"
#include "sortable.h"
#include "stopwords.h"
#include "gc.h"
#include "synonym_map.h"
#include "query_error.h"
#include "field_spec.h"
#include "util/dict.h"
#include "redisearch_api.h"
#include "rules.h"

#ifdef __cplusplus
extern "C" {
#endif

///////////////////////////////////////////////////////////////////////////////////////////////

struct IndexesScanner;
struct DocumentIndexer;

#define NUMERIC_STR "NUMERIC"
#define GEO_STR "GEO"

#define SPEC_NOOFFSETS_STR "NOOFFSETS"
#define SPEC_NOFIELDS_STR "NOFIELDS"
#define SPEC_NOFREQS_STR "NOFREQS"
#define SPEC_NOHL_STR "NOHL"
#define SPEC_SCHEMA_STR "SCHEMA"
#define SPEC_SCHEMA_EXPANDABLE_STR "MAXTEXTFIELDS"
#define SPEC_TEMPORARY_STR "TEMPORARY"
#define SPEC_AS_STR "AS"
#define SPEC_TEXT_STR "TEXT"
#define SPEC_WEIGHT_STR "WEIGHT"
#define SPEC_NOSTEM_STR "NOSTEM"
#define SPEC_PHONETIC_STR "PHONETIC"
#define SPEC_TAG_STR "TAG"
#define SPEC_SORTABLE_STR "SORTABLE"
#define SPEC_UNF_STR "UNF"
#define SPEC_STOPWORDS_STR "STOPWORDS"
#define SPEC_NOINDEX_STR "NOINDEX"
#define SPEC_TAG_SEPARATOR_STR "SEPARATOR"
#define SPEC_TAG_CASE_SENSITIVE_STR "CASESENSITIVE"
#define SPEC_MULTITYPE_STR "MULTITYPE"
#define SPEC_ASYNC_STR "ASYNC"
#define SPEC_SKIPINITIALSCAN_STR "SKIPINITIALSCAN"

#define DEFAULT_SCORE 1.0

#define SPEC_FOLLOW_HASH_ARGS_DEF(rule)                                     \
  {.name = "PREFIX", .target = &rule_prefixes, .type = AC_ARGTYPE_SUBARGS}, \
      {.name = "FILTER",                                                    \
       .target = &(rule)->filter_exp_str,                                   \
       .len = &dummy2,                                                      \
       .type = AC_ARGTYPE_STRING},                                          \
      {.name = "SCORE",                                                     \
       .target = &(rule)->score_default,                                    \
       .len = &dummy2,                                                      \
       .type = AC_ARGTYPE_STRING},                                          \
      {.name = "SCORE_FIELD",                                               \
       .target = &(rule)->score_field,                                      \
       .len = &dummy2,                                                      \
       .type = AC_ARGTYPE_STRING},                                          \
      {.name = "LANGUAGE",                                                  \
       .target = &(rule)->lang_default,                                     \
       .len = &dummy2,                                                      \
       .type = AC_ARGTYPE_STRING},                                          \
      {.name = "LANGUAGE_FIELD",                                            \
       .target = &(rule)->lang_field,                                       \
       .len = &dummy2,                                                      \
       .type = AC_ARGTYPE_STRING},                                          \
      {.name = "PAYLOAD_FIELD",                                             \
       .target = &(rule)->payload_field,                                    \
       .len = &dummy2,                                                      \
       .type = AC_ARGTYPE_STRING},

// TODO: remove usage of keyspace prefix now that RediSearch is out of keyspace 
#define INDEX_SPEC_KEY_PREFIX "idx:"
#define INDEX_SPEC_KEY_FMT INDEX_SPEC_KEY_PREFIX "%s"

#define SPEC_MAX_FIELDS 1024
#define SPEC_MAX_FIELD_ID (sizeof(t_fieldMask) * 8)

// The threshold after which we move to a special encoding for wide fields
#define SPEC_WIDEFIELD_THRESHOLD 32

extern dict *specDict_g;

extern size_t pending_global_indexing_ops;
extern struct IndexesScanner *global_spec_scanner;
extern dict *legacySpecRules;

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
  size_t indexingFailures;
} IndexStats;

typedef enum {
  Index_StoreTermOffsets = 0x01,
  Index_StoreFieldFlags = 0x02,

  // Was StoreScoreIndexes, but these are always stored, so this option is unused
  Index__Reserved1 = 0x04,
  Index_HasCustomStopwords = 0x08,
  Index_StoreFreqs = 0x010,
  Index_StoreNumeric = 0x020,
  Index_StoreByteOffsets = 0x40,
  Index_WideSchema = 0x080,
  Index_HasSmap = 0x100,
  Index_Temporary = 0x200,
  Index_DocIdsOnly = 0x00,

  // If any of the fields has phonetics. This is just a cache for quick lookup
  Index_HasPhonetic = 0x400,
  Index_Async = 0x800,
  Index_SkipInitialScan = 0x1000,
  Index_FromLLAPI = 0x2000,
} IndexFlags;

// redis version (its here because most file include it with no problem,
// we should introduce proper common.h file)

typedef struct Version {
  int majorVersion;
  int minorVersion;
  int patchVersion;
  int buildVersion;  // if not exits then its zero
} Version;

extern Version noScanVersion;
extern Version redisVersion;
extern Version rlecVersion;
extern bool isCrdt;
extern bool isTrimming;

/**
 * This "ID" type is independent of the field mask, and is used to distinguish
 * between one field and another field. For now, the ID is the position in
 * the array of fields - a detail we'll try to hide.
 */
typedef uint16_t FieldSpecDedupeArray[SPEC_MAX_FIELDS];

#define INDEX_DEFAULT_FLAGS \
  Index_StoreFreqs | Index_StoreTermOffsets | Index_StoreFieldFlags | Index_StoreByteOffsets

#define INDEX_STORAGE_MASK                                                                  \
  (Index_StoreFreqs | Index_StoreFieldFlags | Index_StoreTermOffsets | Index_StoreNumeric | \
   Index_WideSchema)

#define INDEX_CURRENT_VERSION 18
#define INDEX_JSON_VERSION 18
#define INDEX_MIN_COMPAT_VERSION 17

#define LEGACY_INDEX_MAX_VERSION 16
#define LEGACY_INDEX_MIN_VERSION 2
#define INDEX_MIN_WITH_SYNONYMS_INT_GROUP_ID 16

// Those versions contains doc table as array, we modified it to be array of linked lists
// todo: decide if we need to keep this, currently I keep it if one day we will find a way to
//       load old rdb versions
#define INDEX_MIN_COMPACTED_DOCTABLE_VERSION 12

// Versions below this always store the frequency
#define INDEX_MIN_NOFREQ_VERSION 6
// Versions below this encode field ids as the actual value,
// above - field ides are encoded as their exponent (bit offset)
#define INDEX_MIN_WIDESCHEMA_VERSION 7

// Versions below this didn't know tag indexes
#define INDEX_MIN_TAGFIELD_VERSION 8

// Versions below this one don't save the document len when serializing the table
#define INDEX_MIN_DOCLEN_VERSION 9

#define INDEX_MIN_BINKEYS_VERSION 10

// Versions below this one do not contains expire information
#define INDEX_MIN_EXPIRE_VERSION 13

// Versions below this contain legacy types; newer versions allow a field
// to contain multiple types
#define INDEX_MIN_MULTITYPE_VERSION 14

#define INDEX_MIN_ALIAS_VERSION 15

#define IDXFLD_LEGACY_FULLTEXT 0
#define IDXFLD_LEGACY_NUMERIC 1
#define IDXFLD_LEGACY_GEO 2
#define IDXFLD_LEGACY_TAG 3
#define IDXFLD_LEGACY_MAX 3

#define Index_SupportsHighlight(spec) \
  (((spec)->flags & Index_StoreTermOffsets) && ((spec)->flags & Index_StoreByteOffsets))

#define FIELD_BIT(fs) (((t_fieldMask)1) << (fs)->ftId)

typedef struct {
  RedisModuleString *types[INDEXFLD_NUM_TYPES];
} IndexSpecFmtStrings;

//---------------------------------------------------------------------------------------------

typedef struct IndexSpec {
  char *name;
  FieldSpec *fields;
  int numFields;

  IndexStats stats;
  IndexFlags flags;

  Trie *terms;

  RSSortingTable *sortables;

  DocTable docs;

  StopWordList *stopwords;

  GCContext *gc;

  SynonymMap *smap;

  uint64_t uniqueId;

  // cached strings, corresponding to number of fields
  IndexSpecFmtStrings *indexStrs;
  struct IndexSpecCache *spcache;

  // For index expiretion
  long long timeout;
  RedisModuleTimerID timerId;
  bool isTimerSet;

  dict *keysDict;
  RSGetValueCallback getValue;
  void *getValueCtx;
  char **aliases;  // Aliases to self-remove when the index is deleted
  struct DocumentIndexer *indexer;

  SchemaRule *rule;

  struct IndexesScanner *scanner;
  // can be true even if scanner == NULL, in case of a scan being cancelled
  // in favor on a newer, pending scan
  bool scan_in_progress;
  bool cascadeDelete;  // remove keys when removing spec
} IndexSpec;

typedef enum SpecOp { SpecOp_Add, SpecOp_Del } SpecOp;

typedef struct SpecOpCtx {
  IndexSpec *spec;
  SpecOp op;
} SpecOpCtx;

typedef struct SpecOpIndexingCtx {
  dict *specs;
  SpecOpCtx *specsOps;
} SpecOpIndexingCtx;

typedef struct {
  void (*dtor)(void *p);
  void *p;
} KeysDictValue;

extern RedisModuleType *IndexSpecType;
extern RedisModuleType *IndexAliasType;

/**
 * This lightweight object contains a COPY of the actual index spec.
 * This makes it safe for other modules to use for information such as
 * field names, WITHOUT worrying about the index schema changing.
 *
 * If the index schema changes, this object is simply recreated rather
 * than modified, making it immutable.
 *
 * It is freed when its reference count hits 0
 */
typedef struct IndexSpecCache {
  FieldSpec *fields;
  size_t nfields;
  size_t refcount;
} IndexSpecCache;

/**
 * For testing only
 */
void Spec_AddToDict(const IndexSpec *spec);

/**
 * compare redis versions
 */
int CompareVestions(Version v1, Version v2);

/**
 * Retrieves the current spec cache from the index, incrementing its
 * reference count by 1. Use IndexSpecCache_Decref to free
 */
IndexSpecCache *IndexSpec_GetSpecCache(const IndexSpec *spec);

/**
 * Decrement the reference count of the spec cache. Should be matched
 * with a previous call of GetSpecCache()
 */
void IndexSpecCache_Decref(IndexSpecCache *cache);

/**
 * Create a new copy of the spec cache from the current index spec
 */
IndexSpecCache *IndexSpec_BuildSpecCache(const IndexSpec *spec);

/*
 * Get a field spec by field name. Case insensitive!
 * Return the field spec if found, NULL if not
 */
const FieldSpec *IndexSpec_GetField(const IndexSpec *spec, const char *name, size_t len);

const char *IndexSpec_GetFieldNameByBit(const IndexSpec *sp, t_fieldMask id);

/* Get the field bitmask id of a text field by name. Return 0 if the field is not found or is not a
 * text field */
t_fieldMask IndexSpec_GetFieldBit(IndexSpec *spec, const char *name, size_t len);

/**
 * Check if phonetic matching is enabled on any field within the fieldmask.
 * Returns true if any field has phonetics, and false if none of the fields
 * require it.
 */
int IndexSpec_CheckPhoneticEnabled(const IndexSpec *sp, t_fieldMask fm);

/* Get a sortable field's sort table index by its name. return -1 if the field was not found or is
 * not sortable */
int IndexSpec_GetFieldSortingIndex(IndexSpec *sp, const char *name, size_t len);

/**
 * Get the field spec from the sortable index
 */
const FieldSpec *IndexSpec_GetFieldBySortingIndex(const IndexSpec *sp, uint16_t idx);

/* Initialize some index stats that might be useful for scoring functions */
void IndexSpec_GetStats(IndexSpec *sp, RSIndexStats *stats);

/*
 * Parse an index spec from redis command arguments.
 * Returns REDISMODULE_ERR if there's a parsing error.
 * The command only receives the relvant part of argv.
 *
 * The format currently is <field> <weight>, <field> <weight> ...
 */
IndexSpec *IndexSpec_ParseRedisArgs(RedisModuleCtx *ctx, RedisModuleString *name,
                                    RedisModuleString **argv, int argc, QueryError *status);

FieldSpec **getFieldsByType(IndexSpec *spec, FieldType type);
int isRdbLoading(RedisModuleCtx *ctx);

/* Create a new index spec from redis arguments, set it in a redis key and start its GC.
 * If an error occurred - we set an error string in err and return NULL.
 */
IndexSpec *IndexSpec_CreateNew(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                               QueryError *status);

/* Start the garbage collection loop on the index spec */
void IndexSpec_StartGC(RedisModuleCtx *ctx, IndexSpec *sp, float initialHZ);
void IndexSpec_StartGCFromSpec(IndexSpec *sp, float initialHZ, uint32_t gcPolicy);

/* Same as above but with ordinary strings, to allow unit testing */
IndexSpec *IndexSpec_Parse(const char *name, const char **argv, int argc, QueryError *status);
FieldSpec *IndexSpec_CreateField(IndexSpec *sp, const char *name, const char *path);

/**
 * Indicate that the index spec should use an internal dictionary,rather than
 * the Redis keyspace
 */
void IndexSpec_MakeKeyless(IndexSpec *sp);

#define IndexSpec_IsKeyless(sp) ((sp)->keysDict != NULL)

void IndexesScanner_Cancel(struct IndexesScanner *scanner, bool still_in_progress);
void IndexSpec_ScanAndReindex(RedisModuleCtx *ctx, IndexSpec *sp);

/**
 * Gets the next text id from the index. This does not currently
 * modify the index
 */
int IndexSpec_CreateTextId(const IndexSpec *sp);

/* Add fields to a redis schema */
int IndexSpec_AddFields(IndexSpec *sp, RedisModuleCtx *ctx, ArgsCursor *ac, bool initialScan,
                        QueryError *status);

//---------------------------------------------------------------------------------------------

IndexSpec *IndexSpec_Load(RedisModuleCtx *ctx, const char *name, int openWrite);

/** Load the index as writeable */
#define INDEXSPEC_LOAD_WRITEABLE 0x01
/** Don't consult the alias table when retrieving the index */
#define INDEXSPEC_LOAD_NOALIAS 0x02
/** The name of the index is in the format of a redis string */
#define INDEXSPEC_LOAD_KEY_RSTRING 0x04

/**
 * The redis string is formatted, and is not the "plain" index name.
 * Impliest RSTRING
 */
#define INDEXSPEC_LOAD_KEY_FORMATTED 0x08

/**
 * Don't load or return the key. Should only be used in cases where the
 * spec is not persisted between threads
 */
#define INDEXSPEC_LOAD_KEYLESS 0x10

#define INDEXSPEC_LOAD_NOTIMERUPDATE 0x20

typedef struct {
  uint32_t flags;
  union {
    const char *cstring;
    RedisModuleString *rstring;
  } name;

  /** key pointer. you should close this when done with the index */
  RedisModuleKey *keyp;
  /** name of alias lookup key to use */
  const char *alookup;
} IndexLoadOptions;

/**
 * Find and load the index using the specified parameters.
 * @return the index spec, or NULL if the index does not exist
 */
IndexSpec *IndexSpec_LoadEx(RedisModuleCtx *ctx, IndexLoadOptions *options);

//---------------------------------------------------------------------------------------------

// Global hook called when an index spec is created
extern void (*IndexSpec_OnCreate)(const IndexSpec *sp);

int IndexSpec_AddTerm(IndexSpec *sp, const char *term, size_t len);

/* Get a random term from the index spec using weighted random. Weighted random is done by sampling
 * N terms from the index and then doing weighted random on them. A sample size of 10-20 should be
 * enough */
char *IndexSpec_GetRandomTerm(IndexSpec *sp, size_t sampleSize);

/*
 * Free an indexSpec.
 */
void IndexSpec_Free(IndexSpec *spec);
void IndexSpec_FreeInternals(IndexSpec *spec);

/**
 * Free the index synchronously. Any keys associated with the index (but not the
 * documents themselves) are freed before this function returns.
 */
void IndexSpec_FreeSync(IndexSpec *spec);

/* Parse a new stopword list and set it. If the parsing fails we revert to the default stopword
 * list, and return 0 */
int IndexSpec_ParseStopWords(IndexSpec *sp, RedisModuleString **strs, size_t len);

/* Return 1 if a term is a stopword for the specific index */
int IndexSpec_IsStopWord(IndexSpec *sp, const char *term, size_t len);

/** Returns a string suitable for indexes. This saves on string creation/destruction */
RedisModuleString *IndexSpec_GetFormattedKey(IndexSpec *sp, const FieldSpec *fs, FieldType forType);
RedisModuleString *IndexSpec_GetFormattedKeyByName(IndexSpec *sp, const char *s, FieldType forType);

IndexSpec *NewIndexSpec(const char *name);
int IndexSpec_AddField(IndexSpec *sp, FieldSpec *fs);
int IndexSpec_RdbLoad(RedisModuleIO *rdb, int encver, int when);
void IndexSpec_RdbSave(RedisModuleIO *rdb, int when);
void IndexSpec_Digest(RedisModuleDigest *digest, void *value);
int CompareVestions(Version v1, Version v2);
int IndexSpec_RegisterType(RedisModuleCtx *ctx);
// int IndexSpec_UpdateWithHash(IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString *key);
void IndexSpec_ClearAliases(IndexSpec *sp);

/*
 * Parse the field mask passed to a query, map field names to a bit mask passed down to the
 * execution engine, detailing which fields the query works on. See FT.SEARCH for API details
 */
t_fieldMask IndexSpec_ParseFieldMask(IndexSpec *sp, RedisModuleString **argv, int argc);

void IndexSpec_InitializeSynonym(IndexSpec *sp);

//---------------------------------------------------------------------------------------------

typedef struct IndexesScanner {
  bool global;
  IndexSpec *spec;
  size_t scannedKeys, totalKeys;
  bool cancelled;
} IndexesScanner;

//---------------------------------------------------------------------------------------------

void Indexes_Init(RedisModuleCtx *ctx);
void Indexes_Free(dict *d);
void Indexes_UpdateMatchingWithSchemaRules(RedisModuleCtx *ctx, RedisModuleString *key, DocumentType type,
                                           RedisModuleString **hashFields);
void Indexes_DeleteMatchingWithSchemaRules(RedisModuleCtx *ctx, RedisModuleString *key,
                                           RedisModuleString **hashFields);
void Indexes_ReplaceMatchingWithSchemaRules(RedisModuleCtx *ctx, RedisModuleString *from_key,
                                            RedisModuleString *to_key);

///////////////////////////////////////////////////////////////////////////////////////////////

#ifdef __cplusplus
}
#endif

#endif  // __SPEC_H__

/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

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
#include "delimiters.h"
#include "gc.h"
#include "synonym_map.h"
#include "query_error.h"
#include "field_spec.h"
#include "util/dict.h"
#include "util/references.h"
#include "redisearch_api.h"
#include "rules.h"
#include <pthread.h>

#ifdef __cplusplus
extern "C" {
#endif

///////////////////////////////////////////////////////////////////////////////////////////////

struct IndexesScanner;
struct DocumentIndexer;

#define SPEC_GEO_STR "GEO"
#define SPEC_GEOMETRY_STR "GEOSHAPE"
#define SPEC_TAG_STR "TAG"
#define SPEC_TEXT_STR "TEXT"
#define SPEC_VECTOR_STR "VECTOR"
#define SPEC_NUMERIC_STR "NUMERIC"

#define SPEC_NOOFFSETS_STR "NOOFFSETS"
#define SPEC_NOFIELDS_STR "NOFIELDS"
#define SPEC_NOFREQS_STR "NOFREQS"
#define SPEC_NOHL_STR "NOHL"
#define SPEC_SCHEMA_STR "SCHEMA"
#define SPEC_SCHEMA_EXPANDABLE_STR "MAXTEXTFIELDS"
#define SPEC_TEMPORARY_STR "TEMPORARY"
#define SPEC_AS_STR "AS"
#define SPEC_WEIGHT_STR "WEIGHT"
#define SPEC_NOSTEM_STR "NOSTEM"
#define SPEC_PHONETIC_STR "PHONETIC"
#define SPEC_SORTABLE_STR "SORTABLE"
#define SPEC_UNF_STR "UNF"
#define SPEC_STOPWORDS_STR "STOPWORDS"
#define SPEC_SET_DELIMITERS_STR "DELIMITERS"
#define SPEC_ADD_DELIMITERS_STR "DELIMITERS+"
#define SPEC_DEL_DELIMITERS_STR "DELIMITERS-"
#define SPEC_NOINDEX_STR "NOINDEX"
#define SPEC_TAG_SEPARATOR_STR "SEPARATOR"
#define SPEC_TAG_CASE_SENSITIVE_STR "CASESENSITIVE"
#define SPEC_MULTITYPE_STR "MULTITYPE"
#define SPEC_ASYNC_STR "ASYNC"
#define SPEC_SKIPINITIALSCAN_STR "SKIPINITIALSCAN"
#define SPEC_WITHSUFFIXTRIE_STR "WITHSUFFIXTRIE"

#define SPEC_GEOMETRY_FLAT_STR "FLAT"
#define SPEC_GEOMETRY_SPHERE_STR "SPHERICAL"

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
#define dictGetRef(he) ((StrongRef){dictGetVal(he)})

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
  long double totalIndexTime; // usec
  size_t totalDocsLen;
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
  Index_HasFieldAlias = 0x4000,
  Index_HasVecSim = 0x8000,
  Index_HasSuffixTrie = 0x10000,
  // If any of the fields has undefined order. This is just a cache for quick lookup
  Index_HasUndefinedOrder = 0x20000,

  Index_HasGeometry = 0x40000,

  Index_HasCustomDelimiters = 0x80000,
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

#define INDEX_CURRENT_VERSION 24
#define INDEX_DELIMITERS_VERSION 24
#define INDEX_GEOMETRY_VERSION 23
#define INDEX_VECSIM_TIERED_VERSION 22
#define INDEX_VECSIM_MULTI_VERSION 21
#define INDEX_VECSIM_2_VERSION 20
#define INDEX_VECSIM_VERSION 19
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

#define Index_StoreFieldMask(spec) \
  ((spec)->flags & Index_StoreFieldFlags)

#define FIELD_BIT(fs) (((t_fieldMask)1) << (fs)->ftId)

typedef struct {
  RedisModuleString *types[INDEXFLD_NUM_TYPES];
} IndexSpecFmtStrings;

//---------------------------------------------------------------------------------------------


typedef struct IndexSpec {
  char *name;                     // Index name
  size_t nameLen;                 // Index name length
  uint64_t uniqueId;              // Id of index
  FieldSpec *fields;              // Fields in the index schema
  int numFields;                  // Number of fields

  IndexStats stats;               // Statistics of memory used and quantities
  IndexFlags flags;               // Flags

  Trie *terms;                    // Trie of all terms. Used for GC and fuzzy queries
  Trie *suffix;                   // Trie of suffix tokens of terms. Used for contains queries
  t_fieldMask suffixMask;         // Mask of all field that support contains query
  dict *keysDict;                 // Global dictionary. Contains inverted indexes of all TEXT TAG NUMERIC VECTOR and GEOSHAPE terms

  RSSortingTable *sortables;      // Contains sortable data of documents

  DocTable docs;                  // Contains metadata of all documents

  StopWordList *stopwords;        // List of stopwords for TEXT fields

  DelimiterList *delimiters;      // Delimiter character list

  GCContext *gc;                  // Garbage collection

  SynonymMap *smap;               // List of synonym
  char **aliases;                 // Aliases to self-remove when the index is deleted

  struct SchemaRule *rule;        // Contains schema rules for follow-the-hash/JSON
  struct IndexesScanner *scanner; // Scans new hash/JSON documents or rescan
  // can be true even if scanner == NULL, in case of a scan being cancelled
  // in favor on a newer, pending scan
  bool scan_in_progress;
  bool cascadeDelete;             // (deprecated) remove keys when removing spec. used by temporary index

  struct DocumentIndexer *indexer;// Indexer of fields into inverted indexes

  // cached strings, corresponding to number of fields
  IndexSpecFmtStrings *indexStrs;
  struct IndexSpecCache *spcache;
  // For index expiration
  long long timeout;
  RedisModuleTimerID timerId;
  bool isTimerSet;

  // bitarray of dialects used by this index
  uint_least8_t used_dialects;

  // For criteria tester
  RSGetValueCallback getValue;
  void *getValueCtx;

  // Count the number of times the index was used
  long long counter;

  // read write lock
  pthread_rwlock_t rwlock;

  // Cursors counters
  size_t cursorsCap;
  size_t activeCursors;

  // Quick access to the spec's strong ref
  StrongRef own_ref;
} IndexSpec;

typedef enum SpecOp { SpecOp_Add, SpecOp_Del } SpecOp;
typedef enum TimerOp { TimerOp_Add, TimerOp_Del } TimerOp;

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
void Spec_AddToDict(RefManager *w_spec);

/**
 * Compare redis versions
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
 * Can handle NULL
 */
void IndexSpecCache_Decref(IndexSpecCache *cache);

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

/**
 * Check that `slop` and/or `inorder` are allowed on all fields matching the fieldmask (e.g., fields cannot have undefined ordering)
 * (`RS_FIELDMASK_ALL` fieldmask checks all fields)
 * Returns true if allowed, and false otherwise.
 * If not allowed, set error message in status.
 */
int IndexSpec_CheckAllowSlopAndInorder(const IndexSpec *sp, t_fieldMask fm, QueryError *status);

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
StrongRef IndexSpec_ParseRedisArgs(RedisModuleCtx *ctx, RedisModuleString *name,
                                   RedisModuleString **argv, int argc, QueryError *status);

arrayof(FieldSpec *) getFieldsByType(IndexSpec *spec, FieldType type);
int isRdbLoading(RedisModuleCtx *ctx);

/* Create a new index spec from redis arguments, set it in a redis key and start its GC.
 * If an error occurred - we set an error string in err and return NULL.
 */
IndexSpec *IndexSpec_CreateNew(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                               QueryError *status);

/* Start the garbage collection loop on the index spec */
void IndexSpec_StartGC(RedisModuleCtx *ctx, StrongRef spec_ref, IndexSpec *sp);
void IndexSpec_StartGCFromSpec(StrongRef spec_ref, IndexSpec *sp, uint32_t gcPolicy);

/* Same as above but with ordinary strings, to allow unit testing */
StrongRef IndexSpec_Parse(const char *name, const char **argv, int argc, QueryError *status);
FieldSpec *IndexSpec_CreateField(IndexSpec *sp, const char *name, const char *path);

// This function locks the spec for writing. use it if you know the spec is not locked
int IndexSpec_DeleteDoc(IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString *key);

// This function does not lock the spec. use it if you know the spec is locked for writing
void IndexSpec_DeleteDoc_Unsafe(IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString *key, t_docId id);

/**
 * Indicate that the index spec should use an internal dictionary,rather than
 * the Redis keyspace
 */
void IndexSpec_MakeKeyless(IndexSpec *sp);

#define IndexSpec_IsKeyless(sp) ((sp)->keysDict != NULL)

void IndexesScanner_Cancel(struct IndexesScanner *scanner);
void IndexSpec_ScanAndReindex(RedisModuleCtx *ctx, StrongRef ref);
#ifdef FTINFO_FOR_INFO_MODULES
/**
 * Exposing all the fields of the index to INFO command.
 */
void IndexSpec_AddToInfo(RedisModuleInfoCtx *ctx, IndexSpec *sp);
#endif

/**
 * Get the total memory usage of all the vector fields in the index (in bytes).
 */
size_t IndexSpec_VectorIndexSize(IndexSpec *sp);

/**
 * Gets the next text id from the index. This does not currently
 * modify the index
 */
int IndexSpec_CreateTextId(const IndexSpec *sp);

/* Add fields to a redis schema */
int IndexSpec_AddFields(StrongRef ref, IndexSpec *sp, RedisModuleCtx *ctx, ArgsCursor *ac, bool initialScan,
                        QueryError *status);

/**
 * Checks that the given parameters pass memory limits (used while starting from RDB)
 */
int VecSimIndex_validate_params(RedisModuleCtx *ctx, VecSimParams *params, QueryError *status);

//---------------------------------------------------------------------------------------------

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

//---------------------------------------------------------------------------------------------

/**
 * Find and load the index using the specified parameters.
 * @return the strong reference to the index spec owned by RediSearch (a borrow), or NULL if the index does not exist.
 * If an owned reference is needed, use StrongRef API to create one.
 */
StrongRef IndexSpec_LoadUnsafe(RedisModuleCtx *ctx, const char *name, int openWrite);

/**
 * Find and load the index using the specified parameters. The call does not increase the spec reference counter
 * (only the weak reference counter).
 * @return the index spec, or NULL if the index does not exist
 */
StrongRef IndexSpec_LoadUnsafeEx(RedisModuleCtx *ctx, IndexLoadOptions *options);

/**
 * Quick access to the spec's strong reference. This function should be called only if
 * the spec is valid and protected (by the GIL or the spec's lock).
 * The call does not increase the spec's reference counters.
 * @return a strong reference to the spec.
 */
StrongRef IndexSpec_GetStrongRefUnsafe(const IndexSpec *spec);

/**
 * @brief Removes the spec from the global data structures
 *
 * @param ref a strong reference to the spec
 */
void IndexSpec_RemoveFromGlobals(StrongRef ref);

/*
 * Free an indexSpec. For LLAPI
 */
void IndexSpec_Free(IndexSpec *spec);

//---------------------------------------------------------------------------------------------

int IndexSpec_AddTerm(IndexSpec *sp, const char *term, size_t len);

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
void IndexSpec_ClearAliases(StrongRef ref);

void IndexSpec_InitializeSynonym(IndexSpec *sp);
void Indexes_SetTempSpecsTimers(TimerOp op);

//---------------------------------------------------------------------------------------------

typedef struct IndexesScanner {
  bool global;
  bool cancelled;
  WeakRef spec_ref;
  char *spec_name;
  size_t scannedKeys, totalKeys;
} IndexesScanner;

double IndexesScanner_IndexedPercent(IndexesScanner *scanner, IndexSpec *sp);

//---------------------------------------------------------------------------------------------

void Indexes_Init(RedisModuleCtx *ctx);
void Indexes_Free(dict *d);
void Indexes_UpdateMatchingWithSchemaRules(RedisModuleCtx *ctx, RedisModuleString *key, DocumentType type,
                                           RedisModuleString **hashFields);
void Indexes_DeleteMatchingWithSchemaRules(RedisModuleCtx *ctx, RedisModuleString *key,
                                           RedisModuleString **hashFields);
void Indexes_ReplaceMatchingWithSchemaRules(RedisModuleCtx *ctx, RedisModuleString *from_key,
                                            RedisModuleString *to_key);

//---------------------------------------------------------------------------------------------

void CleanPool_ThreadPoolStart();
void CleanPool_ThreadPoolDestroy();
size_t CleanInProgressOrPending();

///////////////////////////////////////////////////////////////////////////////////////////////

#ifdef __cplusplus
}
#endif

#endif  // __SPEC_H__

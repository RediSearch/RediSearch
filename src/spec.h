/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef __SPEC_H__
#define __SPEC_H__

#include <stdlib.h>
#include <string.h>

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
#include "util/references.h"
#include "redisearch_api.h"
#include "rules.h"
#include <pthread.h>
#include "info/index_error.h"
#include "obfuscation/hidden.h"

#ifdef __cplusplus
extern "C" {
#endif

///////////////////////////////////////////////////////////////////////////////////////////////

struct IndexesScanner;

// Initial capacity (in bytes) of a new block
#define INDEX_BLOCK_INITIAL_CAP 6

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
#define SPEC_NOINDEX_STR "NOINDEX"
#define SPEC_TAG_SEPARATOR_STR "SEPARATOR"
#define SPEC_TAG_CASE_SENSITIVE_STR "CASESENSITIVE"
#define SPEC_MULTITYPE_STR "MULTITYPE"
#define SPEC_ASYNC_STR "ASYNC"
#define SPEC_SKIPINITIALSCAN_STR "SKIPINITIALSCAN"
#define SPEC_WITHSUFFIXTRIE_STR "WITHSUFFIXTRIE"
#define SPEC_INDEXEMPTY_STR "INDEXEMPTY"
#define SPEC_INDEXMISSING_STR "INDEXMISSING"
#define SPEC_INDEXALL_STR "INDEXALL"

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
       .type = AC_ARGTYPE_STRING},                                          \
      {.name = SPEC_INDEXALL_STR,                                           \
       .target = &(rule)->index_all,                                        \
       .len = &dummy2,                                                      \
       .type = AC_ARGTYPE_STRING},

// TODO: remove usage of keyspace prefix now that RediSearch is out of keyspace
#define INDEX_SPEC_KEY_PREFIX "idx:"
#define INDEX_SPEC_KEY_FMT INDEX_SPEC_KEY_PREFIX "%s"

#define SPEC_MAX_FIELDS 1024
#define SPEC_MAX_FIELD_ID (sizeof(t_fieldMask) * 8)

// The threshold after which we move to a special encoding for wide fields
#define SPEC_WIDEFIELD_THRESHOLD 32

#define MIN_DIALECT_VERSION 1 // MIN_DIALECT_VERSION is expected to change over time as dialects become deprecated.
#define MAX_DIALECT_VERSION 4 // MAX_DIALECT_VERSION may not exceed MIN_DIALECT_VERSION + 7.

extern dict *specDict_g;
#define dictGetRef(he) ((StrongRef){dictGetVal(he)})

typedef enum {
    DEBUG_INDEX_SCANNER_CODE_NEW,
    DEBUG_INDEX_SCANNER_CODE_RUNNING,
    DEBUG_INDEX_SCANNER_CODE_DONE,
    DEBUG_INDEX_SCANNER_CODE_CANCELLED,
    DEBUG_INDEX_SCANNER_CODE_PAUSED,
    DEBUG_INDEX_SCANNER_CODE_RESUMED,
    DEBUG_INDEX_SCANNER_CODE_PAUSED_ON_OOM,
    DEBUG_INDEX_SCANNER_CODE_PAUSED_BEFORE_OOM_RETRY,

    //Insert new codes here (before COUNT)
    DEBUG_INDEX_SCANNER_CODE_COUNT  // Helps with array size checks
    //Do not add new codes after COUNT
} DebugIndexScannerCode;

extern const char *DEBUG_INDEX_SCANNER_STATUS_STRS[];

extern size_t pending_global_indexing_ops;
extern struct IndexesScanner *global_spec_scanner;
extern dict *legacySpecRules;

typedef struct {
  size_t numDocuments;
  size_t numTerms;
  size_t numRecords;
  size_t invertedSize;
  size_t offsetVecsSize;
  size_t offsetVecRecords;
  size_t termsSize;
  size_t totalIndexTime;
  IndexError indexError;
  size_t totalDocsLen;
  uint32_t activeQueries;
  uint32_t activeWrites;
} IndexStats;

typedef enum {
  Index_StoreTermOffsets = 0x01,
  Index_StoreFieldFlags = 0x02,
  Index_HasMultiValue = 0x04,
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

  Index_HasNonEmpty = 0x80000,  // Index has at least one field that does not indexes empty values

} IndexFlags;

// redis version (its here because most file include it with no problem,
// we should introduce proper common.h file)

typedef struct Version {
  int majorVersion;
  int minorVersion;
  int patchVersion;
  int buildVersion;  // if not exits then its zero
} Version;

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

#define INDEX_CURRENT_VERSION 25
#define INDEX_VECSIM_SVS_VAMANA_VERSION 25
#define INDEX_INDEXALL_VERSION 24
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

// Forward declaration
typedef struct InvertedIndex InvertedIndex;

typedef struct IndexSpec {
  const HiddenString *specName;         // Index private name
  char *obfuscatedName;           // Index hashed name
  FieldSpec *fields;              // Fields in the index schema
  int16_t numFields;              // Number of fields
  int16_t numSortableFields;      // Number of sortable fields

  IndexFlags flags;               // Flags
  IndexStats stats;               // Statistics of memory used and quantities

  Trie *terms;                    // Trie of all TEXT terms. Used for GC and fuzzy queries
  Trie *suffix;                   // Trie of TEXT suffix tokens of terms. Used for contains queries
  t_fieldMask suffixMask;         // Mask of all fields that support contains query
  dict *keysDict;                 // Global dictionary. Contains inverted indexes of all TEXT TAG NUMERIC VECTOR and GEOSHAPE terms

  DocTable docs;                  // Contains metadata of all documents

  StopWordList *stopwords;        // List of stopwords for TEXT fields

  GCContext *gc;                  // Garbage collection

  SynonymMap *smap;               // List of synonym
  HiddenString **aliases;         // Aliases to self-remove when the index is deleted

  struct SchemaRule *rule;        // Contains schema rules for follow-the-hash/JSON
  struct IndexesScanner *scanner; // Scans new hash/JSON documents or rescan
  // can be true even if scanner == NULL, in case of a scan being cancelled
  // in favor on a newer, pending scan
  bool scan_in_progress;
  bool scan_failed_OOM; // background indexing failed due to Out Of Memory
  bool monitorDocumentExpiration;
  bool monitorFieldExpiration;
  bool isDuplicate;               // Marks that this index is a duplicate of an existing one

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
  size_t activeCursors;

  // Quick access to the spec's strong ref
  StrongRef own_ref;

  // Contains inverted indexes of missing fields
  dict *missingFieldDict;
  // Maps between field ftid and field index in the fields array
  arrayof(t_fieldIndex) fieldIdToIndex;

  // Contains all the existing documents (for wildcard search)
  InvertedIndex *existingDocs;

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

static inline void IndexSpec_IncrActiveQueries(IndexSpec *sp) {
  __atomic_add_fetch(&sp->stats.activeQueries, 1, __ATOMIC_RELAXED);
}
static inline void IndexSpec_DecrActiveQueries(IndexSpec *sp) {
  __atomic_sub_fetch(&sp->stats.activeQueries, 1, __ATOMIC_RELAXED);
}
static inline uint32_t IndexSpec_GetActiveQueries(IndexSpec *sp) {
  return __atomic_load_n(&sp->stats.activeQueries, __ATOMIC_RELAXED);
}

static inline void IndexSpec_IncrActiveWrites(IndexSpec *sp) {
  __atomic_add_fetch(&sp->stats.activeWrites, 1, __ATOMIC_RELAXED);
}
static inline void IndexSpec_DecrActiveWrites(IndexSpec *sp) {
  __atomic_sub_fetch(&sp->stats.activeWrites, 1, __ATOMIC_RELAXED);
}
static inline uint32_t IndexSpec_GetActiveWrites(IndexSpec *sp) {
  return __atomic_load_n(&sp->stats.activeWrites, __ATOMIC_RELAXED);
}

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
int CompareVersions(Version v1, Version v2);

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
const FieldSpec *IndexSpec_GetField(const IndexSpec *spec, const HiddenString *name);
const FieldSpec *IndexSpec_GetFieldWithLength(const IndexSpec *spec, const char* name, size_t len);

const char *IndexSpec_GetFieldNameByBit(const IndexSpec *sp, t_fieldMask id);

/*
* Get a field spec by field mask.
* Return the field spec if found, NULL if not
*/
const FieldSpec *IndexSpec_GetFieldByBit(const IndexSpec *sp, t_fieldMask id);

/**
 * Get the field specs in the field mask `mask`.
 */
arrayof(FieldSpec *) IndexSpec_GetFieldsByMask(const IndexSpec *sp, t_fieldMask mask);

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

/* Get the number of indexing failures */
size_t IndexSpec_GetIndexErrorCount(const IndexSpec *sp);

/*
 * Parse an index spec from redis command arguments.
 * Returns REDISMODULE_ERR if there's a parsing error.
 * The command only receives the relevant part of argv.
 *
 * The format currently is <field> <weight>, <field> <weight> ...
 */
StrongRef IndexSpec_ParseRedisArgs(RedisModuleCtx *ctx, const HiddenString *name,
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
StrongRef IndexSpec_Parse(const HiddenString *name, const char **argv, int argc, QueryError *status);
// Calls IndexSpec_Parse after wrapping name with a hidden string
StrongRef IndexSpec_ParseC(const char *name, const char **argv, int argc, QueryError *status);

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
void IndexesScanner_ResetProgression(struct IndexesScanner *scanner);

void IndexSpec_ScanAndReindex(RedisModuleCtx *ctx, StrongRef ref);
#ifdef FTINFO_FOR_INFO_MODULES
/**
 * Exposing all the fields of the index to INFO command.
 */
void IndexSpec_AddToInfo(RedisModuleInfoCtx *ctx, IndexSpec *sp);
#endif

/**
 * Gets the next text id from the index. This does not currently
 * modify the index
 */
int IndexSpec_CreateTextId(IndexSpec *sp, t_fieldIndex index);

/* Add fields to a redis schema */
int IndexSpec_AddFields(StrongRef ref, IndexSpec *sp, RedisModuleCtx *ctx, ArgsCursor *ac, bool initialScan,
                        QueryError *status);

// Translate the field mask to an array of field indices based on the "on" bits
// Out capacity should be enough to hold 128 fields
uint16_t IndexSpec_TranslateMaskToFieldIndices(const IndexSpec *sp, t_fieldMask mask, t_fieldIndex *out);

/**
 * Checks that the given parameters pass memory limits (used while starting from RDB)
 */
int VecSimIndex_validate_params(RedisModuleCtx *ctx, VecSimParams *params, QueryError *status);

//---------------------------------------------------------------------------------------------

typedef enum {
  INDEXSPEC_LOAD_NOALIAS = 0x01,      // Don't consult the alias table when retrieving the index
  INDEXSPEC_LOAD_KEY_RSTRING = 0x02,  // The name of the index is in the format of a redis string
  INDEXSPEC_LOAD_NOTIMERUPDATE = 0x04,
  INDEXSPEC_LOAD_NOCOUNTERINC = 0x08,     // Don't increment the (usage) counter of the index
} IndexLoadOptionsFlags;

typedef struct {
  union {
    const char *nameC;
    RedisModuleString *nameR;
  };
  IndexLoadOptionsFlags flags;
} IndexLoadOptions;

//---------------------------------------------------------------------------------------------

/**
 * Find and load the index using the specified parameters.
 * @return the strong reference to the index spec owned by RediSearch (a borrow), or NULL if the index does not exist.
 * If an owned reference is needed, use StrongRef API to create one.
 */
// TODO: Remove the context from this function!
StrongRef IndexSpec_LoadUnsafe(const char *name);

/**
 * Find and load the index using the specified parameters. The call does not increase the spec reference counter
 * (only the weak reference counter).
 * @return the index spec, or NULL if the index does not exist
 */
StrongRef IndexSpec_LoadUnsafeEx(IndexLoadOptions *options);

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
 * @param removeActive - should we call CurrentThread_ClearIndexSpec on the released spec
 */
void IndexSpec_RemoveFromGlobals(StrongRef spec_ref, bool removeActive);

/*
 * Free an indexSpec. For LLAPI
 */
void IndexSpec_Free(IndexSpec *spec);

//---------------------------------------------------------------------------------------------

void IndexSpec_AddTerm(IndexSpec *sp, const char *term, size_t len);

/** Returns a string suitable for indexes. This saves on string creation/destruction */
RedisModuleString *IndexSpec_GetFormattedKey(IndexSpec *sp, const FieldSpec *fs, FieldType forType);
RedisModuleString *IndexSpec_GetFormattedKeyByName(IndexSpec *sp, const char *s, FieldType forType);

IndexSpec *NewIndexSpec(const HiddenString *name);
int IndexSpec_AddField(IndexSpec *sp, FieldSpec *fs);
int IndexSpec_RdbLoad(RedisModuleIO *rdb, int encver, int when);
void IndexSpec_RdbSave(RedisModuleIO *rdb, int when);
void IndexSpec_Digest(RedisModuleDigest *digest, void *value);
int IndexSpec_RegisterType(RedisModuleCtx *ctx);
// int IndexSpec_UpdateWithHash(IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString *key);
void IndexSpec_ClearAliases(StrongRef ref);

void IndexSpec_InitializeSynonym(IndexSpec *sp);
void Indexes_SetTempSpecsTimers(TimerOp op);

//---------------------------------------------------------------------------------------------

typedef struct IndexesScanner {
  bool global;
  bool cancelled;
  bool isDebug;
  bool scanFailedOnOOM;
  WeakRef spec_ref;
  char *spec_name_for_logs;
  size_t scannedKeys;
  RedisModuleString *OOMkey; // The key that caused the OOM
} IndexesScanner;

typedef struct DebugIndexesScanner {
  IndexesScanner base;
  int maxDocsTBscanned;
  int maxDocsTBscannedPause;
  bool wasPaused;
  bool pauseOnOOM;
  int status;
  bool pauseBeforeOOMRetry;
} DebugIndexesScanner;


double IndexesScanner_IndexedPercent(RedisModuleCtx *ctx, IndexesScanner *scanner, const IndexSpec *sp);

/**
 * @return the overhead used by the TAG fields in `sp`, i.e., the size of the
 * TrieMaps used for the `values` and `suffix` fields.
 */
size_t IndexSpec_collect_tags_overhead(const IndexSpec *sp);

/**
 * @return the overhead used by the TEXT fields in `sp`, i.e., the size of the
 * sp->terms and sp->suffix Tries.
 */
size_t IndexSpec_collect_text_overhead(const IndexSpec *sp);

/**
 * @return the overhead used by the NUMERIC and GEO fields in `sp`, i.e., the accumulated size of all
 * numeric tree structs.
 */
size_t IndexSpec_collect_numeric_overhead(IndexSpec *sp);

/**
 * @return all memory used by the index `sp`.
 * Uses the sizes of the doc-table, tag and text overhead if they are not `0`.
 */
size_t IndexSpec_TotalMemUsage(IndexSpec *sp, size_t doctable_tm_size, size_t tags_overhead, size_t text_overhead);

/**
* obfuscate argument is used to determine how we will format the index name
* if obfuscate is true we will return the obfuscated name
* meant to allow us and the user to use the same commands with different outputs
* meaning we don't want to have access to the user data
* @return the formatted name of the index
*/
const char *IndexSpec_FormatName(const IndexSpec *sp, bool obfuscate);
char *IndexSpec_FormatObfuscatedName(const HiddenString *specName);

//---------------------------------------------------------------------------------------------

void Indexes_Init(RedisModuleCtx *ctx);
void Indexes_Free(dict *d);
void Indexes_UpdateMatchingWithSchemaRules(RedisModuleCtx *ctx, RedisModuleString *key, DocumentType type,
                                           RedisModuleString **hashFields);
void Indexes_DeleteMatchingWithSchemaRules(RedisModuleCtx *ctx, RedisModuleString *key,
                                           RedisModuleString **hashFields);
void Indexes_ReplaceMatchingWithSchemaRules(RedisModuleCtx *ctx, RedisModuleString *from_key,
                                            RedisModuleString *to_key);
void Indexes_List(RedisModule_Reply* reply, bool obfuscate);

//---------------------------------------------------------------------------------------------

void CleanPool_ThreadPoolStart();
void CleanPool_ThreadPoolDestroy();
size_t CleanInProgressOrPending();

// Expose reindexpool for debug
void ReindexPool_ThreadPoolDestroy();


///////////////////////////////////////////////////////////////////////////////////////////////

// Tries to promote a WeakRef of a spec to a StrongRef
// If a strong reference was obtained then we also set the current thread's active spec
StrongRef IndexSpecRef_Promote(WeakRef ref);
// Releases a strong reference to a spec
// Must only be called if the spec was promoted successfully
// Will also clear the current thread's active spec
void IndexSpecRef_Release(StrongRef ref);

#ifdef __cplusplus
}
#endif

#endif  // __SPEC_H__

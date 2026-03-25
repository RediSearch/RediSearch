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
#include "config.h"
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
#include "search_disk_api.h"
#include "rs_wall_clock.h"

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

// DebugIndexScannerCode, DEBUG_INDEX_SCANNER_STATUS_STRS, IndexesScanner,
// DebugIndexesScanner structs and scanner functions moved to spec_scanner.h.
#include "spec_scanner.h"

extern dict *legacySpecRules;

typedef struct {
  size_t numDocuments;
  size_t numTerms;
  size_t totalDocsLen;
} ScoringIndexStats;

typedef struct {
  ScoringIndexStats scoring;  // Statistics used for scoring functions
  size_t numRecords;
  size_t invertedSize;
  size_t offsetVecsSize;
  size_t offsetVecRecords;
  size_t termsSize;
  rs_wall_clock_ns_t totalIndexTime;
  IndexError indexError;
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
extern bool isTrimming; // TODO: remove this when redis deprecates sharding trimming events
extern bool isFlex;

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

#define INDEX_CURRENT_VERSION 26
#define INDEX_DISK_VERSION 26
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

//---------------------------------------------------------------------------------------------

// Forward declaration
typedef struct InvertedIndex InvertedIndex;
typedef const void* RedisSearchDiskIndexSpec;

typedef struct CharBuf {
  char *buf;
  size_t len;
} CharBuf;

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
  dict *keysDict;                 // Inverted indexes dictionary of all TEXT terms

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

  // cached fields, corresponding to number of fields
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

  // Disk index handle (NULL for memory-only indexes)
  RedisSearchDiskIndexSpec *diskSpec;
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

// IndexSpecCache — immutable, refcounted snapshot of index fields.
// Full definition and functions in spec_cache.h.
#include "spec_cache.h"

// Struct accessors, field lookups, format, version comparison
#include "spec_struct.h"

// IndexSpec_GetStats, IndexSpec_GetIndexErrorCount
#include "spec_info.h"

// Parsing from redis args
#include "spec_parse.h"

/* Add fields to a redis schema */
int IndexSpec_AddFields(StrongRef ref, IndexSpec *sp, RedisModuleCtx *ctx, ArgsCursor *ac, bool initialScan,
                        QueryError *status);

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

// Lifecycle: creation, destruction, mutation, GC, doc ops, compaction FFI
#include "spec_lifecycle.h"

int IndexSpec_AddField(IndexSpec *sp, FieldSpec *fs);
void IndexSpec_Digest(RedisModuleDigest *digest, void *value);

// RDB serialization, type registration, legacy index support, and
// serialize/deserialize/propagate moved to spec_rdb.h.
#include "spec_rdb.h"
// int IndexSpec_UpdateWithHash(IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString *key);
void IndexSpec_ClearAliases(StrongRef ref);

//---------------------------------------------------------------------------------------------

// Indexes_Init, Indexes_Free, Indexes_Count, Indexes_UpdateMatchingWithSchemaRules,
// Indexes_DeleteMatchingWithSchemaRules, Indexes_ReplaceMatchingWithSchemaRules,
// Indexes_List, Indexes_SetTempSpecsTimers, CleanPool_ThreadPoolStart,
// CleanPool_ThreadPoolDestroy, CleanInProgressOrPending,
// IndexSpec_CreateNew, IndexSpec_LoadUnsafe, IndexSpec_LoadUnsafeEx,
// IndexSpec_RemoveFromGlobals, Indexes_FindMatchingSchemaRules,
// Indexes_SpecOpsIndexingCtxFree moved to spec_registry.h
#include "spec_registry.h"

#ifdef __cplusplus
}
#endif

#endif  // __SPEC_H__

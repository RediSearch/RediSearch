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
#include "trie/trie.h"
#include "sortable.h"
#include "stopwords.h"
#include "gc.h"
#include "synonym_map.h"
#include "field_spec.h"
#include "util/dict.h"
#include "util/references.h"
#include "rules.h"
#include <pthread.h>
#include "info/index_error.h"
#include "obfuscation/hidden.h"
#include "search_disk_api.h"
#include "rs_wall_clock.h"

typedef struct QueryError QueryError;

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
#define MAX_SYNONYM_TERMS 1000000     // reasonable limit for synonym map terms
#define MAX_SYNONYM_GROUP_IDS 4096    // reasonable limit for group IDs per term

// The threshold after which we move to a special encoding for wide fields
#define SPEC_WIDEFIELD_THRESHOLD 32

#define MIN_DIALECT_VERSION 1 // MIN_DIALECT_VERSION is expected to change over time as dialects become deprecated.
#define MAX_DIALECT_VERSION 4 // MAX_DIALECT_VERSION may not exceed MIN_DIALECT_VERSION + 7.

// Generic helpers to read a StrongRef out of a dict entry. (The global index
// registry dictionaries specDict_g / specIdDict_g are declared in indexes.h.)
#define dictGetRef(he) ((StrongRef){dictGetVal(he)})
#define dictFetchRef(dict, key) ((StrongRef){dictFetchValue((dict), (key))})

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
  // Number of inverted-index blocks currently owned by this spec; reported by FT.INFO as
  // `total_inverted_index_blocks`. Writes must go through `IndexStats_BlockCountAdd` (signed
  // delta, atomic). Reads must use `__atomic_load_n`.
  size_t totalInvertedIndexBlocks;
  rs_wall_clock_ns_t totalIndexTime;
  IndexError indexError;
  uint32_t activeQueries;
  uint32_t activeWrites;
} IndexStats;

// Atomically apply `delta` to `stats->totalInvertedIndexBlocks`. Accepts signed deltas:
// negative values wrap via size_t two's-complement, producing the correct unsigned
// subtraction.
static inline void IndexStats_BlockCountAdd(IndexStats *stats, int64_t delta) {
  if (delta) {
    __atomic_add_fetch(&stats->totalInvertedIndexBlocks,
                       (size_t)delta, __ATOMIC_RELAXED);
  }
}

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
extern bool isEnterprise;
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

#define INDEX_CURRENT_VERSION 27
#define INDEX_VECTOR_RERANK_VERSION 27
#define INDEX_DISK_VERSION 26
#define INDEX_VECSIM_SVS_VAMANA_VERSION 25
#define INDEX_ASM_PROPAGATE_DEFINITIONS_VERSION INDEX_VECSIM_SVS_VAMANA_VERSION
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
#define FieldSpec_IsIndexableTextInMask(fs, fm) (FieldSpec_IsIndexableText(fs) && ((fm) & FIELD_BIT(fs)))

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
  uint64_t specId;                // Unique monotonically increasing ID for this spec incarnation
  FieldSpec *fields;              // Fields in the index schema
  uint16_t numFields;             // Number of fields
  uint16_t numSortableFields;     // Number of sortable fields

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

  struct SchemaRule *rule;        // Contains schema rules for follow-the-hash/JSON. It must always be set
  struct IndexesScanner *scanner; // Scans new hash/JSON documents or rescan
  // can be true even if scanner == NULL, in case of a scan being cancelled
  // in favor on a newer, pending scan
  bool scan_in_progress;
  bool scan_failed_OOM; // background indexing failed due to Out Of Memory
  // Number of keys the background build had scanned when it aborted on OOM, frozen
  // before the scanner is freed. IndexesScanner_IndexedPercent derives percent_indexed
  // from it (over the current DbSize) while scan_failed_OOM holds, so an OOM-cancelled
  // build is distinguishable from a completed one (which reports 1.0). Only meaningful
  // when scan_failed_OOM is set.
  size_t scan_failed_OOM_scanned_keys;
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

  // Disk RDB state (NULL for memory-only indexes), pending to be applied at
  // replication ending. Vector index state is stored inline in each field.
  RedisSearchDiskRdbState *pendingDiskRdbState;
  bool diskRegistered;

  // True when the SST+RDB stream indicated the source node was still
  // background-indexing (scan in progress, or a previous scan failed on OOM)
  // when the snapshot was taken. Set at RDB load (SST path only) and consumed at
  // Indexes_FinishSSTReplication, which restarts the async scan so the loading
  // node (replica / hot-restart) finishes the partially populated index.
  // Idempotent re-indexing (DocIdMeta skip) makes the restart a safe backfill.
  bool resume_bg_indexing;
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

/* Get the count of total blocks*/
size_t IndexSpec_TotalBlockCount(IndexSpec *sp);

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

/* Build a new index spec from redis arguments and start its GC. Does NOT add it
 * to the global registry or start the initial scan - the caller-facing
 * Indexes_CreateNewSpec (indexes.h) wraps those registry concerns around this call.
 * If an error occurred - we set an error string in err and return NULL.
 */
IndexSpec *IndexSpec_CreateNew(RedisModuleCtx *ctx, RedisModuleString **argv, int argc,
                               QueryError *status);


/**
 * Convert an IndexSpec to its RDB serialized form, by calling the `IndexSpecType` rdb_save function.
 * Note that the returned RedisModuleString* must be freed by the caller
 * using RedisModule_FreeString
*/
RedisModuleString *IndexSpec_Serialize(IndexSpec *sp);

/**
 * Deserialize an IndexSpec from its RDB serialized form, by calling the `IndexSpecType` rdb_load function.
 * Returns the loaded spec (its single owning reference in sp->own_ref), or NULL on failure.
 * Does NOT publish the spec into the global registry - the caller must pass the
 * result to Indexes_StoreSpecAfterRdbLoad (see indexes.h).
 * Does not consume the serialized string, the caller is responsible for freeing it.
*/
IndexSpec *IndexSpec_Deserialize(const RedisModuleString *serialized, int encver);

/* Start the garbage collection loop on the index spec */
void IndexSpec_StartGC(StrongRef spec_ref, IndexSpec *sp, GCPolicy gcPolicy);

/* Same as IndexSpec_Parse, but takes a NUL-terminated C-string name and wraps it in a HiddenString
 * internally. Intended for unit tests only.
 * Do not use in production or new code: the wrapping requires an extra strlen() over the name,
 * which IndexSpec_Parse avoids by taking a HiddenString directly. */
StrongRef IndexSpec_ParseC(RedisModuleCtx *ctx, const char *name, const char **argv, int argc, QueryError *status);

FieldSpec *IndexSpec_CreateField(IndexSpec *sp, const char *name, const char *path);

// Delete a document from the index by its key name.
// In disk mode, looks up the docId via the key's metadata, removes the
// document from disk by that id, and deletes the DocIdMeta key→docId mapping so
// it stays authoritative (an entry exists iff the doc is indexed). In memory
// mode, pops the document metadata from the DocTable.
// Requires a RedisModuleCtx to access the key's metadata.
// `openKey` is an optional already-open, pinned handle for the document key.
// Pass it when the caller holds the key open and pinned (e.g. the async scan key
// callback, where the key is not addressable by name): the DocIdMeta lookup and
// delete then reuse it instead of reopening the key by name, matching the
// open-key plumbing on the indexing success path. Pass NULL to open by name.
// This function locks the spec for writing.
int IndexSpec_DeleteDoc(IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString *key,
                        RedisModuleKey *openKey);

// Same as IndexSpec_DeleteDoc but does not lock the spec.
// Use when the spec is already locked for writing.
void IndexSpec_DeleteDoc_Unsafe(IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString *key,
                                RedisModuleKey *openKey);

// Delete a document from the index by its docId directly, without needing
// to look it up by key name. Removes the document from the DocTable but does
// NOT clean up DocIdMeta on the key. This is called from the metadata unlink callback
void IndexSpec_DeleteDocById(IndexSpec *spec, t_docId docId);

// (Re)index a single document into the spec.
//
// `openKey` is an optional already-open handle for `key`. Pass it when the
// caller already holds the key open and pinned (e.g. the async scan key
// callback) so the DocIdMeta update can reuse the handle instead of reopening
// the key by name; pass NULL otherwise. The caller retains ownership of
// `openKey` and must keep it valid for the duration of the call.
int IndexSpec_UpdateDoc(IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString *key,
                        DocumentType type, RedisModuleKey *openKey);

// Format the legacy (separate-key) Redis key name for a numeric/tag/geo field.
RedisModuleString *IndexSpec_LegacyGetFormattedKey(IndexSpec *sp, const FieldSpec *fs,
                                                   FieldType forType);

/**
 * Indicate that the index spec should use an internal dictionary,rather than
 * the Redis keyspace
 */
void IndexSpec_MakeKeyless(IndexSpec *sp);

/**
 * Exposing all the fields of the index to INFO command.
 * @param ctx - the redis module info context
 * @param sp - the index spec
 * @param obfuscate - if true, obfuscate the index name and field names
 * @param skip_unsafe_ops - if true, skips operations unsafe in signal handler context (allocations, locks)
 * @param globalScanActive - whether a global background scan is currently running
 */
void IndexSpec_AddToInfo(RedisModuleInfoCtx *ctx, IndexSpec *sp, bool obfuscate, bool skip_unsafe_ops,
                         bool globalScanActive);

/**
 * Gets the next text id from the index. This does not currently
 * modify the index
 */
int IndexSpec_CreateTextId(IndexSpec *sp, t_fieldIndex index);

/* Add fields to a redis schema.
 * Does not schedule the post-alter background scan; the caller is responsible
 * for that (see CreateIndexAlterCommand in module.c). */
int IndexSpec_AddFields(StrongRef ref, IndexSpec *sp, RedisModuleCtx *ctx, ArgsCursor *ac,
                        QueryError *status);

bool IndexSpec_IsCoherent(IndexSpec *sp, sds* prefixes, size_t n_prefixes);

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
 * Per-spec bookkeeping for an already-resolved spec: bumps the usage counter and
 * refreshes the temporary-index timeout timer (subject to the NOCOUNTERINC /
 * NOTIMERUPDATE option flags). Touches no global structures. `spec_ref` must be a
 * valid, non-NULL strong reference. To look up a spec by name and run this, use
 * Indexes_LoadIndexSpecUnsafeEx (indexes.h).
 */
void IndexSpec_OnAcquire(StrongRef spec_ref, IndexLoadOptions *options);

/**
 * Quick access to the spec's strong reference. This function should be called only if
 * the spec is valid and protected (by the GIL or the spec's lock).
 * The call does not increase the spec's reference counters.
 * @return a strong reference to the spec.
 */
StrongRef IndexSpec_GetStrongRefUnsafe(const IndexSpec *spec);

/**
 * @brief Tears down a spec's non-registry global state (aliases, schema
 * prefixes, timeout timer, global field stats) and consumes the strong
 * reference. Does NOT touch the global registry (specDict_g/specIdDict_g).
 * Used on the create/parse failure path, where the spec was never registered;
 * Indexes_RemoveSpecFromGlobals (indexes.h) calls it after the registry deletion.
 *
 * @param ref a strong reference to the spec
 * @param removeActive - should we call CurrentThread_ClearIndexSpec on the released spec
 */
void IndexSpec_Unlink(StrongRef spec_ref, bool removeActive);

/*
 * Free an indexSpec. For LLAPI
 */
void IndexSpec_Free(IndexSpec *spec);

//---------------------------------------------------------------------------------------------

// Whether RDB load/save should use the SST (disk) persistence path for the
// given context.
bool CheckRdbSstPersistence(RedisModuleCtx *ctx, const char *prefix);

// Temporary-index timeout timer management for a single spec.
void IndexSpec_SetTimeoutTimer(IndexSpec *sp, WeakRef spec_ref);
void IndexSpec_ResetTimeoutTimer(IndexSpec *sp);

// Open a disk index from its pending SST/RDB state and materialize its
// disk-backed fields.
bool IndexSpec_SSTRdbOpenAndApply(RedisModuleCtx *ctx, IndexSpec *sp);

// Record that an index drop is pending.
void addPendingIndexDrop();

void IndexSpec_AddTerm(IndexSpec *sp, const char *term, size_t len);

IndexSpec *NewIndexSpec(const HiddenString *name);
// Parse a single spec from the RDB stream. Does not consult the registry or open
// the non-SST on-disk index; the caller resolves duplicate status and then calls
// IndexSpec_RdbLoadOpenDisk.
IndexSpec *IndexSpec_RdbLoad(RedisModuleIO *rdb, int encver, bool useSst, QueryError *status);
// Open the non-SST on-disk index for a spec parsed by IndexSpec_RdbLoad, once the
// caller has resolved sp->isDuplicate. No-op for SST/memory/duplicate specs.
int IndexSpec_RdbLoadOpenDisk(RedisModuleCtx *ctx, IndexSpec *sp, bool useSst, QueryError *status);
void IndexSpec_RdbSave(RedisModuleIO *rdb, IndexSpec *sp, int contextFlags);

// Per-spec callbacks for the IndexSpecType module type. IndexSpec_LegacyRdbLoad
// loads a legacy (pre-RDB-event) index for upgrade.
void *IndexSpec_LegacyRdbLoad(RedisModuleIO *rdb, int encver);
void IndexSpec_RdbSave_Wrapper(RedisModuleIO *rdb, void *value);
void IndexSpec_LegacyFree(void *spec);
// int IndexSpec_UpdateWithHash(IndexSpec *spec, RedisModuleCtx *ctx, RedisModuleString *key);
void IndexSpec_ClearAliases(StrongRef ref);

void IndexSpec_InitializeSynonym(IndexSpec *sp);

//---------------------------------------------------------------------------------------------

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
 * Uses the sizes of the doc-table, tag and text overhead if they are not `0`
 * (otherwise compute them in-place). Vector overhead is expected to be passed in as an argument
 * and will not be computed in-place
 * TODO: fIx so this will account for the entire index memory, preferably by using an allocator,
 * currently it is a best effort that account only for part of the actual memory.
 */
size_t IndexSpec_TotalMemUsage(IndexSpec *sp, size_t doctable_tm_size, size_t tags_overhead,
  size_t text_overhead, size_t vector_overhead);

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

void CleanPool_ThreadPoolStart();
void CleanPool_ThreadPoolDestroy();
size_t CleanInProgressOrPending();


///////////////////////////////////////////////////////////////////////////////////////////////

// Tries to promote a WeakRef of a spec to a StrongRef
// If a strong reference was obtained then we also set the current thread's active spec
StrongRef IndexSpecRef_Promote(WeakRef ref);
// Releases a strong reference to a spec
// Must only be called if the spec was promoted successfully
// Will also clear the current thread's active spec
void IndexSpecRef_Release(StrongRef ref);

// =============================================================================
// Compaction FFI Functions (called by Rust during GC)
// =============================================================================

/**
 * @brief Acquire the IndexSpec write lock
 * @param sp Pointer to the IndexSpec
 */
void IndexSpec_AcquireWriteLock(IndexSpec* sp);

/**
 * @brief Release the IndexSpec write lock
 * @param sp Pointer to the IndexSpec
 */
void IndexSpec_ReleaseWriteLock(IndexSpec* sp);

/**
 * @brief Update a term's document count in the Serving Trie
 *
 * @param sp Pointer to the IndexSpec
 * @param term Pointer to term string (NOT null-terminated)
 * @param term_len Length of term in bytes
 * @param doc_count_decrement Number of documents to decrement from the term's count
 * @return true if the term was completely emptied and deleted from the trie
 */
bool IndexSpec_DecrementTrieTermCount(IndexSpec* sp, const char* term, size_t term_len,
                                      size_t doc_count_decrement);

/**
 * @brief Update IndexScoringStats based on the number of terms removed
 *
 * @param sp Pointer to the IndexSpec
 * @param num_terms_removed Number of terms that became empty during compaction
 */
void IndexSpec_DecrementNumTerms(IndexSpec* sp, uint64_t num_terms_removed);

#ifdef __cplusplus
}
#endif

#endif  // __SPEC_H__

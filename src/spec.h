#ifndef __SPEC_H__
#define __SPEC_H__
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

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
#include "dict/dict.h"
#include "rax/rax.h"
#include "redisearch_api.h"

#ifdef __cplusplus
extern "C" {
#endif

#define NUMERIC_STR "NUMERIC"
#define GEO_STR "GEO"

#define SPEC_NOOFFSETS_STR "NOOFFSETS"
#define SPEC_NOFIELDS_STR "NOFIELDS"
#define SPEC_NOFREQS_STR "NOFREQS"
#define SPEC_NOHL_STR "NOHL"
#define SPEC_SCHEMA_STR "SCHEMA"
#define SPEC_SCHEMA_EXPANDABLE_STR "MAXTEXTFIELDS"
#define SPEC_TEMPORARY_STR "TEMPORARY"
#define SPEC_TEXT_STR "TEXT"
#define SPEC_WEIGHT_STR "WEIGHT"
#define SPEC_NOSTEM_STR "NOSTEM"
#define SPEC_PHONETIC_STR "PHONETIC"
#define SPEC_TAG_STR "TAG"
#define SPEC_SORTABLE_STR "SORTABLE"
#define SPEC_STOPWORDS_STR "STOPWORDS"
#define SPEC_NOINDEX_STR "NOINDEX"
#define SPEC_SEPARATOR_STR "SEPARATOR"
#define SPEC_MULTITYPE_STR "MULTITYPE"
#define SPEC_WITHRULES_STR "WITHRULES"
#define SPEC_ASYNC_STR "ASYNC"

/**
 * If wishing to represent field types positionally, use this
 * enum. Since field types are a bitmask, it's pointless to waste
 * space like this
 */

static const char *SpecTypeNames[] = {[IXFLDPOS_FULLTEXT] = SPEC_TEXT_STR,
                                      [IXFLDPOS_NUMERIC] = NUMERIC_STR,
                                      [IXFLDPOS_GEO] = GEO_STR,
                                      [IXFLDPOS_TAG] = SPEC_TAG_STR};

#define INDEX_SPEC_KEY_PREFIX "idx:"
#define INDEX_SPEC_ALIASES "$idx:aliases$"

#define SPEC_MAX_FIELDS 1024
#define SPEC_MAX_FIELD_ID (sizeof(t_fieldMask) * 8)
// The threshold after which we move to a special encoding for wide fields
#define SPEC_WIDEFIELD_THRESHOLD 32

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

  // Schema/Rule based indexing
  Index_UseRules = 0x800,

  // Index asynchronously, don't report errors..
  Index_Async = 0x1000,

} IndexFlags;

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

#define INDEX_CURRENT_VERSION 15

// Those versions contains doc table as array, we modified it to be array of linked lists
#define INDEX_MIN_COMPACTED_DOCTABLE_VERSION 12
#define INDEX_MIN_COMPAT_VERSION 2
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

struct DocumentIndexer;
struct IndexQueue;
struct IoQueue;

#define SDQ_S_PENDING 0x02
#define SDQ_S_PROCESSING 0x04

typedef struct {
  IndexSpec *spec;
  // Entries which are awaiting indexing
  dict *entries;
  // Entries which are currently being indexed
  dict *active;
  pthread_mutex_t lock;
  int state; // SDQ_S_xxx
} SpecDocQueue;

SpecDocQueue* SpecDocQueue_Create(IndexSpec *spec);
void SpecDocQueue_Free(SpecDocQueue *q);

typedef enum {
  IDX_S_NODELKEYS = 0x01, // Don't delete keys when deleting the index
  IDX_S_DELETED = 0x02, // Index has been deleted via IndexSpec_Free
  IDX_S_REGISTERED = 0x04, // Index is still being created
  IDX_S_LIBORIGIN = 0x08, // Index created using C API
  IDX_S_EXPIRED = 0x10
} IndexState;

struct SpecLegacyInfo;

struct IndexSpec {
  char *name;
  FieldSpec *fields;
  int numFields;

  IndexStats stats;
  IndexFlags flags;
  rax *termsIdx;
  Trie *terms; // for distance, etc.
  struct TagIndex **tags;
  struct NumericRangeTree **nums;
  struct GeoIndex **geos;

  RSSortingTable *sortables;

  DocTable docs;

  StopWordList *stopwords;

  GCContext *gc;

  SynonymMap *smap;
  RedisModuleTimerID timer;

  uint64_t uniqueId;
  size_t refcount;

  struct IndexSpecCache *spcache;
  struct InvertedIndex *cachedInvidx;
  long long timeout;
  long long minPrefix;
  long long maxPrefixExpansions;  // -1 unlimited
  RSGetValueCallback getValue;
  void *getValueCtx;
  char **aliases; // Aliases to self-remove when the index is deleted
  SpecDocQueue *queue;
  struct SpecLegacyInfo *legacy;
  // Lock for the index data, e.g. doc table, inverted indexes, etc.
  pthread_rwlock_t idxlock;
  IndexState state;
};

typedef struct {
  void (*dtor)(void *p);
  void *p;
} KeysDictValue;

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

#define IndexSpec_Incref(spec) (spec)->refcount++

size_t IndexSpec_Decref(IndexSpec *spec);

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

/**
 * Case-sensitive version of GetField()
 */
const FieldSpec *IndexSpec_GetFieldCase(const IndexSpec *spec, const char *name, size_t n);

const char *GetFieldNameByBit(const IndexSpec *sp, t_fieldMask id);

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

FieldSpec **getFieldsByType(IndexSpec *spec, FieldType type);
int isRdbLoading(RedisModuleCtx *ctx);

/* Start the garbage collection loop on the index spec */
void IndexSpec_StartGC(RedisModuleCtx *ctx, IndexSpec *sp, float initialHZ);
void IndexSpec_StartGCFromSpec(IndexSpec *sp, float initialHZ, uint32_t gcPolicy);

typedef struct {
  int replace; // Replace the index if it exists
} IndexCreateOptions;

/**
 * Create an index spec from arguments. Options contains creation options that
 * are parsed
 */
IndexSpec *IndexSpec_ParseArgs(const char *name, ArgsCursor *ac,
                               IndexCreateOptions *options, QueryError *status);

int IndexSpec_Register(IndexSpec *sp, const IndexCreateOptions *options, QueryError *err);

FieldSpec *IndexSpec_CreateField(IndexSpec *sp, const char *name);

#define IndexSpec_IsKeyless(sp) ((sp)->flags != Index_LibOnly)

/**
 * Gets the next text id from the index. This does not currently
 * modify the index
 */
int IndexSpec_CreateTextId(const IndexSpec *sp);

/* Add fields to a redis schema */
int IndexSpec_AddFields(IndexSpec *sp, ArgsCursor *ac, QueryError *status);

void FieldSpec_Initialize(FieldSpec *sp, FieldType types);

IndexSpec *IndexSpec_Load(void *unused, const char *name, int openWrite);

/** Load the index as writeable */
#define INDEXSPEC_LOAD_WRITEABLE 0x01
/** Don't consult the alias table when retrieving the index */
#define INDEXSPEC_LOAD_NOALIAS 0x02
/** The name of the index is in the format of a redis string */
#define INDEXSPEC_LOAD_KEY_RSTRING 0x04
/** Don't update index TTL, for temporary indexes */
#define INDEXSPEC_LOAD_NOTOUCH 0x08

typedef struct {
  uint32_t flags;
  union {
    const char *cstring;
    RedisModuleString *rstring;
  } name;

  /** name of alias lookup key to use */
  const char *alookup;
} IndexLoadOptions;

/**
 * Find and load the index using the specified parameters.
 * @return the index spec, or NULL if the index does not exist
 */
IndexSpec *IndexSpec_LoadEx(void *unused, IndexLoadOptions *options);

// Global hook called when an index spec is created
extern void (*IndexSpec_OnCreate)(const IndexSpec *sp);

struct InvertedIndex *IDX_LoadTerm(IndexSpec *sp, const char *term, size_t n, int flags);
struct NumericRangeTree *IDX_LoadRange(IndexSpec *sp, const FieldSpec *fs, int flags);
struct TagIndex *IDX_LoadTags(IndexSpec *sp, const FieldSpec *fs, int flags);
struct TagIndex *IDX_LoadTagsFieldname(IndexSpec *sp, const char *s, int flags);
struct NumericRangeTree *IDX_LoadRangeFieldname(IndexSpec *, const char *, int);
struct GeoIndex* IDX_LoadGeo(IndexSpec *sp, const FieldSpec *fs, int flags);
struct GeoIndex *IDX_LoadGeoFieldname(IndexSpec *, const char *, int);
/** Lock the index for reading */
void IDX_ReadLock(IndexSpec *);

/** Lock the index for writing */
void IDX_WriteLock(IndexSpec *);

/** Returns false if the index has been dropped (but refcount is still holding it) */
int IDX_IsAlive(const IndexSpec *);

/** Release and re-acquire the read lock */
int IDX_YieldRead(IndexSpec *);

/** Release and reacquire the write lock */
int IDX_YieldWrite(IndexSpec *);

/* Get a random term from the index spec using weighted random. Weighted random is done by sampling
 * N terms from the index and then doing weighted random on them. A sample size of 10-20 should be
 * enough */
char *IndexSpec_GetRandomTerm(IndexSpec *sp, size_t sampleSize);
/*
 * Free an indexSpec. This doesn't free the spec itself as it's not allocated by the parser
 * and should be on the request's stack
 */
void IndexSpec_Free(void *spec);

#define IDXFREE_F_DELDOCS 0x01
void IndexSpec_FreeEx(IndexSpec *sp, int options);

/**
 * Free the index synchronously. Any keys associated with the index (but not the
 * documents themselves) are freed before this function returns.
 */
void IndexSpec_FreeSync(IndexSpec *spec);

/** Delete the redis key from Redis */
void IndexSpec_FreeWithKey(IndexSpec *spec, RedisModuleCtx *ctx);

/* Return 1 if a term is a stopword for the specific index */
int IndexSpec_IsStopWord(IndexSpec *sp, const char *term, size_t len);

IndexSpec *NewIndexSpec(const char *name);
IndexSpec *IDX_CreateEmpty(void);
int IndexSpec_AddField(IndexSpec *sp, FieldSpec *fs);
void *IndexSpec_RdbLoad(RedisModuleIO *rdb, int encver);
void IndexSpec_RdbSave(RedisModuleIO *rdb, void *value);
int IndexSpec_RegisterType(RedisModuleCtx *ctx);
void IndexSpec_ClearAliases(IndexSpec *sp);
void IndexSpec_CleanAll(void);

/*
 * Parse the field mask passed to a query, map field names to a bit mask passed down to the
 * execution engine, detailing which fields the query works on. See FT.SEARCH for API details
 */
t_fieldMask IndexSpec_ParseFieldMask(IndexSpec *sp, RedisModuleString **argv, int argc);

void IndexSpec_InitializeSynonym(IndexSpec *sp);

void Indexes_OnInitScanDone(void);
void Indexes_OnReindexDone(void);
void Indexes_Init(RedisModuleCtx *ctx);

// List of all indexes
extern dict* RSIndexes_g;

#ifdef __cplusplus
}
#endif
#endif

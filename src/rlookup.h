/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef RLOOKUP_H
#define RLOOKUP_H
#include <stdint.h>
#include <assert.h>

#include <spec.h>
#include <search_ctx.h>
#include "value.h"
#include "sortable.h"
#include "util/arr.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
  RLOOKUP_C_STR = 0,
  RLOOKUP_C_INT = 1,
  RLOOKUP_C_DBL = 2,
  RLOOKUP_C_BOOL = 3
} RLookupCoerceType;

/**
 * RLookup Key
 *
 * A lookup key is a structure which contains an array index at which the
 * data may be reliably located. This avoids needless string comparisons by
 * using quick objects rather than "dynamic" string comparison mechanisms.
 *
 * The basic workflow is that users of a given key (i.e. "foo") are expected
 * to first create the key by use of RLookup_GetKey(). This will provide
 * the consumer with an opaque object that is the slot of "foo". Once the
 * key is provided, it may then be use to both read and write the key.
 *
 * Using a pre-defined key also allows the query to maintain a central registry
 * of used names. If a user makes a typo in a query, this registry will easily
 * detect that the name was not used previously.
 *
 * Note that the same name can be registered twice, in which case it will simply
 * increment the reference to the same key.
 *
 * There are two arrays which are accessed to check for the key. Their use is
 * mutually exclusive per-key, though multiple keys may exist which can access
 * either one or the other array. The first array is the "sorting vector" for
 * a given document. The F_SVSRC flag is set on keys which are expected to be
 * found within the sorting vector.
 *
 * The second array is a "dynamic" array within a given result's row data.
 * This is used for data generated on the fly, or for data not stored within
 * the sorting vector.
 */
typedef struct RLookupKey {
  /** The index into the array where the value resides */
  uint16_t dstidx;

  /**
   * If the source of this value points to a sort vector, then this is the
   * index within the sort vector that the value is located
   */
  uint16_t svidx;

  /**
   * Can be F_SVSRC which means the target array is a sorting vector)
   */
  uint32_t flags;

  /** Path and name of this field
   *  path AS name */
  const char *path;
  const char *name;
  size_t name_len;

  /** Pointer to next field in the list */
  struct RLookupKey *next;
} RLookupKey;

typedef struct RLookup {
  RLookupKey *head;
  RLookupKey *tail;

  // Length of the data row. This is not necessarily the number
  // of lookup keys
  uint32_t rowlen;

  // Flags/options
  uint32_t options;

  // If present, then GetKey will consult this list if the value is not found in
  // the existing list of keys.
  IndexSpecCache *spcache;
} RLookup;

// If the key cannot be found, do not mark it as an error, but create it and
// mark it as F_UNRESOLVED
#define RLOOKUP_OPT_UNRESOLVED_OK 0x01

// If a loader was added to load the entire document, this flag will allow
// later calls to GetKey in read mode to create a key (from the schema) even if it is not sortable
#define RLOOKUP_OPT_ALL_LOADED 0x02

/**
 * Row data for a lookup key. This abstracts the question of "where" the
 * data comes from.
 */
typedef struct {
  /** Sorting vector attached to document */
  const RSSortingVector *sv;

  /** Dynamic values obtained from prior processing */
  RSValue **dyn;

  /**
   * How many values actually exist in dyn. Note that this
   * is not the length of the array!
   */
  size_t ndyn;
} RLookupRow;

static inline const RSSortingVector* RLookupRow_GetSortingVector(const RLookupRow* row) {return row->sv;}
static inline void RLookupRow_SetSortingVector(RLookupRow* row, const RSSortingVector* sv) {row->sv = sv;}

typedef enum {
  RLOOKUP_M_READ,   // Get key for reading (create only if in schema and sortable)
  RLOOKUP_M_WRITE,  // Get key for writing
  RLOOKUP_M_LOAD,   // Load key from redis keyspace (include known information on the key, fail if already loaded)
} RLookupMode;

#define RLOOKUP_F_NOFLAGS 0x0 // No special flags to pass.

/**
 * This field is (or assumed to be) part of the document itself.
 * This is a basic flag for a loaded key.
 */
#define RLOOKUP_F_DOCSRC 0x01

/**
 * This field is part of the index schema.
 */
#define RLOOKUP_F_SCHEMASRC 0x02

/** Check the sorting table, if necessary, for the index of the key. */
#define RLOOKUP_F_SVSRC 0x04

/**
 * This key was created by the query itself (not in the document)
 */
#define RLOOKUP_F_QUERYSRC 0x08

/** Copy the key string via strdup. `name` may be freed */
#define RLOOKUP_F_NAMEALLOC 0x10

/**
 * If the key is already present, then overwrite it (relevant only for LOAD or WRITE modes)
 */
#define RLOOKUP_F_OVERRIDE 0x20

/**
 * Request that the key is returned for loading even if it is already loaded.
 */
#define RLOOKUP_F_FORCE_LOAD 0x40

/**
 * This key is unresolved. Its source needs to be derived from elsewhere
 */
#define RLOOKUP_F_UNRESOLVED 0x80

/**
 * This field is hidden within the document and is only used as a transient
 * field for another consumer. Don't output this field.
 */
#define RLOOKUP_F_HIDDEN 0x100

/**
 * The opposite of F_HIDDEN. This field is specified as an explicit return in
 * the RETURN list, so ensure that this gets emitted. Only set if
 * explicitReturn is true in the aggregation request.
 */
#define RLOOKUP_F_EXPLICITRETURN 0x200

/**
 * This key's value is already available in the RLookup table,
 * if it was opened for read but the field is sortable and not normalized,
 * so the data should be exactly the same as in the doc.
 */
#define RLOOKUP_F_VAL_AVAILABLE 0x400

/**
 * This key's value was loaded (by a loader) from the document itself.
 */
#define RLOOKUP_F_ISLOADED 0x800

/**
 * This key type is numeric
 */
#define RLOOKUP_T_NUMERIC 0x1000

// Flags that are allowed to be passed to GetKey
#define RLOOKUP_GET_KEY_FLAGS (RLOOKUP_F_NAMEALLOC | RLOOKUP_F_OVERRIDE | RLOOKUP_F_HIDDEN | RLOOKUP_F_EXPLICITRETURN | \
                               RLOOKUP_F_FORCE_LOAD)
// Flags do not persist to the key, they are just options to GetKey()
#define RLOOKUP_TRANSIENT_FLAGS (RLOOKUP_F_OVERRIDE | RLOOKUP_F_FORCE_LOAD)

/**
 * Get a RLookup key for a given name.
 *
 * 1. On READ mode, a key is returned only if it's already in the lookup table (available from the
 * pipeline upstream), it is part of the index schema and is sortable (and then it is created), or
 * if the lookup table accepts unresolved keys.
 */
RLookupKey *RLookup_GetKey_Read(RLookup *lookup, const char *name, uint32_t flags);
RLookupKey *RLookup_GetKey_ReadEx(RLookup *lookup, const char *name, size_t name_len,
                                  uint32_t flags);
/**
 * Get a RLookup key for a given name.
 *
 * 2. On WRITE mode, a key is created and returned only if it's NOT in the lookup table, unless the
 * override flag is set.
 */
RLookupKey *RLookup_GetKey_Write(RLookup *lookup, const char *name, uint32_t flags);
RLookupKey *RLookup_GetKey_WriteEx(RLookup *lookup, const char *name, size_t name_len,
                                   uint32_t flags);
/**
 * Get a RLookup key for a given name.
 *
 * 3. On LOAD mode, a key is created and returned only if it's NOT in the lookup table (unless the
 * override flag is set), and it is not already loaded. It will override an existing key if it was
 * created for read out of a sortable field, and the field was normalized. A sortable un-normalized
 * field counts as loaded.
 */
RLookupKey *RLookup_GetKey_Load(RLookup *lookup, const char *name, const char *field_name,
                                uint32_t flags);
RLookupKey *RLookup_GetKey_LoadEx(RLookup *lookup, const char *name, size_t name_len,
                                  const char *field_name, uint32_t flags);

/**
 * Get the amount of visible fields is the RLookup
 */
size_t RLookup_GetLength(const RLookup *lookup, const RLookupRow *r, int *skipFieldIndex,
                         int requiredFlags, int excludeFlags, SchemaRule *rule);

/**
 * Get a value from the lookup.
 */

/**
 * Write a value to a lookup table. Key must already be registered, and not
 * refer to a read-only (SVSRC) key.
 *
 * The value written will have its refcount incremented
 */
void RLookup_WriteKey(const RLookupKey *key, RLookupRow *row, RSValue *value);

/**
 * Exactly like RLookup_WriteKey, but does not increment the refcount, allowing
 * idioms such as RLookup_WriteKey(..., RSValue_NewNumber(10)); which would otherwise cause
 * a leak.
 */
void RLookup_WriteOwnKey(const RLookupKey *key, RLookupRow *row, RSValue *value);

/**
 * Move data from the source row to the destination row. The source row is cleared.
 * @param lk lookup common to both rows
 * @param src the source row
 * @param dst the destination row
 */
void RLookupRow_MoveFieldsFrom(const RLookup *lk, RLookupRow *src, RLookupRow *dst);

/**
 * Write a value by-name to the lookup table. This is useful for 'dynamic' keys
 * for which it is not necessary to use the boilerplate of getting an explicit
 * key.
 *
 * The reference count of the value will be incremented.
 */
void RLookup_WriteKeyByName(RLookup *lookup, const char *name, size_t len, RLookupRow *row, RSValue *value);

/**
 * Like WriteKeyByName, but consumes a refcount
 */
void RLookup_WriteOwnKeyByName(RLookup *lookup, const char *name, size_t len, RLookupRow *row, RSValue *value);

/** Get a value from the row, provided the key.
 *
 * This does not actually "search" for the key, but simply performs array
 * lookups!
 *
 * @param lookup The lookup table containing the lookup table data
 * @param key the key that contains the index
 * @param row the row data which contains the value
 * @return the value if found, NULL otherwise.
 */
static inline RSValue *RLookup_GetItem(const RLookupKey *key, const RLookupRow *row) {

  RSValue *ret = NULL;
  if (row->dyn && array_len(row->dyn) > key->dstidx) {
    ret = row->dyn[key->dstidx];
  }
  if (!ret) {
    if (key->flags & RLOOKUP_F_SVSRC) {
      const RSSortingVector* sv = RLookupRow_GetSortingVector(row);
      if (sv && RSSortingVector_Length(sv) > key->svidx) {
        ret = RSSortingVector_Get(sv, key->svidx);
        if (ret != NULL && ret == RSValue_NullStatic()) {
          ret = NULL;
        }
      }
    }
  }
  return ret;
}

/**
 * Wipes the row, retaining its memory but decrefing any included values.
 * This does not free all the memory consumed by the row, but simply resets
 * the row data (preserving any caches) so that it may be refilled.
 */
void RLookupRow_Wipe(RLookupRow *row);

/**
 * Frees all the memory consumed by the row. Implies Wipe(). This should be used
 * when the row object will no longer be used.
 */
void RLookupRow_Reset(RLookupRow *row);

typedef enum {
  /* Use keylist (keys/nkeys) for the fields to list */
  RLOOKUP_LOAD_KEYLIST,
  /* Load only cached keys (don't open keys) */
  RLOOKUP_LOAD_SVKEYS,
  /* Load all keys in the document */
  RLOOKUP_LOAD_ALLKEYS,
  /* Load all the keys in the RLookup object */
  RLOOKUP_LOAD_LKKEYS
} RLookupLoadFlags;

typedef struct {
  struct RedisSearchCtx *sctx;

  /** Needed for the key name, and perhaps the sortable */
  const RSDocumentMetadata *dmd;

  /* Needed for rule filter where dmd does not exist */
  const char *keyPtr;
  DocumentType type;

  /** Keys to load. If present, then loadNonCached and loadAllFields is ignored */
  const RLookupKey **keys;
  /** Number of keys in keys array */
  size_t nkeys;

  /**
   * The following options control the loading of fields, in case non-SORTABLE
   * fields are desired.
   */
  RLookupLoadFlags mode;

  /**
   * Don't use sortables when loading documents. This will enforce the loader to load
   * the fields from the document itself, even if they are sortables and un-normalized.
   */
  bool forceLoad;

  /**
   * Force string return; don't coerce to native type
   */
  bool forceString;

  struct QueryError *status;
} RLookupLoadOptions;

/**
 * Attempt to load a document into the row. The document's fields are placed into
 * their corresponding slots.
 *
 * @param lt Lookup table. Contains the keys to load.
 * @param dst row that should contain the data
 * @param options options controlling the load process
 */
int RLookup_LoadDocument(RLookup *lt, RLookupRow *dst, RLookupLoadOptions *options);

/**
 * Initialize the lookup. If cache is provided, then it will be used as an
 * alternate source for lookups whose fields are absent
 */
void RLookup_Init(RLookup *l, IndexSpecCache *cache);

/**
 * Releases any resources created by this lookup object. Note that if there are
 * lookup keys created with RLOOKUP_F_NOINCREF, those keys will no longer be
 * valid after this call!
 */
void RLookup_Cleanup(RLookup *l);

/**
 * Frees an individual RLookupKey, cleaning up its allocated strings
 */
void RLookupKey_Free(RLookupKey *k);

/**
 * Initialize the lookup with fields from hash.
 */
int RLookup_LoadRuleFields(RedisModuleCtx *ctx, RLookup *it, RLookupRow *dst, IndexSpec *sp, const char *keyptr);

int jsonIterToValue(RedisModuleCtx *ctx, JSONResultsIterator iter, unsigned int apiVersion, RSValue **rsv);


/**
 * Search an index field by its name in the lookup table spec cache.
 */
const FieldSpec *findFieldInSpecCache(const RLookup *lookup, const char *name);

/**
 * Add non-overridden keys from source lookup into destination lookup (overridden keys are skipped).
 * For each key in src, check if it already exists in dest by name.
 * If doesn't exists, create new key in dest.
 * Handle existing keys based on flags (skip with RLOOKUP_F_NOFLAGS, override with RLOOKUP_F_OVERRIDE).
 *
 * Flag handling:
 * - Preserves persistent source key properties (F_SVSRC, F_HIDDEN, F_EXPLICITRETURN, etc.)
 * - Filters out transient flags from source keys (F_OVERRIDE, F_FORCE_LOAD)
 * - Respects caller's control flags for behavior (F_OVERRIDE, F_FORCE_LOAD, etc.)
 * - Targat flags = caller_flags | (source_flags & ~RLOOKUP_TRANSIENT_FLAGS)
 */
void RLookup_AddKeysFrom(const RLookup *src, RLookup *dest, uint32_t flags);

/**
 * Write field data from source row to destination row with different schemas.
 * Iterate through source lookup keys, find corresponding keys in destination by name,
 * and write it to destination row using RLookup_WriteOwnKey().
 * Assumes all source keys exist in destination (enforce with ASSERT).
 */
void RLookupRow_WriteFieldsFrom(const RLookupRow *srcRow, const RLookup *srcLookup,
                               RLookupRow *destRow, RLookup *destLookup);

// exposed to be called from Rust, was inline before that.
int RLookup_JSON_GetAll(RLookup *it, RLookupRow *dst, RLookupLoadOptions *options);

// exposed to be called from Rust, was inline before that.
int loadIndividualKeys(RLookup *it, RLookupRow *dst, RLookupLoadOptions *options);

// exposed to be called from Rust, was inline before that.
RSValue *hvalToValue(const RedisModuleString *src, RLookupCoerceType type);

// exposed to be called from Rust, was inline before that.
RSValue *replyElemToValue(RedisModuleCallReply *rep, RLookupCoerceType otype);

// exposed to be called from Rust, is part of a dependency and was inline before that.
size_t sdslen__(const char* s);

#ifdef __cplusplus
}
#endif

#endif

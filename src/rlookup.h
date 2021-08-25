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
 *
 * It is possible to use the same name in both the sorting vector, and also in
 * the dynamic array. To accomplish this, create a key with the F_OUTPUT flag
 * set. This flag forces the lookup mechanism to ignore the presence of the
 * sorting vector, so that they are essentially different keys, even though
 * they have the same name.
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
   * Can be F_SVSRC which means the target array is a sorting vector, or
   * F_OUTPUT which means that the t
   */
  uint16_t flags;

  /** Type this lookup should be coerced to */
  RLookupCoerceType fieldtype : 16;

  uint32_t refcnt;

  /** Path and name of this field
   *  path AS name */
  const char *path;
  const char *name;

  /** Size of this field */
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

/**
 * Row data for a lookup key. This abstracts the question of "where" the
 * data comes from.
 */
typedef struct {
  /** Sorting vector attached to document */
  const RSSortingVector *sv;

  /** Module key for data that derives directly from a Redis data type */
  RedisModuleKey *rmkey;

  /** Dynamic values obtained from prior processing */
  RSValue **dyn;

  /**
   * How many values actually exist in dyn. Note that this
   * is not the length of the array!
   */
  size_t ndyn;
} RLookupRow;

#define RLOOKUP_F_OEXCL 0x01   // Error if name exists already
#define RLOOKUP_F_OCREAT 0x02  // Create key if it does not exit

/** Force this key to be the output key, bypassing the sort vector */
#define RLOOKUP_F_OUTPUT 0x04

/** Check the sorting table, if necessary, for the index of the key. */
#define RLOOKUP_F_SVSRC 0x08

/** Copy the key string via strdup. `name` may be freed */
#define RLOOKUP_F_NAMEALLOC 0x10

/**
 * Do not increment the reference count of the returned key. Note that a single
 * refcount is still retained within the lookup structure itself
 */
#define RLOOKUP_F_NOINCREF 0x20

/**
 * This field needs to be loaded externally from a document. It is not
 * natively present.
 *
 * The flag is intended to be used by you, the programmer. If you encounter
 * a key with this flag set, then the value must be loaded externally and placed
 * into the row in the corresponding index slot.
 */
#define RLOOKUP_F_DOCSRC 0x40

/**
 * This field is hidden within the document and is only used as a transient
 * field for another consumer. Don't output this field.
 */
#define RLOOKUP_F_HIDDEN 0x80

/**
 * This key is used as sorting key for the result
 */
#define RLOOKUP_F_SORTKEY 0x100

/**
 * This key is unresolved. It source needs to be derived from elsewhere
 */
#define RLOOKUP_F_UNRESOLVED 0x200

/**
 * The opposite of F_HIDDEN. This field is specified as an explicit return in
 * the RETURN list, so ensure that this gets emitted. Only set if
 * explicitReturn is true in the aggregation request.
 */
#define RLOOKUP_F_EXPLICITRETURN 0x400

/**
 * These flags do not persist to the key, they are just options to GetKey()
 */
#define RLOOKUP_TRANSIENT_FLAGS (RLOOKUP_F_OEXCL | RLOOKUP_F_OCREAT | RLOOKUP_F_NOINCREF)

/**
 * Get a RLookup key for a given name. The behavior of this function depends on
 * the flags.
 *
 * If F_OCREAT is not used, then this function will return NULL if a key could
 * not be found, unless OPT_UNRESOLVED_OK is set on the lookup itself. In this
 * case, the key is returned, but has the F_UNRESOLVED flag set.
 */
RLookupKey *RLookup_GetKey(RLookup *lookup, const char *name, int flags);

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
 * idioms such as RLookup_WriteKey(..., RS_NumVal(10)); which would otherwise cause
 * a leak.
 */
void RLookup_WriteOwnKey(const RLookupKey *key, RLookupRow *row, RSValue *value);

/**
 * Move data from the source row to the destination row. The source row is cleared.
 * The destination row should be pre-cleared (though its cache may still
 * exist).
 * @param lk lookup common to both rows
 * @param src the source row
 * @param dst the destination row
 */
void RLookupRow_Move(const RLookup *lk, RLookupRow *src, RLookupRow *dst);

/**
 * Write a value by-name to the lookup table. This is useful for 'dynamic' keys
 * for which it is not necessary to use the boilerplate of getting an explicit
 * key.
 *
 * The reference count of the value will be incremented.
 */
void RLookup_WriteKeyByName(RLookup *lookup, const char *name, RLookupRow *row, RSValue *value);

/**
 * Like WriteKeyByName, but consumes a refcount
 */
void RLookup_WriteOwnKeyByName(RLookup *lookup, const char *name, RLookupRow *row, RSValue *value);

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
      if (row->sv && row->sv->len > key->svidx) {
        ret = row->sv->values[key->svidx];
        if (ret != NULL && ret->t == RSValue_Null) {
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
void RLookupRow_Cleanup(RLookupRow *row);

void RLookupRow_Dump(const RLookupRow *row);

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
   * Don't use sortables when loading documents. This might be used to ensure
   * that only the exact document and not a normalized version is employed
   */
  int noSortables;

  /**
   * Force string return; don't coerce to native type
   */
  int forceString;

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

/** Use incref/decref instead! */
void RLookupKey_FreeInternal(RLookupKey *k);

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

static inline const RLookupKey *RLookup_FindKeyWith(const RLookup *l, uint32_t f) {
  for (const RLookupKey *k = l->head; k; k = k->next) {
    if (k->flags & f) {
      return k;
    }
  }
  return NULL;
}

/**
 * Initialize the lookup with fields from hash.
 */
int RLookup_GetHash(RLookup *it, RLookupRow *dst, RedisModuleCtx *ctx, RedisModuleString *key);

#ifdef __cplusplus
}
#endif

#endif

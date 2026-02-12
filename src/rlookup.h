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

#include "rlookup_rs.h"

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
typedef struct RLookupKey RLookupKey;

#define RLOOKUP_FOREACH(key, rlookup, block) \
    RLookupIterator iter = RLookup_Iter(rlookup); \
    const RLookupKey* key; \
    while (RLookupIterator_Next(&iter, &key)) { \
        block \
    }

/** An iterator over the keys in an `RLookup` returning immutable pointers. */
typedef struct RLookupIterator {
    const struct RLookupKey *current;
} RLookupIterator;

/**
 * Advances the iterator to the next key places a pointer to it into `key`.
 *
 * Returns `true` while there are more keys or `false` to indicate the
 * last key ways returned and the caller should not call this function anymore.
 */
bool RLookupIterator_Next(RLookupIterator* iterator, const RLookupKey** key);

/** A iterator over the keys in an `RLookup` returning mutable pointers. */
typedef struct RLookupIteratorMut {
    struct RLookupKey *current;
} RLookupIteratorMut;

/**
 * Advances the iterator to the next key places a pointer to it into `key`.
 *
 * Returns `true` while there are more keys or `false` to indicate the
 * last key ways returned and the caller should not call this function anymore.
 */
bool RLookupIteratorMut_Next(RLookupIteratorMut* iterator, RLookupKey** key);

/** Returns an immutable iterator over the keys in this RLookup */
RLookupIterator RLookup_Iter(const RLookup* rlookup);

/** Returns an mutable iterator over the keys in this RLookup */
RLookupIteratorMut RLookup_IterMut(const RLookup* rlookup);

// /**
//  * Returns the length of the data row.
//  * This is not necessarily the number of lookup keys
//  */
static inline uint32_t RLookup_GetRowLen(const RLookup* rlookup) {
    // return rlookup->keys.rowlen;
}

// /**
//  * Enables the given set of RLookup options.
//  */
static inline void RLookup_EnableOptions(RLookup* rlookup, uint32_t options) {
    // rlookup->_options |= options;
}

/**
 * Disables the given set of RLookup options.
 */
static inline void RLookup_DisableOptions(RLookup* rlookup, uint32_t options) {
    // rlookup->_options &= ~options;
}

/** Returns `true` if this RLookup has an associated IndexSpecCache. */
static inline bool RLookup_HasIndexSpecCache(const RLookup* rlookup) {
//     return rlookup->_spcache != NULL;
}

// If the key cannot be found, do not mark it as an error, but create it and
// mark it as F_UNRESOLVED
#define RLOOKUP_OPT_UNRESOLVED_OK 0x01

// If a loader was added to load the entire document, this flag will allow
// later calls to GetKey in read mode to create a key (from the schema) even if it is not sortable
#define RLOOKUP_OPT_ALL_LOADED 0x02

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
  const RLookupKey *const *keys;

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

int jsonIterToValue(RedisModuleCtx *ctx, JSONResultsIterator iter, unsigned int apiVersion, RSValue **rsv);

// exposed to be called from Rust, was inline before that.
int RLookup_JSON_GetAll(RLookup *it, RLookupRow *dst, RLookupLoadOptions *options);

// exposed to be called from Rust, was inline before that.
int loadIndividualKeys(RLookup *it, RLookupRow *dst, RLookupLoadOptions *options);

// exposed to be called from Rust, was inline before that.
RSValue *hvalToValue(const RedisModuleString *src, RLookupCoerceType type);

#ifdef __cplusplus
}
#endif

#endif

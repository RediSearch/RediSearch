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
#include "redisearch_rs/headers/rlookup_rs.h"

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

typedef enum {
  RLOOKUP_M_READ,   // Get key for reading (create only if in schema and sortable)
  RLOOKUP_M_WRITE,  // Get key for writing
  RLOOKUP_M_LOAD,   // Load key from redis keyspace (include known information on the key, fail if already loaded)
} RLookupMode;

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
      if (row->sv && RSSortingVector_Length(row->sv) > key->svidx) {
        ret = RSSortingVector_Get(row->sv, key->svidx);
        if (ret != NULL && ret == RS_NullVal()) {
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

sds RLookupRow_DumpSds(const RLookupRow *row, bool obfuscate);

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
 * Initialize the lookup with fields from hash.
 */
int RLookup_LoadRuleFields(RedisModuleCtx *ctx, RLookup *it, RLookupRow *dst, IndexSpec *sp, const char *keyptr);


int jsonIterToValue(RedisModuleCtx *ctx, JSONResultsIterator iter, unsigned int apiVersion, RSValue **rsv);

#ifdef __cplusplus
}
#endif

#endif

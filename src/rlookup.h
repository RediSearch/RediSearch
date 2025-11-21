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
    if (key->flags & RLOOKUPKEYFLAG_SVSRC) {
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
  RLookupLoadMode mode;

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

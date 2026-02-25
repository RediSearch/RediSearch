/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef RLOOKUP_LOAD_DOCUMENT_H
#define RLOOKUP_LOAD_DOCUMENT_H
#include <stdint.h>
#include <assert.h>

#include "rlookup.h"
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

// If the key cannot be found, do not mark it as an error, but create it and
// mark it as F_UNRESOLVED
#define RLOOKUP_OPT_UNRESOLVED_OK 0x01

// If a loader was added to load the entire document, this flag will allow
// later calls to GetKey in read mode to create a key (from the schema) even if it is not sortable
#define RLOOKUP_OPT_ALL_LOADED 0x02

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
int loadIndividualKeys(RLookup *it, RLookupRow *dst, RLookupLoadOptions *options);

#ifdef __cplusplus
}
#endif

#endif

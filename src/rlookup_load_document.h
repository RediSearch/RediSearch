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
#include "rlookup.h"

#ifdef __cplusplus
extern "C" {
#endif

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
   * Load only cached keys (don't open keys)
   */
  bool cachedOnly;

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

int loadIndividualKeys(RLookup *it, RLookupRow *dst, RLookupLoadOptions *options);

int RLookup_LoadDocumentAll(RLookup *lt, RLookupRow *dst, RLookupLoadOptions *options);
int RLookup_LoadDocumentIndividual(RLookup *lt, RLookupRow *dst, RLookupLoadOptions *options);

int RLookup_LoadRuleFields(RedisSearchCtx *sctx, RLookup *it, RLookupRow *dst,
                           IndexSpec *sp, const char *keyptr, QueryError *status);

#ifdef __cplusplus
}
#endif

#endif

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
#include "rejson_api.h"
#include "hiredis/sds.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  struct RedisSearchCtx *sctx;

  /** Needed for the key name, and perhaps the sortable */
  const RSDocumentMetadata *dmd;

  /* Needed for rule filter where dmd does not exist */
  const char *keyPtr;

  /* Optional already-open, pinned key handle (e.g. the value pinned for an
   * AsyncScan callback). When set and the document is a hash, the loader
   * reuses it instead of reopening `keyPtr` by name, and never closes it (the
   * handle is borrowed, owned by the caller). NULL means open by name as
   * before. JSON ignores this: its value is fetched via a separate RedisJSON
   * handle opened by name, so the borrowed RedisModuleKey does not apply. */
  RedisModuleKey *openKey;

  DocumentType type;

  /** Keys to load. If present, then loadNonCached and loadAllFields is ignored */
  const RLookupKey **keys;

  /** Number of keys in keys array */
  size_t nkeys;

  /**
   * Optional array (length `nkeys`, parallel to `keys`) of JSONPaths pre-compiled once from each
   * key's path, so JSON loading does not recompile the same path for every document (MOD-16899).
   * Entry `i` is NULL when key `i` is not a JSON path (does not start with `$`), failed to
   * compile, or when RedisJSON is older than the version exposing `getWithPath`. NULL when not
   * precompiled at all (the loader then falls back to the string-based `japi->get`).
   * Owned by the loader; built by `LoadOptions_CompileKeyPaths` and freed by
   * `LoadOptions_FreeCompiledPaths`.
   */
  JSONPath *compiledPaths;

  /**
   * Scratch slot set to `compiledPaths[i]` for the key currently being loaded, so the fixed
   * `GetKeyFunc` signature does not need the key index. NULL selects the string-path fallback.
   */
  JSONPath activeCompiledPath;

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

/**
 * Precompile the JSONPath of each key in `options->keys` (once), storing the handles in a freshly
 * allocated `options->compiledPaths`. No-op (leaves `compiledPaths` NULL) when there are no keys,
 * RedisJSON is not loaded, or the loaded RedisJSON does not expose `getWithPath` (api ver < 9), in
 * which case JSON loading transparently falls back to the string-based `japi->get`.
 */
void LoadOptions_CompileKeyPaths(RLookupLoadOptions *options);

/** Free the handles allocated by `LoadOptions_CompileKeyPaths` and the array itself. */
void LoadOptions_FreeCompiledPaths(RLookupLoadOptions *options);

int loadIndividualKeys(RLookup *it, RLookupRow *dst, RLookupLoadOptions *options);

int RLookup_LoadDocumentAll(RLookup *lt, RLookupRow *dst, RLookupLoadOptions *options);
int RLookup_LoadDocumentIndividual(RLookup *lt, RLookupRow *dst, RLookupLoadOptions *options);

// added as entry point for the rust code
// Required from Rust therefore exposed as a non-"inline static" function here.
size_t sdslen_rust(const sds s);

#ifdef __cplusplus
}
#endif

#endif

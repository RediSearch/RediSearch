/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "disk_indexer.h"

#include "indexer.h"
#include "indexer_internal.h"

#include "doc_id_meta.h"
#include "forward_index.h"
#include "gc.h"
#include "info/global_stats.h"
#include "info/index_error.h"
#include "query_error_ffi.h"
#include "redis_index.h"
#include "redismodule.h"
#include "rmutil/rm_assert.h"
#include "search_disk.h"
#include "spec.h"
#include "suffix.h"
#include "varint_ffi.h"
#include "vector_index.h"
#include "VecSim/vec_sim.h"

#include <stdint.h>

// DocIdMeta access from the indexing pipeline. When the add-document context
// carries an already-open key handle (supplied by callers that hold the key
// open and pinned, e.g. the async scan key callback), these reuse it via the
// *WithKey variants instead of reopening the key by name; otherwise they fall
// back to the name-based variants, which open and close the key themselves.
// Centralizing the openKey check here keeps every DocIdMeta access on the
// indexing path consistent.
static int actxDocIdMetaGet(RSAddDocumentCtx *aCtx, RedisSearchCtx *ctx, uint64_t *docId) {
  return aCtx->disk.openKey
             ? DocIdMeta_GetWithOpenKey(aCtx->disk.openKey, ctx->spec->specId, docId)
             : DocIdMeta_Get(ctx->redisCtx, aCtx->doc->docKey, ctx->spec->specId, docId);
}

static int actxDocIdMetaSet(RSAddDocumentCtx *aCtx, RedisSearchCtx *ctx, uint64_t docId) {
  return aCtx->disk.openKey
             ? DocIdMeta_SetWithOpenKey(aCtx->disk.openKey, ctx->spec->specId, docId)
             : DocIdMeta_Set(ctx->redisCtx, aCtx->doc->docKey, ctx->spec->specId, docId);
}

void DiskIndexer_StageDocument(RSAddDocumentCtx *aCtx, RedisSearchCtx *ctx) {
  IndexSpec *spec = ctx->spec;
  RS_ASSERT(spec->diskSpec);
  size_t len;
  const char *key = RedisModule_StringPtrLen(aCtx->doc->docKey, &len);
  uint32_t oldLen = 0;

  // Check if the document has expiration time (disk does not support field-level expiration yet)
  if (aCtx->doc->docExpirationTime.tv_sec || aCtx->doc->docExpirationTime.tv_nsec) {
    aCtx->docFlags |= Document_HasExpiration;
  }

  // Get old docId from key metadata (if document already exists). Stashed
  // on `aCtx` so `applyDocTable` can drop the old VecSim / geometry entries
  // once the batch has committed.
  // TODO: Consider calling this from SearchDisk_PutDocument
  uint64_t oldDocId = 0;
  actxDocIdMetaGet(aCtx, ctx, &oldDocId);
  aCtx->disk.oldDocId = oldDocId;

  // Open a per-document write batch that doc-table / inverted-index / tag-index writes
  // will be staged into. The batch is committed (or aborted on error) by
  // `Indexer_Process` once all of `aCtx`'s indexing work has finished.
  aCtx->disk.batch = SearchDisk_CreateWriteBatch(spec);

  // Stage the doc-table write and obtain the new doc-id. The doc-id is
  // assigned synchronously even though the batch has not yet committed.
  t_docId docId = aCtx->disk.batch
    ? SearchDisk_PutDocument(spec->diskSpec, aCtx->disk.batch, key, len,
        aCtx->doc->score, aCtx->docFlags, aCtx->fwIdx->maxTermFreq,
        aCtx->fwIdx->totalFreq, &oldLen, aCtx->doc->docExpirationTime, oldDocId)
    : 0;

  // `SearchDisk_CreateWriteBatch` / `SearchDisk_PutDocument` failure
  // (typically OOM / disk-init failure) is treated as fatal: by the time
  // we are here the disk module is in an unrecoverable state, and the
  // alternative — best-effort cleanup of a partially-staged batch — can
  // itself fail and leave permanent in-memory / on-disk divergence.
  // Crash so the server restarts from a well-defined state.
  RS_LOG_ASSERT_FMT_ALWAYS(docId != 0, "Disk staging failed: %s",
                           aCtx->disk.batch ? "SearchDisk_PutDocument returned 0"
                                            : "SearchDisk_CreateWriteBatch returned NULL");

  aCtx->doc->docId = docId;
  aCtx->disk.oldDocLen = oldLen;
  // No in-memory mutations here — the post-commit apply step in
  // `DiskIndexer_IndexDocument` runs them once the batch has committed.
  // Subsequent stagers read `aCtx->doc->docId` directly, so it is safe
  // to reference even before commit.
}

/**
 * Disk-mode full-text staging: write the per-term postings for each
 * forward-index entry onto `aCtx->disk.batch`. Each entry's `staged` flag
 * captures whether the per-term stage succeeded, so `applyTextIndex` can
 * decide whether to bump the term trie once the batch has committed.
 */
static void stageText(RSAddDocumentCtx *aCtx, RedisSearchCtx *ctx) {
  RS_LOG_ASSERT(ctx, "ctx should not be NULL");
  IndexSpec *spec = ctx->spec;
  RS_ASSERT(spec->diskSpec);
  ForwardIndexIterator it = ForwardIndex_Iterate(aCtx->fwIdx);
  for (ForwardIndexEntry *entry = ForwardIndexIterator_Next(&it); entry;
       entry = ForwardIndexIterator_Next(&it)) {
    const uint8_t *offsets = NULL;
    size_t offsetsLen = 0;
    if ((spec->flags & Index_StoreTermOffsets) && entry->vw) {
      offsets = VVW_GetByteData(entry->vw);
      offsetsLen = VVW_GetByteLength(entry->vw);
    }
    entry->staged = SearchDisk_IndexTerm(spec->diskSpec, aCtx->disk.batch,
                                         entry->term, entry->len, aCtx->doc->docId,
                                         entry->fieldMask, entry->freq,
                                         offsets, offsetsLen);
  }
}

/**
 * Disk-mode staging for non-fulltext fields: loop over indexable fields and
 * stage each onto `aCtx->disk.batch` via `IndexerBulkAdd`. The matching
 * in-memory bookkeeping is deferred to `bulkApplyFields`, which runs only
 * if the batch commit succeeded.
 *
 * On the first stage failure, marks `ACTX_F_ERRORED` and bails — the upstream
 * `commitDocument` will abort the batch.
 */
static void bulkStageFields(RSAddDocumentCtx *aCtx, RedisSearchCtx *sctx) {
  if (aCtx->stateFlags & (ACTX_F_OTHERINDEXED | ACTX_F_ERRORED)) return;

  const Document *doc = aCtx->doc;
  for (size_t ii = 0; ii < doc->numFields; ++ii) {
    const FieldSpec *fs = aCtx->fspecs + ii;
    FieldIndexerData *fdata = aCtx->fdatas + ii;
    if (fs->types == INDEXFLD_T_FULLTEXT || !FieldSpec_IsIndexable(fs) || fdata->isNull) {
      continue;
    }
    if (IndexerBulkAdd(aCtx, sctx, doc->fields + ii, fs, fdata, &aCtx->status) != 0) {
      IndexError_AddQueryError(&aCtx->spec->stats.indexError, &aCtx->status, doc->docKey);
      FieldSpec_AddQueryError(&aCtx->spec->fields[fs->index], &aCtx->status, doc->docKey);
      QueryError_ClearError(&aCtx->status);
      aCtx->stateFlags |= ACTX_F_ERRORED;
      return;
    }
  }
  aCtx->stateFlags |= ACTX_F_OTHERINDEXED;
}

/**
 * Disk-only commit fence: finalize the per-document write batch. Aborts on
 * upstream error or commits it; on success the caller
 * (`DiskIndexer_IndexDocument`) proceeds to the post-commit apply step.
 *
 * Returns true iff the batch committed cleanly and the apply step should run.
 */
static bool commitDocument(RSAddDocumentCtx *aCtx, RedisSearchCtx *ctx) {
  if (aCtx->stateFlags & ACTX_F_ERRORED) {
    // `DiskIndexer_StageDocument` crashes via `RS_LOG_ASSERT_FMT_ALWAYS` on
    // batch-open / staging failure, so reaching here implies the batch is non-NULL.
    SearchDisk_AbortWriteBatch(aCtx->disk.batch);
    // `bulkStageFields` records the originating field error in stats and then
    // clears `aCtx->status`, so by the time we get here `aCtx->status` may be
    // empty. Ensure the reply path sees an error.
    if (!QueryError_HasError(&aCtx->status)) {
      QueryError_SetError(&aCtx->status, QUERY_ERROR_CODE_GENERIC,
                          "Document indexing failed; disk write batch aborted");
    }
    return false;
  }

  if (!SearchDisk_CommitWriteBatch(aCtx->disk.batch)) {
    if (!QueryError_HasError(&aCtx->status)) {
      QueryError_SetError(&aCtx->status, QUERY_ERROR_CODE_GENERIC,
                          "Failed to commit disk write batch");
    }
    aCtx->stateFlags |= ACTX_F_ERRORED;
    return false;
  }
  return true;
}

/**
 * Disk-mode counterpart to memory-mode `makeDocumentId` / `doAssignIds`:
 * publishes the key→docId mapping in Redis (`DocIdMeta_Set`) and folds the
 * scoring-stat deltas captured by `DiskIndexer_StageDocument`. Called by
 * `DiskIndexer_IndexDocument` after `commitDocument` reports a successful commit.
 *
 * `DocIdMeta_Set` failure here means `RedisModule_HashSet` itself failed —
 * effectively OOM / fundamentally broken Redis. The disk batch is already
 * committed (and for REPLACE the prior doc is already gone from disk) so
 * best-effort cleanup would leave the in-memory and on-disk views permanently
 * divergent. Crash via `RS_LOG_ASSERT_ALWAYS` instead so the server restarts
 * from a well-defined state.
 *
 * Memory mode applies the equivalent scoring-stat deltas inline in
 * `makeDocumentId` / `doAssignIds` so that the doc-table and stats stay in
 * sync between consecutive `Indexer_Process` calls within a chain.
 */
static void applyDocTable(RSAddDocumentCtx *aCtx, RedisSearchCtx *ctx) {
  IndexSpec *spec = ctx->spec;
  int rc = actxDocIdMetaSet(aCtx, ctx, aCtx->doc->docId);
  RS_LOG_ASSERT_ALWAYS(rc == REDISMODULE_OK, "DocIdMeta_Set failed after a successful disk commit");

  // `oldDocId` comes from the key→docId mapping in Redis. The de-index path
  // (`IndexSpec_DeleteDoc`) now clears that mapping, so in normal operation a
  // non-zero `oldDocId` means a real on-disk row is being replaced.
  // `oldDocId` (not `oldDocLen`) is the REPLACE signal for stats and GC: a
  // vector/tag/numeric-only document has no full-text tokens, so its `docLen`
  // (== `fwIdx->totalFreq`) is 0. Gating on `oldDocLen != 0` would miss those
  // replaces and leak `numDocuments` (the new doc's `Indexer_AddNewDocStats`
  // increment would never be matched by `Indexer_RemoveOldDocStats`). This mirrors
  // memory mode, which gates `Indexer_RemoveOldDocStats` on whether an old DMD
  // existed, not on its length. `oldDocLen` is still the right value to subtract
  // from `totalDocsLen` (0 for a zero-length old doc is a correct no-op subtraction).
  const bool replaced = aCtx->disk.oldDocId != 0;
  if (replaced) {
    Indexer_RemoveReplacedDocVectorAndGeometry(spec, aCtx->disk.oldDocId);
    Indexer_RemoveOldDocStats(spec, aCtx->disk.oldDocLen);
  }
  Indexer_AddNewDocStats(spec, aCtx->fwIdx->totalFreq);

  if (spec->gc) {
    if (replaced) {
      GCContext_OnUpdate(spec->gc);
    } else {
      GCContext_OnWrite(spec->gc);
    }
  }
}

/**
 * Disk-mode counterpart to `indexText`: apply the in-memory term-trie /
 * suffix-trie / stats updates that pair with the postings staged in
 * `stageText` and now durably committed. `IndexSpec_AddTerm` fires for
 * entries with `entry->staged == true` (i.e. `SearchDisk_IndexTerm` returned
 * true); `addSuffixTrie` is gated independently by `entryWantsSuffixTrie`
 * and runs regardless — matches master behavior.
 *
 * Memory mode does the equivalent work inline in `indexText`, in a single
 * pass over the forward index.
 */
static void applyTextIndex(RSAddDocumentCtx *aCtx, RedisSearchCtx *ctx) {
  IndexSpec *spec = ctx->spec;
  size_t prevNumTerms = spec->stats.scoring.numTerms;
  ForwardIndexIterator it = ForwardIndex_Iterate(aCtx->fwIdx);
  for (ForwardIndexEntry *entry = ForwardIndexIterator_Next(&it); entry;
       entry = ForwardIndexIterator_Next(&it)) {
    if (entry->staged) {
      IndexSpec_AddTerm(spec, entry->term, entry->len);
    }
    if (entryWantsSuffixTrie(spec, entry)) {
      addSuffixTrie(spec->suffix, entry->term, entry->len);
    }
  }
  FieldsGlobalStats_UpdateFieldDocsIndexed(INDEXFLD_T_FULLTEXT, spec->stats.scoring.numTerms - prevNumTerms);
}

/**
 * Disk-mode apply step for non-fulltext fields: runs the per-field-type
 * appliers (`tagApplier`, `vectorApplier`, …) defined in
 * [document.c](document.c) once per indexed field. Called from
 * `DiskIndexer_IndexDocument` after `commitDocument` reports success. Infallible.
 */
static void bulkApplyFields(RSAddDocumentCtx *aCtx) {
  const Document *doc = aCtx->doc;
  for (size_t ii = 0; ii < doc->numFields; ++ii) {
    const FieldSpec *fs = aCtx->fspecs + ii;
    FieldIndexerData *fdata = aCtx->fdatas + ii;
    if (fs->types == INDEXFLD_T_FULLTEXT || !FieldSpec_IsIndexable(fs) || fdata->isNull) {
      continue;
    }
    IndexerBulkApply(aCtx, doc->fields + ii, fs, fdata);
  }
}

/**
 * Disk-mode counterpart to memory-mode `vectorIndexer`. Runs after the
 * per-document disk batch has committed so a failed commit never leaves
 * the VecSim index referencing a doc-id that was not persisted on disk.
 *
 * The vector blobs in `fdata->vector` are borrowed and live until
 * `AddDocumentCtx_Free`, so reading them here is safe.
 */
static void applyVectorInserts(RSAddDocumentCtx *aCtx, RedisSearchCtx *ctx) {
  IndexSpec *spec = ctx->spec;
  const Document *doc = aCtx->doc;
  for (size_t ii = 0; ii < doc->numFields; ++ii) {
    const FieldSpec *fs = aCtx->fspecs + ii;
    FieldIndexerData *fdata = aCtx->fdatas + ii;
    if (!FieldSpec_IsIndexable(fs) || fdata->isNull) continue;
    if (!(doc->fields[ii].indexAs & INDEXFLD_T_VECTOR)) continue;

    VecSimIndex *vecsim = openVectorIndex(ctx->redisCtx, &spec->fields[fs->index], CREATE_INDEX);
    // The disk write already committed; a NULL here (e.g. VecSim allocation
    // failure) would leave the on-disk doc with no matching vector entry, and
    // the next RDB save would persist that divergence. Match the post-commit
    // policy used by `applyDocTable` for `DocIdMeta_Set` failure.
    RS_LOG_ASSERT_ALWAYS(vecsim, "openVectorIndex returned NULL after a successful disk commit");
    const char *curr_vec = (const char *)fdata->vector;
    for (size_t i = 0; i < fdata->numVec; i++) {
      VecSimIndex_AddVector(vecsim, curr_vec, aCtx->doc->docId);
      curr_vec += fdata->vecLen;
    }
  }
}

void DiskIndexer_IndexDocument(RSAddDocumentCtx *aCtx, RedisSearchCtx *ctx) {
  // Stage onto the per-document write batch.
  if (aCtx->fwIdx && !(aCtx->stateFlags & ACTX_F_ERRORED)) {
    stageText(aCtx, ctx);
  }
  bulkStageFields(aCtx, ctx);

  // Commit fence — returns false if the batch was aborted or the commit
  // failed; in either case the apply step must not run.
  if (!commitDocument(aCtx, ctx)) return;

  // Apply RAM bookkeeping for the durably-committed writes.
  applyDocTable(aCtx, ctx);
  if (aCtx->fwIdx) applyTextIndex(aCtx, ctx);
  bulkApplyFields(aCtx);
  applyVectorInserts(aCtx, ctx);
}

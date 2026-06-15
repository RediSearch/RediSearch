/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "indexer.h"
#include "forward_index.h"
#include "inverted_index.h"
#include "inverted_index_ffi.h"
#include "sorting_vector_ffi.h"
#include "geo_index.h"
#include "vector_index.h"
#include "redis_index.h"
#include "suffix.h"
#include "config.h"
#include "rmutil/rm_assert.h"
#include "phonetic_manager.h"
#include "obfuscation/obfuscation_api.h"
#include "redismodule.h"
#include "debug_commands.h"
#include "search_disk.h"
#include "info/global_stats.h"
#include "gc.h"
#include "doc_id_meta.h"
#include "metrics_ffi.h"
#include "tag_index.h"

extern RedisModuleCtx *RSDummyContext;

#include <unistd.h>

static void writeIndexEntry(IndexSpec *spec, InvertedIndex *idx, ForwardIndexEntry *entry) {
  AddRecordOutcome r = InvertedIndex_WriteForwardIndexEntry(idx, entry);

  // Update index statistics:

  // Number of additional bytes
  spec->stats.invertedSize += r.mem_growth;
  IndexStats_BlockCountAdd(&spec->stats, r.blocks_added);
  // Number of records
  spec->stats.numRecords++;

  /* Record the space saved for offset vectors */
  if (spec->flags & Index_StoreTermOffsets) {
    spec->stats.offsetVecsSize += VVW_GetByteLength(entry->vw);
    spec->stats.offsetVecRecords += VVW_GetCount(entry->vw);
  }
}

// Number of terms for each block-allocator block
#define TERMS_PER_BLOCK 128

// Effectively limits the maximum number of documents whose terms can be merged
#define MAX_BULK_DOCS 1024

// Entry for the merged dictionary
typedef struct mergedEntry {
  KHTableEntry base;        // Base structure
  ForwardIndexEntry *head;  // First document containing the term
  ForwardIndexEntry *tail;  // Last document containing the term
} mergedEntry;

// Boilerplate hashtable compare function
static int mergedCompare(const KHTableEntry *ent, const void *s, size_t n, uint32_t h) {
  mergedEntry *e = (mergedEntry *)ent;
  // 0 return value means "true"
  return !(e->head->hash == h && e->head->len == n && memcmp(e->head->term, s, n) == 0);
}

// Boilerplate hash retrieval function. Used for rebalancing the table
static uint32_t mergedHash(const KHTableEntry *ent) {
  mergedEntry *e = (mergedEntry *)ent;
  return e->head->hash;
}

// Boilerplate dict entry allocator
static KHTableEntry *mergedAlloc(void *ctx) {
  return BlkAlloc_Alloc(ctx, sizeof(mergedEntry), sizeof(mergedEntry) * TERMS_PER_BLOCK);
}

// This function used for debugging, and returns how many items are actually in the list
static size_t countMerged(mergedEntry *ent) {
  size_t n = 0;
  for (ForwardIndexEntry *cur = ent->head; cur; cur = cur->next) {
    n++;
  }
  return n;
}

// Returns true on terms that should be indexed in the suffix trie.
static inline bool entryWantsSuffixTrie(const IndexSpec *spec, const ForwardIndexEntry *entry) {
  return (spec->suffixMask & entry->fieldMask)
      && entry->term[0] != STEM_PREFIX
      && entry->term[0] != PHONETIC_PREFIX
      && entry->term[0] != SYNONYM_PREFIX_CHAR
      && strlen(entry->term) != 0;
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
 * Memory-mode full-text indexing: in a single pass over the forward index,
 * write each term's posting into the inverted index and apply the matching
 * trie / suffix-trie / stats bookkeeping inline. There is no commit fence in
 * memory mode, so writes and the matching bookkeeping happen together — a
 * later field's failure cannot orphan this work.
 *
 * `IndexSpec_AddTerm` is gated by the master MOD-4140 perf rule: only the
 * first occurrence of a term in the spec triggers the term-trie update. See
 * MOD-15846 for the downstream `numDocs` / IDF impact and the planned fix.
 * `addSuffixTrie` is gated independently by `entryWantsSuffixTrie` and runs
 * regardless of whether the term is new — matches master behavior.
 */
static void indexText(RSAddDocumentCtx *aCtx, RedisSearchCtx *ctx) {
  RS_LOG_ASSERT(ctx, "ctx should not be NULL");
  IndexSpec *spec = ctx->spec;
  size_t prevNumTerms = spec->stats.scoring.numTerms;
  ForwardIndexIterator it = ForwardIndex_Iterate(aCtx->fwIdx);
  for (ForwardIndexEntry *entry = ForwardIndexIterator_Next(&it); entry;
       entry = ForwardIndexIterator_Next(&it)) {
    bool isNew;
    InvertedIndex *invidx = Redis_OpenInvertedIndex(spec, entry->term, entry->len, 1, &isNew);
    if (invidx) {
      entry->docId = aCtx->doc->docId;
      RS_LOG_ASSERT(entry->docId, "docId should not be 0");
      writeIndexEntry(spec, invidx, entry);
    }
    if (isNew && strlen(entry->term) != 0) {
      IndexSpec_AddTerm(spec, entry->term, entry->len);
    }
    if (entryWantsSuffixTrie(spec, entry)) {
      addSuffixTrie(spec->suffix, entry->term, entry->len);
    }
  }
  FieldsGlobalStats_UpdateFieldDocsIndexed(INDEXFLD_T_FULLTEXT, spec->stats.scoring.numTerms - prevNumTerms);
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
 * Drop the replaced document's VecSim and Geometry entries.
 *
 * These two index types live in memory in both memory mode and disk mode (the
 * inverted-index / tag / doc-table cleanup is handled by `SearchDisk_PutDocument`
 * in disk mode and by `DocTable_PopR` in memory mode — neither covers VecSim or
 * Geometry, hence this dedicated step). Memory mode calls this inline from
 * `makeDocumentId` before the new DMD is allocated; disk mode calls it from
 * `applyDocTable` after the disk batch commits.
 *
 * `VecSimIndex_DeleteVector` and `GeometryIndex_RemoveId` no-op on unknown
 * doc-ids, so this is safe even if the replaced doc had no vector / geometry
 * data, and safe to call defensively on stale key-meta in disk mode.
 */
static void removeReplacedDocVectorAndGeometry(IndexSpec *spec, t_docId oldDocId) {
  if (spec->flags & Index_HasVecSim) {
    for (int i = 0; i < spec->numFields; ++i) {
      if (spec->fields[i].types == INDEXFLD_T_VECTOR) {
        // ctx is NULL because we don't create the index here
        VecSimIndex *vecsim = openVectorIndex(NULL, &spec->fields[i], DONT_CREATE_INDEX);
        if (!vecsim) continue;
        VecSimIndex_DeleteVector(vecsim, oldDocId);
        // TODO: use VecSimReplace instead and if successful, do not insert and remove from doc
      }
    }
  }
  if (spec->flags & Index_HasGeometry) {
    GeometryIndex_RemoveId(spec, oldDocId);
  }
}

/**
 * Remove the old document's contributions from the spec's scoring stats on
 * REPLACE. Paired with `addNewDocStats`. Memory mode passes `dmd->docLen`
 * from the popped DMD; disk mode passes `aCtx->disk.oldDocLen` captured by
 * `SearchDisk_PutDocument`.
 */
static void removeOldDocStats(IndexSpec *spec, uint32_t oldDocLen) {
  RS_LOG_ASSERT(spec->stats.scoring.numDocuments > 0, "numDocuments cannot be negative");
  --spec->stats.scoring.numDocuments;
  RS_LOG_ASSERT(spec->stats.scoring.totalDocsLen >= oldDocLen,
                "totalDocsLen is smaller than oldDocLen");
  spec->stats.scoring.totalDocsLen -= oldDocLen;
}

/**
 * Add the new document's contributions to the spec's scoring stats. Paired
 * with `removeOldDocStats`. Both flows pass `fwIdx->totalFreq` as the new
 * doc's length.
 */
static void addNewDocStats(IndexSpec *spec, uint32_t newDocLen) {
  ++spec->stats.scoring.numDocuments;
  spec->stats.scoring.totalDocsLen += newDocLen;
}

/** Assigns a document ID to a single document. Handles only RAM index */
static RSDocumentMetadata *makeDocumentId(RedisModuleCtx *ctx, RSAddDocumentCtx *aCtx, IndexSpec *spec,
                                          int replace, bool *updated) {
  DocTable *table = &spec->docs;
  Document *doc = aCtx->doc;
  if (replace) {
    RSDocumentMetadata *dmd = DocTable_PopR(table, doc->docKey);
    if (dmd) {
      // Drop the old doc's stats + auxiliary in-memory indexes. The new doc's
      // stats are folded in by the caller via `addNewDocStats`.
      removeOldDocStats(spec, dmd->docLen);
      removeReplacedDocVectorAndGeometry(spec, dmd->id);
      *updated = true;
      DMD_Return(dmd);
    }
  }

  size_t n;
  const char *s = RedisModule_StringPtrLen(doc->docKey, &n);
  RSDocumentMetadata *dmd =
      DocTable_Put(table, s, n, doc->score, aCtx->docFlags, doc->payload, doc->payloadSize, doc->type);
  if (dmd) {
    doc->docId = dmd->id;
  }

  return dmd;
}

// DocIdMeta access from the indexing pipeline. When the add-document context
// carries an already-open key handle (supplied by callers that hold the key
// open and pinned, e.g. the async scan key callback), these reuse it via the
// *WithKey variants instead of reopening the key by name; otherwise they fall
// back to the name-based variants, which open and close the key themselves.
// Centralizing the openKey check here keeps every DocIdMeta access on the
// indexing path consistent.
static int actxDocIdMetaGet(RSAddDocumentCtx *aCtx, RedisSearchCtx *ctx, uint64_t *docId) {
  return aCtx->disk.openKey
             ? DocIdMeta_GetWithKey(aCtx->disk.openKey, ctx->spec->specId, docId)
             : DocIdMeta_Get(ctx->redisCtx, aCtx->doc->docKey, ctx->spec->specId, docId);
}

static int actxDocIdMetaSet(RSAddDocumentCtx *aCtx, RedisSearchCtx *ctx, uint64_t docId) {
  return aCtx->disk.openKey
             ? DocIdMeta_SetWithKey(aCtx->disk.openKey, ctx->spec->specId, docId)
             : DocIdMeta_Set(ctx->redisCtx, aCtx->doc->docKey, ctx->spec->specId, docId);
}

/**
 * Performs bulk document ID assignment to all items in the queue.
 * If one item cannot be assigned an ID, it is marked as being errored.
 *
 * Disk mode opens a fresh per-document write batch, stages the doc-table
 * write onto it, and assigns the new doc-id synchronously. The matching
 * in-memory updates (`DocIdMeta_Set`, scoring stats, GC notification) are
 * deferred to `applyDocTable`, which runs once the batch has committed.
 *
 * Memory mode runs unchanged — the doc-id assignment and all RAM mutations
 * happen inline here.
 *
 * This function also sets the document's sorting vector, if present.
 */
static void doAssignIds(RSAddDocumentCtx *cur, RedisSearchCtx *ctx) {
  IndexSpec *spec = ctx->spec;
  for (; cur; cur = cur->next) {
    if (cur->stateFlags & ACTX_F_ERRORED) {
      continue;
    }

    RS_ASSERT(cur->doc);
    if (SearchDisk_IsEnabled()) {
      RS_ASSERT(spec->diskSpec);
      size_t len;
      const char *key = RedisModule_StringPtrLen(cur->doc->docKey, &len);
      uint32_t oldLen = 0;

      // Check if the document has expiration time (disk does not support field-level expiration yet)
      if (cur->doc->docExpirationTime.tv_sec || cur->doc->docExpirationTime.tv_nsec) {
        cur->docFlags |= Document_HasExpiration;
      }

      // Get old docId from key metadata (if document already exists). Stashed
      // on `cur` so `applyDocTable` can drop the old VecSim / geometry entries
      // once the batch has committed.
      // TODO: Consider calling this from SearchDisk_PutDocument
      uint64_t oldDocId = 0;
      actxDocIdMetaGet(cur, ctx, &oldDocId);
      cur->disk.oldDocId = oldDocId;

      // Open a per-document write batch that doc-table / inverted-index / tag-index writes
      // will be staged into. The batch is committed (or aborted on error) by
      // `Indexer_Process` once all of `cur`'s indexing work has finished.
      cur->disk.batch = SearchDisk_CreateWriteBatch(spec->diskSpec);

      // Stage the doc-table write and obtain the new doc-id. The doc-id is
      // assigned synchronously even though the batch has not yet committed.
      t_docId docId = cur->disk.batch
        ? SearchDisk_PutDocument(spec->diskSpec, cur->disk.batch, key, len,
            cur->doc->score, cur->docFlags, cur->fwIdx->maxTermFreq,
            cur->fwIdx->totalFreq, &oldLen, cur->doc->docExpirationTime, oldDocId)
        : 0;

      // `SearchDisk_CreateWriteBatch` / `SearchDisk_PutDocument` failure
      // (typically OOM / disk-init failure) is treated as fatal: by the time
      // we are here the disk module is in an unrecoverable state, and the
      // alternative — best-effort cleanup of a partially-staged batch — can
      // itself fail and leave permanent in-memory / on-disk divergence.
      // Crash so the server restarts from a well-defined state.
      RS_LOG_ASSERT_FMT_ALWAYS(docId != 0, "Disk staging failed: %s",
                               cur->disk.batch ? "SearchDisk_PutDocument returned 0"
                                               : "SearchDisk_CreateWriteBatch returned NULL");

      cur->doc->docId = docId;
      cur->disk.oldDocLen = oldLen;
      // No in-memory mutations here — the post-commit apply step in
      // `indexDocumentDisk` runs them once the batch has committed.
      // Subsequent stagers read `cur->doc->docId` directly, so it is safe
      // to reference even before commit.
    } else {
      RS_LOG_ASSERT(!cur->doc->docId, "docId must be 0");
      bool updated = false;
      RSDocumentMetadata *md = makeDocumentId(ctx->redisCtx, cur, spec,
                                              cur->options & DOCUMENT_ADD_REPLACE, &updated);
      if (!md) {
        cur->stateFlags |= ACTX_F_ERRORED;
        continue;
      }

      md->maxTermFreq = cur->fwIdx->maxTermFreq;
      md->docLen = cur->fwIdx->totalFreq;
      addNewDocStats(spec, md->docLen);

      if (RSSortingVector_Length(&cur->sv)) {
        DocTable_SetSortingVector(&spec->docs, md, cur->sv);
        cur->sv = RSSortingVector_Empty();
      }

      if (cur->byteOffsets) {
        ByteOffsetWriter_Move(&cur->offsetsWriter, cur->byteOffsets);
        DocTable_SetByteOffsets(md, cur->byteOffsets);
        cur->byteOffsets = NULL;
      }
      Document* doc = cur->doc;
      const bool hasExpiration = doc->docExpirationTime.tv_sec || doc->docExpirationTime.tv_nsec || doc->fieldExpirations;
      if (hasExpiration) {
        // No need to mark the DMD with Document_HasExpiration: the result
        // processor already fetches the DMD from the doc table on every hit,
        // so it can read `expirationTimeNs` directly without going through
        // a flag-gated branch.
        DocTable_UpdateExpiration(&ctx->spec->docs, md, doc->docExpirationTime, doc->fieldExpirations);

        doc->fieldExpirations = NULL; // Moved to DocTable (TTL table actually)
      }
      DMD_Return(md);

      if (spec->gc) {
        if (updated) {
          GCContext_OnUpdate(spec->gc);
        } else {
          GCContext_OnWrite(spec->gc);
        }
      }
    }
  }
}

/**
 * Disk-mode counterpart to memory-mode `makeDocumentId` / `doAssignIds`:
 * publishes the key→docId mapping in Redis (`DocIdMeta_Set`) and folds the
 * scoring-stat deltas captured by `doAssignIds`. Called by
 * `indexDocumentDisk` after `commitDocument` reports a successful commit.
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
  // replaces and leak `numDocuments` (the new doc's `addNewDocStats` increment
  // would never be matched by `removeOldDocStats`). This mirrors memory mode,
  // which gates `removeOldDocStats` on whether an old DMD existed, not on its
  // length. `oldDocLen` is still the right value to subtract from `totalDocsLen`
  // (0 for a zero-length old doc is a correct no-op subtraction).
  const bool replaced = aCtx->disk.oldDocId != 0;
  if (replaced) {
    removeReplacedDocVectorAndGeometry(spec, aCtx->disk.oldDocId);
    removeOldDocStats(spec, aCtx->disk.oldDocLen);
  }
  addNewDocStats(spec, aCtx->fwIdx->totalFreq);

  if (spec->gc) {
    if (replaced) {
      GCContext_OnUpdate(spec->gc);
    } else {
      GCContext_OnWrite(spec->gc);
    }
  }
}

/**
 * Memory-mode non-fulltext indexing: loop over indexable fields, calling
 * `IndexerBulkAdd` (writes inline) followed by `IndexerBulkApply` (in-memory
 * bookkeeping) per field. The apply runs as part of the same iteration so
 * that a later field's failure cannot orphan earlier fields' bookkeeping.
 *
 * On the first add failure, marks `ACTX_F_ERRORED` and bails. Earlier fields
 * stay fully applied; later fields are skipped entirely.
 */
static void bulkIndexFields(RSAddDocumentCtx *aCtx, RedisSearchCtx *sctx) {
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
    IndexerBulkApply(aCtx, doc->fields + ii, fs, fdata);
  }
  aCtx->stateFlags |= ACTX_F_OTHERINDEXED;
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
 * Disk-mode apply step for non-fulltext fields: runs the per-field-type
 * appliers (`tagApplier`, `vectorApplier`, …) defined in
 * [document.c](document.c) once per indexed field. Called from
 * `indexDocumentDisk` after `commitDocument` reports success. Infallible.
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

static void reopenCb(void *arg) {}

// Routines for the merged hash table
#define ACTX_IS_INDEXED(actx)                                           \
  (((actx)->stateFlags & (ACTX_F_OTHERINDEXED | ACTX_F_TEXTINDEXED)) == \
   (ACTX_F_OTHERINDEXED | ACTX_F_TEXTINDEXED))

// Index missing field docs.
// Add field names to missingFieldDict if it is missing in the document
// and add the doc to its corresponding inverted index
static void writeMissingFieldDocs(RSAddDocumentCtx *aCtx, RedisSearchCtx *sctx, arrayof(FieldExpiration) sortedFieldWithExpiration) {
  Document *doc = aCtx->doc;
  IndexSpec *spec = sctx->spec;
  // We use a dictionary as a set, to keep all the fields that we've seen so far (optimization)
  dict *df_fields_dict = dictCreate(&dictTypeHeapHiddenStrings, NULL);

  // collect missing fields in schema
  for (t_fieldIndex i = 0; i < spec->numFields; i++) {
    FieldSpec *fs = spec->fields + i;
    if (FieldSpec_IndexesMissing(fs)) {
      dictAdd(df_fields_dict, (void*)fs->fieldName, fs);
    }
  }

  // if there are no missing fields then there is nothing to index
  if (dictSize(df_fields_dict) == 0) {
    dictRelease(df_fields_dict);
    return;
  }

  // remove fields that are in the document
  for (uint32_t j = 0; j < doc->numFields; j++) {
    dictDelete(df_fields_dict, (void*)doc->fields[j].docFieldName);
  }

  // add indexmissing fields that are in the document but are marked to be expired at some point
  for (uint32_t sortedIndex = 0; sortedIndex < array_len(sortedFieldWithExpiration); sortedIndex++) {
    FieldExpiration* fe = &sortedFieldWithExpiration[sortedIndex];
    FieldSpec* fs = spec->fields + fe->index;
    if (!FieldSpec_IndexesMissing(fs)) {
      continue;
    }
    dictAdd(df_fields_dict, (void*)fs->fieldName, fs);
  }

  // go over all the potentially missing fields and index the document in the matching inverted index
  dictIterator* iter = dictGetIterator(df_fields_dict);
  for (dictEntry *entry = dictNext(iter); entry; entry = dictNext(iter)) {
    const FieldSpec *fs = dictGetVal(entry);
    InvertedIndex *iiMissingDocs = dictFetchValue(spec->missingFieldDict, fs->fieldName);
    if (iiMissingDocs == NULL) {
      size_t index_size;
      iiMissingDocs = NewInvertedIndex(Index_DocIdsOnly, &index_size);
      aCtx->spec->stats.invertedSize += index_size;
      dictAdd(spec->missingFieldDict, (void*)fs->fieldName, iiMissingDocs);
    }
    // Add docId to inverted index
    t_docId docId = aCtx->doc->docId;
    RSIndexResult rec = {.data.tag = RSResultData_Virtual, .docId = docId, .freq = 0,
                         .metrics = MetricsVec_New()};
    AddRecordOutcome r = InvertedIndex_WriteEntryGeneric(iiMissingDocs, &rec);
    aCtx->spec->stats.invertedSize += r.mem_growth;
    IndexStats_BlockCountAdd(&aCtx->spec->stats, r.blocks_added);
  }
  dictReleaseIterator(iter);
  dictRelease(df_fields_dict);
}

// Index the doc in the existing docs inverted index
static void writeExistingDocs(RSAddDocumentCtx *aCtx, RedisSearchCtx *sctx) {
  if (!sctx->spec->rule || !sctx->spec->rule->index_all) {
    return;
  }
  if (!sctx->spec->existingDocs) {
    // Create the inverted index if it doesn't exist
    size_t index_size;
    aCtx->spec->existingDocs = NewInvertedIndex(Index_DocIdsOnly, &index_size);
    aCtx->spec->stats.invertedSize += index_size;
  }

  t_docId docId = aCtx->doc->docId;
  RSIndexResult rec = {.data.tag = RSResultData_Virtual, .docId = docId, .freq = 0,
                       .metrics = MetricsVec_New()};
  AddRecordOutcome r = InvertedIndex_WriteEntryGeneric(sctx->spec->existingDocs, &rec);
  aCtx->spec->stats.invertedSize += r.mem_growth;
  IndexStats_BlockCountAdd(&aCtx->spec->stats, r.blocks_added);
}

/**
 * Disk-only commit fence: finalize the per-document write batch. Aborts on
 * upstream error or commits it; on success the caller (`indexDocumentDisk`)
 * proceeds to the post-commit apply step.
 *
 * Returns true iff the batch committed cleanly and the apply step should run.
 */
static bool commitDocument(RSAddDocumentCtx *aCtx, RedisSearchCtx *ctx) {
  if (aCtx->stateFlags & ACTX_F_ERRORED) {
    // `doAssignIds` crashes via `RS_LOG_ASSERT_FMT_ALWAYS` on batch-open /
    // staging failure, so reaching here implies the batch is non-NULL.
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
 * Memory-mode per-document pipeline. No commit fence and no deferred bookkeeping:
 * each field's write and its matching in-memory bookkeeping run as a single
 * atomic chunk (see `indexText` and `bulkIndexFields`). A later field's
 * failure cannot orphan an earlier field's writes.
 *
 * Doc-table scoring-stat deltas + GC are applied inline in `makeDocumentId` /
 * `doAssignIds`, so there is no `applyDocTable` step here.
 */
static void indexDocumentMemory(RSAddDocumentCtx *aCtx, RedisSearchCtx *ctx,
                                arrayof(FieldExpiration) fes) {
  if (aCtx->fwIdx && !(aCtx->stateFlags & ACTX_F_ERRORED)) {
    indexText(aCtx, ctx);
  }
  bulkIndexFields(aCtx, ctx);
  writeExistingDocs(aCtx, ctx);
  writeMissingFieldDocs(aCtx, ctx, fes);
}

/**
 * Disk-mode per-document pipeline. Three steps with a commit fence between
 * the durable writes and the in-memory bookkeeping that pairs with them:
 *
 *   - Stage: write the doc-table / inverted-index / tag-index entries onto
 *     `aCtx->disk.batch` (`stageText`, `bulkStageFields`).
 *   - Commit fence: `commitDocument` aborts on error or commits the batch;
 *     returns false iff the batch did not become durable.
 *   - Apply: only runs on a successful commit. Updates the RAM-side state
 *     that paired with the now-durable disk writes (`applyDocTable`,
 *     `applyTextIndex`, `bulkApplyFields`, `applyVectorInserts`).
 *
 * On commit failure, the apply step is skipped — no in-memory state was
 * mutated, so there is nothing to roll back.
 *
 * Wildcard (`index_all`) and `INDEXMISSING` indexes are not supported on disk
 * specs, so the matching memory-mode hooks (`writeExistingDocs`,
 * `writeMissingFieldDocs`) are not called here.
 */
static void indexDocumentDisk(RSAddDocumentCtx *aCtx, RedisSearchCtx *ctx) {
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

/**
 * Per-document indexing entry point. Performs the shared prelude (state
 * guards, doc-id assignment, field-expiration setup) and dispatches to the
 * mode-specific pipeline.
 */
static void Indexer_Process(RSAddDocumentCtx *aCtx) {
  RSAddDocumentCtx *firstZeroId = aCtx;
  RedisSearchCtx ctx = *aCtx->sctx;

  if (ACTX_IS_INDEXED(aCtx) || aCtx->stateFlags & (ACTX_F_ERRORED)) {
    // Document is complete or errored. No need for further processing.
    if (!(aCtx->stateFlags & ACTX_F_EMPTY)) {
      return;
    }
  }

  if (!ctx.spec) {
    QueryError_SetCode(&aCtx->status, QUERY_ERROR_CODE_NO_INDEX);
    aCtx->stateFlags |= ACTX_F_ERRORED;
    return;
  }

  Document *doc = aCtx->doc;

  /**
   * Document ID & sorting-vector assignment:
   * In order to hold the GIL for as short a time as possible, we assign
   * document IDs in bulk. We begin using the first document ID that is assumed
   * to be zero.
   *
   * When merging multiple document IDs, the merge stage scans through the chain
   * of proposed documents and selects the first document in the chain missing an
   * ID - the subsequent documents should also all be missing IDs. If none of
   * the documents are missing IDs then the firstZeroId document is NULL and
   * no ID assignment takes place.
   *
   * Assigning IDs in bulk speeds up indexing of smaller documents by about
   * 10% overall.
   */
  if (firstZeroId != NULL && firstZeroId->doc->docId == 0) {
    doAssignIds(firstZeroId, &ctx);
  }

  if (SearchDisk_IsEnabled()) {
    indexDocumentDisk(aCtx, &ctx);
  } else {
    // `doc->fieldExpirations` ownership has already been moved into the TTL
    // table by `doAssignIds` on success. On failure (e.g. `makeDocumentId`
    // returned NULL), the array stays attached to `doc` so `Document_Free`
    // can release it.
    arrayof(FieldExpiration) fes =
        (arrayof(FieldExpiration))DocTable_GetFieldExpirations(&ctx.spec->docs, doc->docId);
    indexDocumentMemory(aCtx, &ctx, fes);
  }
}

int IndexDocument(RSAddDocumentCtx *aCtx) {
  Indexer_Process(aCtx);
  AddDocumentCtx_Finish(aCtx);
  return 0;
}

bool g_isLoading = false;

/**
 * Yield to Redis after a certain number of operations during indexing.
 * This helps keep Redis responsive during long indexing operations.
 * @param ctx The Redis context
 * @param numOps Tue number of operations to count in the counter before considering RSGlobalConfig.indexerYieldEveryOpsWhileLoading. These are related to the number of fields in the document
 * @param flags The flags to pass to RedisModule_Yield
 */
void IndexerYieldWhileLoading(RedisModuleCtx *ctx, unsigned int numOps, int flags) {
  static size_t opCounter = 0;

  // If server is loading, Yield to Redis if the number of operations is greater than the yieldEveryOps
  opCounter += numOps;
  if (g_isLoading && opCounter >= RSGlobalConfig.indexerYieldEveryOpsWhileLoading) {
    opCounter = opCounter % RSGlobalConfig.indexerYieldEveryOpsWhileLoading;
    IncrementLoadYieldCounter(); // Track that we called yield
    unsigned int sleepMicros = GetIndexerSleepBeforeYieldMicros();
    if (sleepMicros > 0) {
      usleep(sleepMicros);
    }
    RedisModule_Yield(ctx, flags, NULL);
  }
}

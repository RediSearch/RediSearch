/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "indexer.h"

#include "disk_indexer.h"
#include "indexer_internal.h"

#include "forward_index.h"
#include "inverted_index.h"
#include "inverted_index_ffi.h"
#include "sorting_vector_ffi.h"
#include "vector_index.h"
#include "vector_compare/vector_compare.h"
#include "redis_index.h"
#include "suffix.h"
#include "config.h"
#include "rmutil/rm_assert.h"
#include "phonetic_manager.h"
#include "redismodule.h"
#include "debug_commands.h"
#include "search_disk.h"
#include "info/global_stats.h"
#include "gc.h"
#include "doc_id_meta.h"
#include "metrics_ffi.h"
#include "module.h"
#include "util/workers.h"
#include "VecSim/vec_sim.h"
#include "byte_offsets.h"
#include "doc_table.h"
#include "geometry_index.h"
#include "index_result_rs.h"
#include "info/index_error.h"
#include "query_error.h"
#include "query_error_ffi.h"
#include "redisearch.h"
#include "rqe_core.h"
#include "rules.h"
#include "search_result_rs.h"
#include "spec.h"
#include "stemmer.h"
#include "synonym_map.h"
#include "ttl_table.h"
#include "ttl_table_rs.h"
#include "util/block_alloc.h"
#include "util/dict/dict.h"
#include "util/khtable.h"
#include "varint_ffi.h"

extern RedisModuleCtx *RSDummyContext;

#include <unistd.h>
#include <stdint.h>
#include <string.h>

static void writeIndexEntry(IndexSpec *spec, InvertedIndex *idx, ForwardIndexEntry *entry,
                            bool hasFieldExpiration) {
  AddRecordOutcome r = InvertedIndex_WriteForwardIndexEntry(idx, entry, hasFieldExpiration);

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

// Build the mask of this document's TEXT fields that carry a field-level
// expiration, in FIELD_BIT space (the same space as ForwardIndexEntry.fieldMask).
// Only text fields participate in term field masks — non-text fields have no
// `ftId` — so a term posting's inline expiration bit is set iff its field mask
// intersects this mask.
static t_fieldMask docExpiringTextFieldMask(const IndexSpec *spec, t_docId docId) {
  const struct FieldExpirationSlice fes = DocTable_GetFieldExpirations(&spec->docs, docId);
  t_fieldMask mask = 0;
  for (size_t i = 0; i < fes.len; ++i) {
    const FieldSpec *fs = &spec->fields[fes.ptr[i].index];
    if (FieldSpec_IsIndexableText(fs)) {
      mask |= FIELD_BIT(fs);
    }
  }
  return mask;
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
  // Text fields carrying a field-level expiration for this document; a posting's
  // inline expiration bit is set when its field mask overlaps this one. Read back
  // from the doc table because `doAssignIds` moved `doc->fieldExpirations` there.
  const t_fieldMask expiringTextFields = docExpiringTextFieldMask(spec, aCtx->doc->docId);
  size_t prevNumTerms = spec->stats.scoring.numTerms;
  ForwardIndexIterator it = ForwardIndex_Iterate(aCtx->fwIdx);
  for (ForwardIndexEntry *entry = ForwardIndexIterator_Next(&it); entry;
       entry = ForwardIndexIterator_Next(&it)) {
    bool isNew;
    InvertedIndex *invidx = Redis_OpenInvertedIndex(spec, entry->term, entry->len, 1, &isNew);
    if (invidx) {
      entry->docId = aCtx->doc->docId;
      RS_LOG_ASSERT(entry->docId, "docId should not be 0");
      writeIndexEntry(spec, invidx, entry, (entry->fieldMask & expiringTextFields) != 0);
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
 * This update's value for schema field `f_idx`, or NULL when this version of the document
 * carries none.
 *
 * `VectorIndex_CheckRemoveId` walks the schema while the preprocessed values are indexed by
 * document field, so the mapping is resolved here. Document fields the schema does not know are
 * zeroed whole, `index` included, so `fieldName` is what distinguishes a real field 0 from a
 * skipped one.
 */
static const FieldIndexerData *replacedFieldValue(const RSAddDocumentCtx *aCtx,
                                                  const t_fieldIndex f_idx) {
  const Document *doc = aCtx->doc;
  for (size_t ii = 0; ii < doc->numFields; ++ii) {
    const FieldSpec *fs = aCtx->fspecs + ii;
    if (!fs->fieldName || fs->index != f_idx) continue;
    const FieldIndexerData *fdata = aCtx->fdatas + ii;
    return fdata->isNull ? NULL : fdata;
  }
  return NULL;
}

/**
 * Whether field `fs`'s existing entry is left in place for the vector-insert site to move,
 * rather than deleted here — and, for a mark that is not yet settled, the point where it is.
 *
 * An unverified mark (no change set: JSON, a background scan, a server without subkey
 * notifications) is resolved by asking the index whether it already holds the value about to
 * be written. `VectorIndex_HoldsVectors` compares the stored bytes and answers false when it
 * cannot tell, so an inconclusive comparison costs a reindex rather than risking a stale
 * vector. Deciding it here rather than at the insert site keeps the delete in the step that
 * owns it: a changed vector's entry goes now, so nothing downstream has to remember to drop
 * it, and nothing is stranded if the insert never runs.
 *
 * A field with no value in this version of the document cannot be moved however confident the
 * mark is — there will be no insert site to move it — so the entry goes now too. The reachable
 * form of that is a JSON document setting the path to `null`: the field is still loaded (as
 * `FLD_VAR_T_NULL`) and so still marked, but `vectorPreprocessor` indexes nothing for it. A
 * path removed outright never becomes a document field, so it is never marked in the first
 * place and takes the ordinary delete below.
 */
static bool keepsReplacedVector(const RSAddDocumentCtx *aCtx, const FieldSpec *fs, VecSimIndex *vecsim,
                                t_docId oldDocId) {
  const ChangedField mark = AddDocumentCtx_FieldChange(aCtx, fs->index);
  if (mark == ChangedField_VerifiedYes) {
    return false;
  }

  const FieldIndexerData *fdata = replacedFieldValue(aCtx, fs->index);
  if (fdata &&
      (mark == ChangedField_VerifiedNo ||
       VectorIndex_HoldsVectors(vecsim, oldDocId, fdata->vector, fdata->numVec))) {
    aCtx->fieldChanges[fs->index] = ChangedField_VerifiedNo;
    return true;
  }
  aCtx->fieldChanges[fs->index] = ChangedField_VerifiedYes;
  return false;
}

/**
 * Drop the replaced document's entry from every VECTOR field of `spec`, except a field whose
 * entry is to be moved onto the document's new doc-id — which is left in place for the
 * vector-insert site, `VectorIndex_RelabelField`.
 *
 * This is where a relabel mark is settled. A change set makes the decision for free; without
 * one, the field's new value is compared against what the index holds. Either way the answer is
 * reached before anything is deleted, so a field that turns out to have changed loses its old
 * entry here and the insert site is left with a straight yes or no.
 *
 * `aCtx` may be NULL, in which case nothing is marked and every entry is dropped.
 * `VecSimIndex_DeleteVector` no-ops on an unknown doc-id, so this is safe even if the replaced
 * document had no vector data.
 */
static void VectorIndex_CheckRemoveId(const IndexSpec *spec, t_docId oldDocId,
                                      const RSAddDocumentCtx *aCtx) {
  for (int i = 0; i < spec->numFields; ++i) {
    FieldSpec *fs = &spec->fields[i];
    if (fs->types != INDEXFLD_T_VECTOR) continue;
    // ctx is NULL because we don't create the index here
    VecSimIndex *vecsim = openVectorIndex(NULL, fs, DONT_CREATE_INDEX);
    if (!vecsim) {
      // No index yet, so no entry to drop and none to move either, whatever the mark said.
      if (aCtx && aCtx->fieldChanges) aCtx->fieldChanges[fs->index] = ChangedField_VerifiedYes;
      continue;
    }
    if (keepsReplacedVector(aCtx, fs, vecsim, oldDocId)) continue;
    VecSimIndex_DeleteVector(vecsim, oldDocId);
  }
}

// Contract documented on the declaration in indexer_internal.h.
void Indexer_HandleReplacedDocVectorAndGeometry(IndexSpec *spec, t_docId oldDocId,
                                                RSAddDocumentCtx *aCtx) {
  if (spec->flags & Index_HasVecSim) {
    VectorIndex_CheckRemoveId(spec, oldDocId, aCtx);
  }
  if (spec->flags & Index_HasGeometry) {
    GeometryIndex_RemoveId(spec, oldDocId);
  }
}

// Contract documented on the declaration in indexer_internal.h.
void Indexer_RemoveOldDocStats(IndexSpec *spec, uint32_t oldDocLen) {
  RS_LOG_ASSERT(spec->stats.scoring.numDocuments > 0, "numDocuments cannot be negative");
  --spec->stats.scoring.numDocuments;
  RS_LOG_ASSERT(spec->stats.scoring.totalDocsLen >= oldDocLen,
                "totalDocsLen is smaller than oldDocLen");
  spec->stats.scoring.totalDocsLen -= oldDocLen;
}

// Contract documented on the declaration in indexer_internal.h.
void Indexer_AddNewDocStats(IndexSpec *spec, uint32_t newDocLen) {
  ++spec->stats.scoring.numDocuments;
  spec->stats.scoring.totalDocsLen += newDocLen;
}

// DocIdMeta access from the indexing pipeline. When the add-document context
// carries an already-open key handle (supplied by callers that hold the key
// open and pinned, e.g. the async scan key callback), these reuse it via the
// *WithKey variants instead of reopening the key by name; otherwise they fall
// back to the name-based variants, which open and close the key themselves.
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

/** Assigns a document ID to a single document. Handles only the RAM index.
 *  The key -> docId mapping is stored on the Redis key via DocIdMeta (unified
 *  with disk mode); the in-memory DocTable only maps docId -> DMD. */
static RSDocumentMetadata *newDocumentId(RedisSearchCtx *sctx, RSAddDocumentCtx *aCtx,
                                          int replace, bool *updated) {
  IndexSpec *spec = sctx->spec;
  DocTable *table = &spec->docs;
  Document *doc = aCtx->doc;

  // Existing key -> docId mapping (memory-mode analogue of the disk oldDocId
  // lookup in doAssignIds).
  uint64_t oldDocId = 0;
  actxDocIdMetaGet(aCtx, sctx, &oldDocId);
  aCtx->oldDocId = oldDocId;

  if (oldDocId) {
    if (replace) {
      // Drop the previous version + its stats/aux indexes; the mapping is
      // overwritten by the actxDocIdMetaSet below.
      RSDocumentMetadata *old = DocTable_DeleteById(table, oldDocId);
      if (old) {
        Indexer_RemoveOldDocStats(spec, old->docLen);
        Indexer_HandleReplacedDocVectorAndGeometry(spec, old->id, aCtx);
        *updated = true;
        DMD_Return(old);
      }
    } else {
      // Already indexed, not a REPLACE: return the existing DMD (former
      // DocTable_Put dedup). Fall through only if the mapping is stale.
      RSDocumentMetadata *existing = (RSDocumentMetadata *)DocTable_Borrow(table, oldDocId);
      if (existing) {
        doc->docId = existing->id;
        return existing;
      }
    }
  }

  size_t n;
  const char *s = RedisModule_StringPtrLen(doc->docKey, &n);
  RSDocumentMetadata *dmd =
      DocTable_Put(table, s, n, doc->score, aCtx->docFlags, doc->payload, doc->payloadSize, doc->type);
  if (dmd) {
    doc->docId = dmd->id;
    // Publish the key -> docId mapping. Crash on failure (matches the disk
    // post-commit policy) rather than leave a DMD with no mapping.
    int rc = actxDocIdMetaSet(aCtx, sctx, dmd->id);
    RS_LOG_ASSERT_ALWAYS(rc == REDISMODULE_OK, "DocIdMeta_Set failed while indexing in memory mode");
  }

  return dmd;
}

/**
 * Performs bulk document ID assignment to all items in the queue.
 * If one item cannot be assigned an ID, it is marked as being errored.
 *
 * Disk mode delegates to `DiskIndexer_StageDocument`; memory mode assigns the
 * doc-id and applies all RAM mutations inline here.
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
      DiskIndexer_StageDocument(cur, ctx);
    } else {
      RS_LOG_ASSERT(!cur->doc->docId, "docId must be 0");
      bool updated = false;
      RSDocumentMetadata *md = newDocumentId(ctx, cur,
                                              cur->options & DOCUMENT_ADD_REPLACE, &updated);
      if (!md) {
        cur->stateFlags |= ACTX_F_ERRORED;
        continue;
      }

      md->maxTermFreq = cur->fwIdx->maxTermFreq;
      md->docLen = cur->fwIdx->totalFreq;
      Indexer_AddNewDocStats(spec, md->docLen);

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
      const bool hasExpiration = doc->docExpirationTime.tv_sec || doc->docExpirationTime.tv_nsec || FieldExpirations_Len(&doc->fieldExpirations) > 0;
      if (hasExpiration) {
        DocTable_UpdateExpiration(&ctx->spec->docs, md, doc->docExpirationTime,
                                  DocTable_TakeFieldExpirations(&doc->fieldExpirations));
      }
      DMD_Return(md);

      handle_gc(spec, updated);
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

static void reopenCb(void *arg) {}

// Routines for the merged hash table
#define ACTX_IS_INDEXED(actx)                                           \
  (((actx)->stateFlags & (ACTX_F_OTHERINDEXED | ACTX_F_TEXTINDEXED)) == \
   (ACTX_F_OTHERINDEXED | ACTX_F_TEXTINDEXED))

// Index missing field docs.
// Add field names to missingFieldDict if it is missing in the document
// and add the doc to its corresponding inverted index
static void writeMissingFieldDocs(RSAddDocumentCtx *aCtx, RedisSearchCtx *sctx,
                                  struct FieldExpirationSlice sortedFieldWithExpiration) {
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
  for (size_t sortedIndex = 0; sortedIndex < sortedFieldWithExpiration.len; sortedIndex++) {
    const FieldExpiration* fe = &sortedFieldWithExpiration.ptr[sortedIndex];
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
      // Complete any rehashing this insert started, else later reads on different
      // threads could end up mutating the dict simultaneously, corrupting it.
      //
      // Cheap because this dict contains just IndexSpec's INDEXMISSING fields.
      //
      // dictRehash migrates a bounded number of buckets per call and returns
      // non-zero while more remain, so loop until it reports done.
      while (dictRehash(spec->missingFieldDict, 100)) {
      }
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
 * Memory-mode per-document pipeline. No commit fence and no deferred bookkeeping:
 * each field's write and its matching in-memory bookkeeping run as a single
 * atomic chunk (see `indexText` and `bulkIndexFields`). A later field's
 * failure cannot orphan an earlier field's writes.
 *
 * Doc-table scoring-stat deltas + GC are applied inline in `makeDocumentId` /
 * `doAssignIds`, so there is no `applyDocTable` step here.
 */
static void indexDocumentMemory(RSAddDocumentCtx *aCtx, RedisSearchCtx *ctx,
                                FieldExpirationSlice fes) {
  if (aCtx->fwIdx && !(aCtx->stateFlags & ACTX_F_ERRORED)) {
    indexText(aCtx, ctx);
  }
  bulkIndexFields(aCtx, ctx);
  writeExistingDocs(aCtx, ctx);
  writeMissingFieldDocs(aCtx, ctx, fes);
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
    DiskIndexer_IndexDocument(aCtx, &ctx);
  } else {
    // `doc->fieldExpirations` ownership has already been moved into the TTL
    // table by `doAssignIds` on success. On failure (e.g. `makeDocumentId`
    // returned NULL), the array stays attached to `doc` so `Document_Free`
    // can release it.
    struct FieldExpirationSlice fes = DocTable_GetFieldExpirations(&ctx.spec->docs, doc->docId);
    indexDocumentMemory(aCtx, &ctx, fes);
  }
}

int IndexDocument(RSAddDocumentCtx *aCtx) {
  Indexer_Process(aCtx);
  AddDocumentCtx_Finish(aCtx);
  return 0;
}

bool g_isLoading = false;

#define RDB_LOAD_THROTTLE_BACKOFF_US 1000

/**
 * Yield to Redis after a certain number of operations during indexing.
 * This helps keep Redis responsive during long indexing operations.
 * @param ctx The Redis context
 * @param numOps Tue number of operations to count in the counter before considering RSGlobalConfig.indexerYieldEveryOpsWhileLoading. These are related to the number of fields in the document
 * @param flags The flags to pass to RedisModule_Yield
 */
void IndexerYieldWhileLoading(RedisModuleCtx *ctx, unsigned int numOps, int flags) {
  static size_t opCounter = 0;

  if (!g_isLoading) {
    return;
  }

  // If server is loading, Yield to Redis if the number of operations is greater than the yieldEveryOps
  opCounter += numOps;
  if (opCounter >= RSGlobalConfig.indexerYieldEveryOpsWhileLoading) {
    opCounter = opCounter % RSGlobalConfig.indexerYieldEveryOpsWhileLoading;
    IncrementLoadYieldCounter(); // Track that we called yield
    unsigned int sleepMicros = GetIndexerSleepBeforeYieldMicros();
    if (sleepMicros > 0) {
      usleep(sleepMicros);
    }
    RedisModule_Yield(ctx, flags, NULL);
  }

  // If server is loading, Yield to Redis if Vector write is throttling.
  if (SearchDisk_IsEnabled() && !IS_SST_RDB_LOADING(ctx) &&
      workersThreadPool_NumThreads() > 0 && SearchDisk_IsVectorWriteThrottling()) {
    RedisModule_Log(ctx, "debug",
                    "RDB load: vector flat buffer full; backing off the rebuild");
    while (SearchDisk_IsVectorWriteThrottling()) {
      usleep(RDB_LOAD_THROTTLE_BACKOFF_US);
      RedisModule_Yield(ctx, flags, NULL);
    }
    RedisModule_Log(ctx, "debug", "RDB load: vector flat buffer throttle cleared; resuming");
  }
}

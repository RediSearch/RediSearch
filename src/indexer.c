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

extern RedisModuleCtx *RSDummyContext;

#include <unistd.h>

// Hook data carrying the per-document mutations that must run only after a
// successful disk-batch commit (doc-id metadata, scoring stats, GC notify).
// Allocated with `rm_malloc` and freed via `freeAssignIdsWriteCommitData`.
typedef struct {
  RedisModuleCtx *redisCtx;
  RedisModuleString *docKey;
  IndexSpec *spec;
  t_docId docId;
  uint32_t oldDocLen;  // > 0 when this insert replaces an existing doc
  uint32_t totalFreq;  // new document length to add to totalDocsLen
} AssignIdsWriteCommitData;

static void freeAssignIdsWriteCommitData(void *user_data) {
  rm_free(user_data);
}

// Hook data for the deferred fulltext term-trie / suffix-trie updates. The hook
// re-walks `aCtx->fwIdx` (still alive until `AddDocumentCtx_Free`) so we don't
// have to snapshot per-term state at staging time.
typedef struct {
  IndexSpec *spec;
  RSAddDocumentCtx *aCtx;
} WriteCurEntriesWriteCommitData;

static void freeWriteCurEntriesWriteCommitData(void *user_data) {
  rm_free(user_data);
}

// Post-commit: register every staged term in the index-wide terms trie and
// (where applicable) the suffix trie, then publish the per-field "docs indexed"
// global stat. Keeping this paired with the staged inverted-index writes means
// the term trie never advertises a term that did not actually reach disk.
static void onCommit_WriteCurEntries(void *user_data) {
  WriteCurEntriesWriteCommitData *data = user_data;
  IndexSpec *spec = data->spec;
  RSAddDocumentCtx *aCtx = data->aCtx;

  size_t prevNumTerms = spec->stats.scoring.numTerms;

  ForwardIndexIterator it = ForwardIndex_Iterate(aCtx->fwIdx);
  ForwardIndexEntry *entry = ForwardIndexIterator_Next(&it);
  while (entry != NULL) {
    IndexSpec_AddTerm(spec, entry->term, entry->len);

    if (spec->suffixMask & entry->fieldMask
        && entry->term[0] != STEM_PREFIX
        && entry->term[0] != PHONETIC_PREFIX
        && entry->term[0] != SYNONYM_PREFIX_CHAR
        && strlen(entry->term) != 0) {
      addSuffixTrie(spec->suffix, entry->term, entry->len);
    }

    entry = ForwardIndexIterator_Next(&it);
  }

  FieldsGlobalStats_UpdateFieldDocsIndexed(INDEXFLD_T_FULLTEXT,
                                           spec->stats.scoring.numTerms - prevNumTerms);
}

// Post-commit: stamp the Redis-side `key -> docId` mapping and apply the scoring
// stats that pair with the staged doc-table write.
//
// `DocIdMeta_Set` only fails when `RedisModule_HashSet` does (OOM or otherwise
// fundamentally-broken Redis). At that point the disk already holds the new doc
// and the module has no clean way to recover — any cleanup we attempt
// (`SearchDisk_DeleteDocumentById`, stats rollback) might itself fail and leave
// the in-memory and on-disk views permanently inconsistent. Crash loudly
// instead so the operator sees the failure and the server restarts into a
// well-defined state from the WAL.
static void onCommit_AssignIds(void *user_data) {
  AssignIdsWriteCommitData *data = user_data;

  int rc = DocIdMeta_Set(data->redisCtx, data->docKey, data->spec->specId, data->docId);
  RS_LOG_ASSERT_ALWAYS(rc == REDISMODULE_OK,
                       "DocIdMeta_Set failed after a successful disk commit");

  bool updated = false;
  if (data->oldDocLen > 0) {
    RS_ASSERT(data->spec->stats.scoring.numDocuments > 0);
    data->spec->stats.scoring.numDocuments--;
    RS_ASSERT(data->spec->stats.scoring.totalDocsLen >= data->oldDocLen);
    data->spec->stats.scoring.totalDocsLen -= data->oldDocLen;
    updated = true;
  }
  data->spec->stats.scoring.totalDocsLen += data->totalFreq;
  ++data->spec->stats.scoring.numDocuments;

  if (data->spec->gc) {
    if (updated) {
      GCContext_OnUpdate(data->spec->gc);
    } else {
      GCContext_OnWrite(data->spec->gc);
    }
  }
}

static void writeIndexEntry(IndexSpec *spec, InvertedIndex *idx, ForwardIndexEntry *entry) {
  size_t sz = InvertedIndex_WriteForwardIndexEntry(idx, entry);

  // Update index statistics:

  // Number of additional bytes
  spec->stats.invertedSize += sz;
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

/**
 * Simple implementation, writes all the entries for a single document. This
 * function is used when there is only one item in the queue. In this case
 * it's simpler to forego building the merged dictionary because there is
 * nothing to merge.
 */
static void writeCurEntries(RSAddDocumentCtx *aCtx, RedisSearchCtx *ctx) {
  RS_LOG_ASSERT(ctx, "ctx should not be NULL");

  IndexSpec *spec = ctx->spec;
  ForwardIndexIterator it = ForwardIndex_Iterate(aCtx->fwIdx);
  ForwardIndexEntry *entry = ForwardIndexIterator_Next(&it);

  // Save the number of terms before indexing the current document for metrics
  size_t prevNumTerms = spec->stats.scoring.numTerms;

  while (entry != NULL) {
    if (spec->diskSpec) {
      // Get offset data if available (when Index_StoreTermOffsets flag is set)
      const uint8_t *offsets = NULL;
      size_t offsetsLen = 0;
      if ((spec->flags & Index_StoreTermOffsets) && entry->vw) {
        offsets = VVW_GetByteData(entry->vw);
        offsetsLen = VVW_GetByteLength(entry->vw);
      }
      // Stage the inverted-index write only. The matching term-trie /
      // suffix-trie updates and the per-field "docs indexed" stat are deferred
      // to the on-commit hook registered after this loop so they fire only
      // once the batch reaches disk.
      SearchDisk_IndexTerm(spec->diskSpec, aCtx->diskBatch, entry->term, entry->len, aCtx->doc->docId, entry->fieldMask, entry->freq, offsets, offsetsLen);
    } else {
      bool isNew;
      InvertedIndex *invidx = Redis_OpenInvertedIndex(ctx->spec, entry->term, entry->len, 1, &isNew);
      if (isNew && strlen(entry->term) != 0) {
        IndexSpec_AddTerm(spec, entry->term, entry->len);
      }
      if (invidx) {
        entry->docId = aCtx->doc->docId;
        RS_LOG_ASSERT(entry->docId, "docId should not be 0");
        writeIndexEntry(spec, invidx, entry);
      }

      if (spec->suffixMask & entry->fieldMask
          && entry->term[0] != STEM_PREFIX
          && entry->term[0] != PHONETIC_PREFIX
          && entry->term[0] != SYNONYM_PREFIX_CHAR
          && strlen(entry->term) != 0) {
        addSuffixTrie(spec->suffix, entry->term, entry->len);
      }
    }

    entry = ForwardIndexIterator_Next(&it);
  }

  if (spec->diskSpec) {
    if (aCtx->diskBatch) {
      // Pair the staged term writes with their in-memory bookkeeping. The hook
      // re-walks `fwIdx` (still alive until `AddDocumentCtx_Free`) so we don't
      // have to snapshot per-term state at staging time.
      WriteCurEntriesWriteCommitData *hook_data = rm_malloc(sizeof(*hook_data));
      hook_data->spec = spec;
      hook_data->aCtx = aCtx;
      SearchDisk_WriteBatch_OnCommit(aCtx->diskBatch, onCommit_WriteCurEntries,
                                     hook_data, freeWriteCurEntriesWriteCommitData);
    }
  } else {
    // Memory mode applies updates eagerly above, so we can publish the global
    // stat directly. The disk path publishes it from `onCommit_WriteCurEntries`.
    FieldsGlobalStats_UpdateFieldDocsIndexed(INDEXFLD_T_FULLTEXT, spec->stats.scoring.numTerms - prevNumTerms);
  }
}

/** Assigns a document ID to a single document. Handles only RAM index */
static RSDocumentMetadata *makeDocumentId(RedisModuleCtx *ctx, RSAddDocumentCtx *aCtx, IndexSpec *spec,
                                          int replace, bool *updated) {
  DocTable *table = &spec->docs;
  Document *doc = aCtx->doc;
  if (replace) {
    RSDocumentMetadata *dmd = DocTable_PopR(table, doc->docKey);
    if (dmd) {
      // Update stats of the index only if the document was there
      RS_LOG_ASSERT(spec->stats.scoring.numDocuments > 0, "numDocuments cannot be negative");
      --spec->stats.scoring.numDocuments;
      RS_LOG_ASSERT(spec->stats.scoring.totalDocsLen >= dmd->docLen, "totalDocsLen is smaller than dmd->docLen");
      spec->stats.scoring.totalDocsLen -= dmd->docLen;
      *updated = true;
      if (spec->flags & Index_HasVecSim) {
        for (int i = 0; i < spec->numFields; ++i) {
          if (spec->fields[i].types == INDEXFLD_T_VECTOR) {
            // ctx is NULL because we don't create the index here
            VecSimIndex *vecsim = openVectorIndex(NULL, &spec->fields[i], DONT_CREATE_INDEX);
            if(!vecsim)
              continue;
            VecSimIndex_DeleteVector(vecsim, dmd->id);
            // TODO: use VecSimReplace instead and if successful, do not insert and remove from doc
          }
        }
      }
      if (spec->flags & Index_HasGeometry) {
        GeometryIndex_RemoveId(spec, dmd->id);
      }

      DMD_Return(dmd);
    }
  }

  size_t n;
  const char *s = RedisModule_StringPtrLen(doc->docKey, &n);
  RSDocumentMetadata *dmd =
      DocTable_Put(table, s, n, doc->score, aCtx->docFlags, doc->payload, doc->payloadSize, doc->type);
  if (dmd) {
    doc->docId = dmd->id;
    ++spec->stats.scoring.numDocuments;
  }

  return dmd;
}

/**
 * Performs bulk document ID assignment to all items in the queue.
 * If one item cannot be assigned an ID, it is marked as being errored.
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
    bool updated = false;
    if (SearchDisk_IsEnabled()) {
      RS_ASSERT(spec->diskSpec);
      size_t len;
      const char *key = RedisModule_StringPtrLen(cur->doc->docKey, &len);
      uint32_t oldLen = 0;

      // Check if the document has expiration time (disk does not support field-level expiration yet)
      if (cur->doc->docExpirationTime.tv_sec || cur->doc->docExpirationTime.tv_nsec) {
        cur->docFlags |= Document_HasExpiration;
      }

      // Get old docId from key metadata (if document already exists)
      // TODO: Consider calling this from SearchDisk_PutDocument
      uint64_t oldDocId = 0;
      DocIdMeta_Get(ctx->redisCtx, cur->doc->docKey, spec->specId, &oldDocId);

      // Open a per-document write batch that doc-table / inverted-index / tag-index writes
      // will be staged into. The batch is committed (or aborted on error) by
      // `Indexer_Process` once all of `cur`'s indexing work has finished.
      cur->diskBatch = SearchDisk_CreateWriteBatch(spec->diskSpec);

      // Put the document and get a new doc-id, and remove the old id->dmd entry
      // if it existed.
      t_docId docId = cur->diskBatch
        ? SearchDisk_PutDocument(spec->diskSpec, cur->diskBatch, key, len,
            cur->doc->score, cur->docFlags, cur->fwIdx->maxTermFreq,
            cur->fwIdx->totalFreq, &oldLen, cur->doc->docExpirationTime, oldDocId)
        : 0;

      if (docId == 0) {
        // Either `SearchDisk_CreateWriteBatch` returned NULL (so `cur->diskBatch`
        // is NULL and the ternary short-circuited to 0) or `SearchDisk_PutDocument`
        // itself failed. Both indicate the storage layer rejected the operation
        // (typically OOM / disk-init failure). Surface the failure via
        // `aCtx->status` and let the batch (if any was opened) be aborted by
        // `AddDocumentCtx_Free` — no in-memory side effects need rolling back
        // since they are now all gated on a successful commit.
        const char *reason = cur->diskBatch
                                 ? "Failed to stage document on disk"
                                 : "Failed to open disk write batch";
        if (!QueryError_HasError(&cur->status)) {
          QueryError_SetError(&cur->status, QUERY_ERROR_CODE_GENERIC, reason);
        }
        cur->stateFlags |= ACTX_F_ERRORED;
        RS_LOG_ASSERT_FMT(false, "Unexpected: %s", reason);
        continue;
      }

      cur->doc->docId = docId;

      // Pair the staged put with an on-commit hook that runs the matching
      // in-memory mutations (Redis-side `key -> docId` map, scoring stats, GC
      // counters) only if the batch reaches disk. On abort or commit failure
      // none of these run, keeping the in-memory view consistent with disk.
      AssignIdsWriteCommitData *hook_data = rm_malloc(sizeof(*hook_data));
      hook_data->redisCtx = ctx->redisCtx;
      hook_data->docKey = cur->doc->docKey;
      hook_data->spec = spec;
      hook_data->docId = docId;
      hook_data->oldDocLen = oldLen;
      hook_data->totalFreq = cur->fwIdx->totalFreq;
      SearchDisk_WriteBatch_OnCommit(cur->diskBatch, onCommit_AssignIds, hook_data,
                                     freeAssignIdsWriteCommitData);
      // Skip the trailing GC notify — it runs from `onCommit_AssignIds` instead.
      continue;
    } else {
      RS_LOG_ASSERT(!cur->doc->docId, "docId must be 0");
      RSDocumentMetadata *md = makeDocumentId(ctx->redisCtx, cur, spec,
                                              cur->options & DOCUMENT_ADD_REPLACE, &updated);
      if (!md) {
        cur->stateFlags |= ACTX_F_ERRORED;
        continue;
      }

      md->maxTermFreq = cur->fwIdx->maxTermFreq;
      md->docLen = cur->fwIdx->totalFreq;
      spec->stats.scoring.totalDocsLen += md->docLen;

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
    }
    if (updated) {
      if (spec->gc) {
        GCContext_OnUpdate(spec->gc);
      }
    } else {
      if (spec->gc) {
        GCContext_OnWrite(spec->gc);
      }
    }
  }
}

static void indexBulkFields(RSAddDocumentCtx *aCtx, RedisSearchCtx *sctx) {
  // Traverse all fields, seeing if there may be something which can be written!
  for (RSAddDocumentCtx *cur = aCtx; cur && cur->doc->docId; cur = cur->next) {
    if (cur->stateFlags & ACTX_F_ERRORED) {
      continue;
    }

    const Document *doc = cur->doc;
    for (size_t ii = 0; ii < doc->numFields; ++ii) {
      const FieldSpec *fs = cur->fspecs + ii;
      FieldIndexerData *fdata = cur->fdatas + ii;
      if (fs->types == INDEXFLD_T_FULLTEXT || !FieldSpec_IsIndexable(fs) || fdata->isNull) {
        continue;
      }
      if (IndexerBulkAdd(cur, sctx, doc->fields + ii, fs, fdata, &cur->status) != 0) {
        IndexError_AddQueryError(&cur->spec->stats.indexError, &cur->status, doc->docKey);
        FieldSpec_AddQueryError(&cur->spec->fields[fs->index], &cur->status, doc->docKey);
        QueryError_ClearError(&cur->status);
        cur->stateFlags |= ACTX_F_ERRORED;
      }
      cur->stateFlags |= ACTX_F_OTHERINDEXED;
    }
  }
}

static void reopenCb(void *arg) {}

// Routines for the merged hash table
#define ACTX_IS_INDEXED(actx)                                           \
  (((actx)->stateFlags & (ACTX_F_OTHERINDEXED | ACTX_F_TEXTINDEXED)) == \
   (ACTX_F_OTHERINDEXED | ACTX_F_TEXTINDEXED))

// In-memory side-index writes for the "doc exists" and "field missing" markers.
// Split out from `writeExistingDocs` / `writeMissingFieldDocs` so the same body
// can run eagerly in memory mode and from an on-commit hook in disk mode.
static void writeExistingDocs_apply(IndexSpec *spec, t_docId docId) {
  if (!spec->existingDocs) {
    size_t index_size;
    spec->existingDocs = NewInvertedIndex(Index_DocIdsOnly, &index_size);
    spec->stats.invertedSize += index_size;
  }
  RSIndexResult rec = {.data.tag = RSResultData_Virtual, .docId = docId, .freq = 0,
                       .metrics = MetricsVec_New()};
  spec->stats.invertedSize += InvertedIndex_WriteEntryGeneric(spec->existingDocs, &rec);
}

static void writeMissingFieldDocs_apply(IndexSpec *spec, Document *doc,
                                        arrayof(FieldExpiration) sortedFieldWithExpiration) {
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
      spec->stats.invertedSize += index_size;
      dictAdd(spec->missingFieldDict, (void*)fs->fieldName, iiMissingDocs);
    }
    // Add docId to inverted index
    RSIndexResult rec = {.data.tag = RSResultData_Virtual, .docId = doc->docId, .freq = 0,
                         .metrics = MetricsVec_New()};
    spec->stats.invertedSize += InvertedIndex_WriteEntryGeneric(iiMissingDocs, &rec);
  }
  dictReleaseIterator(iter);
  dictRelease(df_fields_dict);
}

// Hook data + on-commit shells for the side-index writes above. In disk mode the
// writes are deferred to commit so an aborted or failed-to-commit batch does not
// leave phantom doc IDs in `existingDocs` / `missingFieldDict`.

typedef struct {
  IndexSpec *spec;
  t_docId docId;
} WriteExistingDocsWriteCommitData;

static void freeWriteExistingDocsWriteCommitData(void *user_data) {
  rm_free(user_data);
}

static void onCommit_WriteExistingDocs(void *user_data) {
  WriteExistingDocsWriteCommitData *data = user_data;
  writeExistingDocs_apply(data->spec, data->docId);
}

typedef struct {
  IndexSpec *spec;
  Document *doc;
  arrayof(FieldExpiration) sortedFieldWithExpiration;
} WriteMissingFieldDocsWriteCommitData;

static void freeWriteMissingFieldDocsWriteCommitData(void *user_data) {
  rm_free(user_data);
}

static void onCommit_WriteMissingFieldDocs(void *user_data) {
  WriteMissingFieldDocsWriteCommitData *data = user_data;
  writeMissingFieldDocs_apply(data->spec, data->doc, data->sortedFieldWithExpiration);
}

// Index missing field docs.
// Add field names to missingFieldDict if it is missing in the document
// and add the doc to its corresponding inverted index
static void writeMissingFieldDocs(RSAddDocumentCtx *aCtx, RedisSearchCtx *sctx, arrayof(FieldExpiration) sortedFieldWithExpiration) {
  IndexSpec *spec = sctx->spec;

  if (spec->diskSpec) {
    // Disk mode: defer to an on-commit hook so the in-memory `missingFieldDict`
    // and `invertedSize` accounting only advance when the batch reaches disk.
    // If staging already failed and no batch is open, skip entirely — the doc
    // isn't going to land on disk either.
    if (aCtx->stateFlags & ACTX_F_ERRORED || !aCtx->diskBatch) {
      return;
    }
    WriteMissingFieldDocsWriteCommitData *hook_data = rm_malloc(sizeof(*hook_data));
    hook_data->spec = spec;
    hook_data->doc = aCtx->doc;
    hook_data->sortedFieldWithExpiration = sortedFieldWithExpiration;
    SearchDisk_WriteBatch_OnCommit(aCtx->diskBatch, onCommit_WriteMissingFieldDocs,
                                   hook_data, freeWriteMissingFieldDocsWriteCommitData);
    return;
  }

  writeMissingFieldDocs_apply(spec, aCtx->doc, sortedFieldWithExpiration);
}

// Index the doc in the existing docs inverted index
static void writeExistingDocs(RSAddDocumentCtx *aCtx, RedisSearchCtx *sctx) {
  if (!sctx->spec->rule || !sctx->spec->rule->index_all) {
    return;
  }

  if (sctx->spec->diskSpec) {
    // Disk mode: defer to an on-commit hook (see writeMissingFieldDocs).
    if (aCtx->stateFlags & ACTX_F_ERRORED || !aCtx->diskBatch) {
      return;
    }
    WriteExistingDocsWriteCommitData *hook_data = rm_malloc(sizeof(*hook_data));
    hook_data->spec = sctx->spec;
    hook_data->docId = aCtx->doc->docId;
    SearchDisk_WriteBatch_OnCommit(aCtx->diskBatch, onCommit_WriteExistingDocs,
                                   hook_data, freeWriteExistingDocsWriteCommitData);
    return;
  }

  writeExistingDocs_apply(sctx->spec, aCtx->doc->docId);
}

/**
 * Perform the processing chain on a single document entry, optionally merging
 * the tokens of further entries in the queue
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

  // Index the document in the `existing docs` inverted index
  writeExistingDocs(aCtx, &ctx);

  // On the non-disk path, `doc->fieldExpirations` ownership has already been
  // moved into the TTL table by `doAssignIds` on success. On failure (e.g.
  // `makeDocumentId` returned NULL), the array stays attached to `doc` so
  // `Document_Free` can release it.
  arrayof(FieldExpiration) fes;
  if (SearchDisk_IsEnabled()) {
    fes = doc->fieldExpirations;
  } else {
    fes = (arrayof(FieldExpiration))DocTable_GetFieldExpirations(&ctx.spec->docs, doc->docId);
  }
  writeMissingFieldDocs(aCtx, &ctx, fes);

  // Handle FULLTEXT indexes
  if ((aCtx->fwIdx && (aCtx->stateFlags & ACTX_F_ERRORED) == 0)) {
    writeCurEntries(aCtx, &ctx);
  }

  if (!(aCtx->stateFlags & ACTX_F_OTHERINDEXED)) {
    indexBulkFields(aCtx, &ctx);
  }

  // Finalize the per-document disk write batch. On the happy path this flushes
  // the staged doc-table / inverted-index / tag writes atomically and runs the
  // registered on-commit hooks. If the document already errored anywhere
  // upstream, we leave the batch open — `AddDocumentCtx_Free` aborts it as a
  // safety net, which keeps the cleanup logic in one place instead of three.
  if (aCtx->diskBatch && !(aCtx->stateFlags & ACTX_F_ERRORED)) {
    if (!SearchDisk_CommitWriteBatch(aCtx->diskBatch)) {
      // Tell the reply path that nothing was persisted. Without setting
      // `aCtx->status`, `replyCallback` (which keys off `QueryError_HasError`)
      // would still return OK to the client even though no writes reached disk.
      if (!QueryError_HasError(&aCtx->status)) {
        QueryError_SetError(&aCtx->status, QUERY_ERROR_CODE_GENERIC,
                            "Failed to commit disk write batch");
      }
      aCtx->stateFlags |= ACTX_F_ERRORED;
    }
    aCtx->diskBatch = NULL;
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

/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "indexer.h"
#include "forward_index.h"
#include "numeric_index.h"
#include "inverted_index.h"
#include "geo_index.h"
#include "vector_index.h"
#include "index.h"
#include "redis_index.h"
#include "suffix.h"
#include "rmutil/rm_assert.h"
#include "phonetic_manager.h"
#include "obfuscation/obfuscation_api.h"

extern RedisModuleCtx *RSDummyContext;

#include <unistd.h>

static void writeIndexEntry(IndexSpec *spec, InvertedIndex *idx, IndexEncoder encoder,
                            ForwardIndexEntry *entry) {
  size_t sz = InvertedIndex_WriteForwardIndexEntry(idx, encoder, entry);

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
  IndexEncoder encoder = InvertedIndex_GetEncoder(aCtx->specFlags);

  while (entry != NULL) {
    bool isNew;
    InvertedIndex *invidx = Redis_OpenInvertedIndex(ctx, entry->term, entry->len, 1, &isNew);
    if (isNew && strlen(entry->term) != 0) {
      IndexSpec_AddTerm(spec, entry->term, entry->len);
    }
    if (invidx) {
      entry->docId = aCtx->doc->docId;
      RS_LOG_ASSERT(entry->docId, "docId should not be 0");
      writeIndexEntry(spec, invidx, encoder, entry);
      if (Index_StoreFieldMask(spec)) {
        invidx->fieldMask |= entry->fieldMask;
      }
    }

    if (spec->suffixMask & entry->fieldMask
        && entry->term[0] != STEM_PREFIX
        && entry->term[0] != PHONETIC_PREFIX
        && entry->term[0] != SYNONYM_PREFIX_CHAR
        && strlen(entry->term) != 0) {
      addSuffixTrie(spec->suffix, entry->term, entry->len);
    }

    entry = ForwardIndexIterator_Next(&it);
  }
}

/** Assigns a document ID to a single document. */
static RSDocumentMetadata *makeDocumentId(RedisModuleCtx *ctx, RSAddDocumentCtx *aCtx, IndexSpec *spec,
                                          int replace, QueryError *status) {
  DocTable *table = &spec->docs;
  Document *doc = aCtx->doc;
  if (replace) {
    RSDocumentMetadata *dmd = DocTable_PopR(table, doc->docKey);
    if (dmd) {
      // decrease the number of documents in the index stats only if the document was there
      --spec->stats.numDocuments;
      DMD_Return(aCtx->oldMd);
      aCtx->oldMd = dmd;
      if (spec->gc) {
        GCContext_OnDelete(spec->gc);
      }
      if (spec->flags & Index_HasVecSim) {
        for (int i = 0; i < spec->numFields; ++i) {
          if (spec->fields[i].types == INDEXFLD_T_VECTOR) {
            RedisModuleString *rmstr = IndexSpec_GetFormattedKey(spec, &spec->fields[i], INDEXFLD_T_VECTOR);
            VecSimIndex *vecsim = openVectorIndex(spec, rmstr, CREATE_INDEX);
            VecSimIndex_DeleteVector(vecsim, dmd->id);
            // TODO: use VecSimReplace instead and if successful, do not insert and remove from doc
          }
        }
      }
      if (spec->flags & Index_HasGeometry) {
        GeometryIndex_RemoveId(spec, dmd->id);
      }
    }
  }

  size_t n;
  const char *s = RedisModule_StringPtrLen(doc->docKey, &n);
  RSDocumentMetadata *dmd =
      DocTable_Put(table, s, n, doc->score, aCtx->docFlags, doc->payload, doc->payloadSize, doc->type);
  if (dmd) {
    doc->docId = dmd->id;
    ++spec->stats.numDocuments;
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

    RS_LOG_ASSERT(!cur->doc->docId, "docId must be 0");
    RSDocumentMetadata *md = makeDocumentId(ctx->redisCtx, cur, spec,
                                            cur->options & DOCUMENT_ADD_REPLACE, &cur->status);
    if (!md) {
      cur->stateFlags |= ACTX_F_ERRORED;
      continue;
    }

    md->maxFreq = cur->fwIdx->maxFreq;
    md->len = cur->fwIdx->totalFreq;
    spec->stats.totalDocsLen += md->len;

    if (cur->sv) {
      DocTable_SetSortingVector(&spec->docs, md, cur->sv);
      cur->sv = NULL;
    }

    if (cur->byteOffsets) {
      ByteOffsetWriter_Move(&cur->offsetsWriter, cur->byteOffsets);
      DocTable_SetByteOffsets(md, cur->byteOffsets);
      cur->byteOffsets = NULL;
    }
    DMD_Return(md);
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

// Index missing field docs.
// Add field names to missingFieldDict if it is missing in the document
// and add the doc to its corresponding inverted index
static void writeMissingFieldDocs(RSAddDocumentCtx *aCtx, RedisSearchCtx *sctx) {
  Document *doc = aCtx->doc;
  IndexSpec *spec = sctx->spec;
  bool found_df;
  // We use a dictionary as a set, to keep all the fields that we've seen so far (optimization)
  dict *df_fields_dict = dictCreate(&dictTypeHeapHiddenStrings, NULL);
  uint last_ind = 0;

  for(size_t i = 0; i < spec->numFields; i++) {
    found_df = false;
    const FieldSpec *fs = spec->fields + i;
    if (!FieldSpec_IndexesMissing(fs)) {
      continue;
    }
    if (dictFind(df_fields_dict, (void *)fs->fieldName) != NULL) {
      found_df = true;
    } else {
      for (size_t j = last_ind; j < aCtx->doc->numFields; j++) {
        if (!strcmp(fs->fieldName, doc->fields[j].fieldName)) {
          found_df = true;
          last_ind++;
          break;
        }
        dictAdd(df_fields_dict, (void *)doc->fields[j].fieldName, NULL);
        last_ind++;
      }
    }

    // We wish to index this document for this field only if the document doesn't contain it.
    if (!found_df) {
      InvertedIndex *iiMissingDocs = dictFetchValue(spec->missingFieldDict, fs->fieldName);
      if(iiMissingDocs == NULL) {
        size_t dummy_mem;
        iiMissingDocs = NewInvertedIndex(Index_DocIdsOnly, 1, &dummy_mem);
        dictAdd(spec->missingFieldDict, (void*)fs->fieldName, iiMissingDocs);
      }
      // Add docId to inverted index
      t_docId docId = aCtx->doc->docId;
      IndexEncoder enc = InvertedIndex_GetEncoder(Index_DocIdsOnly);
      RSIndexResult rec = {.type = RSResultType_Virtual, .docId = docId, .offsetsSz = 0, .freq = 0};
      InvertedIndex_WriteEntryGeneric(iiMissingDocs, enc, docId, &rec);
    }
  }

  dictRelease(df_fields_dict);
}

/**
 * Perform the processing chain on a single document entry, optionally merging
 * the tokens of further entries in the queue
 */
static void Indexer_Process(RSAddDocumentCtx *aCtx) {
  RSAddDocumentCtx *parentMap[MAX_BULK_DOCS];
  RSAddDocumentCtx *firstZeroId = aCtx;
  RedisSearchCtx ctx = *aCtx->sctx;

  if (ACTX_IS_INDEXED(aCtx) || aCtx->stateFlags & (ACTX_F_ERRORED)) {
    // Document is complete or errored. No need for further processing.
    if (!(aCtx->stateFlags & ACTX_F_EMPTY)) {
      return;
    }
  }

  if (!ctx.spec) {
    QueryError_SetCode(&aCtx->status, QUERY_ENOINDEX);
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

  // Handle missing values indexing
  writeMissingFieldDocs(aCtx, &ctx);

  // Handle FULLTEXT indexes
  if ((aCtx->fwIdx && (aCtx->stateFlags & ACTX_F_ERRORED) == 0)) {
    writeCurEntries(aCtx, &ctx);
  }

  if (!(aCtx->stateFlags & ACTX_F_OTHERINDEXED)) {
    indexBulkFields(aCtx, &ctx);
  }
}

int IndexDocument(RSAddDocumentCtx *aCtx) {
  Indexer_Process(aCtx);
  AddDocumentCtx_Finish(aCtx);
  return 0;
}

#include "forward_index.h"
#include "inverted_index.h"
#include "redis_index.h"
#include "indexer.h"
#include "geo_index.h"

// Number of terms for each block-allocator block
#define TERMS_PER_BLOCK 128

// Entry for the merged dictionary
typedef struct mergedEntry {
  KHTableEntry base;        // Base structure
  ForwardIndexEntry *head;  // First document containing the term
  ForwardIndexEntry *tail;  // Last document containing the term
} mergedEntry;

static void setDocAsErr(Indexer *bi, RSAddDocumentCtx *ctx, size_t idx) {
  bi->docs[idx] = NULL;
  *array_ensure_tail(&bi->errs, RSAddDocumentCtx *) = ctx;
  assert((ctx->stateFlags & ACTX_F_ERRORED) == 0);
  ctx->stateFlags |= ACTX_F_ERRORED;
}

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

// Take the terms from a single document and merge it into the terms hash table
// for all the documents in the batch
static void addTextDoc(Indexer *indexer, RSAddDocumentCtx *cur) {
  ForwardIndexIterator it = ForwardIndex_Iterate(cur->fwIdx);
  ForwardIndexEntry *entry = ForwardIndexIterator_Next(&it);
  t_docId curId = cur->doc.docId;

  while (entry) {
    entry->docId = curId;

    // Get the entry for it.
    int isNew = 0;
    mergedEntry *mergedEnt = (mergedEntry *)KHTable_GetEntry(&indexer->mergeHt, entry->term,
                                                             entry->len, entry->hash, &isNew);

    if (isNew) {
      mergedEnt->head = mergedEnt->tail = entry;

    } else {
      mergedEnt->tail->next = entry;
      mergedEnt->tail = entry;
    }

    entry->next = NULL;
    entry = ForwardIndexIterator_Next(&it);
  }

  // Set the document's text status as indexed. This is not strictly true,
  // but it means that there is no more index interaction with this specific
  // document.
  cur->stateFlags |= ACTX_F_TEXTINDEXED;
}

static void writeEntry(IndexSpec *spec, InvertedIndex *idx, IndexEncoder encoder,
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

static void writeMergedEntries(Indexer *indexer) {
  RedisSearchCtx *ctx = indexer->sctx;
  KHTable *ht = &indexer->mergeHt;
  IndexEncoder encoder = InvertedIndex_GetEncoder(ctx->spec->flags);

  // Iterate over all the entries
  for (uint32_t curBucketIdx = 0; curBucketIdx < ht->numBuckets; curBucketIdx++) {
    for (KHTableEntry *entp = ht->buckets[curBucketIdx]; entp; entp = entp->next) {
      mergedEntry *merged = (mergedEntry *)entp;

      // Open the inverted index:
      ForwardIndexEntry *fwent = merged->head;
      InvertedIndex *invidx = IDX_LoadTerm(ctx->spec, fwent->term, fwent->len, REDISMODULE_WRITE);

      if (invidx == NULL) {
        continue;
      }

      for (; fwent != NULL; fwent = fwent->next) {
        writeEntry(ctx->spec, invidx, encoder, fwent);
      }
    }
  }
}

static void indexBulkFields(Indexer *indexer) {
  RedisSearchCtx *sctx = indexer->sctx;

  // Traverse all fields, seeing if there may be something which can be written!
  IndexBulkData bData[SPEC_MAX_FIELDS] = {{{NULL}}};
  IndexBulkData *activeBulks[SPEC_MAX_FIELDS];

  size_t numActiveBulks = 0;
  size_t ndocs = array_len(indexer->docs);
  for (size_t ii = 0; ii < ndocs; ++ii) {
    RSAddDocumentCtx *cur = indexer->docs[ii];
    if (!cur) {
      continue;
    }
  }

  // Flush it!
  for (size_t ii = 0; ii < numActiveBulks; ++ii) {
    IndexBulkData *cur = activeBulks[ii];
    IndexerBulkCleanup(cur, sctx);
  }
}

static void handleReplaceDelete(RedisSearchCtx *sctx, t_docId did) {
  // Geo is now indexed..
}

/** Assigns a document ID to a single document. */
static int makeDocumentId(RSAddDocumentCtx *aCtx, RedisSearchCtx *sctx, int replace,
                          QueryError *status) {
  IndexSpec *spec = sctx->spec;
  DocTable *table = &spec->docs;
  Document *doc = &aCtx->doc;
  if (replace) {
    RSDocumentMetadata *dmd = DocTable_PopR(table, doc->docKey);
    if (dmd) {
      // decrease the number of documents in the index stats only if the document was there
      --spec->stats.numDocuments;
      aCtx->oldMd = dmd;
      if (dmd->flags & Document_HasOnDemandDeletable) {
        // Delete all on-demand fields.. this means geo,but could mean other things..
        handleReplaceDelete(sctx, dmd->id);
      }
      if (sctx->spec->gc) {
        GCContext_OnDelete(sctx->spec->gc);
      }
    }
  }

  size_t n;
  const char *s = RedisModule_StringPtrLen(doc->docKey, &n);

  doc->docId =
      DocTable_Put(table, s, n, doc->score, aCtx->docFlags, doc->payload, doc->payloadSize);
  if (doc->docId == 0) {
    assert(!replace);
    QueryError_SetError(status, QUERY_EDOCEXISTS, NULL);
    return -1;
  }
  ++spec->stats.numDocuments;

  return 0;
}

/**
 * Performs bulk document ID assignment to all items in the queue.
 * If one item cannot be assigned an ID, it is marked as being errored.
 *
 * This function also sets the document's sorting vector, if present.
 */
static int doAssignId(RSAddDocumentCtx *cur, RedisSearchCtx *ctx) {
  IndexSpec *spec = ctx->spec;
  assert(!cur->doc.docId);
  int rv = makeDocumentId(cur, ctx, cur->options & DOCUMENT_ADD_REPLACE, &cur->status);
  if (rv != 0) {
    return REDISMODULE_ERR;
  }

  RSDocumentMetadata *md = DocTable_Get(&spec->docs, cur->doc.docId);
  md->maxFreq = cur->fwIdx->maxFreq;
  md->len = cur->fwIdx->totalFreq;

  if (cur->sv) {
    DocTable_SetSortingVector(&spec->docs, cur->doc.docId, cur->sv);
    cur->sv = NULL;
  }

  if (cur->byteOffsets) {
    ByteOffsetWriter_Move(&cur->offsetsWriter, cur->byteOffsets);
    DocTable_SetByteOffsets(&spec->docs, cur->doc.docId, cur->byteOffsets);
    cur->byteOffsets = NULL;
  }
  return REDISMODULE_OK;
}

typedef struct {
  IndexBulkData bdata[SPEC_MAX_FIELDS];
  IndexBulkData *active[SPEC_MAX_FIELDS];
  size_t nactive;
} BulkInfo;

static int addNontextDoc(Indexer *bi, RSAddDocumentCtx *cur, BulkInfo *binfo) {
  const Document *doc = &cur->doc;
  for (size_t ii = 0; ii < doc->numFields; ++ii) {
    const FieldSpec *fs = cur->fspecs + ii;
    FieldIndexerData *fdata = cur->fdatas + ii;
    if (fs->name == NULL || fs->types == INDEXFLD_T_FULLTEXT || !FieldSpec_IsIndexable(fs)) {
      continue;
    }
    IndexBulkData *bulk = &binfo->bdata[fs->index];
    if (!bulk->found) {
      bulk->found = 1;
      binfo->active[binfo->nactive++] = bulk;
    }

    if (IndexerBulkAdd(bulk, cur, bi->sctx, doc->fields + ii, fs, fdata, &cur->status) != 0) {
      return REDISMODULE_ERR;
    }
  }
  cur->stateFlags |= ACTX_F_OTHERINDEXED;
  return REDISMODULE_OK;
}

int Indexer_Add(Indexer *bi, RSAddDocumentCtx *aCtx) {
  int rv = ACTX_Preprocess(aCtx);
  if (rv == REDISMODULE_OK) {
    *array_ensure_tail(&bi->docs, RSAddDocumentCtx *) = aCtx;
  }
  return rv;
}

void Indexer_Index(Indexer *bi, IndexerCallback cb, void *data) {
  // first, try and assign IDs to all the documents in the queue..
  BulkInfo binfo = {.nactive = 0};
  size_t n = array_len(bi->docs);
  for (size_t ii = 0; ii < n; ++ii) {
    RSAddDocumentCtx *cur = bi->docs[ii];
    if (doAssignId(cur, bi->sctx) != REDISMODULE_OK) {
      setDocAsErr(bi, cur, ii);
      continue;
    }
    if ((cur->stateFlags & ACTX_F_TEXTINDEXED) == 0) {
      addTextDoc(bi, cur);
    }
    if ((cur->stateFlags & ACTX_F_OTHERINDEXED) == 0) {
      if (addNontextDoc(bi, cur, &binfo) != REDISMODULE_OK) {
        setDocAsErr(bi, cur, ii);
        continue;
      }
    }
  }
  writeMergedEntries(bi);
  for (size_t ii = 0; ii < binfo.nactive; ++ii) {
    IndexerBulkCleanup(binfo.active[ii], bi->sctx);
  }

  if (cb) {
    Indexer_Iterate(bi, cb, data);
  }
}

void Indexer_Iterate(Indexer *bi, IndexerCallback cb, void *data) {
  size_t n = array_len(bi->docs);
  for (size_t ii = 0; ii < n; ++ii) {
    cb(bi->docs[ii], data);
  }
  n = array_len(bi->errs);
  for (size_t ii = 0; ii < n; ++ii) {
    cb(bi->errs[ii], data);
  }
}

void Indexer_Destroy(Indexer *bi) {
  KHTable_Clear(&bi->mergeHt);
  KHTable_Free(&bi->mergeHt);
  BlkAlloc_FreeAll(&bi->alloc, NULL, 0, 0);
  if (bi->errs) {
    array_free(bi->errs);
  }
  if (bi->docs) {
    array_free(bi->docs);
  }
}

void Indexer_Reset(Indexer *bi) {
  BlkAlloc_Clear(&bi->alloc, NULL, NULL, 0);
  KHTable_Clear(&bi->mergeHt);

  BlkAlloc_Init(&bi->alloc);
  static const KHTableProcs procs = {
      .Alloc = mergedAlloc, .Compare = mergedCompare, .Hash = mergedHash};
  KHTable_Init(&bi->mergeHt, &procs, &bi->alloc, 4096);
  if (bi->docs) {
    array_clear(bi->docs);
  } else {
    bi->docs = array_new(RSAddDocumentCtx *, 8);
  }
  if (bi->errs) {
    array_clear(bi->errs);
  } else {
    bi->errs = array_new(RSAddDocumentCtx *, 8);
  }
}

void Indexer_Init(Indexer *bi, RedisSearchCtx *sctx) {
  bi->sctx = sctx;
  SearchCtx_Incref(sctx);
  Indexer_Reset(bi);
}
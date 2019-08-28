#include "indexer.h"
#include "forward_index.h"
#include "numeric_index.h"
#include "inverted_index.h"
#include "geo_index.h"
#include "index.h"
#include "redis_index.h"

#include <assert.h>
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

// Merges all terms in the queue into a single hash table.
// parentMap is assumed to be a RSAddDocumentCtx*[] of capacity MAX_DOCID_ENTRIES
//
// This function returns the first aCtx which lacks its own document ID.
// This wil be used when actually assigning document IDs later on, so that we
// don't need to seek the document list again for it.
static RSAddDocumentCtx *doMerge(RSAddDocumentCtx *aCtx, KHTable *ht,
                                 RSAddDocumentCtx **parentMap) {

  // Counter is to make sure we don't block the CPU if there are many many items
  // in the queue, though in reality the number of iterations is also limited
  // by MAX_DOCID_ENTRIES
  size_t counter = 0;

  // Current index within the parentMap, this is assigned as the placeholder
  // doc ID value
  size_t curIdIdx = 0;

  RSAddDocumentCtx *cur = aCtx;
  RSAddDocumentCtx *firstZeroId = NULL;

  while (cur && ++counter < 1000 && curIdIdx < MAX_BULK_DOCS) {

    ForwardIndexIterator it = ForwardIndex_Iterate(cur->fwIdx);
    ForwardIndexEntry *entry = ForwardIndexIterator_Next(&it);

    while (entry) {
      // Because we don't have the actual document ID at this point, the document
      // ID field will be used here to point to an index in the parentMap
      // that will contain the parent. The parent itself will contain the
      // document ID when assigned (when the lock is held).
      entry->docId = curIdIdx;

      // Get the entry for it.
      int isNew = 0;
      mergedEntry *mergedEnt =
          (mergedEntry *)KHTable_GetEntry(ht, entry->term, entry->len, entry->hash, &isNew);

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
    parentMap[curIdIdx++] = cur;
    if (firstZeroId == NULL && cur->doc.docId == 0) {
      firstZeroId = cur;
    }

    cur = cur->next;
  }
  return firstZeroId;
}

// Writes all the entries in the hash table to the inverted index.
// parentMap contains the actual mapping between the `docID` field and the actual
// RSAddDocumentCtx which contains the document itself, which by this time should
// have been assigned an ID via makeDocumentId()
static int writeMergedEntries(DocumentIndexer *indexer, RSAddDocumentCtx *aCtx, RedisSearchCtx *ctx,
                              KHTable *ht, RSAddDocumentCtx **parentMap) {

  IndexEncoder encoder = InvertedIndex_GetEncoder(ctx->spec->flags);
  const int isBlocked = AddDocumentCtx_IsBlockable(aCtx);

  // This is used as a cache layer, so that we don't need to derefernce the
  // RSAddDocumentCtx each time.
  uint32_t docIdMap[MAX_BULK_DOCS] = {0};

  // Iterate over all the entries
  for (uint32_t curBucketIdx = 0; curBucketIdx < ht->numBuckets; curBucketIdx++) {
    for (KHTableEntry *entp = ht->buckets[curBucketIdx]; entp; entp = entp->next) {
      mergedEntry *merged = (mergedEntry *)entp;

      // Open the inverted index:
      ForwardIndexEntry *fwent = merged->head;

      // Add the term to the prefix trie. This only needs to be done once per term
      if (fwent->addToTermsTrie) {
        IndexSpec_AddTerm(ctx->spec, fwent->term, fwent->len);
      }

      RedisModuleKey *idxKey = NULL;
      InvertedIndex *invidx = Redis_OpenInvertedIndexEx(ctx, fwent->term, fwent->len, 1, &idxKey);

      if (invidx == NULL) {
        continue;
      }

      for (; fwent != NULL; fwent = fwent->next) {
        // Get the Doc ID for this entry.
        // Note that we cache the lookup result itself, since accessing the
        // parent each time causes some memory access overhead. This saves
        // about 3% overall.
        uint32_t docId = docIdMap[fwent->docId];
        if (docId == 0) {
          // Meaning the entry is not yet in the cache.
          RSAddDocumentCtx *parent = parentMap[fwent->docId];
          if ((parent->stateFlags & ACTX_F_ERRORED) || parent->doc.docId == 0) {
            // Has an error, or for some reason it doesn't have a document ID(!? is this possible)
            continue;
          } else {
            // Place the entry in the cache, so we don't need a pointer dereference next time
            docId = docIdMap[fwent->docId] = parent->doc.docId;
          }
        }

        // Finally assign the document ID to the entry
        fwent->docId = docId;
        writeIndexEntry(ctx->spec, invidx, encoder, fwent);
      }

      if (idxKey) {
        RedisModule_CloseKey(idxKey);
      }

      if (isBlocked && CONCURRENT_CTX_TICK(&indexer->concCtx) && ctx->spec == NULL) {
        aCtx->errorString = "ERR Index is no longer valid!";
        return -1;
      }
    }
  }
  return 0;
}

/**
 * Simple implementation, writes all the entries for a single document. This
 * function is used when there is only one item in the queue. In this case
 * it's simpler to forego building the merged dictionary because there is
 * nothing to merge.
 */
static void writeCurEntries(DocumentIndexer *indexer, RSAddDocumentCtx *aCtx, RedisSearchCtx *ctx) {
  ForwardIndexIterator it = ForwardIndex_Iterate(aCtx->fwIdx);
  ForwardIndexEntry *entry = ForwardIndexIterator_Next(&it);
  IndexEncoder encoder = InvertedIndex_GetEncoder(aCtx->specFlags);
  const int isBlocked = AddDocumentCtx_IsBlockable(aCtx);

  while (entry != NULL) {
    RedisModuleKey *idxKey = NULL;

    if (entry->addToTermsTrie) {
      IndexSpec_AddTerm(ctx->spec, entry->term, entry->len);
    }

    assert(ctx);

    InvertedIndex *invidx = Redis_OpenInvertedIndexEx(ctx, entry->term, entry->len, 1, &idxKey);
    if (invidx) {
      entry->docId = aCtx->doc.docId;
      assert(entry->docId);
      writeIndexEntry(ctx->spec, invidx, encoder, entry);
    }
    if (idxKey) {
      RedisModule_CloseKey(idxKey);
    }

    entry = ForwardIndexIterator_Next(&it);
    if (isBlocked && CONCURRENT_CTX_TICK(&indexer->concCtx) && ctx->spec == NULL) {
      aCtx->errorString = "ERR Index is no longer valid!";
      return;
    }
  }
}

/** Assigns a document ID to a single document. */
static int makeDocumentId(RSAddDocumentCtx *aCtx, IndexSpec *spec, int replace,
                          const char **errorString) {
  DocTable *table = &spec->docs;
  Document *doc = &aCtx->doc;
  RSDocumentKey docKey = MakeDocKeyR(doc->docKey);
  if (replace) {
    RSDocumentMetadata *dmd = DocTable_Pop(table, docKey);
    if (dmd) {
      // decrease the number of documents in the index stats only if the document was there
      --spec->stats.numDocuments;
      aCtx->oldMd = dmd;

      if (spec->gc) {
        // tell the GC we have a deleted document
        GCContext_OnDelete(spec->gc);
      }
    }
  }

  doc->docId = DocTable_Put(table, docKey, doc->score, 0, doc->payload, doc->payloadSize);
  if (doc->docId == 0) {
    *errorString = "Document already exists";
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
static void doAssignIds(RSAddDocumentCtx *cur, RedisSearchCtx *ctx) {
  IndexSpec *spec = ctx->spec;
  for (; cur; cur = cur->next) {
    if (cur->stateFlags & ACTX_F_ERRORED) {
      continue;
    }

    assert(!cur->doc.docId);
    int rv = makeDocumentId(cur, spec, cur->options & DOCUMENT_ADD_REPLACE, &cur->errorString);
    if (rv != 0) {
      cur->stateFlags |= ACTX_F_ERRORED;
      continue;
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
  }
}

static void indexBulkFields(RSAddDocumentCtx *aCtx, RedisSearchCtx *sctx) {
  // Traverse all fields, seeing if there may be something which can be written!
  IndexBulkData bData[SPEC_MAX_FIELDS] = {{0}};
  IndexBulkData *activeBulks[SPEC_MAX_FIELDS];
  size_t numActiveBulks = 0;

  for (RSAddDocumentCtx *cur = aCtx; cur && cur->doc.docId; cur = cur->next) {
    if (cur->stateFlags & ACTX_F_ERRORED) {
      continue;
    }

    const Document *doc = &cur->doc;
    for (size_t ii = 0; ii < doc->numFields; ++ii) {
      const FieldSpec *fs = cur->fspecs + ii;
      fieldData *fdata = cur->fdatas + ii;
      if (fs->name == NULL || fs->type == FIELD_FULLTEXT || !FieldSpec_IsIndexable(fs)) {
        continue;
      }

      const BulkIndexer *procs = GetBulkIndexer(fs->type);
      IndexBulkData *bulk = &bData[fs->index];
      if (!bulk->initialized) {
        if (procs->BulkInit) {
          procs->BulkInit(bulk, fs, sctx);
        }
        bulk->initialized = 1;
        bulk->type = fs->type;
        activeBulks[numActiveBulks++] = bulk;
      }
      if (procs->BulkAdd(bulk, cur, sctx, doc->fields + ii, fs, fdata, &cur->errorString) != 0) {
        cur->stateFlags |= ACTX_F_ERRORED;
      }
      cur->stateFlags |= ACTX_F_OTHERINDEXED;
    }
  }

  // Flush it!
  for (size_t ii = 0; ii < numActiveBulks; ++ii) {
    IndexBulkData *cur = activeBulks[ii];
    const BulkIndexer *procs = GetBulkIndexer(cur->type);
    if (procs->BulkDone) {
      procs->BulkDone(cur, sctx);
    }
    if (cur->indexKey) {
      RedisModule_CloseKey(cur->indexKey);
    }
  }
}

static void reopenCb(RedisModuleKey *k, void *arg) {
  // Index Key
  RedisSearchCtx *ctx = arg;
  // we do not allow empty indexes when loading an existing index
  if (k == NULL || RedisModule_KeyType(k) == REDISMODULE_KEYTYPE_EMPTY ||
      RedisModule_ModuleTypeGetType(k) != IndexSpecType) {
    ctx->spec = NULL;
    return;
  }

  ctx->spec = RedisModule_ModuleTypeGetValue(k);
}

// Routines for the merged hash table
static const KHTableProcs mergedHtProcs = {
    .Alloc = mergedAlloc, .Compare = mergedCompare, .Hash = mergedHash};

#define ACTX_IS_INDEXED(actx)                                           \
  (((actx)->stateFlags & (ACTX_F_OTHERINDEXED | ACTX_F_TEXTINDEXED)) == \
   (ACTX_F_OTHERINDEXED | ACTX_F_TEXTINDEXED))

/**
 * Perform the processing chain on a single document entry, optionally merging
 * the tokens of further entries in the queue
 */
static void Indexer_Process(DocumentIndexer *indexer, RSAddDocumentCtx *aCtx) {
  RSAddDocumentCtx *parentMap[MAX_BULK_DOCS];
  RSAddDocumentCtx *firstZeroId = aCtx;
  RedisSearchCtx ctx = {NULL};

  if (ACTX_IS_INDEXED(aCtx) || aCtx->stateFlags & (ACTX_F_ERRORED)) {
    // Document is complete or errored. No need for further processing.
    return;
  }

  int useTermHt = indexer->size > 1 && (aCtx->stateFlags & ACTX_F_TEXTINDEXED) == 0;
  if (useTermHt) {
    firstZeroId = doMerge(aCtx, &indexer->mergeHt, parentMap);
    if (firstZeroId && firstZeroId->stateFlags & ACTX_F_ERRORED) {
      // Don't treat an errored ctx as being the head of a new ID chain. It's
      // likely that subsequent entries do indeed have IDs.
      firstZeroId = NULL;
    }
  }

  const int isBlocked = AddDocumentCtx_IsBlockable(aCtx);

  if (isBlocked) {
    // Force a context at this point:
    if (!indexer->isDbSelected) {
      RedisModuleCtx *thCtx = RedisModule_GetThreadSafeContext(aCtx->client.bc);
      RedisModule_SelectDb(indexer->redisCtx, RedisModule_GetSelectedDb(thCtx));
      RedisModule_FreeThreadSafeContext(thCtx);
      indexer->isDbSelected = 1;
    }

    ctx.redisCtx = indexer->redisCtx;
    ConcurrentSearch_SetKey(&indexer->concCtx, indexer->specKeyName, &ctx);
    ConcurrentSearchCtx_ResetClock(&indexer->concCtx);
    ConcurrentSearchCtx_Lock(&indexer->concCtx);
  } else {
    ctx = *aCtx->client.sctx;
  }

  if (!ctx.spec || ctx.spec->uniqueId != aCtx->specUniqueId) {
    aCtx->errorString = "ERR Index no longer valid";
    aCtx->stateFlags |= ACTX_F_ERRORED;
    goto cleanup;
  }

  Document *doc = &aCtx->doc;

  /**
   * Document ID assignment:
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
  if (firstZeroId != NULL && firstZeroId->doc.docId == 0) {
    doAssignIds(firstZeroId, &ctx);
  }

  // Handle FULLTEXT indexes
  if (useTermHt) {
    writeMergedEntries(indexer, aCtx, &ctx, &indexer->mergeHt, parentMap);
  } else if ((aCtx->fwIdx && (aCtx->stateFlags & ACTX_F_ERRORED) == 0)) {
    writeCurEntries(indexer, aCtx, &ctx);
  }

  if (!(aCtx->stateFlags & ACTX_F_OTHERINDEXED)) {
    indexBulkFields(aCtx, &ctx);
  }

cleanup:
  if (isBlocked) {
    ConcurrentSearchCtx_Unlock(&indexer->concCtx);
  }
  if (useTermHt) {
    BlkAlloc_Clear(&indexer->alloc, NULL, NULL, 0);
    KHTable_Clear(&indexer->mergeHt);
  }
}

static void DocumentIndexer_Free(DocumentIndexer *indexer) {
  rm_free(indexer->name);
  //  BlkAlloc_FreeAll(&indexer->alloc);
  pthread_cond_destroy(&indexer->cond);
  pthread_mutex_destroy(&indexer->lock);
  rm_free(indexer->concCtx.openKeys);
  RedisModule_FreeString(indexer->redisCtx, indexer->specKeyName);
  KHTable_Clear(&indexer->mergeHt);
  KHTable_Free(&indexer->mergeHt);
  BlkAlloc_FreeAll(&indexer->alloc, NULL, 0, 0);
  RedisModule_FreeThreadSafeContext(indexer->redisCtx);
  rm_free(indexer);
}

static void *Indexer_Run(void *p) {
  DocumentIndexer *indexer = p;

  pthread_mutex_lock(&indexer->lock);
  while (!indexer->shouldStop) {
    while (indexer->head == NULL && !indexer->shouldStop) {
      pthread_cond_wait(&indexer->cond, &indexer->lock);
    }

    if (indexer->shouldStop) {
      break;
    }

    RSAddDocumentCtx *cur = indexer->head;
    indexer->size--;

    if ((indexer->head = cur->next) == NULL) {
      indexer->tail = NULL;
    }
    pthread_mutex_unlock(&indexer->lock);
    Indexer_Process(indexer, cur);
    AddDocumentCtx_Finish(cur);
    pthread_mutex_lock(&indexer->lock);
  }

  DocumentIndexer_Free(indexer);

  return NULL;
}

int Indexer_Add(DocumentIndexer *indexer, RSAddDocumentCtx *aCtx) {
  if (!AddDocumentCtx_IsBlockable(aCtx)) {
    Indexer_Process(indexer, aCtx);
    AddDocumentCtx_Finish(aCtx);
    return 0;
  }

  pthread_mutex_lock(&indexer->lock);

  if (indexer->tail) {
    indexer->tail->next = aCtx;
    indexer->tail = aCtx;
  } else {
    indexer->head = indexer->tail = aCtx;
  }

  pthread_cond_signal(&indexer->cond);
  pthread_mutex_unlock(&indexer->lock);

  indexer->size++;
  return 0;
}

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
/// Multiple Indexers                                                        ///
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

/**
 * Each index (i.e. IndexSpec) will have its own dedicated indexing thread.
 * This is because documents only need to be indexed in order with respect
 * to their document IDs, and the ID namespace is only unique among a given
 * index.
 *
 * Separating background threads also greatly simplifies the work of merging
 * or folding indexing and document ID assignment, as it can be assumed that
 * every item within the document ID belongs to the same index.
 */

// List of all the index threads
typedef struct {
  DocumentIndexer *first;  // First thread in the list
} IndexerList;

// Instance of list
static IndexerList indexers_g = {NULL, 0};

// Returns the given indexer, if it exists
static DocumentIndexer *findAndRemoveDocumentIndexer(const char *specname) {
  DocumentIndexer *prev = NULL;
  for (DocumentIndexer *cur = indexers_g.first; cur; cur = cur->next) {
    if (strcmp(specname, cur->name) == 0) {
      if (prev) {
        prev->next = cur->next;
      } else {
        indexers_g.first = cur->next;
      }
      return cur;
    }
    prev = cur;
  }
  return NULL;
}

static DocumentIndexer *findDocumentIndexer(const char *specname) {
  for (DocumentIndexer *cur = indexers_g.first; cur; cur = cur->next) {
    if (strcmp(specname, cur->name) == 0) {
      return cur;
    }
  }
  return NULL;
}

// Creates a new DocumentIndexer. This initializes the structure and starts the
// thread. This does not insert it into the list of threads, though
// todo: remove the withIndexThread var once we switch to threadpool
static DocumentIndexer *NewDocumentIndexer(const char *name, int options) {
  DocumentIndexer *indexer = rm_calloc(1, sizeof(*indexer));
  indexer->options = options;
  indexer->head = indexer->tail = NULL;

  BlkAlloc_Init(&indexer->alloc);
  static const KHTableProcs procs = {
      .Alloc = mergedAlloc, .Compare = mergedCompare, .Hash = mergedHash};
  KHTable_Init(&indexer->mergeHt, &procs, &indexer->alloc, 4096);

  if (!(options & INDEXER_THREADLESS)) {
    pthread_cond_init(&indexer->cond, NULL);
    pthread_mutex_init(&indexer->lock, NULL);
    static pthread_t dummyThr;
    pthread_create(&dummyThr, NULL, Indexer_Run, indexer);
  }
  indexer->name = rm_strdup(name);
  indexer->next = NULL;
  indexer->redisCtx = RedisModule_GetThreadSafeContext(NULL);
  indexer->specKeyName =
      RedisModule_CreateStringPrintf(indexer->redisCtx, INDEX_SPEC_KEY_FMT, indexer->name);

  ConcurrentSearchCtx_InitSingle(&indexer->concCtx, indexer->redisCtx,
                                 REDISMODULE_READ | REDISMODULE_WRITE, reopenCb);
  return indexer;
}

void DropDocumentIndexer(const char *specname) {
  DocumentIndexer *match = findAndRemoveDocumentIndexer(specname);
  if (!match) {
    return;
  }

  match->isDeleted = true;

  if (match->refCounter == 0) {
    if (match->options & INDEXER_THREADLESS) {
      DocumentIndexer_Free(match);
    } else {
      pthread_mutex_lock(&match->lock);
      match->shouldStop = true;
      pthread_cond_signal(&match->cond);
      pthread_mutex_unlock(&match->lock);
    }
    return;
  }
  assert(!(match->options & INDEXER_THREADLESS));
}

// Get the document indexer for the given index name. If the indexer does not
// exist, it is created and placed into the list of indexes
DocumentIndexer *GetDocumentIndexer(const char *specname, int options) {
  DocumentIndexer *match = findDocumentIndexer(specname);
  if (match) {
    return match;
  }

  DocumentIndexer *newIndexer = NewDocumentIndexer(specname, options);
  newIndexer->next = indexers_g.first;
  indexers_g.first = newIndexer;
  return newIndexer;
}

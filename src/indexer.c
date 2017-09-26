#include "indexer.h"
#include "forward_index.h"
#include "numeric_index.h"
#include "inverted_index.h"
#include "geo_index.h"
#include "index.h"
#include "redis_index.h"

#include <assert.h>

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
#define MAX_DOCID_ENTRIES 1024

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
static void doMerge(RSAddDocumentCtx *aCtx, KHTable *ht, RSAddDocumentCtx **parentMap) {

  // Counter is to make sure we don't block the CPU if there are many many items
  // in the queue, though in reality the number of iterations is also limited
  // by MAX_DOCID_ENTRIES
  size_t counter = 0;

  // Current index within the parentMap, this is assigned as the placeholder
  // doc ID value
  size_t curIdIdx = 0;

  RSAddDocumentCtx *cur = aCtx;

  while (cur && ++counter < 1000 && curIdIdx < MAX_DOCID_ENTRIES) {

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

    cur->stateFlags |= ACTX_F_MERGED;
    parentMap[curIdIdx++] = cur;

    cur = cur->next;
  }
}

// Writes all the entries in the hash table to the inverted index.
// parentMap contains the actual mapping between the `docID` field and the actual
// RSAddDocumentCtx which contains the document itself, which by this time should
// have been assigned an ID via makeDocumentId()
static int writeMergedEntries(DocumentIndexer *indexer, RSAddDocumentCtx *aCtx, RedisSearchCtx *ctx,
                              KHTable *ht, RSAddDocumentCtx **parentMap) {

  IndexEncoder encoder = InvertedIndex_GetEncoder(ctx->spec->flags);

  // This is used as a cache layer, so that we don't need to derefernce the
  // RSAddDocumentCtx each time.
  uint32_t docIdMap[MAX_DOCID_ENTRIES] = {0};

  // Iterate over all the entries
  for (uint32_t curBucketIdx = 0; curBucketIdx < ht->numBuckets; curBucketIdx++) {
    for (KHTableEntry *entp = ht->buckets[curBucketIdx]; entp; entp = entp->next) {
      mergedEntry *merged = (mergedEntry *)entp;

      // Open the inverted index:
      ForwardIndexEntry *fwent = merged->head;

      // Add the term to the prefix trie. This only needs to be done once per term
      IndexSpec_AddTerm(ctx->spec, fwent->term, fwent->len);

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

      if (CONCURRENT_CTX_TICK(&indexer->concCtx) && ctx->spec == NULL) {
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

  while (entry != NULL) {
    RedisModuleKey *idxKey = NULL;

    IndexSpec_AddTerm(ctx->spec, entry->term, entry->len);

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
    if (CONCURRENT_CTX_TICK(&indexer->concCtx) && ctx->spec == NULL) {
      aCtx->errorString = "ERR Index is no longer valid!";
      return;
    }
  }
}

/** Assigns a document ID to a single document. */
static int makeDocumentId(Document *doc, IndexSpec *spec, int replace, const char **errorString) {
  const char *keystr = RedisModule_StringPtrLen(doc->docKey, NULL);
  DocTable *table = &spec->docs;
  if (replace) {
    if (DocTable_Delete(table, keystr)) {
      // decrease the number of documents in the index stats only if the document was there
      --spec->stats.numDocuments;
    }
  }
  doc->docId = DocTable_Put(table, keystr, doc->score, 0, doc->payload, doc->payloadSize);
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
    int rv =
        makeDocumentId(&cur->doc, spec, cur->options & DOCUMENT_ADD_REPLACE, &cur->errorString);
    if (rv != 0) {
      cur->stateFlags |= ACTX_F_ERRORED;
      continue;
    }

    RSDocumentMetadata *md = DocTable_Get(&spec->docs, cur->doc.docId);
    md->maxFreq = cur->fwIdx->maxFreq;
    if (cur->sv) {
      DocTable_SetSortingVector(&spec->docs, cur->doc.docId, cur->sv);
      cur->sv = NULL;
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

/**
 * Perform the processing chain on a single document entry, optionally merging
 * the tokens of further entries in the queue
 */
static void Indexer_Process(DocumentIndexer *indexer, RSAddDocumentCtx *aCtx) {
  RSAddDocumentCtx *parentMap[MAX_DOCID_ENTRIES];
  RedisSearchCtx ctx = {NULL};

  if (aCtx->stateFlags & ACTX_F_ERRORED) {
    // Document has an error, no need for further processing
    return;
  }

  // If this document's tokens are merged or not
  const int needsFtIndex = !(aCtx->stateFlags & ACTX_F_MERGED);

  // If all its text fields have been merged and it doesn't need any non-text field
  // handling, return and save ourselves allocating new contexts.
  if (!needsFtIndex && (aCtx->stateFlags & ACTX_F_NONTXTFLDS) == 0) {
    return;
  }

  int useHt = 0;
  if (indexer->size > 1 && needsFtIndex) {
    useHt = 1;
    doMerge(aCtx, &indexer->mergeHt, parentMap);
  }

  // Force a context at this point:
  if (!indexer->isDbSelected) {
    RedisModuleCtx *thCtx = RedisModule_GetThreadSafeContext(aCtx->bc);
    RedisModule_SelectDb(indexer->redisCtx, RedisModule_GetSelectedDb(thCtx));
    RedisModule_FreeThreadSafeContext(thCtx);
    indexer->isDbSelected = 1;
  }

  ctx.redisCtx = indexer->redisCtx;

  ConcurrentSearch_SetKey(&indexer->concCtx, indexer->redisCtx, indexer->specKeyName, &ctx);
  ConcurrentSearchCtx_ResetClock(&indexer->concCtx);
  ConcurrentSearchCtx_Lock(&indexer->concCtx);

  if (!ctx.spec) {
    aCtx->errorString = "ERR Index no longer valid";
    aCtx->stateFlags |= ACTX_F_ERRORED;
    goto cleanup;
  }

  Document *doc = &aCtx->doc;

  /**
   * Document ID assignment:
   * In order to hold the GIL for as short a time as possible, we assign
   * document IDs in bulk. The assumption is made that all aCtxs which have
   * the MERGED flag set are either merged from a previous aCtx, and therefore
   * already have a document ID, or they are merged as the beginning of a new
   * merge table (mergedEntries) and all lack document IDs.
   *
   * Assigning IDs in bulk speeds up indexing of smaller documents by about
   * 10% overall.
   */
  if (!doc->docId) {
    doAssignIds(aCtx, &ctx);
    if (aCtx->stateFlags & ACTX_F_ERRORED) {
      if (useHt) {
        writeMergedEntries(indexer, aCtx, &ctx, &indexer->mergeHt, parentMap);
      }
      goto cleanup;
    }
  }

  if (needsFtIndex) {
    if (useHt) {
      writeMergedEntries(indexer, aCtx, &ctx, &indexer->mergeHt, parentMap);
    } else {
      writeCurEntries(indexer, aCtx, &ctx);
    }
  }

  for (int i = 0; i < doc->numFields; i++) {
    const FieldSpec *fs = aCtx->fspecs + i;
    fieldData *fdata = aCtx->fdatas + i;
    if (fs->name == NULL || !FieldSpec_IsIndexable(fs)) {
      // Means this document field does not have a corresponding spec
      continue;
    }

    IndexerFunc ifx = GetIndexIndexer(fs->type);
    if (ifx == NULL) {
      continue;
    }

    if (ifx(aCtx, &ctx, &doc->fields[i], fs, fdata, &aCtx->errorString) != 0) {
      aCtx->stateFlags |= ACTX_F_ERRORED;
      goto cleanup;
    }
  }

cleanup:
  ConcurrentSearchCtx_Unlock(&indexer->concCtx);
  if (useHt) {
    BlkAlloc_Clear(&indexer->alloc, NULL, NULL, 0);
    KHTable_Clear(&indexer->mergeHt);
  }
}

static void *Indexer_Run(void *p) {
  DocumentIndexer *indexer = p;

  while (1) {
    pthread_mutex_lock(&indexer->lock);
    while (indexer->head == NULL) {
      pthread_cond_wait(&indexer->cond, &indexer->lock);
    }

    RSAddDocumentCtx *cur = indexer->head;
    indexer->size--;

    if ((indexer->head = cur->next) == NULL) {
      indexer->tail = NULL;
    }
    pthread_mutex_unlock(&indexer->lock);
    Indexer_Process(indexer, cur);
    AddDocumentCtx_Finish(cur);
  }
  return NULL;
}

int Indexer_Add(DocumentIndexer *indexer, RSAddDocumentCtx *aCtx) {

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
  volatile int lockMod;    // "Spinlock" in case the list needs to be modified
} IndexerList;

// Instance of list
static IndexerList indexers_g = {NULL, 0};

// Returns the given indexer, if it exists
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
static DocumentIndexer *NewDocumentIndexer(const char *name) {
  DocumentIndexer *indexer = calloc(1, sizeof(*indexer));
  indexer->head = indexer->tail = NULL;

  BlkAlloc_Init(&indexer->alloc);
  static const KHTableProcs procs = {
      .Alloc = mergedAlloc, .Compare = mergedCompare, .Hash = mergedHash};
  KHTable_Init(&indexer->mergeHt, &procs, &indexer->alloc, 4096);

  ConcurrentSearchCtx_Init(NULL, &indexer->concCtx);
  pthread_cond_init(&indexer->cond, NULL);
  pthread_mutex_init(&indexer->lock, NULL);
  static pthread_t dummyThr;
  pthread_create(&dummyThr, NULL, Indexer_Run, indexer);
  indexer->name = strdup(name);
  indexer->next = NULL;
  indexer->redisCtx = RedisModule_GetThreadSafeContext(NULL);
  indexer->specKeyName =
      RedisModule_CreateStringPrintf(indexer->redisCtx, INDEX_SPEC_KEY_FMT, indexer->name);

  ConcurrentSearchCtx_InitEx(&indexer->concCtx, REDISMODULE_READ | REDISMODULE_WRITE, reopenCb);
  return indexer;
}

// Get the document indexer for the given index name. If the indexer does not
// exist, it is created and placed into the list of indexes
DocumentIndexer *GetDocumentIndexer(const char *specname) {
  DocumentIndexer *match = findDocumentIndexer(specname);
  if (match) {
    return match;
  }

  // This is akin to a spinlock. Wait until lockMod is 0, and then atomically
  // set it to 1.
  while (!__sync_bool_compare_and_swap(&indexers_g.lockMod, 0, 1)) {
  }

  // Try to find it again. Another thread may have modified the list while
  // we were waiting for lockMod to become 0.
  match = findDocumentIndexer(specname);
  if (match) {
    indexers_g.lockMod = 0;
    return match;
  }

  DocumentIndexer *newIndexer = NewDocumentIndexer(specname);
  newIndexer->next = indexers_g.first;
  indexers_g.first = newIndexer;
  indexers_g.lockMod = 0;
  return newIndexer;
}

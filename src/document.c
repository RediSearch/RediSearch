#include <string.h>
#include <assert.h>

#include "document.h"
#include "forward_index.h"
#include "geo_index.h"
#include "index.h"
#include "numeric_filter.h"
#include "numeric_index.h"
#include "redis_index.h"
#include "rmutil/strings.h"
#include "rmutil/util.h"
#include "spec.h"
#include "tokenize.h"
#include "util/logging.h"
#include "search_request.h"
#include "rmalloc.h"
#include "concurrent_ctx.h"

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
/// General Architecture                                                     ///
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

/**
 * The indexing process begins by creating a new RSAddDocumentCtx and adding
 * a document to it (as described in the header file). Internally this is
 * divided into several steps.
 *
 * [Creation]
 * When a new context is created, it caches several bits from the IndexSpec
 * so that it won't need to access them again until later. This reduces the
 * amount of times the context will need to lock the GIL.
 *
 * [Preprocessing]
 * It is assumed that Document_AddToIndexes is called on a separate thread,
 * and with the GIL being unlocked. The first thing this function does
 * is begin 'preprocessing'. Preprocessing in this sense just means converting
 * the raw field text into a format suitable for indexing. For numeric fields
 * this means parsing the value as a number, for geo fields this means parsing
 * the value as a coordinate pair, and for text fields, this means performing
 * tokenization. In all these cases, this is pure CPU processing and this can
 * all be parallelized because there is no need to hold the GIL.
 *
 * Each preprocessor function receives access to its 'fieldData' structure
 * where it can store the result of the preprocessing. The fulltext
 * preprocessing is the forward index, which is stored directly in the
 * AddDocumentCtx, as it is handled specially at a later stage.
 *
 * [Queueing]
 * After a document has been preprocessed, it is placed inside the indexing
 * queue via DocumentIndexer_Add. This schedules the document to be indexed
 * in the order it was placed within the queue. When the DocumentIndexer processes
 * the context, it will assign it a document ID and index the result of each of
 * the preprocessors, calling its corresponding 'indexer' function. Again, text
 * fields are handled specially. Once the indexing functions have all been
 * called, the indexing thread will send a reply back to the client.
 */

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
/// Indexing Thread Details                                                  ///
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
/**
 * The indexing thread ensures that documents are written in the order they have
 * been assigned their IDs. Because indexing and document ID assignment is done
 * in the same thread globally for the entire index, even if the thread sleeps
 * and/or yields, other threads will never insert items into the index, but
 * rather submit new items to the indexer queue.
 *
 * Because GIL locking is the essential bottleneck, the less time the lock is
 * held, and the fewer times it needs to be locked, the faster the indexing will
 * be. The indexer is designed in a way that GIL locking is kept to a minimum,
 * and document IDs are assigned at the latest possible point in time.
 *
 * At a high level, the indexing thread performs the following sequence:
 *
 * [Waiting]
 * The thread waits for an item to arrive on the queue. It then locks the queue
 * lock, pops the item, and then unlocks the queue lock again.
 *
 * [Merging Terms]
 * In this context, merging is creating a dictionary of terms found in all
 * documents currently in the queue. The dictionary value is a list of term
 * records, per the order its corresponding document was placed in the queue.
 *
 * [GIL Locking]
 * Once the terms are merged, the GIL is locked
 *
 * [Bulk ID Assignment]
 * If the current document does not have an ID, the bulk ID assignment begins.
 * At this point, all documents pending in the queue are being assigned a
 * document ID in serial. See the function definition to see why this is in
 * serial
 *
 * [Writing Merged Terms]
 * For each entry in the merged dictionary, its inverted index is open, and
 * the entry's records are written into the inverted index. This saves a lot
 * of CPU and GIL time because the inverted index is opened only once for
 * each term
 *
 * [Indexing other fields]
 * Other fields (e.g. numeric, geo) are indexed as well
 *
 * [GIL Unlocked, Reply Sent]
 * At this point, the document has been indexed.
 */

// Preprocessors can store field data to this location
typedef union FieldData {
  double numeric;  // i.e. the numeric value of the field
  struct {
    char *slon;
    char *slat;
  } geo;  // lon/lat pair
} fieldData;

typedef struct DocumentIndexer {
  RSAddDocumentCtx *head;       // first item in the queue
  RSAddDocumentCtx *tail;       // last item in the queue
  pthread_mutex_t lock;         // lock - only used when adding or removing items from the queue
  pthread_cond_t cond;          // condition - used to wait on items added to the queue
  size_t size;                  // number of items in the queue
  ConcurrentSearchCtx concCtx;  // GIL locking. This is repopulated with the relevant key data

  char *name;  // The name of the index this structure belongs to. For use with the list of indexers
  struct DocumentIndexer *next;  // Next structure in the indexer list
} DocumentIndexer;

// For documentation, see these functions' definitions
static void DocumentIndexer_Process(DocumentIndexer *indexer, RSAddDocumentCtx *ctx);
static void *DocumentIndexer_Run(void *arg);
static int DocumentIndexer_Add(DocumentIndexer *indexer, RSAddDocumentCtx *aCtx);
static DocumentIndexer *GetDocumentIndexer(const char *specname);

RSAddDocumentCtx *NewAddDocumentCtx(RedisModuleBlockedClient *client, IndexSpec *sp,
                                    Document *base) {
  // Block the client and create the context!
  RSAddDocumentCtx *aCtx = calloc(1, sizeof(*aCtx));
  aCtx->bc = client;
  aCtx->thCtx = RedisModule_GetThreadSafeContext(aCtx->bc);
  aCtx->doc = *base;

  RedisModule_AutoMemory(aCtx->thCtx);
  aCtx->rsCtx.redisCtx = aCtx->thCtx;
  aCtx->next = NULL;
  aCtx->rsCtx.keyName = RedisModule_CreateStringPrintf(aCtx->thCtx, INDEX_SPEC_KEY_FMT, sp->name);
  aCtx->rsCtx.spec = NULL;

  aCtx->fwIdx = NewForwardIndex(&aCtx->doc, sp->flags);
  aCtx->specFlags = sp->flags;
  aCtx->stopwords = sp->stopwords;

  StopWordList_Ref(sp->stopwords);

  // Also, get the field specs. We cache this here because the context is unlocked
  // during the actual tokenization
  Document *doc = &aCtx->doc;

  aCtx->fspecs = malloc(sizeof(*aCtx->fspecs) * doc->numFields);
  aCtx->fdatas = malloc(sizeof(*aCtx->fdatas) * doc->numFields);

  for (int i = 0; i < doc->numFields; i++) {
    const DocumentField *f = doc->fields + i;
    FieldSpec *fs = IndexSpec_GetField(sp, f->name, strlen(f->name));
    if (fs) {
      aCtx->fspecs[i] = *fs;
      if (FieldSpec_IsSortable(fs) && aCtx->sv == NULL) {
        aCtx->sv = NewSortingVector(sp->sortables->len);
      }
    } else {
      aCtx->fspecs[i].name = NULL;
    }
  }
  aCtx->doc.docId = 0;
  return aCtx;
}

void AddDocumentCtx_Free(RSAddDocumentCtx *aCtx) {
  Document_FreeDetached(&aCtx->doc, aCtx->thCtx);
  RedisModule_FreeThreadSafeContext(aCtx->thCtx);

  if (aCtx->fwIdx) {
    ForwardIndexFree(aCtx->fwIdx);
  }
  if (aCtx->stopwords) {
    StopWordList_Unref(aCtx->stopwords);
  }

  free(aCtx->fspecs);
  free(aCtx->fdatas);
  free(aCtx);
}

#define ACTX_SPEC(actx) (actx)->rsCtx.spec

#define FIELD_HANDLER(name)                                                                \
  static int name(RSAddDocumentCtx *aCtx, const DocumentField *field, const FieldSpec *fs, \
                  fieldData *fdata, const char **errorString)

#define FIELD_INDEXER FIELD_HANDLER
#define FIELD_PREPROCESSOR FIELD_HANDLER

typedef int (*FieldFunc)(RSAddDocumentCtx *aCtx, const DocumentField *field, const FieldSpec *fs,
                         fieldData *fdata, const char **errorString);

typedef FieldFunc PreprocessorFunc;
typedef FieldFunc IndexerFunc;

FIELD_PREPROCESSOR(fulltextPreprocessor) {
  const char *c = RedisModule_StringPtrLen(field->text, NULL);
  if (FieldSpec_IsSortable(fs)) {
    RSSortingVector_Put(aCtx->sv, fs->sortIdx, (void *)c, RS_SORTABLE_STR);
  }

  Stemmer *stemmer = FieldSpec_IsNoStem(fs) ? NULL : aCtx->fwIdx->stemmer;
  aCtx->totalTokens = tokenize(c, fs->weight, fs->id, aCtx->fwIdx, forwardIndexTokenFunc, stemmer,
                               aCtx->totalTokens, aCtx->stopwords);
  return 0;
}

FIELD_PREPROCESSOR(numericPreprocessor) {
  if (RedisModule_StringToDouble(field->text, &fdata->numeric) == REDISMODULE_ERR) {
    *errorString = "Could not parse numeric index value";
    return -1;
  }

  // If this is a sortable numeric value - copy the value to the sorting vector
  if (FieldSpec_IsSortable(fs)) {
    RSSortingVector_Put(aCtx->sv, fs->sortIdx, &fdata->numeric, RS_SORTABLE_NUM);
  }
  return 0;
}

FIELD_INDEXER(numericIndexer) {
  NumericRangeTree *rt = OpenNumericIndex(&aCtx->rsCtx, fs->name);
  NumericRangeTree_Add(rt, aCtx->doc.docId, fdata->numeric);
  return 0;
}

FIELD_PREPROCESSOR(geoPreprocessor) {
  const char *c = RedisModule_StringPtrLen(field->text, NULL);
  char *pos = strpbrk(c, " ,");
  if (!pos) {
    *errorString = "Invalid lon/lat format. Use \"lon lat\" or \"lon,lat\"";
    return -1;
  }
  *pos = '\0';
  pos++;
  fdata->geo.slon = (char *)c;
  fdata->geo.slat = (char *)pos;
  return 0;
}

FIELD_INDEXER(geoIndexer) {
  GeoIndex gi = {.ctx = &aCtx->rsCtx, .sp = fs};
  int rv = GeoIndex_AddStrings(&gi, aCtx->doc.docId, fdata->geo.slon, fdata->geo.slat);

  if (rv == REDISMODULE_ERR) {
    *errorString = "Could not index geo value";
    return -1;
  }
  return 0;
}

static PreprocessorFunc getPreprocessor(const FieldType ft) {
  switch (ft) {
    case F_FULLTEXT:
      return fulltextPreprocessor;
    case F_NUMERIC:
      return numericPreprocessor;
    case F_GEO:
      return geoPreprocessor;
    default:
      return NULL;
  }
}

static IndexerFunc getIndexer(const FieldType ft) {
  switch (ft) {
    case F_NUMERIC:
      return numericIndexer;
    case F_GEO:
      return geoIndexer;
    case F_FULLTEXT:
    default:
      return NULL;
  }
}

static void sendReply(RSAddDocumentCtx *aCtx) {
  if (aCtx->errorString) {
    RedisModule_ReplyWithError(aCtx->thCtx, aCtx->errorString);
  } else {
    RedisModule_ReplyWithSimpleString(aCtx->thCtx, "OK");
  }
  RedisModule_UnblockClient(aCtx->bc, aCtx);
}

int Document_AddToIndexes(RSAddDocumentCtx *aCtx) {
  Document *doc = &aCtx->doc;
  RedisSearchCtx *ctx = &aCtx->rsCtx;

  int ourRv = REDISMODULE_OK;

  for (int i = 0; i < doc->numFields; i++) {
    const FieldSpec *fs = aCtx->fspecs + i;
    fieldData *fdata = aCtx->fdatas + i;
    if (fs->name == NULL) {
      LG_DEBUG("Skipping field %s not in index!", doc->fields[i].name);
      continue;
    }

    // Get handler
    PreprocessorFunc pp = getPreprocessor(fs->type);
    if (pp == NULL) {
      continue;
    }

    if (pp(aCtx, &doc->fields[i], fs, fdata, &aCtx->errorString) != 0) {
      ourRv = REDISMODULE_ERR;
      goto cleanup;
    }
  }

  // Get the specname
  const char *specName = RedisModule_StringPtrLen(aCtx->rsCtx.keyName, NULL);
  specName += sizeof(INDEX_SPEC_KEY_FMT) - 1;

  if (DocumentIndexer_Add(GetDocumentIndexer(specName), aCtx) != 0) {
    ourRv = REDISMODULE_ERR;
    goto cleanup;
  }

cleanup:
  if (ourRv != REDISMODULE_OK) {
    if (aCtx->errorString == NULL) {
      aCtx->errorString = "ERR couldn't index document";
    }
    sendReply(aCtx);
  }
  return ourRv;
}

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
static int writeMergedEntries(DocumentIndexer *indexer, RSAddDocumentCtx *aCtx, KHTable *ht,
                              RSAddDocumentCtx **parentMap) {

  IndexEncoder encoder = InvertedIndex_GetEncoder(ACTX_SPEC(aCtx)->flags);

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
      IndexSpec_AddTerm(aCtx->rsCtx.spec, fwent->term, fwent->len);

      RedisModuleKey *idxKey = NULL;
      InvertedIndex *invidx =
          Redis_OpenInvertedIndexEx(&aCtx->rsCtx, fwent->term, fwent->len, 1, &idxKey);

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
        writeIndexEntry(ACTX_SPEC(aCtx), invidx, encoder, fwent);
      }

      if (idxKey) {
        RedisModule_CloseKey(idxKey);
      }

      if (CONCURRENT_CTX_TICK(&indexer->concCtx) && ACTX_SPEC(aCtx) == NULL) {
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
static void writeCurEntries(DocumentIndexer *indexer, RSAddDocumentCtx *aCtx) {
  ForwardIndexIterator it = ForwardIndex_Iterate(aCtx->fwIdx);
  ForwardIndexEntry *entry = ForwardIndexIterator_Next(&it);
  IndexEncoder encoder = InvertedIndex_GetEncoder(aCtx->specFlags);
  RedisSearchCtx *ctx = &aCtx->rsCtx;

  while (entry != NULL) {
    RedisModuleKey *idxKey = NULL;

    IndexSpec_AddTerm(ctx->spec, entry->term, entry->len);
    InvertedIndex *invidx = Redis_OpenInvertedIndexEx(ctx, entry->term, entry->len, 1, &idxKey);
    if (invidx) {
      entry->docId = aCtx->doc.docId;
      assert(entry->docId);
      writeIndexEntry(ctx->spec, invidx, encoder, entry);
    }

    entry = ForwardIndexIterator_Next(&it);
    if (CONCURRENT_CTX_TICK(&indexer->concCtx) && ACTX_SPEC(aCtx) == NULL) {
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
    DocTable_Delete(table, keystr);
    --spec->stats.numDocuments;
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
static void doAssignIds(RSAddDocumentCtx *cur) {
  IndexSpec *spec = ACTX_SPEC(cur);
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
    }
  }
}

static void reopenCb(RedisModuleKey *k, void *arg) {
  // Index Key
  RSAddDocumentCtx *aCtx = arg;
  // we do not allow empty indexes when loading an existing index
  if (k == NULL || RedisModule_KeyType(k) == REDISMODULE_KEYTYPE_EMPTY ||
      RedisModule_ModuleTypeGetType(k) != IndexSpecType) {
    aCtx->rsCtx.spec = NULL;
    return;
  }

  aCtx->rsCtx.spec = RedisModule_ModuleTypeGetValue(k);
}

// Routines for the merged hash table
static const KHTableProcs mergedHtProcs = {
    .Alloc = mergedAlloc, .Compare = mergedCompare, .Hash = mergedHash};

/**
 * Perform the processing chain on a single document entry, optionally merging
 * the tokens of further entries in the queue
 */
static void DocumentIndexer_Process(DocumentIndexer *indexer, RSAddDocumentCtx *aCtx) {
  RedisSearchCtx *ctx = &aCtx->rsCtx;

  KHTable mergedEntries;
  BlkAlloc entriesAlloc;
  BlkAlloc_Init(&entriesAlloc);

  // No need to intialize this
  RSAddDocumentCtx *parentMap[MAX_DOCID_ENTRIES];

  if (aCtx->stateFlags & ACTX_F_ERRORED) {
    // Document has an error, no need for further processing
    return;
  }

  // If this document's tokens are merged or not
  const int needsFtIndex = !(aCtx->stateFlags & ACTX_F_MERGED);
  int useHt = 0;  // Whether we're using the merged hashtable

  // Only use merging if there is more than one item in the queue and the current
  // document is not already merged
  if (indexer->size > 1 && needsFtIndex) {
    useHt = 1;
    KHTable_Init(&mergedEntries, &mergedHtProcs, &entriesAlloc, aCtx->fwIdx->hits->numItems);
    doMerge(aCtx, &mergedEntries, parentMap);
  }

  ConcurrentSearch_SetKey(&indexer->concCtx, aCtx->rsCtx.redisCtx, ctx->keyName, aCtx);
  ConcurrentSearchCtx_ResetClock(&indexer->concCtx);
  ConcurrentSearchCtx_Lock(&indexer->concCtx);

  if (!ACTX_SPEC(aCtx)) {
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
    doAssignIds(aCtx);
    if (aCtx->stateFlags & ACTX_F_ERRORED) {
      if (useHt) {
        writeMergedEntries(indexer, aCtx, &mergedEntries, parentMap);
      }
      goto cleanup;
    }
  }

  if (needsFtIndex) {
    if (useHt) {
      writeMergedEntries(indexer, aCtx, &mergedEntries, parentMap);
    } else {
      writeCurEntries(indexer, aCtx);
    }
  }

  for (int i = 0; i < doc->numFields; i++) {
    const FieldSpec *fs = aCtx->fspecs + i;
    fieldData *fdata = aCtx->fdatas + i;
    if (fs->name == NULL) {
      // Means this document field does not have a corresponding spec
      continue;
    }

    IndexerFunc ifx = getIndexer(fs->type);
    if (ifx == NULL) {
      continue;
    }

    if (ifx(aCtx, &doc->fields[i], fs, fdata, &aCtx->errorString) != 0) {
      aCtx->stateFlags |= ACTX_F_ERRORED;
      goto cleanup;
    }
  }

cleanup:
  ConcurrentSearchCtx_Unlock(&indexer->concCtx);
  if (useHt) {
    BlkAlloc_FreeAll(&entriesAlloc, NULL, NULL, 0);
    KHTable_Free(&mergedEntries);
  }
}

static void *DocumentIndexer_Run(void *p) {
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
    DocumentIndexer_Process(indexer, cur);
    sendReply(cur);
  }
  return NULL;
}

static int DocumentIndexer_Add(DocumentIndexer *indexer, RSAddDocumentCtx *aCtx) {

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
  ConcurrentSearchCtx_Init(NULL, &indexer->concCtx);
  pthread_cond_init(&indexer->cond, NULL);
  pthread_mutex_init(&indexer->lock, NULL);
  static pthread_t dummyThr;
  pthread_create(&dummyThr, NULL, DocumentIndexer_Run, indexer);
  indexer->name = strdup(name);
  indexer->next = NULL;

  ConcurrentSearchCtx_InitEx(&indexer->concCtx, REDISMODULE_READ | REDISMODULE_WRITE, reopenCb);
  return indexer;
}

// Get the document indexer for the given index name. If the indexer does not
// exist, it is created and placed into the list of indexes
static DocumentIndexer *GetDocumentIndexer(const char *specname) {
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
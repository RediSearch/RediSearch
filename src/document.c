#include <string.h>
#include <assert.h>

#include "document.h"
#include "forward_index.h"
#include "numeric_filter.h"
#include "numeric_index.h"
#include "rmutil/strings.h"
#include "rmutil/util.h"
#include "util/mempool.h"
#include "spec.h"
#include "tokenize.h"
#include "util/logging.h"
#include "search_request.h"
#include "rmalloc.h"
#include "indexer.h"

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
// Memory pool for RSAddDocumentContext contexts
static mempool_t *actxPool_g = NULL;

// For documentation, see these functions' definitions
static void *allocDocumentContext(void) {
  // See if there's one in the pool?
  RSAddDocumentCtx *aCtx = calloc(1, sizeof(*aCtx));
  return aCtx;
}

static void freeDocumentContext(void *p) {
  RSAddDocumentCtx *aCtx = p;
  if (aCtx->fwIdx) {
    ForwardIndexFree(aCtx->fwIdx);
  }

  free(aCtx->fspecs);
  free(aCtx->fdatas);
  free(aCtx);
}

RSAddDocumentCtx *NewAddDocumentCtx(IndexSpec *sp, Document *base) {

  if (!actxPool_g) {
    actxPool_g = mempool_new(16, allocDocumentContext, freeDocumentContext);
  }

  // Get a new context
  RSAddDocumentCtx *aCtx = mempool_get(actxPool_g);

  // Per-client fields; these must always be recreated
  aCtx->bc = NULL;

  size_t oldFieldCount = aCtx->doc.numFields;
  aCtx->doc = *base;

  aCtx->next = NULL;

  if (aCtx->fwIdx) {
    ForwardIndex_Reset(aCtx->fwIdx, &aCtx->doc, sp->flags);
    aCtx->stateFlags = 0;
    aCtx->errorString = NULL;
    aCtx->totalTokens = 0;
  } else {
    aCtx->fwIdx = NewForwardIndex(&aCtx->doc, sp->flags);
  }

  aCtx->specFlags = sp->flags;
  aCtx->stopwords = sp->stopwords;
  aCtx->indexer = GetDocumentIndexer(sp->name);

  StopWordList_Ref(sp->stopwords);

  // Also, get the field specs. We cache this here because the
  // context is unlocked
  // during the actual tokenization
  Document *doc = &aCtx->doc;

  // We might be able to use the old block of fields. But, if it's too small,
  // just free and call malloc again; that's basically what realloc does
  // anyway.
  if (oldFieldCount != 0 && oldFieldCount < doc->numFields) {
    free(aCtx->fspecs);
    free(aCtx->fdatas);
    oldFieldCount = 0;
  }

  if (oldFieldCount == 0) {
    aCtx->fspecs = malloc(sizeof(*aCtx->fspecs) * doc->numFields);
    aCtx->fdatas = malloc(sizeof(*aCtx->fdatas) * doc->numFields);
  }

  for (int i = 0; i < doc->numFields; i++) {
    const DocumentField *f = doc->fields + i;
    FieldSpec *fs = IndexSpec_GetField(sp, f->name, strlen(f->name));
    if (fs) {
      aCtx->fspecs[i] = *fs;
      if (FieldSpec_IsSortable(fs) && aCtx->sv == NULL) {
        aCtx->sv = NewSortingVector(sp->sortables->len);
      }
      if (fs->type != F_FULLTEXT) {
        aCtx->stateFlags |= ACTX_F_NONTXTFLDS;
      }
    } else {
      aCtx->fspecs[i].name = NULL;
    }
  }
  aCtx->doc.docId = 0;
  return aCtx;
}

static int replyCallback(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  RSAddDocumentCtx *aCtx = RedisModule_GetBlockedClientPrivateData(ctx);
  if (aCtx->errorString) {
    RedisModule_ReplyWithError(ctx, aCtx->errorString);
  } else {
    RedisModule_ReplyWithSimpleString(ctx, "OK");
  }
  AddDocumentCtx_Free(aCtx);
  return REDISMODULE_OK;
}

static void threadCallback(void *p) {
  Document_AddToIndexes(p);
}

void AddDocumentCtx_Finish(RSAddDocumentCtx *aCtx) {
  RedisModule_UnblockClient(aCtx->bc, aCtx);
}

// How many bytes in a document to warrant it being tokenized in a separate thread
#define SELF_EXEC_THRESHOLD 1024

void AddDocumentCtx_Submit(RSAddDocumentCtx *aCtx, RedisModuleCtx *ctx, uint32_t options) {
  aCtx->options = options;
  aCtx->bc = RedisModule_BlockClient(ctx, replyCallback, NULL, NULL, 0);
  assert(aCtx->bc);
  size_t totalSize = 0;
  for (size_t ii = 0; ii < aCtx->doc.numFields; ++ii) {
    const FieldSpec *fs = aCtx->fspecs + ii;
    if (fs->name && fs->type == F_FULLTEXT) {
      size_t n;
      RedisModule_StringPtrLen(aCtx->doc.fields[ii].text, &n);
      totalSize += n;
    }
  }

  if (totalSize >= SELF_EXEC_THRESHOLD) {
    ConcurrentSearch_ThreadPoolRun(threadCallback, aCtx, CONCURRENT_POOL_INDEX);
  } else {
    Document_AddToIndexes(aCtx);
  }
}

void AddDocumentCtx_Free(RSAddDocumentCtx *aCtx) {
  // Destroy the common fields:
  Document_FreeDetached(&aCtx->doc, aCtx->indexer->redisCtx);

  if (aCtx->sv) {
    SortingVector_Free(aCtx->sv);
    aCtx->sv = NULL;
  }

  if (aCtx->stopwords) {
    StopWordList_Unref(aCtx->stopwords);
    aCtx->stopwords = NULL;
  }

  mempool_release(actxPool_g, aCtx);
}

#define FIELD_HANDLER(name)                                                                \
  static int name(RSAddDocumentCtx *aCtx, const DocumentField *field, const FieldSpec *fs, \
                  fieldData *fdata, const char **errorString)

#define FIELD_INDEXER(name)                                                                \
  static int name(RSAddDocumentCtx *aCtx, RedisSearchCtx *ctx, const DocumentField *field, \
                  const FieldSpec *fs, fieldData *fdata, const char **errorString)

#define FIELD_PREPROCESSOR FIELD_HANDLER

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
  RedisModuleKey *idxKey;
  NumericRangeTree *rt = OpenNumericIndex(ctx, fs->name, &idxKey);
  NumericRangeTree_Add(rt, aCtx->doc.docId, fdata->numeric);
  RedisModule_CloseKey(idxKey);
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
  GeoIndex gi = {.ctx = ctx, .sp = fs};
  int rv = GeoIndex_AddStrings(&gi, aCtx->doc.docId, fdata->geo.slon, fdata->geo.slat);

  if (rv == REDISMODULE_ERR) {
    *errorString = "Could not index geo value";
    return -1;
  }
  return 0;
}

PreprocessorFunc GetIndexPreprocessor(const FieldType ft) {
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

IndexerFunc GetIndexIndexer(const FieldType ft) {
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

int Document_AddToIndexes(RSAddDocumentCtx *aCtx) {
  Document *doc = &aCtx->doc;
  int ourRv = REDISMODULE_OK;

  for (int i = 0; i < doc->numFields; i++) {
    const FieldSpec *fs = aCtx->fspecs + i;
    fieldData *fdata = aCtx->fdatas + i;
    if (fs->name == NULL) {
      LG_DEBUG("Skipping field %s not in index!", doc->fields[i].name);
      continue;
    }

    // Get handler
    PreprocessorFunc pp = GetIndexPreprocessor(fs->type);
    if (pp == NULL) {
      continue;
    }

    if (pp(aCtx, &doc->fields[i], fs, fdata, &aCtx->errorString) != 0) {
      ourRv = REDISMODULE_ERR;
      goto cleanup;
    }
  }

  if (Indexer_Add(aCtx->indexer, aCtx) != 0) {
    ourRv = REDISMODULE_ERR;
    goto cleanup;
  }

cleanup:
  if (ourRv != REDISMODULE_OK) {
    if (aCtx->errorString == NULL) {
      aCtx->errorString = "ERR couldn't index document";
    }
    AddDocumentCtx_Finish(aCtx);
  }
  return ourRv;
}
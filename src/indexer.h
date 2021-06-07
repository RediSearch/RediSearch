#ifndef INDEXER_H
#define INDEXER_H

#include "document.h"
#include "util/khtable.h"
#include "util/block_alloc.h"
#include "concurrent_ctx.h"
#include "util/arr.h"
// Preprocessors can store field data to this location
typedef struct FieldIndexerData {
  double numeric;  // i.e. the numeric value of the field
  const char *geoSlon;
  const char *geoSlat;
  char **tags;
  const void *vector;
  size_t vecLen;
} FieldIndexerData;

typedef struct DocumentIndexer {
  RSAddDocumentCtx *head;          // first item in the queue
  RSAddDocumentCtx *tail;          // last item in the queue
  pthread_mutex_t lock;            // lock - only used when adding or removing items from the queue
  pthread_cond_t cond;             // condition - used to wait on items added to the queue
  size_t size;                     // number of items in the queue
  ConcurrentSearchCtx concCtx;     // GIL locking. This is repopulated with the relevant key data
  RedisModuleCtx *redisCtx;        // Context for keeping the spec key
  RedisModuleString *specKeyName;  // Cached, used for opening/closing the spec key.
  uint64_t specId;                 // Unique spec ID. Used to verify we haven't been replaced
  int isDbSelected;
  struct DocumentIndexer *next;  // Next structure in the indexer list
  KHTable mergeHt;               // Hashtable and block allocator for merging
  BlkAlloc alloc;
  int options;
  pthread_t thr;
  size_t refcount;
} DocumentIndexer;

#define INDEXER_THREADLESS 0x01

// Set when the indexer is about to be deleted
#define INDEXER_STOPPED 0x02

size_t Indexer_Decref(DocumentIndexer *indexer);

size_t Indexer_Incref(DocumentIndexer *indexer);

void Indexer_Free(DocumentIndexer *indexer);
DocumentIndexer *NewIndexer(IndexSpec *spec);

/**
 * Add a document to the indexing queue. If successful, the indexer now takes
 * ownership of the document context (until it DocumentAddCtx_Finish).
 */
int Indexer_Add(DocumentIndexer *indexer, RSAddDocumentCtx *aCtx);

/**
 * Function to preprocess field data. This should do as much stateless processing
 * as possible on the field - this means things like input validation and normalization.
 *
 * The `fdata` field is used to contain the result of the processing, which is then
 * actually written to the index at a later point in time.
 *
 * This function is called with the GIL released.
 */
typedef int (*PreprocessorFunc)(RSAddDocumentCtx *aCtx, const DocumentField *field,
                                const FieldSpec *fs, FieldIndexerData *fdata, QueryError *status);

/**
 * Function to write the entry for the field into the actual index. This is called
 * with the GIL locked, and it should therefore only write data, and nothing more.
 */
typedef int (*IndexerFunc)(RSAddDocumentCtx *aCtx, RedisSearchCtx *ctx, const DocumentField *field,
                           const FieldSpec *fs, FieldIndexerData *fdata, QueryError *status);

typedef struct {
  RedisModuleKey *indexKeys[INDEXFLD_NUM_TYPES];
  void *indexDatas[INDEXFLD_NUM_TYPES];
  FieldType typemask;
  int found;
} IndexBulkData;

// IndexerBulkAdd(bulk, cur, sctx, doc->fields + ii, fs, fdata, &cur->status);

int IndexerBulkAdd(IndexBulkData *bulk, RSAddDocumentCtx *cur, RedisSearchCtx *sctx,
                   const DocumentField *field, const FieldSpec *fs, FieldIndexerData *fdata,
                   QueryError *status);
void IndexerBulkCleanup(IndexBulkData *cur, RedisSearchCtx *sctx);

#endif

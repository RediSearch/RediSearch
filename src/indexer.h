#ifndef INDEXER_H
#define INDEXER_H

#include "document.h"
#include "util/khtable.h"
#include "util/block_alloc.h"
#include "concurrent_ctx.h"

// Preprocessors can store field data to this location
typedef union FieldData {
  double numeric;  // i.e. the numeric value of the field
  struct {
    char *slon;
    char *slat;
  } geo;  // lon/lat pair
  Vector *tags;
} fieldData;

typedef struct DocumentIndexer {
  RSAddDocumentCtx *head;          // first item in the queue
  RSAddDocumentCtx *tail;          // last item in the queue
  pthread_mutex_t lock;            // lock - only used when adding or removing items from the queue
  pthread_cond_t cond;             // condition - used to wait on items added to the queue
  size_t size;                     // number of items in the queue
  ConcurrentSearchCtx concCtx;     // GIL locking. This is repopulated with the relevant key data
  RedisModuleCtx *redisCtx;        // Context for keeping the spec key
  RedisModuleString *specKeyName;  // Cached, used for opening/closing the spec key.
  int isDbSelected;

  char *name;  // The name of the index this structure belongs to. For use with the list of indexers
  struct DocumentIndexer *next;  // Next structure in the indexer list
  KHTable mergeHt;               // Hashtable and block allocator for merging
  BlkAlloc alloc;
} DocumentIndexer;

/**
 * Get the indexing thread for the given spec `specname`. If no such thread is
 * running, a new one will be instantiated.
 */
DocumentIndexer *GetDocumentIndexer(const char *specname);

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
                                const FieldSpec *fs, fieldData *fdata, const char **errorString);

/**
 * Function to write the entry for the field into the actual index. This is called
 * with the GIL locked, and it should therefore only write data, and nothing more.
 */
typedef int (*IndexerFunc)(RSAddDocumentCtx *aCtx, RedisSearchCtx *ctx, const DocumentField *field,
                           const FieldSpec *fs, fieldData *fdata, const char **errorString);

/**
 * Get the preprocessor function for a given index type
 */
PreprocessorFunc GetIndexPreprocessor(const FieldType ft);

/**
 * Get the indexer function for a given index type.
 */
IndexerFunc GetIndexIndexer(const FieldType ft);

#endif
#ifndef INDEXER_H
#define INDEXER_H

#include "document.h"
#include "util/khtable.h"
#include "util/block_alloc.h"
#include "concurrent_ctx.h"
#include "util/arr.h"
#include "inverted_index.h"
// Preprocessors can store field data to this location
typedef struct FieldIndexerData {
  double numeric;  // i.e. the numeric value of the field
  const char *geoSlon;
  const char *geoSlat;
  char **tags;
} FieldIndexerData;

typedef struct Indexer {
  RSAddDocumentCtx **docs;
  RSAddDocumentCtx **errs;  // documents with errors in them
  RedisSearchCtx *sctx;
  KHTable mergeHt;  // Hashtable and block allocator for merging
  BlkAlloc alloc;
} Indexer;

void Indexer_Init(Indexer *bi, RedisSearchCtx *sctx);
int Indexer_Add(Indexer *bi, RSAddDocumentCtx *ctx);

typedef void (*IndexerCallback)(RSAddDocumentCtx *, void *privdata);
void Indexer_Index(Indexer *bi, IndexerCallback cb, void *data);
void Indexer_Reset(Indexer *bi);
void Indexer_Destroy(Indexer *bi);
void Indexer_Iterate(Indexer *bi, IndexerCallback cb, void *data);

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

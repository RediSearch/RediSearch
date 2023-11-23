/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef INDEXER_H
#define INDEXER_H

#include "document.h"
#include "util/khtable.h"
#include "util/block_alloc.h"
#include "concurrent_ctx.h"
#include "util/arr.h"
#include "geometry_index.h"
// Preprocessors can store field data to this location
typedef struct FieldIndexerData {
  int isMulti;
  int isNull;
  struct {
    // This is a struct and not a union since when FieldSpec options is `FieldSpec_Dynamic`:
    // it can store data as several types, e.g., as numeric and as tag)

    // Single value
    double numeric;  // i.e. the numeric value of the field
    arrayof(char*) tags;
    struct {
      const void *vector;
      size_t vecLen;
      size_t numVec;
    };

    // Multi value
    arrayof(double) arrNumeric;

    struct {
      const char *str;
      size_t strlen;
      GEOMETRY_FORMAT format;
    };
    // struct {
    //   arrayof(GEOMETRY) arrGeometry;
    // };
  };

} FieldIndexerData;

typedef struct DocumentIndexer {
  ConcurrentSearchCtx concCtx;     // GIL locking. This is repopulated with the relevant key data
  RedisModuleCtx *redisCtx;        // Context for keeping the spec key
  RedisModuleString *specKeyName;  // Cached, used for opening/closing the spec key.
  uint64_t specId;                 // Unique spec ID. Used to verify we haven't been replaced
  int isDbSelected;
  KHTable mergeHt;               // Hashtable and block allocator for merging
  BlkAlloc alloc;
} DocumentIndexer;

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
typedef int (*PreprocessorFunc)(RSAddDocumentCtx *aCtx, RedisSearchCtx *sctx, DocumentField *field,
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

int IndexerBulkAdd(IndexBulkData *bulk, RSAddDocumentCtx *cur, RedisSearchCtx *sctx,
                   const DocumentField *field, const FieldSpec *fs, FieldIndexerData *fdata,
                   QueryError *status);
void IndexerBulkCleanup(IndexBulkData *cur, RedisSearchCtx *sctx);

#endif

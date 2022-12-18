
#pragma once

#include "document.h"
#include "tag_index.h"
#include "util/khtable.h"
#include "util/block_alloc.h"
#include "concurrent_ctx.h"
#include "util/arr.h"

///////////////////////////////////////////////////////////////////////////////////////////////

#define INDEXER_THREADLESS 0x01

// Set when the indexer is about to be deleted
#define INDEXER_STOPPED 0x02

// Preprocessors can store field data to this location

struct FieldIndexerData {
  double numeric;  // i.e. the numeric value of the field
  const char *geoSlon;
  const char *geoSlat;
  TagIndex::Tags tags;
};

//---------------------------------------------------------------------------------------------

struct MergeMapEntry {
  ForwardIndexEntry *head;  // First document containing the term
  ForwardIndexEntry *tail;  // Last document containing the term

  size_t countMerged() const;
};

typedef UnorderedMap<std::string, MergeMapEntry*> MergeMap;

//---------------------------------------------------------------------------------------------

struct DocumentIndexerConcurrentKey : ConcurrentKey {
  RedisSearchCtx sctx;

  DocumentIndexerConcurrentKey(RedisModuleString *keyName, RedisSearchCtx sctx, RedisModuleKey *key = NULL) :
    ConcurrentKey(key, keyName, REDISMODULE_READ | REDISMODULE_WRITE), sctx(sctx) {}

  void Reopen() override {
    // we do not allow empty indexes when loading an existing index
    if (key == NULL || RedisModule_KeyType(key) == REDISMODULE_KEYTYPE_EMPTY ||
        RedisModule_ModuleTypeGetType(key) != IndexSpecType) {
      sctx.spec = NULL;
      return;
    }

    sctx.spec = static_cast<IndexSpec *>(RedisModule_ModuleTypeGetValue(key));
    if (sctx.spec->uniqueId != sctx.specId) {
      sctx.spec = NULL;
    }
  }
};

//---------------------------------------------------------------------------------------------

struct DocumentIndexer : Object {
  List<AddDocumentCtx*> addQueue;

  pthread_t thr;
  pthread_mutex_t lock;            // lock - only used when adding or removing items from the queue
  pthread_cond_t cond;             // condition - used to wait on items added to the queue

  ConcurrentSearch concCtx;        // GIL locking. This is repopulated with the relevant key data
  RedisModuleCtx *redisCtx;        // Context for keeping the spec key
  RedisModuleString *specKeyName;  // Cached, used for opening/closing the spec key
  IndexSpecId specId;              // Unique spec ID. Used to verify we haven't been replaced

  //struct DocumentIndexer *next;    // Next structure in the indexer list

  BlkAlloc<MergeMapEntry> mergePool;
  MergeMap mergeMap;               // Hashtable and block allocator for merging

  unsigned int options;
  bool isDbSelected;

  DocumentIndexer(IndexSpec &spec);
  ~DocumentIndexer();

  void Add(AddDocumentCtx *aCtx);

  void Process(AddDocumentCtx *aCtx);
  AddDocumentCtx *merge(AddDocumentCtx *actx, AddDocumentCtx **parentMap);

  void main();
  static void *_main(void *self);

  void Stop();
  bool ShouldStop() const { return options & INDEXER_STOPPED; }

  void writeEntries(AddDocumentCtx *aCtx, RedisSearchCtx *ctx);
  int writeMergedEntries(AddDocumentCtx *aCtx, RedisSearchCtx *ctx, AddDocumentCtx **parentMap);
};

//---------------------------------------------------------------------------------------------

/**
 * Function to preprocess field data. This should do as much stateless processing
 * as possible on the field - this means things like input validation and normalization.
 *
 * The `fdata` field is used to contain the result of the processing, which is then
 * actually written to the index at a later point in time.
 *
 * This function is called with the GIL released.
 */
typedef int (*PreprocessorFunc)(AddDocumentCtx *aCtx, const DocumentField *field,
                                const FieldSpec *fs, FieldIndexerData *fdata, QueryError *status);

/**
 * Function to write the entry for the field into the actual index. This is called
 * with the GIL locked, and it should therefore only write data, and nothing more.
 */
typedef int (*IndexerFunc)(AddDocumentCtx *aCtx, RedisSearchCtx *ctx, const DocumentField *field,
                           const FieldSpec *fs, FieldIndexerData *fdata, QueryError *status);

//---------------------------------------------------------------------------------------------

struct IndexBulkData {
  RedisModuleKey *indexKeys[INDEXFLD_NUM_TYPES];
  void *indexDatas[INDEXFLD_NUM_TYPES];
  FieldType typemask;
  int found;

  bool Add(AddDocumentCtx *cur, RedisSearchCtx *sctx, const DocumentField &field,
           const FieldSpec *fs, FieldIndexerData *fdata, QueryError *status);

  void Cleanup(RedisSearchCtx *sctx);

  static void indexBulkFields(AddDocumentCtx *aCtx, RedisSearchCtx *sctx);

  // private:
  bool numericIndexer(AddDocumentCtx *aCtx, RedisSearchCtx *ctx, const DocumentField &field,
    const FieldSpec *fs, FieldIndexerData *fdata, QueryError *status);

  bool geoIndexer(AddDocumentCtx *aCtx, RedisSearchCtx *ctx, const DocumentField &field,
    const FieldSpec *fs, FieldIndexerData *fdata, QueryError *status);

  bool tagIndexer(AddDocumentCtx *aCtx, RedisSearchCtx *ctx, const DocumentField &field,
    const FieldSpec *fs, FieldIndexerData *fdata, QueryError *status);
};

///////////////////////////////////////////////////////////////////////////////////////////////

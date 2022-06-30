
#pragma once

#include "document.h"
#include "tag_index.h"
#include "util/khtable.h"
#include "util/block_alloc.h"
#include "concurrent_ctx.h"
#include "util/arr.h"

///////////////////////////////////////////////////////////////////////////////////////////////

// Preprocessors can store field data to this location

struct FieldIndexerData {
  double numeric;  // i.e. the numeric value of the field
  const char *geoSlon;
  const char *geoSlat;
  TagIndex::Tags tags;
};

//---------------------------------------------------------------------------------------------

struct MergeHashTable : KHTable {
  virtual int Compare(const KHTableEntry *ent, const void *s, size_t n, uint32_t h);
  virtual uint32_t Hash(const KHTableEntry *ent);
  virtual KHTableEntry *Alloc(void *ctx);
};

//---------------------------------------------------------------------------------------------

struct DocumentIndexer : public Object {
  AddDocumentCtx *head;            // first item in the queue
  AddDocumentCtx *tail;            // last item in the queue
  pthread_mutex_t lock;            // lock - only used when adding or removing items from the queue
  pthread_cond_t cond;             // condition - used to wait on items added to the queue
  size_t size;                     // number of items in the queue
  ConcurrentSearchCtx concCtx;     // GIL locking. This is repopulated with the relevant key data
  RedisModuleCtx *redisCtx;        // Context for keeping the spec key
  RedisModuleString *specKeyName;  // Cached, used for opening/closing the spec key.
  uint64_t specId;                 // Unique spec ID. Used to verify we haven't been replaced
  bool isDbSelected;
  struct DocumentIndexer *next;    // Next structure in the indexer list
  BlkAlloc alloc;
  MergeHashTable mergeHt;          // Hashtable and block allocator for merging
  int options;
  pthread_t thr;
  size_t refcount;

  DocumentIndexer(IndexSpec *spec);
  ~DocumentIndexer();

  static DocumentIndexer *Copy(DocumentIndexer *indexer);
  void Free();
  void Process(AddDocumentCtx *aCtx);

  int Add(AddDocumentCtx *aCtx);

  void main();
  static void *_main(void *self);

  bool ShouldStop() const { return options & INDEXER_STOPPED; }

  int writeMergedEntries(AddDocumentCtx *aCtx, RedisSearchCtx *ctx, KHTable *ht, AddDocumentCtx **parentMap);
  void writeCurEntries(AddDocumentCtx *aCtx, RedisSearchCtx *ctx);

// private:
  size_t Decref();
  size_t Incref();
};

//---------------------------------------------------------------------------------------------

#define INDEXER_THREADLESS 0x01

// Set when the indexer is about to be deleted
#define INDEXER_STOPPED 0x02

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

  int Add(AddDocumentCtx *cur, RedisSearchCtx *sctx, const DocumentField *field,
          const FieldSpec *fs, FieldIndexerData *fdata, QueryError *status);

  void Cleanup(RedisSearchCtx *sctx);

  static void indexBulkFields(AddDocumentCtx *aCtx, RedisSearchCtx *sctx);
};

///////////////////////////////////////////////////////////////////////////////////////////////

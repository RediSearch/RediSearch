
#pragma once

#include "search_ctx.h"
#include "spec.h"
#include "redisearch.h"
#include "tokenize.h"
#include "concurrent_ctx.h"
#include "byte_offsets.h"
#include "query_error.h"

#include "redismodule.h"
#include "rmutil/args.h"

#include <pthread.h>

///////////////////////////////////////////////////////////////////////////////////////////////

/**
 * General Architecture
 * --------------------
 * To index a document, call Document_PrepareForAdd on the document itself.
 * This initializes the Document structure for indexing purposes. Once the
 * document has been prepared, acquire a new RSAddDocumentCtx() by calling
 * NewAddDocumentCtx().
 *
 * Once the new context has been received, call Document_AddToIndexes(). This
 * will start tokenizing the documents, and should be called in a separate
 * thread. This function will tokenize the document and send a reply back to
 * the client. You may free the RSAddDocumentCtx structure by calling
 * AddDocumentCtx_Free().
 *
 * See document.c for the internals.
 */

//---------------------------------------------------------------------------------------------

struct DocumentField {
  const char *name;  // Can either be char or RMString
  RedisModuleString *text;
  FieldType indexAs;
};

//---------------------------------------------------------------------------------------------

struct Document {
  RedisModuleString *docKey;
  DocumentField *fields;
  uint32_t numFields;
  RSLanguage language;
  float score;
  t_docId docId;
  const char *payload;
  size_t payloadSize;
  uint32_t flags;

  Document(RedisModuleString *docKey, double score, RSLanguage lang);
  ~Document();

  int ReplyFields(RedisModuleCtx *ctx);
  DocumentField *GetField(const char *fieldName);

  void AddField(const char *fieldname, RedisModuleString *fieldval, uint32_t typemask);
  void AddFieldC(const char *fieldname, const char *val, size_t vallen, uint32_t typemask);

  void SetPayload(const void *payload, size_t n);
  void MakeStringsOwner();
  void MakeRefOwner();
  void Clear();

  static void Move(Document *dst, Document *src);

  int LoadSchemaFields(RedisSearchCtx *sctx);
  int LoadAllFields(RedisModuleCtx *ctx);
  void LoadPairwiseArgs(RedisModuleString **args, size_t nargs);
};

//---------------------------------------------------------------------------------------------

/**
 * Document should decrement the reference count to the contained strings. Used
 * when the user does not want to retain his own reference to them. It effectively
 * "steals" a reference.
 *
 * This only applies to _values_; not keys. Used internally by the C API
 */
#define DOCUMENT_F_OWNREFS 0x01

/**
 * Indicates that the document owns a reference to the field contents,
 * the language string, and the payload.
 *
 * The document always owns the field array, though.
 */
#define DOCUMENT_F_OWNSTRINGS 0x02

/**
 * The document has been moved to another target. This is quicker than
 * zero'ing the entire structure
 */
#define DOCUMENT_F_DEAD 0x08

//---------------------------------------------------------------------------------------------

struct RSAddDocumentCtx;

typedef void (*DocumentAddCompleted)(struct RSAddDocumentCtx *, RedisModuleCtx *, void *);

struct AddDocumentOptions {
  uint32_t options;                 // DOCUMENT_ADD_XXX
  RSLanguage language;              // Language document should be indexed as
  RedisModuleString *payload;       // Arbitrary payload provided on return with WITHPAYLOADS
  RedisModuleString **fieldsArray;  // Field, Value, Field Value
  size_t numFieldElems;             // Number of elements
  double score;                     // Score of the document
  const char *evalExpr;             // Only add the document if this expression evaluates to true.
  DocumentAddCompleted donecb;      // Callback to invoke when operation is done
};

//---------------------------------------------------------------------------------------------

#define DOCUMENT_ADD_REPLACE 0x01
#define DOCUMENT_ADD_PARTIAL 0x02
#define DOCUMENT_ADD_NOSAVE 0x04
#define DOCUMENT_ADD_CURTHREAD 0x08  // Perform operation in main thread
#define DOCUMENT_ADD_NOCREATE 0x10   // Don't create document if not exist (replace ONLY)

struct ForwardIndex;
struct FieldIndexerData;

//---------------------------------------------------------------------------------------------

// The context has had its forward entries merged in the merge table. We can
// skip merging its tokens
#define ACTX_F_TEXTINDEXED 0x01

// The context has had an error and should not be processed further
#define ACTX_F_ERRORED 0x02

// Non-text fields have been indexed.
#define ACTX_F_OTHERINDEXED 0x04

// The content has indexable fields
#define ACTX_F_INDEXABLES 0x08

// The content has sortable fields
#define ACTX_F_SORTABLES 0x10

// Don't block/unblock the client when indexing. This is the case when the
// operation is being done from within the context of AOF
#define ACTX_F_NOBLOCK 0x20

// Document is entirely empty (no sortables, indexables)
#define ACTX_F_EMPTY 0x40

//---------------------------------------------------------------------------------------------

struct DocumentIndexer;

// Context used when indexing documents

struct RSAddDocumentCtx {
  struct RSAddDocumentCtx *next;  // Next context in the queue
  Document doc;                   // Document which is being indexed

  union {
    RedisModuleBlockedClient *bc;  // Client
    RedisSearchCtx *sctx;
  } client;

  // Forward index. This contains all the terms found in the document
  struct ForwardIndex *fwIdx;

  struct DocumentIndexer *indexer;

  // Sorting vector for the document. If the document has sortable fields, they
  // are added to here as well
  RSSortingVector *sv;

  // Byte offsets for highlighting. If term offsets are stored, this contains
  // the field byte offset for each term.
  RSByteOffsets *byteOffsets;
  ByteOffsetWriter offsetsWriter;

  // Information about each field in the document. This is read from the spec
  // and cached, so that we can look it up without holding the GIL
  FieldSpec *fspecs;
  Tokenizer *tokenizer;

  // Old document data. Contains sortables
  RSDocumentMetadata *oldMd;

  // New flags to assign to the document
  RSDocumentFlags docFlags;

  // Scratch space used by per-type field preprocessors (see the source)
  struct FieldIndexerData *fdatas;
  QueryError status;     // Error message is placed here if there is an error during processing
  uint32_t totalTokens;  // Number of tokens, used for offset vector
  uint32_t specFlags;    // Cached index flags
  uint8_t options;       // Indexing options - i.e. DOCUMENT_ADD_xxx
  uint8_t stateFlags;    // Indexing state, ACTX_F_xxx
  DocumentAddCompleted donecb;
  void *donecbData;

  RSAddDocumentCtx(IndexSpec *sp, Document *base, QueryError *status);
  ~RSAddDocumentCtx();

  void Submit(RedisSearchCtx *sctx, uint32_t options);

  void Finish();
  int AddToIndexes();

  bool IsBlockable() const { return !(stateFlags & ACTX_F_NOBLOCK); }
};

//---------------------------------------------------------------------------------------------

int Document_EvalExpression(RedisSearchCtx *sctx, RedisModuleString *key, const char *expr,
                            int *result, QueryError *err);

//---------------------------------------------------------------------------------------------

// Don't create document if it does not exist. Replace only
#define REDIS_SAVEDOC_NOCREATE 0x01

int Redis_SaveDocument(RedisSearchCtx *ctx, Document *doc, int options, QueryError *status);

//---------------------------------------------------------------------------------------------

// Document add functions:

int RSAddDocumentCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int RSSafeAddDocumentCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int RSAddHashCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int RSSafeAddHashCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

int RS_AddDocument(RedisSearchCtx *sctx, RedisModuleString *name, const AddDocumentOptions *opts,
                   QueryError *status);

///////////////////////////////////////////////////////////////////////////////////////////////

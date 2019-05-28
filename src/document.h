#ifndef __RS_DOCUMENT_H__
#define __RS_DOCUMENT_H__
#include <pthread.h>
#include "redismodule.h"
#include "search_ctx.h"
#include "redisearch.h"
#include "tokenize.h"
#include "concurrent_ctx.h"
#include "byte_offsets.h"
#include "rmutil/args.h"
#include "query_error.h"

#ifdef __cplusplus
extern "C" {
#endif

////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////
/// General Architecture                                                     ///
////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////////////////////////////////////////

/**
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

typedef struct {
  const char *name;
  RedisModuleString *text;
  FieldType indexAs;
} DocumentField;

typedef struct Document {
  RedisModuleString *docKey;
  DocumentField *fields;
  uint32_t numFields;
  float score;
  const char *language;
  t_docId docId;
  const char *payload;
  size_t payloadSize;
  uint32_t flags;
} Document;

// The document is "static" -- don't free the document's contents
#define DOCUMENT_F_STATIC 0x01

struct RSAddDocumentCtx;

typedef void (*DocumentAddCompleted)(struct RSAddDocumentCtx *, RedisModuleCtx *, void *);

typedef struct {
  uint32_t options;                 // DOCUMENT_ADD_XXX
  const char *language;             // Language document should be indexed as
  RedisModuleString *payload;       // Arbitrary payload provided on return with WITHPAYLOADS
  RedisModuleString **fieldsArray;  // Field, Value, Field Value
  size_t numFieldElems;             // Number of elements
  double score;                     // Score of the document
  const char *evalExpr;             // Only add the document if this expression evaluates to true.
  DocumentAddCompleted donecb;      // Callback to invoke when operation is done

} AddDocumentOptions;

int AddDocumentOptions_Parse(AddDocumentOptions *opts, ArgsCursor *ac, QueryError *status);

void Document_AddField(Document *d, const char *fieldname, RedisModuleString *fieldval,
                       uint32_t typemask);

/**
 * Initialize document structure with the relevant fields. numFields will allocate
 * the fields array, but you must still actually copy the data along.
 *
 * Note that this function assumes that the pointers passed in will remain valid
 * throughout the lifetime of the document. If you need to make independent copies
 * of the data within the document, call Document_Detach on the document (after
 * calling this function).
 */
void Document_Init(Document *doc, RedisModuleString *docKey, double score, int numFields,
                   const char *lang, const char *payload, size_t payloadSize);

/**
 * Prepare a document for being added to NewAddDocumentCtx.
 * This calls Document_Init and Document_Detach, and aims to eliminate
 * common boilerplate when parsing arguments
 *
 * doc: The document to initialize
 * docKey: The string ID of the document
 * score: Document store
 * options: fields and other input parameters for the operation
 * ctx: Owning context, used for Detach()
 *
 */
void Document_PrepareForAdd(Document *doc, RedisModuleString *docKey, double score,
                            const AddDocumentOptions *opts, RedisModuleCtx *ctx);

/**
 * Copy any data from the document into its own independent copies. srcCtx is
 * the context owning any RedisModuleString items - which are assigned using
 * RedisModule_RetainString.
 *
 * If the document contains fields, the field data is also retained.
 */
void Document_Detach(Document *doc, RedisModuleCtx *srcCtx);

/**
 * These two functions are used to manipulate the internal field data within a
 * document _without_ additional allocations. ClearDetachedFields() will
 * clear the document's field count to 0, whereas DetachFields will detach
 * the newly loaded field data (via Redis_LoadDocument).
 */
void Document_ClearDetachedFields(Document *doc, RedisModuleCtx *anyCtx);
void Document_DetachFields(Document *doc, RedisModuleCtx *ctx);

/**
 * Print contents of document to screen
 */
void Document_Dump(const Document *doc);
/**
 * Free any copied data within the document. anyCtx is any non-NULL
 * RedisModuleCtx. The reason for requiring a context is more related to the
 * Redis Module API requiring a context for AutoMemory purposes, though in
 * this case, the pointers are already removed from AutoMemory manangement
 * anyway.
 *
 * This function also calls Document_Free
 */
void Document_FreeDetached(Document *doc, RedisModuleCtx *anyCtx);

/**
 * Free the document's internals (like the field array).
 */
void Document_Free(Document *doc);

#define DOCUMENT_ADD_REPLACE 0x01
#define DOCUMENT_ADD_PARTIAL 0x02
#define DOCUMENT_ADD_NOSAVE 0x04
#define DOCUMENT_ADD_CURTHREAD 0x08  // Perform operation in main thread
#define DOCUMENT_ADD_NOCREATE 0x10   // Don't create document if not exist (replace ONLY)

struct ForwardIndex;
struct FieldIndexerData;

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

struct DocumentIndexer;

/** Context used when indexing documents */
typedef struct RSAddDocumentCtx {
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
  RSTokenizer *tokenizer;

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
} RSAddDocumentCtx;

#define AddDocumentCtx_IsBlockable(aCtx) (!((aCtx)->stateFlags & ACTX_F_NOBLOCK))

/**
 * Creates a new context used for adding documents. Once created, call
 * Document_AddToIndexes on it.
 *
 * - client is a blocked client which will be used as the context for this
 *   operation.
 * - sp is the index that this document will be added to
 * - base is the document to be index. The context will take ownership of the
 *   document's contents (but not the structure itself). Thus, you should not
 *   call Document_Free on the document after a successful return of this
 *   function.
 *
 * When done, call AddDocumentCtx_Free
 */
RSAddDocumentCtx *NewAddDocumentCtx(IndexSpec *sp, Document *base, QueryError *status);

/**
 * At this point the context will take over from the caller, and handle sending
 * the replies and so on.
 */
void AddDocumentCtx_Submit(RSAddDocumentCtx *aCtx, RedisSearchCtx *sctx, uint32_t options);

/**
 * Indicate that processing is finished on the current document
 */
void AddDocumentCtx_Finish(RSAddDocumentCtx *aCtx);
/**
 * This function will tokenize the document and add the resultant tokens to
 * the relevant inverted indexes. This function should be called from a
 * worker thread (see ConcurrentSearch functions).
 *
 *
 * When this function completes, it will send the reply to the client and
 * unblock the client passed when the context was first created.
 */
int Document_AddToIndexes(RSAddDocumentCtx *ctx);

/**
 * Free the AddDocumentCtx. Should be done once AddToIndexes() completes; or
 * when the client is unblocked.
 */
void AddDocumentCtx_Free(RSAddDocumentCtx *aCtx);

/* Load a single document */
int Redis_LoadDocumentC(RedisSearchCtx *ctx, const char *s, size_t n, Document *doc);
int Redis_LoadDocumentR(RedisSearchCtx *ctx, RedisModuleString *key, Document *doc);
#define Redis_LoadDocument Redis_LoadDocumentR

/* Evaluate an IF expression (e.g. IF "@foo == 'bar'") against a document, by getting the
 * properties from the sorting table or from the hash representation of the document.
 *
 * NOTE: This is disconnected from the document indexing flow, and loads the document and discards
 * of it internally
 *
 * Returns  REDISMODULE_ERR on failure, OK otherwise*/
int Document_EvalExpression(RedisSearchCtx *sctx, RedisModuleString *key, const char *expr,
                            int *result, QueryError *err);

/**
 * Load a single document fields is an array of fields to load from a document.
 * keyp is an [out] pointer to a key which may be closed after the document field
 * is no longer required. Can be NULL
 */
int Redis_LoadDocumentEx(RedisSearchCtx *ctx, RedisModuleString *key, const char **fields,
                         size_t nfields, Document *doc, RedisModuleKey **keyp);

// Don't create document if it does not exist. Replace only
#define REDIS_SAVEDOC_NOCREATE 0x01
/**
 * Save a document in the index. Used for returning contents in search results.
 */
int Redis_SaveDocument(RedisSearchCtx *ctx, Document *doc, int options, QueryError *status);

/* Serialzie the document's fields to a redis client */
int Document_ReplyFields(RedisModuleCtx *ctx, Document *doc);

DocumentField *Document_GetField(Document *d, const char *fieldName);

// Document add functions:
int RSAddDocumentCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int RSSafeAddDocumentCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int RSAddHashCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int RSSafeAddHashCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

int RS_AddDocument(RedisSearchCtx *sctx, RedisModuleString *name, const AddDocumentOptions *opts,
                   QueryError *status);
#ifdef __cplusplus
}
#endif
#endif

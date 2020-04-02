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
  const char *name;  // Can either be char or RMString
  RedisModuleString *text;
  FieldType indexAs;
} DocumentField;

typedef struct Document {
  RedisModuleString *docKey;
  RedisModuleKey *keyobj;  // return from OpenKey, used for cache
  DocumentField *fields;
  uint32_t numFields;
  RSLanguage language;
  float score;
  t_docId docId;
  const char *payload;
  size_t payloadSize;
  uint32_t flags;
} Document;

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
 * Never own the contents. Used for fast-track performance
 */
#define DOCUMENT_F_NEVEROWN 0x04

/**
 * The document has been moved to another target. This is quicker than
 * zero'ing the entire structure
 */
#define DOCUMENT_F_DEAD 0x08

struct RSAddDocumentCtx;

typedef void (*DocumentAddCompleted)(struct RSAddDocumentCtx *, RedisModuleCtx *, void *);

typedef struct {
  uint32_t options;                 // DOCUMENT_ADD_XXX
  RSLanguage language;              // Language document should be indexed as
  RedisModuleString *payload;       // Arbitrary payload provided on return with WITHPAYLOADS
  RedisModuleString **fieldsArray;  // Field, Value, Field Value
  size_t numFieldElems;             // Number of elements
  double score;                     // Score of the document
  const char *evalExpr;             // Only add the document if this expression evaluates to true.
} AddDocumentOptions;

void Document_AddField(Document *d, const char *fieldname, RedisModuleString *fieldval,
                       uint32_t typemask);

/**
 * Add a simple char buffer value. This creates an RMString internally, so this
 * must be used with F_OWNSTRINGS
 */
void Document_AddFieldC(Document *d, const char *fieldname, const char *val, size_t vallen,
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
void Document_Init(Document *doc, RedisModuleString *docKey, double score, RSLanguage lang);
void Document_SetPayload(Document *doc, const void *payload, size_t n);

/**
 * Make the document the owner of the strings it contains
 */
void Document_MakeStringsOwner(Document *doc);

/**
 * Make the document object steal references to the document's strings.
 */
void Document_MakeRefOwner(Document *doc);

/**
 * Clear the document of its fields. This does not free the document
 * or clear its name
 */
void Document_Clear(Document *doc);

/**
 * Move the contents of one document to another. This also manages ownership
 * semantics
 */
void Document_Move(Document *dst, Document *src);

/**
 * Load all fields specified in the schema to the document. Note that
 * the document must then be freed using Document_Free().
 *
 * The document must already have the docKey set
 */
int Document_LoadSchemaFields(Document *doc, RedisSearchCtx *sctx, QueryError *err);

/**
 * Load all the fields into the document.
 */
int Document_LoadAllFields(Document *doc, RedisModuleCtx *ctx);

void Document_LoadPairwiseArgs(Document *doc, RedisModuleString **args, size_t nargs);

/**
 * Print contents of document to screen
 */
void Document_Dump(const Document *doc);  // LCOV_EXCL_LINE debug
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
#define DOCUMENT_ADD_REPLYCTX 0x08  // Reply with status messages
#define DOCUMENT_ADD_NOCREATE 0x10  // Don't create document if not exist (replace ONLY)

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

// Document is entirely empty (no sortables, indexables)
#define ACTX_F_EMPTY 0x40

// Do not send reply on success/failure
#define ACTX_F_NOREPLY 0x80

struct DocumentIndexer;

/** Context used when indexing documents */
typedef struct RSAddDocumentCtx {
  Document doc;  // Document which is being indexed

  // Forward index. This contains all the terms found in the document
  struct ForwardIndex *fwIdx;

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
} RSAddDocumentCtx;

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
RSAddDocumentCtx *ACTX_New(IndexSpec *sp, Document *base, QueryError *status);

int ACTX_Preprocess(RSAddDocumentCtx *aCtx);

/** Indexes a single document */
int ACTX_Index(RSAddDocumentCtx *aCtx, RedisSearchCtx *sctx, uint32_t options);

/**
 * Free the AddDocumentCtx. Should be done once AddToIndexes() completes; or
 * when the client is unblocked.
 */
void ACTX_Free(RSAddDocumentCtx *aCtx);

/* Evaluate an IF expression (e.g. IF "@foo == 'bar'") against a document, by getting the
 * properties from the sorting table or from the hash representation of the document.
 *
 * NOTE: This is disconnected from the document indexing flow, and loads the document and discards
 * of it internally
 *
 * Returns  REDISMODULE_ERR on failure, OK otherwise*/
int Document_EvalExpression(RedisSearchCtx *sctx, RedisModuleString *key, const char *expr,
                            int *result, QueryError *err);

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

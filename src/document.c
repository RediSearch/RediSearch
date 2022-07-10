
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
#include "rmalloc.h"
#include "indexer.h"
#include "tag_index.h"
#include "aggregate/expr/expression.h"
#include "rmutil/rm_assert.h"

#include <string.h>
#include <inttypes.h>

///////////////////////////////////////////////////////////////////////////////////////////////

// Memory pool for RSAddDocumentContext contexts
//static mempool_t *actxPool_g = NULL;
template<> AddDocumentPool MemPoolObject<AddDocumentPool>::pool(16, 0, true);

#define DUP_FIELD_ERRSTR "Requested to index field twice"

#define FIELD_IS_VALID(aCtx, ix) ((aCtx)->fspecs[ix].name != NULL)

//---------------------------------------------------------------------------------------------

int AddDocumentCtx::SetDocument(IndexSpec *sp, Document *doc, size_t oldFieldCount) {
  stateFlags &= ~ACTX_F_INDEXABLES;
  stateFlags &= ~ACTX_F_TEXTINDEXED;
  stateFlags &= ~ACTX_F_OTHERINDEXED;

  if (oldFieldCount < doc->numFields) {
    // Pre-allocate the field specs
    fspecs = rm_realloc(fspecs, sizeof(*fspecs) * doc->numFields);
    fdatas = rm_realloc(fdatas, sizeof(*fdatas) * doc->numFields);
  }

  for (size_t ii = 0; ii < doc->numFields; ++ii) {
    // zero out field data. We check at the destructor to see if there is any
    // left-over tag data here; if we've realloc'd, then this contains garbage
    fdatas[ii].tags = TagIndex::Tags();
  }
  size_t numTextIndexable = 0;

  // size: uint16_t * SPEC_MAX_FIELDS
  FieldSpecDedupeArray dedupe;
  int hasTextFields = 0;
  int hasOtherFields = 0;

  for (size_t i = 0; i < doc->numFields; i++) {
    DocumentField *f = doc->fields + i;
    const FieldSpec *fs = sp->GetField(f->name, strlen(f->name));
    if (!fs || !f->text) {
      fspecs[i].name = NULL;
      fspecs[i].types = 0;
      continue;
    }

    fspecs[i] = *fs;
    if (dedupe[fs->index]) {
      status.SetErrorFmt(QUERY_EDUPFIELD, "Tried to insert `%s` twice", fs->name);
      return -1;
    }

    dedupe[fs->index] = 1;

    if (fs->IsSortable()) {
      // mark sortable fields to be updated in the state flags
      stateFlags |= ACTX_F_SORTABLES;
    }

    // See what we want the given field indexed as:
    if (!f->indexAs) {
      f->indexAs = fs->types;
    } else {
      // Verify the flags:
      if ((f->indexAs & fs->types) != f->indexAs) {
        status.SetErrorFmt(QUERY_EUNSUPPTYPE,
                               "Tried to index field %s as type not specified in schema", fs->name);
        return -1;
      }
    }

    if (fs->IsIndexable()) {
      if (f->indexAs & INDEXFLD_T_FULLTEXT) {
        numTextIndexable++;
        hasTextFields = 1;
      }

      if (f->indexAs != INDEXFLD_T_FULLTEXT) {
        // has non-text but indexable fields
        hasOtherFields = 1;
      }

      if (FIELD_CHKIDX(f->indexAs, INDEXFLD_T_GEO)) {
        docFlags = Document_HasOnDemandDeletable;
      }
    }
  }

  if (hasTextFields || hasOtherFields) {
    stateFlags |= ACTX_F_INDEXABLES;
  } else {
    stateFlags &= ~ACTX_F_INDEXABLES;
  }

  if (!hasTextFields) {
    stateFlags |= ACTX_F_TEXTINDEXED;
  } else {
    stateFlags &= ~ACTX_F_TEXTINDEXED;
  }

  if (!hasOtherFields) {
    stateFlags |= ACTX_F_OTHERINDEXED;
  } else {
    stateFlags &= ~ACTX_F_OTHERINDEXED;
  }

  if ((stateFlags & ACTX_F_SORTABLES) && sv == NULL) {
    sv = new RSSortingVector(sp->sortables->len);
  }

  int empty = (sv == NULL) && !hasTextFields && !hasOtherFields;
  if (empty) {
    stateFlags |= ACTX_F_EMPTY;
  }

  if ((options & DOCUMENT_ADD_NOSAVE) == 0 && numTextIndexable &&
      (sp->flags & Index_StoreByteOffsets)) {
    if (!byteOffsets) {
      byteOffsets = new RSByteOffsets();
      offsetsWriter = *new ByteOffsetWriter();
    }
    byteOffsets->ReserveFields(numTextIndexable);
  }

  Document::Move(doc, doc);
  return 0;
}

//---------------------------------------------------------------------------------------------

// Creates a new context used for adding documents. Once created, call
// Document::AddToIndexes on it.
//
// - client is a blocked client which will be used as the context for this
//   operation.
// - sp is the index that this document will be added to
// - base is the document to be index. The context will take ownership of the
//   document's contents (but not the structure itself). Thus, you should not
//   call Document_Free on the document after a successful return of this
//   function.
//
// When done, call delete

AddDocumentCtx::AddDocumentCtx(IndexSpec *sp, Document *b, QueryError *status_) {
  stateFlags = 0;
  status.ClearError();
  totalTokens = 0;
  docFlags = 0;
  client.bc = NULL;
  next = NULL;
  specFlags = sp->flags;
  indexer = sp->indexer;
  RS_LOG_ASSERT(sp->indexer, "No indexer");
  indexer->Incref();

  // Assign the document:
  if (SetDocument(sp, b, doc.numFields) != 0) {
    *status_ = status;
    status.detail = NULL;
    throw Error("AddDocumentCtx::SetDocument failed");
  }

  // try to reuse the forward index on recycled contexts
  if (fwIdx) {
    fwIdx->Reset(&doc, sp->flags);
  } else {
    fwIdx = new ForwardIndex(&doc, sp->flags);
  }

  if (sp->smap) {
    // we get a read only copy of the synonym map for accessing in the index thread with out worring
    // about thready safe issues
    fwIdx->smap = sp->smap->GetReadOnlyCopy();
  } else {
    fwIdx->smap = NULL;
  }

  tokenizer = GetTokenizer(b->language, fwIdx->stemmer, sp->stopwords);
  doc.docId = 0;
}

//---------------------------------------------------------------------------------------------

static void doReplyFinish(AddDocumentCtx *aCtx, RedisModuleCtx *ctx) {
  aCtx->donecb(aCtx, ctx, aCtx->donecbData);
  aCtx->indexer->Decref();
  delete aCtx;
}

static int replyCallback(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
  AddDocumentCtx *aCtx = RedisModule_GetBlockedClientPrivateData(ctx);
  doReplyFinish(aCtx, ctx);
  return REDISMODULE_OK;
}

static void threadCallback(void *p) {
  Document::AddToIndexes(p);
}

//---------------------------------------------------------------------------------------------

// Indicate that processing is finished on the current document

void AddDocumentCtx::Finish() {
  if (stateFlags & ACTX_F_NOBLOCK) {
    doReplyFinish(this, client.sctx->redisCtx);
  } else {
    RedisModule_UnblockClient(client.bc, this);
  }
}

//---------------------------------------------------------------------------------------------

// How many bytes in a document to warrant it being tokenized in a separate thread
#define SELF_EXEC_THRESHOLD 1024

/**
 * Print contents of document to screen
 */

// LCOV_EXCL_START debug
void Document::Dump() const {
  printf("Document Key: %s. ID=%" PRIu64 "\n", RedisModule_StringPtrLen(docKey, NULL),
         docId);
  for (size_t ii = 0; ii < numFields; ++ii) {
    printf("  [%lu]: %s => %s\n", ii, fields[ii].name,
           RedisModule_StringPtrLen(fields[ii].text, NULL));
  }
}
// LCOV_EXCL_STOP

//---------------------------------------------------------------------------------------------

bool AddDocumentCtx::ReplaceMerge(RedisSearchCtx *sctx) {
  /**
   * The REPLACE operation contains fields which must be reindexed. This means
   * that a new document ID needs to be assigned, and as a consequence, all
   * fields must be reindexed.
   */
  // Free the old field data
  size_t oldFieldCount = doc.numFields;

  doc.Clear();
  int rv = doc.LoadSchemaFields(sctx);
  if (rv != REDISMODULE_OK) {
    status.SetError(QUERY_ENODOC, "Could not load existing document");
    donecb(this, sctx->redisCtx, donecbData);
    delete this;
    return true;
  }

  // Keep hold of the new fields.
  doc.MakeStringsOwner();
  SetDocument(sctx->spec, &doc, oldFieldCount);
  return false;
}

//---------------------------------------------------------------------------------------------

bool AddDocumentCtx::handlePartialUpdate(RedisSearchCtx *sctx) {
  // Handle partial update of fields
  if (stateFlags & ACTX_F_INDEXABLES) {
    return ReplaceMerge(sctx);
  } else {
    // No indexable fields are updated, we can just update the metadata.
    // Quick update just updates the score, payload and sortable fields of the document.
    // Thus full-reindexing of the document is not required
    UpdateNoIndex(sctx);
    return true;
  }
}

//---------------------------------------------------------------------------------------------

// At this point the context will take over from the caller, and handle sending
// the replies and so on.

void AddDocumentCtx::Submit(RedisSearchCtx *sctx, uint32_t options) {
  options = options;
  if ((options & DOCUMENT_ADD_PARTIAL) && handlePartialUpdate(sctx)) {
    return;
  }

  // We actually modify (!) the strings in the document, so we always require
  // ownership
  doc.MakeStringsOwner();

  if (IsBlockable()) {
    client.bc = RedisModule_BlockClient(sctx->redisCtx, replyCallback, NULL, NULL, 0);
  } else {
    client.sctx = sctx;
  }

  RS_LOG_ASSERT(client.bc, "No blocked client");
  size_t totalSize = 0;
  for (size_t ii = 0; ii < doc.numFields; ++ii) {
    const DocumentField *ff = doc.fields + ii;
    if (fspecs[ii].name && (ff->indexAs & (INDEXFLD_T_FULLTEXT | INDEXFLD_T_TAG))) {
      size_t n;
      RedisModule_StringPtrLen(doc.fields[ii].text, &n);
      totalSize += n;
    }
  }

  if (totalSize >= SELF_EXEC_THRESHOLD && IsBlockable()) {
    ConcurrentSearch_ThreadPoolRun(threadCallback, this, CONCURRENT_POOL_INDEX);
  } else {
    Document::AddToIndexes(this);
  }
}

//---------------------------------------------------------------------------------------------

// Free AddDocumentCtx. Should be done once AddToIndexes() completes; or
// when the client is unblocked.

AddDocumentCtx::~AddDocumentCtx() {
  // Free preprocessed data; this is the only reliable place to do it
  for (size_t i = 0; i < doc.numFields; ++i) {
    if (FIELD_IS_VALID(this, i) && (fspecs + i)->IsFieldType(INDEXFLD_T_TAG) &&
        !!fdatas[i].tags) {
      fdatas[i].tags.Clear();
    }
  }

  delete sv;
  delete byteOffsets;
  delete tokenizer;

  if (oldMd) {
    oldMd->Decref();
    oldMd = NULL;
  }

  offsetsWriter.Cleanup();
  status.ClearError();

  delete fwIdx;

  rm_free(fspecs);
  rm_free(fdatas);
}

//---------------------------------------------------------------------------------------------

#define FIELD_HANDLER(name)                                                                \
  static int name(AddDocumentCtx *aCtx, const DocumentField *field, const FieldSpec *fs,   \
                  FieldIndexerData *fdata, QueryError *status)

#define FIELD_BULK_INDEXER(name)                                                            \
  static int name(IndexBulkData *bulk, AddDocumentCtx *aCtx, RedisSearchCtx *ctx,           \
                  const DocumentField *field, const FieldSpec *fs, FieldIndexerData *fdata, \
                  QueryError *status)

#define FIELD_BULK_CTOR(name) \
  static void name(IndexBulkData *bulk, const FieldSpec *fs, RedisSearchCtx *ctx)
#define FIELD_BULK_FINALIZER(name) static void name(IndexBulkData *bulk, RedisSearchCtx *ctx)

#define FIELD_PREPROCESSOR FIELD_HANDLER

//---------------------------------------------------------------------------------------------

FIELD_PREPROCESSOR(fulltextPreprocessor) {
  size_t fl;
  const char *c = RedisModule_StringPtrLen(field->text, &fl);
  if (fs->IsSortable()) {
    aCtx->sv->Put(fs->sortIdx, (void *)c, RS_SORTABLE_STR);
  }

  if (fs->IsIndexable()) {
    VarintVectorWriter *curOffsetWriter = NULL;
    RSByteOffsetField *curOffsetField = NULL;
    if (aCtx->byteOffsets) {
      curOffsetField = aCtx->byteOffsets->AddField(fs->ftId, aCtx->totalTokens + 1);
      curOffsetWriter = &aCtx->offsetsWriter;
    }

    ForwardIndexTokenizerCtx tokCtx(aCtx->fwIdx, c, curOffsetWriter, fs->ftId, fs->ftWeight);

    uint32_t options = TOKENIZE_DEFAULT_OPTIONS;
    if (fs->IsNoStem()) {
      options |= TOKENIZE_NOSTEM;
    }
    if (fs->IsPhonetics()) {
      options |= TOKENIZE_PHONETICS;
    }
    aCtx->tokenizer->Start((char *)c, fl, options);

    Token tok;
    uint32_t newTokPos;
    while (0 != (newTokPos = aCtx->tokenizer->Next(&tok))) {
      tokCtx.TokenFunc(&tok);
    }
    uint32_t lastTokPos = aCtx->tokenizer->lastOffset;

    if (curOffsetField) {
      curOffsetField->lastTokPos = lastTokPos;
    }
    aCtx->totalTokens = lastTokPos;
  }
  return 0;
}

//---------------------------------------------------------------------------------------------

FIELD_PREPROCESSOR(numericPreprocessor) {
  if (RedisModule_StringToDouble(field->text, &fdata->numeric) == REDISMODULE_ERR) {
    status->SetCode(QUERY_ENOTNUMERIC);
    return -1;
  }

  // If this is a sortable numeric value - copy the value to the sorting vector
  if (fs->IsSortable()) {
    aCtx->sv->Put(fs->sortIdx, &fdata->numeric, RS_SORTABLE_NUM);
  }
  return 0;
}

//---------------------------------------------------------------------------------------------

FIELD_BULK_INDEXER(numericIndexer) {
  NumericRangeTree *rt = bulk->indexDatas[INDEXTYPE_TO_POS(INDEXFLD_T_NUMERIC)];
  if (!rt) {
    RedisModuleString *keyName = ctx->spec->GetFormattedKey(fs, INDEXFLD_T_NUMERIC);
    rt = bulk->indexDatas[IXFLDPOS_NUMERIC] =
        OpenNumericIndex(ctx, keyName, &bulk->indexKeys[IXFLDPOS_NUMERIC]);
    if (!rt) {
      status->SetError(QUERY_EGENERIC, "Could not open numeric index for indexing");
      return -1;
    }
  }
  size_t sz = rt->Add(aCtx->doc.docId, fdata->numeric);
  ctx->spec->stats.invertedSize += sz; // TODO: exact amount
  ctx->spec->stats.numRecords++;
  return 0;
}

//---------------------------------------------------------------------------------------------

FIELD_PREPROCESSOR(geoPreprocessor) {
  const char *c = RedisModule_StringPtrLen(field->text, NULL);
  char *pos = strpbrk(c, " ,");
  if (!pos) {
    status->SetCode(QUERY_EGEOFORMAT);
    return -1;
  }
  *pos = '\0';
  pos++;
  fdata->geoSlon = c;
  fdata->geoSlat = pos;
  return 0;
}

//---------------------------------------------------------------------------------------------

FIELD_BULK_INDEXER(geoIndexer) {
  GeoIndex gi(ctx, fs);
  int rv = gi.AddStrings(aCtx->doc.docId, fdata->geoSlon, fdata->geoSlat);

  if (rv == REDISMODULE_ERR) {
    status->SetError(QUERY_EGENERIC, "Could not index geo value");
    return -1;
  }
  return 0;
}

//---------------------------------------------------------------------------------------------

FIELD_PREPROCESSOR(tagPreprocessor) {
  fdata->tags = TagIndex::Tags(fs->tagSep, fs->tagFlags, field);
  if (!fdata->tags) {
    return 0;
  }
  if (fs->IsSortable()) {
    size_t fl;
    const char *c = RedisModule_StringPtrLen(field->text, &fl);
    aCtx->sv->Put(fs->sortIdx, (void *)c, RS_SORTABLE_STR);
  }
  return 0;
}

//---------------------------------------------------------------------------------------------

//@@ make template
FIELD_BULK_INDEXER(tagIndexer) {
  TagIndex *tidx = bulk->indexDatas[IXFLDPOS_TAG];
  if (!tidx) {
    RedisModuleString *kname = ctx->spec->GetFormattedKey(fs, INDEXFLD_T_TAG);
    tidx = bulk->indexDatas[IXFLDPOS_TAG] = TagIndex::Open(ctx, kname, 1, &bulk->indexKeys[IXFLDPOS_TAG]);
    if (!tidx) {
      status->SetError(QUERY_EGENERIC, "Could not open tag index for indexing");
      return -1;
    }
  }

  ctx->spec->stats.invertedSize += tidx->Index(fdata->tags, aCtx->doc.docId);
  ctx->spec->stats.numRecords++;
  return 0;
}

//---------------------------------------------------------------------------------------------

static PreprocessorFunc preprocessorMap[] = {
    // nl break
    [IXFLDPOS_FULLTEXT] = fulltextPreprocessor,
    [IXFLDPOS_NUMERIC] = numericPreprocessor,
    [IXFLDPOS_GEO] = geoPreprocessor,
    [IXFLDPOS_TAG] = tagPreprocessor};

//---------------------------------------------------------------------------------------------

int IndexBulkData::Add(AddDocumentCtx *cur, RedisSearchCtx *sctx, const DocumentField *field,
                       const FieldSpec *fs, FieldIndexerData *fdata, QueryError *status) {
  int rc = 0;
  for (size_t ii = 0; ii < INDEXFLD_NUM_TYPES && rc == 0; ++ii) {
    // see which types are supported in the current field...
    if (field->indexAs & INDEXTYPE_FROM_POS(ii)) {
      switch (ii) {
        case IXFLDPOS_TAG:
          rc = tagIndexer(this, cur, sctx, field, fs, fdata, status);
          break;
        case IXFLDPOS_NUMERIC:
          rc = numericIndexer(this, cur, sctx, field, fs, fdata, status);
          break;
        case IXFLDPOS_GEO:
          rc = geoIndexer(this, cur, sctx, field, fs, fdata, status);
          break;
        case IXFLDPOS_FULLTEXT:
          break;
        default:
          rc = -1;
          status->SetError(QUERY_EINVAL, "BUG: invalid index type");
          break;
      }
    }
  }
  return rc;
}

//---------------------------------------------------------------------------------------------

void IndexBulkData::Cleanup(RedisSearchCtx *sctx) {
  for (size_t ii = 0; ii < INDEXFLD_NUM_TYPES; ++ii) {
    if (indexKeys[ii]) {
      RedisModule_CloseKey(indexKeys[ii]);
    }
  }
}

//---------------------------------------------------------------------------------------------

/**
 * This function will tokenize the document and add the resultant tokens to
 * the relevant inverted indexes. This function should be called from a
 * worker thread (see ConcurrentSearch functions).
 *
 *
 * When this function completes, it will send the reply to the client and
 * unblock the client passed when the context was first created.
 */

static int Document::AddToIndexes(AddDocumentCtx *aCtx) {
  Document *doc = &aCtx->doc;
  int ourRv = REDISMODULE_OK;

  for (size_t i = 0; i < doc->numFields; i++) {
    const FieldSpec *fs = aCtx->fspecs + i;
    const DocumentField *ff = doc->fields + i;
    FieldIndexerData *fdata = aCtx->fdatas + i;

    if (fs->name == NULL || ff->indexAs == 0) {
      LG_DEBUG("Skipping field %s not in index!", doc->fields[i].name);
      continue;
    }

    for (size_t ii = 0; ii < INDEXFLD_NUM_TYPES; ++ii) {
      if (!FIELD_CHKIDX(ff->indexAs, INDEXTYPE_FROM_POS(ii))) {
        continue;
      }

      PreprocessorFunc pp = preprocessorMap[ii];
      if (pp(aCtx, &doc->fields[i], fs, fdata, &aCtx->status) != 0) {
        ourRv = REDISMODULE_ERR;
        goto cleanup;
      }
    }
  }

  if (aCtx->indexer->Add(aCtx) != 0) {
    ourRv = REDISMODULE_ERR;
    goto cleanup;
  }

cleanup:
  if (ourRv != REDISMODULE_OK) {
    aCtx->status.SetCode(QUERY_EGENERIC);
    aCtx->Finish();
  }
  return ourRv;
}

//---------------------------------------------------------------------------------------------

/* Evaluate an IF expression (e.g. IF "@foo == 'bar'") against a document, by getting the
 * properties from the sorting table or from the hash representation of the document.
 *
 * NOTE: This is disconnected from the document indexing flow, and loads the document and discards
 * of it internally
 *
 * Returns  REDISMODULE_ERR on failure, OK otherwise
 */

static int Document::EvalExpression(RedisSearchCtx *sctx, RedisModuleString *key, const char *expr,
                                    int *result, QueryError *status) {
  int rc = REDISMODULE_ERR;
  const RSDocumentMetadata *dmd = sctx->spec->docs.GetByKeyR(key);
  if (!dmd) {
    // We don't know the document...
    status->SetError(QUERY_ENODOC, "");
    return REDISMODULE_ERR;
  }

  // Try to parser the expression first, fail if we can't
  try {
    RSExpr e(expr, strlen(expr), status);

    if (status->HasError()) {
      return REDISMODULE_ERR;
    }

    RLookupRow row;
    RSValue rv;
    RLookupLoadOptions *loadopts = NULL;
    ExprEval *evaluator = NULL;
    IndexSpecCache *spcache = sctx->spec->GetSpecCache();
    RLookup lookup_s(spcache);
    if (e.GetLookupKeys(&lookup_s, status) == EXPR_EVAL_ERR) {
      goto done;
    }

    loadopts = new RLookupLoadOptions(sctx, dmd, status);
    if (lookup_s.LoadDocument(&row, loadopts) != REDISMODULE_OK) {
      // printf("Couldn't load document!\n");
      delete loadopts;
      goto done;
    }

    evaluator = new ExprEval(status, &lookup_s, &row, &e);
    if (evaluator->Eval(&rv) != EXPR_EVAL_OK) {
      // printf("Eval not OK!!! SAD!!\n");
      delete evaluator;
      goto done;
    }

    *result = rv.BoolTest();
    rv.Clear();
    rc = REDISMODULE_OK;

  // Clean up:
done:
    row.Cleanup();
    return rc;
  } catch (...) {
    return REDISMODULE_ERR;
  }
}

//---------------------------------------------------------------------------------------------

void AddDocumentCtx::UpdateNoIndex(RedisSearchCtx *sctx) {
#define BAIL(s)                                \
  do {                                         \
    status.SetError(QUERY_EGENERIC, s);        \
    donecb(this, sctx->redisCtx, donecbData);  \
    delete this;                               \
  } while (0);

  Document *doc = doc;
  t_docId docId = sctx->spec->docs.GetIdR(doc->docKey);
  if (docId == 0) {
    BAIL("Couldn't load old document");
  }
  RSDocumentMetadata *md = sctx->spec->docs.Get(docId);
  if (!md) {
    BAIL("Couldn't load document metadata");
  }

  // Update the score
  md->score = doc->score;
  // Set the payload if needed
  if (doc->payload) {
    sctx->spec->docs.SetPayload(docId, doc->payload, doc->payloadSize);
  }

  if (stateFlags & ACTX_F_SORTABLES) {
    FieldSpecDedupeArray dedupes = {0};
    // Update sortables if needed
    for (int i = 0; i < doc->numFields; i++) {
      DocumentField *f = &doc->fields[i];
      const FieldSpec *fs = sctx->spec->GetField(f->name, strlen(f->name));
      if (fs == NULL || !fs->IsSortable()) {
        continue;
      }

      if (dedupes[fs->index]) {
        BAIL(DUP_FIELD_ERRSTR);
      }

      dedupes[fs->index] = 1;

      int idx = sctx->spec->GetFieldSortingIndex(f->name, strlen(f->name));
      if (idx < 0) continue;

      if (!md->sortVector) {
        md->sortVector = new RSSortingVector(sctx->spec->sortables->len);
      }

      RS_LOG_ASSERT((fs->options & FieldSpec_Dynamic) == 0, "Dynamic field cannot use PARTIAL");

      switch (fs->types) {
        case INDEXFLD_T_FULLTEXT:
        case INDEXFLD_T_TAG:
          md->sortVector->Put(idx, (void *)RedisModule_StringPtrLen(f->text, NULL),
                              RS_SORTABLE_STR);
          break;
        case INDEXFLD_T_NUMERIC: {
          double numval;
          if (RedisModule_StringToDouble(f->text, &numval) == REDISMODULE_ERR) {
            BAIL("Could not parse numeric index value");
          }
          md->sortVector->Put(idx, &numval, RS_SORTABLE_NUM);
          break;
        }
        default:
          BAIL("Unsupported sortable type");
          break;
      }
    }
  }
}

//---------------------------------------------------------------------------------------------

DocumentField *Document::GetField(const char *fieldName) {
  if (!this || !fieldName) return NULL;

  for (int i = 0; i < numFields; i++) {
    if (!strcasecmp(fields[i].name, fieldName)) {
      return &fields[i];
    }
  }
  return NULL;
}

///////////////////////////////////////////////////////////////////////////////////////////////

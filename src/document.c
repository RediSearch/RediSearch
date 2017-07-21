#include <string.h>

#include "document.h"
#include "forward_index.h"
#include "geo_index.h"
#include "index.h"
#include "numeric_filter.h"
#include "numeric_index.h"
#include "redis_index.h"
#include "rmutil/strings.h"
#include "rmutil/util.h"
#include "spec.h"
#include "tokenize.h"
#include "util/logging.h"
#include "search_request.h"
#include "rmalloc.h"

void Document_Init(Document *doc, RedisModuleString *docKey, double score, int numFields,
                   const char *lang, const char *payload, size_t payloadSize) {
  doc->docKey = docKey;
  doc->score = (float)score;
  doc->numFields = numFields;
  doc->fields = calloc(doc->numFields, sizeof(DocumentField));
  doc->language = lang;
  doc->payload = payload;
  doc->payloadSize = payloadSize;
}

int Redis_SaveDocument(RedisSearchCtx *ctx, Document *doc) {

  RedisModuleKey *k =
      RedisModule_OpenKey(ctx->redisCtx, doc->docKey, REDISMODULE_WRITE | REDISMODULE_READ);
  if (k == NULL || (RedisModule_KeyType(k) != REDISMODULE_KEYTYPE_EMPTY &&
                    RedisModule_KeyType(k) != REDISMODULE_KEYTYPE_HASH)) {
    return REDISMODULE_ERR;
  }

  for (int i = 0; i < doc->numFields; i++) {
    RedisModule_HashSet(k, REDISMODULE_HASH_CFIELDS, doc->fields[i].name, doc->fields[i].text,
                        NULL);
  }
  return REDISMODULE_OK;
}

static int Document_Store(Document *doc, RedisSearchCtx *ctx, int nosave, int replace,
                          const char **errorString) {
  const char *keystr = RedisModule_StringPtrLen(doc->docKey, NULL);
  DocTable *table = &ctx->spec->docs;

  // if we're in replace mode, first we need to try and delete the older version of the document
  if (replace) {
    DocTable_Delete(table, keystr);
  }
  doc->docId = DocTable_Put(table, keystr, doc->score, 0, doc->payload, doc->payloadSize);

  // Make sure the document is not already in the index - it needs to be
  // incremental!
  if (doc->docId == 0) {
    *errorString = "Couldn't allocate new document table entry";
    return -1;
  }

  if (!nosave) {
    // first save the document as hash
    if (REDISMODULE_ERR == Redis_SaveDocument(ctx, doc)) {
      *errorString = "Couldn't save document";
      goto fail;
    }
  }

  return 0;

fail:
  DocTable_Delete(table, keystr);
  return -1;
}

typedef struct {
  ForwardIndex *idx;
  RSSortingVector *sv;
  Document *doc;
  size_t totalTokens;
} indexingContext;

static void ensureSortingVector(RedisSearchCtx *sctx, indexingContext *ictx) {
  if (!ictx->sv) {
    ictx->sv = NewSortingVector(sctx->spec->sortables->len);
  }
}

static int indexField(RedisSearchCtx *ctx, const DocumentField *field, const char **errorString,
                      indexingContext *ictx) {
  // printf("Tokenizing %s: %s\n",
  // RedisModule_StringPtrLen(doc.fields[i].name, NULL),
  //        RedisModule_StringPtrLen(doc.fields[i].text, NULL));

  const FieldSpec *fs = IndexSpec_GetField(ctx->spec, field->name, strlen(field->name));
  if (fs == NULL) {
    LG_DEBUG("Skipping field %s not in index!", field->name);
    return 0;
  }

  const char *c = RedisModule_StringPtrLen(field->text, NULL);

  switch (fs->type) {
    case F_FULLTEXT:
      if (fs->sortable) {
        ensureSortingVector(ctx, ictx);
        RSSortingVector_Put(ictx->sv, fs->sortIdx, (void *)c, RS_SORTABLE_STR);
      }

      ictx->totalTokens = tokenize(c, fs->weight, fs->id, ictx->idx, forwardIndexTokenFunc,
                                   ictx->idx->stemmer, ictx->totalTokens, ctx->spec->stopwords);
      break;
    case F_NUMERIC: {
      double score;

      if (RedisModule_StringToDouble(field->text, &score) == REDISMODULE_ERR) {
        *errorString = "Could not parse numeric index value";
        return -1;
      }

      NumericRangeTree *rt = OpenNumericIndex(ctx, fs->name);
      NumericRangeTree_Add(rt, ictx->doc->docId, score);

      // If this is a sortable numeric value - copy the value to the sorting vector
      if (fs->sortable) {
        ensureSortingVector(ctx, ictx);
        RSSortingVector_Put(ictx->sv, fs->sortIdx, &score, RS_SORTABLE_NUM);
      }
      break;
    }
    case F_GEO: {

      char *pos = strpbrk(c, " ,");
      if (!pos) {
        *errorString = "Invalid lon/lat format. Use \"lon lat\" or \"lon,lat\"";
        return -1;
      }
      *pos = '\0';
      pos++;
      char *slon = (char *)c, *slat = (char *)pos;

      GeoIndex gi = {.ctx = ctx, .sp = fs};
      if (GeoIndex_AddStrings(&gi, ictx->doc->docId, slon, slat) == REDISMODULE_ERR) {
        *errorString = "Could not index geo value";
        return -1;
      }
    }
    default:
      break;
  }
  return 0;
}

/* Add a parsed document to the index. If replace is set, we will add it be deleting an older
 * version of it first */
int Document_AddToIndexes(Document *doc, RedisSearchCtx *ctx, const char **errorString,
                          int options) {
  int replace = options & DOCUMENT_ADD_REPLACE;
  int nosave = options & DOCUMENT_ADD_NOSAVE;

  if (Document_Store(doc, ctx, nosave, replace, errorString) != 0) {
    return REDISMODULE_ERR;
  }

  ForwardIndex *idx = NewForwardIndex(doc);
  indexingContext ictx = {.idx = idx, .doc = doc, .totalTokens = 0};
  int ourRv = REDISMODULE_OK;

  for (int i = 0; i < doc->numFields; i++) {
    // printf("Tokenizing %s: %s\n",
    // RedisModule_StringPtrLen(doc.fields[i].name, NULL),
    //        RedisModule_StringPtrLen(doc.fields[i].text, NULL));

    if (indexField(ctx, &doc->fields[i], errorString, &ictx) == -1) {
      ourRv = REDISMODULE_ERR;
      goto cleanup;
    }
  }

  RSDocumentMetadata *md = DocTable_Get(&ctx->spec->docs, doc->docId);
  md->maxFreq = idx->maxFreq;
  if (ictx.sv) {
    DocTable_SetSortingVector(&ctx->spec->docs, doc->docId, ictx.sv);
  }

  // printf("totaltokens :%d\n", totalTokens);
  if (ictx.totalTokens > 0) {
    ForwardIndexIterator it = ForwardIndex_Iterate(idx);

    ForwardIndexEntry *entry = ForwardIndexIterator_Next(&it);

    IndexEncoder encoder = InvertedIndex_GetEncoder(ctx->spec->flags);

    while (entry != NULL) {
      // ForwardIndex_NormalizeFreq(idx, entry);
      int isNew = IndexSpec_AddTerm(ctx->spec, entry->term, entry->len);
      InvertedIndex *invidx = Redis_OpenInvertedIndex(ctx, entry->term, entry->len, 1);
      if (isNew) {
        ctx->spec->stats.numTerms += 1;
        ctx->spec->stats.termsSize += entry->len;
      }
      size_t sz = InvertedIndex_WriteForwardIndexEntry(invidx, encoder, entry);

      /*******************************************
      * update stats for the index
      ********************************************/

      /* record the change in capacity of the buffer */
      // ctx->spec->stats.invertedCap += w->bw.buf->cap - cap;
      // ctx->spec->stats.skipIndexesSize += w->skipIndexWriter.buf->cap - skcap;
      // ctx->spec->stats.scoreIndexesSize += w->scoreWriter.bw.buf->cap - sccap;
      /* record the actual size consumption change */
      ctx->spec->stats.invertedSize += sz;

      ctx->spec->stats.numRecords++;
      /* increment the number of terms if this is a new term*/

      /* Record the space saved for offset vectors */
      if (ctx->spec->flags & Index_StoreTermOffsets) {
        ctx->spec->stats.offsetVecsSize += entry->vw->bw.buf->offset;
        ctx->spec->stats.offsetVecRecords += entry->vw->nmemb;
      }
      // Redis_CloseWriter(w);

      entry = ForwardIndexIterator_Next(&it);
    }
    // ctx->spec->stats->numDocuments += 1;
  }
  ctx->spec->stats.numDocuments += 1;

cleanup:
  ForwardIndexFree(idx);
  return ourRv;
}

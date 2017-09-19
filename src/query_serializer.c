#if 0
#include "query.h"
#include "fragmenter.h"

// Strip spaces from a buffer in place.
static size_t stripDuplicateSpaces(char *s, size_t n) {
  int isLastSpace = 0;
  size_t oix = 0;
  char *out = s;
  for (size_t ii = 0; ii < n; ++ii) {
    if (isspace(s[ii])) {
      if (isLastSpace) {
        continue;
      } else {
        isLastSpace = 1;
        out[oix++] = ' ';
      }
    } else {
      isLastSpace = 0;
      out[oix++] = s[ii];
    }
  }
  return oix;
}

#define ELLIPSIS "... "

static int maybeFragmentizeOffsets(const RSSearchRequest *req, const ResultEntry *result,
                                   const DocumentField *docField, FragmentList *fragList) {
  if (result->termOffsets) {
    const FieldSpec *fs =
        IndexSpec_GetField(req->sctx->spec, docField->name, strlen(docField->name));
    if (fs) {
      return ResultTermOffsets_Fragmentize(result->termOffsets, fragList, fs->id,
                                           RedisModule_StringPtrLen(docField->text, NULL));
    }
  }
  return 0;
}

static void sendSummarizedField(const RSSearchRequest *req, RedisSearchCtx *sctx,
                                const ReturnedField *fieldInfo, const DocumentField *docField,
                                const ResultEntry *result) {
  FragmentList frags;
  const FragmentTerm *terms = ARRAY_GETARRAY_AS(&result->termOffsets->terms, const FragmentTerm *);
  size_t numTerms = ARRAY_GETSIZE_AS(&result->termOffsets->terms, FragmentTerm);

  // Dump the terms:
  // for (size_t ii = 0; ii < numTerms; ++ii) {
  //   printf("Have term '%.*s'\n", (int)terms[ii].len, terms[ii].tok);
  // }

  FragmentList_Init(&frags, terms, numTerms, 8, 6);

  // Start gathering the terms
  HighlightTags tags = {.openTag = fieldInfo->openTag, .closeTag = fieldInfo->closeTag};
  if (!tags.openTag) {
    tags.openTag = "";
  }
  if (!tags.closeTag) {
    tags.closeTag = "";
  }

  // First actually generate the fragments
  const char *doc = RedisModule_StringPtrLen(docField->text, NULL);
  if (!maybeFragmentizeOffsets(req, result, docField, &frags)) {
    Stemmer *stemmer = NewStemmer(SnowballStemmer, req->language);
    FragmentList_Fragmentize(&frags, doc, stemmer, sctx->spec->stopwords);
    if (stemmer) {
      stemmer->Free(stemmer);
    }
  }

  RedisModuleCtx *ctx = sctx->redisCtx;

  if (fieldInfo->mode == SummarizeMode_WholeField) {
    // Simplest. Just send entire doc
    char *hlDoc = FragmentList_HighlightWholeDocS(&frags, &tags);
    RedisModule_ReplyWithStringBuffer(ctx, hlDoc, strlen(hlDoc));
    free(hlDoc);
    FragmentList_Free(&frags);
    return;
  }

  Array *iovsArr;
  size_t numIovArr = fieldInfo->numFrags;
  const size_t numFrags = FragmentList_GetNumFrags(&frags);

  if (numIovArr > numFrags) {
    numIovArr = numFrags;
  }

  int order;
  switch (fieldInfo->mode) {
    case SummarizeMode_ByOrder:
      order = HIGHLIGHT_ORDER_POS;
      break;
    case SummarizeMode_ByRelevance:
      order = HIGHLIGHT_ORDER_SCORE;
      break;
    case SummarizeMode_ByRelOrder:
    case SummarizeMode_Synopsis:
      order = HIGHLIGHT_ORDER_SCOREPOS;
      break;

    case SummarizeMode_WholeField:
    case SummarizeMode_None:
      // Unreached?
      order = -1;
      break;
  }

  iovsArr = calloc(numIovArr, sizeof(*iovsArr));

  FragmentList_HighlightFragments(&frags, &tags, fieldInfo->contextLen, iovsArr, numIovArr, order);
  if (fieldInfo->mode != SummarizeMode_Synopsis) {
    RedisModule_ReplyWithArray(ctx, numIovArr);
  }

  Array bufTmp;
  Array_Init(&bufTmp);

  for (size_t ii = 0; ii < numIovArr; ++ii) {
    Array *curIovs = iovsArr + ii;
    struct iovec *iovs = ARRAY_GETARRAY_AS(curIovs, struct iovec *);
    size_t numIovs = ARRAY_GETSIZE_AS(curIovs, struct iovec);
    size_t lastSize = bufTmp.len;

    for (size_t jj = 0; jj < numIovs; ++jj) {
      Array_Write(&bufTmp, iovs[jj].iov_base, iovs[jj].iov_len);
    }

    size_t newSize = stripDuplicateSpaces(bufTmp.data + lastSize, bufTmp.len - lastSize);
    Array_Resize(&bufTmp, lastSize + newSize);

    if (fieldInfo->mode == SummarizeMode_Synopsis) {
      Array_Write(&bufTmp, ELLIPSIS, sizeof(ELLIPSIS) - 1);
    } else {
      RedisModule_ReplyWithStringBuffer(ctx, bufTmp.data, bufTmp.len);
      Array_Resize(&bufTmp, 0);
    }
  }

  if (fieldInfo->mode == SummarizeMode_Synopsis) {
    RedisModule_ReplyWithStringBuffer(ctx, bufTmp.data, bufTmp.len);
  }

  Array_Free(&bufTmp);
  for (size_t ii = 0; ii < numIovArr; ++ii) {
    Array_Free(iovsArr + ii);
  }
  free(iovsArr);
  FragmentList_Free(&frags);
}

static void sendFullFields(const Document *doc, RedisModuleCtx *ctx) {
  RedisModule_ReplyWithArray(ctx, doc->numFields * 2);
  for (size_t ii = 0; ii < doc->numFields; ++ii) {
    RedisModule_ReplyWithStringBuffer(ctx, doc->fields[ii].name, strlen(doc->fields[ii].name));
    if (doc->fields[ii].text) {
      RedisModule_ReplyWithString(ctx, doc->fields[ii].text);
    } else {
      RedisModule_ReplyWithNull(ctx);
    }
  }
}

static void sendDocumentFields(RSSearchRequest *req, RedisSearchCtx *sctx, const Document *doc,
                               const ResultEntry *result) {

  const FieldList *fields = &req->fields;
  RedisModuleCtx *ctx = sctx->redisCtx;
  RedisModule_ReplyWithArray(ctx, fields->numFields * 2);
  for (size_t ii = 0; ii < fields->numFields; ii++) {
    size_t idx = fields->fields[ii].nameIndex;
    const DocumentField *docField = doc->fields + idx;
    const ReturnedField *field = fields->fields + ii;
    RedisModule_ReplyWithStringBuffer(ctx, fields->rawFields[idx], strlen(fields->rawFields[idx]));

    if (idx >= doc->numFields || !docField->text) {
      RedisModule_ReplyWithNull(ctx);
      continue;
    }

    if (field->mode != SummarizeMode_None) {
      sendSummarizedField(req, sctx, field, docField, result);
    } else {
      RedisModule_ReplyWithString(ctx, docField->text);
    }
  }
}

int QueryResult_Serialize(QueryResult *r, RedisSearchCtx *sctx, RSSearchRequest *req) {
  RedisModuleCtx *ctx = sctx->redisCtx;

  if (r->errorString != NULL) {
    return RedisModule_ReplyWithError(ctx, r->errorString);
  }

  RedisModule_ReplyWithArray(ctx, REDISMODULE_POSTPONED_ARRAY_LEN);
  RedisModule_ReplyWithLongLong(ctx, (long long)r->totalResults);
  size_t arrlen = 1;

  const int withDocs = (req->fields.numFields || (req->flags & Search_NoContent) == 0);

  const char **fieldList = NULL;
  size_t numFieldList = 0;

  if (req->fields.numFields) {
    fieldList = (const char **)req->fields.rawFields;
    numFieldList = req->fields.numRawFields;
  }

  for (size_t i = 0; i < r->numResults; ++i) {

    Document doc = {NULL};
    RedisModuleKey *rkey = NULL;
    const ResultEntry *result = r->results + i;

    if (withDocs) {
      // Current behavior skips entire result if document does not exist.
      // I'm unusre if that's intentional or an oversight.
      RedisModuleString *idstr = RedisModule_CreateString(ctx, result->id, strlen(result->id));
      Redis_LoadDocumentEx(sctx, idstr, fieldList, numFieldList, &doc, &rkey);
      RedisModule_FreeString(ctx, idstr);
    }

    ++arrlen;

    RedisModule_ReplyWithStringBuffer(ctx, result->id, strlen(result->id));

    if (req->flags & Search_WithScores) {
      ++arrlen;
      RedisModule_ReplyWithDouble(ctx, result->score);
    }

    if (req->flags & Search_WithPayloads) {
      ++arrlen;
      const RSPayload *payload = result->payload;
      if (payload) {
        RedisModule_ReplyWithStringBuffer(ctx, payload->data, payload->len);
      } else {
        RedisModule_ReplyWithNull(ctx);
      }
    }

    if (req->flags & Search_WithSortKeys) {
      ++arrlen;
      const RSSortableValue *sortkey = result->sortKey;
      if (sortkey) {
        if (sortkey->type == RS_SORTABLE_NUM) {
          RedisModule_ReplyWithDouble(ctx, sortkey->num);
        } else {
          // RS_SORTABLE_NIL, RS_SORTABLE_STR
          RedisModule_ReplyWithStringBuffer(ctx, sortkey->str, strlen(sortkey->str));
        }
      } else {
        RedisModule_ReplyWithNull(ctx);
      }
    }

    if (withDocs) {
      ++arrlen;

      if (!req->fields.numFields) {
        sendFullFields(&doc, ctx);
      } else {
        sendDocumentFields(req, sctx, &doc, result);
      }

      if (rkey) {
        RedisModule_CloseKey(rkey);
      }
      Document_Free(&doc);
    }
  }

  RedisModule_ReplySetArrayLength(ctx, arrlen);

  return REDISMODULE_OK;
}
#endif
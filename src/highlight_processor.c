#include "result_processor.h"
#include "fragmenter.h"
#include "value.h"
#include "util/minmax.h"
#include <ctype.h>

static int fragmentizeOffsets(const RSSearchRequest *req, const char *fieldName,
                              const char *fieldText, size_t fieldLen, RSIndexResult *indexResult,
                              RSByteOffsets *byteOffsets, FragmentList *fragList) {
  const FieldSpec *fs = IndexSpec_GetField(req->sctx->spec, fieldName, strlen(fieldName));
  if (!fs) {
    return 0;
  }

  RSOffsetIterator offsIter = RSIndexResult_IterateOffsets(indexResult);
  FragmentTermIterator fragIter;
  RSByteOffsetIterator bytesIter;
  if (RSByteOffset_Iterate(byteOffsets, fs->id, &bytesIter) != REDISMODULE_OK) {
    return 0;
  }

  FragmentTermIterator_InitOffsets(&fragIter, &bytesIter, &offsIter);
  FragmentList_FragmentizeIter(fragList, fieldText, fieldLen, &fragIter);
  return 1;
}

#define HL_SEP_STR "... "

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

static void summarizeField(const RSSearchRequest *req, const ReturnedField *fieldInfo,
                           const char *fieldName, RSValue *returnedField, SearchResult *r) {

  FragmentList frags;
  FragmentList_Init(&frags, 8, 6);

  // Start gathering the terms
  HighlightTags tags = {.openTag = fieldInfo->openTag, .closeTag = fieldInfo->closeTag};
  if (!tags.openTag) {
    tags.openTag = "";
  }
  if (!tags.closeTag) {
    tags.closeTag = "";
  }

  // First actually generate the fragments
  const char *docStr;
  size_t docLen;
  docStr = RedisModule_StringPtrLen(returnedField->rstrval, &docLen);
  if (!fragmentizeOffsets(req, fieldName, docStr, docLen, r->indexResult, r->md->byteOffsets,
                          &frags)) {
    // Should we fragmentize on the fly? TODO
    RSFieldMap_Set(&r->fields, fieldName, RS_NullVal());
    return;
  }

  if (fieldInfo->mode == SummarizeMode_WholeField) {
    // Simplest. Just send entire doc
    char *hlDoc = FragmentList_HighlightWholeDocS(&frags, &tags);
    RSFieldMap_Set(&r->fields, fieldName, RS_CStringVal(hlDoc));
    FragmentList_Free(&frags);
    return;
  }

  Array *iovsArr;
  size_t numIovArr = Min(fieldInfo->numFrags, FragmentList_GetNumFrags(&frags));

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
    Array_Write(&bufTmp, HL_SEP_STR, sizeof(HL_SEP_STR) - 1);
  }

  // Set the string value to the contents of the array
  RSFieldMap_Set(&r->fields, fieldName, RS_StringVal(strndup(bufTmp.data, bufTmp.len), bufTmp.len));

  Array_Free(&bufTmp);
  for (size_t ii = 0; ii < numIovArr; ++ii) {
    Array_Free(iovsArr + ii);
  }
  free(iovsArr);
  FragmentList_Free(&frags);
}

static int hlp_Next(ResultProcessorCtx *ctx, SearchResult *r) {
  int rc = ResultProcessor_Next(ctx->upstream, r, 0);
  if (rc == RS_RESULT_EOF) {
    return rc;
  }

  RSSearchRequest *req = ctx->privdata;
  RSByteOffsets *byteOffsets = r->md->byteOffsets;

  // Assume we have highlighting here
  for (size_t ii = 0; ii < req->fields.numFields; ++ii) {
    if (req->fields.fields[ii].mode == SummarizeMode_None) {
      // Ignore!
      continue;
    }

    // Otherwise, summarize this field now
    const ReturnedField *rf = req->fields.fields + ii;
    const char *fName = req->fields.rawFields[rf->nameIndex];
    RSValue *fieldValue = RSFieldMap_Get(r->fields, fName);

    // Check if we have the value for this field.
    // TODO: Might this be 'RSValue_String' later on?
    if (fieldValue == NULL || fieldValue->t != RSValue_RedisString) {
      RSFieldMap_Set(&r->fields, fName, RS_NullVal());
      continue;
    }

    summarizeField(req, rf, fName, fieldValue, r);
  }
  return RS_RESULT_OK;
}

static void hlp_Free(ResultProcessor *rp) {
  free(rp);
}

ResultProcessor *NewHighlightProcessor(ResultProcessor *parent, RSSearchRequest *req) {
  ResultProcessor *rp = NewResultProcessor(parent, req);
  rp->Next = hlp_Next;
  rp->Free = hlp_Free;
  return rp;
}
#include "result_processor.h"
#include "fragmenter.h"
#include "value.h"
#include "util/minmax.h"
#include <ctype.h>

/**
 * Attempts to fragmentize a single field from its offset entries. This takes
 * the field name, gets the matching field ID, retrieves the offset iterator
 * for the field ID, and fragments the text based on the offsets. The fragmenter
 * itself is in fragmenter.{c,h}
 *
 * Returns true if the fragmentation succeeded, false otherwise.
 */
static int fragmentizeOffsets(IndexSpec *spec, const char *fieldName, const char *fieldText,
                              size_t fieldLen, RSIndexResult *indexResult,
                              RSByteOffsets *byteOffsets, FragmentList *fragList) {
  const FieldSpec *fs = IndexSpec_GetField(spec, fieldName, strlen(fieldName));
  if (!fs) {
    return 0;
  }

  int rc = 0;
  RSOffsetIterator offsIter = RSIndexResult_IterateOffsets(indexResult);
  FragmentTermIterator fragIter;
  RSByteOffsetIterator bytesIter;
  if (RSByteOffset_Iterate(byteOffsets, fs->id, &bytesIter) != REDISMODULE_OK) {
    goto done;
  }

  FragmentTermIterator_InitOffsets(&fragIter, &bytesIter, &offsIter);
  FragmentList_FragmentizeIter(fragList, fieldText, fieldLen, &fragIter);
  rc = 1;

done:
  offsIter.Free(offsIter.ctx);
  return rc;
}

#define HL_SEP_STR "... "

// Strip spaces from a buffer in place. Returns the new length of the text,
// with all duplicate spaces stripped and converted to a single ' '.
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

static void summarizeField(IndexSpec *spec, const ReturnedField *fieldInfo, const char *fieldName,
                           RSValue *returnedField, SearchResult *r) {

  FragmentList frags;
  FragmentList_Init(&frags, 8, 6);

  // Start gathering the terms
  HighlightTags tags = {.openTag = fieldInfo->openTag, .closeTag = fieldInfo->closeTag};

  // First actually generate the fragments
  size_t docLen;
  const char *docStr = RSValue_StringPtrLen(returnedField, &docLen);
  if (!fragmentizeOffsets(spec, fieldName, docStr, docLen, r->indexResult, r->md->byteOffsets,
                          &frags)) {
    // Can't fragmentize from the offsets
    // Should we fragmentize on the fly? TODO
    RSFieldMap_Set(&r->fields, fieldName, RS_NullVal());
    return;
  }

  if (fieldInfo->mode == SummarizeMode_WholeField) {
    // No need to return snippets; just return the entire doc with relevant tags
    // highlighted.
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
      // TODO: Now that we're no longer returning a nested array, is it still
      // necessary to distinguish between RelOrder and Synopsis?
      order = HIGHLIGHT_ORDER_SCOREPOS;
      break;

    case SummarizeMode_WholeField:
    case SummarizeMode_None:
    default:
      // Should never reach here.
      order = -1;
      break;
  }

  // Storage for the list of fragments
  iovsArr = calloc(numIovArr, sizeof(*iovsArr));

  FragmentList_HighlightFragments(&frags, &tags, fieldInfo->contextLen, iovsArr, numIovArr, order);

  // Buffer to store concatenated fragments
  Array bufTmp;
  Array_InitEx(&bufTmp, ArrayAlloc_LibC);

  for (size_t ii = 0; ii < numIovArr; ++ii) {
    Array *curIovs = iovsArr + ii;
    struct iovec *iovs = ARRAY_GETARRAY_AS(curIovs, struct iovec *);
    size_t numIovs = ARRAY_GETSIZE_AS(curIovs, struct iovec);
    size_t lastSize = bufTmp.len;

    for (size_t jj = 0; jj < numIovs; ++jj) {
      Array_Write(&bufTmp, iovs[jj].iov_base, iovs[jj].iov_len);
    }

    // Duplicate spaces for the current snippet are eliminated here. We shouldn't
    // move it to the end because the delimiter itself may contain a special kind
    // of whitespace.
    size_t newSize = stripDuplicateSpaces(bufTmp.data + lastSize, bufTmp.len - lastSize);
    Array_Resize(&bufTmp, lastSize + newSize);
    Array_Write(&bufTmp, HL_SEP_STR, sizeof(HL_SEP_STR) - 1);
  }

  // Set the string value to the contents of the array. It might be nice if we didn't
  // need to strndup it.
  size_t hlLen;
  char *hlText = Array_Steal(&bufTmp, &hlLen);
  RSFieldMap_Set(&r->fields, fieldName, RS_StringVal(hlText, hlLen));

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

  const FieldList *fields = ctx->privdata;
  RSByteOffsets *byteOffsets = r->md->byteOffsets;

  // Assume we have highlighting here
  for (size_t ii = 0; ii < fields->numFields; ++ii) {
    if (fields->fields[ii].mode == SummarizeMode_None) {
      // Ignore - this is a field for `RETURN`, not `SUMMARIZE`
      continue;
    }

    // Otherwise, summarize this field now
    const ReturnedField *rf = fields->fields + ii;
    const char *fName = fields->rawFields[rf->nameIndex];
    RSValue *fieldValue = RSFieldMap_Get(r->fields, fName);

    // Check if we have the value for this field.
    // TODO: Might this be 'RSValue_String' later on?
    if (fieldValue == NULL || !RSValue_IsString(fieldValue)) {
      RSFieldMap_Set(&r->fields, fName, RS_NullVal());
      continue;
    }

    summarizeField(RP_SPEC(ctx), rf, fName, fieldValue, r);
  }
  return RS_RESULT_OK;
}

ResultProcessor *NewHighlightProcessor(ResultProcessor *parent, RSSearchRequest *req) {
  ResultProcessor *rp = NewResultProcessor(parent, &req->fields);
  rp->Next = hlp_Next;
  return rp;
}
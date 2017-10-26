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

static void normalizeSettings(const char *name, const ReturnedField *srcField,
                              const ReturnedField *defaults, ReturnedField *out) {
  if (srcField == NULL) {
    // Global setting
    *out = *defaults;
    out->name = (char *)name;
    return;
  }

  // Otherwise it gets more complex
  if ((defaults->mode & SummarizeMode_Highlight) &&
      (srcField->mode & SummarizeMode_Highlight) == 0) {
    out->highlightSettings = defaults->highlightSettings;
  } else if (srcField->mode && SummarizeMode_Highlight) {
    out->highlightSettings = srcField->highlightSettings;
  }

  if ((defaults->mode & SummarizeMode_Synopsis) && (srcField->mode & SummarizeMode_Synopsis) == 0) {
    out->summarizeSettings = defaults->summarizeSettings;
  } else {
    out->summarizeSettings = srcField->summarizeSettings;
  }

  out->mode |= defaults->mode | srcField->mode;
  out->name = (char *)name;
}

static void summarizeField(IndexSpec *spec, const ReturnedField *fieldInfo, const char *fieldName,
                           RSValue *returnedField, SearchResult *r) {

  FragmentList frags;
  FragmentList_Init(&frags, 8, 6);

  // Start gathering the terms
  HighlightTags tags = {.openTag = fieldInfo->highlightSettings.openTag,
                        .closeTag = fieldInfo->highlightSettings.closeTag};

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

  // Highlight only
  if (fieldInfo->mode == SummarizeMode_Highlight) {
    // No need to return snippets; just return the entire doc with relevant tags
    // highlighted.
    char *hlDoc = FragmentList_HighlightWholeDocS(&frags, &tags);
    RSFieldMap_Set(&r->fields, fieldName, RS_CStringVal(hlDoc));
    FragmentList_Free(&frags);
    return;
  }

  Array *iovsArr;
  size_t numIovArr = Min(fieldInfo->summarizeSettings.numFrags, FragmentList_GetNumFrags(&frags));

  // Storage for the list of fragments
  iovsArr = calloc(numIovArr, sizeof(*iovsArr));
  FragmentList_HighlightFragments(&frags, &tags, fieldInfo->summarizeSettings.contextLen, iovsArr,
                                  numIovArr, HIGHLIGHT_ORDER_SCOREPOS);

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
    Array_Write(&bufTmp, fieldInfo->summarizeSettings.separator,
                strlen(fieldInfo->summarizeSettings.separator));
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

static void processField(ResultProcessorCtx *ctx, SearchResult *r, ReturnedField *spec) {
  const char *fName = spec->name;
  RSValue *fieldValue = RSFieldMap_Get(r->fields, fName);

  // Check if we have the value for this field.
  // TODO: Might this be 'RSValue_String' later on?
  if (fieldValue == NULL || !RSValue_IsString(fieldValue)) {
    RSFieldMap_Set(&r->fields, fName, RS_NullVal());
    return;
  }

  summarizeField(RP_SPEC(ctx), spec, fName, fieldValue, r);
}

static int hlp_Next(ResultProcessorCtx *ctx, SearchResult *r) {
  int rc = ResultProcessor_Next(ctx->upstream, r, 0);
  if (rc == RS_RESULT_EOF) {
    return rc;
  }

  const FieldList *fields = ctx->privdata;
  RSByteOffsets *byteOffsets = r->md->byteOffsets;
  if (fields->numFields) {
    for (size_t ii = 0; ii < fields->numFields; ++ii) {
      if (fields->fields[ii].mode == SummarizeMode_None &&
          fields->defaultField.mode == SummarizeMode_None) {
        // Ignore - this is a field for `RETURN`, not `SUMMARIZE`
        continue;
      } else {
        ReturnedField combinedSpec = {0};
        normalizeSettings(fields->fields[ii].name, fields->fields + ii, &fields->defaultField,
                          &combinedSpec);
        processField(ctx, r, &combinedSpec);
      }
    }
  } else if (fields->defaultField.mode != SummarizeMode_None) {
    for (size_t ii = 0; ii < r->fields->len; ++ii) {
      ReturnedField spec = {0};
      normalizeSettings(r->fields->fields[ii].key, NULL, &fields->defaultField, &spec);
      processField(ctx, r, &spec);
    }
  }
  return RS_RESULT_OK;
}

ResultProcessor *NewHighlightProcessor(ResultProcessor *parent, RSSearchRequest *req) {
  ResultProcessor *rp = NewResultProcessor(parent, &req->fields);
  rp->Next = hlp_Next;
  return rp;
}
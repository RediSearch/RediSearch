/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "rmutil/util.h"
#include "rmutil/strings.h"
#include "rmutil/args.h"
#include "util/array.h"
#include "search_options.h"

/**
 * HIGHLIGHT [FIELDS {num} {field}…] [TAGS {open} {close}]
 * SUMMARISE [FIELDS {num} {field} …] [LEN {len}] [FRAGS {num}]
 */

static int parseFieldList(ArgsCursor *ac, FieldList *fields, Array *fieldPtrs) {
  ArgsCursor fieldArgs = {0};
  if (AC_GetVarArgs(ac, &fieldArgs) != AC_OK) {
    return -1;
  }

  while (!AC_IsAtEnd(&fieldArgs)) {
    HiddenString *name = AC_GetHiddenString(&fieldArgs);
    ReturnedField *fieldInfo = FieldList_GetCreateField(fields, name, NULL);
    size_t ix = (fieldInfo - fields->fields);
    Array_Write(fieldPtrs, &ix, sizeof(size_t));
  }

  return 0;
}

static void setHighlightSettings(HighlightSettings *tgt, const HighlightSettings *defaults) {
  rm_free(tgt->closeTag);
  rm_free(tgt->openTag);

  tgt->closeTag = NULL;
  tgt->openTag = NULL;
  if (defaults->openTag) {
    tgt->openTag = HiddenString_Retain(defaults->openTag);
  }
  if (defaults->closeTag) {
    tgt->closeTag = HiddenString_Retain(defaults->closeTag);
  }
}

static void setSummarizeSettings(SummarizeSettings *tgt, const SummarizeSettings *defaults) {
  *tgt = *defaults;
  if (tgt->separator) {
    HiddenString_TakeOwnership(tgt->separator);
    HiddenString_Retain(tgt->separator);
  }
}

static void setFieldSettings(ReturnedField *tgt, const ReturnedField *defaults, int isHighlight) {
  if (isHighlight) {
    setHighlightSettings(&tgt->highlightSettings, &defaults->highlightSettings);
    tgt->mode |= SummarizeMode_Highlight;
  } else {
    setSummarizeSettings(&tgt->summarizeSettings, &defaults->summarizeSettings);
    tgt->mode |= SummarizeMode_Synopsis;
  }
}

static int parseCommon(ArgsCursor *ac, FieldList *fields, int isHighlight) {
  size_t numFields = 0;
  int rc = REDISMODULE_OK;

  ReturnedField defOpts = {.summarizeSettings = {.contextLen = SUMMARIZE_FRAGSIZE_DEFAULT,
                                                 .numFrags = SUMMARIZE_FRAGCOUNT_DEFAULT,
                                                 .separator = NewHiddenString(SUMMARIZE_DEFAULT_SEPARATOR, strlen(SUMMARIZE_DEFAULT_SEPARATOR), false)},
                           .highlightSettings = {.openTag = NewHiddenString(SUMMARIZE_DEFAULT_OPEN_TAG, strlen(SUMMARIZE_DEFAULT_OPEN_TAG), false),
                                                 .closeTag = NewHiddenString(SUMMARIZE_DEFAULT_CLOSE_TAG, strlen(SUMMARIZE_DEFAULT_CLOSE_TAG), false)}};

  Array fieldPtrs;
  Array_Init(&fieldPtrs);

  if (AC_AdvanceIfMatch(ac, "FIELDS")) {
    if (parseFieldList(ac, fields, &fieldPtrs) != 0) {
      rc = REDISMODULE_ERR;
      goto done;
    }
  }

  while (!AC_IsAtEnd(ac)) {
    if (isHighlight && AC_AdvanceIfMatch(ac, "TAGS")) {
      // Open tag, close tag
      if (AC_NumRemaining(ac) < 2) {
        rc = REDISMODULE_ERR;
        goto done;
      }
      defOpts.highlightSettings.openTag = AC_GetHiddenString(ac);
      defOpts.highlightSettings.closeTag = AC_GetHiddenString(ac);
    } else if (!isHighlight && AC_AdvanceIfMatch(ac, "LEN")) {
      if (AC_GetUnsigned(ac, &defOpts.summarizeSettings.contextLen, 0) != AC_OK) {
        rc = REDISMODULE_ERR;
        goto done;
      }
    } else if (!isHighlight && AC_AdvanceIfMatch(ac, "FRAGS")) {
      unsigned tmp;
      if (AC_GetUnsigned(ac, &tmp, 0) != AC_OK) {
        rc = REDISMODULE_ERR;
        goto done;
      }
      defOpts.summarizeSettings.numFrags = tmp;
    } else if (!isHighlight && AC_AdvanceIfMatch(ac, "SEPARATOR")) {
      defOpts.summarizeSettings.separator = AC_GetHiddenString(ac);
      if (!defOpts.summarizeSettings.separator) {
        rc = REDISMODULE_ERR;
        goto done;
      }
    } else {
      break;
    }
  }

  if (fieldPtrs.len) {
    size_t numNewPtrs = ARRAY_GETSIZE_AS(&fieldPtrs, size_t);
    for (size_t ii = 0; ii < numNewPtrs; ++ii) {
      size_t ix = ARRAY_GETARRAY_AS(&fieldPtrs, size_t *)[ii];
      ReturnedField *fieldInfo = fields->fields + ix;
      setFieldSettings(fieldInfo, &defOpts, isHighlight);
    }
  } else {
    setFieldSettings(&fields->defaultField, &defOpts, isHighlight);
  }

done:
  Array_Free(&fieldPtrs);
  if (defOpts.highlightSettings.openTag) {
    HiddenString_Free(defOpts.highlightSettings.openTag);
  }
  if (defOpts.highlightSettings.closeTag) {
    HiddenString_Free(defOpts.highlightSettings.closeTag);
  }
  if (defOpts.summarizeSettings.separator) {
    HiddenString_Free(defOpts.summarizeSettings.separator);
  }
  return rc;
}

int ParseSummarize(ArgsCursor *ac, FieldList *fields) {
  return parseCommon(ac, fields, 0);
}

int ParseHighlight(ArgsCursor *ac, FieldList *fields) {
  return parseCommon(ac, fields, 1);
}

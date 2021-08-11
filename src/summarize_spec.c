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
    const char *name = AC_GetStringNC(&fieldArgs, NULL);
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
    tgt->openTag = rm_strdup(defaults->openTag);
  }
  if (defaults->closeTag) {
    tgt->closeTag = rm_strdup(defaults->closeTag);
  }
}

static void setSummarizeSettings(SummarizeSettings *tgt, const SummarizeSettings *defaults) {
  *tgt = *defaults;
  if (tgt->separator) {
    tgt->separator = rm_strdup(tgt->separator);
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
                                                 .separator = SUMMARIZE_DEFAULT_SEPARATOR},
                           .highlightSettings = {.openTag = SUMMARIZE_DEFAULT_OPEN_TAG,
                                                 .closeTag = SUMMARIZE_DEFAULT_CLOSE_TAG}};

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
      defOpts.highlightSettings.openTag = (char *)AC_GetStringNC(ac, NULL);
      defOpts.highlightSettings.closeTag = (char *)AC_GetStringNC(ac, NULL);
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
      if (AC_GetString(ac, (const char **)&defOpts.summarizeSettings.separator, NULL, 0) != AC_OK) {
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
  return rc;
}

int ParseSummarize(ArgsCursor *ac, FieldList *fields) {
  return parseCommon(ac, fields, 0);
}

int ParseHighlight(ArgsCursor *ac, FieldList *fields) {
  return parseCommon(ac, fields, 1);
}

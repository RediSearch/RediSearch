#include "summarize_spec.h"
#include "search_request.h"
#include "rmutil/util.h"
#include "rmutil/strings.h"
#include "util/array.h"

/**
 * HIGHLIGHT [FIELDS {num} {field}…] [TAGS {open} {close}]
 * SUMMARISE [FIELDS {num} {field} …] [LEN {len}] [FRAGS {num}]
*/

static int parseTags(RedisModuleString **argv, int argc, size_t *offset, const char **open,
                     const char **close) {
  if (argc - *offset < 3) {
    return REDISMODULE_ERR;
  }

  ++*offset;

  RMUtil_ParseArgs(argv, argc, *offset, "cc", open, close);
  *offset += 2;
  return REDISMODULE_OK;
}

static int parseSeparator(RedisModuleString **argv, int argc, size_t *offset, const char **sep) {
  if (argc - *offset < 2) {
    return REDISMODULE_ERR;
  }
  ++*offset;
  *sep = RedisModule_StringPtrLen(argv[*offset], NULL);
  ++*offset;
  return REDISMODULE_OK;
}

static int parseFragLen(RedisModuleString **argv, int argc, size_t *offset, uint32_t *fragSize) {
  if (argc - *offset < 2) {
    return REDISMODULE_ERR;
  }
  ++*offset;
  long long tmp;
  if (RMUtil_ParseArgs(argv, argc, *offset, "l", &tmp) != REDISMODULE_OK) {
    return REDISMODULE_ERR;
  }
  *fragSize = tmp;
  ++*offset;
  return REDISMODULE_OK;
}

static int parseNumFrags(RedisModuleString **argv, int argc, size_t *offset, uint16_t *numFrags) {
  uint32_t numP;
  int rv = parseFragLen(argv, argc, offset, &numP);
  if (rv == 0) {
    *numFrags = numP;
  }
  return rv;
}

static int parseFieldList(RedisModuleString **argv, int argc, size_t *offset, FieldList *fields,
                          Array *fieldPtrs) {
  ++*offset;
  if (*offset == argc) {
    return -1;
  }
  long long numFields = 0;
  if (RedisModule_StringToLongLong(argv[*offset], &numFields) != REDISMODULE_OK) {
    return -1;
  }

  ++*offset;
  if (argc - *offset < numFields) {
    return -1;
  }

  for (size_t ii = 0; ii < numFields; ++ii) {
    ReturnedField *fieldInfo = FieldList_GetCreateField(fields, argv[*offset + ii]);
    size_t ix = (fieldInfo - fields->fields);
    Array_Write(fieldPtrs, &ix, sizeof(size_t));
  }
  *offset += numFields;
  return 0;
}

static void setHighlightSettings(HighlightSettings *tgt, const HighlightSettings *defaults) {
  free(tgt->closeTag);
  free(tgt->openTag);

  tgt->closeTag = NULL;
  tgt->openTag = NULL;
  if (defaults->openTag) {
    tgt->openTag = strdup(defaults->openTag);
  }
  if (defaults->closeTag) {
    tgt->closeTag = strdup(defaults->closeTag);
  }
}

static void setSummarizeSettings(SummarizeSettings *tgt, const SummarizeSettings *defaults) {
  *tgt = *defaults;
  if (tgt->separator) {
    tgt->separator = strdup(tgt->separator);
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

static int parseCommon(RedisModuleString **argv, int argc, size_t *offset, FieldList *fields,
                       int isHighlight) {
  size_t numFields = 0;
  int rc = REDISMODULE_OK;

  ReturnedField defOpts = {.summarizeSettings = {.contextLen = SUMMARIZE_FRAGSIZE_DEFAULT,
                                                 .numFrags = SUMMARIZE_FRAGCOUNT_DEFAULT,
                                                 .separator = SUMMARIZE_DEFAULT_SEPARATOR},
                           .highlightSettings = {.openTag = SUMMARIZE_DEFAULT_OPEN_TAG,
                                                 .closeTag = SUMMARIZE_DEFAULT_CLOSE_TAG}};

  Array fieldPtrs;
  ++*offset;
  Array_Init(&fieldPtrs);

  if (*offset == argc) {
    goto ok;
  }

  if (RMUtil_StringEqualsCaseC(argv[*offset], "FIELDS")) {
    if (parseFieldList(argv, argc, offset, fields, &fieldPtrs) != 0) {
      rc = -1;
      goto done;
    }
  }

  while (*offset != argc) {
    if (isHighlight && RMUtil_StringEqualsCaseC(argv[*offset], "TAGS")) {
      if (parseTags(argv, argc, offset, (const char **)&defOpts.highlightSettings.openTag,
                    (const char **)&defOpts.highlightSettings.closeTag) != 0) {
        rc = REDISMODULE_ERR;
        goto done;
      }
    } else if (!isHighlight && RMUtil_StringEqualsCaseC(argv[*offset], "LEN")) {
      if (parseFragLen(argv, argc, offset, &defOpts.summarizeSettings.contextLen) != 0) {
        rc = REDISMODULE_ERR;
        goto done;
      }
    } else if (!isHighlight && RMUtil_StringEqualsCaseC(argv[*offset], "FRAGS")) {
      if (parseNumFrags(argv, argc, offset, &defOpts.summarizeSettings.numFrags) != 0) {
        rc = REDISMODULE_ERR;
        goto done;
      }
    } else if (!isHighlight && RMUtil_StringEqualsCaseC(argv[*offset], "SEPARATOR")) {
      if (parseSeparator(argv, argc, offset, (const char **)&defOpts.summarizeSettings.separator) !=
          0) {
        rc = REDISMODULE_ERR;
        goto done;
      }
    } else {
      break;
    }
  }

ok:
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
  fields->wantSummaries = 1;

done:
  Array_Free(&fieldPtrs);
  return rc;
}

int ParseSummarize(RedisModuleString **argv, int argc, size_t *offset, FieldList *fields) {
  return parseCommon(argv, argc, offset, fields, 0);
}

int ParseHighlight(RedisModuleString **argv, int argc, size_t *offset, FieldList *fields) {
  return parseCommon(argv, argc, offset, fields, 1);
}
#include "search_options.h"

#include "rmutil/util.h"
#include "rmutil/strings.h"
#include "rmutil/args.h"
#include "util/array.h"

///////////////////////////////////////////////////////////////////////////////////////////////

/**
 * HIGHLIGHT [FIELDS {num} {field}…] [TAGS {open} {close}]
 * SUMMARISE [FIELDS {num} {field} …] [LEN {len}] [FRAGS {num}]
 */

int FieldList::parseFieldList(ArgsCursor *ac, Array<size_t> *fieldPtrs) {
  ArgsCursor fieldArgs;
  if (ac->GetVarArgs(&fieldArgs) != AC_OK) {
    return -1;
  }

  while (!fieldArgs.IsAtEnd()) {
    const char *name = fieldArgs.GetStringNC(NULL);
    ReturnedField *fieldInfo = GetCreateField(name);
    size_t ix = fieldInfo - fields;
    fieldPtrs.Write(&ix, sizeof(size_t));
  }

  return 0;
}

//---------------------------------------------------------------------------------------------

void HighlightSettings::setHighlightSettings(const HighlightSettings *defaults) {
  rm_free(closeTag);
  rm_free(openTag);

  closeTag = NULL;
  openTag = NULL;
  if (defaults->openTag) {
    openTag = rm_strdup(defaults->openTag);
  }
  if (defaults->closeTag) {
    closeTag = rm_strdup(defaults->closeTag);
  }
}

//---------------------------------------------------------------------------------------------

void SummarizeSettings::setSummarizeSettings(const SummarizeSettings *defaults) {
  *this = *defaults;
  if (separator) {
    separator = rm_strdup(separator);
  }
}

//---------------------------------------------------------------------------------------------

void ReturnedField::setFieldSettings(const ReturnedField *defaults, int isHighlight) {
  if (isHighlight) {
    highlightSettings.setHighlightSettings(&defaults->highlightSettings);
    mode |= SummarizeMode_Highlight;
  } else {
    summarizeSettings.setSummarizeSettings(&defaults->summarizeSettings);
    mode |= SummarizeMode_Synopsis;
  }
}

//---------------------------------------------------------------------------------------------

int FieldList::parseArgs(ArgsCursor *ac, bool isHighlight) {
  size_t numFields = 0;
  int rc = REDISMODULE_OK;

  ReturnedField defOpts;
  Array<size_t> fieldPtrs;

  if (ac->AdvanceIfMatch("FIELDS")) {
    if (parseFieldList(ac, &fieldPtrs) != 0) {
      rc = REDISMODULE_ERR;
      goto done;
    }
  }

  while (!ac->IsAtEnd()) {
    if (isHighlight && ac->AdvanceIfMatch("TAGS")) {
      // Open tag, close tag
      if (ac->NumRemaining() < 2) {
        rc = REDISMODULE_ERR;
        goto done;
      }
      defOpts.highlightSettings.openTag = (char *)ac->GetStringNC(NULL);
      defOpts.highlightSettings.closeTag = (char *)ac->GetStringNC(NULL);
    } else if (!isHighlight && ac->AdvanceIfMatch("LEN")) {
      if (ac->GetUnsigned( &defOpts.summarizeSettings.contextLen, 0) != AC_OK) {
        rc = REDISMODULE_ERR;
        goto done;
      }
    } else if (!isHighlight && ac->AdvanceIfMatch("FRAGS")) {
      unsigned tmp;
      if (ac->GetUnsigned( &tmp, 0) != AC_OK) {
        rc = REDISMODULE_ERR;
        goto done;
      }
      defOpts.summarizeSettings.numFrags = tmp;
    } else if (!isHighlight && ac->AdvanceIfMatch("SEPARATOR")) {
      if (ac->GetString((const char **)&defOpts.summarizeSettings.separator, NULL, 0) != AC_OK) {
        rc = REDISMODULE_ERR;
        goto done;
      }
    } else {
      break;
    }
  }

  if (fieldPtrs.len) {
    size_t numNewPtrs = fieldPtrs.ARRAY_GETSIZE_AS();
    for (size_t ii = 0; ii < numNewPtrs; ++ii) {
      size_t ix = fieldPtrs.ARRAY_GETARRAY_AS()[ii];
      ReturnedField fieldInfo = fields[ix];
      fieldInfo.setFieldSettings(&defOpts, isHighlight);
    }
  } else {
    defaultField.setFieldSettings(&defOpts, isHighlight);
  }

done:
  return rc;
}

//---------------------------------------------------------------------------------------------

void FieldList::ParseSummarize(ArgsCursor *ac) {
  if (parseArgs(ac, false) == REDISMODULE_ERR) {
    throw Error("Bad arguments for SUMMARIZE");
  }
}

//---------------------------------------------------------------------------------------------

void FieldList::ParseHighlight(ArgsCursor *ac) {
  if (parseArgs(ac, true) == REDISMODULE_ERR) {
    throw Error("Bad arguments for HIGHLIGHT");
  }
}

///////////////////////////////////////////////////////////////////////////////////////////////

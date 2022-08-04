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

bool FieldList::parseFieldList(ArgsCursor *ac, Vector<size_t> fieldPtrs) {
  ArgsCursor fieldArgs;
  if (ac->GetVarArgs(&fieldArgs) != AC_OK) {
    return false;
  }

  while (!fieldArgs.IsAtEnd()) {
    const char *name = fieldArgs.GetStringNC(NULL);
    ReturnedField *fieldInfo = &GetCreateField(name);
    // size_t ix = fieldInfo - fields;
    // fieldPtrs.push_back(ix);
    fieldPtrs.push_back(fieldInfo);
  }

  return true;
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
  Vector<size_t> fieldPtrs;

  if (ac->AdvanceIfMatch("FIELDS")) {
    if (!parseFieldList(ac, fieldPtrs)) {
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

  if (!fieldPtrs.empty()) {
    for (size_t i = 0; i < fieldPtrs.size(); ++i) {
      size_t ix = fieldPtrs[i];
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

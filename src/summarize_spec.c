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

bool FieldList::parseFields(ArgsCursor *ac, Vector<ReturnedField> &retFields) {
  ArgsCursor fieldArgs;
  if (ac->GetVarArgs(&fieldArgs) != AC_OK) {
    return false;
  }

  while (!fieldArgs.IsAtEnd()) {
    const char *name = fieldArgs.GetStringNC(NULL);
    ReturnedField &field = CreateField(name);
    retFields.push_back(field);
  }

  return true;
}

//---------------------------------------------------------------------------------------------

HighlightSettings &HighlightSettings::operator=(const HighlightSettings &settings) {
  openTag = settings.openTag;
  closeTag = settings.closeTag;
  return *this;
}

//---------------------------------------------------------------------------------------------

SummarizeSettings &SummarizeSettings::operator=(const SummarizeSettings &settings) {
  contextLen = settings.contextLen;
  numFrags = settings.numFrags;
  separator = settings.separator;
  return *this;
}

//---------------------------------------------------------------------------------------------

void ReturnedField::set(const ReturnedField &field, bool isHighlight) {
  if (isHighlight) {
    highlightSettings = field.highlightSettings;
    mode |= SummarizeMode_Highlight;
  } else {
    summarizeSettings = field.summarizeSettings;
    mode |= SummarizeMode_Synopsis;
  }
}

//---------------------------------------------------------------------------------------------

int FieldList::parseArgs(ArgsCursor *ac, bool isHighlight) {
  if (ac->AdvanceIfMatch("FIELDS")) {
    if (!parseFields(ac, fields)) {
      return REDISMODULE_ERR;
    }
  }

  ReturnedField defaults;
  //Vector<size_t> fields;

  while (!ac->IsAtEnd()) {
    if (isHighlight && ac->AdvanceIfMatch("TAGS")) {
      // Open tag, close tag
      if (ac->NumRemaining() < 2) {
        return REDISMODULE_ERR;
      }
      defaults.highlightSettings.openTag = (char *)ac->GetStringNC(NULL);
      defaults.highlightSettings.closeTag = (char *)ac->GetStringNC(NULL);
    } else if (!isHighlight && ac->AdvanceIfMatch("LEN")) {
      if (ac->GetUnsigned(&defaults.summarizeSettings.contextLen, 0) != AC_OK) {
        return REDISMODULE_ERR;
      }
    } else if (!isHighlight && ac->AdvanceIfMatch("FRAGS")) {
      unsigned n;
      if (ac->GetUnsigned( &n, 0) != AC_OK) {
        return REDISMODULE_ERR;
      }
      defaults.summarizeSettings.numFrags = n;
    } else if (!isHighlight && ac->AdvanceIfMatch("SEPARATOR")) {
      if (ac->GetStdString(&defaults.summarizeSettings.separator, 0) != AC_OK) {
        return REDISMODULE_ERR;
      }
    } else {
      break;
    }
  }

  if (!fields.empty()) {
    for (auto &field: fields) {
      field.set(defaults, isHighlight);
    }
  } else {
    defaultField.set(defaults, isHighlight);
  }

  return REDISMODULE_OK;
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

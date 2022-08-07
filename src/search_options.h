#pragma once

#include "stopwords.h"
#include "redisearch.h"
#include "numeric_filter.h"
#include "geo_index.h"
#include "config.h"
#include "rlookup.h"
#include "util/array.h"

///////////////////////////////////////////////////////////////////////////////////////////////

typedef Vector<Vector<iovec>> IOVecArrays;

enum SummarizeMode {
  SummarizeMode_None = 0x00, // No summaries
  SummarizeMode_Highlight = 0x01,
  SummarizeMode_Synopsis = 0x02
};

#define SUMMARIZE_MODE_DEFAULT SummarizeMode_Synopsis
#define SUMMARIZE_FRAGSIZE_DEFAULT 20
#define SUMMARIZE_FRAGCOUNT_DEFAULT 3
#define SUMMARIZE_DEFAULT_OPEN_TAG "<b>"
#define SUMMARIZE_DEFAULT_CLOSE_TAG "</b>"
#define SUMMARIZE_DEFAULT_SEPARATOR "... "

struct SummarizeSettings {
  uint32_t contextLen;
  uint16_t numFrags;
  char *separator;

  SummarizeSettings() : contextLen(SUMMARIZE_FRAGSIZE_DEFAULT),
    numFrags(SUMMARIZE_FRAGCOUNT_DEFAULT), separator(SUMMARIZE_DEFAULT_SEPARATOR) {}

  void setSummarizeSettings(const SummarizeSettings *defaults);
};

//---------------------------------------------------------------------------------------------

struct HighlightSettings {
  char *openTag;
  char *closeTag;

  HighlightSettings() : openTag(SUMMARIZE_DEFAULT_OPEN_TAG), closeTag(SUMMARIZE_DEFAULT_CLOSE_TAG) {}

  void setHighlightSettings(const HighlightSettings *defaults);
};

//---------------------------------------------------------------------------------------------

struct ReturnedField {
  const char *name;

  const RLookupKey *lookupKey; // Lookup key associated with field
  SummarizeSettings summarizeSettings;
  HighlightSettings highlightSettings;
  SummarizeMode mode;
  int explicitReturn; // Whether this field was explicitly requested by `RETURN`

  ReturnedField() : name(0), lookupKey(0), mode(SummarizeMode_None), explicitReturn(0) {}
  ~ReturnedField();

  void setFieldSettings(const ReturnedField *defaults, int isHighlight);
  char *trimField(const char *docStr, size_t *docLen, size_t estWordSize) const;
  struct ReturnedField normalizeSettings(const ReturnedField &defaults) const;
};

//---------------------------------------------------------------------------------------------

struct FieldList {
  // "Template" field. Contains settings applied to all other fields
  ReturnedField defaultField;

  // List of individual field specifications
  Vector<ReturnedField> fields;

  // Whether this list contains fields explicitly selected by `RETURN`
  uint16_t explicitReturn;

  FieldList() : explicitReturn(0) {}

  ReturnedField GetCreateField(const char *name);

  size_t NumFields() { return fields.size(); }

  int parseArgs(ArgsCursor *ac, bool isHighlight);
  bool parseFieldList(ArgsCursor *ac, Vector<size_t> fieldPtrs);
  void ParseSummarize(ArgsCursor *ac);
  void ParseHighlight(ArgsCursor *ac);

  void RestrictReturn();
};

//---------------------------------------------------------------------------------------------

enum RSSearchFlags {
  Search_Verbatim = 0x02,
  Search_NoStopwrods = 0x04,
  Search_InOrder = 0x20,
  Search_HasSlop = 0x200
};

//---------------------------------------------------------------------------------------------

#define RS_DEFAULT_QUERY_FLAGS 0x00

// maximum results you can get in one query
#define SEARCH_REQUEST_RESULTS_MAX 1000000

//---------------------------------------------------------------------------------------------

struct RSSearchOptions {
  const char *expanderName;
  const char *scorerName;
  RSLanguage language;

  uint32_t flags;
  t_fieldMask fieldmask;
  int slop;

  Vector<const char *> inkeys;

  // Keys are converted into arrays. This is done when the actual search ctx is available.
  Vector<t_docId> inids;

  //std::shared_ptr<const StopWordList> stopwords;
  StopWordList *stopwords;

  // Legacy options
  struct Legacy {
    Vector<NumericFilter*> filters;
    GeoFilter *gf;
    Vector<const char *> infields;

    Legacy() {
      for (auto filter: filters) {
        delete filter;
      }
      gf = NULL;
    }
  } legacy;

  RSSearchOptions() {
    expanderName = NULL;
    scorerName = NULL;
    language = RS_LANG_ENGLISH;
    flags = 0;
    slop = -1;
    fieldmask = RS_FIELDMASK_ALL;
  }

  ~RSSearchOptions() {}
};

///////////////////////////////////////////////////////////////////////////////////////////////

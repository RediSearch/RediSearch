#pragma once

#include "stopwords.h"
#include "redisearch.h"
#include "numeric_filter.h"
#include "geo_index.h"
#include "config.h"
#include "rlookup.h"

///////////////////////////////////////////////////////////////////////////////////////////////

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

  // Lookup key associated with field
  const RLookupKey *lookupKey;
  SummarizeSettings summarizeSettings;
  HighlightSettings highlightSettings;
  SummarizeMode mode;
  // Whether this field was explicitly requested by `RETURN`
  int explicitReturn;

  ReturnedField() : name(0), lookupKey(0), explicitReturn(0) {}
  ~ReturnedField();

  void setFieldSettings(const ReturnedField *defaults, int isHighlight);
};

//---------------------------------------------------------------------------------------------

struct FieldList {
  // "Template" field. Contains settings applied to all other fields.
  ReturnedField defaultField;

  // List of individual field specifications
  ReturnedField *fields;
  size_t numFields;

  // Whether this list contains fields explicitly selected by `RETURN`
  uint16_t explicitReturn;

  ReturnedField *GetCreateField(const char *name);
  ~FieldList();

  int ParseSummarize(ArgsCursor *ac);
  int ParseHighlight(ArgsCursor *ac);
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

  const char **inkeys;
  size_t ninkeys;

  // Keys are converted into arrays. This is done when the actual search ctx is available.
  t_docId *inids;
  size_t nids;

  std::shared_ptr<const StopWordList> stopwords;

  // Legacy options
  struct Legacy {
    NumericFilter **filters;
    GeoFilter *gf;
    const char **infields;
    size_t ninfields;

    Legacy() {
      filters = NULL;
      gf = NULL;
      infields = NULL;
      ninfields = 0;
    }
  } legacy;

  RSSearchOptions() {
    expanderName = NULL;
    scorerName = NULL;
    language = RS_LANG_ENGLISH;

    flags = 0;
    slop = -1;
    fieldmask = RS_FIELDMASK_ALL;

    nids = 0;
    inids = NULL;

    ninkeys = 0;
    inkeys = NULL;
  }
};

///////////////////////////////////////////////////////////////////////////////////////////////

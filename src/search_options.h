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

//---------------------------------------------------------------------------------------------

struct SummarizeSettings {
  uint32_t contextLen;
  uint16_t numFrags;
  String separator;

  SummarizeSettings()
    : contextLen{SUMMARIZE_FRAGSIZE_DEFAULT}
    , numFrags{SUMMARIZE_FRAGCOUNT_DEFAULT}
    , separator{SUMMARIZE_DEFAULT_SEPARATOR}
  { }

  SummarizeSettings &operator=(const SummarizeSettings &settings);
};

//---------------------------------------------------------------------------------------------

struct HighlightSettings {
  String openTag;
  String closeTag;

  HighlightSettings()
    : openTag{SUMMARIZE_DEFAULT_OPEN_TAG}
    , closeTag{SUMMARIZE_DEFAULT_CLOSE_TAG}
  { }

  HighlightSettings &operator=(const HighlightSettings &settings);
};

//---------------------------------------------------------------------------------------------

struct ReturnedField {
  String name;

  const RLookupKey *lookupKey; // Lookup key associated with field, @@TODO: ownership
  SummarizeSettings summarizeSettings;
  HighlightSettings highlightSettings;
  SummarizeMode mode;
  bool explicitReturn; // Whether this field was explicitly requested by `RETURN`

  ReturnedField(const char *name_ = "")
    : name{name_}
    , lookupKey{nullptr}
    , mode{SummarizeMode_None}
    , explicitReturn{false}
  { }
  ~ReturnedField();

  void set(const ReturnedField &field, bool isHighlight);
  char *trimField(const char *docStr, size_t *docLen, size_t estWordSize) const;
  ReturnedField normalizeSettings(const ReturnedField &defaults) const;
};

//---------------------------------------------------------------------------------------------

struct FieldList {
  // "Template" field. Contains settings applied to all other fields
  ReturnedField defaultField;

  // List of individual field specifications
  Vector<ReturnedField> fields;

  // Whether this list contains fields explicitly selected by `RETURN`
  bool explicitReturn;

  FieldList() : explicitReturn(false) {}

  ReturnedField &CreateField(const char *name);

  size_t NumFields() const { return fields.size(); }

  int parseArgs(ArgsCursor *ac, bool isHighlight);
  bool parseFields(ArgsCursor *ac, Vector<ReturnedField> &fields);
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
      gf = nullptr;
    }
  } legacy;

  RSSearchOptions() {
    expanderName = nullptr;
    scorerName = nullptr;
    language = RS_LANG_ENGLISH;
    flags = 0;
    slop = -1;
    fieldmask = RS_FIELDMASK_ALL;
  }

  ~RSSearchOptions() {}
};

///////////////////////////////////////////////////////////////////////////////////////////////

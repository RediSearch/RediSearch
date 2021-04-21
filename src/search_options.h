#ifndef RS_QUERY_OPTIONS_H_
#define RS_QUERY_OPTIONS_H_

#include <stopwords.h>
#include <redisearch.h>
#include "numeric_filter.h"
#include "geo_index.h"
#include "config.h"
#include "rlookup.h"

typedef enum {
  // No summaries
  SummarizeMode_None = 0x00,
  SummarizeMode_Highlight = 0x01,
  SummarizeMode_Synopsis = 0x02
} SummarizeMode;

#define SUMMARIZE_MODE_DEFAULT SummarizeMode_Synopsis
#define SUMMARIZE_FRAGSIZE_DEFAULT 20
#define SUMMARIZE_FRAGCOUNT_DEFAULT 3
#define SUMMARIZE_DEFAULT_OPEN_TAG "<b>"
#define SUMMARIZE_DEFAULT_CLOSE_TAG "</b>"
#define SUMMARIZE_DEFAULT_SEPARATOR "... "

typedef struct {
  uint32_t contextLen;
  uint16_t numFrags;
  char *separator;
} SummarizeSettings;

typedef struct {
  char *openTag;
  char *closeTag;
} HighlightSettings;

typedef struct {
  // path AS name
  const char *path;
  const char *name;

  /* Lookup key associated with field */
  const RLookupKey *lookupKey;
  SummarizeSettings summarizeSettings;
  HighlightSettings highlightSettings;
  SummarizeMode mode;
  // Whether this field was explicitly requested by `RETURN`
  int explicitReturn;
} ReturnedField;

typedef struct {
  // "Template" field. This contains settings applied to all other fields
  ReturnedField defaultField;

  // List of individual field specifications
  ReturnedField *fields;
  size_t numFields;

  // Whether this list contains fields explicitly selected by `RETURN`
  uint16_t explicitReturn;
} FieldList;

// "path AS name"
// If `path` is NULL then `path` = `name`
ReturnedField *FieldList_GetCreateField(FieldList *fields, const char *name, const char *path);
void FieldList_Free(FieldList *fields);

int ParseSummarize(ArgsCursor *ac, FieldList *fields);
int ParseHighlight(ArgsCursor *ac, FieldList *fields);

typedef enum {
  Search_Verbatim = 0x02,
  Search_NoStopwrods = 0x04,
  Search_InOrder = 0x20,
  Search_HasSlop = 0x200
} RSSearchFlags;

#define RS_DEFAULT_QUERY_FLAGS 0x00

typedef struct {
  const char *expanderName;
  const char *scorerName;
  RSLanguage language;

  uint32_t flags;
  t_fieldMask fieldmask;
  int slop;

  const char **inkeys;
  size_t ninkeys;

  // Keys are converted into arrays. This is done when the actual
  // search ctx is available
  t_docId *inids;
  size_t nids;

  const StopWordList *stopwords;

  /** Legacy options */
  struct {
    NumericFilter **filters;
    GeoFilter *gf;
    const char **infields;
    size_t ninfields;
  } legacy;
} RSSearchOptions;

static inline void RSSearchOptions_Init(RSSearchOptions *options) {
  memset(options, 0, sizeof(*options));
  options->slop = -1;
  options->fieldmask = RS_FIELDMASK_ALL;
}

#endif
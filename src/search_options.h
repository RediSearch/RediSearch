#ifndef RS_QUERY_OPTIONS_H_
#define RS_QUERY_OPTIONS_H_

#include <stopwords.h>
#include <redisearch.h>
#include <sortable.h>

typedef enum {
  Search_NoContent = 0x01,
  Search_Verbatim = 0x02,
  Search_NoStopwrods = 0x04,

  Search_WithScores = 0x08,
  Search_WithPayloads = 0x10,

  Search_InOrder = 0x20,

  Search_WithSortKeys = 0x40,
  Search_AggregationQuery = 0x80,
  Search_NoSort = 0x100,
  Search_CursorIterator = 0x200
} RSSearchFlags;

#define RS_DEFAULT_QUERY_FLAGS 0x00
#define RS_CURSOR_INIT (size_t) - 1

// maximum results you can get in one query
#define SEARCH_REQUEST_RESULTS_MAX 1000000

typedef struct {
  /* The index name - since we need to open the spec in a side thread */
  char *indexName;

  // Stopword list
  StopWordList *stopwords;

  // Query language
  const char *language;

  // Pointer to payload info. The target resides in the search request itself
  RSPayload *payload;

  // Global field mask from INFIELDS
  t_fieldMask fieldMask;

  // Global flags
  RSSearchFlags flags;

  // Slop control
  int slop;

  int concurrentMode;

  RSSortingKey *sortBy;

  /* Paging */
  size_t cursor;
  size_t offset;
  size_t num;

  char *expander;

  char *scorer;

} RSSearchOptions;

#define RS_DEFAULT_SEARCHOPTS          \
  ((RSSearchOptions){                  \
      .stopwords = NULL,               \
      .language = NULL,                \
      .payload = NULL,                 \
      .fieldMask = RS_FIELDMASK_ALL,   \
      .flags = RS_DEFAULT_QUERY_FLAGS, \
      .slop = -1,                      \
      .concurrentMode = 1,             \
      .sortBy = NULL,                  \
      .offset = 0,                     \
      .num = 10,                       \
      .expander = NULL,                \
      .scorer = NULL,                  \
  })

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
  char *name;
  SummarizeSettings summarizeSettings;
  HighlightSettings highlightSettings;
  SummarizeMode mode;
  // Whether this field was explicitly requested by `RETURN`
  int explicitReturn;
} ReturnedField;

typedef struct {
  ReturnedField defaultField;

  // List of individual field specifications
  ReturnedField *fields;
  size_t numFields;
  uint16_t wantSummaries;
  // Whether this list contains fields explicitly selected by `RETURN`
  uint16_t explicitReturn;
} FieldList;

#endif
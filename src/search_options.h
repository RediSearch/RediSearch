#ifndef RS_QUERY_OPTIONS_H_
#define RS_QUERY_OPTIONS_H_

#include <stopwords.h>
#include <redisearch.h>
#include <spec.h>

typedef enum {
  Search_NoContent = 0x01,
  Search_Verbatim = 0x02,
  Search_NoStopwrods = 0x04,

  Search_WithScores = 0x08,
  Search_WithPayloads = 0x10,

  Search_InOrder = 0x20,

  Search_WithSortKeys = 0x40,

} RSSearchFlags;

#define RS_DEFAULT_QUERY_FLAGS 0x00

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

#endif
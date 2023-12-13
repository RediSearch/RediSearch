#include "src/module.h"
#include "util/heap.h"
#include "query.h"

#include <stdbool.h>


typedef enum {
  SPECIAL_CASE_NONE,
  SPECIAL_CASE_KNN,
  SPECIAL_CASE_SORTBY
} searchRequestSpecialCase;

typedef struct {
  size_t k;               // K value
  const char* fieldName;  // Field name
  bool shouldSort;        // Should run presort before the coordinator sort
  size_t offset;          // Reply offset
  heap_t *pq;             // Priority queue
  QueryNode* queryNode;   // Query node
} knnContext;

typedef struct {
  const char* sortKey;  // SortKey name;
  bool asc;             // Sort order ASC/DESC
  size_t offset;        // SortKey reply offset
} sortbyContext;

typedef struct {
  union {
    knnContext knn;
    sortbyContext sortby;
  };
  searchRequestSpecialCase specialCaseType;
} specialCaseCtx;

typedef struct {
  char *queryString;
  long long offset;
  long long limit;
  long long requestedResultsCount;
  long long initTime;
  long long timeout;
  int withScores;
  int withExplainScores;
  int withPayload;
  int withSortby;
  int sortAscending;
  int withSortingKeys;
  int noContent;
  uint32_t format; // QEXEC_FORMAT_EXPAND or QEXEC_FORMAT_DEFAULT (0 implies STRING)

  specialCaseCtx** specialCases;
  const char** requiredFields;
  // used to signal profile flag and count related args
  int profileArgs;
  int profileLimited;
  clock_t profileClock;
  void *reducer;
} searchRequestCtx;

specialCaseCtx *prepareOptionalTopKCase(const char *query_string, RedisModuleString **argv, int argc,
                             QueryError *status);

void SpecialCaseCtx_Free(specialCaseCtx* ctx);

void processResultFormat(uint32_t *flags, MRReply *map);

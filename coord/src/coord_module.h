#include "src/module.h"
#include "util/heap.h"
#include "query.h"
#include "special_case_ctx.h"
#include "rs_wall_clock.h"

#include <stdbool.h>

typedef struct {
  char *queryString;
  long long offset;
  long long limit;
  long long requestedResultsCount;
  rs_wall_clock initClock;
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
  rs_wall_clock profileClock;
  void *reducer;
} searchRequestCtx;

specialCaseCtx *prepareOptionalTopKCase(const char *query_string, RedisModuleString **argv, int argc,
                             QueryError *status);

void SpecialCaseCtx_Free(specialCaseCtx* ctx);

void processResultFormat(uint32_t *flags, MRReply *map);

int DistAggregateCommandImp(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, bool isDebug);
int DistSearchCommandImp(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, bool isDebug);
int ProfileCommandHandlerImp(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, bool isDebug);

size_t GetNumShards_UnSafe();

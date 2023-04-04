/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "query_config.h"
#include "config.h"


/*
typedef struct QueryConfig {
  long long maxPrefixExpansions;
  long long minTermPrefix;
  long long maxResultsToUnsortedMode;
  long long minUnionIterHeap;
} QueryConfig;
*/
extern RSConfig RSGlobalConfig;

void queryConfig_init(QueryConfig *config) {
    config->maxPrefixExpansions = RSGlobalConfig.maxPrefixExpansions;
    config->minTermPrefix = RSGlobalConfig.minTermPrefix;
    config->maxResultsToUnsortedMode = RSGlobalConfig.maxResultsToUnsortedMode;
    config->minUnionIterHeap = RSGlobalConfig.minUnionIterHeap;
    config->printProfileClock = RSGlobalConfig.printProfileClock;
}
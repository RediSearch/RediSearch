/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#pragma once
#ifdef __cplusplus
extern "C" {
#endif

typedef struct QueryConfig {
  long long maxPrefixExpansions;
  long long minTermPrefix;
  long long maxResultsToUnsortedMode;
  long long minUnionIterHeap;
  int printProfileClock;
} QueryConfig;

void queryConfig_init(QueryConfig *config);

#ifdef __cplusplus
}
#endif

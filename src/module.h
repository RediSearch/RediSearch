/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef RS_MODULE_H_
#define RS_MODULE_H_

#include "redismodule.h"
#include <query_node.h>
#include <coord/rmr/reply.h>
#include <util/heap.h>
#include "rmutil/rm_assert.h"

// Hack to support Alpine Linux 3 where __STRING is not defined
#if !defined(__GLIBC__) && !defined(__STRING)
#include <sys/cdefs.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

#define PROXY_FILTERED "_proxy-filtered"

int RediSearch_InitModuleInternal(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

int IsMaster();
int IsEnterprise();

size_t GetNumShards_UnSafe();

void GetFormattedRedisVersion(char *buf, size_t len);
void GetFormattedRedisEnterpriseVersion(char *buf, size_t len);

/** Cleans up all globals in the module */
void RediSearch_CleanupModule(void);

// Local spellcheck command
int SpellCheckCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

int RMCreateSearchCommand(RedisModuleCtx *ctx, const char *name,
                  RedisModuleCmdFunc callback, const char *flags, int firstkey,
                  int lastkey, int keystep, const char *aclCategories);

/** Module-level dummy context for certain dummy RM_XXX operations */
extern RedisModuleCtx *RSDummyContext;
/** Indicates that RediSearch_Init was called */
extern int RS_Initialized;

#define RS_AutoMemory(ctx)                      \
do {                                            \
  RS_LOG_ASSERT(ctx != RSDummyContext, "");     \
  RedisModule_AutoMemory(ctx);                  \
} while (0)

#define RedisModule_ReplyWithLiteral(ctx, literal) \
  RedisModule_ReplyWithStringBuffer(ctx, literal, sizeof(literal) - 1)

#define SEARCH_ACL_CATEGORY "search"
#define SEARCH_ACL_INTERNAL_CATEGORY "_search_internal"

#define RM_TRY(expr)                                                  \
  if (expr == REDISMODULE_ERR) {                                      \
    RedisModule_Log(ctx, "warning", "Could not run " __STRING(expr)); \
    return REDISMODULE_ERR;                                           \
  }

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
  long long initClock;
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

int DistAggregateCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int DistSearchCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

#ifdef __cplusplus
}
#endif
#endif

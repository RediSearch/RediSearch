/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef RS_MODULE_H_
#define RS_MODULE_H_

#include "redismodule.h"
#include <query_node.h>
#include <coord/rmr/reply.h>
#include <util/heap.h>
#include "rmutil/rm_assert.h"
#include "shard_window_ratio.h"
#include "coord/special_case_ctx.h"
#include "rs_wall_clock.h"
#include "thpool/thpool.h"

// Hack to support Alpine Linux 3 where __STRING is not defined
#if !defined(__GLIBC__) && !defined(__STRING)
#include <sys/cdefs.h>
#endif

// Module-level dummy context for certain dummy RM_XXX operations
extern RedisModuleCtx *RSDummyContext;

#ifdef __cplusplus
extern "C" {
#endif

// Filter from proxy listing and statistics (e.g., command-stats, latency report etc.)
#define CMD_PROXY_FILTERED "_proxy-filtered"
// Internal command - for internal use, i.e., should NOT be executed by the user
// as it may bypass ACL validations (e.g., '_FT.SEARCH`), or result in an
// unwanted situation such as an unsynchronized cluster (e.g., '_FT.CREATE').
// Thus, these commands are not exposed to the user. For more info, see redis
// docs and code.
#define CMD_INTERNAL "internal"

int RediSearch_InitModuleConfig(RedisModuleCtx *ctx, RedisModuleString **argv, int argc, int registerConfig, int isClusterEnabled);
int RediSearch_InitModuleInternal(RedisModuleCtx *ctx);

extern redisearch_thpool_t *depleterPool;

int IsMaster();
bool IsEnterprise();

size_t GetNumShards_UnSafe();

void GetFormattedRedisVersion(char *buf, size_t len);
void GetFormattedRedisEnterpriseVersion(char *buf, size_t len);

/** Cleans up all globals in the module */
void RediSearch_CleanupModule(void);

// Local spellcheck command
int SpellCheckCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

// Indicates that RediSearch_Init was called
extern int RS_Initialized;

#define RS_AutoMemory(ctx)                      \
do {                                            \
  RedisModule_Assert(ctx != RSDummyContext);    \
  RedisModule_AutoMemory(ctx);                  \
} while (0)

#define RedisModule_ReplyWithLiteral(ctx, literal) \
  RedisModule_ReplyWithStringBuffer(ctx, literal, sizeof(literal) - 1)

#define SEARCH_ACL_CATEGORY "search"
#define SEARCH_ACL_INTERNAL_CATEGORY "_search_internal"

#define NOPERM_ERR "NOPERM User does not have the required permissions to query the index"
#define CLUSTERDOWN_ERR "ERRCLUSTER Uninitialized cluster state, could not perform command"
#define NODEBUG_ERR "Debug commands are disabled, please follow the redis configuration guide to enable them"

#define RM_TRY(expr)                                                  \
  if (expr == REDISMODULE_ERR) {                                      \
    RedisModule_Log(ctx, "warning", "Could not run " __STRING(expr)); \
    return REDISMODULE_ERR;                                           \
  }

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

bool debugCommandsEnabled(RedisModuleCtx *ctx);

specialCaseCtx *prepareOptionalTopKCase(const char *query_string, RedisModuleString **argv, int argc, uint dialectVersion,
                             QueryError *status);

void SpecialCaseCtx_Free(specialCaseCtx* ctx);

void processResultFormat(uint32_t *flags, MRReply *map);

int DistAggregateCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);
int DistSearchCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

void ScheduleContextCleanup(RedisModuleCtx *thctx, struct RedisSearchCtx *sctx);

bool should_return_error(MRReply *reply);

#ifdef __cplusplus
}
#endif
#endif

/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#ifndef RS_MODULE_H_
#define RS_MODULE_H_

#include "redismodule.h"
#include "rmutil/rm_assert.h"

#ifdef __cplusplus
extern "C" {
#endif
int RediSearch_InitModuleInternal(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

int IsMaster();
int IsEnterprise();

void GetFormattedRedisVersion(char *buf, size_t len);
void GetFormattedRedisEnterpriseVersion(char *buf, size_t len);

/** Cleans up all globals in the module */
void RediSearch_CleanupModule(void);

// Local spellcheck command
int SpellCheckCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc);

/** Indicates that RediSearch_Init was called */
extern int RS_Initialized;
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

// A macro that extracts the first argument from a variadic macro
#define SECOND_ARG_HELPER(first, second, ...) second
#define SECOND_ARG(...) SECOND_ARG_HELPER(__VA_ARGS__)

#define RM_CREATE_COMMAND(aclCategories, ...)                                  \
  if (RedisModule_CreateCommand(__VA_ARGS__) == REDISMODULE_ERR) {             \
    RedisModule_Log(ctx, "warning", "Could not create command " #__VA_ARGS__); \
    return REDISMODULE_ERR;                                                    \
  } else {                                                                     \
    RedisModuleCommand *command =                                              \
      RedisModule_GetCommand(ctx, SECOND_ARG(__VA_ARGS__));                    \
    if (!command) {                                                            \
      RedisModule_Log(ctx, "warning",                                          \
        "Could not find command " STRINGIFY(SECOND_ARG(__VA_ARGS__)));         \
      return REDISMODULE_ERR;                                                  \
    }                                                                          \
    if (aclCategories != NULL) {                                               \
      if (RedisModule_SetCommandACLCategories(command, aclCategories) == REDISMODULE_ERR) {\
        RedisModule_Log(ctx, "warning",                                        \
          "Failed to set ACL categories for command " STRINGIFY(SECOND_ARG(__VA_ARGS__)) ". Got error code: %d", errno); \
        return REDISMODULE_ERR;                                                \
      }                                                                        \
    }                                                                          \
  }

#define RM_CREATE_DEPRECATED_COMMAND(...) \
  RM_CREATE_COMMAND(NULL, __VA_ARGS__)

#ifdef __cplusplus
}
#endif
#endif

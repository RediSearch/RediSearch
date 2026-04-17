#pragma once

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include "redismodule.h"

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

/**
 * Initializes a global subscriber that reports Rust `tracing` traces through `redismodule` logging.
 */
void TracingRedisModule_Init(RedisModuleCtx *ctx);

/**
 * Initialize RediSearch's panic hook, without replaacing the pre-existing panic hook (if any).
 *
 * Panic messages will be logged through `tracing` at the `ERROR` level.
 */
void RustPanicHook_Init(void);

/**
 * Add the current backtrace as a new section to the report printed
 * by RediSearch's INFO command.
 *
 * # Safety
 *
 * `ctx` must be a valid pointer to a `RedisModuleInfoCtx`.
 */
void AddToInfo_RustBacktrace(RedisModuleInfoCtx *ctx);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus

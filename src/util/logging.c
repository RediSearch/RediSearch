/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#include "logging.h"

#include <stdarg.h>       // for va_end, va_list, va_start
#include <stdio.h>        // for vsnprintf

#include "module.h"       // for RSDummyContext
#include "redismodule.h"  // for RedisModule_Log

#define LOG_MAX_LEN    1024 /* aligned with LOG_MAX_LEN in redis */

void LogCallback(const char *level, const char *fmt, ...) {
  char msg[LOG_MAX_LEN];

  va_list ap;
  va_start(ap, fmt);
  vsnprintf(msg, sizeof(msg), fmt, ap);
  RedisModule_Log(RSDummyContext, level, "%s", msg);
  va_end(ap);
}

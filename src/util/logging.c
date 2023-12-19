/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include "logging.h"
#include <stdarg.h>
#include "module.h"

#define LOG_MAX_LEN    1024 /* aligned with LOG_MAX_LEN in redis */
int LOGGING_LEVEL = 0;
// L_DEBUG | L_INFO

void LOGGING_INIT(int level) {
  LOGGING_LEVEL = level;
}

void LogCallback(const char *level, const char *fmt, ...) {
  char msg[LOG_MAX_LEN];

  va_list ap;
  va_start(ap, fmt);
  vsnprintf(msg, sizeof(msg), fmt, ap);
  RedisModule_Log(RSDummyContext, level, "%s", msg);
  va_end(ap);
}

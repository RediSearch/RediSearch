/*
 * Copyright Redis Ltd. 2016 - present
 * Licensed under your choice of the Redis Source Available License 2.0 (RSALv2) or
 * the Server Side Public License v1 (SSPLv1).
 */

#include <stdarg.h>
#include "logging.h"
#include "module.h"

#define MAX_LOG_LENGTH 1024
int LOGGING_LEVEL = 0;
// L_DEBUG | L_INFO

void LOGGING_INIT(int level) {
  LOGGING_LEVEL = level;
}


void LogCallback(const char *message, ...) {
  va_list ap;
  va_start(ap, message);
  char fmt_msg[MAX_LOG_LENGTH];
  vsnprintf(fmt_msg, MAX_LOG_LENGTH, message, ap);
  RedisModule_Log(RSDummyContext, "debug", "%s", fmt_msg);
  va_end(ap);
}
